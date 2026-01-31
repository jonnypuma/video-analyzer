import os
import pathlib
import subprocess
import json
import re
import sqlite3
import uuid
import threading
import csv
import io
import time
import sys
import glob
import signal
import tempfile
import zipfile
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from flask import Flask, render_template, jsonify, make_response, request, send_file, Response  # type: ignore
from contextlib import contextmanager
from collections import OrderedDict

# --- OPTIONAL DEPENDENCIES ---
try:
    from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
    HAS_SCHEDULER = True
except ImportError:
    print("WARNING: 'apscheduler' not found. Scheduled scans will be disabled.")
    HAS_SCHEDULER = False

app = Flask(__name__)

# --- CONFIGURATION ---
OUTPUT_DIR = '/output'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_OUTPUT_FALLBACK = os.path.join(BASE_DIR, 'results')
if not os.path.exists(OUTPUT_DIR) and os.path.exists(LOCAL_OUTPUT_FALLBACK):
    OUTPUT_DIR = LOCAL_OUTPUT_FALLBACK
DB_PATH = os.path.join(OUTPUT_DIR, 'processed_videos.db')
VIDEO_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mpeg', '.mpg', '.mov', '.ts', '.m2ts', '.webm', '.wmv'}
SYSTEM_DIRS = {'bin', 'boot', 'dev', 'etc', 'home', 'lib', 'lib64', 'media', 'mnt', 'opt', 'proc', 'root', 'run', 'sbin', 'srv', 'sys', 'tmp', 'usr', 'var', 'app', 'defaults', 'config', 'output'}

# --- CONSTANTS ---
DB_TIMEOUT = 120  # Database connection timeout in seconds
PROCESSED_MAP_CHUNK_SIZE = 10000  # Number of records to load from database per chunk
MAX_RETRIES = 2  # Maximum retries for file analysis
RETRY_DELAY_INITIAL = 1  # Initial retry delay in seconds (exponential backoff)
RPU_CACHE_MAX_SIZE = 50000  # Maximum RPU cache entries (LRU eviction)
LOG_CLEANUP_LIMIT = 5  # Number of old log files to keep
MAX_SCAN_ATTEMPTS = 3  # Maximum scan attempts before skipping a file
PROGRESS_UPDATE_INTERVAL = 10  # Update progress every N files (reduces lock contention)
SUBPROCESS_TIMEOUT = 30  # Subprocess timeout in seconds (30 seconds per command)

# --- GLOBAL STATE ---
APP_START_TIME = time.time()
APP_VERSION = os.environ.get("APP_VERSION", "dev")
PROGRESS = {
    "status": "idle", "current": 0, "total": 0, "file": "Waiting...", 
    "last_full_scan": "Never", "last_duration": "--",
    "scan_completed": False, "new_found": 0, "failed_count": 0, "last_duration": "0s",
    "eta": "", "start_time": 0, "paused": False, "warning_count": 0
}
ABORT_SCAN = False
PAUSE_EVENT = threading.Event()
PAUSE_EVENT.set()
LOG_CACHE = []
DIAG_LOG_TS = 0.0
API_LOG_TS = 0.0
progress_lock = threading.Lock()
db_access_lock = threading.Lock()
LOG_FILE = ""
FAIL_FILE = ""
DEBUG_MODE = False

# PROCESS TRACKING FOR INSTANT KILL
ACTIVE_PROCS = set()
proc_lock = threading.Lock()

# RPU CACHE - Cache RPU extraction results to avoid re-extraction
# Key: (file_path, file_size, mtime), Value: {'dovi_data': dict, 'rpu_size': int}
# Using LRU eviction - most recently used items are kept
RPU_CACHE = OrderedDict()  # OrderedDict for LRU behavior
rpu_cache_lock = threading.Lock()

def clear_rpu_cache() -> None:
    """
    Clear the RPU cache. Useful for force rescans or when cache becomes stale.
    """
    global RPU_CACHE
    with rpu_cache_lock:
        RPU_CACHE.clear()
        if DEBUG_MODE: log_debug("RPU cache cleared", "DEBUG")

scheduler = None
if HAS_SCHEDULER:
    try:
        scheduler = BackgroundScheduler()
        scheduler.start()
    except (OSError, ValueError) as e:
        print(f"Error starting scheduler: {e}")

# --- HELPERS ---
def setup_new_log_files() -> None:
    """Initialize new log files for the current scan session."""
    global LOG_FILE, FAIL_FILE
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    LOG_FILE = os.path.join(OUTPUT_DIR, f"{ts}_scan_activity.log")
    FAIL_FILE = os.path.join(OUTPUT_DIR, f"{ts}_scan_failures.csv")
    try:
        with open(FAIL_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f, delimiter='|').writerow(['Timestamp', 'Volume', 'Path', 'Filename', 'Error'])
    except (OSError, IOError) as e:
        if DEBUG_MODE:
            log_debug(f"Failed to create failure log file: {e}", "WARNING")

def cleanup_old_logs(limit: int = LOG_CLEANUP_LIMIT) -> None:
    """
    Clean up old log files, keeping only the most recent ones.
    
    Args:
        limit: Number of old log files to keep (default: LOG_CLEANUP_LIMIT)
    """
    try:
        for pattern in ["*_scan_activity.log", "*_scan_failures.csv"]:
            files = sorted(glob.glob(os.path.join(OUTPUT_DIR, pattern)))
            if len(files) > limit:
                for f in files[:-limit]:
                    try: 
                        os.remove(f)
                    except (OSError, IOError) as e:
                        if DEBUG_MODE:
                            log_debug(f"Failed to remove old log file {f}: {e}", "WARNING")
    except (OSError, IOError) as e:
        if DEBUG_MODE:
            log_debug(f"Error during log cleanup: {e}", "WARNING")

def cleanup_old_rpu_files() -> None:
    """Clean up any leftover RPU temporary files from previous runs."""
    try:
        temp_dir = tempfile.gettempdir()
        for pattern in ['dovi_*_rpu.bin', 'temp_*_rpu.bin']:
            for temp_file in glob.glob(os.path.join(temp_dir, pattern)):
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                        if DEBUG_MODE:
                            log_debug(f"Cleaned up leftover RPU temp file: {temp_file}", "DEBUG")
                except OSError:
                    pass  # File may have been deleted already or is in use
    except (OSError, PermissionError) as e:
        if DEBUG_MODE:
            log_debug(f"Error cleaning up old RPU files: {e}", "WARNING")

setup_new_log_files()
cleanup_old_logs()

def log_debug(msg: str, level: str = "INFO") -> None:
    """Log a debug message with optional level (DEBUG, INFO, WARNING, ERROR)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    safe = str(msg).encode('utf-8', 'replace').decode('utf-8')
    fmt = f"[{ts}] [{level}] {safe}"
    print(fmt, flush=True)
    try:
        if LOG_FILE:
            with open(LOG_FILE, 'a', encoding='utf-8') as f: f.write(f"{fmt}\n")
    except OSError as e:
        print(f"Failed to write to log file: {e}", flush=True)
    with progress_lock:
        LOG_CACHE.append(fmt)
        if len(LOG_CACHE) > 500: LOG_CACHE.pop(0)

def log_failure(vol: str, path: str, name: str, err: str) -> None:
    """Log a scan failure to both the failure CSV and debug log."""
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if FAIL_FILE:
            with open(FAIL_FILE, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f, delimiter='|').writerow([ts, vol, path, name, err])
        # Also log to debug console
        log_debug(f"[FAILURE] {vol}: {name} - {err}", "ERROR")
    except (OSError, IOError) as e:
        log_debug(f"Failed to write failure log: {e}", "WARNING")

def log_scan_warning(path: str, name: str, message: str) -> None:
    """Log a scan warning to the failure CSV so it shows in the failure log file."""
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if FAIL_FILE:
            with open(FAIL_FILE, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f, delimiter='|').writerow([ts, 'WARNING', path, name, message])
        with progress_lock:
            PROGRESS["warning_count"] = PROGRESS.get("warning_count", 0) + 1
    except (OSError, IOError) as e:
        log_debug(f"Failed to write warning log: {e}", "WARNING")

def record_scan_history(entry: Dict[str, Any]) -> None:
    """
    Persist a scan history entry (keep last 50).
    """
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = dict(entry)
        payload["timestamp"] = payload.get("timestamp") or now
        with get_db() as conn:
            conn.execute(
                "INSERT INTO scan_history (entry, created_at) VALUES (?, ?)",
                (json.dumps(payload), payload["timestamp"])
            )
            conn.execute(
                "DELETE FROM scan_history WHERE id NOT IN (SELECT id FROM scan_history ORDER BY id DESC LIMIT 50)"
            )
    except Exception as e:
        if DEBUG_MODE:
            log_debug(f"Failed to record scan history: {e}", "WARNING")

def wait_if_paused() -> None:
    """Block worker threads while scan is paused; abort still exits immediately."""
    while not PAUSE_EVENT.is_set():
        if ABORT_SCAN:
            raise RuntimeError("Scan Aborted")
        time.sleep(0.2)

def get_mount_status() -> dict:
    """
    Get status of all mounted volumes.
    
    Returns:
        Dictionary mapping volume names to their mount paths
    """
    mounts = {}
    if os.environ.get("SCAN_PATHS"):
        for p in os.environ.get("SCAN_PATHS").split(','):
            p = p.strip()
            if os.path.exists(p):
                mounts[os.path.basename(p)] = p
        return mounts

    abs_output = os.path.abspath(OUTPUT_DIR)
    try:
        for d in os.listdir('/'):
            path = os.path.join('/', d)
            abs_path = os.path.abspath(path)
            if d in SYSTEM_DIRS: continue
            if abs_path == abs_output: continue
            if os.path.isdir(path):
                mounts[d] = path
    except (OSError, ValueError) as e:
        log_debug(f"⚠️ Error detecting paths: {e}")
    return mounts

# --- DATABASE ---
@contextmanager
def get_db() -> Any:
    """
    Context manager for database connections with automatic commit/rollback.
    
    Yields:
        sqlite3.Connection: Database connection object
        
    Example:
        with get_db() as conn:
            conn.execute("SELECT * FROM videos")
    """
    """
    Context manager for database connections with thread safety.
    
    Yields:
        sqlite3.Connection: Database connection object
    """
    with db_access_lock:
        conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

@contextmanager
def get_db_readonly() -> Any:
    """
    Context manager for read-only database connections without a global lock.
    Safe with WAL; allows UI polling during scans.
    """
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
    finally:
        conn.close()

def ensure_video_column(col: str, type_def: str) -> None:
    """
    Ensure a column exists on videos table (safe for hot paths).
    """
    try:
        with get_db() as conn:
            existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(videos)").fetchall()}
            if col not in existing_cols:
                log_debug(f"Migrating DB: Adding missing column '{col}'...", "WARNING")
                conn.execute(f"ALTER TABLE videos ADD COLUMN {col} {type_def}")
    except sqlite3.Error as e:
        log_debug(f"Migration Error: {e}", "ERROR")
def init_db() -> None:
    """
    Initialize the database with required tables and migrations.
    """
    log_debug("Initializing Database...")
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with get_db_readonly() as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
        conn.execute('CREATE TABLE IF NOT EXISTS scan_history (id INTEGER PRIMARY KEY AUTOINCREMENT, entry TEXT, created_at TEXT)')
        conn.execute('''CREATE TABLE IF NOT EXISTS videos 
                        (filename TEXT, category TEXT, profile TEXT, el_type TEXT, 
                         container TEXT, source_vol TEXT, full_path TEXT PRIMARY KEY,
                         last_scanned TEXT, resolution TEXT, bitrate_mbps REAL, scan_error TEXT,
                         is_hybrid INTEGER DEFAULT 0, secondary_hdr TEXT,
                         width INTEGER, height INTEGER, file_size INTEGER, bl_compatibility_id TEXT,
                        audio_codecs TEXT, audio_langs TEXT, audio_channels TEXT, subtitles TEXT, max_cll TEXT, max_fall TEXT,
                        fps REAL, aspect_ratio TEXT,
                        imdb_id TEXT, tvdb_id TEXT, tmdb_id TEXT, rotten_id TEXT, metacritic_id TEXT, trakt_id TEXT,
                        imdb_rating REAL, tvdb_rating REAL, tmdb_rating REAL, rotten_rating REAL, metacritic_rating REAL, trakt_rating REAL,
                         scan_attempts INTEGER DEFAULT 0,
                         video_source TEXT, source_format TEXT, video_codec TEXT, is_3d INTEGER DEFAULT 0, edition TEXT, year INTEGER,
                         media_type TEXT, show_title TEXT, season INTEGER, episode INTEGER, movie_title TEXT, episode_title TEXT,
                         nfo_missing INTEGER DEFAULT 0, validation_flag TEXT)''')
        
        try:
            existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(videos)").fetchall()}
            required_cols = {
                'audio_codecs': 'TEXT', 'audio_langs': 'TEXT', 'audio_channels': 'TEXT', 'subtitles': 'TEXT', 
                'max_cll': 'TEXT', 'max_fall': 'TEXT', 'scan_attempts': 'INTEGER DEFAULT 0',
                'fps': 'REAL', 'aspect_ratio': 'TEXT',
                'imdb_id': 'TEXT', 'tvdb_id': 'TEXT', 'tmdb_id': 'TEXT', 'rotten_id': 'TEXT', 'metacritic_id': 'TEXT', 'trakt_id': 'TEXT',
                'imdb_rating': 'REAL', 'tvdb_rating': 'REAL', 'tmdb_rating': 'REAL', 'rotten_rating': 'REAL', 'metacritic_rating': 'REAL', 'trakt_rating': 'REAL',
                'video_source': 'TEXT', 'source_format': 'TEXT', 'video_codec': 'TEXT', 
                'is_3d': 'INTEGER DEFAULT 0', 'edition': 'TEXT', 'year': 'INTEGER',
                'media_type': 'TEXT', 'show_title': 'TEXT', 'season': 'INTEGER', 'episode': 'INTEGER',
                'movie_title': 'TEXT', 'episode_title': 'TEXT', 'nfo_missing': 'INTEGER DEFAULT 0', 'validation_flag': 'TEXT'
            }
            for col, type_def in required_cols.items():
                if col not in existing_cols: 
                    log_debug(f"Migrating DB: Adding missing column '{col}'...")
                    conn.execute(f"ALTER TABLE videos ADD COLUMN {col} {type_def}")
        except sqlite3.Error as e:
            log_debug(f"Migration Error: {e}")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON videos (category)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vol ON videos (source_vol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_profile ON videos (profile)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_container ON videos (container)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_resolution ON videos (resolution)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_error ON videos (scan_error)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_is_hybrid ON videos (is_hybrid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_last_scanned ON videos (last_scanned)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_video_source ON videos (video_source)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_format ON videos (source_format)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_video_codec ON videos (video_codec)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_is_3d ON videos (is_3d)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_year ON videos (year)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_type ON videos (media_type)")
        
        defaults = {'threads': '4', 'skip_words': 'trailer,sample', 'min_size_mb': '50', 'refresh_interval': '60', 'notif_style': 'modal', 'force_rescan': 'false', 'column_order': '', 'scan_folders': '[]', 'scan_extras': 'false', 'debug_mode': 'false'}
        for k, v in defaults.items(): conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))

    log_debug("Database ready.")
    
    mounts = get_mount_status()
    if mounts:
        log_debug("--- VOLUME STATUS CHECK ---")
        for vol, path in mounts.items():
            log_debug(f"Volume {vol}: ONLINE ✅")
    else:
        log_debug("⚠️ No media volumes detected in root.")

# --- EXECUTION WRAPPER ---
def run_command(cmd_list: list, capture: bool = True, capture_stderr: bool = False) -> tuple[int, str, str]:
    """
    Runs a command with ability to kill it instantly.
    
    Matches old version: always uses text=True, paths are already encoded via os.fsencode/fsdecode
    in scan_file_worker before being passed here.
    
    Args:
        cmd_list: List of command and arguments (paths should already be properly encoded strings)
        capture: If True, capture and return stdout; if False, return None
        capture_stderr: If True, capture and return stderr (useful for error diagnostics)
        
    Returns:
        Tuple of (return_code, stdout, stderr) if capture_stderr is True, (return_code, stdout, "") otherwise
    """
    global ABORT_SCAN
    if ABORT_SCAN: raise RuntimeError("Scan Aborted")
    
    # Match old version exactly: use text=True, pass paths as strings
    # Path is already normalized via os.fsencode/fsdecode in scan_file_worker
    p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE if capture_stderr else subprocess.DEVNULL, text=True, start_new_session=True)
    
    with proc_lock: ACTIVE_PROCS.add(p)
    
    # Use a separate thread to enforce timeout (communicate timeout may not work if process is truly hung)
    timeout_occurred = threading.Event()
    def kill_on_timeout():
        time.sleep(SUBPROCESS_TIMEOUT)
        if p.poll() is None:  # Process still running
            timeout_occurred.set()
            if DEBUG_MODE: log_debug(f"[RUN_COMMAND] Timeout thread killing process {p.pid} for: {cmd_list[0]}", "WARNING")
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except (OSError, ProcessLookupError, ValueError):
                pass
            try:
                p.kill()
            except (OSError, ProcessLookupError, ValueError):
                pass
    
    timeout_thread = threading.Thread(target=kill_on_timeout, daemon=True)
    timeout_thread.start()
    
    try:
        # Check for abort before blocking on communicate
        if ABORT_SCAN: 
            p.kill()
            raise RuntimeError("Scan Aborted")
        # Try communicate with timeout first
        stdout = ""
        stderr = ""
        try:
            stdout_result, stderr_result = p.communicate(timeout=SUBPROCESS_TIMEOUT + 2)  # Give timeout thread a head start
            stdout = stdout_result if stdout_result else ""
            stderr = (stderr_result if stderr_result else "") if capture_stderr else ""
        except subprocess.TimeoutExpired:
            # This should rarely happen since timeout thread should kill it first
            stdout = ""
            stderr = ""
        
        # Check if timeout occurred (timeout thread killed the process)
        if timeout_occurred.is_set():
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                    p.wait(timeout=1)
                except (OSError, ProcessLookupError, ValueError, subprocess.TimeoutExpired):
                    pass
            with proc_lock: ACTIVE_PROCS.discard(p)
            raise RuntimeError(f"Command timed out after {SUBPROCESS_TIMEOUT}s: {cmd_list[0]}")
    finally:
        timeout_thread.join(timeout=0.1)  # Wait briefly for timeout thread
        with proc_lock: ACTIVE_PROCS.discard(p)
    
    return p.returncode, (stdout if capture else ""), (stderr if capture_stderr else "")

# --- ANALYSIS ---
def parse_filename_metadata(filename: str) -> dict:
    """
    Extract metadata from filename including source, format, edition, year, and 3D status.
    
    Args:
        filename: Filename to parse
        
    Returns:
        Dictionary with video_source, source_format, edition, year, is_3d
    """
    filename_lower = filename.lower()
    
    # Extract video source (Bluray, UHD Bluray, WEB-Rip, WEB-DL, DVD, etc.)
    video_source = None
    source_patterns = [
        (r'\b(uhd[-\s]?blu[-\s]?ray|uhd|ultra[-\s]?hd)\b', 'UHD Bluray'),
        (r'\b(blu[.\-\s]?ray|blueray|blue[.\-\s]?ray|bd|brdisk|br[-\s]?disk)\b', 'Bluray'),
        (r'\b(webdl|web[.\-\s]?dl|web[-\s]?dlrip)\b', 'WEB-DL'),
        (r'\b(webrip|web[.\-\s]?rip)\b', 'WEB-Rip'),
        (r'\b(web)\b', 'WEB'),
        (r'\b(bdrip|bd[.\-\s]?rip)\b', 'BD-Rip'),
        (r'\b(dvd[-\s]?rip|dvd)\b', 'DVD'),
        (r'\b(hd[-\s]?dvd)\b', 'HD-DVD'),
        (r'\b(laserdisc|ld)\b', 'Laserdisc'),
        (r'\b(hdtv|tv[-\s]?rip)\b', 'HDTV'),
        (r'\b(sdtv|tv[-\s]?rip)\b', 'SDTV'),
        (r'\b(vhs|betamax)\b', 'VHS'),
    ]
    for pattern, source in source_patterns:
        if re.search(pattern, filename_lower):
            video_source = source
            break
    if video_source == 'Bluray':
        if re.search(r'\b(2160p|4k|uhd|ultra[-\s]?hd)\b', filename_lower):
            video_source = 'UHD Bluray'
    
    # Extract source format (ISO, BR-DISK, Remux, etc.)
    source_format = None
    format_patterns = [
        (r'\.(iso|img)\b', 'ISO'),
        (r'\b(complete|\.complete\.)\b', 'ISO'),
        (r'\b(br[-\s]?disk|brdisk|br[-\s]?disk)\b', 'BR-DISK'),
        (r'\b(remux|bdremux)\b', 'Remux'),
        (r'\b(encode|encoded)\b', 'Encode'),
        (r'\b(rip)\b', 'Rip'),
    ]
    for pattern, fmt in format_patterns:
        if re.search(pattern, filename_lower):
            source_format = fmt
            break
    
    # Extract edition
    edition = None
    edition_patterns = [
        (r'\b(remastered)\b', 'Remastered'),
        (r'\b(uncut)\b', 'Uncut'),
        (r'\b(unrated)\b', 'Unrated'),
        (r'\b(directors[-\s]?cut|dircut|dc)\b', "Director's Cut"),
        (r'\b(extended[-\s]?cut|extended)\b', 'Extended Cut'),
        (r'\b((?:20th|25th|30th|40th|50th)[-\s]?anniversary[-\s]?edition)\b', 'Anniversary Edition'),
        (r'\b(collectors[-\s]?edition|collector\'s[-\s]?edition)\b', "Collector's Edition"),
        (r'\b(limited[-\s]?edition)\b', 'Limited Edition'),
        (r'\b(deluxe[-\s]?edition)\b', 'Deluxe Edition'),
        (r'\b(steelbook)\b', 'Steelbook'),
        (r'\b(imax)\b', 'IMAX'),
        (r'\b(final[-\s]?cut)\b', 'Final Cut'),
        (r'\b(ultimate[-\s]?cut)\b', 'Ultimate Cut'),
        (r'\b(open[-\s]?matte)\b', 'Open Matte'),
        (r'\b(special[-\s]?edition)\b', 'Special Edition'),
        (r'\b(special[-\s]?edition[-\s]?4k|4k[-\s]?special[-\s]?edition)\b', 'Special Edition 4K'),
        (r'\b(theatrical[-\s]?cut|theatrical)\b', 'Theatrical Cut'),
        (r'\b(ultimate[-\s]?edition)\b', 'Ultimate Edition'),
        (r'\b(collectors[-\s]?set|collector\'s[-\s]?set)\b', "Collector's Set"),
        (r'\b(vinegar[-\s]?syndrome)\b', 'Vinegar Syndrome'),
        (r'\b(criterion[-\s]?edition|criterion)\b', 'Criterion Edition'),
    ]
    for pattern, ed in edition_patterns:
        if re.search(pattern, filename_lower):
            edition = ed
            break
    
    # Extract year (4-digit year, typically between 1900-2100)
    year = None
    year_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', filename)
    if year_match:
        try:
            year = int(year_match.group(1))
        except (ValueError, AttributeError):
            pass
    
    # Check if 3D
    is_3d = 1 if re.search(r'\b(3d|sbs|hsbs|ou|h-ou|half-ou)\b', filename_lower) else 0
    
    return {
        'video_source': video_source,
        'source_format': source_format,
        'edition': edition,
        'year': year,
        'is_3d': is_3d
    }

def compute_validation_flag(meta: dict) -> str | None:
    """
    Build a validation flag string for inconsistent media metadata.
    """
    flags = []
    media_type = (meta.get('media_type') or '').strip().lower()
    season = meta.get('season')
    episode = meta.get('episode')
    show_title = meta.get('show_title')
    episode_title = meta.get('episode_title')
    movie_title = meta.get('movie_title')

    if media_type == 'movie':
        if season is not None or episode is not None:
            flags.append('movie_with_season_episode')
        if show_title or episode_title:
            flags.append('movie_with_show_fields')
    elif media_type == 'tv':
        if movie_title:
            flags.append('tv_with_movie_title')
        if season is None or episode is None:
            flags.append('tv_missing_season_episode')
    else:
        if season is not None or episode is not None:
            flags.append('type_missing_with_season_episode')

    return ",".join(flags) if flags else None

def parse_tv_from_filename(filename_lower: str) -> tuple[str | None, int | None, int | None]:
    match = re.search(r'\b[sS](\d{1,2})[ ._-]*[eE](\d{1,2})\b', filename_lower)
    if match:
        return 'tv', int(match.group(1)), int(match.group(2))
    match = re.search(r'\b(\d{1,2})x(\d{1,2})\b', filename_lower)
    if match:
        return 'tv', int(match.group(1)), int(match.group(2))
    return None, None, None

def parse_kodi_nfo(nfo_path: str) -> dict:
    if not nfo_path or not os.path.exists(nfo_path):
        return {}
    try:
        tree = ET.parse(nfo_path)
        root = tree.getroot()
    except (ET.ParseError, OSError, UnicodeDecodeError):
        try:
            with open(nfo_path, "rb") as f:
                raw = f.read()
            text = raw.decode("utf-8", errors="replace")
            text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", text)
            # Try parsing full text first
            root = ET.fromstring(text)
        except Exception:
            try:
                # Fallback: extract the first valid root block
                for tag in ("movie", "tvshow", "episodedetails", "episode"):
                    match = re.search(rf"<{tag}[\s\S]*?</{tag}>", text, re.IGNORECASE)
                    if match:
                        root = ET.fromstring(match.group(0))
                        break
                else:
                    return {}
            except Exception:
                return {}

    def find_text(tag_name: str) -> str | None:
        target = tag_name.lower()
        for node in root.iter():
            node_tag = node.tag.split('}')[-1].lower()
            if node_tag == target and node.text:
                return node.text.strip()
        return None

    def find_any_text(tag_names: list[str]) -> str | None:
        for name in tag_names:
            value = find_text(name)
            if value:
                return value
        return None

    def parse_year(text: str | None) -> int | None:
        if not text:
            return None
        match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    def parse_rating_text(text: str | None) -> float | None:
        if not text:
            return None
        match = re.search(r'(\d+(?:\.\d+)?)', text)
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None

    def find_uniqueid(unique_type: str) -> str | None:
        for node in root.iter():
            if node.tag.split('}')[-1].lower() != 'uniqueid':
                continue
            if node.attrib.get('type', '').lower() == unique_type.lower() and node.text:
                return node.text.strip()
        return None

    def apply_ratings_block(data: dict) -> None:
        for node in root.iter():
            if node.tag.split('}')[-1].lower() != 'rating':
                continue
            name = (node.attrib.get('name') or '').lower()
            value_node = None
            for child in node:
                if child.tag.split('}')[-1].lower() == 'value':
                    value_node = child
                    break
            rating_val = parse_rating_text(value_node.text.strip() if value_node is not None and value_node.text else None)
            if rating_val is None:
                continue
            if name == 'imdb':
                data['imdb_rating'] = rating_val
            elif name in ('themoviedb', 'tmdb'):
                data['tmdb_rating'] = rating_val
            elif name == 'tvdb':
                data['tvdb_rating'] = rating_val
            elif name == 'trakt':
                data['trakt_rating'] = rating_val
            elif name in ('rottentomatoes', 'rotten'):
                data['rotten_rating'] = rating_val
            elif name == 'metacritic':
                data['metacritic_rating'] = rating_val

    tag = (root.tag or '').lower()
    data: dict[str, Any] = {}

    if tag in ('episodedetails', 'episode'):
        data['media_type'] = 'tv'
        data['show_title'] = find_any_text(['showtitle', 'tvshowtitle', 'seriesname', 'showname'])
        data['episode_title'] = find_text('title')
        season_text = find_text('season')
        episode_text = find_text('episode')
        if season_text and season_text.isdigit():
            data['season'] = int(season_text)
        if episode_text and episode_text.isdigit():
            data['episode'] = int(episode_text)
        data['year'] = parse_year(find_text('premiered') or find_text('aired') or find_text('year'))
        data['imdb_id'] = find_uniqueid('imdb') or find_text('imdbid')
        data['tvdb_id'] = find_uniqueid('tvdb') or find_text('tvdbid')
        data['tmdb_id'] = find_uniqueid('tmdb') or find_text('tmdbid')
        data['trakt_id'] = find_uniqueid('trakt') or find_text('traktid')
        data['rotten_id'] = find_any_text(['rottentomatoesid', 'rottentomatoes', 'rottenid', 'rottentomatoes_id'])
        data['metacritic_id'] = find_any_text(['metacriticid', 'metacritic', 'metacritic_id'])
        apply_ratings_block(data)
    elif tag == 'tvshow':
        data['media_type'] = 'tv'
        data['show_title'] = find_any_text(['title', 'showtitle', 'tvshowtitle', 'seriesname', 'showname'])
        data['year'] = parse_year(find_text('premiered') or find_text('year'))
        data['imdb_id'] = find_uniqueid('imdb') or find_text('imdbid')
        data['tvdb_id'] = find_uniqueid('tvdb') or find_text('tvdbid')
        data['tmdb_id'] = find_uniqueid('tmdb') or find_text('tmdbid')
        data['trakt_id'] = find_uniqueid('trakt') or find_text('traktid')
        data['rotten_id'] = find_any_text(['rottentomatoesid', 'rottentomatoes', 'rottenid', 'rottentomatoes_id'])
        data['metacritic_id'] = find_any_text(['metacriticid', 'metacritic', 'metacritic_id'])
        apply_ratings_block(data)
    elif tag == 'movie':
        data['media_type'] = 'movie'
        data['title'] = find_text('title')
        data['year'] = parse_year(find_text('year') or find_text('premiered') or find_text('releasedate'))
        data['imdb_id'] = find_uniqueid('imdb') or find_text('imdbid')
        data['tvdb_id'] = find_uniqueid('tvdb') or find_text('tvdbid')
        data['tmdb_id'] = find_uniqueid('tmdb') or find_text('tmdbid')
        data['trakt_id'] = find_uniqueid('trakt') or find_text('traktid')
        data['rotten_id'] = find_any_text(['rottentomatoesid', 'rottentomatoes', 'rottenid', 'rottentomatoes_id'])
        data['metacritic_id'] = find_any_text(['metacriticid', 'metacritic', 'metacritic_id'])
        apply_ratings_block(data)
    else:
        data['show_title'] = find_any_text(['showtitle', 'tvshowtitle', 'seriesname', 'showname'])
        data['episode_title'] = find_text('title')
        data['title'] = find_text('title')
        data['year'] = parse_year(find_text('premiered') or find_text('aired') or find_text('year'))
        data['imdb_id'] = find_uniqueid('imdb') or find_text('imdbid')
        data['tvdb_id'] = find_uniqueid('tvdb') or find_text('tvdbid')
        data['tmdb_id'] = find_uniqueid('tmdb') or find_text('tmdbid')
        data['trakt_id'] = find_uniqueid('trakt') or find_text('traktid')
        data['rotten_id'] = find_any_text(['rottentomatoesid', 'rottentomatoes', 'rottenid', 'rottentomatoes_id'])
        data['metacritic_id'] = find_any_text(['metacriticid', 'metacritic', 'metacritic_id'])
        apply_ratings_block(data)

    return data

def coerce_tv_nfo_to_movie(result: dict, filename_base: str, media_type_guess: str | None, file_path: str | None = None) -> None:
    """Coerce TV-style NFO to movie when show title is a generic Movies folder."""
    if result.get('media_type') != 'tv':
        return
    if media_type_guess == 'tv':
        return
    show_title = (result.get('show_title') or '').strip().lower()
    if show_title not in ('movies', 'movie'):
        return
    movie_title = result.get('movie_title') or result.get('episode_title') or guess_movie_title_from_filename(filename_base)
    if not movie_title:
        return
    log_debug(f"[NFO] Coercing TV-style NFO to movie for '{filename_base}' (show_title='{show_title}')", "WARNING")
    if file_path:
        log_scan_warning(file_path, filename_base, f"NFO shows TV with show_title '{show_title}', coerced to movie")
    result['media_type'] = 'movie'
    result['movie_title'] = movie_title
    result['show_title'] = None
    result['season'] = None
    result['episode'] = None
    result['episode_title'] = None

def guess_movie_title_from_filename(filename: str) -> str | None:
    name = pathlib.Path(filename).stem
    name = re.sub(r'[._]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    if not name:
        return None
    # Remove year and common media tags
    name = re.sub(r'\b(19\d{2}|20[0-2]\d)\b', ' ', name)
    name = re.sub(r'\b(4320p|2160p|1080p|720p|480p|8k|4k|uhd|hdr|hdr10\+?|dolbyvision|dv|remux|bluray|blu-ray|bdrip|web[-\s]?dl|webrip|x265|x264|hevc|h\.?265|h\.?264|aac|dts|truehd|atmos|ddp|dd\+|eac3|ac3|10bit|8bit|nf|amzn|itunes)\b', ' ', name, flags=re.IGNORECASE)
    # Drop bracketed metadata
    name = re.sub(r'\[[^\]]+\]|\([^\)]+\)|\{[^\}]+\}', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or None

def guess_episode_title_from_filename(filename: str) -> str | None:
    name = pathlib.Path(filename).stem
    match = re.search(r'\b(s\d{1,2}e\d{1,2})\b', name, re.IGNORECASE)
    if match:
        name = name[match.end():]
    else:
        match = re.search(r'\b(\d{1,2}x\d{1,2})\b', name, re.IGNORECASE)
        if match:
            name = name[match.end():]
    name = re.sub(r'[._]+', ' ', name)
    name = re.sub(r'\[[^\]]+\]|\([^\)]+\)|\{[^\}]+\}', ' ', name)
    name = re.sub(r'\b(19\d{2}|20[0-2]\d)\b', ' ', name)
    name = re.sub(r'\b(4320p|2160p|1080p|720p|480p|8k|4k|uhd|hdr|hdr10\+?|dolbyvision|dv|remux|bluray|blu-ray|bdrip|web[-\s]?dl|webrip|x265|x264|hevc|h\.?265|h\.?264|aac|dts|truehd|atmos|ddp|dd\+|eac3|ac3|10bit|8bit|nf|amzn|itunes)\b', ' ', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip()
    return name or None

def find_kodi_nfo_candidates(file_path: str, media_type_hint: str | None) -> list[str]:
    candidates: list[str] = []
    try:
        file_path_obj = pathlib.Path(file_path)
    except OSError:
        return candidates
    same_stem = file_path_obj.with_suffix('.nfo')
    if same_stem.exists():
        candidates.append(str(same_stem))
    else:
        try:
            def normalize(name: str) -> str:
                name = re.sub(r'[._]+', ' ', name)
                name = re.sub(r'\s+', ' ', name).strip().lower()
                return name
            target_norm = normalize(file_path_obj.stem)
            nfo_files = list(file_path_obj.parent.glob('*.nfo'))
            for nfo in nfo_files:
                if normalize(nfo.stem) == target_norm:
                    candidates.append(str(nfo))
                    break
            if not candidates and len(nfo_files) == 1:
                candidates.append(str(nfo_files[0]))
        except OSError:
            pass
    if media_type_hint == 'tv':
        # Try to locate episode NFOs that don't share the same stem
        try:
            filename_lower = file_path_obj.name.lower()
            _, season_guess, episode_guess = parse_tv_from_filename(filename_lower)
            if season_guess is not None and episode_guess is not None:
                for nfo in file_path_obj.parent.glob('*.nfo'):
                    nfo_data = parse_kodi_nfo(str(nfo))
                    if not nfo_data:
                        continue
                    if (
                        nfo_data.get('media_type') == 'tv'
                        and nfo_data.get('season') == season_guess
                        and nfo_data.get('episode') == episode_guess
                    ):
                        candidates.append(str(nfo))
                        break
        except OSError:
            pass
        # tvshow.nfo often lives in the series root (up to a few levels up)
        for parent in [file_path_obj.parent, file_path_obj.parent.parent, file_path_obj.parent.parent.parent]:
            tvshow_nfo = parent / 'tvshow.nfo'
            if tvshow_nfo.exists():
                candidates.append(str(tvshow_nfo))
    else:
        # movie.nfo or folder-named .nfo can live a few levels up
        for parent in [file_path_obj.parent, file_path_obj.parent.parent, file_path_obj.parent.parent.parent]:
            movie_nfo = parent / 'movie.nfo'
            if movie_nfo.exists():
                candidates.append(str(movie_nfo))
            folder_nfo = parent / f"{parent.name}.nfo"
            if folder_nfo.exists():
                candidates.append(str(folder_nfo))
        tvshow_nfo = file_path_obj.parent / 'tvshow.nfo'
        if tvshow_nfo.exists():
            candidates.append(str(tvshow_nfo))
    # Keep order but dedupe
    seen = set()
    ordered = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            ordered.append(c)
    return ordered

def guess_show_title_from_path(file_path: str) -> str | None:
    try:
        file_path_obj = pathlib.Path(file_path)
    except OSError:
        return None
    for parent in file_path_obj.parents:
        name = parent.name.strip()
        if not name:
            continue
        if re.match(r'^(season|s\d{1,2}|specials?)$', name, re.IGNORECASE):
            continue
        if re.match(r'^(season)[\s._-]*\d{1,2}$', name, re.IGNORECASE):
            continue
        if re.match(r'^s[\s._-]*\d{1,2}$', name, re.IGNORECASE):
            continue
        return name
    return None

def extract_video_codec(filename: str, probe_data: dict) -> str | None:
    """
    Extract video codec from ffprobe data, with filename as fallback.
    Embedded metadata takes precedence if there's a discrepancy.
    
    Args:
        filename: Filename to check for codec hints
        probe_data: ffprobe JSON data
        
    Returns:
        Video codec string (HEVC, H.264, AV1, etc.) or None
    """
    codec_from_probe = None
    codec_from_filename = None
    
    # Extract from ffprobe video stream
    for stream in probe_data.get('streams', []):
        if stream.get('codec_type') == 'video':
            codec_long = stream.get('codec_name', '').lower()
            codec_long_lower = codec_long.lower()
            
            codec_map = {
                'hevc': 'HEVC', 'h265': 'HEVC', 'h.265': 'HEVC', 'x265': 'HEVC',
                'av1': 'AV1', 'av01': 'AV1',
                'h264': 'H.264', 'h.264': 'H.264', 'x264': 'H.264', 'avc': 'H.264',
                'mpeg4': 'MPEG-4', 'mpeg2video': 'MPEG-2', 'mpeg1video': 'MPEG-1',
                'vc1': 'VC-1', 'wmv3': 'WMV3',
                'vp8': 'VP8', 'vp9': 'VP9',
                'xvid': 'Xvid', 'divx': 'DivX',
            }
            
            for key, codec in codec_map.items():
                if key in codec_long_lower:
                    codec_from_probe = codec
                    break
            
            if codec_from_probe:
                break
    
    # Extract from filename
    filename_lower = filename.lower()
    filename_codec_patterns = [
        (r'\b(hevc|h265|h\.265|x265)\b', 'HEVC'),
        (r'\b(av1|av01)\b', 'AV1'),
        (r'\b(h264|h\.264|x264|avc)\b', 'H.264'),
        (r'\b(mpeg[-\s]?4|mpeg4)\b', 'MPEG-4'),
        (r'\b(mpeg[-\s]?2|mpeg2)\b', 'MPEG-2'),
        (r'\b(mpeg[-\s]?1|mpeg1)\b', 'MPEG-1'),
        (r'\b(vc[-\s]?1)\b', 'VC-1'),
        (r'\b(wmv3)\b', 'WMV3'),
        (r'\b(vp8)\b', 'VP8'),
        (r'\b(vp9)\b', 'VP9'),
        (r'\b(xvid)\b', 'Xvid'),
        (r'\b(divx)\b', 'DivX'),
    ]
    
    for pattern, codec in filename_codec_patterns:
        if re.search(pattern, filename_lower):
            codec_from_filename = codec
            break
    
    # Prefer embedded metadata over filename
    if codec_from_probe:
        return codec_from_probe
    return codec_from_filename

def analyze_file_deep(path: str) -> dict:
    """
    Perform deep analysis of a video file to extract metadata.
    
    Args:
        path: Full path to the video file
        
    Returns:
        Dictionary containing all extracted metadata including format, profile, 
        resolution, bitrate, HDR info, audio/subtitle tracks, etc.
    """
    result = {
        'format': 'sdr_only', 'dovi_profile': None, 'dovi_el_type': None, 
        'bl_compatibility_id': None, 'hdr_format_secondary': None, 
        'resolution': None, 'width': 0, 'height': 0, 'bitrate': 0, 
        'is_hybrid': 0, 'is_source_hybrid': 0, 'error': None,
        'audio_codecs': [], 'audio_langs': [], 'audio_channels': [], 'subtitles': [], 
        'max_cll': None, 'max_fall': None,
        'fps': None, 'aspect_ratio': None,
        'imdb_id': None, 'tvdb_id': None, 'tmdb_id': None, 'rotten_id': None, 'metacritic_id': None, 'trakt_id': None,
        'imdb_rating': None, 'tvdb_rating': None, 'tmdb_rating': None, 'rotten_rating': None, 'metacritic_rating': None, 'trakt_rating': None,
        'video_source': None, 'source_format': None, 'video_codec': None, 
        'is_3d': 0, 'edition': None, 'year': None,
        'media_type': None, 'show_title': None, 'season': None, 'episode': None, 'movie_title': None, 'episode_title': None,
        'nfo_missing': 1
    }
    
    # Early validation - check if file exists and is accessible
    try:
        if not os.path.exists(path): 
            result['error'] = "File not found"
            return _finalize_result(result)
        # Try to access the file to catch permission errors early
        if not os.access(path, os.R_OK):
            result['error'] = "File not accessible (permission denied)"
            return _finalize_result(result)
    except (OSError, UnicodeEncodeError, UnicodeDecodeError) as e:
        result['error'] = f"File access error: {str(e)}"
        if DEBUG_MODE:
            log_debug(f"Early file access check failed for {path}: {e}", "ERROR")
        return _finalize_result(result)

    # Initialize variables that might be used in nested try blocks
    enhancement_layer_found = False
    sec_hdrs = []
    is_hlg_base = False

    try:
        # 1. FFPROBE
        # Path is already properly encoded via os.fsencode/fsdecode in scan_file_worker
        if DEBUG_MODE: log_debug(f"[FFPROBE] Starting ffprobe for: {path}", "DEBUG")
        probe_cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', path]
        rc, out, err = run_command(probe_cmd, capture_stderr=True)
        if DEBUG_MODE: log_debug(f"[FFPROBE] Completed with return code: {rc}", "DEBUG")
        if rc != 0: 
            error_msg = f"ffprobe failed with return code {rc}"
            if err:
                # Include stderr output for more detailed error information
                error_msg = f"ffprobe failed (code {rc}): {err.strip()}"
            result['error'] = error_msg
            if DEBUG_MODE: log_debug(f"ffprobe failed for {path}: {error_msg}", "ERROR")
            return _finalize_result(result)
        try:
            probe_data = json.loads(out)
        except json.JSONDecodeError as e:
            result['error'] = f"Failed to parse ffprobe JSON: {e}"
            if DEBUG_MODE: log_debug(f"JSON parse error for {path}: {e}", "ERROR")
            return _finalize_result(result)

        video_stream = next((s for s in probe_data.get('streams', []) if s['codec_type'] == 'video'), None)
        if not video_stream: 
            result['error'] = "No Video Stream"
            return _finalize_result(result)

        width = int(video_stream.get('width', 0))
        height = int(video_stream.get('height', 0))
        result['width'] = width; result['height'] = height
        if width >= 7680 or height >= 4320: result['resolution'] = "8K"
        elif width >= 3800 or height >= 2100: result['resolution'] = "4K"
        elif width >= 1900 or height >= 1000: result['resolution'] = "1080p"
        elif width >= 1200 or height >= 700: result['resolution'] = "720p"
        else: result['resolution'] = "SD"

        dar = video_stream.get('display_aspect_ratio')
        if isinstance(dar, str) and ':' in dar:
            try:
                num, den = dar.split(':', 1)
                num_val = float(num)
                den_val = float(den)
                if den_val:
                    result['aspect_ratio'] = f"{(num_val / den_val):.2f}".rstrip('0').rstrip('.')
            except (ValueError, ZeroDivisionError):
                pass
        if not result.get('aspect_ratio') and width and height:
            result['aspect_ratio'] = f"{(width / height):.2f}".rstrip('0').rstrip('.')

        fps_raw = video_stream.get('avg_frame_rate') or video_stream.get('r_frame_rate')
        if fps_raw and isinstance(fps_raw, str) and '/' in fps_raw:
            try:
                num, den = fps_raw.split('/', 1)
                num_val = float(num)
                den_val = float(den)
                if den_val:
                    result['fps'] = round(num_val / den_val, 3)
            except (ValueError, ZeroDivisionError):
                pass
        
        bit_raw = video_stream.get('bit_rate') or probe_data.get('format', {}).get('bit_rate')
        if bit_raw: result['bitrate'] = round(int(bit_raw) / 1_000_000, 2)

        # Extract metadata from filename
        filename_base = os.path.basename(path)
        filename_lower = filename_base.lower()
        is_source_hybrid = bool(re.search(r'\bhybrid\b', filename_lower))
        if not is_source_hybrid:
            parent_dir = os.path.basename(os.path.dirname(path))
            if parent_dir and re.search(r'\bhybrid\b', parent_dir.lower()):
                is_source_hybrid = True
        result['is_source_hybrid'] = 1 if is_source_hybrid else 0

        filename_meta = parse_filename_metadata(filename_base)
        result['video_source'] = filename_meta['video_source']
        result['source_format'] = filename_meta['source_format']
        result['edition'] = filename_meta['edition']
        result['year'] = filename_meta['year']
        result['is_3d'] = filename_meta['is_3d']

        media_type_guess, season_guess, episode_guess = parse_tv_from_filename(filename_lower)
        if media_type_guess:
            result['media_type'] = media_type_guess
            result['season'] = season_guess
            result['episode'] = episode_guess

        nfo_candidates = find_kodi_nfo_candidates(path, result['media_type'])
        result['nfo_missing'] = 0 if nfo_candidates else 1
        if nfo_candidates:
            for nfo_path in nfo_candidates:
                nfo_data = parse_kodi_nfo(nfo_path)
                if not nfo_data:
                    continue
                if not result['year'] and nfo_data.get('year'):
                    result['year'] = nfo_data['year']
                if not result['media_type'] and nfo_data.get('media_type'):
                    result['media_type'] = nfo_data['media_type']
                if not result['show_title'] and nfo_data.get('show_title'):
                    result['show_title'] = nfo_data['show_title']
                if result['season'] is None and nfo_data.get('season') is not None:
                    result['season'] = nfo_data['season']
                if result['episode'] is None and nfo_data.get('episode') is not None:
                    result['episode'] = nfo_data['episode']
                if not result['show_title'] and nfo_data.get('show_title'):
                    result['show_title'] = nfo_data['show_title']
                if not result['episode_title'] and nfo_data.get('episode_title'):
                    result['episode_title'] = nfo_data['episode_title']
                if not result.get('imdb_id') and nfo_data.get('imdb_id'):
                    result['imdb_id'] = nfo_data['imdb_id']
                if not result.get('tvdb_id') and nfo_data.get('tvdb_id'):
                    result['tvdb_id'] = nfo_data['tvdb_id']
                if not result.get('tmdb_id') and nfo_data.get('tmdb_id'):
                    result['tmdb_id'] = nfo_data['tmdb_id']
                if not result.get('rotten_id') and nfo_data.get('rotten_id'):
                    result['rotten_id'] = nfo_data['rotten_id']
                if not result.get('metacritic_id') and nfo_data.get('metacritic_id'):
                    result['metacritic_id'] = nfo_data['metacritic_id']
                if not result.get('trakt_id') and nfo_data.get('trakt_id'):
                    result['trakt_id'] = nfo_data['trakt_id']
                if result.get('imdb_rating') is None and nfo_data.get('imdb_rating') is not None:
                    result['imdb_rating'] = nfo_data['imdb_rating']
                if result.get('tvdb_rating') is None and nfo_data.get('tvdb_rating') is not None:
                    result['tvdb_rating'] = nfo_data['tvdb_rating']
                if result.get('tmdb_rating') is None and nfo_data.get('tmdb_rating') is not None:
                    result['tmdb_rating'] = nfo_data['tmdb_rating']
                if result.get('rotten_rating') is None and nfo_data.get('rotten_rating') is not None:
                    result['rotten_rating'] = nfo_data['rotten_rating']
                if result.get('metacritic_rating') is None and nfo_data.get('metacritic_rating') is not None:
                    result['metacritic_rating'] = nfo_data['metacritic_rating']
                if result.get('trakt_rating') is None and nfo_data.get('trakt_rating') is not None:
                    result['trakt_rating'] = nfo_data['trakt_rating']
                if not result['movie_title'] and nfo_data.get('title') and (nfo_data.get('media_type') == 'movie' or result['media_type'] != 'tv'):
                    result['movie_title'] = nfo_data['title']

        coerce_tv_nfo_to_movie(result, filename_base, media_type_guess, path)

        if result['media_type'] == 'tv':
            if not result['show_title']:
                result['show_title'] = guess_show_title_from_path(path)

        if not result['movie_title'] and result['media_type'] != 'tv':
            movie_title_guess = guess_movie_title_from_filename(filename_base)
            if movie_title_guess:
                result['movie_title'] = movie_title_guess
                if not result['media_type']:
                    result['media_type'] = 'movie'
        
        is_remux = result['source_format'] == 'Remux' or 'remux' in filename_lower
        if is_remux and not result['video_source']:
            is_uhd_remux = bool(re.search(r'\b(uhd|ultra[-\s]?hd|2160p|uhd[-\s]?blu[-\s]?ray)\b', filename_lower)) or result['resolution'] == "4K"
            is_1080p_remux = bool(re.search(r'\b1080p\b', filename_lower))
            if is_uhd_remux:
                result['video_source'] = "UHD Bluray"
            elif is_1080p_remux:
                result['video_source'] = "Bluray"
        
        # Extract video codec
        result['video_codec'] = extract_video_codec(filename_base, probe_data)

        color_transfer = video_stream.get('color_transfer', 'unknown')
        side_data = video_stream.get('side_data_list', [])

        # Check for HDR10+ in side_data (multiple possible names)
        hdr10plus_detected = False
        for sd in side_data:
            sd_type = sd.get('side_data_type', '')
            if 'HDR Dynamic Metadata' in sd_type or 'HDR10+' in sd_type or 'HDR10Plus' in sd_type:
                hdr10plus_detected = True
                break
        if hdr10plus_detected:
            sec_hdrs.append("HDR10+")
        
        if "arib-std-b67" in color_transfer: is_hlg_base = True; sec_hdrs.append("HLG")
        elif "smpte2084" in color_transfer: sec_hdrs.append("HDR10")

        # Extract Dolby Vision compatibility ID from ffprobe side_data
        bl_compatibility_id = None
        dv_header = next((x for x in side_data if 'DOVI configuration record' in x.get('side_data_type', '')), None)
        if dv_header:
            # Try multiple possible field names for compatibility ID
            bl_compatibility_id = (dv_header.get('compatibility_id') or 
                                  dv_header.get('dv_bl_signal_compatibility_id') or
                                  dv_header.get('bl_compatibility_id') or
                                  dv_header.get('compatibility'))
            if bl_compatibility_id is not None:
                bl_compatibility_id = str(bl_compatibility_id)
        
        # Check for enhancement layer streams (for FEL/MEL detection)
        for stream in probe_data.get('streams', []):
            if stream.get('codec_type') == 'video':
                codec_name = stream.get('codec_name', '').lower()
                # Check for enhancement layer indicators
                if 'enhancement' in codec_name or stream.get('tags', {}).get('enhancement', ''):
                    enhancement_layer_found = True
                    break

        # 2. DOVI_TOOL (with caching)
        # Check cache first - use file path + size + modified time as key
        file_size = 0
        file_mtime = 0
        try:
            stat_info = os.stat(path)
            file_size = stat_info.st_size
            file_mtime = stat_info.st_mtime
        except OSError:
            pass
        
        cache_key = (path, file_size, file_mtime)
        dovi_data = None
        rpu_size = 0
        
        # Check cache (LRU - move to end if found)
        with rpu_cache_lock:
            if cache_key in RPU_CACHE:
                # Move to end (most recently used) for LRU behavior
                cached = RPU_CACHE.pop(cache_key)
                RPU_CACHE[cache_key] = cached
                dovi_data = cached.get('dovi_data')
                rpu_size = cached.get('rpu_size', 0)
                if DEBUG_MODE: log_debug(f"Using cached RPU data for {path}", "DEBUG")
        
        # If not in cache, extract RPU
        if not dovi_data:
            # Use tempfile for safe temporary file creation
            rpu_fd, rpu_file = tempfile.mkstemp(suffix='_rpu.bin', prefix='dovi_')
            try:
                os.close(rpu_fd)  # Close file descriptor, we only need the path
                if ABORT_SCAN: raise RuntimeError("Scan Aborted")
                # Match old version exactly: use string path with text=True
                # Path is already normalized via os.fsencode/fsdecode in scan_file_worker
                ffmpeg_cmd = ['ffmpeg', '-i', path, '-c:v', 'copy', '-to', '2', '-f', 'hevc', '-y', '-']
                dovi_extract = ['dovi_tool', 'extract-rpu', '-', '-o', rpu_file]
                
                p1 = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, start_new_session=True)
                with proc_lock: ACTIVE_PROCS.add(p1)
                
                p2 = subprocess.Popen(dovi_extract, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, start_new_session=True)
                with proc_lock: ACTIVE_PROCS.add(p2)
                
                p1.stdout.close()
                # Match old version exactly - no timeout on communicate, just let it run
                # The subprocess timeouts in run_command() handle other commands
                # Check for abort before blocking
                if ABORT_SCAN: raise RuntimeError("Scan Aborted")
                p2.communicate()
                
                with proc_lock: 
                    ACTIVE_PROCS.discard(p1)
                    ACTIVE_PROCS.discard(p2)

                if p2.returncode == 0 and os.path.exists(rpu_file):
                    rpu_size = os.path.getsize(rpu_file)
                    if rpu_size > 0:
                        rc_info, out_info, _ = run_command(['dovi_tool', 'info', '-i', rpu_file, '-f', '0'])
                        json_start = out_info.find('{')
                        if json_start != -1:
                            dovi_data = json.loads(out_info[json_start:])
                            
                            # Cache the result (LRU - add to end, remove from front if needed)
                            with rpu_cache_lock:
                                # Limit cache size using LRU eviction
                                if len(RPU_CACHE) >= RPU_CACHE_MAX_SIZE:
                                    # Remove oldest entry (least recently used - first item)
                                    oldest_key = next(iter(RPU_CACHE))
                                    del RPU_CACHE[oldest_key]
                                    if DEBUG_MODE: log_debug(f"RPU cache full, evicted oldest entry. Cache size: {len(RPU_CACHE)}", "DEBUG")
                                # Add new entry at end (most recently used)
                                RPU_CACHE[cache_key] = {'dovi_data': dovi_data, 'rpu_size': rpu_size}
                                if DEBUG_MODE: log_debug(f"Cached RPU data for {path} (cache size: {len(RPU_CACHE)})", "DEBUG")
            except RuntimeError as e:
                # Don't catch RuntimeError (abort) - let it propagate
                # Also cleanup processes if abort happens
                with proc_lock:
                    try:
                        if 'p1' in locals(): ACTIVE_PROCS.discard(p1)
                        if 'p2' in locals(): ACTIVE_PROCS.discard(p2)
                    except:
                        pass
                raise
            except (OSError, subprocess.SubprocessError) as e:
                if DEBUG_MODE: log_debug(f"RPU extraction error for {path}: {e}", "ERROR")
            finally:
                # Always cleanup RPU file, even if there was an error
                if 'rpu_file' in locals() and os.path.exists(rpu_file):
                    try:
                        os.remove(rpu_file)
                    except OSError as e:
                        if DEBUG_MODE: log_debug(f"Failed to remove RPU file {rpu_file}: {e}", "WARNING")
        
        # Store dovi_data for processing after all tests are complete
        dovi_profile_raw = None
        dovi_el_type_raw = None
        if dovi_data:
            dovi_profile_raw = str(dovi_data.get('dovi_profile'))
            # Extract EL type (FEL or MEL) - check multiple possible field names and structures
            el_type = None
            # Try various possible field names
            for key in ['el_type', 'enhancement_layer_type', 'el', 'enhancement_layer', 'layer_type']:
                if key in dovi_data:
                    el_type = dovi_data[key]
                    break
            
            # Also check nested structures
            if not el_type and 'rpu' in dovi_data:
                rpu_info = dovi_data.get('rpu', {})
                for key in ['el_type', 'enhancement_layer_type', 'el', 'enhancement_layer']:
                    if key in rpu_info:
                        el_type = rpu_info[key]
                        break
            
            if el_type:
                el_str = str(el_type).upper()
                if 'FEL' in el_str or el_str == 'F' or 'FULL' in el_str:
                    dovi_el_type_raw = 'FEL'
                elif 'MEL' in el_str or el_str == 'M' or 'MINIMAL' in el_str:
                    dovi_el_type_raw = 'MEL'
                else:
                    dovi_el_type_raw = el_str
            # For P7, if no explicit el_type but profile is 7, try to infer from RPU characteristics
            elif dovi_profile_raw == "7":
                # Check RPU file size - FEL typically has larger RPU files due to full enhancement data
                # Also check if enhancement layer was found in video streams
                if enhancement_layer_found:
                    # If enhancement layer stream exists, it's likely FEL
                    dovi_el_type_raw = 'FEL'
                else:
                    # Get RPU threshold from settings, default to 50000 bytes
                    rpu_threshold = 50000
                    try:
                        with get_db() as conn:
                            threshold_setting = conn.execute("SELECT value FROM settings WHERE key='rpu_fel_threshold'").fetchone()
                            if threshold_setting:
                                rpu_threshold = int(threshold_setting[0])
                    except (ValueError, TypeError, sqlite3.Error):
                        pass
                    
                    if rpu_size > rpu_threshold:
                        # Larger RPU might indicate FEL (heuristic)
                        dovi_el_type_raw = 'FEL'
                    else:
                        # Otherwise, for P7 without clear indicators, default to MEL
                        # (MEL is more common for P7)
                        dovi_el_type_raw = 'MEL'
        
        # Note: Format determination happens AFTER MediaInfo to ensure all sources are checked
        # This will be done at the end after MediaInfo has a chance to add to sec_hdrs

        # 3. MEDIAINFO (Raw CLI Parsing)
        # Note: HAS_MEDIAINFO check removed because we now use CLI which is always installed in Docker
        # MediaInfo can hang on certain files, so we make it optional - skip if it times out or errors
        rc_mi = -1
        out_mi = ""
        try:
            if DEBUG_MODE: log_debug(f"[MEDIAINFO] Starting mediainfo for: {path}", "DEBUG")
            try:
                rc_mi, out_mi, _ = run_command(['mediainfo', '--Output=JSON', path])
                if DEBUG_MODE: log_debug(f"[MEDIAINFO] Completed with return code: {rc_mi}", "DEBUG")
            except (RuntimeError, Exception) as e:
                # Catch ALL exceptions from MediaInfo (timeout, errors, etc.) and continue without it
                if DEBUG_MODE: 
                    error_msg = str(e).lower()
                    if "timed out" in error_msg or "timeout" in error_msg:
                        log_debug(f"[MEDIAINFO] Timed out for: {path}, skipping MediaInfo and continuing", "WARNING")
                    else:
                        log_debug(f"[MEDIAINFO] Error for {path}: {e}, skipping MediaInfo and continuing", "WARNING")
                # Continue without MediaInfo data - file will still be processed with ffprobe data
                rc_mi = -1
                out_mi = ""
            
            if rc_mi == 0 and out_mi:
                try:
                    mi_data = json.loads(out_mi)
                except json.JSONDecodeError as e:
                    if DEBUG_MODE: log_debug(f"[MEDIAINFO] JSON decode error for {path}: {e}, skipping MediaInfo data", "WARNING")
                    mi_data = None
                
                if mi_data:
                    tracks = mi_data.get('media', {}).get('track', [])
                    for t in tracks:
                        ttype = t.get('@type')
                        if ttype == 'Audio':
                            codec = t.get('Format_Commercial_IfAny') or t.get('Format')
                            if codec: result['audio_codecs'].append(codec)
                            lang = t.get('Language')
                            if lang: result['audio_langs'].append(lang)
                            channels = t.get('Channels') or t.get('Channel(s)') or t.get('Channel_s_')
                            if channels is not None:
                                channel_text = str(channels)
                                match = re.search(r'\d+(?:\.\d+)?', channel_text)
                                result['audio_channels'].append(match.group(0) if match else channel_text)
                        elif ttype == 'Text':
                            lang = t.get('Language')
                            if lang: result['subtitles'].append(lang)
                        elif ttype == 'Video':
                            if t.get('MaxCLL'): result['max_cll'] = t['MaxCLL'].replace(' cd/m2', '').strip()
                            if t.get('MaxFALL'): result['max_fall'] = t['MaxFALL'].replace(' cd/m2', '').strip()
                            # Check MediaInfo for HDR10+ indicators (especially important for DV hybrids)
                            hdr_format = t.get('HDR_Format', '')
                            hdr_compat = t.get('HDR_Format_Compatibility', '')
                            
                            # Check HDR_Format_Compatibility first (most reliable for hybrids)
                            if hdr_compat:
                                compat_str = str(hdr_compat).upper()
                                if 'HDR10+' in compat_str or 'HDR10PLUS' in compat_str:
                                    if "HDR10+" not in sec_hdrs:
                                        sec_hdrs.append("HDR10+")
                                        if DEBUG_MODE:
                                            log_debug(f"HDR10+ detected from MediaInfo HDR_Format_Compatibility: {hdr_compat}", "DEBUG")
                            
                            # Check HDR_Format for SMPTE ST 2094 (HDR10+)
                            if hdr_format:
                                hdr_str = str(hdr_format).upper()
                                if 'SMPTE ST 2094' in hdr_str or 'SMPTE2094' in hdr_str or '2094' in hdr_str:
                                    if "HDR10+" not in sec_hdrs:
                                        sec_hdrs.append("HDR10+")
                                        if DEBUG_MODE:
                                            log_debug(f"HDR10+ detected from MediaInfo HDR_Format: {hdr_format}", "DEBUG")
                            
                            # Also check for HDR10+ in transfer characteristics
                            transfer = t.get('transfer_characteristics') or t.get('Transfer_Characteristics')
                            if transfer and ('2094' in str(transfer) or 'HDR10+' in str(transfer).upper()):
                                if "HDR10+" not in sec_hdrs:
                                    sec_hdrs.append("HDR10+")
                                    if DEBUG_MODE:
                                        log_debug(f"HDR10+ detected from MediaInfo transfer_characteristics: {transfer}", "DEBUG")
        except Exception as e:
            # Outer catch for any unexpected errors - continue without MediaInfo
            if DEBUG_MODE: 
                log_debug(f"[MEDIAINFO] Outer exception for {path}: {e}, continuing without MediaInfo data", "WARNING")

        # NOW DETERMINE FORMATS AFTER ALL TESTS (ffprobe, dovi_tool, mediainfo) ARE COMPLETE
        # Priority: DV > HDR10+ > HDR10 > HLG > SDR
        
        # Step 1: Determine main format (highest level detected)
        if dovi_profile_raw:
            # DV detected - set as main format
            result['format'] = 'dovi'
            result['bl_compatibility_id'] = bl_compatibility_id if bl_compatibility_id is not None else str(dovi_data.get('bl_compatibility_id', 'None') if dovi_data else 'None')
            if result['bl_compatibility_id'] == 'None': result['bl_compatibility_id'] = None
            
            # Determine DV profile
            bl_id = result['bl_compatibility_id']
            if dovi_profile_raw == "8":
                # P8.4: compatibility_id = 4 OR HLG base layer (even if compatibility_id is None)
                if is_hlg_base:
                    result['dovi_profile'] = "8.4"
                    if DEBUG_MODE:
                        log_debug(f"Detected P8.4 (HLG base layer, bl_id={bl_id})", "DEBUG")
                elif bl_id == "4":
                    result['dovi_profile'] = "8.4"
                    if DEBUG_MODE:
                        log_debug(f"Detected P8.4 (bl_id=4)", "DEBUG")
                elif bl_id == "1": 
                    result['dovi_profile'] = "8.1"
                else: 
                    # Check sec_hdrs for highest level (HDR10+ > HDR10)
                    if "HDR10+" in sec_hdrs:
                        result['dovi_profile'] = "8.1"  # P8.1 with HDR10+ base
                    elif "HDR10" in sec_hdrs:
                        result['dovi_profile'] = "8.1"
                    else:
                        result['dovi_profile'] = "8"
            else:
                result['dovi_profile'] = dovi_profile_raw
            
            # Set EL type
            result['dovi_el_type'] = dovi_el_type_raw
            
            if DEBUG_MODE:
                log_debug(f"DV Profile {result['dovi_profile']}, BL_ID: {bl_id}, HLG base: {is_hlg_base}, Sec HDRs: {sec_hdrs}", "DEBUG")
        elif is_hlg_base:
            result['format'] = 'hlg'
        elif "HDR10+" in sec_hdrs:
            # HDR10+ is the main format, HDR10 is the base layer (should be in secondary_hdr)
            result['format'] = 'hdr10plus'
        elif "HDR10" in sec_hdrs:
            result['format'] = 'hdr10'
        # else remains 'sdr_only'
        
        # Step 2: Determine secondary HDR (highest level from sec_hdrs, excluding main format)
        if result['format'] == 'dovi' and sec_hdrs:
            # For DV, all sec_hdrs are secondary (hybrid) - pick highest level
            # Priority: HDR10+ > HDR10 > HLG
            if "HDR10+" in sec_hdrs:
                result['hdr_format_secondary'] = "HDR10+"
            elif "HDR10" in sec_hdrs:
                result['hdr_format_secondary'] = "HDR10"
            elif "HLG" in sec_hdrs:
                result['hdr_format_secondary'] = "HLG"
            else:
                result['hdr_format_secondary'] = "+".join(sec_hdrs)
            result['is_hybrid'] = 1
        elif sec_hdrs:
            # For non-DV, filter out the main format from sec_hdrs
            # Normalize both to same format for comparison
            # Format values: 'hdr10plus', 'hdr10', 'hlg' -> normalize to match sec_hdrs: "HDR10+", "HDR10", "HLG"
            format_to_sec = {
                'hdr10plus': 'HDR10+',
                'hdr10': 'HDR10',
                'hlg': 'HLG',
                'sdr_only': None  # SDR has no secondary
            }
            main_format_sec = format_to_sec.get(result['format'])
            
            clean_sec = []
            for h in sec_hdrs:
                # Don't include if it matches the main format
                if h != main_format_sec:
                    clean_sec.append(h)
            if clean_sec:
                # Pick highest level from remaining
                if "HDR10+" in clean_sec:
                    result['hdr_format_secondary'] = "HDR10+"
                elif "HDR10" in clean_sec:
                    result['hdr_format_secondary'] = "HDR10"
                elif "HLG" in clean_sec:
                    result['hdr_format_secondary'] = "HLG"
                else:
                    result['hdr_format_secondary'] = "+".join(clean_sec)
                result['is_hybrid'] = 1

    except RuntimeError as e:
        if "Scan Aborted" in str(e):
            result['error'] = "Scan aborted by user"
            if DEBUG_MODE: log_debug(f"Scan aborted during analysis of {path}", "WARNING")
        else:
            result['error'] = f"Runtime error: {str(e)}"
            if DEBUG_MODE: log_debug(f"Runtime error analyzing {path}: {e}", "ERROR")
    except Exception as e:
        result['error'] = f"Unexpected error: {str(e)}"
        if DEBUG_MODE: log_debug(f"Error analyzing {path}: {e}", "ERROR")
        import traceback
        if DEBUG_MODE: log_debug(f"Traceback: {traceback.format_exc()}", "DEBUG")
    
    return _finalize_result(result)

def _create_error_result(error_msg: str) -> dict:
    """
    Create a standardized error result dictionary.
    
    Args:
        error_msg: Error message to include in the result
        
    Returns:
        Dictionary with error result structure
    """
    return {
        'format': 'sdr_only', 'dovi_profile': None, 'dovi_el_type': None, 
        'bl_compatibility_id': None, 'hdr_format_secondary': None, 
        'resolution': None, 'width': 0, 'height': 0, 'bitrate': 0, 
        'is_hybrid': 0, 'is_source_hybrid': 0, 'error': error_msg,
        'audio_codecs': [], 'audio_langs': [], 'audio_channels': [], 'subtitles': [], 
        'max_cll': None, 'max_fall': None,
        'fps': None, 'aspect_ratio': None,
        'imdb_id': None, 'tvdb_id': None, 'tmdb_id': None, 'rotten_id': None, 'metacritic_id': None, 'trakt_id': None,
        'imdb_rating': None, 'tvdb_rating': None, 'tmdb_rating': None, 'rotten_rating': None, 'metacritic_rating': None, 'trakt_rating': None,
        'video_source': None, 'source_format': None, 'video_codec': None,
        'is_3d': 0, 'edition': None, 'year': None,
        'media_type': None, 'show_title': None, 'season': None, 'episode': None, 'movie_title': None, 'episode_title': None,
        'nfo_missing': 1
    }

def _finalize_result(res: dict) -> dict:
    """
    Finalize analysis result by converting lists to strings for database storage.
    
    Args:
        res: Result dictionary from analyze_file_deep
        
    Returns:
        Dictionary with lists converted to comma-separated strings
    """
    def to_str(val, preserve_order: bool = False):
        if isinstance(val, list):
            cleaned = [str(v) for v in val if v not in (None, '')]
            if preserve_order:
                return ", ".join(cleaned)
            return ", ".join(sorted(list(set(cleaned))))
        return str(val) if val is not None else None

    res['audio_codecs'] = to_str(res['audio_codecs'], preserve_order=True)
    res['audio_langs'] = to_str(res['audio_langs'], preserve_order=True)
    res['audio_channels'] = to_str(res['audio_channels'], preserve_order=True)
    res['subtitles'] = to_str(res['subtitles'])
    return res

# --- WORKER ---
def scan_file_worker(path_obj: pathlib.Path) -> dict:
    """
    Worker function to scan a single video file.
    
    Args:
        path_obj: Path object pointing to the video file
        
    Returns:
        Dictionary containing all file metadata ready for database insertion
    """
    # Use os.fsencode/fsdecode for proper filesystem encoding handling
    try:
        full_path_str = os.fsdecode(os.fsencode(str(path_obj)))
        filename = os.fsdecode(os.fsencode(path_obj.name))
    except (UnicodeEncodeError, UnicodeDecodeError, OSError) as e:
        # Fallback if encoding fails - use path as-is
        if DEBUG_MODE:
            log_debug(f"Path encoding failed for {path_obj}, using fallback: {e}", "WARNING")
        full_path_str = str(path_obj)
    filename = path_obj.name
    
    # Early validation - check if file is accessible before attempting analysis
    try:
        if not os.path.exists(full_path_str):
            if DEBUG_MODE:
                log_debug(f"File does not exist: {full_path_str}", "ERROR")
            return {
                "filename": filename, "category": 'sdr_only', "profile": None,
                "el_type": None, "container": path_obj.suffix.lower().replace('.', ''), 
                "source_vol": path_obj.parts[1] if len(path_obj.parts) > 1 else "Unknown",
                "full_path": full_path_str, "last_scanned": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "resolution": None, "bitrate_mbps": 0,
                "scan_error": "File not found", "is_hybrid": 0, "is_source_hybrid": 0,
                "secondary_hdr": None, "width": 0,
                "height": 0, "file_size": 0,
                "bl_compatibility_id": None,
                "audio_codecs": [], "audio_langs": [], "audio_channels": [], "subtitles": [], "max_cll": None, "max_fall": None,
                "fps": None, "aspect_ratio": None,
                "imdb_id": None, "tvdb_id": None, "tmdb_id": None, "rotten_id": None, "metacritic_id": None, "trakt_id": None,
                "imdb_rating": None, "tvdb_rating": None, "tmdb_rating": None, "rotten_rating": None, "metacritic_rating": None, "trakt_rating": None,
                "scan_attempts": 0,
                "video_source": None, "source_format": None, "video_codec": None,
                "is_3d": 0, "edition": None, "year": None, "media_type": None,
                "show_title": None, "season": None, "episode": None, "movie_title": None, "episode_title": None,
                "validation_flag": None
            }
        if not os.access(full_path_str, os.R_OK):
            if DEBUG_MODE:
                log_debug(f"File not accessible: {full_path_str}", "ERROR")
            return {
                "filename": filename, "category": 'sdr_only', "profile": None,
                "el_type": None, "container": path_obj.suffix.lower().replace('.', ''), 
                "source_vol": path_obj.parts[1] if len(path_obj.parts) > 1 else "Unknown",
                "full_path": full_path_str, "last_scanned": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "resolution": None, "bitrate_mbps": 0,
                "scan_error": "File not accessible (permission denied)", "is_hybrid": 0, "is_source_hybrid": 0,
                "secondary_hdr": None, "width": 0,
                "height": 0, "file_size": 0,
                "bl_compatibility_id": None,
                "audio_codecs": [], "audio_langs": [], "audio_channels": [], "subtitles": [], "max_cll": None, "max_fall": None,
                "fps": None, "aspect_ratio": None,
                "imdb_id": None, "tvdb_id": None, "tmdb_id": None, "rotten_id": None, "metacritic_id": None, "trakt_id": None,
                "imdb_rating": None, "tvdb_rating": None, "tmdb_rating": None, "rotten_rating": None, "metacritic_rating": None, "trakt_rating": None,
                "scan_attempts": 0,
                "video_source": None, "source_format": None, "video_codec": None,
                "is_3d": 0, "edition": None, "year": None, "media_type": None,
                "show_title": None, "season": None, "episode": None, "movie_title": None, "episode_title": None
            }
    except (OSError, UnicodeEncodeError, UnicodeDecodeError) as e:
        if DEBUG_MODE:
            log_debug(f"File validation error for {full_path_str}: {e}", "ERROR")
        return {
            "filename": filename, "category": 'sdr_only', "profile": None,
            "el_type": None, "container": path_obj.suffix.lower().replace('.', ''), 
            "source_vol": path_obj.parts[1] if len(path_obj.parts) > 1 else "Unknown",
            "full_path": full_path_str, "last_scanned": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "resolution": None, "bitrate_mbps": 0,
            "scan_error": f"File validation error: {str(e)}", "is_hybrid": 0, "is_source_hybrid": 0,
            "secondary_hdr": None, "width": 0,
            "height": 0, "file_size": 0,
            "bl_compatibility_id": None,
            "audio_codecs": [], "audio_langs": [], "audio_channels": [], "subtitles": [], "max_cll": None, "max_fall": None,
            "fps": None, "aspect_ratio": None,
            "imdb_id": None, "tvdb_id": None, "tmdb_id": None, "rotten_id": None, "metacritic_id": None, "trakt_id": None,
            "imdb_rating": None, "tvdb_rating": None, "tmdb_rating": None, "rotten_rating": None, "metacritic_rating": None, "trakt_rating": None,
            "scan_attempts": 0,
            "video_source": None, "source_format": None, "video_codec": None,
            "is_3d": 0, "edition": None, "year": None, "media_type": None,
            "show_title": None, "season": None, "episode": None, "movie_title": None, "episode_title": None,
            "validation_flag": None
        }
    
    container = path_obj.suffix.lower().replace('.', '')
    source_vol = path_obj.parts[1] if len(path_obj.parts) > 1 else "Unknown"
    
    with progress_lock: PROGRESS["file"] = f"Analyzing: {filename}"
    if DEBUG_MODE: log_debug(f"Processing: {full_path_str}", "DEBUG")
    
    # Retry logic with exponential backoff for transient failures
    max_retries = MAX_RETRIES
    retry_delay = RETRY_DELAY_INITIAL
    meta = None
    for attempt in range(max_retries + 1):
        try:
            # Check for abort before starting analysis
            if ABORT_SCAN:
                log_debug(f"[ABORT] Abort detected before analyzing {filename}, skipping", "INFO")
                meta = _create_error_result("Scan aborted by user")
                break
            wait_if_paused()
            
            # Call analyze_file_deep directly (subprocess timeouts are handled within)
            meta = analyze_file_deep(full_path_str)
            break  # Success, exit retry loop
        except RuntimeError as e:
            if "Scan Aborted" in str(e):
                log_debug(f"[ABORT] Abort detected during analysis of {filename}", "INFO")
                meta = _create_error_result("Scan aborted by user")
                break  # Don't retry on abort
            if attempt < max_retries:
                if DEBUG_MODE: log_debug(f"Retry {attempt + 1}/{max_retries} for {full_path_str} after {retry_delay}s", "WARNING")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                if DEBUG_MODE: log_debug(f"Max retries reached for {full_path_str}", "ERROR")
                meta = _create_error_result(f"Failed after {max_retries} retries: {str(e)}")
        except Exception as e:
            if attempt < max_retries:
                if DEBUG_MODE: log_debug(f"Retry {attempt + 1}/{max_retries} for {full_path_str} after {retry_delay}s: {e}", "WARNING")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                if DEBUG_MODE: log_debug(f"Max retries reached for {full_path_str}: {e}", "ERROR")
                meta = _create_error_result(f"Failed after {max_retries} retries: {str(e)}")
    
    if meta is None:
        meta = _create_error_result("Analysis failed")
    file_size = 0
    try:
        file_size = os.path.getsize(full_path_str)
    except OSError as e:
        if DEBUG_MODE: log_debug(f"Failed to get file size for {full_path_str}: {e}", "WARNING")

    # Note: scan_attempts will be calculated in run_scan based on previous attempts
    validation_flag = compute_validation_flag({
        "media_type": meta.get('media_type'),
        "show_title": meta.get('show_title'),
        "episode_title": meta.get('episode_title'),
        "movie_title": meta.get('movie_title'),
        "season": meta.get('season'),
        "episode": meta.get('episode')
    })
    return {
        "filename": filename, "category": meta['format'], "profile": meta['dovi_profile'],
        "el_type": meta['dovi_el_type'], "container": container, "source_vol": source_vol,
        "full_path": full_path_str, "last_scanned": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "resolution": meta['resolution'], "bitrate_mbps": meta['bitrate'],
        "scan_error": meta['error'], "is_hybrid": meta['is_hybrid'], "is_source_hybrid": meta.get('is_source_hybrid', 0),
        "secondary_hdr": meta['hdr_format_secondary'], "width": meta['width'],
        "height": meta['height'], "file_size": file_size,
        "bl_compatibility_id": meta['bl_compatibility_id'],
        "audio_codecs": meta['audio_codecs'], "audio_langs": meta['audio_langs'], "audio_channels": meta['audio_channels'],
        "subtitles": meta['subtitles'], "max_cll": meta['max_cll'], "max_fall": meta['max_fall'],
        "fps": meta.get('fps'), "aspect_ratio": meta.get('aspect_ratio'),
        "imdb_id": meta.get('imdb_id'), "tvdb_id": meta.get('tvdb_id'), "tmdb_id": meta.get('tmdb_id'),
        "rotten_id": meta.get('rotten_id'), "metacritic_id": meta.get('metacritic_id'), "trakt_id": meta.get('trakt_id'),
        "imdb_rating": meta.get('imdb_rating'), "tvdb_rating": meta.get('tvdb_rating'), "tmdb_rating": meta.get('tmdb_rating'),
        "rotten_rating": meta.get('rotten_rating'), "metacritic_rating": meta.get('metacritic_rating'), "trakt_rating": meta.get('trakt_rating'),
        "scan_attempts": 0,  # Will be updated in run_scan based on previous attempts
        "video_source": meta['video_source'], "source_format": meta['source_format'], "video_codec": meta['video_codec'],
        "is_3d": meta['is_3d'], "edition": meta['edition'], "year": meta['year'],
        "media_type": meta.get('media_type'), "show_title": meta.get('show_title'),
        "season": meta.get('season'), "episode": meta.get('episode'),
        "movie_title": meta.get('movie_title'), "episode_title": meta.get('episode_title'),
        "validation_flag": validation_flag
    }

def build_backfill_metadata(file_path: str, filename: str, current: dict) -> dict:
    filename_base = filename
    filename_lower = filename_base.lower()
    result: dict[str, Any] = {}

    filename_meta = parse_filename_metadata(filename_base)
    media_type_guess, season_guess, episode_guess = parse_tv_from_filename(filename_lower)
    if media_type_guess and not current.get('media_type'):
        result['media_type'] = media_type_guess
    if season_guess is not None and current.get('season') is None:
        result['season'] = season_guess
    if episode_guess is not None and current.get('episode') is None:
        result['episode'] = episode_guess
    if not current.get('year') and filename_meta.get('year'):
        result['year'] = filename_meta['year']

    nfo_candidates = find_kodi_nfo_candidates(file_path, current.get('media_type') or result.get('media_type'))
    for nfo_path in nfo_candidates:
        nfo_data = parse_kodi_nfo(nfo_path)
        if not nfo_data:
            continue
        if not current.get('year') and nfo_data.get('year'):
            result['year'] = nfo_data['year']
        if not current.get('media_type') and nfo_data.get('media_type'):
            result['media_type'] = nfo_data['media_type']
        if not current.get('show_title') and nfo_data.get('show_title'):
            result['show_title'] = nfo_data['show_title']
        if not current.get('episode_title') and nfo_data.get('episode_title'):
            result['episode_title'] = nfo_data['episode_title']
        if current.get('season') is None and nfo_data.get('season') is not None:
            result['season'] = nfo_data['season']
        if current.get('episode') is None and nfo_data.get('episode') is not None:
            result['episode'] = nfo_data['episode']
        if not current.get('movie_title') and nfo_data.get('title'):
            media_type_val = current.get('media_type') or result.get('media_type')
            if nfo_data.get('media_type') == 'movie' or media_type_val != 'tv':
                result['movie_title'] = nfo_data['title']
        for key in (
            'imdb_id', 'tvdb_id', 'tmdb_id', 'rotten_id', 'metacritic_id', 'trakt_id',
            'imdb_rating', 'tvdb_rating', 'tmdb_rating', 'rotten_rating', 'metacritic_rating', 'trakt_rating'
        ):
            if current.get(key) is None and nfo_data.get(key) is not None:
                result[key] = nfo_data[key]

    if not current.get('media_type'):
        scratch = {
            'media_type': result.get('media_type'),
            'show_title': result.get('show_title'),
            'season': result.get('season'),
            'episode': result.get('episode'),
            'movie_title': result.get('movie_title'),
            'episode_title': result.get('episode_title')
        }
        coerce_tv_nfo_to_movie(scratch, filename_base, media_type_guess, file_path)
        if scratch.get('media_type') == 'movie' and result.get('media_type') != 'movie':
            result['media_type'] = 'movie'
            result['movie_title'] = scratch.get('movie_title') or result.get('movie_title')
            result['show_title'] = None
            result['season'] = None
            result['episode'] = None
            result['episode_title'] = None

    if not current.get('show_title') and (current.get('media_type') or result.get('media_type')) == 'tv':
        result['show_title'] = result.get('show_title') or guess_show_title_from_path(file_path)

    if not current.get('episode_title') and (current.get('media_type') or result.get('media_type')) == 'tv':
        result['episode_title'] = result.get('episode_title')

    if not current.get('movie_title') and not result.get('movie_title') and (current.get('media_type') or result.get('media_type')) != 'tv':
        movie_title_guess = guess_movie_title_from_filename(filename_base)
        if movie_title_guess:
            result['movie_title'] = movie_title_guess
            if not current.get('media_type'):
                result['media_type'] = result.get('media_type') or 'movie'

    return result

def sanitize_string_for_db(value) -> str | None:
    """
    Sanitize a string value for database insertion by handling invalid UTF-8 characters.
    
    Args:
        value: String value to sanitize (can be None, str, or other types)
        
    Returns:
        Sanitized string safe for SQLite insertion, or None if input was None
    """
    """
    Sanitize a string value for database insertion by handling invalid UTF-8 characters.
    
    Args:
        value: String value to sanitize
        
    Returns:
        Sanitized string safe for SQLite insertion
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return str(value)
    # Encode with 'replace' to handle invalid characters, then decode back
    # This replaces invalid surrogates and other problematic characters with replacement characters
    try:
        return value.encode('utf-8', 'replace').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # Fallback: replace all problematic characters
        return value.encode('utf-8', 'replace').decode('utf-8', 'replace')

def sanitize_dict_for_db(data: dict) -> dict:
    """
    Sanitize all string values in a dictionary for database insertion.
    
    Args:
        data: Dictionary containing video metadata
        
    Returns:
        Dictionary with all string values sanitized
    """
    sanitized = {}
    for key, value in data.items():
        if isinstance(value, str):
            sanitized[key] = sanitize_string_for_db(value)
        else:
            sanitized[key] = value
    return sanitized

def save_batch_to_db(data_list: list) -> None:
    """
    Save a batch of video metadata to the database.
    
    Args:
        data_list: List of dictionaries containing video metadata
    """
    if not data_list: return
    
    # Sanitize all string values in the data before insertion
    sanitized_list = [sanitize_dict_for_db(item) for item in data_list]
    
    try:
        with get_db() as conn:
            conn.executemany("""INSERT OR REPLACE INTO videos 
                (filename, category, profile, el_type, container, source_vol, full_path, last_scanned, 
                 resolution, bitrate_mbps, scan_error, is_hybrid, is_source_hybrid, secondary_hdr, width, height, 
                 file_size, bl_compatibility_id, audio_codecs, audio_langs, audio_channels, subtitles, max_cll, max_fall, fps, aspect_ratio,
                 imdb_id, tvdb_id, tmdb_id, rotten_id, metacritic_id, trakt_id,
                 imdb_rating, tvdb_rating, tmdb_rating, rotten_rating, metacritic_rating, trakt_rating,
                 scan_attempts, video_source, source_format, video_codec, is_3d, edition, year, media_type, show_title, season, episode, movie_title, episode_title, nfo_missing, validation_flag) 
                VALUES (:filename, :category, :profile, :el_type, :container, :source_vol, :full_path, :last_scanned, 
                 :resolution, :bitrate_mbps, :scan_error, :is_hybrid, :is_source_hybrid, :secondary_hdr, :width, :height, 
                 :file_size, :bl_compatibility_id, :audio_codecs, :audio_langs, :audio_channels, :subtitles, :max_cll, :max_fall, :fps, :aspect_ratio,
                 :imdb_id, :tvdb_id, :tmdb_id, :rotten_id, :metacritic_id, :trakt_id,
                 :imdb_rating, :tvdb_rating, :tmdb_rating, :rotten_rating, :metacritic_rating, :trakt_rating,
                 :scan_attempts, :video_source, :source_format, :video_codec, :is_3d, :edition, :year, :media_type, :show_title, :season, :episode, :movie_title, :episode_title, :nfo_missing, :validation_flag)""", sanitized_list)
            if DEBUG_MODE:
                for item in sanitized_list:
                    log_debug(f"Saved to DB: {item['filename']} -> {item['category']} {item['profile']} (error: {item.get('scan_error', 'None')})", "DEBUG")
    except sqlite3.Error as e:
        log_debug(f"Database error saving batch: {e}", "ERROR")
        if DEBUG_MODE:
            log_debug(f"Failed batch items: {[item.get('filename', 'unknown') for item in sanitized_list]}", "ERROR")
    except sqlite3.Error as e:
        log_debug(f"Unexpected error saving batch: {e}", "ERROR")
        if DEBUG_MODE:
            log_debug(f"Failed batch items: {[item.get('filename', 'unknown') for item in sanitized_list]}", "ERROR")
            # Log the specific problematic item if possible
            try:
                for idx, item in enumerate(sanitized_list):
                    try:
                        # Try to identify which item is causing the issue
                        test_str = str(item)
                    except (UnicodeEncodeError, UnicodeDecodeError) as item_err:
                        log_debug(f"Item {idx} has encoding issue: {item_err}", "ERROR")
            except (OSError, sqlite3.Error) as cleanup_err:
                # Silently ignore errors during cleanup of problematic items
                if DEBUG_MODE:
                    log_debug(f"Error during cleanup iteration: {cleanup_err}", "DEBUG")

# --- SCAN HELPERS ---
def load_processed_map() -> dict:
    """
    Load processed files map from database for efficient lookups during scanning.
    
    Returns:
        Dictionary mapping file paths to their metadata (size, attempts, error)
    """
    processed_map = {}
    with get_db() as conn:
        total_count = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        log_debug(f"[INIT] Database contains {total_count} records. Loading into memory...", "INFO")
        chunk_size = PROCESSED_MAP_CHUNK_SIZE
        offset = 0
        while True:
            rows = conn.execute("SELECT full_path, file_size, scan_attempts, scan_error FROM videos LIMIT ? OFFSET ?", (chunk_size, offset)).fetchall()
            if not rows:
                break
            processed_map.update({row[0]: {'size': row[1], 'attempts': row[2] or 0, 'error': row[3]} for row in rows})
            offset += chunk_size
            with progress_lock:
                PROGRESS["file"] = f"Loading database: {offset}/{total_count} records..."
            if offset % 10000 == 0:
                log_debug(f"[INIT] Loading database: {offset}/{total_count} records...", "INFO")
        log_debug(f"[INIT] Database loading complete: {offset}/{total_count} records loaded", "INFO")
    return processed_map

def prepare_scan_paths(target_vols: list | None, force_rescan: bool) -> tuple[list, dict]:
    """
    Prepare scan paths and volume mappings based on target volumes.
    
    Args:
        target_vols: List of target volume names, or None for all volumes
        force_rescan: Whether to reset scan attempts for target volumes
    
    Returns:
        Tuple of (scan_paths list, path_to_vol mapping dictionary)
    """
    online_mounts = get_mount_status() 
    scan_paths = []
    path_to_vol = {}
        
    if target_vols and len(target_vols) > 0:
        for vol_name in target_vols:
            if vol_name in online_mounts:
                scan_path = online_mounts[vol_name]
                scan_paths.append(scan_path)
                path_to_vol[scan_path] = vol_name
                if force_rescan:
                    with get_db() as conn:
                        conn.execute("UPDATE videos SET scan_attempts=0 WHERE source_vol=?", (vol_name,))
    else:
        for vol_name, scan_path in sorted(online_mounts.items()):
            scan_paths.append(scan_path)
            path_to_vol[scan_path] = vol_name
    
    return scan_paths, path_to_vol


def build_scan_paths_from_folders(scan_folders: list, target_vols: list | None, force_rescan: bool, scan_mode: str) -> tuple[list, dict]:
    """
    Build scan paths from a list of folder entries with volume + relative path.
    """
    online_mounts = get_mount_status()
    scan_paths = []
    path_to_vol = {}
    vol_names = set()
    selected_vols = set(target_vols) if target_vols else None

    for entry in scan_folders:
        vol_name = (entry.get('volume') or '').strip()
        if not vol_name:
            continue
        if selected_vols and vol_name not in selected_vols:
            continue
        if entry.get('muted'):
            continue
        entry_type = (entry.get('type') or 'auto').strip().lower()
        if scan_mode in ('tv', 'movie') and entry_type not in (scan_mode,):
            continue
        base = online_mounts.get(vol_name)
        if not base:
            continue
        rel_path = (entry.get('path') or '').strip()
        if rel_path:
            candidate = rel_path
            if not os.path.isabs(candidate):
                candidate = os.path.join(base, rel_path.lstrip('/\\'))
        else:
            candidate = base

        base_real = os.path.realpath(base)
        target_real = os.path.realpath(candidate)
        if not target_real.startswith(base_real):
            continue
        if not os.path.isdir(target_real):
            continue
        scan_paths.append(target_real)
        path_to_vol[target_real] = vol_name
        vol_names.add(vol_name)

    if force_rescan and vol_names:
        with get_db() as conn:
            for vol_name in vol_names:
                conn.execute("UPDATE videos SET scan_attempts=0 WHERE source_vol=?", (vol_name,))

    return scan_paths, path_to_vol

def collect_files_to_scan(scan_paths: list, path_to_vol: dict, processed_map: dict, 
                          skip_words: list, min_size: int, force_rescan: bool, start_time: float,
                          scan_extras: bool) -> tuple[list, set]:
    """
    Scan directories and collect files that need to be analyzed.
    
    Args:
        scan_paths: List of paths to scan
        path_to_vol: Mapping of paths to volume names
        processed_map: Dictionary of already processed files
        skip_words: List of words to skip in filenames
        min_size: Minimum file size in bytes
        force_rescan: Whether to force rescan of all files
        start_time: Scan start time for progress updates
    
    Returns:
        Tuple of (files_to_scan list, all_found_files set)
    """
    files_to_scan = []
    all_found_files = set() 
    total_seen = 0
    
    with progress_lock:
        PROGRESS["file"] = "Scanning directories..."
    log_debug("[CRAWL] Starting directory scan...", "INFO")
        
    for path in scan_paths:
        wait_if_paused()
        if ABORT_SCAN:
            log_debug("[ABORT] Abort detected in collect_files_to_scan, stopping directory scan", "INFO")
            break
        current_vol = path_to_vol.get(path, os.path.basename(path) if path else "Unknown")
        if not os.path.exists(path):
            log_debug(f"[CRAWL] Volume path does not exist: {path}", "WARNING")
            continue
        
        with progress_lock:
            PROGRESS["file"] = f"Scanning [{current_vol}]: Starting..."
        log_debug(f"[CRAWL] Starting scan of volume: {current_vol}", "INFO")
        
        try:
            dir_count = 0
            for root, dirs, files in os.walk(path):
                wait_if_paused()
                if ABORT_SCAN:
                    log_debug(f"[ABORT] Abort detected while scanning {root}, stopping directory walk", "INFO")
                    break
                dir_count += 1
                if dir_count <= 10 or dir_count % 100 == 0:
                    log_debug(f"[CRAWL] [{current_vol}] Traversing directory {dir_count}: {root}", "INFO")
                if os.path.isfile(os.path.join(root, '.scanignore')):
                    dirs[:] = []
                    continue
                if not scan_extras:
                    def should_skip_extras(parent_dir: str) -> bool:
                        try:
                            season_dir = re.compile(r'^(season[\s._-]*\d+|s\d{1,2})$', re.IGNORECASE)
                            with os.scandir(parent_dir) as it:
                                for entry in it:
                                    name = entry.name
                                    if entry.is_file():
                                        if name.lower().endswith('.nfo'):
                                            return True
                                        if pathlib.Path(name).suffix.lower() in VIDEO_EXTENSIONS:
                                            return True
                                    elif entry.is_dir():
                                        if season_dir.match(name):
                                            return True
                        except OSError:
                            return False
                        return False
                    dirs[:] = [d for d in dirs if not (d.lower() == 'extras' and should_skip_extras(root))]
                dirs[:] = [d for d in dirs if not d.startswith('.')]
                
                for f in files:
                    wait_if_paused()
                    ext = os.path.splitext(f)[1].lower()
                    if ext not in VIDEO_EXTENSIONS:
                        continue
                    
                    total_seen += 1
                    full_p = os.path.join(root, f)
                    
                    if any(s in f.lower() for s in skip_words):
                        if DEBUG_MODE:
                            log_debug(f"Skipping file (skip_words): {full_p}", "DEBUG")
                        continue
                    
                    try:
                        current_size = os.path.getsize(full_p)
                        if min_size > 0 and current_size < min_size:
                            if DEBUG_MODE:
                                log_debug(f"Skipping file (size < {min_size}): {full_p} ({current_size} bytes)", "DEBUG")
                            continue
                    except (OSError, PermissionError) as e:
                        if DEBUG_MODE:
                            log_debug(f"Error getting size for {full_p}: {e}", "DEBUG")
                        continue
                            
                    fp_str = os.fsdecode(os.fsencode(full_p))
                    all_found_files.add(fp_str)
                    
                    existing = processed_map.get(fp_str)
                    should_scan = False
                    
                    if not existing:
                        should_scan = True
                        if DEBUG_MODE:
                            log_debug(f"New file to scan: {fp_str}", "DEBUG")
                    elif existing['size'] != current_size:
                        should_scan = True
                        if DEBUG_MODE:
                            log_debug(f"File size changed: {fp_str} ({existing['size']} -> {current_size})", "DEBUG")
                    elif force_rescan:
                        should_scan = True
                        if DEBUG_MODE:
                            log_debug(f"Force rescan: {fp_str}", "DEBUG")
                    elif existing['attempts'] > MAX_SCAN_ATTEMPTS:
                        should_scan = False
                        if DEBUG_MODE:
                            log_debug(f"Skipping file (attempts > {MAX_SCAN_ATTEMPTS}): {fp_str}", "DEBUG")
                    elif existing.get('error'):
                        should_scan = True
                        if DEBUG_MODE:
                            log_debug(f"Rescanning file with error: {fp_str}", "DEBUG")
                    
                    if should_scan:
                        files_to_scan.append(pathlib.Path(fp_str))
                        if DEBUG_MODE and len(files_to_scan) % 100 == 0:
                            log_debug(f"Added {len(files_to_scan)} files to scan queue...", "DEBUG")
                    
                    if total_seen == 1 or total_seen % 500 == 0:
                        elapsed = int(time.time() - start_time)
                        with progress_lock:
                            PROGRESS["file"] = f"Scanning [{current_vol}]: Found {total_seen} files ({len(files_to_scan)} new)"
                            PROGRESS["last_duration"] = f"{elapsed}s"
                        if DEBUG_MODE:
                            log_debug(f"[CRAWL] [{current_vol}] Found {total_seen} files ({len(files_to_scan)} new) - {elapsed}s elapsed", "DEBUG")
        except (OSError, PermissionError) as e:
            log_debug(f"Error scanning {path}: {e}", "ERROR")
    
    return files_to_scan, all_found_files

def analyze_files(files_to_scan: list, processed_map: dict, settings: dict, 
                  final_threads: int, start_time: float) -> dict:
    """
    Analyze files using ThreadPoolExecutor and return metrics.
    
    Args:
        files_to_scan: List of file paths to analyze
        processed_map: Dictionary of processed files for attempt tracking
        settings: Dictionary of scan settings
        final_threads: Number of threads to use
        start_time: Scan start time for progress updates
    
    Returns:
        Dictionary containing metrics_sum and metrics_count
    """
    log_debug(f"[ANALYZING] {len(files_to_scan)} files (New/Modified)...", "INFO")
    with progress_lock:
        PROGRESS["total"] = len(files_to_scan)
        
    batch_buffer = []
    metrics_sum = {"bitrate": 0.0, "width": 0, "height": 0, "file_size": 0}
    metrics_count = {"bitrate": 0, "width": 0, "height": 0, "file_size": 0}
    progress_updates = {"current": 0, "failed_count": 0, "new_found": 0}
            
    with ThreadPoolExecutor(max_workers=final_threads) as executor:
        futures = [executor.submit(scan_file_worker, m) for m in files_to_scan]
        
        for f in as_completed(futures):
            wait_if_paused()
            if ABORT_SCAN:
                log_debug("[ABORT] Abort detected in analyze_files loop, stopping file processing", "INFO")
                # Cancel remaining futures
                for future in futures:
                    future.cancel()
                break
            try:
                res = f.result()
                if DEBUG_MODE:
                    log_debug(f"Scanned: {res['filename']} -> {res['category']} {res['profile']} (error: {res.get('scan_error', 'None')}, path: {res['full_path']})", "DEBUG")
                batch_buffer.append(res)
                
                if not res.get('scan_error'):
                    if res.get('bitrate_mbps'):
                        metrics_sum["bitrate"] += float(res['bitrate_mbps'])
                        metrics_count["bitrate"] += 1
                    if res.get('width'):
                        metrics_sum["width"] += int(res['width'])
                        metrics_count["width"] += 1
                    if res.get('height'):
                        metrics_sum["height"] += int(res['height'])
                        metrics_count["height"] += 1
                    if res.get('file_size'):
                        metrics_sum["file_size"] += int(res['file_size'])
                        metrics_count["file_size"] += 1
                
                attempts = processed_map.get(res['full_path'], {}).get('attempts', 0)
                if res['scan_error']:
                    attempts += 1
                else:
                    attempts = 0
                res['scan_attempts'] = attempts

                if res['scan_error']:
                    log_failure(res['source_vol'], res['full_path'], res['filename'], res['scan_error'])
                    progress_updates["failed_count"] += 1
                    if DEBUG_MODE:
                        log_debug(f"File has error, will still be saved: {res['full_path']} - {res['scan_error']}", "DEBUG")
                else:
                    progress_updates["new_found"] += 1
                    if DEBUG_MODE:
                        log_debug(f"File scanned successfully: {res['full_path']}", "DEBUG")
                
                progress_updates["current"] += 1
                
                batch_size = 50
                try:
                    batch_size = int(settings.get('batch_size', 50))
                except (ValueError, TypeError):
                    pass
                        
                if len(batch_buffer) >= batch_size:
                    save_batch_to_db(batch_buffer)
                    batch_buffer = []
                            
                if progress_updates["current"] >= PROGRESS_UPDATE_INTERVAL:
                    with progress_lock: 
                        old_current = PROGRESS.get("current", 0)
                        old_total = PROGRESS.get("total", 0)
                        PROGRESS["current"] += progress_updates["current"]
                        PROGRESS["failed_count"] += progress_updates["failed_count"]
                        PROGRESS["new_found"] += progress_updates["new_found"]
                        elapsed = int(time.time() - start_time)
                        PROGRESS["last_duration"] = f"{elapsed}s"
                        if old_current == 0 and PROGRESS["current"] == progress_updates["current"] and old_total != PROGRESS.get("total", 0):
                            log_debug(f"[WARNING] PROGRESS current reset detected! Was {old_current}/{old_total}, now {PROGRESS['current']}/{PROGRESS.get('total', 0)}", "WARNING")
                        if PROGRESS["current"] > 0 and PROGRESS["total"] > 0 and elapsed > 0:
                            rate = PROGRESS["current"] / elapsed
                            remaining = PROGRESS["total"] - PROGRESS["current"]
                            eta_seconds = int(remaining / rate) if rate > 0 else 0
                            PROGRESS["eta"] = f"{eta_seconds}s" if eta_seconds > 0 else "calculating..."
                    global DIAG_LOG_TS
                    now = time.time()
                    if now - DIAG_LOG_TS >= 5:
                        log_debug(
                            f"[SCAN_DIAG] current={PROGRESS.get('current', 0)}/{PROGRESS.get('total', 0)} "
                            f"new={PROGRESS.get('new_found', 0)} failed={PROGRESS.get('failed_count', 0)} "
                            f"batch_buffer={len(batch_buffer)}",
                            "INFO"
                        )
                        DIAG_LOG_TS = now
                    progress_updates = {"current": 0, "failed_count": 0, "new_found": 0}
            except Exception as e:
                log_debug(f"Thread error processing file: {e}", "ERROR")
                import traceback
                log_debug(f"Thread error traceback: {traceback.format_exc()}", "ERROR")
                progress_updates["failed_count"] += 1
                progress_updates["current"] += 1
    
    if batch_buffer:
        save_batch_to_db(batch_buffer)

    if progress_updates["current"] > 0:
        with progress_lock:
            PROGRESS["current"] += progress_updates["current"]
            PROGRESS["failed_count"] += progress_updates["failed_count"]
            PROGRESS["new_found"] += progress_updates["new_found"]
    
    return {"metrics_sum": metrics_sum, "metrics_count": metrics_count}

def cleanup_deleted_files(target_vols: list | None, scan_paths: list, all_found_files: set) -> None:
    """
    Remove files from database that no longer exist on disk.
    
    Args:
        target_vols: List of target volume names, or None
        scan_paths: List of scan paths
        all_found_files: Set of all files found during scan
    """
    log_debug("🧹 Running cleanup...", "INFO")
    to_del = []
    with get_db() as conn:
        if target_vols and len(target_vols) > 0:
            placeholders = ','.join('?' * len(target_vols))
            sql = f"SELECT full_path FROM videos WHERE source_vol IN ({placeholders})"
            existing_db_files = {row[0] for row in conn.execute(sql, tuple(target_vols)).fetchall()}
        else:
            online_prefixes = tuple(scan_paths)
            all_rows = conn.execute("SELECT full_path FROM videos").fetchall()
            existing_db_files = {r[0] for r in all_rows if r[0].startswith(online_prefixes)}
    
    for f in existing_db_files:
        if f not in all_found_files:
            to_del.append(f)
    
    if to_del:
        log_debug(f"Cleaning up {len(to_del)} missing files from DB...", "INFO")
        with get_db() as conn:
            conn.executemany("DELETE FROM videos WHERE full_path=?", [(f,) for f in to_del])

def finalize_scan(metrics_sum: dict, metrics_count: dict, start_time: float,
                  scan_mode: str, target_vols: Optional[List[str]],
                  scan_folder: dict | None) -> None:
    """
    Finalize scan by updating database settings and PROGRESS state.
    
    Args:
        metrics_sum: Dictionary of accumulated metrics
        metrics_count: Dictionary of metric counts
        start_time: Scan start time
    """
    dur = f"{int(time.time() - start_time)}s"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_full_scan', ?)", (now,))
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_duration', ?)", (dur,))
            
    avg_bitrate = round(metrics_sum["bitrate"] / metrics_count["bitrate"], 2) if metrics_count["bitrate"] > 0 else 0
    avg_width = round(metrics_sum["width"] / metrics_count["width"]) if metrics_count["width"] > 0 else 0
    avg_height = round(metrics_sum["height"] / metrics_count["height"]) if metrics_count["height"] > 0 else 0
    avg_file_size_mb = round(metrics_sum["file_size"] / metrics_count["file_size"] / (1024 * 1024), 2) if metrics_count["file_size"] > 0 else 0
            
    with progress_lock:
        PROGRESS.update({"last_full_scan": now, "last_duration": dur, "scan_completed": True, "status": "idle", "paused": False})
        PROGRESS["last_report"] = {
            "scanned": PROGRESS["total"],
            "new": PROGRESS["new_found"],
            "failed": PROGRESS["failed_count"],
            "warnings": PROGRESS.get("warning_count", 0),
            "duration": dur,
            "avg_bitrate": avg_bitrate,
            "avg_width": avg_width,
            "avg_height": avg_height,
            "avg_file_size_mb": avg_file_size_mb
        }
        history_entry = {
            "status": "complete",
            "duration": dur,
            "scanned": PROGRESS["total"],
            "new": PROGRESS["new_found"],
            "failed": PROGRESS["failed_count"],
            "warnings": PROGRESS.get("warning_count", 0),
            "scan_mode": scan_mode,
            "target_vols": target_vols or [],
            "scan_folder": scan_folder.get("path") if isinstance(scan_folder, dict) else None
        }
    record_scan_history(history_entry)
            
    log_debug(f"[SUCCESS] Finished: {dur}. Added: {PROGRESS['new_found']}. Errors: {PROGRESS['failed_count']}", "INFO")


def run_scan(thread_count: Optional[int] = None, target_vols: Optional[List[str]] = None, 
             force_rescan: bool = False, debug: bool = False, scan_mode: str = "all",
             scan_folder: dict | None = None) -> None:
    """
    Main scan function that orchestrates the entire scanning process.
    
    This function coordinates database loading, file collection, analysis, and cleanup.
    It handles abort signals and ensures proper cleanup of resources.
    
    Args:
        thread_count: Number of worker threads to use for file analysis. If None, uses saved setting.
        target_vols: List of volume names to scan. If None, scans all mounted volumes.
        force_rescan: If True, resets scan attempts and rescans all files regardless of previous status.
        debug: If True, enables verbose debug logging throughout the scan process.
        
    Raises:
        RuntimeError: If scan is already in progress (race condition protection).
    """
    global PROGRESS, ABORT_SCAN, DEBUG_MODE
    start_time = time.time()
    
    # Check and set status atomically to prevent race condition
    with progress_lock:
        if PROGRESS["status"] == "scanning":
            log_debug(f"[WARNING] Attempted to start scan while already scanning! Current progress: {PROGRESS.get('current', 0)}/{PROGRESS.get('total', 0)}", "WARNING")
            return
        # Atomically set status to scanning before releasing lock
        PROGRESS.update({"status": "scanning", "current": 0, "total": 0, "file": "Initializing...", "scan_completed": False, "new_found": 0, "failed_count": 0, "warning_count": 0, "last_duration": "0s", "start_time": start_time})
    
    ABORT_SCAN = False
    PAUSE_EVENT.set()
    with progress_lock:
        PROGRESS["paused"] = False
    DEBUG_MODE = debug
    
    # Clear RPU cache on force rescan to ensure fresh data
    if force_rescan:
        clear_rpu_cache()
    
    setup_new_log_files()
    cleanup_old_logs()

    final_threads = 4
    try:
        with get_db() as conn:
            saved = conn.execute("SELECT value FROM settings WHERE key='threads'").fetchone()
            if saved: final_threads = int(saved[0])
    except sqlite3.Error as e:
        if DEBUG_MODE: log_debug(f"Error reading thread setting: {e}")
    if thread_count: final_threads = int(thread_count)

    log_debug("[INIT] Initializing scan...", "INFO")
    
    try:
        with get_db() as conn:
            settings = dict(conn.execute("SELECT key, value FROM settings").fetchall())
        
        skip_words = [w.strip().lower() for w in settings.get('skip_words', '').split(',') if w.strip()]
        min_size = int(settings.get('min_size_mb', 0)) * 1024 * 1024
        scan_extras = str(settings.get('scan_extras', 'false')).lower() == 'true'
        
        log_debug(f"[STARTED] Scan started. Threads={final_threads}. Force={force_rescan}. Debug={DEBUG_MODE}", "INFO")
        
        processed_map = load_processed_map()
        scan_paths, path_to_vol = prepare_scan_paths(target_vols, force_rescan)
        scan_folders = []
        try:
            scan_folders = json.loads(settings.get('scan_folders', '[]') or '[]')
        except (json.JSONDecodeError, TypeError):
            scan_folders = []
        if isinstance(scan_folders, list) and scan_folders:
            if scan_folder and isinstance(scan_folder, dict):
                match = next(
                    (f for f in scan_folders
                     if (f.get('volume') or '') == (scan_folder.get('volume') or '')
                     and (f.get('path') or '') == (scan_folder.get('path') or '')),
                    None
                )
                if match:
                    folder_paths, folder_map = build_scan_paths_from_folders([match], target_vols, force_rescan, scan_mode)
                    if folder_paths:
                        scan_paths, path_to_vol = folder_paths, folder_map
                else:
                    folder_paths, folder_map = build_scan_paths_from_folders(scan_folders, target_vols, force_rescan, scan_mode)
                    if folder_paths:
                        scan_paths, path_to_vol = folder_paths, folder_map
            else:
                folder_paths, folder_map = build_scan_paths_from_folders(scan_folders, target_vols, force_rescan, scan_mode)
                if folder_paths:
                    scan_paths, path_to_vol = folder_paths, folder_map
        files_to_scan, all_found_files = collect_files_to_scan(scan_paths, path_to_vol, processed_map, 
                                                               skip_words, min_size, force_rescan, start_time, scan_extras)
        
        metrics = {"metrics_sum": {"bitrate": 0.0, "width": 0, "height": 0, "file_size": 0},
                   "metrics_count": {"bitrate": 0, "width": 0, "height": 0, "file_size": 0}}
        
        if not ABORT_SCAN and files_to_scan:
            metrics = analyze_files(files_to_scan, processed_map, settings, final_threads, start_time)
        
        if not ABORT_SCAN:
            cleanup_deleted_files(target_vols, scan_paths, all_found_files)
            finalize_scan(metrics["metrics_sum"], metrics["metrics_count"], start_time, scan_mode, target_vols, scan_folder)
        else:
            log_debug("[ABORT] Killing active subprocesses...")
            with proc_lock:
                for p in ACTIVE_PROCS:
                    try: 
                        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                    except (OSError, ProcessLookupError, ValueError) as e:
                        # Process may already be terminated or invalid
                        if DEBUG_MODE:
                            log_debug(f"Failed to kill process {p.pid}: {e}", "DEBUG")
            log_debug("[ABORT] User aborted.")
            dur = f"{int(time.time() - start_time)}s"
            with progress_lock:
                PROGRESS.update({"status": "idle", "file": "Aborted", "paused": False, "scan_completed": True, "last_duration": dur})
                PROGRESS["last_report"] = {
                    "scanned": PROGRESS.get("current", 0),
                    "new": PROGRESS.get("new_found", 0),
                    "failed": PROGRESS.get("failed_count", 0),
                    "warnings": PROGRESS.get("warning_count", 0),
                    "duration": dur,
                    "aborted": True
                }
            record_scan_history({
                "status": "aborted",
                "duration": dur,
                "scanned": PROGRESS.get("current", 0),
                "new": PROGRESS.get("new_found", 0),
                "failed": PROGRESS.get("failed_count", 0),
                "warnings": PROGRESS.get("warning_count", 0),
                "scan_mode": scan_mode,
                "target_vols": target_vols or [],
                "scan_folder": scan_folder.get("path") if isinstance(scan_folder, dict) else None
            })

    except Exception as e:
        log_debug(f"[ERROR] CRITICAL: {e}")
        import traceback; traceback.print_exc()
        with progress_lock: PROGRESS["status"] = "idle"

# --- ROUTES ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/health')
@app.route('/api/health')
def health_check() -> Response:
    """
    Health check endpoint for monitoring and load balancers.
    
    Checks database connectivity and returns system status.
    Used by monitoring systems to verify the application is running correctly.
    
    Returns:
        JSON response with status, database connectivity, and scan status
    """
    try:
        # Check database connectivity
        with get_db() as conn:
            conn.execute("SELECT 1").fetchone()
        db_status = "ok"
    except sqlite3.Error:
        db_status = "error"
    
    status = "healthy" if db_status == "ok" else "degraded"
    payload = {
        "status": status,
        "database": db_status,
        "scan_status": PROGRESS.get("status", "unknown"),
        "uptime_seconds": int(time.time() - APP_START_TIME),
        "version": APP_VERSION,
    }
    return jsonify(payload), (200 if status == "healthy" else 503)


@app.errorhandler(400)
def api_bad_request(error) -> Response:
    if not request.path.startswith('/api/'):
        return error
    return jsonify({
        "status": "error",
        "error": "bad_request",
        "message": "Bad request"
    }), 400


@app.errorhandler(404)
def api_not_found(error) -> Response:
    if not request.path.startswith('/api/'):
        return error
    return jsonify({
        "status": "error",
        "error": "not_found",
        "message": "Endpoint not found"
    }), 404


@app.errorhandler(500)
def api_internal_error(error) -> Response:
    if not request.path.startswith('/api/'):
        return error
    return jsonify({
        "status": "error",
        "error": "internal_error",
        "message": "Internal server error"
    }), 500

@app.route('/api/logs')
def get_logs() -> Response:
    """
    Get recent log entries from the in-memory log cache.
    
    Returns:
        JSON array of recent log messages
    """
    with progress_lock: 
        return jsonify(list(LOG_CACHE))

@app.route('/api/scan_history')
def get_scan_history() -> Response:
    """
    Return recent scan history entries.
    """
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT entry FROM scan_history ORDER BY id DESC LIMIT 50"
            ).fetchall()
        entries: List[dict] = []
        for row in rows:
            try:
                entries.append(json.loads(row[0]))
            except (TypeError, ValueError):
                continue
        return jsonify({"status": "ok", "entries": entries})
    except Exception as e:
        log_debug(f"Failed to load scan history: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/download_log')
def download_log() -> Union[Response, Tuple[str, int]]:
    """
    Download the current scan activity log file.
    
    Returns:
        File download response if log file exists, or 404 error message if not found
    """
    if LOG_FILE and os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, as_attachment=True, download_name=os.path.basename(LOG_FILE))
    return "No log found", 404

@app.route('/download_failures')
def download_failures() -> Union[Response, Tuple[str, int]]:
    """Download the current scan failures CSV file."""
    if FAIL_FILE and os.path.exists(FAIL_FILE):
        return send_file(FAIL_FILE, as_attachment=True, download_name=os.path.basename(FAIL_FILE))
    return "No failures log found", 404

@app.route('/api/failures')
def get_failures() -> Response:
    """Get recent failures/warnings from the scan failures CSV file."""
    limit = 200
    try:
        limit = int(request.args.get('limit', limit))
    except (TypeError, ValueError):
        limit = 200
    entries: list[dict] = []
    if FAIL_FILE and os.path.exists(FAIL_FILE):
        try:
            with open(FAIL_FILE, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f, delimiter='|')
                for row in reader:
                    if len(row) < 5:
                        continue
                    ts, vol, path, name, msg = row[:5]
                    entry_type = 'warning' if vol == 'WARNING' else 'failure'
                    entries.append({
                        "type": entry_type,
                        "timestamp": ts,
                        "volume": vol,
                        "path": path,
                        "name": name,
                        "message": msg
                    })
        except OSError:
            pass
    if limit > 0 and len(entries) > limit:
        entries = entries[-limit:]
    failures = [e for e in entries if e["type"] == "failure"]
    warnings = [e for e in entries if e["type"] == "warning"]
    return jsonify({"failures": failures, "warnings": warnings})

@app.route('/api/pre_scan_check')
def pre_scan_check() -> Response:
    """
    Check mount status of all volumes before scanning.
    
    Returns volume status (online/offline/empty) for all known volumes,
    including volumes that exist in the database but may not be currently mounted.
    
    Returns:
        JSON array of volume status objects with name, status, and path
    """
    mounted = get_mount_status() 
    with get_db() as conn:
        rows = conn.execute("SELECT DISTINCT source_vol FROM videos").fetchall()
        db_vols = {r[0] for r in rows if r[0]}
    all_vols = set(mounted.keys()) | db_vols
    result = []
    for v in sorted(list(all_vols)):
        status = "offline"
        path = mounted.get(v, None)
        if path and os.path.exists(path):
            status = "online"
            try:
                if not os.listdir(path): 
                    status = "empty"
            except (OSError, PermissionError) as e:
                # Directory may be inaccessible, keep status as "online"
                if DEBUG_MODE:
                    log_debug(f"Cannot list directory {path}: {e}", "DEBUG")
        result.append({"name": v, "status": status, "path": path})
    return jsonify(result)

def parse_advanced_search(search_query: str) -> Tuple[str, Dict[str, Any]]:
    """
    Parse advanced search syntax to extract field:value patterns.
    
    Supports patterns like:
    - field:value (e.g., year:2020, codec:HEVC)
    - field:>value (e.g., size:>10GB, year:>2020)
    - field:<value (e.g., size:<5GB)
    - field:>=value, field:<=value, field:!=value
    
    Args:
        search_query: Search query string that may contain field:value patterns
        
    Returns:
        Tuple of (remaining_search_text, extracted_filters_dict)
        
    Examples:
        text, filters = parse_advanced_search("year:2020 codec:HEVC some movie")
        # Returns: ("some movie", {'year': '2020', 'video_codec': 'HEVC'})
        
        text, filters = parse_advanced_search("size:>10GB year:>=2020")
        # Returns: ("", {'size_op': '>', 'size_val': '10GB', 'year': '>=2020'})
    """
    if not search_query:
        return '', {}
    
    extracted_filters = {}
    remaining_parts = []
    
    # Pattern to match field:operator?value (e.g., year:2020, size:>10GB, codec:HEVC)
    # Matches: field_name, optional operator (>, <, >=, <=, !=), value (supports quoted strings)
    # Allows optional whitespace around the operator/value.
    pattern = r'\b(\w+):\s*(>=|<=|!=|>|<)?\s*("[^"]+"|\'[^\']+\'|[^\s]+)'
    matches = re.finditer(pattern, search_query)
    
    # Field name mapping from search syntax to filter parameter names
    field_map = {
        'year': 'year',
        'codec': 'video_codec',
        'source': 'video_source',
        'format': 'source_format',
        'resolution': 'resolution',
        'res': 'resolution',
        'profile': 'profile',
        'prof': 'profile',
        'volume': 'volume',
        'vol': 'volume',
        'category': 'category',
        'cat': 'category',
        'container': 'container',
        'cont': 'container',
        'size': 'size',
        'bitrate': 'bitrate',
        'bit': 'bitrate',
        'edition': 'edition',
        'hybrid': 'source_hybrid',
        'dual': 'is_hybrid',
        'dual_hdr': 'is_hybrid',
        'source_hybrid': 'source_hybrid',
        'hybrid_src': 'source_hybrid',
        '3d': 'is_3d',
        'nfo': 'nfo_missing',
        'nfo_missing': 'nfo_missing',
    }
    
    # Collect all matches with their positions
    match_positions = []
    for match in matches:
        field_name = match.group(1).lower()
        operator = match.group(2) or ''
        value = match.group(3)
        if value and ((value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'"))):
            value = value[1:-1]
        start_pos = match.start()
        end_pos = match.end()
        match_positions.append((start_pos, end_pos, field_name, operator, value))
    
    # If no advanced filters were found, keep the original search text
    if not match_positions:
        return search_query.strip(), {}

    # Remove matched patterns from search query and build remaining text
    if match_positions:
        last_pos = 0
        for start, end, field_name, operator, value in sorted(match_positions):
            # Add text before this match
            remaining_parts.append(search_query[last_pos:start])
            last_pos = end
            
            # Process the field:value pattern
            if field_name in field_map:
                filter_key = field_map[field_name]
                
                # Handle size and bitrate with operators
                if filter_key == 'size':
                    if operator:
                        extracted_filters['size_op'] = operator
                        extracted_filters['size_val'] = value
                    else:
                        # Default to = if no operator
                        extracted_filters['size_op'] = '='
                        extracted_filters['size_val'] = value
                elif filter_key == 'bitrate':
                    if operator:
                        extracted_filters['bit_op'] = operator
                        extracted_filters['bit_val'] = value
                    else:
                        extracted_filters['bit_op'] = '='
                        extracted_filters['bit_val'] = value
                # Handle year with or without operators
                elif filter_key == 'year':
                    if operator:
                        extracted_filters['year_op'] = operator
                        extracted_filters['year_val'] = value
                    else:
                        extracted_filters['year'] = value
                elif filter_key == 'is_hybrid':
                    # Convert boolean-like values
                    if value.lower() in ('1', 'true', 'yes', 'y'):
                        extracted_filters['is_hybrid'] = '1'
                    elif value.lower() in ('0', 'false', 'no', 'n'):
                        extracted_filters['is_hybrid'] = '0'
                elif filter_key == 'source_hybrid':
                    if value.lower() in ('1', 'true', 'yes', 'y'):
                        extracted_filters['source_hybrid'] = '1'
                    elif value.lower() in ('0', 'false', 'no', 'n'):
                        extracted_filters['source_hybrid'] = '0'
                elif filter_key == 'is_3d':
                    if value.lower() in ('1', 'true', 'yes', 'y'):
                        extracted_filters['is_3d'] = '1'
                    else:
                        extracted_filters['is_3d'] = '0'
                else:
                    # Regular field:value
                    extracted_filters[filter_key] = value
        
        # Add remaining text after last match
        remaining_parts.append(search_query[last_pos:])
    
    # Join remaining parts and clean up whitespace
    remaining_text = ' '.join(remaining_parts).strip()
    
    return remaining_text, extracted_filters

def build_filter_query(args: Dict[str, Any], exclude_key: Optional[str] = None) -> Tuple[str, List[Any]]:
    """
    Build SQL WHERE clause and parameters from filter arguments.
    
    Constructs a SQL WHERE clause with placeholders and corresponding parameter list
    based on the provided filter arguments. Supports various filter types including
    search, category, volume, profile, resolution, status, and custom size/bitrate operators.
    
    Args:
        args: Dictionary of filter arguments (typically from request.args or request.json)
        exclude_key: Optional key to exclude from the filter query (useful for nested queries)
        
    Returns:
        Tuple of (WHERE clause string, parameter list)
        
    Example:
        where, params = build_filter_query({'category': 'dovi', 'resolution': '4K'})
        # Returns: ("1=1 AND category = ? AND resolution = ?", ['dovi', '4K'])
    """
    conditions = ["1=1"]; params = []
    
    # Parse advanced search syntax if search parameter exists
    # Create a copy of args to avoid modifying the original dict
    args = dict(args)
    search_query = args.get('search', '').strip()
    if search_query:
        remaining_search, advanced_filters = parse_advanced_search(search_query)
        # Merge advanced filters into args (advanced filters take precedence)
        args.update(advanced_filters)
        # Update search with remaining text
        args['search'] = remaining_search
    
    blank_token = '__blank__'
    mappings = [('search', 'filename'), ('category', 'category'), ('volume', 'source_vol'), ('profile', 'profile'), ('el', 'el_type'), ('container', 'container'), ('resolution', 'resolution'), ('status', 'scan_error'), ('audio', 'audio_codecs'), ('video_codec', 'video_codec'), ('video_source', 'video_source'), ('source_format', 'source_format'), ('edition', 'edition'), ('media_type', 'media_type'), ('nfo_missing', 'nfo_missing')]
    for key, col in mappings:
        if key == exclude_key: continue
        val = args.get(key, '').strip()
        if val:
            if key == 'search':
                conditions.append(f"(LOWER({col}) LIKE ? OR LOWER(full_path) LIKE ?)")
                params.extend([f"%{val.lower()}%", f"%{val.lower()}%"])
            elif key == 'status': 
                if val == 'failed': conditions.append("scan_error IS NOT NULL AND scan_error != ''")
                elif val == 'ok': conditions.append("(scan_error IS NULL OR scan_error = '')")
            elif key == 'audio':
                values = [v.strip() for v in val.split(',') if v.strip()]
                if blank_token in values:
                    values = [v for v in values if v != blank_token]
                    blank_clause = f"({col} IS NULL OR {col} = '')"
                    if values:
                        like_clauses = [f"LOWER({col}) LIKE ?" for _ in values]
                        params.extend([f"%{v.lower()}%" for v in values])
                        conditions.append(f"({ ' OR '.join(like_clauses + [blank_clause]) })")
                    else:
                        conditions.append(blank_clause)
                else:
                    conditions.append(f"LOWER({col}) LIKE ?"); params.append(f"%{val.lower()}%")
            elif key == 'video_codec':
                values = [v.strip() for v in val.split(',') if v.strip()]
                if blank_token in values:
                    values = [v for v in values if v != blank_token]
                    blank_clause = f"({col} IS NULL OR {col} = '')"
                    if values:
                        placeholders = ','.join('?' * len(values))
                        conditions.append(f"(LOWER({col}) IN ({placeholders}) OR {blank_clause})")
                        params.extend([v.lower() for v in values])
                    else:
                        conditions.append(blank_clause)
                elif len(values) > 1:
                    placeholders = ','.join('?' * len(values))
                    conditions.append(f"LOWER({col}) IN ({placeholders})")
                    params.extend([v.lower() for v in values])
                else:
                    conditions.append(f"LOWER({col}) = ?"); params.append(val.lower())
            elif key == 'nfo_missing':
                val_lower = val.lower()
                if val_lower in ('missing', 'none', '1', 'true', 'yes'):
                    conditions.append(f"{col} = 1")
                elif val_lower in ('found', '0', 'false', 'no'):
                    conditions.append(f"{col} = 0")
            elif ',' in val or val == blank_token:
                # Handle multiple values (comma-separated) for any filter type, including blanks
                values = [v.strip() for v in val.split(',') if v.strip()]
                if blank_token in values:
                    values = [v for v in values if v != blank_token]
                    blank_clause = f"({col} IS NULL OR {col} = '')"
                    if values:
                        placeholders = ','.join('?' * len(values))
                        conditions.append(f"(LOWER({col}) IN ({placeholders}) OR {blank_clause})")
                        params.extend([v.lower() for v in values])
                    else:
                        conditions.append(blank_clause)
                elif values:
                    placeholders = ','.join('?' * len(values))
                    conditions.append(f"LOWER({col}) IN ({placeholders})")
                    params.extend([v.lower() for v in values])
            else:
                conditions.append(f"LOWER({col}) = ?"); params.append(val.lower())
    if exclude_key != 'secondary_hdr':
        sec = args.get('secondary_hdr', '').strip()
        if sec:
            values = [v.strip() for v in sec.split(',') if v.strip()]
            if blank_token in values or sec == 'none':
                values = [v for v in values if v != blank_token and v != 'none']
                blank_clause = "(secondary_hdr IS NULL OR secondary_hdr = '')"
                if values:
                    placeholders = ','.join('?' * len(values))
                    conditions.append(f"(LOWER(secondary_hdr) IN ({placeholders}) OR {blank_clause})")
                    params.extend([v.lower() for v in values])
                else:
                    conditions.append(blank_clause)
            elif ',' in sec:
                placeholders = ','.join('?' * len(values))
                conditions.append(f"LOWER(secondary_hdr) IN ({placeholders})")
                params.extend([v.lower() for v in values])
            else:
                conditions.append("LOWER(secondary_hdr) = ?"); params.append(sec.lower())
    if exclude_key != 'is_hybrid':
        hyb = args.get('is_hybrid', '').strip()
        if hyb == "1": conditions.append("is_hybrid = 1")
        elif hyb == "0": conditions.append("is_hybrid = 0 AND category != 'sdr_only'")
    if exclude_key != 'source_hybrid':
        src_hyb = args.get('source_hybrid', '').strip()
        if src_hyb == "1":
            conditions.append("is_source_hybrid = 1")
        elif src_hyb == "0":
            conditions.append("is_source_hybrid = 0")
    
    # Handle size filtering with operators
    if exclude_key != 'size':
        size_op = args.get('size_op', '').strip()
        size_val = args.get('size_val', '').strip()
        if size_op and size_val:
            try:
                # Parse value - handle GB, MB, etc.
                size_val_clean = size_val.upper().replace('GB', '').replace('MB', '').replace(' ', '').strip()
                size_bytes = float(size_val_clean)
                if 'GB' in size_val.upper():
                    size_bytes = size_bytes * 1024 * 1024 * 1024
                elif 'MB' in size_val.upper():
                    size_bytes = size_bytes * 1024 * 1024
                elif 'KB' in size_val.upper():
                    size_bytes = size_bytes * 1024
                
                if size_op == '>':
                    conditions.append("file_size > ?")
                elif size_op == '<':
                    conditions.append("file_size < ?")
                elif size_op == '=' or size_op == '==':
                    conditions.append("file_size = ?")
                elif size_op == '>=':
                    conditions.append("file_size >= ?")
                elif size_op == '<=':
                    conditions.append("file_size <= ?")
                params.append(int(size_bytes))
            except (ValueError, TypeError):
                pass  # Ignore invalid size values
    
    # Handle bitrate filtering with operators
    if exclude_key != 'bitrate':
        bit_op = args.get('bit_op', '').strip()
        bit_val = args.get('bit_val', '').strip()
        if bit_op and bit_val:
            try:
                # Parse value - handle Mbps, etc.
                bit_val_clean = bit_val.upper().replace('MBPS', '').replace('MBIT/S', '').replace(' ', '').strip()
                bitrate_val = float(bit_val_clean)
                
                if bit_op == '>':
                    conditions.append("bitrate_mbps > ?")
                elif bit_op == '<':
                    conditions.append("bitrate_mbps < ?")
                elif bit_op == '=' or bit_op == '==':
                    conditions.append("bitrate_mbps = ?")
                elif bit_op == '>=':
                    conditions.append("bitrate_mbps >= ?")
                elif bit_op == '<=':
                    conditions.append("bitrate_mbps <= ?")
                params.append(bitrate_val)
            except (ValueError, TypeError):
                pass  # Ignore invalid bitrate values
    
    # Handle year filtering with operators
    if exclude_key != 'year':
        year_op = args.get('year_op', '').strip()
        year_val = args.get('year_val', '').strip()
        year = args.get('year', '').strip()
        if year_op and year_val:
            try:
                year_int = int(year_val)
                if year_op == '>':
                    conditions.append("year > ?")
                elif year_op == '<':
                    conditions.append("year < ?")
                elif year_op == '>=':
                    conditions.append("year >= ?")
                elif year_op == '<=':
                    conditions.append("year <= ?")
                elif year_op == '!=':
                    conditions.append("year != ?")
                params.append(year_int)
            except (ValueError, TypeError):
                pass
        elif year:
            try:
                year_int = int(year)
                conditions.append("year = ?")
                params.append(year_int)
            except (ValueError, TypeError):
                pass
    
    # Handle is_3d filtering
    if exclude_key != 'is_3d':
        is_3d_val = args.get('is_3d', '').strip()
        if is_3d_val:
            if is_3d_val == '1':
                conditions.append("is_3d = 1")
            elif is_3d_val == '0':
                conditions.append("is_3d = 0")
    
    return " AND ".join(conditions), params

def _build_stats_from_rows(rows: list) -> dict:
    stats = {
        "total": len(rows), "failed": 0, "hybrid": 0, "source_hybrid": 0,
        "dovi": 0, "dovi_p7_fel": 0, "dovi_p7_mel": 0, "dovi_p81": 0, "dovi_p84": 0, "dovi_p8": 0, "dovi_p5": 0,
        "hdr10plus": 0, "hdr10": 0, "hlg": 0, "sdr": 0,
        "vol_labels": [], "vol_data": [], "res_labels": [], "res_data": [],
        "secondary_hdrs": {}
    }
    for r in rows:
        if r['scan_error']:
            stats['failed'] += 1
            continue
        if r['is_hybrid']:
            stats['hybrid'] += 1
        try:
            if r['is_source_hybrid']:
                stats['source_hybrid'] += 1
        except (KeyError, IndexError, TypeError):
            pass
        cat, prof, el = r['category'], r['profile'], r['el_type']
        if cat == 'dovi':
            stats['dovi'] += 1
            if prof == '7':
                stats['dovi_p7_fel' if el == 'FEL' else 'dovi_p7_mel'] += 1
            elif prof == '5':
                stats['dovi_p5'] += 1
            elif prof == '8.1':
                stats['dovi_p81'] += 1
            elif prof == '8.4':
                stats['dovi_p84'] += 1
            elif prof == '8':
                stats['dovi_p8'] += 1
        elif cat == 'hdr10plus':
            stats['hdr10plus'] += 1
        elif cat == 'hdr10':
            stats['hdr10'] += 1
        elif cat == 'hlg':
            stats['hlg'] += 1
        elif cat == 'sdr_only':
            stats['sdr'] += 1
    return stats

@app.route('/download_csv')
def download_csv() -> Response:
    """
    Download filtered video data as CSV file.
    
    Applies current filter parameters and exports matching video records
    to a CSV file for external analysis.
    
    Returns:
        CSV file download response with filtered video data
    """
    where_clause, params = build_filter_query(request.args)
    sort_map = {'file': 'filename', 'hybrid': 'is_hybrid', 'source_hybrid': 'is_source_hybrid', 'main': 'category', 'prof': 'profile', 'el': 'el_type', 'sec': 'secondary_hdr', 'res': 'resolution', 'bit': 'bitrate_mbps', 'vol': 'source_vol', 'cont': 'container', 'scan': 'last_scanned', 'stat': 'scan_error', 'size': 'file_size'}
    db_sort = sort_map.get(request.args.get('sort'), 'last_scanned'); order = request.args.get('order', 'desc')
    page = request.args.get('page')
    per_page = request.args.get('per_page')
    limit_clause = ""
    limit_params: list[Any] = []
    try:
        if page is not None and per_page is not None:
            page_val = max(1, int(page))
            per_page_val = max(1, int(per_page))
            offset = (page_val - 1) * per_page_val
            limit_clause = " LIMIT ? OFFSET ?"
            limit_params = [per_page_val, offset]
    except (ValueError, TypeError):
        limit_clause = ""
        limit_params = []
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT filename, category, profile, el_type, container, source_vol, full_path, last_scanned, resolution, bitrate_mbps, scan_error, is_hybrid, is_source_hybrid, secondary_hdr, width, height, file_size, bl_compatibility_id, audio_codecs, audio_channels, subtitles, max_cll, fps, aspect_ratio, imdb_id, tvdb_id, tmdb_id, rotten_id, metacritic_id, trakt_id, imdb_rating, tvdb_rating, tmdb_rating, rotten_rating, metacritic_rating, trakt_rating, nfo_missing FROM videos WHERE {where_clause} ORDER BY {db_sort} {order}{limit_clause}",
            params + limit_params
        ).fetchall()
    si = io.StringIO()
    csv.writer(si).writerows([['Filename', 'Cat', 'Prof', 'EL', 'Cont', 'Vol', 'Path', 'Date', 'Res', 'Bitrate', 'Error', 'Dual HDR', 'Hybrid', 'SecHDR', 'Width', 'Height', 'Size', 'BL_ID', 'Audio', 'AudioCh', 'Subs', 'MaxCLL', 'FPS', 'Aspect', 'IMDB_ID', 'TVDB_ID', 'TMDB_ID', 'Rotten_ID', 'Metacritic_ID', 'Trakt_ID', 'IMDB_Rating', 'TVDB_Rating', 'TMDB_Rating', 'Rotten_Rating', 'Metacritic_Rating', 'Trakt_Rating', 'NFO_Missing']] + list(rows))
    return make_response(si.getvalue(), 200, {"Content-Disposition": "attachment; filename=media_export.csv", "Content-type": "text/csv"})

@app.route('/download_json')
def download_json() -> Response:
    """
    Download filtered video data as JSON file.
    
    Applies current filter parameters and exports matching video records
    to a JSON file for external analysis.
    
    Returns:
        JSON file download response with filtered video data
    """
    where_clause, params = build_filter_query(request.args)
    sort_map = {'file': 'filename', 'hybrid': 'is_hybrid', 'source_hybrid': 'is_source_hybrid', 'main': 'category', 'prof': 'profile', 'el': 'el_type', 'sec': 'secondary_hdr', 'res': 'resolution', 'bit': 'bitrate_mbps', 'vol': 'source_vol', 'cont': 'container', 'scan': 'last_scanned', 'stat': 'scan_error', 'size': 'file_size'}
    db_sort = sort_map.get(request.args.get('sort'), 'last_scanned'); order = request.args.get('order', 'desc')
    page = request.args.get('page')
    per_page = request.args.get('per_page')
    limit_clause = ""
    limit_params: list[Any] = []
    try:
        if page is not None and per_page is not None:
            page_val = max(1, int(page))
            per_page_val = max(1, int(per_page))
            offset = (page_val - 1) * per_page_val
            limit_clause = " LIMIT ? OFFSET ?"
            limit_params = [per_page_val, offset]
    except (ValueError, TypeError):
        limit_clause = ""
        limit_params = []
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT filename, category, profile, el_type, container, source_vol, full_path, last_scanned, resolution, bitrate_mbps, scan_error, is_hybrid, is_source_hybrid, secondary_hdr, width, height, file_size, bl_compatibility_id, audio_codecs, audio_channels, subtitles, max_cll, max_fall, fps, aspect_ratio, imdb_id, tvdb_id, tmdb_id, rotten_id, metacritic_id, trakt_id, imdb_rating, tvdb_rating, tmdb_rating, rotten_rating, metacritic_rating, trakt_rating, nfo_missing FROM videos WHERE {where_clause} ORDER BY {db_sort} {order}{limit_clause}",
            params + limit_params
        ).fetchall()
    
    # Convert rows to list of dictionaries
    data = []
    for row in rows:
        data.append({
            'filename': row[0], 'category': row[1], 'profile': row[2], 'el_type': row[3],
            'container': row[4], 'source_vol': row[5], 'full_path': row[6], 'last_scanned': row[7],
            'resolution': row[8], 'bitrate_mbps': row[9], 'scan_error': row[10], 'is_hybrid': row[11],
            'is_source_hybrid': row[12], 'secondary_hdr': row[13], 'width': row[14], 'height': row[15], 'file_size': row[16],
            'bl_compatibility_id': row[17], 'audio_codecs': row[18], 'audio_channels': row[19], 'subtitles': row[20],
            'max_cll': row[21], 'max_fall': row[22], 'fps': row[23], 'aspect_ratio': row[24],
            'imdb_id': row[25], 'tvdb_id': row[26], 'tmdb_id': row[27], 'rotten_id': row[28], 'metacritic_id': row[29], 'trakt_id': row[30],
            'imdb_rating': row[31], 'tvdb_rating': row[32], 'tmdb_rating': row[33], 'rotten_rating': row[34], 'metacritic_rating': row[35], 'trakt_rating': row[36],
            'nfo_missing': row[37]
        })
    
    json_str = json.dumps(data, indent=2, ensure_ascii=False)
    return make_response(json_str, 200, {"Content-Disposition": "attachment; filename=media_export.json", "Content-type": "application/json"})

@app.route('/api/backup', methods=['POST'])
def backup_database() -> Response:
    """
    Create a backup of the database and settings.
    
    Returns:
        ZIP file download response containing database and settings backup
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"video_analyzer_backup_{timestamp}.zip"
        backup_path = os.path.join(OUTPUT_DIR, backup_filename)
        
        # Create ZIP file
        with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Backup database
            if os.path.exists(DB_PATH):
                zipf.write(DB_PATH, os.path.basename(DB_PATH))
            
            # Backup settings from database
            settings_backup = {}
            try:
                with get_db() as conn:
                    rows = conn.execute("SELECT key, value FROM settings").fetchall()
                    settings_backup = {row[0]: row[1] for row in rows}
            except sqlite3.Error as e:
                log_debug(f"Error reading settings for backup: {e}", "WARNING")
            
            # Write settings as JSON to ZIP
            settings_json = json.dumps(settings_backup, indent=2)
            zipf.writestr("settings.json", settings_json)
        
        # Return file for download
        return send_file(backup_path, as_attachment=True, download_name=backup_filename, mimetype='application/zip')
    except Exception as e:
        log_debug(f"Backup failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/restore', methods=['POST'])
def restore_database() -> Response:
    """
    Restore database and settings from a backup file.
    
    Expects a ZIP file upload containing:
    - processed_videos.db (database file)
    - settings.json (settings as JSON)
    
    Returns:
        JSON response with status and message
    """
    try:
        if 'file' not in request.files:
            return jsonify({"status": "error", "message": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"status": "error", "message": "No file selected"}), 400
        
        # Validate file extension
        if not file.filename.lower().endswith('.zip'):
            return jsonify({"status": "error", "message": "Invalid file type. Only ZIP files are supported"}), 400
        
        # Save uploaded file temporarily
        temp_zip = os.path.join(OUTPUT_DIR, f"restore_temp_{uuid.uuid4().hex[:8]}.zip")
        file.save(temp_zip)
        
        try:
            # Extract and validate ZIP contents
            db_restored = False
            settings_restored = False
            
            with zipfile.ZipFile(temp_zip, 'r') as zipf:
                file_list = zipf.namelist()
                
                # Restore database
                db_basename = os.path.basename(DB_PATH)
                if db_basename in file_list:
                    # Backup current database first
                    if os.path.exists(DB_PATH):
                        backup_old = DB_PATH + f".pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        shutil.copy2(DB_PATH, backup_old)
                    
                    # Extract database
                    zipf.extract(db_basename, OUTPUT_DIR)
                    extracted_db = os.path.join(OUTPUT_DIR, db_basename)
                    if os.path.exists(extracted_db) and extracted_db != DB_PATH:
                        shutil.move(extracted_db, DB_PATH)
                    db_restored = True
                
                # Restore settings
                if 'settings.json' in file_list:
                    settings_data = zipf.read('settings.json').decode('utf-8')
                    settings_dict = json.loads(settings_data)
                    
                    # Update settings in database
                    with get_db() as conn:
                        for key, value in settings_dict.items():
                            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
                        conn.commit()
                    settings_restored = True
            
            # Clean up temp file
            os.remove(temp_zip)
            
            if db_restored and settings_restored:
                return jsonify({"status": "success", "message": "Database and settings restored successfully"}), 200
            elif db_restored:
                return jsonify({"status": "success", "message": "Database restored successfully (settings not found in backup)"}), 200
            elif settings_restored:
                return jsonify({"status": "success", "message": "Settings restored successfully (database not found in backup)"}), 200
            else:
                return jsonify({"status": "error", "message": "Backup file does not contain database or settings"}), 400
                
        except zipfile.BadZipFile:
            os.remove(temp_zip)
            return jsonify({"status": "error", "message": "Invalid ZIP file format"}), 400
        except json.JSONDecodeError:
            os.remove(temp_zip)
            return jsonify({"status": "error", "message": "Invalid settings.json format"}), 400
        except Exception as e:
            if os.path.exists(temp_zip):
                os.remove(temp_zip)
            log_debug(f"Restore failed: {e}", "ERROR")
            return jsonify({"status": "error", "message": f"Restore failed: {str(e)}"}), 500
            
    except Exception as e:
        log_debug(f"Restore failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/filter_presets', methods=['GET'])
def get_filter_presets() -> Response:
    """
    Get all saved filter presets.
    
    Returns:
        JSON object with preset names as keys and filter configurations as values
    """
    try:
        with get_db() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key='filter_presets'").fetchone()
            if row:
                presets = json.loads(row[0])
                return jsonify(presets), 200
            else:
                return jsonify({}), 200
    except json.JSONDecodeError:
        return jsonify({}), 200
    except Exception as e:
        log_debug(f"Failed to get filter presets: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/filter_presets', methods=['POST'])
def save_filter_preset() -> Response:
    """
    Save a filter preset.
    
    Expects JSON body:
    {
        "name": "preset_name",
        "filters": { ... filter configuration ... }
    }
    
    Returns:
        JSON response with status and message
    """
    try:
        data = request.get_json()
        if not data or 'name' not in data or 'filters' not in data:
            return jsonify({"status": "error", "message": "Missing 'name' or 'filters' in request"}), 400
        
        preset_name = data['name'].strip()
        if not preset_name:
            return jsonify({"status": "error", "message": "Preset name cannot be empty"}), 400
        
        preset_filters = data['filters']
        
        # Load existing presets
        with get_db() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key='filter_presets'").fetchone()
            if row:
                presets = json.loads(row[0])
            else:
                presets = {}
            
            # Add or update preset
            presets[preset_name] = preset_filters
            
            # Save back to database
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", 
                        ('filter_presets', json.dumps(presets)))
            conn.commit()
        
        return jsonify({"status": "success", "message": f"Preset '{preset_name}' saved successfully"}), 200
    except json.JSONDecodeError:
        return jsonify({"status": "error", "message": "Invalid JSON format"}), 400
    except Exception as e:
        log_debug(f"Failed to save filter preset: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/filter_presets/<preset_name>', methods=['DELETE'])
def delete_filter_preset(preset_name: str) -> Response:
    """
    Delete a filter preset.
    
    Args:
        preset_name: Name of the preset to delete
    
    Returns:
        JSON response with status and message
    """
    try:
        # Load existing presets
        with get_db() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key='filter_presets'").fetchone()
            if not row:
                return jsonify({"status": "error", "message": "No presets found"}), 404
            
            presets = json.loads(row[0])
            if preset_name not in presets:
                return jsonify({"status": "error", "message": f"Preset '{preset_name}' not found"}), 404
            
            # Remove preset
            del presets[preset_name]
            
            # Save back to database
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", 
                        ('filter_presets', json.dumps(presets)))
            conn.commit()
        
        return jsonify({"status": "success", "message": f"Preset '{preset_name}' deleted successfully"}), 200
    except json.JSONDecodeError:
        return jsonify({"status": "error", "message": "Invalid preset data format"}), 500
    except Exception as e:
        log_debug(f"Failed to delete filter preset: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/videos')
def get_videos() -> Response:
    """
    Get paginated video records with filtering, sorting, and statistics.
    
    Supports pagination, multiple filter types, sorting, and returns
    comprehensive statistics and filter options for the UI.
    
    Query Parameters:
        page: Page number (default: 1)
        per_page: Items per page (default: 100)
        sort: Sort column key (default: 'last_scanned')
        order: Sort order 'asc' or 'desc' (default: 'desc')
        Various filter parameters (category, profile, volume, etc.)
        
    Returns:
        JSON response with rows, stats, pagination info, and filter options
    """
    ensure_video_column('is_source_hybrid', 'INTEGER DEFAULT 0')
    main_where, main_params = build_filter_query(request.args)
    page = int(request.args.get('page', 1)); per_page = int(request.args.get('per_page', 100))
    sort_map = {'file': 'filename', 'hybrid': 'is_hybrid', 'source_hybrid': 'is_source_hybrid', 'main': 'category', 'prof': 'profile', 'el': 'el_type', 'sec': 'secondary_hdr', 'res': 'resolution', 'bit': 'bitrate_mbps', 'vol': 'source_vol', 'cont': 'container', 'scan': 'last_scanned', 'stat': 'scan_error', 'size': 'file_size', 'video_source': 'video_source', 'source_format': 'source_format', 'video_codec': 'video_codec', 'is_3d': 'is_3d', 'edition': 'edition', 'year': 'year', 'media_type': 'media_type', 'show_title': 'show_title', 'season': 'season', 'episode': 'episode', 'movie_title': 'movie_title', 'episode_title': 'episode_title', 'cll': 'max_cll', 'fall': 'max_fall'}
    db_sort = sort_map.get(request.args.get('sort'), 'last_scanned'); order = request.args.get('order', 'desc')

    with get_db_readonly() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {main_where}", main_params).fetchone()[0]
        rows = conn.execute(f"SELECT filename, category, profile, el_type, container, source_vol, full_path, last_scanned, resolution, bitrate_mbps, scan_error, is_hybrid, is_source_hybrid, secondary_hdr, width, height, file_size, bl_compatibility_id, audio_codecs, audio_langs, audio_channels, subtitles, max_cll, max_fall, video_source, source_format, video_codec, is_3d, edition, year, media_type, show_title, season, episode, movie_title, episode_title, nfo_missing, fps, aspect_ratio, imdb_id, tvdb_id, tmdb_id, rotten_id, metacritic_id, trakt_id, imdb_rating, tvdb_rating, tmdb_rating, rotten_rating, metacritic_rating, trakt_rating FROM videos WHERE {main_where} ORDER BY {db_sort} {order} LIMIT ? OFFSET ?", main_params + [per_page, (page-1)*per_page]).fetchall()
        global API_LOG_TS
        now = time.time()
        if PROGRESS.get("status") == "scanning" and now - API_LOG_TS >= 5:
            log_debug(
                f"[API_VIDEOS] total={total} rows={len(rows)} page={page} per_page={per_page}",
                "INFO"
            )
            API_LOG_TS = now
        
        stats_total_raw = conn.execute("SELECT category, profile, el_type, resolution, source_vol, scan_error, is_hybrid, is_source_hybrid, secondary_hdr FROM videos").fetchall()
        stats_filtered_raw = conn.execute(f"SELECT category, profile, el_type, resolution, source_vol, scan_error, is_hybrid, is_source_hybrid, secondary_hdr FROM videos WHERE {main_where}", main_params).fetchall()
        stats = _build_stats_from_rows(stats_total_raw)
        stats_filtered = _build_stats_from_rows(stats_filtered_raw)

        where_cache: dict[str | None, tuple[str, list[Any]]] = {}

        def get_where(exclude_key: str | None):
            if exclude_key in where_cache:
                return where_cache[exclude_key]
            w, p = build_filter_query(request.args, exclude_key=exclude_key)
            where_cache[exclude_key] = (w, p)
            return w, p

        def get_cnt(col, key):
            w, p = get_where(key)
            return {r[0]: r[1] for r in conn.execute(f"SELECT {col}, COUNT(*) FROM videos WHERE {col} != '' AND {col} IS NOT NULL AND {w} GROUP BY {col}", p).fetchall()}

        def get_cnt_with_where(col, where_clause, params):
            clause = f"{col} != '' AND {col} IS NOT NULL"
            if where_clause:
                clause = f"{clause} AND {where_clause}"
            return {r[0]: r[1] for r in conn.execute(f"SELECT {col}, COUNT(*) FROM videos WHERE {clause} GROUP BY {col}", params).fetchall()}

        def get_secondary_counts(where_clause, params):
            clause = where_clause or "1=1"
            result = {}
            for key, val in conn.execute(f"SELECT secondary_hdr, COUNT(*) FROM videos WHERE {clause} GROUP BY secondary_hdr", params).fetchall():
                label = key if key else 'none'
                result[label] = val
            return result

        def get_blank_cnt(col, key):
            w, p = get_where(key)
            return conn.execute(f"SELECT COUNT(*) FROM videos WHERE ({col} IS NULL OR {col} = '') AND {w}", p).fetchone()[0]
        
        def get_audio_codecs(key):
            """Get unique audio codecs, splitting comma-separated values"""
            w, p = get_where(key)
            rows = conn.execute(f"SELECT audio_codecs FROM videos WHERE audio_codecs != '' AND audio_codecs IS NOT NULL AND {w}", p).fetchall()
            codec_counts = {}
            for row in rows:
                if row[0]:
                    # Split by comma and process each codec
                    codecs = [c.strip() for c in row[0].split(',')]
                    for codec in codecs:
                        if codec:
                            codec_counts[codec] = codec_counts.get(codec, 0) + 1
            return codec_counts

        def get_path_counts(where_clause: str, params: list[Any]) -> tuple[list[str], list[int]]:
            try:
                row = conn.execute("SELECT value FROM settings WHERE key='scan_folders'").fetchone()
                folders = json.loads(row[0]) if row and row[0] else []
            except Exception:
                folders = []
            if isinstance(folders, dict):
                folders = [folders]
            if not isinstance(folders, list):
                folders = []
            labels: list[str] = []
            counts: list[int] = []
            for f in folders or []:
                if not isinstance(f, dict):
                    continue
                if f.get('muted'):
                    continue
                vol = (f.get('volume') or '').strip()
                path = (f.get('path') or '').strip()
                if not vol:
                    continue
                label = f"{vol}{'/' + path if path else ''}"
                labels.append(label)
                if path:
                    normalized = path.replace('\\', '/').strip('/')
                    prefix = f"/{vol}/{normalized}"
                    like_pattern = f"%{prefix}%"
                    count = conn.execute(
                        f"SELECT COUNT(*) FROM videos WHERE {where_clause} AND source_vol = ? AND (full_path LIKE ? OR REPLACE(full_path, '\\\\', '/') LIKE ?)",
                        params + [vol, like_pattern, like_pattern]
                    ).fetchone()[0]
                else:
                    count = conn.execute(
                        f"SELECT COUNT(*) FROM videos WHERE {where_clause} AND source_vol = ?",
                        params + [vol]
                    ).fetchone()[0]
                counts.append(count)
            return labels, counts
        
        cnt_vol_total = get_cnt_with_where('source_vol', None, [])
        cnt_res_total = get_cnt_with_where('resolution', None, [])
        stats['vol_labels'] = list(cnt_vol_total.keys()); stats['vol_data'] = list(cnt_vol_total.values())
        stats['res_labels'] = list(cnt_res_total.keys()); stats['res_data'] = list(cnt_res_total.values())
        stats['secondary_hdrs'] = get_secondary_counts(None, [])
        stats['last_scan_time'] = PROGRESS["last_duration"]
        stats['path_labels'], stats['path_data'] = get_path_counts("1=1", [])

        cnt_vol_filtered = get_cnt_with_where('source_vol', main_where, main_params)
        cnt_res_filtered = get_cnt_with_where('resolution', main_where, main_params)
        stats_filtered['vol_labels'] = list(cnt_vol_filtered.keys()); stats_filtered['vol_data'] = list(cnt_vol_filtered.values())
        stats_filtered['res_labels'] = list(cnt_res_filtered.keys()); stats_filtered['res_data'] = list(cnt_res_filtered.values())
        stats_filtered['secondary_hdrs'] = get_secondary_counts(main_where, main_params)
        stats_filtered['path_labels'], stats_filtered['path_data'] = get_path_counts(main_where, main_params)
        cnt_vol = get_cnt('source_vol', 'volume')
        cnt_res = get_cnt('resolution', 'resolution')

        w_status, p_status = build_filter_query(request.args, exclude_key='status')
        failed_cnt = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {w_status} AND scan_error IS NOT NULL AND scan_error != ''", p_status).fetchone()[0]
        ok_cnt = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {w_status} AND (scan_error IS NULL OR scan_error = '')", p_status).fetchone()[0]
        w_hyb, p_hyb = build_filter_query(request.args, exclude_key='is_hybrid')
        hyb_yes = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {w_hyb} AND is_hybrid = 1", p_hyb).fetchone()[0]
        hyb_no = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {w_hyb} AND is_hybrid = 0 AND category != 'sdr_only'", p_hyb).fetchone()[0]
        w_src_hyb, p_src_hyb = build_filter_query(request.args, exclude_key='source_hybrid')
        src_hyb_yes = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {w_src_hyb} AND is_source_hybrid = 1", p_src_hyb).fetchone()[0]
        src_hyb_no = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {w_src_hyb} AND is_source_hybrid = 0", p_src_hyb).fetchone()[0]
        w_3d, p_3d = build_filter_query(request.args, exclude_key='is_3d')
        d3d_yes = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {w_3d} AND is_3d = 1", p_3d).fetchone()[0]
        d3d_no = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {w_3d} AND is_3d = 0", p_3d).fetchone()[0]

        opts = {
            'categories': get_cnt('category', 'category'), 'profiles': get_cnt('profile', 'profile'),
            'el_types': get_cnt('el_type', 'el'), 'containers': get_cnt('container', 'container'),
            'volumes': cnt_vol, 'resolutions': cnt_res,
            'secondary_hdrs': get_cnt('secondary_hdr', 'secondary_hdr'),
            'audio_codecs': get_audio_codecs('audio'),
            'video_sources': get_cnt('video_source', 'video_source'),
            'source_formats': get_cnt('source_format', 'source_format'),
            'video_codecs': get_cnt('video_codec', 'video_codec'),
            'editions': get_cnt('edition', 'edition'),
            'media_types': get_cnt('media_type', 'media_type'),
            'blank_counts': {
                'category': get_blank_cnt('category', 'category'),
                'profile': get_blank_cnt('profile', 'profile'),
                'el': get_blank_cnt('el_type', 'el'),
                'container': get_blank_cnt('container', 'container'),
                'volume': get_blank_cnt('source_vol', 'volume'),
                'resolution': get_blank_cnt('resolution', 'resolution'),
                'secondary_hdr': get_blank_cnt('secondary_hdr', 'secondary_hdr'),
                'audio': get_blank_cnt('audio_codecs', 'audio'),
                'video_source': get_blank_cnt('video_source', 'video_source'),
                'source_format': get_blank_cnt('source_format', 'source_format'),
                'video_codec': get_blank_cnt('video_codec', 'video_codec'),
                'edition': get_blank_cnt('edition', 'edition'),
                'media_type': get_blank_cnt('media_type', 'media_type')
            },
            'special_hybrid': {'1': hyb_yes, '0': hyb_no}, 
            'special_source_hybrid': {'1': src_hyb_yes, '0': src_hyb_no},
            'special_status': {'ok': ok_cnt, 'failed': failed_cnt},
            'special_is_3d': {'1': d3d_yes, '0': d3d_no}
        }
        return jsonify({"rows": [list(r) for r in rows], "stats": stats, "stats_filtered": stats_filtered, "page": page, "total_items": total, "total_pages": (total + per_page - 1) // per_page, "filter_options": opts})


@app.route('/api/filter_paths', methods=['POST'])
def filter_paths() -> Response:
    """
    Return full_path list for current filters.
    """
    payload = request.get_json(silent=True) or {}
    filters = payload.get('filters') or {}
    where_clause, params = build_filter_query(filters)
    with get_db_readonly() as conn:
        rows = conn.execute(f"SELECT full_path FROM videos WHERE {where_clause}", params).fetchall()
    return jsonify({"paths": [r[0] for r in rows]})


@app.route('/api/rescan_file', methods=['POST'])
def rescan_file() -> Response:
    """
    Rescan a single file and update its database entry.
    """
    try:
        payload = request.get_json(silent=True) or {}
        full_path = payload.get('full_path')
        if not full_path:
            return jsonify({"status": "error", "message": "Missing full_path"}), 400

        res = scan_file_worker(pathlib.Path(full_path))
        save_batch_to_db([res])
        return jsonify({"status": "ok"})
    except Exception as e:
        log_debug(f"Rescan failed for {payload.get('full_path')}: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update_media_type', methods=['POST'])
def update_media_type() -> Response:
    """
    Update media_type for a single file.
    """
    try:
        payload = request.get_json(silent=True) or {}
        full_path = payload.get('full_path')
        media_type = (payload.get('media_type') or '').strip().lower()
        if not full_path:
            return jsonify({"status": "error", "message": "Missing full_path"}), 400
        if media_type not in ('movie', 'tv'):
            media_type = None
        with get_db() as conn:
            conn.execute("UPDATE videos SET media_type=? WHERE full_path=?", (media_type, full_path))
            update_validation_flag_for_path(conn, full_path)
        return jsonify({"status": "ok"})
    except Exception as e:
        log_debug(f"Update media_type failed for {payload.get('full_path')}: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update_metadata', methods=['POST'])
def update_metadata() -> Response:
    """
    Update show/movie metadata fields for a single file.
    """
    try:
        payload = request.get_json(silent=True) or {}
        full_path = payload.get('full_path')
        if not full_path:
            return jsonify({"status": "error", "message": "Missing full_path"}), 400
        text_fields = {
            'show_title': 'show_title',
            'episode_title': 'episode_title',
            'movie_title': 'movie_title',
            'video_source': 'video_source',
            'source_format': 'source_format',
            'category': 'category',
            'secondary_hdr': 'secondary_hdr'
        }
        int_fields = {
            'season': 'season',
            'episode': 'episode',
            'year': 'year'
        }
        updates = []
        params = []

        for key in text_fields:
            if key in payload:
                val = (payload.get(key) or '').strip() or None
                updates.append(f"{text_fields[key]}=?")
                params.append(val)

        for key in int_fields:
            if key in payload:
                raw_val = payload.get(key)
                try:
                    val = int(raw_val) if raw_val is not None and raw_val != '' else None
                except (ValueError, TypeError):
                    val = None
                updates.append(f"{int_fields[key]}=?")
                params.append(val)

        if not updates:
            return jsonify({"status": "ok"})

        with get_db() as conn:
            conn.execute(
                f"UPDATE videos SET {', '.join(updates)} WHERE full_path=?",
                params + [full_path]
            )
            update_validation_flag_for_path(conn, full_path)
        return jsonify({"status": "ok"})
    except Exception as e:
        log_debug(f"Update metadata failed for {payload.get('full_path')}: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/bulk_update_metadata', methods=['POST'])
def bulk_update_metadata() -> Response:
    """
    Update metadata fields (and optional media_type) for multiple files.
    """
    try:
        payload = request.get_json(silent=True) or {}
        paths = payload.get('paths') or []
        updates_payload = payload.get('updates') or {}
        media_type = (payload.get('media_type') or '').strip().lower()
        if not isinstance(paths, list) or not paths:
            return jsonify({"status": "error", "message": "Missing paths"}), 400

        text_fields = {
            'show_title': 'show_title',
            'episode_title': 'episode_title',
            'movie_title': 'movie_title',
            'video_source': 'video_source',
            'source_format': 'source_format',
            'category': 'category',
            'secondary_hdr': 'secondary_hdr'
        }
        int_fields = {
            'season': 'season',
            'episode': 'episode',
            'year': 'year'
        }

        updates = []
        params = []
        for key in text_fields:
            if key in updates_payload:
                val = (updates_payload.get(key) or '').strip() or None
                updates.append(f"{text_fields[key]}=?")
                params.append(val)

        for key in int_fields:
            if key in updates_payload:
                raw_val = updates_payload.get(key)
                try:
                    val = int(raw_val) if raw_val is not None and raw_val != '' else None
                except (ValueError, TypeError):
                    val = None
                updates.append(f"{int_fields[key]}=?")
                params.append(val)

        if media_type:
            if media_type not in ('movie', 'tv'):
                media_type = None
            updates.append("media_type=?")
            params.append(media_type)
        elif 'media_type' in payload:
            updates.append("media_type=?")
            params.append(None)

        if not updates:
            return jsonify({"status": "ok", "updated": 0})

        updated = 0
        with get_db() as conn:
            for full_path in paths:
                conn.execute(
                    f"UPDATE videos SET {', '.join(updates)} WHERE full_path=?",
                    params + [full_path]
                )
                update_validation_flag_for_path(conn, full_path)
                updated += 1
        return jsonify({"status": "ok", "updated": updated})
    except Exception as e:
        log_debug(f"Bulk update metadata failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/backfill_metadata', methods=['POST'])
def backfill_metadata() -> Response:
    """
    Backfill missing metadata fields using .nfo and filename heuristics.
    """
    try:
        with progress_lock:
            if PROGRESS.get("status") == "scanning":
                return jsonify({"status": "error", "message": "Scan in progress"}), 409
        payload = request.get_json(silent=True) or {}
        fill_blanks_only = payload.get('fill_blanks_only', True)
        updated = 0
        with get_db() as conn:
            rows = conn.execute(
                """SELECT full_path, filename, media_type, show_title, episode_title, season, episode, movie_title, year,
                          imdb_id, tvdb_id, tmdb_id, rotten_id, metacritic_id, trakt_id,
                          imdb_rating, tvdb_rating, tmdb_rating, rotten_rating, metacritic_rating, trakt_rating
                   FROM videos
                   WHERE media_type IS NULL OR media_type = ''
                      OR show_title IS NULL OR show_title = ''
                      OR episode_title IS NULL OR episode_title = ''
                      OR movie_title IS NULL OR movie_title = ''
                      OR season IS NULL OR episode IS NULL OR year IS NULL
                      OR imdb_id IS NULL OR imdb_id = ''
                      OR tvdb_id IS NULL OR tvdb_id = ''
                      OR tmdb_id IS NULL OR tmdb_id = ''
                      OR rotten_id IS NULL OR rotten_id = ''
                      OR metacritic_id IS NULL OR metacritic_id = ''
                      OR trakt_id IS NULL OR trakt_id = ''
                      OR imdb_rating IS NULL OR tvdb_rating IS NULL OR tmdb_rating IS NULL
                      OR rotten_rating IS NULL OR metacritic_rating IS NULL OR trakt_rating IS NULL"""
            ).fetchall()
            total = len(rows)
            with progress_lock:
                PROGRESS.update({
                    "status": "scanning",
                    "current": 0,
                    "total": total,
                    "file": "Backfilling metadata...",
                    "start_time": time.time(),
                    "last_duration": "0s",
                    "eta": ""
                })
            log_debug(f"[BACKFILL] Starting backfill for {total} files (blanks_only={fill_blanks_only})", "INFO")
            for row in rows:
                current = {
                    'media_type': row['media_type'],
                    'show_title': row['show_title'],
                    'episode_title': row['episode_title'],
                    'season': row['season'],
                    'episode': row['episode'],
                    'movie_title': row['movie_title'],
                    'year': row['year'],
                    'imdb_id': row['imdb_id'],
                    'tvdb_id': row['tvdb_id'],
                    'tmdb_id': row['tmdb_id'],
                    'rotten_id': row['rotten_id'],
                    'metacritic_id': row['metacritic_id'],
                    'trakt_id': row['trakt_id'],
                    'imdb_rating': row['imdb_rating'],
                    'tvdb_rating': row['tvdb_rating'],
                    'tmdb_rating': row['tmdb_rating'],
                    'rotten_rating': row['rotten_rating'],
                    'metacritic_rating': row['metacritic_rating'],
                    'trakt_rating': row['trakt_rating']
                }
                updates = build_backfill_metadata(row['full_path'], row['filename'], current)
                if not updates:
                    continue
                if fill_blanks_only:
                    updates = {k: v for k, v in updates.items() if v is not None and (current.get(k) is None or current.get(k) == '')}
                if not updates:
                    continue
                new_media_type = updates.get('media_type', current.get('media_type'))
                new_show_title = updates.get('show_title', current.get('show_title'))
                new_episode_title = updates.get('episode_title', current.get('episode_title'))
                new_season = updates.get('season', current.get('season'))
                new_episode = updates.get('episode', current.get('episode'))
                new_movie_title = updates.get('movie_title', current.get('movie_title'))
                new_year = updates.get('year', current.get('year'))
                new_imdb_id = updates.get('imdb_id', current.get('imdb_id'))
                new_tvdb_id = updates.get('tvdb_id', current.get('tvdb_id'))
                new_tmdb_id = updates.get('tmdb_id', current.get('tmdb_id'))
                new_rotten_id = updates.get('rotten_id', current.get('rotten_id'))
                new_metacritic_id = updates.get('metacritic_id', current.get('metacritic_id'))
                new_trakt_id = updates.get('trakt_id', current.get('trakt_id'))
                new_imdb_rating = updates.get('imdb_rating', current.get('imdb_rating'))
                new_tvdb_rating = updates.get('tvdb_rating', current.get('tvdb_rating'))
                new_tmdb_rating = updates.get('tmdb_rating', current.get('tmdb_rating'))
                new_rotten_rating = updates.get('rotten_rating', current.get('rotten_rating'))
                new_metacritic_rating = updates.get('metacritic_rating', current.get('metacritic_rating'))
                new_trakt_rating = updates.get('trakt_rating', current.get('trakt_rating'))
                validation_flag = compute_validation_flag({
                    "media_type": new_media_type,
                    "show_title": new_show_title,
                    "episode_title": new_episode_title,
                    "movie_title": new_movie_title,
                    "season": new_season,
                    "episode": new_episode
                })
                conn.execute(
                    "UPDATE videos SET media_type=?, show_title=?, episode_title=?, season=?, episode=?, movie_title=?, year=?, imdb_id=?, tvdb_id=?, tmdb_id=?, rotten_id=?, metacritic_id=?, trakt_id=?, imdb_rating=?, tvdb_rating=?, tmdb_rating=?, rotten_rating=?, metacritic_rating=?, trakt_rating=?, validation_flag=? WHERE full_path=?",
                    (
                        new_media_type,
                        new_show_title,
                        new_episode_title,
                        new_season,
                        new_episode,
                        new_movie_title,
                        new_year,
                        new_imdb_id,
                        new_tvdb_id,
                        new_tmdb_id,
                        new_rotten_id,
                        new_metacritic_id,
                        new_trakt_id,
                        new_imdb_rating,
                        new_tvdb_rating,
                        new_tmdb_rating,
                        new_rotten_rating,
                        new_metacritic_rating,
                        new_trakt_rating,
                        validation_flag,
                        row['full_path']
                    )
                )
                updated += 1
                with progress_lock:
                    PROGRESS["current"] += 1
                    PROGRESS["file"] = f"Backfilling: {row['filename']}"
                    elapsed = int(time.time() - PROGRESS.get("start_time", time.time()))
                    PROGRESS["last_duration"] = f"{elapsed}s"
                    if PROGRESS["current"] > 0 and PROGRESS["total"] > 0 and elapsed > 0:
                        rate = PROGRESS["current"] / elapsed
                        remaining = PROGRESS["total"] - PROGRESS["current"]
                        eta_seconds = int(remaining / rate) if rate > 0 else 0
                        PROGRESS["eta"] = f"{eta_seconds}s" if eta_seconds > 0 else "calculating..."
            log_debug(f"[BACKFILL] Completed. Updated {updated} files.", "INFO")
            with progress_lock:
                PROGRESS.update({"status": "idle", "current": 0, "total": 0, "file": "Waiting...", "eta": ""})
        return jsonify({"status": "ok", "updated": updated})
    except Exception as e:
        log_debug(f"Backfill metadata failed: {e}", "ERROR")
        with progress_lock:
            PROGRESS.update({"status": "idle", "current": 0, "total": 0, "file": "Waiting...", "eta": ""})
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/delete', methods=['POST'])
def delete_files() -> Response:
    """
    Delete video records from the database.
    
    Supports deleting specific files by path or deleting all files matching
    the provided filter criteria.
    
    Request Body:
        paths: List of file paths to delete (optional)
        delete_all_filter: If True, delete all files matching filters (optional)
        filters: Filter criteria for bulk deletion (optional)
        
    Returns:
        JSON response with deletion status and count of deleted records
    """
    data = request.json; paths = data.get('paths', []); delete_all = data.get('delete_all_filter', False)
    with get_db() as conn:
        count = 0
        if delete_all:
            w, p = build_filter_query(data.get('filters', {}))
            count = conn.execute(f"DELETE FROM videos WHERE {w}", p).rowcount
        else:
            for p in paths: conn.execute("DELETE FROM videos WHERE full_path = ?", (p,)); count += 1
    return jsonify({"status": "deleted", "count": count})

@app.route('/api/settings', methods=['GET', 'POST'])
def handle_settings() -> Response:
    """
    Get or update application settings.
    
    GET: Returns all current settings as JSON
    POST: Updates specified settings and optionally configures scheduled scans
    
    Request Body (POST):
        mode: Scan schedule mode ('daily', 'interval', or 'manual')
        value: Schedule value (time for daily, hours for interval)
        threads: Number of worker threads
        skip_words: Comma-separated words to skip in filenames
        min_size_mb: Minimum file size in MB
        batch_size: Database batch insert size
        And other settings...
        
    Returns:
        JSON response with settings (GET) or status (POST)
    """
    if request.method == 'POST':
        d = request.json
        try:
            with get_db() as conn:
                if 'mode' in d:
                    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('scan_mode', ?)", (d['mode'],))
                    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('scan_value', ?)", (d['value'],))
                    if scheduler: scheduler.remove_all_jobs()
                    if scheduler and d['mode'] == 'daily': h, m = d['value'].split(':'); scheduler.add_job(run_scan, 'cron', hour=h, minute=m)
                    elif scheduler and d['mode'] == 'interval': scheduler.add_job(run_scan, 'interval', hours=int(d['value']))
                if 'threads' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('threads', ?)", (str(d['threads']),))
                if 'skip_words' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('skip_words', ?)", (d['skip_words'],))
                if 'min_size_mb' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('min_size_mb', ?)", (str(d['min_size_mb']),))
                if 'log_limit' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('log_limit', ?)", (str(d['log_limit']),))
                if 'debug_mode' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('debug_mode', ?)", (str(d['debug_mode']).lower(),))
                if 'refresh_interval' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('refresh_interval', ?)", (str(d['refresh_interval']),))
                if 'visible_cols' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('visible_cols', ?)", (d['visible_cols'],))
                if 'column_widths' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('column_widths', ?)", (d['column_widths'],))
                if 'sort_order' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('sort_order', ?)", (d['sort_order'],))
                if 'notif_style' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('notif_style', ?)", (d['notif_style'],))
                if 'batch_size' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('batch_size', ?)", (str(d['batch_size']),))
                if 'rpu_fel_threshold' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('rpu_fel_threshold', ?)", (str(d['rpu_fel_threshold']),))
                if 'force_rescan' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('force_rescan', ?)", (str(d['force_rescan']).lower(),))
                if 'column_order' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('column_order', ?)", (d['column_order'],))
                for key, value in d.items():
                    if key.startswith(('visible_cols_', 'column_order_', 'column_widths_')):
                        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
                if 'scan_folders' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('scan_folders', ?)", (d['scan_folders'],))
                if 'scan_extras' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('scan_extras', ?)", (str(d['scan_extras']).lower(),))
            return jsonify({"status": "success"})
        except Exception as e:
            import traceback
            log_debug(f"Settings save failed: {e}", "ERROR")
            log_debug(traceback.format_exc(), "ERROR")
            return jsonify({"status": "error", "message": str(e)}), 500
    else:
        with get_db() as conn: res = dict(conn.execute("SELECT key, value FROM settings").fetchall())
        return jsonify(res)


@app.route('/api/browse', methods=['GET'])
def browse_volume() -> Response:
    """
    Browse directories within a mounted volume.
    """
    volume = (request.args.get('volume') or '').strip()
    rel_path = (request.args.get('path') or '').strip()
    mounts = get_mount_status()
    base = mounts.get(volume)
    if not base:
        return jsonify({"status": "error", "message": "Invalid volume"}), 400
    if rel_path:
        target = os.path.join(base, rel_path.lstrip('/\\'))
    else:
        target = base
    base_real = os.path.realpath(base)
    target_real = os.path.realpath(target)
    if not target_real.startswith(base_real):
        return jsonify({"status": "error", "message": "Invalid path"}), 400
    if not os.path.isdir(target_real):
        return jsonify({"status": "error", "message": "Path not found"}), 404
    try:
        dirs = sorted([
            d for d in os.listdir(target_real)
            if os.path.isdir(os.path.join(target_real, d)) and not d.startswith('.')
        ])
    except OSError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify({"status": "ok", "volume": volume, "path": rel_path, "dirs": dirs})


@app.route('/api/cleanup_db', methods=['POST'])
def cleanup_db() -> Response:
    """
    Remove DB entries for offline volumes or paths outside selected scan folders.
    """
    try:
        deleted = perform_cleanup_db(delete=True)
        return jsonify({"status": "ok", "deleted": deleted})
    except Exception as e:
        log_debug(f"Cleanup DB failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/cleanup_db_preview', methods=['GET'])
def cleanup_db_preview() -> Response:
    """
    Preview count of DB entries that would be removed.
    """
    try:
        count = perform_cleanup_db(delete=False)
        return jsonify({"status": "ok", "count": count})
    except Exception as e:
        log_debug(f"Cleanup DB preview failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500


def perform_cleanup_db(delete: bool) -> int:
    mounts = get_mount_status()
    online_vols = set(mounts.keys())
    with get_db() as conn:
        settings = dict(conn.execute("SELECT key, value FROM settings").fetchall())
        scan_folders = []
        try:
            scan_folders = json.loads(settings.get('scan_folders', '[]') or '[]')
        except (json.JSONDecodeError, TypeError):
            scan_folders = []
        allowed_bases = []
        if isinstance(scan_folders, list):
            for entry in scan_folders:
                if entry.get('muted'):
                    continue
                vol_name = (entry.get('volume') or '').strip()
                if not vol_name or vol_name not in mounts:
                    continue
                base = mounts.get(vol_name)
                rel_path = (entry.get('path') or '').strip()
                if rel_path:
                    candidate = rel_path
                    if not os.path.isabs(candidate):
                        candidate = os.path.join(base, rel_path.lstrip('/\\'))
                else:
                    candidate = base
                base_real = os.path.realpath(base)
                target_real = os.path.realpath(candidate)
                if target_real.startswith(base_real) and os.path.isdir(target_real):
                    allowed_bases.append(target_real)

        rows = conn.execute("SELECT full_path, source_vol FROM videos").fetchall()
        to_delete = []
        for row in rows:
            full_path = row["full_path"]
            vol = row["source_vol"]
            if vol not in online_vols:
                to_delete.append((full_path,))
                continue
            if allowed_bases:
                try:
                    real_path = os.path.realpath(full_path)
                except OSError:
                    to_delete.append((full_path,))
                    continue
                if not any(real_path.startswith(base) for base in allowed_bases):
                    to_delete.append((full_path,))
        if delete and to_delete:
            conn.executemany("DELETE FROM videos WHERE full_path=?", to_delete)
    return len(to_delete)


def update_validation_flag_for_path(conn: sqlite3.Connection, full_path: str) -> None:
    row = conn.execute(
        "SELECT media_type, show_title, episode_title, movie_title, season, episode FROM videos WHERE full_path=?",
        (full_path,)
    ).fetchone()
    if not row:
        return
    validation_flag = compute_validation_flag(dict(row))
    conn.execute("UPDATE videos SET validation_flag=? WHERE full_path=?", (validation_flag, full_path))

@app.route('/api/db/maintenance', methods=['POST'])
def db_maintenance() -> Response:
    """
    Run database maintenance operations (VACUUM and ANALYZE).
    Optimizes the database by reclaiming space and updating query statistics.
    
    Returns:
        JSON response with status and message
    """
    try:
        with get_db() as conn:
            log_debug("[DB_MAINT] Starting database maintenance (VACUUM)...", "INFO")
            conn.execute("VACUUM")
            log_debug("[DB_MAINT] VACUUM completed. Running ANALYZE...", "INFO")
            conn.execute("ANALYZE")
            log_debug("[DB_MAINT] Database maintenance completed successfully", "INFO")
        return jsonify({"status": "success", "message": "Database maintenance completed successfully"}), 200
    except Exception as e:
        log_debug(f"[DB_MAINT] Database maintenance failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/start', methods=['POST'])
def start() -> Tuple[Response, int] | Response:
    """
    Start a new scan with specified parameters.
    
    Request Body:
        targets: List of volume names to scan (optional, scans all if empty)
        threads: Number of worker threads (optional, default: 4)
        force_rescan: Force rescan of all files (optional, default: False)
        debug_mode: Enable debug logging (optional, default: False)
        
    Returns:
        JSON response with status "started" or "busy" (400 if already scanning)
    """
    targets = request.json.get('targets', [])
    threads = int(request.json.get('threads', 4))
    force = request.json.get('force_rescan', False)
    debug = request.json.get('debug_mode', False)
    scan_mode = (request.json.get('scan_mode') or 'all').lower()
    if scan_mode not in ('all', 'tv', 'movie'):
        scan_mode = 'all'
    scan_folder = request.json.get('scan_folder')
    if PROGRESS["status"] != "scanning":
        threading.Thread(target=run_scan, args=(threads, targets, force, debug, scan_mode, scan_folder), daemon=True).start()
        return jsonify({"status": "started"})
    return jsonify({"status": "busy"}), 400

@app.route('/abort', methods=['POST'])
def abort() -> Response:
    """
    Abort the currently running scan.
    
    Immediately sets ABORT_SCAN flag and kills all active subprocesses.
    """
    global ABORT_SCAN
    # Only log and process abort if a scan is actually running
    with progress_lock:
        is_scanning = PROGRESS.get("status") == "scanning"
    
    if not is_scanning:
        # If no scan is running, just return success without logging or setting ABORT_SCAN
        return jsonify({"status": "idle", "killed_processes": 0, "message": "No scan in progress"})
    
    log_debug("[ABORT] Abort requested by user", "INFO")
    ABORT_SCAN = True
    PAUSE_EVENT.set()
    
    # Immediately kill all active subprocesses
    killed_count = 0
    with proc_lock:
        active_procs = list(ACTIVE_PROCS)  # Create a copy to iterate over
        log_debug(f"[ABORT] Found {len(active_procs)} active subprocesses to kill", "INFO")
        for p in active_procs:
            try:
                log_debug(f"[ABORT] Killing subprocess PID {p.pid}", "INFO")
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                killed_count += 1
            except (OSError, ProcessLookupError, ValueError) as e:
                log_debug(f"[ABORT] Failed to kill process {p.pid}: {e}", "WARNING")
                # Try direct kill as fallback
                try:
                    p.kill()
                    killed_count += 1
                except (OSError, ProcessLookupError, ValueError):
                    pass
    
    log_debug(f"[ABORT] Abort acknowledged. Killed {killed_count} subprocesses. Scan will stop at next check.", "INFO")
    
    # Update PROGRESS immediately so UI reflects abort status
    with progress_lock:
        PROGRESS["file"] = "Aborting..."
        PROGRESS["paused"] = False
        # Don't change status to "idle" yet - let run_scan do that when it finishes
    
    return jsonify({"status": "aborting", "killed_processes": killed_count})

@app.route('/pause', methods=['POST'])
def toggle_pause():
    """Toggle pause/resume for the active scan."""
    with progress_lock:
        if PROGRESS.get("status") != "scanning":
            return jsonify({"status": "idle", "paused": False})
    if PAUSE_EVENT.is_set():
        PAUSE_EVENT.clear()
        with progress_lock:
            PROGRESS["paused"] = True
        return jsonify({"status": "paused", "paused": True})
    PAUSE_EVENT.set()
    with progress_lock:
        PROGRESS["paused"] = False
    return jsonify({"status": "scanning", "paused": False})

@app.route('/progress')
def get_progress():
    """Get current scan progress information."""
    with progress_lock: 
        d = PROGRESS.copy()
    return jsonify(d)

@app.route('/clear_completed', methods=['POST'])
def clear_completed():
    """Clear the scan completion flag after user acknowledges the result."""
    with progress_lock: 
        PROGRESS["scan_completed"] = False
    return jsonify({"status": "cleared"})

# Init DB on load
init_db()

# Cleanup leftover temp files on startup
cleanup_old_rpu_files()

if __name__ == "__main__": 
    app.run(host='0.0.0.0', port=6002)
