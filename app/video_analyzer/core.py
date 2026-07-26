from __future__ import annotations

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
import fnmatch
import hashlib
import signal
import tempfile
import zipfile
import shutil
import urllib.parse
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
import copy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from contextlib import contextmanager
from collections import OrderedDict

from flask import (
    Flask, render_template, jsonify, make_response, request, send_file, Response, Blueprint,
)

try:
    from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
    HAS_SCHEDULER = True
except ImportError:
    BackgroundScheduler = None  # type: ignore
    HAS_SCHEDULER = False

bp = Blueprint("main", __name__)

# --- begin migrated monolith ---
# --- CONFIGURATION ---
# Allow override for tests / alternate data dirs (default remains /output for Docker).
OUTPUT_DIR = (os.environ.get("VIDEO_ANALYZER_OUTPUT") or "").strip() or '/output'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # app/
LOCAL_OUTPUT_FALLBACK = os.path.join(BASE_DIR, 'results')
if not os.path.exists(OUTPUT_DIR) and os.path.exists(LOCAL_OUTPUT_FALLBACK):
    OUTPUT_DIR = LOCAL_OUTPUT_FALLBACK
DB_PATH = os.path.join(OUTPUT_DIR, 'processed_videos.db')
CHANGELOG_PATH = os.path.join(BASE_DIR, 'CHANGELOG.md')
if not os.path.exists(CHANGELOG_PATH):
    _changelog_alt = os.path.join(os.path.dirname(BASE_DIR), 'CHANGELOG.md')
    if os.path.exists(_changelog_alt):
        CHANGELOG_PATH = _changelog_alt
VIDEO_EXTENSIONS = {
    '.mkv', '.mp4', '.avi', '.mpeg', '.mpg', '.mov', '.ts', '.m2ts', '.webm', '.wmv',
    # Raw / bitstream containers
    '.obu', '.ivf', '.av1',
    '.hevc', '.h265', '.265',
    '.h264', '.264', '.avc',
    '.vvc', '.h266', '.266',
}
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
MEDIAINFO_TIMEOUT = 120  # MediaInfo can be slower on large REMUX files

# --- GLOBAL STATE ---
APP_START_TIME = time.time()
APP_VERSION_FALLBACK = os.environ.get("APP_VERSION", "dev")
RADARR_URL = (os.environ.get("RADARR_URL") or "").strip().rstrip("/")
RADARR_API_KEY = (os.environ.get("RADARR_API_KEY") or "").strip()
SONARR_URL = (os.environ.get("SONARR_URL") or "").strip().rstrip("/")
SONARR_API_KEY = (os.environ.get("SONARR_API_KEY") or "").strip()
ARR_STATUS_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": None}
TOOL_VERSION_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": None}
# Unfiltered library stats for /api/videos/meta (invalidated on any DB write via get_db).
LIBRARY_STATS_CACHE: Dict[str, Any] = {"bundle": None}
library_stats_cache_lock = threading.Lock()
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

def is_heavy_job_running() -> bool:
    """True while a library scan or backfill owns PROGRESS status=scanning."""
    with progress_lock:
        return PROGRESS.get("status") == "scanning"

def reject_if_busy(status_code: int = 409):
    """
    Return a (jsonify, status_code) tuple if a heavy job is running, else None.
    Use at the top of mutating/heavy endpoints to avoid SQLite contention.
    """
    with progress_lock:
        if PROGRESS.get("status") != "scanning":
            return None
        file_msg = PROGRESS.get("file") or "in progress"
        current = PROGRESS.get("current", 0)
        total = PROGRESS.get("total", 0)
    detail = str(file_msg)
    if total:
        detail = f"{detail} ({current}/{total})"
    return jsonify({
        "status": "busy",
        "message": f"A scan or heavy job is already running: {detail}",
    }), status_code

# PROCESS TRACKING FOR INSTANT KILL
ACTIVE_PROCS = set()
proc_lock = threading.Lock()

# RPU CACHE - Cache RPU extraction results to avoid re-extraction
# Key: (file_path, file_size, mtime), Value: {'dovi_data': dict, 'rpu_size': int}
# Using LRU eviction - most recently used items are kept
RPU_CACHE = OrderedDict()  # OrderedDict for LRU behavior
rpu_cache_lock = threading.Lock()

def app_version() -> str:
    """Return the latest semantic version listed in CHANGELOG.md."""
    try:
        with open(CHANGELOG_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                match = re.match(r"^##\s+v?(\d+\.\d+\.\d+)\s*$", line.strip(), re.IGNORECASE)
                if match:
                    return match.group(1)
    except OSError:
        pass
    return (APP_VERSION_FALLBACK or "dev").strip()

def app_version_label() -> str:
    version = app_version()
    return version if version.lower().startswith("v") or version == "dev" else f"v{version}"

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

_WEEKDAY_CRON = {'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'}

def _parse_schedule_time(raw: str, default_hour: int = 3, default_minute: int = 0) -> tuple[int, int]:
    """Parse HH:MM; fall back to default when missing/invalid."""
    text = (raw or '').strip()
    if not text or ':' not in text:
        return default_hour, default_minute
    try:
        h_str, m_str = text.split(':', 1)
        hour, minute = int(h_str), int(m_str)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except (TypeError, ValueError):
        pass
    return default_hour, default_minute

def apply_scan_schedule(mode: str, value: str) -> None:
    """
    Configure APScheduler for scan_mode/scan_value.

    Modes:
      - manual: no jobs
      - daily: value = HH:MM
      - interval: value = hours (int)
      - weekly: value = dow  or  dow|HH:MM  (dow = mon..sun; default time 03:00)
      - monthly: value = day or day|HH:MM  (day = 1..31; default time 03:00)
    """
    if not scheduler:
        return
    try:
        scheduler.remove_all_jobs()
    except Exception as e:
        log_debug(f"Scheduler remove_all_jobs failed: {e}", "WARNING")

    mode_l = (mode or 'manual').strip().lower()
    val = (value or '').strip()
    if mode_l in ('', 'manual') or (mode_l != 'manual' and not val and mode_l != 'interval'):
        return
    if mode_l == 'manual':
        return

    try:
        if mode_l == 'daily':
            hour, minute = _parse_schedule_time(val)
            scheduler.add_job(run_scan, 'cron', hour=hour, minute=minute, id='scheduled_scan', replace_existing=True)
        elif mode_l == 'interval':
            hours = max(1, int(val))
            scheduler.add_job(run_scan, 'interval', hours=hours, id='scheduled_scan', replace_existing=True)
        elif mode_l == 'weekly':
            parts = val.split('|', 1)
            dow = parts[0].strip().lower()
            if dow not in _WEEKDAY_CRON:
                raise ValueError(f"Invalid weekday '{dow}' (expected mon..sun)")
            hour, minute = _parse_schedule_time(parts[1] if len(parts) > 1 else '')
            scheduler.add_job(
                run_scan, 'cron', day_of_week=dow, hour=hour, minute=minute,
                id='scheduled_scan', replace_existing=True,
            )
        elif mode_l == 'monthly':
            parts = val.split('|', 1)
            day = int(parts[0].strip())
            if day < 1 or day > 31:
                raise ValueError(f"Invalid day of month '{day}' (expected 1..31)")
            hour, minute = _parse_schedule_time(parts[1] if len(parts) > 1 else '')
            scheduler.add_job(
                run_scan, 'cron', day=day, hour=hour, minute=minute,
                id='scheduled_scan', replace_existing=True,
            )
        else:
            log_debug(f"Unknown scan schedule mode '{mode_l}' — no job registered", "WARNING")
            return
        log_debug(f"Scan schedule applied: mode={mode_l} value={val}", "INFO")
    except Exception as e:
        log_debug(f"Failed to apply scan schedule mode={mode_l} value={val}: {e}", "ERROR")
        raise

def restore_scan_schedule_from_settings() -> None:
    """Re-register scheduled scan jobs from persisted settings (survives restart)."""
    if not scheduler:
        return
    try:
        with get_db() as conn:
            rows = dict(conn.execute(
                "SELECT key, value FROM settings WHERE key IN ('scan_mode', 'scan_value')"
            ).fetchall())
        apply_scan_schedule(rows.get('scan_mode', 'manual'), rows.get('scan_value', ''))
    except Exception as e:
        log_debug(f"Could not restore scan schedule from settings: {e}", "WARNING")

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

def is_path_within_root(path: str, root: str) -> bool:
    """
    True if path resolves inside root (realpath + commonpath).
    Avoids prefix tricks like /mnt/movies vs /mnt/movies_backup.
    """
    try:
        real_path = os.path.realpath(path)
        real_root = os.path.realpath(root)
    except (OSError, ValueError):
        return False
    try:
        return os.path.commonpath([real_path, real_root]) == real_root
    except ValueError:
        # Different drives / uncomparable paths (e.g. Windows)
        return False

def get_allowed_media_roots() -> list[str]:
    """Return configured/discovered media mount roots for path confinement."""
    roots: list[str] = []
    for p in get_mount_status().values():
        if p and str(p).strip():
            roots.append(str(p).strip())
    return roots

def resolve_allowed_media_path(path: str) -> tuple[Optional[str], Optional[str]]:
    """
    Normalize path and ensure it lies under an allowed media mount.

    Returns:
        (realpath, None) on success, or (None, error_message) on rejection.
    """
    if path is None or not str(path).strip():
        return None, "Missing path"
    try:
        real = os.path.realpath(os.path.normpath(str(path).strip()))
    except (OSError, ValueError):
        return None, "Invalid path"

    roots = get_allowed_media_roots()
    if not roots:
        return None, "No media mounts configured"
    for root in roots:
        if is_path_within_root(real, root):
            return real, None
    return None, "Path is outside allowed media mounts"

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
            # Invalidate only when this connection mutated rows (avoid wiping cache on read-only get_db use).
            changed = conn.total_changes > 0
            conn.commit()
            if changed:
                invalidate_library_stats_cache()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

def invalidate_library_stats_cache() -> None:
    """Drop cached unfiltered library stats so the next meta request rebuilds them."""
    with library_stats_cache_lock:
        LIBRARY_STATS_CACHE["bundle"] = None

def _load_scan_folders(conn: Any) -> list:
    try:
        row = conn.execute("SELECT value FROM settings WHERE key='scan_folders'").fetchone()
        folders = json.loads(row[0]) if row and row[0] else []
    except Exception:
        folders = []
    if isinstance(folders, dict):
        folders = [folders]
    if not isinstance(folders, list):
        folders = []
    return folders

def _path_counts_for_where(conn: Any, where_clause: str, params: list[Any]) -> Tuple[List[str], List[int]]:
    labels: list[str] = []
    counts: list[int] = []
    for f in _load_scan_folders(conn) or []:
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
                params + [vol, like_pattern, like_pattern],
            ).fetchone()[0]
        else:
            count = conn.execute(
                f"SELECT COUNT(*) FROM videos WHERE {where_clause} AND source_vol = ?",
                params + [vol],
            ).fetchone()[0]
        counts.append(count)
    return labels, counts

def _group_col_counts(conn: Any, col: str, where_clause: Optional[str] = None, params: Optional[list[Any]] = None) -> Dict[Any, int]:
    clause = f"{col} != '' AND {col} IS NOT NULL"
    query_params: list[Any] = list(params or [])
    if where_clause:
        clause = f"{clause} AND {where_clause}"
    return {
        r[0]: r[1]
        for r in conn.execute(
            f"SELECT {col}, COUNT(*) FROM videos WHERE {clause} GROUP BY {col}",
            query_params,
        ).fetchall()
    }

def _secondary_hdr_counts(conn: Any, where_clause: str, params: list[Any]) -> Dict[str, int]:
    clause = where_clause or "1=1"
    result: Dict[str, int] = {}
    for key, val in conn.execute(
        f"SELECT secondary_hdr, COUNT(*) FROM videos WHERE {clause} GROUP BY secondary_hdr",
        params,
    ).fetchall():
        result[key if key else 'none'] = val
    return result

def _audio_codec_counts_sql(conn: Any, where_sql: str, params: list[Any]) -> Dict[str, int]:
    """
    Count individual codecs from comma-separated audio_codecs using a recursive CTE.
    Avoids fetching every matching row into Python for splitting.
    """
    clause = where_sql or "1=1"
    rows = conn.execute(
        f"""
        WITH RECURSIVE
        filtered AS (
            SELECT audio_codecs AS raw
            FROM videos
            WHERE audio_codecs IS NOT NULL AND audio_codecs != '' AND ({clause})
        ),
        split(codec, rest) AS (
            SELECT
                TRIM(
                    CASE
                        WHEN INSTR(raw, ',') > 0 THEN SUBSTR(raw, 1, INSTR(raw, ',') - 1)
                        ELSE raw
                    END
                ),
                CASE
                    WHEN INSTR(raw, ',') > 0 THEN SUBSTR(raw, INSTR(raw, ',') + 1)
                    ELSE ''
                END
            FROM filtered
            UNION ALL
            SELECT
                TRIM(
                    CASE
                        WHEN INSTR(rest, ',') > 0 THEN SUBSTR(rest, 1, INSTR(rest, ',') - 1)
                        ELSE rest
                    END
                ),
                CASE
                    WHEN INSTR(rest, ',') > 0 THEN SUBSTR(rest, INSTR(rest, ',') + 1)
                    ELSE ''
                END
            FROM split
            WHERE rest != ''
        )
        SELECT codec, COUNT(*) AS cnt
        FROM split
        WHERE codec IS NOT NULL AND codec != ''
        GROUP BY codec
        """,
        params,
    ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows if r[0]}

def _compute_enriched_stats(conn: Any, where_sql: str, params: list[Any], include_sizes: bool = False) -> Dict[str, Any]:
    """Build ribbon/chart stats for a WHERE scope via SQL aggregations (no full-row Python scan)."""
    stats = _build_stats_sql(conn, where_sql, params)
    if where_sql == "1=1":
        vol = _group_col_counts(conn, 'source_vol', None, [])
        res = _group_col_counts(conn, 'resolution', None, [])
    else:
        vol = _group_col_counts(conn, 'source_vol', where_sql, params)
        res = _group_col_counts(conn, 'resolution', where_sql, params)
    stats['vol_labels'] = list(vol.keys())
    stats['vol_data'] = list(vol.values())
    stats['res_labels'] = list(res.keys())
    stats['res_data'] = list(res.values())
    stats['secondary_hdrs'] = _secondary_hdr_counts(conn, where_sql, params)
    stats['path_labels'], stats['path_data'] = _path_counts_for_where(conn, where_sql, params)
    stats['last_scan_time'] = PROGRESS["last_duration"]
    stats['last_full_scan'] = PROGRESS.get("last_full_scan") or "Never"
    if include_sizes:
        stats['total_size_all'] = conn.execute("SELECT COALESCE(SUM(file_size), 0) FROM videos").fetchone()[0]
        stats['total_size_movie'] = conn.execute(
            "SELECT COALESCE(SUM(file_size), 0) FROM videos WHERE LOWER(media_type) = 'movie'"
        ).fetchone()[0]
        stats['total_size_tv'] = conn.execute(
            "SELECT COALESCE(SUM(file_size), 0) FROM videos WHERE LOWER(media_type) = 'tv'"
        ).fetchone()[0]
    return stats

def _build_stats_sql(conn: Any, where_sql: str, params: list[Any]) -> Dict[str, Any]:
    """
    Ribbon badge counts using SQL aggregates.
    Matches prior Python semantics: failed rows count toward total/failed only;
    category/hybrid badges ignore failed rows.
    """
    ok = "(scan_error IS NULL OR scan_error = '')"
    row = conn.execute(
        f"""
        SELECT
          COUNT(*) AS total,
          COALESCE(SUM(CASE WHEN scan_error IS NOT NULL AND scan_error != '' THEN 1 ELSE 0 END), 0) AS failed,
          COALESCE(SUM(CASE WHEN {ok} AND is_hybrid = 1 THEN 1 ELSE 0 END), 0) AS hybrid,
          COALESCE(SUM(CASE WHEN {ok} AND COALESCE(is_source_hybrid, 0) = 1 THEN 1 ELSE 0 END), 0) AS source_hybrid,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' THEN 1 ELSE 0 END), 0) AS dovi,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '7' AND el_type = 'FEL' THEN 1 ELSE 0 END), 0) AS dovi_p7_fel,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '7' AND IFNULL(el_type, '') != 'FEL' THEN 1 ELSE 0 END), 0) AS dovi_p7_mel,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '5' THEN 1 ELSE 0 END), 0) AS dovi_p5,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '8.1' THEN 1 ELSE 0 END), 0) AS dovi_p81,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '8.4' THEN 1 ELSE 0 END), 0) AS dovi_p84,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '8' THEN 1 ELSE 0 END), 0) AS dovi_p8,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '10.1' THEN 1 ELSE 0 END), 0) AS dovi_p101,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '10.4' THEN 1 ELSE 0 END), 0) AS dovi_p104,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '10' THEN 1 ELSE 0 END), 0) AS dovi_p10,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'dovi' AND profile = '20' THEN 1 ELSE 0 END), 0) AS dovi_p20,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'hdr10plus' THEN 1 ELSE 0 END), 0) AS hdr10plus,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'hdr10' THEN 1 ELSE 0 END), 0) AS hdr10,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'hlg' THEN 1 ELSE 0 END), 0) AS hlg,
          COALESCE(SUM(CASE WHEN {ok} AND category = 'sdr_only' THEN 1 ELSE 0 END), 0) AS sdr
        FROM videos
        WHERE {where_sql}
        """,
        params,
    ).fetchone()
    return {
        "total": int(row["total"] or 0),
        "failed": int(row["failed"] or 0),
        "hybrid": int(row["hybrid"] or 0),
        "source_hybrid": int(row["source_hybrid"] or 0),
        "dovi": int(row["dovi"] or 0),
        "dovi_p7_fel": int(row["dovi_p7_fel"] or 0),
        "dovi_p7_mel": int(row["dovi_p7_mel"] or 0),
        "dovi_p81": int(row["dovi_p81"] or 0),
        "dovi_p84": int(row["dovi_p84"] or 0),
        "dovi_p8": int(row["dovi_p8"] or 0),
        "dovi_p5": int(row["dovi_p5"] or 0),
        "dovi_p101": int(row["dovi_p101"] or 0),
        "dovi_p104": int(row["dovi_p104"] or 0),
        "dovi_p10": int(row["dovi_p10"] or 0),
        "dovi_p20": int(row["dovi_p20"] or 0),
        "hdr10plus": int(row["hdr10plus"] or 0),
        "hdr10": int(row["hdr10"] or 0),
        "hlg": int(row["hlg"] or 0),
        "sdr": int(row["sdr"] or 0),
        "vol_labels": [],
        "vol_data": [],
        "res_labels": [],
        "res_data": [],
        "secondary_hdrs": {},
    }

def get_or_build_library_stats_bundle(conn: Any) -> Dict[str, Any]:
    """
    Cached unfiltered library stats (all / movie / tv) + library_total.
    Safe across filter clicks; rebuilt after any write transaction.
    """
    with library_stats_cache_lock:
        cached = LIBRARY_STATS_CACHE.get("bundle")
        if cached is not None:
            return copy.deepcopy(cached)

    bundle = {
        "library_total": conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0],
        "stats": _compute_enriched_stats(conn, "1=1", [], include_sizes=True),
        "stats_movie": _compute_enriched_stats(conn, "LOWER(media_type) = 'movie'", []),
        "stats_tv": _compute_enriched_stats(conn, "LOWER(media_type) = 'tv'", []),
    }
    with library_stats_cache_lock:
        LIBRARY_STATS_CACHE["bundle"] = bundle
        return copy.deepcopy(bundle)

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


def _as_int(value: Any) -> Optional[int]:
    """Convert value to int when possible; otherwise return None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


ARR_RETRY_MAX_ATTEMPTS = 3
ARR_RETRY_INITIAL_DELAY = 3.0  # Give ARR servers time to recover (avoid hammering when overloaded)
ARR_RETRY_BACKOFF = 2.0


def _arr_request(
    base_url: str,
    api_key: str,
    method: str,
    endpoint: str,
    payload: Optional[dict] = None,
    query: Optional[dict] = None,
    timeout_seconds: int = 20
) -> Any:
    """
    Make an authenticated ARR API request with retry logic (5xx, 429, timeouts).
    Returns parsed JSON when present.
    """
    ep = endpoint.lstrip('/')
    url = f"{base_url}/api/v3/{ep}"
    if query:
        query_clean = {k: v for k, v in query.items() if v is not None and str(v).strip() != ''}
        if query_clean:
            url = f"{url}?{urllib.parse.urlencode(query_clean)}"

    _log_safe = f"{method} {base_url}/api/v3/{ep}"
    if DEBUG_MODE:
        log_debug(f"[ARR] {_log_safe}", "DEBUG")

    body = None
    headers = {"X-Api-Key": api_key, "Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    last_err: Optional[Exception] = None
    for attempt in range(1, ARR_RETRY_MAX_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                raw = resp.read().decode("utf-8", errors="replace").strip()
                if not raw:
                    return {}
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return {"raw": raw}
        except urllib.error.HTTPError as e:
            err_text = ""
            try:
                err_text = e.read().decode("utf-8", errors="replace").strip()
            except Exception:
                err_text = ""
            msg = f"ARR HTTP {e.code}: {err_text[:300]}" if err_text else f"ARR HTTP {e.code}"
            last_err = RuntimeError(msg)
            retryable = e.code >= 500 or e.code == 429
            if retryable and attempt < ARR_RETRY_MAX_ATTEMPTS:
                delay = ARR_RETRY_INITIAL_DELAY * (ARR_RETRY_BACKOFF ** (attempt - 1))
                log_debug(f"[ARR] {_log_safe} -> {msg}, retry {attempt}/{ARR_RETRY_MAX_ATTEMPTS} in {delay:.1f}s", "WARNING")
                time.sleep(delay)
            else:
                log_debug(f"[ARR] FAIL {_log_safe} -> {msg}", "ERROR")
                raise last_err
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            reason = getattr(e, "reason", e) if hasattr(e, "reason") else e
            msg = f"ARR connection failed: {reason}"
            last_err = RuntimeError(msg)
            if attempt < ARR_RETRY_MAX_ATTEMPTS:
                delay = ARR_RETRY_INITIAL_DELAY * (ARR_RETRY_BACKOFF ** (attempt - 1))
                log_debug(f"[ARR] {_log_safe} -> {msg}, retry {attempt}/{ARR_RETRY_MAX_ATTEMPTS} in {delay:.1f}s", "WARNING")
                time.sleep(delay)
            else:
                log_debug(f"[ARR] FAIL {_log_safe} -> {msg}", "ERROR")
                raise last_err
    raise last_err or RuntimeError("ARR request failed")


def _queue_radarr_search(item: Dict[str, Any]) -> Tuple[bool, str]:
    """Queue a Radarr movie search for a single DB item."""
    if not RADARR_URL or not RADARR_API_KEY:
        return False, "Radarr is not configured (RADARR_URL/RADARR_API_KEY)"

    tmdb_id = _as_int(item.get("tmdb_id"))
    imdb_id = str(item.get("imdb_id") or "").strip()
    movie_id: Optional[int] = None

    log_debug(f"[ARR] Radarr lookup: tmdb_id={tmdb_id}, imdb_id={imdb_id or '(none)'}", "INFO")

    if tmdb_id is not None:
        movies = _arr_request(RADARR_URL, RADARR_API_KEY, "GET", "movie", query={"tmdbId": tmdb_id})
        if isinstance(movies, list) and movies:
            movie_id = _as_int(movies[0].get("id") or movies[0].get("Id"))

    if movie_id is None and imdb_id:
        movies = _arr_request(RADARR_URL, RADARR_API_KEY, "GET", "movie", query={"imdbId": imdb_id})
        if isinstance(movies, list) and movies:
            movie_id = _as_int(movies[0].get("id") or movies[0].get("Id"))

    if movie_id is None:
        log_debug(f"[ARR] Radarr movie?tmdbId/imdbId= returned empty, trying GET /movie (all)", "INFO")
        all_movies = _arr_request(RADARR_URL, RADARR_API_KEY, "GET", "movie")
        if isinstance(all_movies, list):
            for m in all_movies:
                mid = _as_int(m.get("id") or m.get("Id"))
                mtmdb = _as_int(m.get("tmdbId") or m.get("tmdb_id") or m.get("TmdbId"))
                mimdb = str(m.get("imdbId") or m.get("imdb_id") or "").strip()
                if mid and (mtmdb == tmdb_id or (imdb_id and mimdb == imdb_id)):
                    movie_id = mid
                    log_debug(f"[ARR] Radarr found via all-movies filter: movieId={movie_id}", "INFO")
                    break

    if movie_id is None:
        msg = "Radarr item not found (tmdb_id/imdb_id missing or unmatched)"
        log_debug(f"[ARR] Radarr skip: {msg}", "WARNING")
        return False, msg

    _arr_request(
        RADARR_URL,
        RADARR_API_KEY,
        "POST",
        "command",
        payload={"name": "MoviesSearch", "movieIds": [movie_id]}
    )
    log_debug(f"[ARR] Radarr queued MoviesSearch for movieId={movie_id}", "INFO")
    return True, "Queued Radarr movie search"


def _queue_sonarr_search(item: Dict[str, Any]) -> Tuple[bool, str]:
    """Queue a Sonarr episode/series search for a single DB item."""
    if not SONARR_URL or not SONARR_API_KEY:
        return False, "Sonarr is not configured (SONARR_URL/SONARR_API_KEY)"

    tvdb_series_id = _as_int(item.get("tvdb_series_id"))
    season = _as_int(item.get("season"))
    episode = _as_int(item.get("episode"))
    series_id: Optional[int] = None

    log_debug(f"[ARR] Sonarr lookup: tvdb_series_id={tvdb_series_id}, season={season}, episode={episode}", "INFO")

    def _extract_series_id(resp: Any) -> Optional[int]:
        """Extract series id from Sonarr API response (list or single object)."""
        if isinstance(resp, list) and resp:
            s = resp[0]
        elif isinstance(resp, dict) and resp:
            s = resp
        else:
            return None
        return _as_int(s.get("id") or s.get("Id"))

    if tvdb_series_id is not None:
        # Try series?tvdbid= (lowercase - some Sonarr versions expect this)
        series = _arr_request(SONARR_URL, SONARR_API_KEY, "GET", "series", query={"tvdbid": tvdb_series_id})
        series_id = _extract_series_id(series)
        if series_id is None:
            log_debug(f"[ARR] Sonarr series?tvdbid= returned {type(series).__name__} len={len(series) if isinstance(series, (list, dict)) else 0}, trying tvdbId", "INFO")
            # Fallback: series?tvdbId= (camelCase)
            series = _arr_request(SONARR_URL, SONARR_API_KEY, "GET", "series", query={"tvdbId": tvdb_series_id})
            series_id = _extract_series_id(series)
        if series_id is None:
            log_debug(f"[ARR] Sonarr series?tvdbId= returned {type(series).__name__} len={len(series) if isinstance(series, (list, dict)) else 0}, trying series/lookup", "INFO")
            # Fallback: series/lookup (can 503 when SkyHook/TVDB is down - catch and continue)
            for lookup_query in [{"tvdbId": tvdb_series_id}, {"term": f"tvdb:{tvdb_series_id}"}]:
                if series_id is not None:
                    break
                try:
                    lookup = _arr_request(SONARR_URL, SONARR_API_KEY, "GET", "series/lookup", query=lookup_query)
                    if isinstance(lookup, list) and lookup:
                        for s in lookup:
                            sid = _as_int(s.get("id") or s.get("Id"))
                            if sid and sid > 0:
                                series_id = sid
                                log_debug(f"[ARR] Sonarr found via lookup: seriesId={series_id}", "INFO")
                                break
                except Exception as lookup_err:
                    log_debug(f"[ARR] Sonarr series/lookup failed (continuing): {lookup_err}", "WARNING")
        if series_id is None:
            log_debug(f"[ARR] Sonarr trying GET /series (all) and filter by tvdbId", "INFO")
            # Fallback: fetch all series and filter client-side (tvdbId param often returns empty)
            all_series = _arr_request(SONARR_URL, SONARR_API_KEY, "GET", "series")
            if isinstance(all_series, list):
                count = len(all_series)
                log_debug(f"[ARR] Sonarr GET /series returned {count} series", "INFO")
                sample_tvdb = []
                for s in all_series[:10]:
                    stvdb = _as_int(s.get("tvdbId") or s.get("tvdb_id") or s.get("TvdbId"))
                    if stvdb is not None:
                        sample_tvdb.append(stvdb)
                if sample_tvdb:
                    log_debug(f"[ARR] Sonarr sample tvdbIds from /series: {sample_tvdb}", "INFO")
                for s in all_series:
                    sid = _as_int(s.get("id") or s.get("Id"))
                    stvdb = _as_int(s.get("tvdbId") or s.get("tvdb_id") or s.get("TvdbId"))
                    if sid and stvdb == tvdb_series_id:
                        series_id = sid
                        log_debug(f"[ARR] Sonarr found via all-series filter: seriesId={series_id}", "INFO")
                        break
                if series_id is None and count > 0:
                    all_tvdb = [_as_int(s.get("tvdbId") or s.get("tvdb_id") or s.get("TvdbId")) for s in all_series]
                    log_debug(f"[ARR] Sonarr no match for tvdbId={tvdb_series_id}; series tvdbIds present: {len([x for x in all_tvdb if x is not None])}", "WARNING")
                    # Match by show title within all_series (NFO tvdbId can be wrong, e.g. old TVDB migration)
                    show_title = str(item.get("show_title") or "").strip()
                    if not show_title:
                        # Fallback: derive from filename (e.g. The.Secret.Life.of.Us.S03E01... -> The Secret Life of Us)
                        fn = str(item.get("filename") or "").strip()
                        m = re.search(r"^(.+?)[.\s_-]*[sS]\d{1,2}[.\s_-]*[eE]\d{1,2}", fn)
                        if m:
                            show_title = m.group(1).replace(".", " ").replace("_", " ").strip()
                    if show_title and series_id is None:
                        log_debug(f"[ARR] Sonarr trying title match: '{show_title[:50]}'", "INFO")

                        def _norm(t: str) -> str:
                            return re.sub(r"[^\w]", "", t.lower()) if t else ""

                        want = _norm(show_title)
                        for s in all_series:
                            title = str(s.get("title") or s.get("Title") or "").strip()
                            sid = _as_int(s.get("id") or s.get("Id"))
                            tnorm = _norm(title)
                            matched = want in tnorm or (len(tnorm) >= 8 and tnorm in want)
                            if sid and want and matched:
                                series_id = sid
                                log_debug(f"[ARR] Sonarr found via title match in /series: seriesId={series_id} title={title}", "INFO")
                                break

    if series_id is None and tvdb_series_id is None and season is not None and episode is not None:
        show_title = str(item.get("show_title") or "").strip()
        if not show_title:
            fn = str(item.get("filename") or "").strip()
            m = re.search(r"^(.+?)[.\s_-]*[sS]\d{1,2}[.\s_-]*[eE]\d{1,2}", fn)
            if m:
                show_title = m.group(1).replace(".", " ").replace("_", " ").strip()
        if show_title:
            log_debug(f"[ARR] Sonarr no tvdb_series_id; trying title match: '{show_title[:50]}'", "INFO")
            all_series = _arr_request(SONARR_URL, SONARR_API_KEY, "GET", "series")
            if isinstance(all_series, list):

                def _norm(t: str) -> str:
                    return re.sub(r"[^\w]", "", t.lower()) if t else ""

                want = _norm(show_title)
                for s in all_series:
                    title = str(s.get("title") or s.get("Title") or "").strip()
                    sid = _as_int(s.get("id") or s.get("Id"))
                    tnorm = _norm(title)
                    matched = want in tnorm or (len(tnorm) >= 8 and tnorm in want)
                    if sid and want and matched:
                        series_id = sid
                        log_debug(f"[ARR] Sonarr found via title match (no NFO): seriesId={series_id} title={title}", "INFO")
                        break

    if series_id is None:
        msg = "Sonarr series not found (tvdb_series_id missing or unmatched)"
        log_debug(f"[ARR] Sonarr skip: {msg}", "WARNING")
        return False, msg

    if season is not None:
        _arr_request(
            SONARR_URL,
            SONARR_API_KEY,
            "POST",
            "command",
            payload={"name": "SeasonSearch", "seriesId": series_id, "seasonNumber": season}
        )
        log_debug(f"[ARR] Sonarr queued SeasonSearch for seriesId={series_id} S{season}", "INFO")
        return True, "Queued Sonarr season search"

    _arr_request(
        SONARR_URL,
        SONARR_API_KEY,
        "POST",
        "command",
        payload={"name": "SeriesSearch", "seriesId": series_id}
    )
    log_debug(f"[ARR] Sonarr queued SeriesSearch for seriesId={series_id}", "INFO")
    return True, "Queued Sonarr series search"


def _arr_service_status(name: str, base_url: str, api_key: str) -> Dict[str, Any]:
    """Return connectivity/config status for a single ARR service."""
    if not base_url or not api_key:
        return {
            "name": name,
            "ok": False,
            "configured": False,
            "message": "Not configured"
        }
    try:
        payload = _arr_request(base_url, api_key, "GET", "system/status", timeout_seconds=5)
        version = str((payload or {}).get("version") or "").strip() if isinstance(payload, dict) else ""
        msg = "Connected"
        if version:
            msg = f"Connected ({version})"
        return {
            "name": name,
            "ok": True,
            "configured": True,
            "message": msg
        }
    except Exception as e:
        return {
            "name": name,
            "ok": False,
            "configured": True,
            "message": str(e)
        }

def _extract_tool_version(tool: str, output: str) -> str:
    """Extract a concise version string from common media tool output."""
    text = (output or "").strip()
    first_line = text.splitlines()[0] if text else ""
    if not first_line:
        return "unknown"

    patterns = {
        "ffmpeg": r"ffmpeg version\s+([^\s]+)",
        "ffprobe": r"ffprobe version\s+([^\s]+)",
        "mediainfo": r"MediaInfo(?:Lib)?(?:\s+Command line)?\s*(?:-|,|v|version)?\s*v?([0-9]+(?:\.[0-9]+)+)",
        "dovi_tool": r"dovi_tool\s+([^\s]+)",
    }
    search_text = text if tool == "mediainfo" else first_line
    match = re.search(patterns.get(tool, r"([0-9][^\s]*)"), search_text, re.IGNORECASE)
    return match.group(1) if match else first_line

def _tool_version(tool: str, command: list[str]) -> Dict[str, Any]:
    """Return installation and version details for an external tool."""
    if shutil.which(command[0]) is None:
        return {"installed": False, "version": None, "message": "Not found"}
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=3,
            check=False
        )
        output = (result.stdout or result.stderr or "").strip()
        version = _extract_tool_version(tool, output)
        return {
            "installed": result.returncode == 0,
            "version": version,
            "message": version if result.returncode == 0 else (output.splitlines()[0] if output else "Error")
        }
    except (OSError, subprocess.SubprocessError) as e:
        return {"installed": False, "version": None, "message": str(e)}

def get_tool_versions() -> Dict[str, Any]:
    """Return cached external tool versions for the health modal."""
    now = time.time()
    cached = TOOL_VERSION_CACHE.get("payload")
    if cached and now - float(TOOL_VERSION_CACHE.get("ts") or 0) < 300:
        return cached

    python_version = sys.version.split()[0]
    payload = {
        "python": {
            "installed": True,
            "version": python_version,
            "message": python_version
        },
        "ffmpeg": _tool_version("ffmpeg", ["ffmpeg", "-version"]),
        "ffprobe": _tool_version("ffprobe", ["ffprobe", "-version"]),
        "mediainfo": _tool_version("mediainfo", ["mediainfo", "--Version"]),
        "dovi_tool": _tool_version("dovi_tool", ["dovi_tool", "--version"]),
    }
    TOOL_VERSION_CACHE["ts"] = now
    TOOL_VERSION_CACHE["payload"] = payload
    return payload

def init_db() -> None:
    """
    Initialize the database with required tables and migrations.
    """
    log_debug("Initializing Database...")
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with get_db() as conn:
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
                         nfo_missing INTEGER DEFAULT 0, missing INTEGER DEFAULT 0, validation_flag TEXT,
                         dup_group_key TEXT, dup_exact_key TEXT, dup_count INTEGER DEFAULT 0)''')
        
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
                'is_source_hybrid': 'INTEGER DEFAULT 0',
                'media_type': 'TEXT', 'show_title': 'TEXT', 'season': 'INTEGER', 'episode': 'INTEGER',
                'movie_title': 'TEXT', 'episode_title': 'TEXT', 'nfo_missing': 'INTEGER DEFAULT 0', 'missing': 'INTEGER DEFAULT 0', 'validation_flag': 'TEXT',
                'dup_group_key': 'TEXT', 'dup_exact_key': 'TEXT', 'dup_count': 'INTEGER DEFAULT 0',
                'tvdb_series_id': 'TEXT', 'tvdb_episode_id': 'TEXT', 'imdb_series_id': 'TEXT', 'imdb_episode_id': 'TEXT',
                'tmdb_series_id': 'TEXT', 'tmdb_episode_id': 'TEXT', 'trakt_series_id': 'TEXT', 'trakt_episode_id': 'TEXT',
                'rotten_series_id': 'TEXT', 'rotten_episode_id': 'TEXT', 'metacritic_series_id': 'TEXT', 'metacritic_episode_id': 'TEXT'
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dup_group_key ON videos (dup_group_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dup_exact_key ON videos (dup_exact_key)")
        # Additional filter / sort helpers (safe IF NOT EXISTS for existing DBs)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_el_type ON videos (el_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_secondary_hdr ON videos (secondary_hdr)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_edition ON videos (edition)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_missing ON videos (missing)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_nfo_missing ON videos (nfo_missing)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_is_source_hybrid ON videos (is_source_hybrid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_size ON videos (file_size)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bitrate_mbps ON videos (bitrate_mbps)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dup_count ON videos (dup_count)")
        # Expression indexes match LOWER(...) filter predicates used by build_filter_query
        conn.execute("CREATE INDEX IF NOT EXISTS idx_category_lower ON videos (LOWER(category))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_type_lower ON videos (LOWER(media_type))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vol_lower ON videos (LOWER(source_vol))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_resolution_lower ON videos (LOWER(resolution))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_secondary_hdr_lower ON videos (LOWER(secondary_hdr))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_profile_lower ON videos (LOWER(profile))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_el_type_lower ON videos (LOWER(el_type))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_container_lower ON videos (LOWER(container))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_edition_lower ON videos (LOWER(edition))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_video_source_lower ON videos (LOWER(video_source))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_format_lower ON videos (LOWER(source_format))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_video_codec_lower ON videos (LOWER(video_codec))")
        recompute_duplicate_counts(conn)
        
        defaults = {'threads': '4', 'skip_words': 'trailer,sample', 'min_size_mb': '50', 'refresh_interval': '60', 'notif_style': 'modal', 'force_rescan': 'false', 'column_order': '', 'scan_folders': '[]', 'scan_extras': 'false', 'debug_mode': 'false', 'remove_missing_from_db': 'true', 'duplicate_check_on_scan': 'false'}
        for k, v in defaults.items(): conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))

        # Restore last scan ribbon values so idle UI survives container restarts
        try:
            persisted = dict(conn.execute(
                "SELECT key, value FROM settings WHERE key IN ('last_full_scan', 'last_duration')"
            ).fetchall())
            with progress_lock:
                if persisted.get("last_full_scan"):
                    PROGRESS["last_full_scan"] = persisted["last_full_scan"]
                if persisted.get("last_duration"):
                    PROGRESS["last_duration"] = persisted["last_duration"]
        except Exception as e:
            log_debug(f"Could not restore last scan settings: {e}", "WARNING")

    log_debug("Database ready.")
    
    mounts = get_mount_status()
    if mounts:
        log_debug("--- VOLUME STATUS CHECK ---")
        for vol, path in mounts.items():
            log_debug(f"Volume {vol}: ONLINE ✅")
    else:
        log_debug("⚠️ No media volumes detected in root.")

# --- EXECUTION WRAPPER ---
def run_command(cmd_list: list, capture: bool = True, capture_stderr: bool = False, timeout_seconds: Optional[int] = None) -> tuple[int, str, str]:
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
    
    timeout_value = timeout_seconds if timeout_seconds is not None else SUBPROCESS_TIMEOUT
    
    # Use a separate thread to enforce timeout (communicate timeout may not work if process is truly hung)
    timeout_occurred = threading.Event()
    def kill_on_timeout():
        time.sleep(timeout_value)
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
            stdout_result, stderr_result = p.communicate(timeout=timeout_value + 2)  # Give timeout thread a head start
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
            raise RuntimeError(f"Command timed out after {timeout_value}s: {cmd_list[0]}")
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

    def normalize_rating_name(name: str) -> str:
        return re.sub(r'[^a-z0-9]+', '', (name or '').lower())

    def is_rotten_rating_name(name: str) -> bool:
        n = normalize_rating_name(name)
        return any(token in n for token in ('rottentomatoes', 'rottentomato', 'tomatometer', 'tomatometre')) or n in ('rotten', 'tomato')

    def rotten_rating_priority(name: str) -> int:
        """
        Lower is better.
        Prefer All (e.g. tomatometerallcritics), then users/audience, then critics, then generic.
        """
        n = normalize_rating_name(name)
        has_all = 'all' in n
        has_users = ('audience' in n) or ('user' in n)
        has_critics = 'critic' in n
        if has_all and has_critics:
            return 0  # tomatometerallcritics (canonical Tomatometer)
        if has_all and has_users:
            return 1  # tomatometerallaudience
        if has_all:
            return 2
        if has_users:
            return 3
        if has_critics:
            return 4
        return 5  # rottentomatoes / tomatometer / etc.

    def normalize_rotten_value(rating_val: float, max_attr: str | None) -> float:
        """Store RT as 0–100 when NFO uses max=10 style values (e.g. 8.2 → 82)."""
        max_val = None
        if max_attr:
            try:
                max_val = float(max_attr)
            except ValueError:
                max_val = None
        if max_val and max_val > 0 and max_val <= 10 and rating_val <= max_val:
            return round(rating_val * (100.0 / max_val), 1)
        if max_val is None and rating_val <= 10:
            # Common scraper quirk: percentage written on a 0–10 scale without max=
            return round(rating_val * 10.0, 1)
        return rating_val

    def apply_ratings_block(data: dict) -> None:
        best_rotten: tuple[int, float] | None = None  # (priority, value)
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
            elif is_rotten_rating_name(name):
                score = normalize_rotten_value(rating_val, node.attrib.get('max'))
                prio = rotten_rating_priority(name)
                if best_rotten is None or prio < best_rotten[0]:
                    best_rotten = (prio, score)
            elif name == 'metacritic':
                data['metacritic_rating'] = rating_val
        if best_rotten is not None:
            data['rotten_rating'] = best_rotten[1]

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

def normalize_dup_text(value: str | None) -> str:
    text = (value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[._-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def build_duplicate_group_key(meta: dict) -> str | None:
    media_type = (meta.get('media_type') or '').strip().lower()
    if media_type == 'movie':
        tmdb_id = (meta.get('tmdb_id') or '').strip()
        imdb_id = (meta.get('imdb_id') or '').strip()
        movie_title = normalize_dup_text(meta.get('movie_title') or meta.get('title') or meta.get('filename'))
        year = meta.get('year')
        if tmdb_id:
            return f"movie:tmdb:{tmdb_id}"
        if imdb_id:
            return f"movie:imdb:{imdb_id}"
        if movie_title and year:
            return f"movie:title_year:{movie_title}:{year}"
        if movie_title:
            return f"movie:title:{movie_title}"
        return None

    if media_type == 'tv':
        season = meta.get('season')
        episode = meta.get('episode')
        tvdb_series_id = (meta.get('tvdb_series_id') or '').strip()
        imdb_series_id = (meta.get('imdb_series_id') or '').strip()
        show_title = normalize_dup_text(meta.get('show_title'))
        se = None
        try:
            if season is not None and episode is not None:
                se = f"s{int(season):02}e{int(episode):02}"
        except (TypeError, ValueError):
            se = None
        if se and tvdb_series_id:
            return f"tv:tvdb_series_se:{tvdb_series_id}:{se}"
        if se and imdb_series_id:
            return f"tv:imdb_series_se:{imdb_series_id}:{se}"
        if se and show_title:
            return f"tv:show_se:{show_title}:{se}"
        if tvdb_series_id:
            return f"tv:series:{tvdb_series_id}"
        if show_title:
            return f"tv:series_title:{show_title}"
        return None
    return None

def build_duplicate_exact_key(full_path: str | None, file_size: int | None, sample_bytes: int = 4 * 1024 * 1024) -> str | None:
    if not full_path or not isinstance(file_size, int) or file_size <= 0:
        return None
    try:
        hasher = hashlib.sha1()
        with open(full_path, 'rb') as f:
            head = f.read(sample_bytes)
            hasher.update(head)
            if file_size > sample_bytes:
                tail_size = min(sample_bytes, file_size)
                f.seek(max(0, file_size - tail_size))
                tail = f.read(tail_size)
                hasher.update(tail)
        return f"{file_size}:{hasher.hexdigest()}"
    except Exception:
        return None

def recompute_duplicate_group_keys_for_paths(conn: sqlite3.Connection, paths: list[str]) -> None:
    if not paths:
        return
    unique_paths = [p for p in dict.fromkeys(paths) if p]
    if not unique_paths:
        return
    placeholders = ','.join('?' for _ in unique_paths)
    rows = conn.execute(
        f"""SELECT full_path, filename, media_type, movie_title, show_title, season, episode, year,
                    tmdb_id, imdb_id, tvdb_series_id, imdb_series_id
             FROM videos
             WHERE full_path IN ({placeholders})""",
        unique_paths
    ).fetchall()
    updates = []
    for row in rows:
        meta = {
            'filename': row['filename'],
            'media_type': row['media_type'],
            'movie_title': row['movie_title'],
            'show_title': row['show_title'],
            'season': row['season'],
            'episode': row['episode'],
            'year': row['year'],
            'tmdb_id': row['tmdb_id'],
            'imdb_id': row['imdb_id'],
            'tvdb_series_id': row['tvdb_series_id'],
            'imdb_series_id': row['imdb_series_id']
        }
        dup_group_key = build_duplicate_group_key(meta)
        updates.append((dup_group_key, row['full_path']))
    if updates:
        conn.executemany("UPDATE videos SET dup_group_key=? WHERE full_path=?", updates)

def recompute_duplicate_counts(conn: sqlite3.Connection) -> None:
    conn.execute(
        """UPDATE videos
           SET dup_count = COALESCE((
               SELECT COUNT(*) FROM videos v2
               WHERE v2.dup_group_key = videos.dup_group_key
           ), 0)"""
    )

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
            # Only use "single .nfo in folder" for non-TV; for episodes we require a per-episode match
            if not candidates and len(nfo_files) == 1 and media_type_hint != 'tv':
                candidates.append(str(nfo_files[0]))
        except OSError:
            pass
    if media_type_hint == 'tv':
        # Episode NFO must match this episode (same-stem already handled above).
        # Also try same-folder NFOs whose content has matching season/episode (e.g. different stem).
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
        # tvshow.nfo is series-level; add it for series ID extraction (does not count as NFO found for episodes)
        for parent in file_path_obj.parents:
            if re.match(r'^(season|s\d{1,2}|specials?)$', parent.name, re.IGNORECASE):
                continue
            if re.match(r'^(season)[\s._-]*\d{1,2}$', parent.name, re.IGNORECASE):
                continue
            if re.match(r'^s[\s._-]*\d{1,2}$', parent.name, re.IGNORECASE):
                continue
            tvshow_nfo = parent / 'tvshow.nfo'
            if tvshow_nfo.exists():
                candidates.append(str(tvshow_nfo))
                break
    else:
        # movie.nfo or folder-named .nfo only in the same directory as the video file
        # (do not walk up: a movie.nfo in a parent folder applies to the whole tree, not this file)
        parent = file_path_obj.parent
        movie_nfo = parent / 'movie.nfo'
        if movie_nfo.exists():
            candidates.append(str(movie_nfo))
        folder_nfo = parent / f"{parent.name}.nfo"
        if folder_nfo.exists():
            candidates.append(str(folder_nfo))
        tvshow_nfo = parent / 'tvshow.nfo'
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

def _enrich_from_nfo_and_filename(path: str, result: dict) -> None:
    """
    Merge metadata from filename and NFO into result. Used so that even when
    ffprobe/MediaInfo fails, we still have ARR-relevant metadata (tmdb_id,
    tvdb_series_id, season, episode, show_title, etc.) for Sonarr/Radarr lookup.
    """
    filename_base = os.path.basename(path)
    filename_lower = filename_base.lower()
    filename_meta = parse_filename_metadata(filename_base)
    media_type_guess, season_guess, episode_guess = parse_tv_from_filename(filename_lower)
    if not result.get('year') and filename_meta.get('year'):
        result['year'] = filename_meta['year']
    if not result.get('video_source') and filename_meta.get('video_source'):
        result['video_source'] = filename_meta['video_source']
    if not result.get('source_format') and filename_meta.get('source_format'):
        result['source_format'] = filename_meta['source_format']
    if not result.get('edition') and filename_meta.get('edition'):
        result['edition'] = filename_meta['edition']
    if not result.get('is_3d') and filename_meta.get('is_3d'):
        result['is_3d'] = filename_meta['is_3d']

    nfo_candidates = find_kodi_nfo_candidates(path, result.get('media_type'))
    for nfo_path in nfo_candidates:
        nfo_data = parse_kodi_nfo(nfo_path)
        if not nfo_data:
            continue
        is_tvshow_nfo = pathlib.Path(nfo_path).name.lower() == 'tvshow.nfo'
        if not result.get('year') and nfo_data.get('year'):
            result['year'] = nfo_data['year']
        nfo_media_type = (nfo_data.get('media_type') or '').strip().lower()
        if nfo_media_type == 'movie' and not is_tvshow_nfo:
            # A concrete movie NFO must win over stale/guessed TV typing.
            result['media_type'] = 'movie'
            result['show_title'] = None
            result['season'] = None
            result['episode'] = None
            result['episode_title'] = None
        elif not result.get('media_type') and nfo_media_type:
            # Never let series-level tvshow.nfo classify non-TV files by itself.
            if not (is_tvshow_nfo and media_type_guess != 'tv'):
                result['media_type'] = nfo_media_type
        if not result.get('show_title') and nfo_data.get('show_title'):
            result['show_title'] = nfo_data['show_title']
        if result.get('season') is None and nfo_data.get('season') is not None:
            result['season'] = nfo_data['season']
        if result.get('episode') is None and nfo_data.get('episode') is not None:
            result['episode'] = nfo_data['episode']
        if not result.get('episode_title') and nfo_data.get('episode_title'):
            result['episode_title'] = nfo_data['episode_title']
        if is_tvshow_nfo:
            for k in ('tvdb', 'imdb', 'tmdb', 'trakt', 'rotten', 'metacritic'):
                val = nfo_data.get(f'{k}_id')
                if val and not result.get(f'{k}_series_id'):
                    result[f'{k}_series_id'] = val
        else:
            if nfo_data.get('media_type') != 'movie':
                for k in ('tvdb', 'imdb', 'tmdb', 'trakt', 'rotten', 'metacritic'):
                    val = nfo_data.get(f'{k}_id')
                    if val and not result.get(f'{k}_episode_id'):
                        result[f'{k}_episode_id'] = val
            if result.get('media_type') != 'tv':
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
        if not result.get('movie_title') and nfo_data.get('title') and (nfo_data.get('media_type') == 'movie' or result.get('media_type') != 'tv'):
            result['movie_title'] = nfo_data['title']
    coerce_tv_nfo_to_movie(result, filename_base, media_type_guess, path)

    # Filename is fallback only when NFO/current metadata did not identify type.
    media_type_now = (result.get('media_type') or '').strip().lower()
    if not media_type_now and media_type_guess:
        result['media_type'] = media_type_guess
        media_type_now = media_type_guess
    if media_type_now == 'tv':
        if season_guess is not None and result.get('season') is None:
            result['season'] = season_guess
        if episode_guess is not None and result.get('episode') is None:
            result['episode'] = episode_guess
    elif media_type_now == 'movie':
        # Keep movie rows clean from filename episode guesses.
        result['season'] = None
        result['episode'] = None
        result['episode_title'] = None
    if result.get('media_type') == 'tv' and not result.get('show_title'):
        result['show_title'] = guess_show_title_from_path(path)

def extract_video_codec(filename: str, probe_data: dict) -> str | None:
    """
    Extract video codec from ffprobe data, with filename as fallback.
    Embedded metadata takes precedence if there's a discrepancy.
    
    Args:
        filename: Filename to check for codec hints
        probe_data: ffprobe JSON data
        
    Returns:
        Video codec string (HEVC, H.264, AV1, VVC, etc.) or None
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
                'vvc': 'VVC', 'h266': 'VVC', 'h.266': 'VVC', 'x266': 'VVC',
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
        (r'\b(vvc|h266|h\.266|x266)\b', 'VVC'),
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

_DOVI_CONFIG_BOXES = {b"dvcC", b"dvvC", b"dvwC"}
_ISOM_CONTAINER_BOXES = {
    b"moov", b"trak", b"mdia", b"minf", b"stbl", b"stsd",
    b"udta", b"meta", b"iprp", b"ipco", b"moof", b"traf",
    b"vexu", b"eyes",
}
_ISOM_CONTAINER_EXTS = {".mp4", ".mov", ".m4v", ".cmfv", ".qt"}


def _decode_dovi_config_payload(payload: bytes) -> dict | None:
    """Decode dvcC/dvvC/dvwC payload into profile / level / compatibility id."""
    if len(payload) < 4:
        return None
    tmp = (payload[2] << 8) | payload[3]
    profile = (tmp >> 9) & 0x7F
    level = (tmp >> 3) & 0x3F
    rpu = (tmp >> 2) & 0x01
    el = (tmp >> 1) & 0x01
    bl = tmp & 0x01
    compat = None
    if len(payload) >= 5:
        compat = (payload[4] >> 4) & 0x0F
    return {
        "dovi_profile": str(profile),
        "dovi_level": str(level),
        "rpu_present": bool(rpu),
        "el_present": bool(el),
        "bl_present": bool(bl),
        "bl_compatibility_id": str(compat) if compat is not None else None,
        "dv_version_major": payload[0],
        "dv_version_minor": payload[1],
    }


def _scan_buffer_for_dovi_config(buf: bytes) -> dict | None:
    """Find and decode the first dvcC/dvvC/dvwC box inside a byte buffer."""
    for box in _DOVI_CONFIG_BOXES:
        idx = buf.find(box)
        if idx < 4:
            continue
        size_off = idx - 4
        nsize = int.from_bytes(buf[size_off:size_off + 4], "big")
        if 8 <= nsize <= 64 and size_off + nsize <= len(buf):
            payload = buf[idx + 4:size_off + nsize]
        else:
            payload = buf[idx + 4:idx + 4 + 24]
        decoded = _decode_dovi_config_payload(payload)
        if decoded:
            decoded["box_type"] = box.decode("ascii", errors="replace")
            return decoded
    return None


def parse_isom_dovi_config(path: str) -> dict | None:
    """
    Parse Dolby Vision config from ISOBMFF (MP4/MOV) without loading the whole file.

    Looks for dvcC / dvvC / dvwC boxes. Profile 20 (MV-HEVC stereo) uses dvwC.
    Also notes Video Extended Usage stereo signalling (vexu/eyes).

    Returns:
        Dict with dovi_profile, bl_compatibility_id, is_stereo, box_type — or None.
    """
    try:
        file_size = os.path.getsize(path)
    except OSError:
        return None
    if file_size < 16:
        return None

    found: dict | None = None
    is_stereo = False

    def walk(f, end: int, depth: int = 0) -> None:
        nonlocal found, is_stereo
        if depth > 24:
            return
        while f.tell() + 8 <= end:
            start = f.tell()
            header = f.read(8)
            if len(header) < 8:
                break
            size = int.from_bytes(header[:4], "big")
            typ = header[4:8]
            hdr_len = 8
            if size == 1:
                largesize = f.read(8)
                if len(largesize) < 8:
                    break
                size = int.from_bytes(largesize, "big")
                hdr_len = 16
            elif size == 0:
                size = end - start
            if size < hdr_len or start + size > end:
                break
            payload_start = start + hdr_len
            payload_end = start + size
            payload_size = payload_end - payload_start

            if typ in _DOVI_CONFIG_BOXES and found is None and payload_size >= 4:
                payload = f.read(min(payload_size, 32))
                decoded = _decode_dovi_config_payload(payload)
                if decoded:
                    found = decoded
                    found["box_type"] = typ.decode("ascii", errors="replace")
            elif typ in (b"vexu", b"eyes"):
                is_stereo = True
                if typ == b"vexu" and payload_size > 0:
                    f.seek(payload_start)
                    walk(f, payload_end, depth + 1)
            elif typ in _ISOM_CONTAINER_BOXES and payload_size > 0:
                f.seek(payload_start)
                if typ == b"stsd" and payload_size >= 8:
                    f.read(8)
                elif typ == b"meta" and payload_size >= 4:
                    f.read(4)
                walk(f, payload_end, depth + 1)
            elif found is None and 16 < payload_size < 8192 and typ not in (b"mdat", b"free", b"skip", b"wide"):
                # Sample entries (dvh1/hvc1/…) are not ISO containers — scan nested DOVI boxes
                f.seek(payload_start)
                peek = f.read(payload_size)
                if b"vexu" in peek or b"eyes" in peek:
                    is_stereo = True
                decoded = _scan_buffer_for_dovi_config(peek)
                if decoded:
                    found = decoded

            f.seek(payload_end)
            if found is not None and is_stereo:
                return

    try:
        with open(path, "rb") as f:
            walk(f, file_size, 0)
    except OSError:
        return None

    if found is None and not is_stereo:
        return None
    if found is None:
        return {"is_stereo": True}
    found["is_stereo"] = is_stereo or found.get("dovi_profile") == "20"
    return found


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
        'tvdb_series_id': None, 'tvdb_episode_id': None, 'imdb_series_id': None, 'imdb_episode_id': None,
        'tmdb_series_id': None, 'tmdb_episode_id': None, 'trakt_series_id': None, 'trakt_episode_id': None,
        'rotten_series_id': None, 'rotten_episode_id': None, 'metacritic_series_id': None, 'metacritic_episode_id': None,
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

    # Extract metadata from filename and NFO early, so we have ARR-relevant data even when ffprobe/MediaInfo fails
    _enrich_from_nfo_and_filename(path, result)

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

        nfo_candidates = find_kodi_nfo_candidates(path, result['media_type'])
        episode_nfo_candidates = [c for c in nfo_candidates if pathlib.Path(c).name.lower() != 'tvshow.nfo']
        result['nfo_missing'] = 0 if episode_nfo_candidates else 1
        if nfo_candidates:
            for nfo_path in nfo_candidates:
                nfo_data = parse_kodi_nfo(nfo_path)
                if not nfo_data:
                    continue
                is_tvshow_nfo = pathlib.Path(nfo_path).name.lower() == 'tvshow.nfo'
                if not result['year'] and nfo_data.get('year'):
                    result['year'] = nfo_data['year']
                nfo_media_type = (nfo_data.get('media_type') or '').strip().lower()
                if nfo_media_type == 'movie' and not is_tvshow_nfo:
                    # A concrete movie NFO must win over stale/guessed TV typing.
                    result['media_type'] = 'movie'
                    result['show_title'] = None
                    result['season'] = None
                    result['episode'] = None
                    result['episode_title'] = None
                elif not result['media_type'] and nfo_media_type:
                    # Never let series-level tvshow.nfo classify non-TV files by itself.
                    if not (is_tvshow_nfo and media_type_guess != 'tv'):
                        result['media_type'] = nfo_media_type
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
                if is_tvshow_nfo:
                    for k in ('tvdb', 'imdb', 'tmdb', 'trakt', 'rotten', 'metacritic'):
                        val = nfo_data.get(f'{k}_id')
                        if val:
                            result[f'{k}_series_id'] = val
                else:
                    # Only set episode_id for TV episode NFOs; movie NFOs have imdb_id/tmdb_id etc. which go to main columns only
                    if nfo_data.get('media_type') != 'movie':
                        for k in ('tvdb', 'imdb', 'tmdb', 'trakt', 'rotten', 'metacritic'):
                            val = nfo_data.get(f'{k}_id')
                            if val:
                                result[f'{k}_episode_id'] = val
                if result.get('media_type') != 'tv':
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

        # Filename is fallback only when NFO/current metadata did not identify type.
        media_type_now = (result.get('media_type') or '').strip().lower()
        if not media_type_now and media_type_guess:
            result['media_type'] = media_type_guess
            media_type_now = media_type_guess
        if media_type_now == 'tv':
            if season_guess is not None and result.get('season') is None:
                result['season'] = season_guess
            if episode_guess is not None and result.get('episode') is None:
                result['episode'] = episode_guess
        elif media_type_now == 'movie':
            # Keep movie rows clean from filename episode guesses.
            result['season'] = None
            result['episode'] = None
            result['episode_title'] = None

        if result['media_type'] == 'tv':
            if not result['show_title']:
                result['show_title'] = guess_show_title_from_path(path)

        if not result['movie_title'] and result['media_type'] != 'tv':
            movie_title_guess = guess_movie_title_from_filename(filename_base)
            if movie_title_guess:
                result['movie_title'] = movie_title_guess
                if not result['media_type']:
                    result['media_type'] = 'movie'
        # Movies must not have series/episode IDs; those columns are for TV only
        if result.get('media_type') == 'movie':
            for k in ('tvdb', 'imdb', 'tmdb', 'trakt', 'rotten', 'metacritic'):
                result[f'{k}_series_id'] = None
                result[f'{k}_episode_id'] = None
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

        # Extract Dolby Vision profile/compatibility from ffprobe side_data
        dovi_profile_from_ffprobe = None
        bl_compatibility_id = None
        dv_header = next((x for x in side_data if 'DOVI configuration record' in x.get('side_data_type', '')), None)
        if dv_header:
            # Try to get profile directly from ffprobe side_data payload
            for profile_key in ('dv_profile', 'dovi_profile', 'profile'):
                profile_val = dv_header.get(profile_key)
                if profile_val is not None and str(profile_val).strip():
                    dovi_profile_from_ffprobe = str(profile_val).strip()
                    break
            # Some ffprobe builds only include profile in side_data_type text
            if not dovi_profile_from_ffprobe:
                sd_type_text = str(dv_header.get('side_data_type', ''))
                m = re.search(r'profile[^0-9]*([0-9]{1,2})', sd_type_text, flags=re.IGNORECASE)
                if m:
                    dovi_profile_from_ffprobe = m.group(1)

            # Try multiple possible field names for compatibility ID
            bl_compatibility_id = (dv_header.get('compatibility_id') or 
                                  dv_header.get('dv_bl_signal_compatibility_id') or
                                  dv_header.get('bl_compatibility_id') or
                                  dv_header.get('compatibility'))
            if bl_compatibility_id is not None:
                bl_compatibility_id = str(bl_compatibility_id)
        
        # Check for enhancement layer streams (for FEL/MEL detection)
        video_streams = [s for s in probe_data.get('streams', []) if s.get('codec_type') == 'video']
        for stream in video_streams:
            codec_name = stream.get('codec_name', '').lower()
            tags = stream.get('tags', {}) or {}
            tag_blob = " ".join(str(v) for v in tags.values()).lower()
            # Check for enhancement layer indicators (name/tags); dual HEVC alone is not enough
            if (
                'enhancement' in codec_name
                or 'enhancement' in tag_blob
                or str(tags.get('enhancement', '')).strip()
                or 'el' == str(tags.get('title', '')).strip().lower()
                or 'enhancement' in str(tags.get('title', '')).lower()
            ):
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
        
        def _rank_dovi_info(info: dict | None, size: int) -> tuple:
            """Prefer FEL, then any profile, then larger RPU."""
            if not info:
                return (0, 0, size)
            el = str(info.get('el_type') or '').upper()
            has_prof = 1 if info.get('dovi_profile') is not None else 0
            fel = 2 if 'FEL' in el or el in ('F', 'FULL') else (1 if el else 0)
            return (fel, has_prof, size)

        def _extract_rpu_via_ffmpeg(video_map_index: int | None) -> tuple[dict | None, int]:
            """Extract RPU from one video map (None = ffmpeg default video)."""
            rpu_fd, rpu_path = tempfile.mkstemp(suffix='_rpu.bin', prefix='dovi_')
            local_data = None
            local_size = 0
            p1 = None
            p2 = None
            try:
                os.close(rpu_fd)
                if ABORT_SCAN:
                    raise RuntimeError("Scan Aborted")
                ffmpeg_cmd = ['ffmpeg', '-i', path]
                if video_map_index is not None:
                    ffmpeg_cmd += ['-map', f'0:v:{video_map_index}']
                ffmpeg_cmd += ['-c:v', 'copy', '-to', '2', '-f', 'hevc', '-y', '-']
                dovi_extract = ['dovi_tool', 'extract-rpu', '-', '-o', rpu_path]

                p1 = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, start_new_session=True)
                with proc_lock:
                    ACTIVE_PROCS.add(p1)
                p2 = subprocess.Popen(dovi_extract, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, start_new_session=True)
                with proc_lock:
                    ACTIVE_PROCS.add(p2)
                p1.stdout.close()
                if ABORT_SCAN:
                    raise RuntimeError("Scan Aborted")
                p2.communicate()
                with proc_lock:
                    ACTIVE_PROCS.discard(p1)
                    ACTIVE_PROCS.discard(p2)
                if p2.returncode == 0 and os.path.exists(rpu_path):
                    local_size = os.path.getsize(rpu_path)
                    if local_size > 0:
                        rc_info, out_info, _ = run_command(['dovi_tool', 'info', '-i', rpu_path, '-f', '0'])
                        json_start = out_info.find('{')
                        if json_start != -1:
                            local_data = json.loads(out_info[json_start:])
            finally:
                with proc_lock:
                    if p1 is not None:
                        ACTIVE_PROCS.discard(p1)
                    if p2 is not None:
                        ACTIVE_PROCS.discard(p2)
                if os.path.exists(rpu_path):
                    try:
                        os.remove(rpu_path)
                    except OSError as e:
                        if DEBUG_MODE:
                            log_debug(f"Failed to remove RPU file {rpu_path}: {e}", "WARNING")
            return local_data, local_size

        # If not in cache, extract RPU — try default then each video stream (DT-DL EL may not be v:0)
        if not dovi_data:
            try:
                map_indexes: list[int | None]
                n_vid = len(video_streams)
                if n_vid > 1:
                    # DT-DL: RPU is usually on the EL track (not v:0 / ffmpeg default).
                    # Try secondary video maps first, then BL, then default.
                    map_indexes = list(range(1, n_vid)) + [0, None]
                else:
                    map_indexes = [None]
                    if n_vid == 1:
                        map_indexes.append(0)
                best_data = None
                best_size = 0
                best_rank = (-1, -1, -1)
                for map_idx in map_indexes:
                    if ABORT_SCAN:
                        raise RuntimeError("Scan Aborted")
                    cand_data, cand_size = _extract_rpu_via_ffmpeg(map_idx)
                    rank = _rank_dovi_info(cand_data, cand_size)
                    if rank > best_rank:
                        best_rank = rank
                        best_data = cand_data
                        best_size = cand_size
                    # Stop early on clear FEL
                    if cand_data and 'FEL' in str(cand_data.get('el_type') or '').upper():
                        break
                dovi_data = best_data
                rpu_size = best_size
                if dovi_data:
                    with rpu_cache_lock:
                        if len(RPU_CACHE) >= RPU_CACHE_MAX_SIZE:
                            oldest_key = next(iter(RPU_CACHE))
                            del RPU_CACHE[oldest_key]
                            if DEBUG_MODE:
                                log_debug(f"RPU cache full, evicted oldest entry. Cache size: {len(RPU_CACHE)}", "DEBUG")
                        RPU_CACHE[cache_key] = {'dovi_data': dovi_data, 'rpu_size': rpu_size}
                        if DEBUG_MODE:
                            log_debug(f"Cached RPU data for {path} (cache size: {len(RPU_CACHE)})", "DEBUG")
            except RuntimeError:
                raise
            except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as e:
                if DEBUG_MODE:
                    log_debug(f"RPU extraction error for {path}: {e}", "ERROR")
        
        # Store dovi_data for processing after all tests are complete
        dovi_profile_raw = None
        dovi_el_type_raw = None
        mi_el_hint = None
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
        elif dovi_profile_from_ffprobe:
            dovi_profile_raw = dovi_profile_from_ffprobe
            if DEBUG_MODE:
                log_debug(f"Dolby Vision detected from ffprobe DOVI side_data: profile={dovi_profile_raw}", "DEBUG")
        
        # Note: Format determination happens AFTER MediaInfo to ensure all sources are checked
        # This will be done at the end after MediaInfo has a chance to add to sec_hdrs
        def parse_dv_profile_from_mediainfo(*values: Any) -> str | None:
            """
            Parse Dolby Vision profile hints from MediaInfo HDR fields.

            Handles tokens like:
              - dvhe.08.06
              - dvav.10.01 / dva1.10.04
              - Profile 10 / Profile 10.1 / Profile 20
            """
            parts = [str(v) for v in values if v]
            if not parts:
                return None
            text = " ".join(parts)
            lower = text.lower()

            if not any(tok in lower for tok in ("dolby vision", "dovi", "dvhe", "dvh1", "dvav", "dva1", "dav1")):
                return None

            # Codec/profile token format:
            #   dvav.10.01, dva1.10.04, dvhe.08.04, dav1.10, dvh1.20
            m = re.search(r'(?:dv(?:he|h1|av|a1)|dav1)\.(\d{1,2})(?:\.(\d{2}))?', lower)
            if m:
                profile_num = str(int(m.group(1)))
                compat_raw = m.group(2)
                if compat_raw is not None:
                    compat_num = str(int(compat_raw))
                    if compat_num == "1":
                        return f"{profile_num}.1"
                    if compat_num == "4":
                        return f"{profile_num}.4"
                return profile_num

            # Free-form "Profile 10.1" / "Profile 20" style
            m = re.search(r'profile\s*([0-9]{1,2}(?:\.[0-9])?)', lower)
            if m:
                return m.group(1)

            # Filename-like "DOVI P10.1" / "DOVI P20" style
            m = re.search(r'\bdovi?\s*p?([0-9]{1,2}(?:\.[0-9])?)\b', lower)
            if m:
                return m.group(1)

            return None

        # 3. MEDIAINFO (Raw CLI Parsing)
        # Note: HAS_MEDIAINFO check removed because we now use CLI which is always installed in Docker
        # MediaInfo can hang on certain files, so we make it optional - skip if it times out or errors
        rc_mi = -1
        out_mi = ""
        try:
            if DEBUG_MODE: log_debug(f"[MEDIAINFO] Starting mediainfo for: {path}", "DEBUG")
            try:
                rc_mi, out_mi, _ = run_command(['mediainfo', '--Output=JSON', path], timeout_seconds=MEDIAINFO_TIMEOUT)
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
                            hdr_profile = t.get('HDR_Format_Profile', '')
                            hdr_format_string = t.get('HDR_Format_String', '')
                            hdr_settings = t.get('HDR_Format_Settings', '')
                            hdr_version = t.get('HDR_Format_Version', '')

                            # AV1 Dolby Vision can be present in MediaInfo strings even when dovi_tool
                            # cannot extract RPU (e.g. dvav/dva1 profile markers).
                            if not dovi_profile_raw:
                                mi_dv_profile = parse_dv_profile_from_mediainfo(
                                    hdr_format, hdr_compat, hdr_profile, hdr_format_string, hdr_settings, hdr_version
                                )
                                if mi_dv_profile:
                                    dovi_profile_raw = mi_dv_profile
                                    if DEBUG_MODE:
                                        log_debug(
                                            f"Dolby Vision detected from MediaInfo fields: profile={mi_dv_profile}",
                                            "DEBUG"
                                        )

                            # FEL/MEL from MediaInfo settings (e.g. "BL+EL+RPU", "EL+RPU", "FEL", "MEL")
                            settings_u = str(hdr_settings or '').upper()
                            if settings_u:
                                if (
                                    'FEL' in settings_u
                                    or 'BL+EL' in settings_u
                                    or 'EL+RPU' in settings_u
                                    or re.search(r'(?<![A-Z])EL(?![A-Z])', settings_u)
                                ):
                                    mi_el_hint = 'FEL'
                                elif mi_el_hint is None and (
                                    'MEL' in settings_u
                                    or ('BL+RPU' in settings_u and 'EL' not in settings_u)
                                ):
                                    mi_el_hint = 'MEL'

                            # Check HDR_Format_Compatibility first (most reliable for hybrids)
                            if hdr_compat:
                                compat_str = str(hdr_compat).upper()
                                if 'HDR10+' in compat_str or 'HDR10PLUS' in compat_str:
                                    if "HDR10+" not in sec_hdrs:
                                        sec_hdrs.append("HDR10+")
                                        if DEBUG_MODE:
                                            log_debug(f"HDR10+ detected from MediaInfo HDR_Format_Compatibility: {hdr_compat}", "DEBUG")
                                if 'HDR10' in compat_str and "HDR10" not in sec_hdrs:
                                    sec_hdrs.append("HDR10")
                                if 'HLG' in compat_str:
                                    is_hlg_base = True
                                    if "HLG" not in sec_hdrs:
                                        sec_hdrs.append("HLG")
                            
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

        # ISOBMFF dvcC/dvvC/dvwC — authoritative delivery profile for MP4/MOV.
        # Profile 20 RPU metadata often looks like Profile 5; trust the container box.
        isom_dovi = None
        try:
            ext = pathlib.Path(path).suffix.lower()
            if ext in _ISOM_CONTAINER_EXTS:
                isom_dovi = parse_isom_dovi_config(path)
        except Exception as e:
            if DEBUG_MODE:
                log_debug(f"[ISOM-DOVI] parse failed for {path}: {e}", "WARNING")
            isom_dovi = None
        if isom_dovi:
            if isom_dovi.get("is_stereo"):
                result["is_3d"] = 1
            if isom_dovi.get("dovi_profile"):
                isom_prof = str(isom_dovi["dovi_profile"])
                if dovi_profile_raw and dovi_profile_raw != isom_prof and DEBUG_MODE:
                    log_debug(
                        f"ISOMBF {isom_dovi.get('box_type')} profile {isom_prof} "
                        f"overrides prior profile {dovi_profile_raw}",
                        "DEBUG",
                    )
                dovi_profile_raw = isom_prof
            if isom_dovi.get("bl_compatibility_id") is not None:
                bl_compatibility_id = str(isom_dovi["bl_compatibility_id"])

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
            if dovi_profile_raw in ("8", "10"):
                # For profiles 8/10:
                #   *.4 => HLG compatibility (HLG base layer or bl_id=4)
                #   *.1 => HDR10 compatibility (bl_id=1 or HDR10/HDR10+ signals)
                #   bare profile => no compatibility hint available
                profile_prefix = dovi_profile_raw
                if is_hlg_base:
                    result['dovi_profile'] = f"{profile_prefix}.4"
                    if DEBUG_MODE:
                        log_debug(f"Detected P{profile_prefix}.4 (HLG base layer, bl_id={bl_id})", "DEBUG")
                elif bl_id == "4":
                    result['dovi_profile'] = f"{profile_prefix}.4"
                    if DEBUG_MODE:
                        log_debug(f"Detected P{profile_prefix}.4 (bl_id=4)", "DEBUG")
                elif bl_id == "1":
                    result['dovi_profile'] = f"{profile_prefix}.1"
                else:
                    # Check sec_hdrs for highest level (HDR10+ > HDR10)
                    if "HDR10+" in sec_hdrs:
                        result['dovi_profile'] = f"{profile_prefix}.1"
                    elif "HDR10" in sec_hdrs:
                        result['dovi_profile'] = f"{profile_prefix}.1"
                    else:
                        result['dovi_profile'] = profile_prefix
            else:
                result['dovi_profile'] = dovi_profile_raw

            # Profile 20 is MV-HEVC stereoscopic Dolby Vision
            if str(result.get('dovi_profile') or '').split('.')[0] == '20':
                result['is_3d'] = 1
            
            # Set EL type (RPU first; MediaInfo BL+EL/FEL/MEL as fallback) — P7 only
            if not dovi_el_type_raw and mi_el_hint:
                dovi_el_type_raw = mi_el_hint
                if DEBUG_MODE:
                    log_debug(f"EL type from MediaInfo HDR_Format_Settings: {mi_el_hint}", "DEBUG")
            prof_base = str(result.get('dovi_profile') or '').split('.')[0]
            if prof_base == '7':
                result['dovi_el_type'] = dovi_el_type_raw
            else:
                # FEL/MEL are Profile 7 concepts; ignore for P5/P20/etc.
                result['dovi_el_type'] = None
            
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
            err_result = {
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
                "tvdb_series_id": None, "tvdb_episode_id": None, "imdb_series_id": None, "imdb_episode_id": None,
                "tmdb_series_id": None, "tmdb_episode_id": None, "trakt_series_id": None, "trakt_episode_id": None,
                "rotten_series_id": None, "rotten_episode_id": None, "metacritic_series_id": None, "metacritic_episode_id": None,
                "imdb_rating": None, "tvdb_rating": None, "tmdb_rating": None, "rotten_rating": None, "metacritic_rating": None, "trakt_rating": None,
                "scan_attempts": 0,
                "video_source": None, "source_format": None, "video_codec": None,
                "is_3d": 0, "edition": None, "year": None, "media_type": None,
                "show_title": None, "season": None, "episode": None, "movie_title": None, "episode_title": None,
                "nfo_missing": 1,
                "validation_flag": None
            }
            _enrich_from_nfo_and_filename(full_path_str, err_result)
            return err_result
        if not os.access(full_path_str, os.R_OK):
            if DEBUG_MODE:
                log_debug(f"File not accessible: {full_path_str}", "ERROR")
            err_result = {
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
                "tvdb_series_id": None, "tvdb_episode_id": None, "imdb_series_id": None, "imdb_episode_id": None,
                "tmdb_series_id": None, "tmdb_episode_id": None, "trakt_series_id": None, "trakt_episode_id": None,
                "rotten_series_id": None, "rotten_episode_id": None, "metacritic_series_id": None, "metacritic_episode_id": None,
                "imdb_rating": None, "tvdb_rating": None, "tmdb_rating": None, "rotten_rating": None, "metacritic_rating": None, "trakt_rating": None,
                "scan_attempts": 0,
                "video_source": None, "source_format": None, "video_codec": None,
                "is_3d": 0, "edition": None, "year": None, "media_type": None,
                "show_title": None, "season": None, "episode": None, "movie_title": None, "episode_title": None,
                "nfo_missing": 1, "validation_flag": None
            }
            _enrich_from_nfo_and_filename(full_path_str, err_result)
            return err_result
    except (OSError, UnicodeEncodeError, UnicodeDecodeError) as e:
        if DEBUG_MODE:
            log_debug(f"File validation error for {full_path_str}: {e}", "ERROR")
        err_result = {
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
            "tvdb_series_id": None, "tvdb_episode_id": None, "imdb_series_id": None, "imdb_episode_id": None,
            "tmdb_series_id": None, "tmdb_episode_id": None, "trakt_series_id": None, "trakt_episode_id": None,
            "rotten_series_id": None, "rotten_episode_id": None, "metacritic_series_id": None, "metacritic_episode_id": None,
            "imdb_rating": None, "tvdb_rating": None, "tmdb_rating": None, "rotten_rating": None, "metacritic_rating": None, "trakt_rating": None,
            "scan_attempts": 0,
            "video_source": None, "source_format": None, "video_codec": None,
            "is_3d": 0, "edition": None, "year": None, "media_type": None,
            "show_title": None, "season": None, "episode": None, "movie_title": None, "episode_title": None,
            "nfo_missing": 1, "validation_flag": None
        }
        _enrich_from_nfo_and_filename(full_path_str, err_result)
        return err_result
    
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
    if meta.get('error'):
        _enrich_from_nfo_and_filename(full_path_str, meta)
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
        "tvdb_series_id": None if meta.get('media_type') == 'movie' else meta.get('tvdb_series_id'),
        "tvdb_episode_id": None if meta.get('media_type') == 'movie' else meta.get('tvdb_episode_id'),
        "imdb_series_id": None if meta.get('media_type') == 'movie' else meta.get('imdb_series_id'),
        "imdb_episode_id": None if meta.get('media_type') == 'movie' else meta.get('imdb_episode_id'),
        "tmdb_series_id": None if meta.get('media_type') == 'movie' else meta.get('tmdb_series_id'),
        "tmdb_episode_id": None if meta.get('media_type') == 'movie' else meta.get('tmdb_episode_id'),
        "trakt_series_id": None if meta.get('media_type') == 'movie' else meta.get('trakt_series_id'),
        "trakt_episode_id": None if meta.get('media_type') == 'movie' else meta.get('trakt_episode_id'),
        "rotten_series_id": None if meta.get('media_type') == 'movie' else meta.get('rotten_series_id'),
        "rotten_episode_id": None if meta.get('media_type') == 'movie' else meta.get('rotten_episode_id'),
        "metacritic_series_id": None if meta.get('media_type') == 'movie' else meta.get('metacritic_series_id'),
        "metacritic_episode_id": None if meta.get('media_type') == 'movie' else meta.get('metacritic_episode_id'),
        "imdb_rating": meta.get('imdb_rating'), "tvdb_rating": meta.get('tvdb_rating'), "tmdb_rating": meta.get('tmdb_rating'),
        "rotten_rating": meta.get('rotten_rating'), "metacritic_rating": meta.get('metacritic_rating'), "trakt_rating": meta.get('trakt_rating'),
        "scan_attempts": 0,  # Will be updated in run_scan based on previous attempts
        "video_source": meta['video_source'], "source_format": meta['source_format'], "video_codec": meta['video_codec'],
        "is_3d": meta['is_3d'], "edition": meta['edition'], "year": meta['year'],
        "media_type": meta.get('media_type'), "show_title": meta.get('show_title'),
        "season": meta.get('season'), "episode": meta.get('episode'),
        "movie_title": meta.get('movie_title'), "episode_title": meta.get('episode_title'),
        "nfo_missing": meta.get('nfo_missing', 1),
        "missing": 0,
        "validation_flag": validation_flag
    }

def build_backfill_metadata(file_path: str, filename: str, current: dict) -> dict:
    filename_base = filename
    filename_lower = filename_base.lower()
    result: dict[str, Any] = {}

    filename_meta = parse_filename_metadata(filename_base)
    media_type_guess, season_guess, episode_guess = parse_tv_from_filename(filename_lower)
    if not current.get('year') and filename_meta.get('year'):
        result['year'] = filename_meta['year']

    nfo_candidates = find_kodi_nfo_candidates(file_path, current.get('media_type') or result.get('media_type'))
    for nfo_path in nfo_candidates:
        nfo_data = parse_kodi_nfo(nfo_path)
        if not nfo_data:
            continue
        is_tvshow_nfo = pathlib.Path(nfo_path).name.lower() == 'tvshow.nfo'
        if not current.get('year') and nfo_data.get('year'):
            result['year'] = nfo_data['year']
        nfo_media_type = (nfo_data.get('media_type') or '').strip().lower()
        current_media_type = (current.get('media_type') or '').strip().lower()
        if nfo_media_type == 'movie' and not is_tvshow_nfo and current_media_type != 'movie':
            # Backfill correction: flip stale TV rows to movie when movie NFO is present.
            result['media_type'] = 'movie'
            result['show_title'] = None
            result['season'] = None
            result['episode'] = None
            result['episode_title'] = None
        elif not current.get('media_type') and nfo_media_type:
            # Never let series-level tvshow.nfo classify non-TV files by itself.
            if not (is_tvshow_nfo and media_type_guess != 'tv'):
                result['media_type'] = nfo_media_type
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
        if is_tvshow_nfo:
            for k in ('tvdb', 'imdb', 'tmdb', 'trakt', 'rotten', 'metacritic'):
                val = nfo_data.get(f'{k}_id')
                if val and current.get(f'{k}_series_id') is None:
                    result[f'{k}_series_id'] = val
        else:
            # Only set episode_id for TV episode NFOs; movie NFOs must not write to episode_id
            if nfo_data.get('media_type') != 'movie':
                for k in ('tvdb', 'imdb', 'tmdb', 'trakt', 'rotten', 'metacritic'):
                    val = nfo_data.get(f'{k}_id')
                    if val and current.get(f'{k}_episode_id') is None:
                        result[f'{k}_episode_id'] = val
        for key in (
            'imdb_id', 'tvdb_id', 'tmdb_id', 'rotten_id', 'metacritic_id', 'trakt_id',
            'imdb_rating', 'tvdb_rating', 'tmdb_rating', 'rotten_rating', 'metacritic_rating', 'trakt_rating'
        ):
            if current.get(key) is None and nfo_data.get(key) is not None:
                if key.endswith('_id') and (current.get('media_type') == 'tv' or result.get('media_type') == 'tv'):
                    continue
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

    # Filename is fallback only when NFO/current metadata did not identify type.
    media_type_now = (current.get('media_type') or result.get('media_type') or '').strip().lower()
    if not media_type_now and media_type_guess:
        result['media_type'] = media_type_guess
        media_type_now = media_type_guess
    if media_type_now == 'tv':
        if season_guess is not None and current.get('season') is None and result.get('season') is None:
            result['season'] = season_guess
        if episode_guess is not None and current.get('episode') is None and result.get('episode') is None:
            result['episode'] = episode_guess
    elif media_type_now == 'movie':
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

def save_batch_to_db(data_list: list, duplicate_check_on_scan: Optional[bool] = None) -> None:
    """
    Save a batch of video metadata to the database.
    
    Args:
        data_list: List of dictionaries containing video metadata
    """
    if not data_list: return
    
    # Resolve duplicate-check behavior (default off unless setting enabled).
    if duplicate_check_on_scan is None:
        try:
            with get_db_readonly() as conn:
                row = conn.execute("SELECT value FROM settings WHERE key='duplicate_check_on_scan'").fetchone()
                duplicate_check_on_scan = str((row[0] if row else 'false')).lower() == 'true'
        except Exception:
            duplicate_check_on_scan = False

    # Ensure missing=0 for all saved files (they exist on disk)
    for item in data_list:
        item.setdefault('missing', 0)
        if duplicate_check_on_scan:
            item['dup_group_key'] = build_duplicate_group_key(item)
            if not item.get('dup_exact_key'):
                item['dup_exact_key'] = build_duplicate_exact_key(item.get('full_path'), item.get('file_size'))
        else:
            item.setdefault('dup_group_key', None)
            item.setdefault('dup_exact_key', None)
        item.setdefault('dup_count', 0)
    
    # Sanitize all string values in the data before insertion
    sanitized_list = [sanitize_dict_for_db(item) for item in data_list]
    
    try:
        with get_db() as conn:
            conn.executemany("""INSERT OR REPLACE INTO videos 
                (filename, category, profile, el_type, container, source_vol, full_path, last_scanned, 
                 resolution, bitrate_mbps, scan_error, is_hybrid, is_source_hybrid, secondary_hdr, width, height, 
                 file_size, bl_compatibility_id, audio_codecs, audio_langs, audio_channels, subtitles, max_cll, max_fall, fps, aspect_ratio,
                 imdb_id, tvdb_id, tmdb_id, rotten_id, metacritic_id, trakt_id,
                 tvdb_series_id, tvdb_episode_id, imdb_series_id, imdb_episode_id, tmdb_series_id, tmdb_episode_id,
                 trakt_series_id, trakt_episode_id, rotten_series_id, rotten_episode_id, metacritic_series_id, metacritic_episode_id,
                 imdb_rating, tvdb_rating, tmdb_rating, rotten_rating, metacritic_rating, trakt_rating,
                 scan_attempts, video_source, source_format, video_codec, is_3d, edition, year, media_type, show_title, season, episode, movie_title, episode_title, nfo_missing, missing, validation_flag, dup_group_key, dup_exact_key, dup_count) 
                VALUES (:filename, :category, :profile, :el_type, :container, :source_vol, :full_path, :last_scanned, 
                 :resolution, :bitrate_mbps, :scan_error, :is_hybrid, :is_source_hybrid, :secondary_hdr, :width, :height, 
                 :file_size, :bl_compatibility_id, :audio_codecs, :audio_langs, :audio_channels, :subtitles, :max_cll, :max_fall, :fps, :aspect_ratio,
                 :imdb_id, :tvdb_id, :tmdb_id, :rotten_id, :metacritic_id, :trakt_id,
                 :tvdb_series_id, :tvdb_episode_id, :imdb_series_id, :imdb_episode_id, :tmdb_series_id, :tmdb_episode_id,
                 :trakt_series_id, :trakt_episode_id, :rotten_series_id, :rotten_episode_id, :metacritic_series_id, :metacritic_episode_id,
                 :imdb_rating, :tvdb_rating, :tmdb_rating, :rotten_rating, :metacritic_rating, :trakt_rating,
                 :scan_attempts, :video_source, :source_format, :video_codec, :is_3d, :edition, :year, :media_type, :show_title, :season, :episode, :movie_title, :episode_title, :nfo_missing, :missing, :validation_flag, :dup_group_key, :dup_exact_key, :dup_count)""", sanitized_list)
            if duplicate_check_on_scan:
                recompute_duplicate_counts(conn)
            if DEBUG_MODE:
                for item in sanitized_list:
                    log_debug(f"Saved to DB: {item['filename']} -> {item['category']} {item['profile']} (error: {item.get('scan_error', 'None')})", "DEBUG")
    except sqlite3.Error as e:
        log_debug(f"Database error saving batch: {e}", "ERROR")
        if DEBUG_MODE:
            log_debug(f"Failed batch items: {[item.get('filename', 'unknown') for item in sanitized_list]}", "ERROR")

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

def parse_skip_rules(skip_tokens: list) -> tuple[list[str], list[str], list[tuple[str, str]]]:
    """
    Parse IGNORE tokens into file and folder skip rules.

    Prefixes:
      (none)  file only — substring match on filename
      /       folder only — exact name, or fnmatch if pattern contains *
      %       file + folder — files: substring (or fnmatch if *); folders: contains (or fnmatch if *)

    Returns:
        (file_substrings, file_globs, folder_rules) where folder_rules are
        (mode, pattern) with mode in {'exact', 'contains', 'glob'}.
    """
    file_subs: list[str] = []
    file_globs: list[str] = []
    folder_rules: list[tuple[str, str]] = []
    for raw in skip_tokens or []:
        tok = (raw or "").strip().lower()
        if not tok or tok in {"/", "%"}:
            continue
        if tok.startswith("/"):
            pat = tok[1:].strip()
            if not pat:
                continue
            folder_rules.append(("glob" if "*" in pat else "exact", pat))
        elif tok.startswith("%"):
            pat = tok[1:].strip()
            if not pat:
                continue
            if "*" in pat:
                file_globs.append(pat)
                folder_rules.append(("glob", pat))
            else:
                file_subs.append(pat)
                folder_rules.append(("contains", pat))
        else:
            if "*" in tok:
                file_globs.append(tok)
            else:
                file_subs.append(tok)
    return file_subs, file_globs, folder_rules

def folder_matches_skip_rules(dirname: str, folder_rules: list[tuple[str, str]]) -> bool:
    """Return True if directory name should be pruned from the scan walk."""
    if not folder_rules:
        return False
    name = (dirname or "").lower()
    for mode, pat in folder_rules:
        if mode == "exact" and name == pat:
            return True
        if mode == "contains" and pat in name:
            return True
        if mode == "glob" and fnmatch.fnmatch(name, pat):
            return True
    return False

def file_matches_skip_rules(filename: str, file_subs: list[str], file_globs: list[str]) -> bool:
    """Return True if filename should be skipped (IGNORE file rules)."""
    fl = (filename or "").lower()
    if any(s in fl for s in file_subs):
        return True
    if any(fnmatch.fnmatch(fl, g) for g in file_globs):
        return True
    return False

def collect_files_to_scan(scan_paths: list, path_to_vol: dict, processed_map: dict, 
                          skip_words: list, min_size: int, force_rescan: bool, start_time: float,
                          scan_extras: bool) -> tuple[list, set]:
    """
    Scan directories and collect files that need to be analyzed.
    
    Args:
        scan_paths: List of paths to scan
        path_to_vol: Mapping of paths to volume names
        processed_map: Dictionary of already processed files
        skip_words: IGNORE tokens (optional / folder-only or % file+folder prefixes)
        min_size: Minimum file size in bytes
        force_rescan: Whether to force rescan of all files
        start_time: Scan start time for progress updates
    
    Returns:
        Tuple of (files_to_scan list, all_found_files set)
    """
    files_to_scan = []
    all_found_files = set() 
    total_seen = 0
    last_vol_started = None
    vol_start_time = 0.0
    file_subs, file_globs, folder_rules = parse_skip_rules(skip_words)
    
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
        
        if current_vol != last_vol_started:
            last_vol_started = current_vol
            vol_start_time = time.time()
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
                # After 3s per volume or when we find files, switch from "Starting" to "Found..."
                # Throttle: every 50 dirs when no files yet; every 1 or 500 when we have files
                elapsed_vol = time.time() - vol_start_time
                show_found = elapsed_vol >= 3.0 or total_seen >= 1
                throttle = (total_seen == 0 and dir_count % 50 == 0) or (total_seen == 1) or (total_seen > 1 and total_seen % 500 == 0)
                if show_found and throttle:
                    elapsed = int(time.time() - start_time)
                    with progress_lock:
                        PROGRESS["file"] = f"Scanning [{current_vol}]: Found {total_seen} files ({len(files_to_scan)} new)"
                        PROGRESS["last_duration"] = f"{elapsed}s"
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
                if folder_rules:
                    kept = []
                    for d in dirs:
                        if folder_matches_skip_rules(d, folder_rules):
                            if DEBUG_MODE:
                                log_debug(f"Skipping folder (IGNORE): {os.path.join(root, d)}", "DEBUG")
                            continue
                        kept.append(d)
                    dirs[:] = kept
                
                for f in files:
                    wait_if_paused()
                    ext = os.path.splitext(f)[1].lower()
                    if ext not in VIDEO_EXTENSIONS:
                        continue
                    
                    total_seen += 1
                    full_p = os.path.join(root, f)
                    
                    if file_matches_skip_rules(f, file_subs, file_globs):
                        if DEBUG_MODE:
                            log_debug(f"Skipping file (IGNORE): {full_p}", "DEBUG")
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
    duplicate_check_on_scan = str(settings.get('duplicate_check_on_scan', 'false')).lower() == 'true'
            
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
                    save_batch_to_db(batch_buffer, duplicate_check_on_scan=duplicate_check_on_scan)
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
        save_batch_to_db(batch_buffer, duplicate_check_on_scan=duplicate_check_on_scan)

    if progress_updates["current"] > 0:
        with progress_lock:
            PROGRESS["current"] += progress_updates["current"]
            PROGRESS["failed_count"] += progress_updates["failed_count"]
            PROGRESS["new_found"] += progress_updates["new_found"]
    
    return {"metrics_sum": metrics_sum, "metrics_count": metrics_count}

def count_removed_files(target_vols: list | None, scan_paths: list, all_found_files: set) -> int:
    """
    Count files in DB that would be removed (no longer on disk).
    Does not modify the database.
    """
    with get_db() as conn:
        if target_vols and len(target_vols) > 0:
            placeholders = ','.join('?' * len(target_vols))
            sql = f"SELECT full_path FROM videos WHERE source_vol IN ({placeholders})"
            existing_db_files = {row[0] for row in conn.execute(sql, tuple(target_vols)).fetchall()}
        else:
            online_prefixes = tuple(scan_paths)
            all_rows = conn.execute("SELECT full_path FROM videos").fetchall()
            existing_db_files = {r[0] for r in all_rows if r[0].startswith(online_prefixes)}
    return sum(1 for f in existing_db_files if f not in all_found_files)


def cleanup_deleted_files(target_vols: list | None, scan_paths: list, all_found_files: set, remove_from_db: bool = True) -> int:
    """
    Remove or mark files from database that no longer exist on disk.
    
    Args:
        target_vols: List of target volume names, or None
        scan_paths: List of scan paths
        all_found_files: Set of all files found during scan
        remove_from_db: If True, delete rows; if False, set missing=1
    
    Returns:
        Number of files removed or marked missing
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
    
    removed = len(to_del)
    if to_del:
        with get_db() as conn:
            if remove_from_db:
                log_debug(f"Removing {removed} missing files from DB...", "INFO")
                conn.executemany("DELETE FROM videos WHERE full_path=?", [(f,) for f in to_del])
            else:
                log_debug(f"Marking {removed} missing files in DB...", "INFO")
                conn.executemany("UPDATE videos SET missing = 1 WHERE full_path = ?", [(f,) for f in to_del])
    return removed

def finalize_scan(metrics_sum: dict, metrics_count: dict, start_time: float,
                  scan_mode: str, target_vols: Optional[List[str]],
                  scan_folder: dict | None, removed: int = 0, remove_missing_from_db: bool = True) -> None:
    """
    Finalize scan by updating database settings and PROGRESS state.
    
    Args:
        metrics_sum: Dictionary of accumulated metrics
        metrics_count: Dictionary of metric counts
        start_time: Scan start time
    """
    dur = f"{int(time.time() - start_time)}s"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dup_files = 0
    dup_groups = 0
    with get_db() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_full_scan', ?)", (now,))
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_duration', ?)", (dur,))
        dup_files = conn.execute("SELECT COUNT(*) FROM videos WHERE COALESCE(dup_count, 0) > 1").fetchone()[0]
        dup_groups = conn.execute(
            "SELECT COUNT(DISTINCT dup_group_key) FROM videos WHERE COALESCE(dup_count, 0) > 1 AND dup_group_key IS NOT NULL AND dup_group_key != ''"
        ).fetchone()[0]
            
    avg_bitrate = round(metrics_sum["bitrate"] / metrics_count["bitrate"], 2) if metrics_count["bitrate"] > 0 else 0
    avg_width = round(metrics_sum["width"] / metrics_count["width"]) if metrics_count["width"] > 0 else 0
    avg_height = round(metrics_sum["height"] / metrics_count["height"]) if metrics_count["height"] > 0 else 0
    avg_file_size_mb = round(metrics_sum["file_size"] / metrics_count["file_size"] / (1024 * 1024), 2) if metrics_count["file_size"] > 0 else 0
            
    with progress_lock:
        PROGRESS.update({"last_full_scan": now, "last_duration": dur, "scan_completed": True, "status": "idle", "paused": False})
        PROGRESS["last_report"] = {
            "scanned": PROGRESS["total"],
            "new": PROGRESS["new_found"],
            "removed": removed,
            "failed": PROGRESS["failed_count"],
            "warnings": PROGRESS.get("warning_count", 0),
            "duration": dur,
            "date": now,
            "avg_bitrate": avg_bitrate,
            "avg_width": avg_width,
            "avg_height": avg_height,
            "avg_file_size_mb": avg_file_size_mb,
            "remove_missing_from_db": remove_missing_from_db,
            "duplicates": dup_files,
            "duplicate_groups": dup_groups
        }
        history_entry = {
            "status": "complete",
            "duration": dur,
            "scanned": PROGRESS["total"],
            "new": PROGRESS["new_found"],
            "removed": removed,
            "failed": PROGRESS["failed_count"],
            "warnings": PROGRESS.get("warning_count", 0),
            "scan_mode": scan_mode,
            "target_vols": target_vols or [],
            "scan_folder": scan_folder.get("path") if isinstance(scan_folder, dict) else None,
            "remove_missing_from_db": remove_missing_from_db,
            "duplicates": dup_files,
            "duplicate_groups": dup_groups
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
        PROGRESS.update({"status": "scanning", "current": 0, "total": 0, "file": "Initializing...", "scan_completed": False, "new_found": 0, "removed": 0, "failed_count": 0, "warning_count": 0, "last_duration": "0s", "start_time": start_time})
    
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
        
        # Removed count only known after crawl completes (all_found_files is complete)
        removed = count_removed_files(target_vols, scan_paths, all_found_files) if not ABORT_SCAN else 0
        total_found = len(all_found_files)
        with progress_lock:
            PROGRESS["removed"] = removed
            PROGRESS["total_found"] = total_found
            PROGRESS["file"] = f"Found {total_found} ({len(files_to_scan)} new / {removed} removed)"
        
        metrics = {"metrics_sum": {"bitrate": 0.0, "width": 0, "height": 0, "file_size": 0},
                   "metrics_count": {"bitrate": 0, "width": 0, "height": 0, "file_size": 0}}
        
        if not ABORT_SCAN and files_to_scan:
            metrics = analyze_files(files_to_scan, processed_map, settings, final_threads, start_time)
        
        if not ABORT_SCAN:
            remove_missing_from_db = str(settings.get('remove_missing_from_db', 'true')).lower() == 'true'
            removed = cleanup_deleted_files(target_vols, scan_paths, all_found_files, remove_from_db=remove_missing_from_db)
            with progress_lock:
                PROGRESS["removed"] = removed
                total_found = PROGRESS.get("total_found", PROGRESS.get("total", 0))
                if total_found > 0:
                    PROGRESS["file"] = f"Found {total_found} ({PROGRESS['new_found']} new / {removed} removed)"
                elif removed > 0:
                    PROGRESS["file"] = f"Cleanup: {removed} removed"
            finalize_scan(metrics["metrics_sum"], metrics["metrics_count"], start_time, scan_mode, target_vols, scan_folder, removed, remove_missing_from_db)
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
                _now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                PROGRESS["last_report"] = {
                    "scanned": PROGRESS.get("current", 0),
                    "new": PROGRESS.get("new_found", 0),
                    "failed": PROGRESS.get("failed_count", 0),
                    "warnings": PROGRESS.get("warning_count", 0),
                    "duration": dur,
                    "date": _now,
                    "aborted": True,
                    "duplicates": 0,
                    "duplicate_groups": 0
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
                "scan_folder": scan_folder.get("path") if isinstance(scan_folder, dict) else None,
                "duplicates": 0,
                "duplicate_groups": 0
            })

    except Exception as e:
        log_debug(f"[ERROR] CRITICAL: {e}")
        import traceback; traceback.print_exc()
        with progress_lock: PROGRESS["status"] = "idle"

# --- ROUTES ---
@bp.route('/')
def index():
    return render_template('index.html', app_version_label=app_version_label())

@bp.route('/health')
@bp.route('/api/health')
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
    sonarr = _arr_service_status("sonarr", SONARR_URL, SONARR_API_KEY)
    radarr = _arr_service_status("radarr", RADARR_URL, RADARR_API_KEY)
    payload = {
        "status": status,
        "database": db_status,
        "scan_status": PROGRESS.get("status", "unknown"),
        "uptime_seconds": int(time.time() - APP_START_TIME),
        "version": app_version_label(),
        "tools": get_tool_versions(),
        "sonarr": sonarr,
        "radarr": radarr,
    }
    return jsonify(payload), (200 if status == "healthy" else 503)


@bp.app_errorhandler(400)
def api_bad_request(error) -> Response:
    if not request.path.startswith('/api/'):
        return error
    return jsonify({
        "status": "error",
        "error": "bad_request",
        "message": "Bad request"
    }), 400


@bp.app_errorhandler(404)
def api_not_found(error) -> Response:
    if not request.path.startswith('/api/'):
        return error
    return jsonify({
        "status": "error",
        "error": "not_found",
        "message": "Endpoint not found"
    }), 404


@bp.app_errorhandler(500)
def api_internal_error(error) -> Response:
    if not request.path.startswith('/api/'):
        return error
    return jsonify({
        "status": "error",
        "error": "internal_error",
        "message": "Internal server error"
    }), 500

@bp.route('/api/logs')
def get_logs() -> Response:
    """
    Get recent log entries from the in-memory log cache.
    
    Returns:
        JSON array of recent log messages
    """
    with progress_lock: 
        return jsonify(list(LOG_CACHE))


@bp.route('/api/log_client_error', methods=['POST'])
def log_client_error() -> Response:
    """
    Append a client-side error to the log cache so it appears in the in-app console.
    """
    payload = request.get_json(silent=True) or {}
    msg = payload.get("message") or payload.get("msg") or ""
    if msg:
        log_debug(f"[CLIENT] {msg}", "ERROR")
    return jsonify({"status": "ok"})


@bp.route('/api/scan_history')
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

@bp.route('/download_log')
def download_log() -> Union[Response, Tuple[str, int]]:
    """
    Download the current scan activity log file.
    
    Returns:
        File download response if log file exists, or 404 error message if not found
    """
    if LOG_FILE and os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, as_attachment=True, download_name=os.path.basename(LOG_FILE))
    return "No log found", 404

@bp.route('/download_failures')
def download_failures() -> Union[Response, Tuple[str, int]]:
    """Download the current scan failures CSV file."""
    if FAIL_FILE and os.path.exists(FAIL_FILE):
        return send_file(FAIL_FILE, as_attachment=True, download_name=os.path.basename(FAIL_FILE))
    return "No failures log found", 404

@bp.route('/api/failures')
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
                first_row = True
                for row in reader:
                    if len(row) < 5:
                        continue
                    ts, vol, path, name, msg = row[:5]
                    # Skip header row
                    if first_row and vol == 'Volume':
                        first_row = False
                        continue
                    first_row = False
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

@bp.route('/api/pre_scan_check')
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
        'missing': 'missing',
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
    mappings = [('search', 'filename'), ('category', 'category'), ('volume', 'source_vol'), ('profile', 'profile'), ('el', 'el_type'), ('container', 'container'), ('resolution', 'resolution'), ('status', 'scan_error'), ('audio', 'audio_codecs'), ('video_codec', 'video_codec'), ('video_source', 'video_source'), ('source_format', 'source_format'), ('edition', 'edition'), ('media_type', 'media_type'), ('nfo_missing', 'nfo_missing'), ('missing', 'missing')]
    for key, col in mappings:
        if key == exclude_key: continue
        val = args.get(key, '').strip()
        if val:
            if key == 'search':
                conditions.append(f"(LOWER({col}) LIKE ? OR LOWER(full_path) LIKE ?)")
                params.extend([f"%{val.lower()}%", f"%{val.lower()}%"])
            elif key == 'status': 
                values = [v.strip().lower() for v in val.split(',') if v.strip()]
                has_ok = 'ok' in values
                has_failed = 'failed' in values
                if has_ok and has_failed:
                    pass  # both selected = no status filter
                elif has_failed and not has_ok:
                    conditions.append("scan_error IS NOT NULL AND scan_error != ''")
                elif has_ok and not has_failed:
                    conditions.append("(scan_error IS NULL OR scan_error = '')")
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
                values = [v.strip().lower() for v in val.split(',') if v.strip()]
                want_missing = any(v in ('missing', 'none', '1', 'true', 'yes') for v in values)
                want_found = any(v in ('found', '0', 'false', 'no') for v in values)
                if want_missing and want_found:
                    pass
                elif want_missing:
                    conditions.append(f"{col} = 1")
                elif want_found:
                    conditions.append(f"{col} = 0")
            elif key == 'missing':
                values = [v.strip().lower() for v in val.split(',') if v.strip()]
                want_yes = any(v in ('yes', '1', 'true', 'y') for v in values)
                want_no = any(v in ('no', '0', 'false', 'n') for v in values)
                if want_yes and want_no:
                    pass
                elif want_yes:
                    conditions.append(f"{col} = 1")
                elif want_no:
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
        hyb_vals = [v.strip() for v in hyb.split(',') if v.strip()]
        if hyb_vals == ['1'] or hyb == "1":
            conditions.append("is_hybrid = 1")
        elif hyb_vals == ['0'] or hyb == "0":
            conditions.append("is_hybrid = 0 AND category != 'sdr_only'")
    if exclude_key != 'source_hybrid':
        src_hyb = args.get('source_hybrid', '').strip()
        src_vals = [v.strip() for v in src_hyb.split(',') if v.strip()]
        if src_vals == ['1'] or src_hyb == "1":
            conditions.append("is_source_hybrid = 1")
        elif src_vals == ['0'] or src_hyb == "0":
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
            vals = [v.strip() for v in is_3d_val.split(',') if v.strip()]
            if vals == ['1'] or is_3d_val == '1':
                conditions.append("is_3d = 1")
            elif vals == ['0'] or is_3d_val == '0':
                conditions.append("is_3d = 0")
    
    return " AND ".join(conditions), params

def parse_sort_order(value: Any, default: str = "desc") -> str:
    """Return a safe SQL sort direction."""
    order = str(value or default).strip().lower()
    return "ASC" if order == "asc" else "DESC"

def parse_positive_int(value: Any, default: int, max_value: int) -> int:
    """Parse a positive integer request value and clamp it."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(1, parsed), max_value)

# Shared SELECT list / sort map for table API + CSV/JSON exports
_VIDEOS_ROW_COLUMNS = (
    "filename, category, profile, el_type, container, source_vol, full_path, last_scanned, resolution, "
    "bitrate_mbps, scan_error, is_hybrid, is_source_hybrid, secondary_hdr, width, height, file_size, "
    "bl_compatibility_id, audio_codecs, audio_langs, audio_channels, subtitles, max_cll, max_fall, "
    "video_source, source_format, video_codec, is_3d, edition, year, media_type, show_title, season, "
    "episode, movie_title, episode_title, nfo_missing, missing, fps, aspect_ratio, imdb_id, tvdb_id, "
    "tmdb_id, rotten_id, metacritic_id, trakt_id, tvdb_series_id, tvdb_episode_id, imdb_series_id, "
    "imdb_episode_id, tmdb_series_id, tmdb_episode_id, trakt_series_id, trakt_episode_id, "
    "rotten_series_id, rotten_episode_id, metacritic_series_id, metacritic_episode_id, imdb_rating, "
    "tvdb_rating, tmdb_rating, rotten_rating, metacritic_rating, trakt_rating, dup_group_key, "
    "dup_exact_key, dup_count"
)
_VIDEOS_COLUMN_NAMES = [c.strip() for c in _VIDEOS_ROW_COLUMNS.split(',') if c.strip()]
_VIDEOS_SORT_MAP = {
    'file': 'filename', 'hybrid': 'is_hybrid', 'source_hybrid': 'is_source_hybrid', 'main': 'category',
    'prof': 'profile', 'el': 'el_type', 'sec': 'secondary_hdr', 'res': 'resolution',
    'bit': 'bitrate_mbps', 'vol': 'source_vol', 'cont': 'container', 'scan': 'last_scanned',
    'stat': 'scan_error', 'size': 'file_size', 'dup': 'dup_count', 'video_source': 'video_source',
    'source_format': 'source_format', 'video_codec': 'video_codec', 'is_3d': 'is_3d',
    'edition': 'edition', 'year': 'year', 'media_type': 'media_type', 'show_title': 'show_title',
    'season': 'season', 'episode': 'episode', 'movie_title': 'movie_title',
    'episode_title': 'episode_title', 'cll': 'max_cll', 'fall': 'max_fall',
}
_EXPORT_CHUNK_SIZE = 1000

def _export_query_parts(args: Dict[str, Any]) -> tuple[str, list[Any], str, str, str, list[Any]]:
    """Build WHERE/ORDER/LIMIT pieces for exports from request args."""
    where_clause, params = build_filter_query(args)
    db_sort = _VIDEOS_SORT_MAP.get(args.get('sort'), 'last_scanned')
    order = parse_sort_order(args.get('order'))
    page = args.get('page')
    per_page = args.get('per_page')
    limit_clause = ""
    limit_params: list[Any] = []
    try:
        if page is not None and per_page is not None:
            page_val = parse_positive_int(page, 1, 1000000)
            per_page_val = parse_positive_int(per_page, 50, 100000)
            offset = (page_val - 1) * per_page_val
            limit_clause = " LIMIT ? OFFSET ?"
            limit_params = [per_page_val, offset]
    except (ValueError, TypeError):
        limit_clause = ""
        limit_params = []
    return where_clause, params, db_sort, order, limit_clause, limit_params

def _row_to_export_dict(row: Any) -> dict[str, Any]:
    values = list(row)
    # Pad/truncate defensively if schema drift
    if len(values) < len(_VIDEOS_COLUMN_NAMES):
        values.extend([None] * (len(_VIDEOS_COLUMN_NAMES) - len(values)))
    return {name: values[i] for i, name in enumerate(_VIDEOS_COLUMN_NAMES)}

@bp.route('/download_csv')
def download_csv() -> Response:
    """
    Download filtered video data as CSV (same columns as /api/videos table rows).
    Streams in chunks to limit peak memory on large libraries.
    """
    where_clause, params, db_sort, order, limit_clause, limit_params = _export_query_parts(request.args)
    # When exporting "current page", limit_clause is set; otherwise stream full filtered set.
    def generate():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(_VIDEOS_COLUMN_NAMES)
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)
        with get_db_readonly() as conn:
            if limit_clause:
                rows = conn.execute(
                    f"SELECT {_VIDEOS_ROW_COLUMNS} FROM videos WHERE {where_clause} "
                    f"ORDER BY {db_sort} {order}{limit_clause}",
                    params + limit_params,
                ).fetchall()
                for row in rows:
                    writer.writerow([row[i] if i < len(row) else None for i in range(len(_VIDEOS_COLUMN_NAMES))])
                yield buf.getvalue()
                return
            offset = 0
            while True:
                chunk = conn.execute(
                    f"SELECT {_VIDEOS_ROW_COLUMNS} FROM videos WHERE {where_clause} "
                    f"ORDER BY {db_sort} {order} LIMIT ? OFFSET ?",
                    params + [_EXPORT_CHUNK_SIZE, offset],
                ).fetchall()
                if not chunk:
                    break
                for row in chunk:
                    writer.writerow([row[i] if i < len(row) else None for i in range(len(_VIDEOS_COLUMN_NAMES))])
                yield buf.getvalue()
                buf.seek(0)
                buf.truncate(0)
                offset += _EXPORT_CHUNK_SIZE
                if len(chunk) < _EXPORT_CHUNK_SIZE:
                    break

    return Response(
        generate(),
        mimetype='text/csv; charset=utf-8',
        headers={"Content-Disposition": "attachment; filename=media_export.csv"},
    )

@bp.route('/download_json')
def download_json() -> Response:
    """
    Download filtered video data as JSON array of objects (same fields as /api/videos).
    Streams a JSON array in chunks to limit peak memory on large libraries.
    """
    where_clause, params, db_sort, order, limit_clause, limit_params = _export_query_parts(request.args)

    def generate():
        yield '['
        first = True
        with get_db_readonly() as conn:
            if limit_clause:
                rows = conn.execute(
                    f"SELECT {_VIDEOS_ROW_COLUMNS} FROM videos WHERE {where_clause} "
                    f"ORDER BY {db_sort} {order}{limit_clause}",
                    params + limit_params,
                ).fetchall()
                for row in rows:
                    if not first:
                        yield ','
                    first = False
                    yield json.dumps(_row_to_export_dict(row), ensure_ascii=False)
            else:
                offset = 0
                while True:
                    chunk = conn.execute(
                        f"SELECT {_VIDEOS_ROW_COLUMNS} FROM videos WHERE {where_clause} "
                        f"ORDER BY {db_sort} {order} LIMIT ? OFFSET ?",
                        params + [_EXPORT_CHUNK_SIZE, offset],
                    ).fetchall()
                    if not chunk:
                        break
                    for row in chunk:
                        if not first:
                            yield ','
                        first = False
                        yield json.dumps(_row_to_export_dict(row), ensure_ascii=False)
                    offset += _EXPORT_CHUNK_SIZE
                    if len(chunk) < _EXPORT_CHUNK_SIZE:
                        break
        yield ']'

    return Response(
        generate(),
        mimetype='application/json; charset=utf-8',
        headers={"Content-Disposition": "attachment; filename=media_export.json"},
    )

@bp.route('/api/backup', methods=['POST'])
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

_RESTORE_ALLOWED_BASENAMES = frozenset({'processed_videos.db', 'settings.json'})

def _zip_member_path_is_safe(member_name: str) -> bool:
    """Reject absolute paths, drive letters, and parent-directory traversal in ZIP names."""
    if member_name is None:
        return False
    name = str(member_name).replace('\\', '/')
    if not name or name in ('.', '..'):
        return False
    # Directory entries are fine to ignore later; still must not traverse
    parts = [p for p in name.split('/') if p not in ('', '.')]
    if any(p == '..' for p in parts):
        return False
    if name.startswith('/') or name.startswith('../'):
        return False
    # Windows drive / UNC style
    if len(name) >= 2 and name[1] == ':':
        return False
    if name.startswith('//'):
        return False
    return True

def _validate_restore_zip_members(zipf: zipfile.ZipFile) -> dict[str, str]:
    """
    Validate every ZIP member for path traversal and map allowed restore basenames
    to their archive member names. Raises ValueError on unsafe members.
    """
    found: dict[str, str] = {}
    for info in zipf.infolist():
        raw = info.filename
        if not _zip_member_path_is_safe(raw):
            raise ValueError(f"Unsafe ZIP member path rejected: {raw!r}")
        # Skip pure directory entries
        name = raw.replace('\\', '/')
        if name.endswith('/'):
            continue
        # Reject symlink/special entries when detectable (Unix external attrs)
        is_symlink = ((info.external_attr >> 16) & 0o170000) == 0o120000
        if is_symlink:
            raise ValueError(f"Symlink ZIP member rejected: {raw!r}")
        base = os.path.basename(name)
        if base in _RESTORE_ALLOWED_BASENAMES:
            if base in found:
                raise ValueError(f"Duplicate restore member for {base}")
            found[base] = raw
    return found

def _write_zip_member_to_path(zipf: zipfile.ZipFile, member_name: str, dest_path: str) -> None:
    """Copy a ZIP member to an absolute destination path (never uses extract path logic)."""
    dest_dir = os.path.dirname(os.path.abspath(dest_path))
    os.makedirs(dest_dir, exist_ok=True)
    # Ensure dest stays inside OUTPUT_DIR
    out_real = os.path.realpath(OUTPUT_DIR)
    dest_real_parent = os.path.realpath(dest_dir)
    if not is_path_within_root(dest_real_parent, out_real) and dest_real_parent != out_real:
        raise ValueError("Restore destination escapes output directory")
    with zipf.open(member_name, 'r') as src, open(dest_path, 'wb') as dst:
        shutil.copyfileobj(src, dst)

@bp.route('/api/restore', methods=['POST'])
def restore_database() -> Response:
    """
    Restore database and settings from a backup file.
    
    Expects a ZIP file upload containing:
    - processed_videos.db (database file)
    - settings.json (settings as JSON)
    
    Members are validated against ZIP-slip / symlink tricks; only known basenames
    are written into OUTPUT_DIR via explicit copy (not zipfile.extract path joins).
    
    Returns:
        JSON response with status and message
    """
    try:
        busy = reject_if_busy()
        if busy:
            return busy
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
            db_restored = False
            settings_restored = False
            db_basename = os.path.basename(DB_PATH)
            
            with zipfile.ZipFile(temp_zip, 'r') as zipf:
                members = _validate_restore_zip_members(zipf)
                
                # Restore database by streaming member bytes to a temp file under OUTPUT_DIR
                if db_basename in members:
                    if os.path.exists(DB_PATH):
                        backup_old = DB_PATH + f".pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        shutil.copy2(DB_PATH, backup_old)
                    dest_tmp = os.path.join(OUTPUT_DIR, f"{db_basename}.restore_{uuid.uuid4().hex[:8]}")
                    try:
                        _write_zip_member_to_path(zipf, members[db_basename], dest_tmp)
                        os.replace(dest_tmp, DB_PATH)
                    finally:
                        if os.path.exists(dest_tmp):
                            try:
                                os.remove(dest_tmp)
                            except OSError:
                                pass
                    db_restored = True
                    invalidate_library_stats_cache()
                
                # Restore settings (read JSON bytes; never extract to arbitrary paths)
                if 'settings.json' in members:
                    settings_data = zipf.read(members['settings.json']).decode('utf-8')
                    settings_dict = json.loads(settings_data)
                    if not isinstance(settings_dict, dict):
                        raise ValueError("settings.json must be a JSON object")
                    with get_db() as conn:
                        for key, value in settings_dict.items():
                            conn.execute(
                                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                                (str(key), str(value)),
                            )
                        conn.commit()
                    settings_restored = True
            
            if db_restored and settings_restored:
                return jsonify({"status": "success", "message": "Database and settings restored successfully"}), 200
            elif db_restored:
                return jsonify({"status": "success", "message": "Database restored successfully (settings not found in backup)"}), 200
            elif settings_restored:
                return jsonify({"status": "success", "message": "Settings restored successfully (database not found in backup)"}), 200
            else:
                return jsonify({"status": "error", "message": "Backup file does not contain database or settings"}), 400
                
        except zipfile.BadZipFile:
            return jsonify({"status": "error", "message": "Invalid ZIP file format"}), 400
        except json.JSONDecodeError:
            return jsonify({"status": "error", "message": "Invalid settings.json format"}), 400
        except ValueError as ve:
            return jsonify({"status": "error", "message": str(ve)}), 400
        except Exception as e:
            log_debug(f"Restore failed: {e}", "ERROR")
            return jsonify({"status": "error", "message": f"Restore failed: {str(e)}"}), 500
        finally:
            if os.path.exists(temp_zip):
                try:
                    os.remove(temp_zip)
                except OSError:
                    pass
            
    except Exception as e:
        log_debug(f"Restore failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/filter_presets', methods=['GET'])
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

@bp.route('/api/filter_presets', methods=['POST'])
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

@bp.route('/api/filter_presets/<preset_name>', methods=['DELETE'])
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

def _build_videos_meta_payload(args: Dict[str, Any], include_filter_options: bool = True) -> Dict[str, Any]:
    """Build heavy stats (+ optional filter_options) payload for the current filter args."""
    main_where, main_params = build_filter_query(args)
    media_type_arg = (args.get('media_type') or '').strip().lower()
    if media_type_arg == 'movie':
        media_scope_key = "stats_movie"
    elif media_type_arg == 'tv':
        media_scope_key = "stats_tv"
    else:
        media_scope_key = "stats"

    with get_db_readonly() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {main_where}", main_params).fetchone()[0]

        # Unfiltered totals come from cache (rebuilt only after DB writes).
        lib_bundle = get_or_build_library_stats_bundle(conn)
        stats = lib_bundle["stats"]
        stats["last_scan_time"] = PROGRESS["last_duration"]
        stats["last_full_scan"] = PROGRESS.get("last_full_scan") or "Never"
        stats_media_scoped = lib_bundle[media_scope_key]
        stats_media_scoped["last_scan_time"] = PROGRESS["last_duration"]
        stats_media_scoped["last_full_scan"] = PROGRESS.get("last_full_scan") or "Never"

        stats_filtered = _compute_enriched_stats(conn, main_where, main_params, include_sizes=False)

        payload = {
            "stats": stats,
            "stats_filtered": stats_filtered,
            "stats_media_scoped": stats_media_scoped,
            "total_items": total,
        }
        if not include_filter_options:
            return payload

        where_cache: dict[str | None, tuple[str, list[Any]]] = {}

        def get_where(exclude_key: str | None):
            if exclude_key in where_cache:
                return where_cache[exclude_key]
            w, p = build_filter_query(args, exclude_key=exclude_key)
            where_cache[exclude_key] = (w, p)
            return w, p

        def get_cnt(col, key):
            w, p = get_where(key)
            return {
                r[0]: r[1]
                for r in conn.execute(
                    f"SELECT {col}, COUNT(*) FROM videos WHERE {col} != '' AND {col} IS NOT NULL AND {w} GROUP BY {col}",
                    p,
                ).fetchall()
            }

        def get_blank_cnt(col, key):
            w, p = get_where(key)
            return conn.execute(
                f"SELECT COUNT(*) FROM videos WHERE ({col} IS NULL OR {col} = '') AND {w}",
                p,
            ).fetchone()[0]

        def get_audio_codecs(key):
            """Count codecs from comma-separated audio_codecs via SQL (no Python row scan)."""
            w, p = get_where(key)
            return _audio_codec_counts_sql(conn, w, p)

        cnt_vol = get_cnt('source_vol', 'volume')
        cnt_res = get_cnt('resolution', 'resolution')

        w_status, p_status = build_filter_query(args, exclude_key='status')
        failed_cnt = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_status} AND scan_error IS NOT NULL AND scan_error != ''",
            p_status,
        ).fetchone()[0]
        ok_cnt = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_status} AND (scan_error IS NULL OR scan_error = '')",
            p_status,
        ).fetchone()[0]
        w_hyb, p_hyb = build_filter_query(args, exclude_key='is_hybrid')
        hyb_yes = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_hyb} AND is_hybrid = 1", p_hyb
        ).fetchone()[0]
        hyb_no = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_hyb} AND is_hybrid = 0 AND category != 'sdr_only'",
            p_hyb,
        ).fetchone()[0]
        w_src_hyb, p_src_hyb = build_filter_query(args, exclude_key='source_hybrid')
        src_hyb_yes = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_src_hyb} AND is_source_hybrid = 1", p_src_hyb
        ).fetchone()[0]
        src_hyb_no = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_src_hyb} AND is_source_hybrid = 0", p_src_hyb
        ).fetchone()[0]
        w_3d, p_3d = build_filter_query(args, exclude_key='is_3d')
        d3d_yes = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_3d} AND is_3d = 1", p_3d
        ).fetchone()[0]
        d3d_no = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_3d} AND is_3d = 0", p_3d
        ).fetchone()[0]
        w_missing, p_missing = build_filter_query(args, exclude_key='missing')
        missing_yes = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_missing} AND missing = 1", p_missing
        ).fetchone()[0]
        missing_no = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_missing} AND (missing = 0 OR missing IS NULL)",
            p_missing,
        ).fetchone()[0]

        payload["filter_options"] = {
            'categories': get_cnt('category', 'category'),
            'profiles': get_cnt('profile', 'profile'),
            'el_types': get_cnt('el_type', 'el'),
            'containers': get_cnt('container', 'container'),
            'volumes': cnt_vol,
            'resolutions': cnt_res,
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
                'media_type': get_blank_cnt('media_type', 'media_type'),
            },
            'special_hybrid': {'1': hyb_yes, '0': hyb_no},
            'special_source_hybrid': {'1': src_hyb_yes, '0': src_hyb_no},
            'special_status': {'ok': ok_cnt, 'failed': failed_cnt},
            'special_is_3d': {'1': d3d_yes, '0': d3d_no},
            'special_missing': {'1': missing_yes, '0': missing_no},
        }
        return payload


@bp.route('/api/videos')
def get_videos() -> Response:
    """
    Fast paginated video rows for the table.

    Returns rows + pagination only. Ribbons/charts/filter facet counts come from
    GET /api/videos/meta so the table can render without waiting on aggregations.
    """
    ensure_video_column('is_source_hybrid', 'INTEGER DEFAULT 0')
    main_where, main_params = build_filter_query(request.args)
    page = parse_positive_int(request.args.get('page'), 1, 1000000)
    per_page = parse_positive_int(request.args.get('per_page'), 50, 500)
    db_sort = _VIDEOS_SORT_MAP.get(request.args.get('sort'), 'last_scanned')
    order = parse_sort_order(request.args.get('order'))

    with get_db_readonly() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM videos WHERE {main_where}", main_params).fetchone()[0]
        library_total = None
        with library_stats_cache_lock:
            cached_bundle = LIBRARY_STATS_CACHE.get("bundle")
            if cached_bundle is not None:
                library_total = cached_bundle.get("library_total")
        if library_total is None:
            library_total = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
        rows = conn.execute(
            f"SELECT {_VIDEOS_ROW_COLUMNS} FROM videos WHERE {main_where} ORDER BY {db_sort} {order} LIMIT ? OFFSET ?",
            main_params + [per_page, (page - 1) * per_page],
        ).fetchall()
        global API_LOG_TS
        now = time.time()
        if PROGRESS.get("status") == "scanning" and now - API_LOG_TS >= 5:
            log_debug(
                f"[API_VIDEOS] total={total} rows={len(rows)} page={page} per_page={per_page}",
                "INFO",
            )
            API_LOG_TS = now
        return jsonify({
            "rows": [list(r) for r in rows],
            "page": page,
            "total_items": total,
            "total_pages": (total + per_page - 1) // per_page,
            "library_total": library_total,
        })


@bp.route('/api/videos/meta')
def get_videos_meta() -> Response:
    """
    Heavy dashboard metadata for current filters: stats, charts data, optional filter options.

    Query:
      include_options=0|1 (default 1). Set 0 to skip expensive facet/dropdown count queries.
    """
    ensure_video_column('is_source_hybrid', 'INTEGER DEFAULT 0')
    include_raw = str(request.args.get('include_options', '1')).strip().lower()
    include_filter_options = include_raw not in ('0', 'false', 'no', 'off')
    return jsonify(_build_videos_meta_payload(request.args, include_filter_options=include_filter_options))


@bp.route('/api/filter_paths', methods=['POST'])
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


@bp.route('/api/arr_search_replace', methods=['POST'])
def arr_search_replace() -> Response:
    """
    Queue Sonarr/Radarr searches for one or more selected files.
    """
    payload = request.get_json(silent=True) or {}
    raw_paths = payload.get("paths")
    single_path = payload.get("full_path")
    if isinstance(raw_paths, list):
        paths = [str(p).strip() for p in raw_paths if str(p).strip()]
    elif single_path:
        paths = [str(single_path).strip()]
    else:
        paths = []

    if not paths:
        return jsonify({"status": "error", "message": "Missing paths"}), 400
    if len(paths) > 500:
        return jsonify({"status": "error", "message": "Too many paths in one request (max 500)"}), 400

    placeholders = ",".join(["?"] * len(paths))
    sql = (
        "SELECT full_path, filename, media_type, season, episode, tmdb_id, tvdb_id, tvdb_series_id, imdb_id, show_title "
        f"FROM videos WHERE full_path IN ({placeholders})"
    )
    with get_db_readonly() as conn:
        rows = conn.execute(sql, paths).fetchall()

    by_path: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        by_path[str(row[0])] = {
            "full_path": row[0],
            "filename": row[1],
            "media_type": row[2],
            "season": row[3],
            "episode": row[4],
            "tmdb_id": row[5],
            "tvdb_id": row[6],
            "tvdb_series_id": row[7] if len(row) > 7 else None,
            "imdb_id": row[8] if len(row) > 8 else None,
            "show_title": row[9] if len(row) > 9 else None,
        }

    results = []
    success_count = 0
    for path in paths:
        item = by_path.get(path)
        if not item:
            log_debug(f"[ARR] Path not in DB: {path[:80]}...", "WARNING")
            results.append({"full_path": path, "status": "error", "message": "Path not found in database"})
            continue

        media_type = str(item.get("media_type") or "").strip().lower()
        fn = item.get("filename") or "(unknown)"
        log_debug(f"[ARR] Processing: {fn} media_type={media_type or '(blank)'} tvdb={item.get('tvdb_id')} tmdb={item.get('tmdb_id')}", "INFO")
        try:
            if media_type == "movie":
                ok, message = _queue_radarr_search(item)
            elif media_type == "tv":
                ok, message = _queue_sonarr_search(item)
            else:
                # Fallback inference when media_type is blank/missing
                if _as_int(item.get("tmdb_id")) is not None:
                    ok, message = _queue_radarr_search(item)
                elif _as_int(item.get("tvdb_series_id")) is not None:
                    ok, message = _queue_sonarr_search(item)
                elif _as_int(item.get("season")) is not None and _as_int(item.get("episode")) is not None:
                    ok, message = _queue_sonarr_search(item)
                else:
                    ok, message = False, "Unknown media_type and no tmdb_id/tvdb_series_id/season+episode for ARR lookup"
        except Exception as e:
            log_debug(f"[ARR] Exception for {fn}: {e}", "ERROR")
            ok, message = False, str(e)

        if ok:
            success_count += 1
            results.append({"full_path": path, "status": "ok", "message": message})
        else:
            results.append({"full_path": path, "status": "error", "message": message})

    failed_count = len(paths) - success_count
    return jsonify({
        "status": "ok",
        "processed": len(paths),
        "success": success_count,
        "failed": failed_count,
        "results": results
    })


@bp.route('/api/arr_status', methods=['GET'])
def arr_status() -> Response:
    """
    Get Sonarr/Radarr connectivity status for menu indicator.
    """
    now = time.time()
    cached = ARR_STATUS_CACHE.get("payload")
    cached_ts = float(ARR_STATUS_CACHE.get("ts") or 0.0)
    # Short cache keeps context menu snappy while avoiding constant network calls.
    if cached and (now - cached_ts) < 15:
        return jsonify(cached)

    sonarr = _arr_service_status("sonarr", SONARR_URL, SONARR_API_KEY)
    radarr = _arr_service_status("radarr", RADARR_URL, RADARR_API_KEY)
    overall_ok = bool(sonarr.get("ok")) and bool(radarr.get("ok"))
    payload = {
        "status": "ok",
        "overall_ok": overall_ok,
        "sonarr": sonarr,
        "radarr": radarr
    }
    ARR_STATUS_CACHE["ts"] = now
    ARR_STATUS_CACHE["payload"] = payload
    return jsonify(payload)


@bp.route('/api/rescan_file', methods=['POST'])
def rescan_file() -> Response:
    """
    Rescan a single file and update its database entry.
    Path must resolve under a configured/discovered media mount.
    """
    busy = reject_if_busy()
    if busy:
        return busy
    try:
        payload = request.get_json(silent=True) or {}
        full_path = payload.get('full_path')
        allowed, err = resolve_allowed_media_path(full_path)
        if err:
            return jsonify({"status": "error", "message": err}), 400

        res = scan_file_worker(pathlib.Path(allowed))
        save_batch_to_db([res])
        return jsonify({"status": "ok"})
    except Exception as e:
        log_debug(f"Rescan failed for {(request.get_json(silent=True) or {}).get('full_path')}: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

_RESCAN_FILES_MAX_BATCH = 50

@bp.route('/api/rescan_files', methods=['POST'])
def rescan_files() -> Response:
    """
    Rescan multiple files in one request (threaded analysis, batched DB writes).

    Body: { "paths": ["..."], "threads": 2 }
    Max 50 paths per call — clients should chunk larger selections.
    """
    busy = reject_if_busy()
    if busy:
        return busy
    try:
        payload = request.get_json(silent=True) or {}
        paths = payload.get('paths')
        if not isinstance(paths, list) or not paths:
            return jsonify({"status": "error", "message": "Missing paths"}), 400
        if len(paths) > _RESCAN_FILES_MAX_BATCH:
            return jsonify({
                "status": "error",
                "message": f"Max {_RESCAN_FILES_MAX_BATCH} paths per request; chunk larger selections",
            }), 400

        threads = parse_positive_int(payload.get('threads'), 2, 8)
        allowed_items: list[tuple[str, str]] = []
        errors: list[dict[str, str]] = []
        for raw in paths:
            allowed, err = resolve_allowed_media_path(raw)
            if err:
                errors.append({"path": str(raw), "message": err})
            else:
                allowed_items.append((str(raw), allowed))

        ok_paths: list[str] = []
        batch_buffer: list[dict] = []

        def _work(item: tuple[str, str]) -> tuple[str, str, Optional[dict], Optional[str]]:
            orig, allowed = item
            try:
                result = scan_file_worker(pathlib.Path(allowed))
                return ("ok", orig, result, None)
            except Exception as exc:
                return ("err", orig, None, str(exc))

        if allowed_items:
            with ThreadPoolExecutor(max_workers=min(threads, len(allowed_items))) as executor:
                futures = [executor.submit(_work, item) for item in allowed_items]
                for fut in as_completed(futures):
                    status, orig, result, err_msg = fut.result()
                    if status == "ok" and result is not None:
                        ok_paths.append(orig)
                        batch_buffer.append(result)
                        if len(batch_buffer) >= 10:
                            save_batch_to_db(batch_buffer)
                            batch_buffer = []
                    else:
                        errors.append({"path": orig, "message": err_msg or "Rescan failed"})
            if batch_buffer:
                save_batch_to_db(batch_buffer)

        failed = len(errors)
        ok_count = len(ok_paths)
        if failed == 0:
            status = "ok"
        elif ok_count == 0:
            status = "error"
        else:
            status = "partial"
        return jsonify({
            "status": status,
            "ok": ok_count,
            "failed": failed,
            "total": len(paths),
            "errors": errors[:50],
        })
    except Exception as e:
        log_debug(f"Batch rescan failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/debug_deep', methods=['POST'])
def debug_deep_file() -> Response:
    """
    Run debug_deep.py for a single file and return raw output.
    Path must resolve under a configured/discovered media mount.
    """
    busy = reject_if_busy()
    if busy:
        return busy
    payload = request.get_json(silent=True) or {}
    full_path = payload.get('full_path')
    allowed, err = resolve_allowed_media_path(full_path)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    if not os.path.exists(allowed):
        return jsonify({"status": "error", "message": "File not found"}), 404

    script_path = os.path.join(BASE_DIR, 'debug_deep.py')
    if not os.path.exists(script_path):
        return jsonify({"status": "error", "message": "debug_deep.py not found"}), 500

    try:
        rc, out, err_out = run_command([sys.executable, script_path, allowed], capture=True, capture_stderr=True, timeout_seconds=180)
        output = (out or "")
        if err_out:
            output += f"\n\n--- STDERR ---\n{err_out}"
        # Keep payload bounded to avoid huge API responses.
        if len(output) > 200000:
            output = output[-200000:]
        return jsonify({"status": "ok", "return_code": rc, "output": output})
    except Exception as e:
        log_debug(f"debug_deep failed for {allowed}: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/update_media_type', methods=['POST'])
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
            recompute_duplicate_group_keys_for_paths(conn, [full_path])
            recompute_duplicate_counts(conn)
        return jsonify({"status": "ok"})
    except Exception as e:
        log_debug(f"Update media_type failed for {payload.get('full_path')}: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/update_metadata', methods=['POST'])
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
            recompute_duplicate_group_keys_for_paths(conn, [full_path])
            recompute_duplicate_counts(conn)
        return jsonify({"status": "ok"})
    except Exception as e:
        log_debug(f"Update metadata failed for {payload.get('full_path')}: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/bulk_update_metadata', methods=['POST'])
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
            recompute_duplicate_group_keys_for_paths(conn, paths)
            recompute_duplicate_counts(conn)
        return jsonify({"status": "ok", "updated": updated})
    except Exception as e:
        log_debug(f"Bulk update metadata failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/backfill_metadata', methods=['POST'])
def backfill_metadata() -> Response:
    """
    Backfill missing metadata fields using .nfo and filename heuristics.
    """
    try:
        busy = reject_if_busy()
        if busy:
            return busy
        payload = request.get_json(silent=True) or {}
        fill_blanks_only = payload.get('fill_blanks_only', True)
        updated = 0
        with get_db() as conn:
            rows = conn.execute(
                """SELECT full_path, filename, media_type, show_title, episode_title, season, episode, movie_title, year,
                          imdb_id, tvdb_id, tmdb_id, rotten_id, metacritic_id, trakt_id,
                          tvdb_series_id, tvdb_episode_id, imdb_series_id, imdb_episode_id, tmdb_series_id, tmdb_episode_id,
                          trakt_series_id, trakt_episode_id, rotten_series_id, rotten_episode_id, metacritic_series_id, metacritic_episode_id,
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
                    'tvdb_series_id': row['tvdb_series_id'] if 'tvdb_series_id' in row.keys() else None,
                    'tvdb_episode_id': row['tvdb_episode_id'] if 'tvdb_episode_id' in row.keys() else None,
                    'imdb_series_id': row['imdb_series_id'] if 'imdb_series_id' in row.keys() else None,
                    'imdb_episode_id': row['imdb_episode_id'] if 'imdb_episode_id' in row.keys() else None,
                    'tmdb_series_id': row['tmdb_series_id'] if 'tmdb_series_id' in row.keys() else None,
                    'tmdb_episode_id': row['tmdb_episode_id'] if 'tmdb_episode_id' in row.keys() else None,
                    'trakt_series_id': row['trakt_series_id'] if 'trakt_series_id' in row.keys() else None,
                    'trakt_episode_id': row['trakt_episode_id'] if 'trakt_episode_id' in row.keys() else None,
                    'rotten_series_id': row['rotten_series_id'] if 'rotten_series_id' in row.keys() else None,
                    'rotten_episode_id': row['rotten_episode_id'] if 'rotten_episode_id' in row.keys() else None,
                    'metacritic_series_id': row['metacritic_series_id'] if 'metacritic_series_id' in row.keys() else None,
                    'metacritic_episode_id': row['metacritic_episode_id'] if 'metacritic_episode_id' in row.keys() else None,
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
                new_tvdb_series_id = updates.get('tvdb_series_id', current.get('tvdb_series_id'))
                new_tvdb_episode_id = updates.get('tvdb_episode_id', current.get('tvdb_episode_id'))
                new_imdb_series_id = updates.get('imdb_series_id', current.get('imdb_series_id'))
                new_imdb_episode_id = updates.get('imdb_episode_id', current.get('imdb_episode_id'))
                new_tmdb_series_id = updates.get('tmdb_series_id', current.get('tmdb_series_id'))
                new_tmdb_episode_id = updates.get('tmdb_episode_id', current.get('tmdb_episode_id'))
                new_trakt_series_id = updates.get('trakt_series_id', current.get('trakt_series_id'))
                new_trakt_episode_id = updates.get('trakt_episode_id', current.get('trakt_episode_id'))
                new_rotten_series_id = updates.get('rotten_series_id', current.get('rotten_series_id'))
                new_rotten_episode_id = updates.get('rotten_episode_id', current.get('rotten_episode_id'))
                new_metacritic_series_id = updates.get('metacritic_series_id', current.get('metacritic_series_id'))
                new_metacritic_episode_id = updates.get('metacritic_episode_id', current.get('metacritic_episode_id'))
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
                    "UPDATE videos SET media_type=?, show_title=?, episode_title=?, season=?, episode=?, movie_title=?, year=?, imdb_id=?, tvdb_id=?, tmdb_id=?, rotten_id=?, metacritic_id=?, trakt_id=?, tvdb_series_id=?, tvdb_episode_id=?, imdb_series_id=?, imdb_episode_id=?, tmdb_series_id=?, tmdb_episode_id=?, trakt_series_id=?, trakt_episode_id=?, rotten_series_id=?, rotten_episode_id=?, metacritic_series_id=?, metacritic_episode_id=?, imdb_rating=?, tvdb_rating=?, tmdb_rating=?, rotten_rating=?, metacritic_rating=?, trakt_rating=?, validation_flag=? WHERE full_path=?",
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
                        new_tvdb_series_id,
                        new_tvdb_episode_id,
                        new_imdb_series_id,
                        new_imdb_episode_id,
                        new_tmdb_series_id,
                        new_tmdb_episode_id,
                        new_trakt_series_id,
                        new_trakt_episode_id,
                        new_rotten_series_id,
                        new_rotten_episode_id,
                        new_metacritic_series_id,
                        new_metacritic_episode_id,
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
            if updated > 0:
                recompute_duplicate_group_keys_for_paths(conn, [row['full_path'] for row in rows])
                recompute_duplicate_counts(conn)
            with progress_lock:
                PROGRESS.update({"status": "idle", "current": 0, "total": 0, "file": "Waiting...", "eta": ""})
        return jsonify({"status": "ok", "updated": updated})
    except Exception as e:
        log_debug(f"Backfill metadata failed: {e}", "ERROR")
        with progress_lock:
            PROGRESS.update({"status": "idle", "current": 0, "total": 0, "file": "Waiting...", "eta": ""})
        return jsonify({"status": "error", "message": str(e)}), 500

def parse_duplicate_group_info(group_key: str | None) -> dict:
    key = (group_key or '').strip()
    if not key:
        return {"basis": "unknown", "media_type": None}
    parts = key.split(':')
    if len(parts) >= 2:
        media_type = parts[0]
        basis = parts[1]
        return {"basis": basis, "media_type": media_type}
    return {"basis": "unknown", "media_type": None}

@bp.route('/api/duplicates/rebuild', methods=['POST'])
def rebuild_duplicates() -> Response:
    """
    Rebuild persistent duplicate keys/counters.
    Optionally includes exact fingerprint refresh.
    """
    busy = reject_if_busy()
    if busy:
        return busy
    try:
        payload = request.get_json(silent=True) or {}
        filters = payload.get('filters') or {}
        include_exact = bool(payload.get('include_exact', False))
        where, params = build_filter_query(filters)
        updated = 0
        with get_db() as conn:
            rows = conn.execute(
                f"""SELECT full_path, filename, media_type, movie_title, show_title, season, episode, year,
                            tmdb_id, imdb_id, tvdb_series_id, imdb_series_id, file_size, dup_exact_key
                     FROM videos WHERE {where}""",
                params
            ).fetchall()
            updates: list[tuple[str | None, str | None, str]] = []
            for row in rows:
                row_dict = dict(row)
                group_key = build_duplicate_group_key(row_dict)
                exact_key = row['dup_exact_key']
                if include_exact:
                    exact_key = build_duplicate_exact_key(row['full_path'], row['file_size'])
                updates.append((group_key, exact_key, row['full_path']))
            if updates:
                conn.executemany(
                    "UPDATE videos SET dup_group_key=?, dup_exact_key=? WHERE full_path=?",
                    updates
                )
                updated = len(updates)
            recompute_duplicate_counts(conn)
        return jsonify({"status": "ok", "updated": updated, "include_exact": include_exact})
    except Exception as e:
        log_debug(f"Rebuild duplicates failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/duplicates/groups', methods=['POST'])
def list_duplicate_groups() -> Response:
    """
    Return duplicate groups (logical + exact).

    When filters are present: include a group if ANY member matches the filter,
    but report the full group size (all members). This avoids hiding 3+ copy
    groups when a table filter only matches one of the copies.
    """
    try:
        payload = request.get_json(silent=True) or {}
        filters = payload.get('filters') or {}
        where, params = build_filter_query(filters)
        filters_active = where.replace(' ', '') != '1=1' or bool(params)
        groups: list[dict[str, Any]] = []
        with get_db() as conn:
            if filters_active:
                # Full group counts; restrict to keys touched by the filter.
                logical_sql = f"""
                    SELECT dup_group_key, COUNT(*) AS file_count, SUM(COALESCE(file_size,0)) AS total_size,
                           MAX(media_type) AS media_type, MAX(COALESCE(movie_title, show_title, filename)) AS title_sample,
                           MAX(year) AS year_sample, MAX(season) AS season_sample, MAX(episode) AS episode_sample
                    FROM videos
                    WHERE dup_group_key IS NOT NULL AND dup_group_key != ''
                      AND dup_group_key IN (
                          SELECT DISTINCT dup_group_key FROM videos
                          WHERE ({where}) AND dup_group_key IS NOT NULL AND dup_group_key != ''
                      )
                    GROUP BY dup_group_key
                    HAVING COUNT(*) > 1
                """
                exact_sql = f"""
                    SELECT dup_exact_key, COUNT(*) AS file_count, SUM(COALESCE(file_size,0)) AS total_size,
                           MAX(COALESCE(movie_title, show_title, filename)) AS title_sample,
                           MAX(media_type) AS media_type
                    FROM videos
                    WHERE dup_exact_key IS NOT NULL AND dup_exact_key != ''
                      AND dup_exact_key IN (
                          SELECT DISTINCT dup_exact_key FROM videos
                          WHERE ({where}) AND dup_exact_key IS NOT NULL AND dup_exact_key != ''
                      )
                    GROUP BY dup_exact_key
                    HAVING COUNT(*) > 1
                """
            else:
                logical_sql = f"""
                    SELECT dup_group_key, COUNT(*) AS file_count, SUM(COALESCE(file_size,0)) AS total_size,
                           MAX(media_type) AS media_type, MAX(COALESCE(movie_title, show_title, filename)) AS title_sample,
                           MAX(year) AS year_sample, MAX(season) AS season_sample, MAX(episode) AS episode_sample
                    FROM videos
                    WHERE {where} AND dup_group_key IS NOT NULL AND dup_group_key != ''
                    GROUP BY dup_group_key
                    HAVING COUNT(*) > 1
                """
                exact_sql = f"""
                    SELECT dup_exact_key, COUNT(*) AS file_count, SUM(COALESCE(file_size,0)) AS total_size,
                           MAX(COALESCE(movie_title, show_title, filename)) AS title_sample,
                           MAX(media_type) AS media_type
                    FROM videos
                    WHERE {where} AND dup_exact_key IS NOT NULL AND dup_exact_key != ''
                    GROUP BY dup_exact_key
                    HAVING COUNT(*) > 1
                """

            logical_rows = conn.execute(logical_sql, params).fetchall()
            for row in logical_rows:
                info = parse_duplicate_group_info(row['dup_group_key'])
                label = row['title_sample'] or 'Unknown title'
                if (row['media_type'] or info.get('media_type')) == 'tv' and row['season_sample'] is not None and row['episode_sample'] is not None:
                    label = f"{label} S{int(row['season_sample']):02}E{int(row['episode_sample']):02}"
                elif row['year_sample']:
                    label = f"{label} ({int(row['year_sample'])})"
                groups.append({
                    "group_id": f"logical|{row['dup_group_key']}",
                    "group_key": row['dup_group_key'],
                    "group_type": "logical",
                    "match_basis": info.get('basis') or 'logical',
                    "media_type": row['media_type'] or info.get('media_type'),
                    "title": label,
                    "file_count": int(row['file_count'] or 0),
                    "total_size": int(row['total_size'] or 0)
                })

            exact_rows = conn.execute(exact_sql, params).fetchall()
            for row in exact_rows:
                groups.append({
                    "group_id": f"exact|{row['dup_exact_key']}",
                    "group_key": row['dup_exact_key'],
                    "group_type": "exact",
                    "match_basis": "size+fingerprint",
                    "media_type": row['media_type'],
                    "title": row['title_sample'] or 'Exact duplicate set',
                    "file_count": int(row['file_count'] or 0),
                    "total_size": int(row['total_size'] or 0)
                })
        groups.sort(key=lambda g: (g.get('file_count', 0), g.get('total_size', 0)), reverse=True)
        return jsonify({
            "status": "ok",
            "groups": groups,
            "group_count": len(groups),
            "filters_applied": filters_active,
        })
    except Exception as e:
        log_debug(f"List duplicate groups failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/duplicates/members', methods=['POST'])
def list_duplicate_members() -> Response:
    """
    Return files belonging to a duplicate group.
    """
    try:
        payload = request.get_json(silent=True) or {}
        group_id = (payload.get('group_id') or '').strip()
        if not group_id or '|' not in group_id:
            return jsonify({"status": "error", "message": "Missing/invalid group_id"}), 400
        group_type, group_key = group_id.split('|', 1)
        if group_type not in ('logical', 'exact'):
            return jsonify({"status": "error", "message": "Invalid group type"}), 400
        key_col = 'dup_group_key' if group_type == 'logical' else 'dup_exact_key'
        with get_db() as conn:
            rows = conn.execute(
                f"""SELECT filename, full_path, source_vol, file_size, resolution, bitrate_mbps, video_codec, source_format,
                            category, secondary_hdr, audio_codecs, profile, el_type, media_type, movie_title, show_title,
                            season, episode, year, scan_error, last_scanned
                     FROM videos
                     WHERE {key_col}=?
                     ORDER BY
                       CASE LOWER(COALESCE(source_format,'')) WHEN 'remux' THEN 4 WHEN 'bluray' THEN 3 WHEN 'web-dl' THEN 2 WHEN 'webrip' THEN 1 ELSE 0 END DESC,
                       CASE LOWER(COALESCE(resolution,'')) WHEN '8k' THEN 5 WHEN '4k' THEN 4 WHEN '2160p' THEN 4 WHEN '1080p' THEN 3 WHEN '720p' THEN 2 ELSE 0 END DESC,
                       COALESCE(bitrate_mbps, 0) DESC,
                       COALESCE(file_size, 0) DESC,
                       COALESCE(last_scanned, '') DESC""",
                (group_key,)
            ).fetchall()
            members: list[dict[str, Any]] = []
            for idx, row in enumerate(rows):
                members.append({
                    "filename": row['filename'],
                    "full_path": row['full_path'],
                    "source_vol": row['source_vol'],
                    "file_size": int(row['file_size'] or 0),
                    "resolution": row['resolution'],
                    "bitrate_mbps": row['bitrate_mbps'],
                    "video_codec": row['video_codec'],
                    "source_format": row['source_format'],
                    "category": row['category'],
                    "secondary_hdr": row['secondary_hdr'],
                    "audio_codecs": row['audio_codecs'],
                    "profile": row['profile'],
                    "el_type": row['el_type'],
                    "media_type": row['media_type'],
                    "movie_title": row['movie_title'],
                    "show_title": row['show_title'],
                    "season": row['season'],
                    "episode": row['episode'],
                    "year": row['year'],
                    "scan_error": row['scan_error'],
                    "last_scanned": row['last_scanned'],
                    "keep_recommended": idx == 0
                })
        return jsonify({"status": "ok", "group_id": group_id, "members": members, "member_count": len(members)})
    except Exception as e:
        log_debug(f"List duplicate members failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/delete', methods=['POST'])
def delete_files() -> Response:
    """
    Delete video records from the database, optionally removing files/folders on disk.

    Body:
      paths: list of full paths (required unless delete_all_filter)
      delete_all_filter: delete all DB rows matching filters (DB-only; disk delete forbidden)
      filters: filter object when delete_all_filter is true
      delete_files_on_disk: bool — remove video files under allowed mounts
      delete_folders: bool — after file delete, remove parent folders when safe
    """
    busy = reject_if_busy()
    if busy:
        return busy
    data = request.json or {}
    paths = data.get('paths') or []
    if not isinstance(paths, list):
        paths = []
    paths = [str(p).strip() for p in paths if str(p).strip()]
    delete_all = bool(data.get('delete_all_filter', False))
    delete_files_on_disk = bool(data.get('delete_files_on_disk', False))
    delete_folders = bool(data.get('delete_folders', False))

    if delete_folders and not delete_files_on_disk:
        return jsonify({
            "status": "error",
            "message": "delete_folders requires delete_files_on_disk",
        }), 400
    if delete_all and (delete_files_on_disk or delete_folders):
        return jsonify({
            "status": "error",
            "message": "Disk/folder delete is not allowed with delete-all-filtered; select explicit paths",
        }), 400
    if not delete_all and not paths:
        return jsonify({"status": "error", "message": "No paths provided"}), 400

    files_deleted: list[str] = []
    folders_deleted: list[str] = []
    disk_errors: list[dict[str, str]] = []
    folders_skipped: list[dict[str, str]] = []

    # Resolve allowed realpaths for disk ops
    allowed_by_orig: dict[str, str] = {}
    if delete_files_on_disk:
        for p in paths:
            allowed, err = resolve_allowed_media_path(p)
            if err:
                disk_errors.append({"path": p, "message": err})
            else:
                allowed_by_orig[p] = allowed

        deleting_reals = set(allowed_by_orig.values())

        for orig, allowed in list(allowed_by_orig.items()):
            try:
                if os.path.isfile(allowed) or os.path.islink(allowed):
                    os.remove(allowed)
                    files_deleted.append(allowed)
                elif not os.path.exists(allowed):
                    # Already gone on disk — still remove DB row
                    pass
                else:
                    disk_errors.append({"path": orig, "message": "Not a regular file"})
            except OSError as e:
                disk_errors.append({"path": orig, "message": str(e)})

        if delete_folders:
            # Unique parent folders of successfully targeted files
            folder_candidates: dict[str, set[str]] = {}
            for allowed in allowed_by_orig.values():
                parent = os.path.dirname(allowed)
                if not parent:
                    continue
                folder_candidates.setdefault(parent, set()).add(allowed)

            mount_roots = {os.path.realpath(r) for r in get_allowed_media_roots()}
            for folder, members in folder_candidates.items():
                folder_allowed, folder_err = resolve_allowed_media_path(folder)
                if folder_err:
                    folders_skipped.append({"path": folder, "message": folder_err})
                    continue
                folder_real = folder_allowed
                if folder_real in mount_roots:
                    folders_skipped.append({
                        "path": folder_real,
                        "message": "Refusing to delete media mount root",
                    })
                    continue
                # Block if any other video remains in the folder (after this delete set)
                others = []
                try:
                    for name in os.listdir(folder_real):
                        fp = os.path.join(folder_real, name)
                        try:
                            if not os.path.isfile(fp) and not os.path.islink(fp):
                                continue
                        except OSError:
                            continue
                        if pathlib.Path(name).suffix.lower() not in VIDEO_EXTENSIONS:
                            continue
                        fp_real = os.path.realpath(fp)
                        if fp_real in deleting_reals:
                            continue  # targeted by this request (may already be removed)
                        if os.path.exists(fp_real):
                            others.append(fp_real)
                except OSError as e:
                    folders_skipped.append({"path": folder_real, "message": str(e)})
                    continue
                if others:
                    folders_skipped.append({
                        "path": folder_real,
                        "message": f"Folder still contains {len(others)} other video file(s)",
                    })
                    continue
                try:
                    shutil.rmtree(folder_real)
                    folders_deleted.append(folder_real)
                    log_debug(f"[DELETE] Removed folder {folder_real}", "WARNING")
                except OSError as e:
                    disk_errors.append({"path": folder_real, "message": str(e)})

    with get_db() as conn:
        count = 0
        if delete_all:
            w, p = build_filter_query(data.get('filters', {}))
            count = conn.execute(f"DELETE FROM videos WHERE {w}", p).rowcount
        else:
            db_paths = set(paths)
            for allowed in allowed_by_orig.values():
                db_paths.add(allowed)
            for p in db_paths:
                count += conn.execute("DELETE FROM videos WHERE full_path = ?", (p,)).rowcount
        recompute_duplicate_counts(conn)

    return jsonify({
        "status": "deleted",
        "count": count,
        "files_deleted": len(files_deleted),
        "folders_deleted": len(folders_deleted),
        "files_deleted_paths": files_deleted[:100],
        "folders_deleted_paths": folders_deleted[:100],
        "folders_skipped": folders_skipped[:50],
        "disk_errors": disk_errors[:50],
        "delete_files_on_disk": delete_files_on_disk,
        "delete_folders": delete_folders,
    })


@bp.route('/api/delete/preview', methods=['POST'])
def delete_preview() -> Response:
    """
    Preview disk/folder impact for an explicit path list (no mutations).
    """
    data = request.json or {}
    paths = data.get('paths') or []
    if not isinstance(paths, list):
        paths = []
    paths = [str(p).strip() for p in paths if str(p).strip()]
    if not paths:
        return jsonify({"status": "error", "message": "No paths provided"}), 400

    allowed_by_orig: dict[str, str] = {}
    path_errors: list[dict[str, str]] = []
    for p in paths:
        allowed, err = resolve_allowed_media_path(p)
        if err:
            path_errors.append({"path": p, "message": err})
        else:
            allowed_by_orig[p] = allowed

    deleting_reals = set(allowed_by_orig.values())
    mount_roots = {os.path.realpath(r) for r in get_allowed_media_roots()}
    folder_map: dict[str, dict[str, Any]] = {}
    for allowed in allowed_by_orig.values():
        parent = os.path.dirname(allowed)
        if not parent:
            continue
        if parent not in folder_map:
            folder_map[parent] = {"path": parent, "files": [], "ok": True, "reason": ""}
        folder_map[parent]["files"].append(allowed)

    folders = []
    for parent, info in folder_map.items():
        folder_allowed, folder_err = resolve_allowed_media_path(parent)
        if folder_err:
            info["ok"] = False
            info["reason"] = folder_err
            info["path"] = parent
            folders.append(info)
            continue
        info["path"] = folder_allowed
        if folder_allowed in mount_roots:
            info["ok"] = False
            info["reason"] = "Media mount root — will not be deleted"
            folders.append(info)
            continue
        others = []
        try:
            for name in os.listdir(folder_allowed):
                fp = os.path.join(folder_allowed, name)
                if not os.path.isfile(fp) and not os.path.islink(fp):
                    continue
                if pathlib.Path(name).suffix.lower() not in VIDEO_EXTENSIONS:
                    continue
                fp_real = os.path.realpath(fp)
                if fp_real not in deleting_reals:
                    others.append(fp_real)
        except OSError as e:
            info["ok"] = False
            info["reason"] = str(e)
            folders.append(info)
            continue
        if others:
            info["ok"] = False
            info["reason"] = f"Contains {len(others)} other video file(s)"
            info["other_videos"] = others[:20]
        folders.append(info)

    return jsonify({
        "status": "ok",
        "file_count": len(allowed_by_orig),
        "files": list(allowed_by_orig.values()),
        "folders": folders,
        "path_errors": path_errors,
        "deletable_folder_count": sum(1 for f in folders if f.get("ok")),
    })


@bp.route('/api/settings', methods=['GET', 'POST'])
def handle_settings() -> Response:
    """
    Get or update application settings.
    
    GET: Returns all current settings as JSON
    POST: Updates specified settings and optionally configures scheduled scans
    
    Request Body (POST):
        mode: Scan schedule mode ('manual', 'daily', 'interval', 'weekly', or 'monthly')
        value: Schedule value (HH:MM for daily; hours for interval;
               dow or dow|HH:MM for weekly; day or day|HH:MM for monthly)
        threads: Number of worker threads
        skip_words: Comma-separated IGNORE tokens (optional / folder-only or % file+folder prefixes; * globs ok)
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
                    apply_scan_schedule(d.get('mode', 'manual'), d.get('value', ''))
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
                if 'remove_missing_from_db' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('remove_missing_from_db', ?)", (str(d['remove_missing_from_db']).lower(),))
                if 'duplicate_check_on_scan' in d: conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('duplicate_check_on_scan', ?)", (str(d['duplicate_check_on_scan']).lower(),))
            return jsonify({"status": "success"})
        except Exception as e:
            import traceback
            log_debug(f"Settings save failed: {e}", "ERROR")
            log_debug(traceback.format_exc(), "ERROR")
            return jsonify({"status": "error", "message": str(e)}), 500
    else:
        with get_db() as conn: res = dict(conn.execute("SELECT key, value FROM settings").fetchall())
        return jsonify(res)


@bp.route('/api/nfo_content', methods=['GET'])
def get_nfo_content() -> Response:
    """
    Return the raw NFO file content for a video path.
    Looks up NFO candidates (same-stem, tvshow.nfo for TV) and returns the first found.
    Video path and resolved NFO must lie under an allowed media mount.
    """
    path_arg = request.args.get('path', '').strip()
    allowed, err = resolve_allowed_media_path(path_arg)
    if err:
        return jsonify({"status": "error", "message": err}), 400
    full_path = allowed
    with get_db() as conn:
        row = conn.execute(
            "SELECT media_type FROM videos WHERE full_path = ?", (full_path,)
        ).fetchone()
        if row is None:
            # Also try original path form in case DB stored a non-realpath variant
            row = conn.execute(
                "SELECT media_type FROM videos WHERE full_path = ?", (path_arg,)
            ).fetchone()
    media_type = row[0] if row else None
    candidates = find_kodi_nfo_candidates(full_path, media_type)
    if not candidates:
        return jsonify({"status": "error", "message": "No NFO found for this file"}), 404
    nfo_path = candidates[0]
    nfo_allowed, nfo_err = resolve_allowed_media_path(nfo_path)
    if nfo_err:
        return jsonify({"status": "error", "message": "NFO path is outside allowed media mounts"}), 400
    try:
        with open(nfo_allowed, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
    except OSError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify({"status": "ok", "content": content, "nfo_path": nfo_allowed})


@bp.route('/api/browse', methods=['GET'])
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
    if not is_path_within_root(target_real, base_real):
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


@bp.route('/api/cleanup_db', methods=['POST'])
def cleanup_db() -> Response:
    """
    Remove DB entries for offline volumes or paths outside selected scan folders.
    """
    busy = reject_if_busy()
    if busy:
        return busy
    try:
        deleted = perform_cleanup_db(delete=True)
        return jsonify({"status": "ok", "deleted": deleted})
    except Exception as e:
        log_debug(f"Cleanup DB failed: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500


@bp.route('/api/cleanup_db_preview', methods=['GET'])
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
                if is_path_within_root(target_real, base_real) and os.path.isdir(target_real):
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
                if not any(is_path_within_root(real_path, base) for base in allowed_bases):
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

@bp.route('/api/db/maintenance', methods=['POST'])
def db_maintenance() -> Response:
    """
    Run database maintenance operations (VACUUM and ANALYZE).
    Optimizes the database by reclaiming space and updating query statistics.
    
    Returns:
        JSON response with status and message
    """
    busy = reject_if_busy()
    if busy:
        return busy
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

@bp.route('/start', methods=['POST'])
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
    busy = reject_if_busy(status_code=400)
    if busy:
        return busy
    targets = request.json.get('targets', [])
    threads = int(request.json.get('threads', 4))
    force = request.json.get('force_rescan', False)
    debug = request.json.get('debug_mode', False)
    scan_mode = (request.json.get('scan_mode') or 'all').lower()
    if scan_mode not in ('all', 'tv', 'movie'):
        scan_mode = 'all'
    scan_folder = request.json.get('scan_folder')
    threading.Thread(target=run_scan, args=(threads, targets, force, debug, scan_mode, scan_folder), daemon=True).start()
    return jsonify({"status": "started"})

@bp.route('/abort', methods=['POST'])
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

@bp.route('/pause', methods=['POST'])
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

@bp.route('/progress')
def get_progress():
    """Get current scan progress information."""
    with progress_lock: 
        d = PROGRESS.copy()
    return jsonify(d)

@bp.route('/clear_completed', methods=['POST'])
def clear_completed():
    """Clear the scan completion flag after user acknowledges the result."""
    with progress_lock: 
        PROGRESS["scan_completed"] = False
    return jsonify({"status": "cleared"})
