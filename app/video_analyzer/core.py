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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, TimeoutError, wait
from contextlib import contextmanager
from collections import OrderedDict

from flask import (
    Flask, render_template, jsonify, make_response, request, send_file, Response, Blueprint, session,
)

try:
    from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
    HAS_SCHEDULER = True
except ImportError:
    BackgroundScheduler = None  # type: ignore
    HAS_SCHEDULER = False

from video_analyzer.blueprint import bp
from video_analyzer.config import (
    APP_VERSION_FALLBACK, BASE_DIR, CHANGELOG_PATH, DB_PATH, DB_TIMEOUT,
    LOCAL_OUTPUT_FALLBACK, LOG_CLEANUP_LIMIT, MAX_RETRIES, MAX_SCAN_ATTEMPTS,
    MEDIAINFO_TIMEOUT, OUTPUT_DIR, PROCESSED_MAP_CHUNK_SIZE, PROGRESS_UPDATE_INTERVAL,
    RADARR_API_KEY, RADARR_URL, RETRY_DELAY_INITIAL, RPU_CACHE_MAX_SIZE,
    SONARR_API_KEY, SONARR_URL, SUBPROCESS_TIMEOUT, SYSTEM_DIRS, VIDEO_EXTENSIONS,
    app_version, app_version_label,
)
from video_analyzer.db.connection import get_db, get_db_readonly, invalidate_library_stats_cache
from video_analyzer.db.schema import ensure_video_column, init_db
from video_analyzer.state import (
    ACTIVE_PROCS, ACTIVE_SCAN_FILES, ARR_STATUS_CACHE, LIBRARY_STATS_CACHE,
    LOG_CACHE, PAUSE_EVENT, PROGRESS, RPU_CACHE, TOOL_VERSION_CACHE,
    db_access_lock, library_stats_cache_lock, proc_lock, progress_lock, rpu_cache_lock,
    APP_START_TIME,
)
from video_analyzer import state as va_state

# Compatibility aliases for tests and remaining core functions.
ABORT_SCAN = va_state.ABORT_SCAN
DEBUG_MODE = va_state.DEBUG_MODE
ACTIVE_SCAN_JOB_ID = va_state.ACTIVE_SCAN_JOB_ID
LOG_FILE = va_state.LOG_FILE
FAIL_FILE = va_state.FAIL_FILE
DIAG_LOG_TS = va_state.DIAG_LOG_TS
API_LOG_TS = va_state.API_LOG_TS
scheduler = va_state.scheduler

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
def clear_rpu_cache() -> None:
    """
    Clear the RPU cache. Useful for force rescans or when cache becomes stale.
    """
    global RPU_CACHE
    with rpu_cache_lock:
        RPU_CACHE.clear()
        if va_state.DEBUG_MODE: log_debug("RPU cache cleared", "DEBUG")

if HAS_SCHEDULER:
    try:
        va_state.scheduler = BackgroundScheduler()
        va_state.scheduler.start()
        scheduler = va_state.scheduler
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
    # log paths on va_state
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    va_state.LOG_FILE = os.path.join(OUTPUT_DIR, f"{ts}_scan_activity.log")
    va_state.FAIL_FILE = os.path.join(OUTPUT_DIR, f"{ts}_scan_failures.csv")
    try:
        with open(va_state.FAIL_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f, delimiter='|').writerow(['Timestamp', 'Volume', 'Path', 'Filename', 'Error'])
    except (OSError, IOError) as e:
        if va_state.DEBUG_MODE:
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
                        if va_state.DEBUG_MODE:
                            log_debug(f"Failed to remove old log file {f}: {e}", "WARNING")
    except (OSError, IOError) as e:
        if va_state.DEBUG_MODE:
            log_debug(f"Error during log cleanup: {e}", "WARNING")


setup_new_log_files()
cleanup_old_logs()

def log_debug(msg: str, level: str = "INFO") -> None:
    """Log a debug message with optional level (DEBUG, INFO, WARNING, ERROR)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    safe = str(msg).encode('utf-8', 'replace').decode('utf-8')
    fmt = f"[{ts}] [{level}] {safe}"
    print(fmt, flush=True)
    try:
        if va_state.LOG_FILE:
            with open(va_state.LOG_FILE, 'a', encoding='utf-8') as f: f.write(f"{fmt}\n")
    except OSError as e:
        print(f"Failed to write to log file: {e}", flush=True)
    with progress_lock:
        LOG_CACHE.append(fmt)
        if len(LOG_CACHE) > 500: LOG_CACHE.pop(0)

def log_failure(vol: str, path: str, name: str, err: str) -> None:
    """Log a scan failure to both the failure CSV and debug log."""
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if va_state.FAIL_FILE:
            with open(va_state.FAIL_FILE, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f, delimiter='|').writerow([ts, vol, path, name, err])
        # Also log to debug console
        log_debug(f"[FAILURE] {vol}: {name} - {err}", "ERROR")
    except (OSError, IOError) as e:
        log_debug(f"Failed to write failure log: {e}", "WARNING")

def log_scan_warning(path: str, name: str, message: str) -> None:
    """Log a scan warning to the failure CSV so it shows in the failure log file."""
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if va_state.FAIL_FILE:
            with open(va_state.FAIL_FILE, 'a', newline='', encoding='utf-8') as f:
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
        if va_state.DEBUG_MODE:
            log_debug(f"Failed to record scan history: {e}", "WARNING")

def create_scan_job(options: Dict[str, Any]) -> str:
    """Create a durable scan record before work begins."""
    job_id = str(uuid.uuid4())
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute(
            """INSERT INTO scan_jobs
               (job_id, status, started_at, options, progress)
               VALUES (?, 'running', ?, ?, ?)""",
            (job_id, now, json.dumps(options), json.dumps({}))
        )
    return job_id

def update_scan_job(job_id: str | None, status: str | None = None,
                    progress: Dict[str, Any] | None = None) -> None:
    """Persist the latest durable state for a running or completed scan."""
    if not job_id:
        return
    try:
        fields: list[str] = []
        values: list[Any] = []
        if status:
            fields.append("status = ?")
            values.append(status)
        if progress is not None:
            fields.append("progress = ?")
            values.append(json.dumps(progress))
        if status in {"completed", "aborted", "failed", "interrupted"}:
            fields.append("finished_at = ?")
            values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if fields:
            values.append(job_id)
            with get_db() as conn:
                conn.execute(f"UPDATE scan_jobs SET {', '.join(fields)} WHERE job_id = ?", values)
    except (sqlite3.Error, TypeError, ValueError) as e:
        if va_state.DEBUG_MODE:
            log_debug(f"Failed to persist scan job {job_id}: {e}", "WARNING")

def wait_if_paused() -> None:
    """Block worker threads while scan is paused; abort still exits immediately."""
    while not PAUSE_EVENT.is_set():
        if va_state.ABORT_SCAN:
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
    if va_state.DEBUG_MODE:
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
    # abort flag on va_state
    if va_state.ABORT_SCAN: raise RuntimeError("Scan Aborted")
    
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
            if va_state.DEBUG_MODE: log_debug(f"[RUN_COMMAND] Timeout thread killing process {p.pid} for: {cmd_list[0]}", "WARNING")
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
        if va_state.ABORT_SCAN: 
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

def compute_quality_anomaly_flag(meta: dict) -> str | None:
    """Detect conservative codec/quality outliers from analyzed video metadata."""
    flags = []
    width = int(meta.get("width") or 0)
    height = int(meta.get("height") or 0)
    bitrate = float(meta.get("bitrate_mbps") or 0)
    codec = str(meta.get("video_codec") or "").lower().replace(".", "").replace("-", "")
    if width >= 3840 or height >= 2160:
        if 0 < bitrate < 8:
            flags.append("low_bitrate_4k")
        if codec in {"h264", "avc", "avc1", "mpeg4"}:
            flags.append("legacy_codec_4k")
    elif width >= 1920 or height >= 1080:
        if 0 < bitrate < 2:
            flags.append("low_bitrate_1080p")
    if float(meta.get("fps") or 0) > 120:
        flags.append("unusual_frame_rate")
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
        if va_state.DEBUG_MODE:
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
        if va_state.DEBUG_MODE: log_debug(f"[FFPROBE] Starting ffprobe for: {path}", "DEBUG")
        probe_cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', path]
        rc, out, err = run_command(probe_cmd, capture_stderr=True)
        if va_state.DEBUG_MODE: log_debug(f"[FFPROBE] Completed with return code: {rc}", "DEBUG")
        if rc != 0: 
            error_msg = f"ffprobe failed with return code {rc}"
            if err:
                # Include stderr output for more detailed error information
                error_msg = f"ffprobe failed (code {rc}): {err.strip()}"
            result['error'] = error_msg
            if va_state.DEBUG_MODE: log_debug(f"ffprobe failed for {path}: {error_msg}", "ERROR")
            return _finalize_result(result)
        try:
            probe_data = json.loads(out)
        except json.JSONDecodeError as e:
            result['error'] = f"Failed to parse ffprobe JSON: {e}"
            if va_state.DEBUG_MODE: log_debug(f"JSON parse error for {path}: {e}", "ERROR")
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
                if va_state.DEBUG_MODE: log_debug(f"Using cached RPU data for {path}", "DEBUG")
        
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
                if va_state.ABORT_SCAN:
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
                if va_state.ABORT_SCAN:
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
                        if va_state.DEBUG_MODE:
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
                    if va_state.ABORT_SCAN:
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
                            if va_state.DEBUG_MODE:
                                log_debug(f"RPU cache full, evicted oldest entry. Cache size: {len(RPU_CACHE)}", "DEBUG")
                        RPU_CACHE[cache_key] = {'dovi_data': dovi_data, 'rpu_size': rpu_size}
                        if va_state.DEBUG_MODE:
                            log_debug(f"Cached RPU data for {path} (cache size: {len(RPU_CACHE)})", "DEBUG")
            except RuntimeError:
                raise
            except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as e:
                if va_state.DEBUG_MODE:
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
            if va_state.DEBUG_MODE:
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
            if va_state.DEBUG_MODE: log_debug(f"[MEDIAINFO] Starting mediainfo for: {path}", "DEBUG")
            try:
                rc_mi, out_mi, _ = run_command(['mediainfo', '--Output=JSON', path], timeout_seconds=MEDIAINFO_TIMEOUT)
                if va_state.DEBUG_MODE: log_debug(f"[MEDIAINFO] Completed with return code: {rc_mi}", "DEBUG")
            except (RuntimeError, Exception) as e:
                # Catch ALL exceptions from MediaInfo (timeout, errors, etc.) and continue without it
                if va_state.DEBUG_MODE: 
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
                    if va_state.DEBUG_MODE: log_debug(f"[MEDIAINFO] JSON decode error for {path}: {e}, skipping MediaInfo data", "WARNING")
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
                                    if va_state.DEBUG_MODE:
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
                                        if va_state.DEBUG_MODE:
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
                                        if va_state.DEBUG_MODE:
                                            log_debug(f"HDR10+ detected from MediaInfo HDR_Format: {hdr_format}", "DEBUG")
                            
                            # Also check for HDR10+ in transfer characteristics
                            transfer = t.get('transfer_characteristics') or t.get('Transfer_Characteristics')
                            if transfer and ('2094' in str(transfer) or 'HDR10+' in str(transfer).upper()):
                                if "HDR10+" not in sec_hdrs:
                                    sec_hdrs.append("HDR10+")
                                    if va_state.DEBUG_MODE:
                                        log_debug(f"HDR10+ detected from MediaInfo transfer_characteristics: {transfer}", "DEBUG")
        except Exception as e:
            # Outer catch for any unexpected errors - continue without MediaInfo
            if va_state.DEBUG_MODE: 
                log_debug(f"[MEDIAINFO] Outer exception for {path}: {e}, continuing without MediaInfo data", "WARNING")

        # ISOBMFF dvcC/dvvC/dvwC — authoritative delivery profile for MP4/MOV.
        # Profile 20 RPU metadata often looks like Profile 5; trust the container box.
        isom_dovi = None
        try:
            ext = pathlib.Path(path).suffix.lower()
            if ext in _ISOM_CONTAINER_EXTS:
                isom_dovi = parse_isom_dovi_config(path)
        except Exception as e:
            if va_state.DEBUG_MODE:
                log_debug(f"[ISOM-DOVI] parse failed for {path}: {e}", "WARNING")
            isom_dovi = None
        if isom_dovi:
            if isom_dovi.get("is_stereo"):
                result["is_3d"] = 1
            if isom_dovi.get("dovi_profile"):
                isom_prof = str(isom_dovi["dovi_profile"])
                if dovi_profile_raw and dovi_profile_raw != isom_prof and va_state.DEBUG_MODE:
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
                    if va_state.DEBUG_MODE:
                        log_debug(f"Detected P{profile_prefix}.4 (HLG base layer, bl_id={bl_id})", "DEBUG")
                elif bl_id == "4":
                    result['dovi_profile'] = f"{profile_prefix}.4"
                    if va_state.DEBUG_MODE:
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
                if va_state.DEBUG_MODE:
                    log_debug(f"EL type from MediaInfo HDR_Format_Settings: {mi_el_hint}", "DEBUG")
            prof_base = str(result.get('dovi_profile') or '').split('.')[0]
            if prof_base == '7':
                result['dovi_el_type'] = dovi_el_type_raw
            else:
                # FEL/MEL are Profile 7 concepts; ignore for P5/P20/etc.
                result['dovi_el_type'] = None
            
            if va_state.DEBUG_MODE:
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
            if va_state.DEBUG_MODE: log_debug(f"Scan aborted during analysis of {path}", "WARNING")
        else:
            result['error'] = f"Runtime error: {str(e)}"
            if va_state.DEBUG_MODE: log_debug(f"Runtime error analyzing {path}: {e}", "ERROR")
    except Exception as e:
        result['error'] = f"Unexpected error: {str(e)}"
        if va_state.DEBUG_MODE: log_debug(f"Error analyzing {path}: {e}", "ERROR")
        import traceback
        if va_state.DEBUG_MODE: log_debug(f"Traceback: {traceback.format_exc()}", "DEBUG")
    
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


def _format_active_scan_label(last_completed: str | None = None) -> str:
    """Build PROGRESS['file'] from ACTIVE_SCAN_FILES (caller must hold progress_lock)."""
    n = len(ACTIVE_SCAN_FILES)
    if n == 0:
        if last_completed:
            return f"Done: {last_completed}"
        return PROGRESS.get("file") or "Analyzing..."
    # Most recently started is last in OrderedDict
    newest = next(reversed(ACTIVE_SCAN_FILES.values()))
    if n == 1:
        return f"Analyzing: {newest}"
    return f"Analyzing ({n}): {newest} (+{n - 1} more)"

def begin_scan_file(full_path: str, filename: str) -> None:
    """Register a file as in-flight and refresh the scan-info label."""
    with progress_lock:
        ACTIVE_SCAN_FILES[full_path] = filename or os.path.basename(full_path) or full_path
        ACTIVE_SCAN_FILES.move_to_end(full_path)
        PROGRESS["file"] = _format_active_scan_label()
        PROGRESS["active_count"] = len(ACTIVE_SCAN_FILES)

def end_scan_file(full_path: str, filename: str | None = None) -> None:
    """Unregister a finished file and refresh the scan-info label."""
    with progress_lock:
        ACTIVE_SCAN_FILES.pop(full_path, None)
        PROGRESS["file"] = _format_active_scan_label(last_completed=filename)
        PROGRESS["active_count"] = len(ACTIVE_SCAN_FILES)

def clear_active_scan_files() -> None:
    with progress_lock:
        ACTIVE_SCAN_FILES.clear()
        PROGRESS["active_count"] = 0

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
        if va_state.DEBUG_MODE:
            log_debug(f"Path encoding failed for {path_obj}, using fallback: {e}", "WARNING")
        full_path_str = str(path_obj)
        filename = path_obj.name
    else:
        filename = path_obj.name

    begin_scan_file(full_path_str, filename)
    try:
        return _scan_file_worker_body(path_obj, full_path_str, filename)
    finally:
        end_scan_file(full_path_str, filename)

def _scan_file_worker_body(path_obj: pathlib.Path, full_path_str: str, filename: str) -> dict:
    """Inner scan worker (active-file tracking handled by scan_file_worker)."""
    # Early validation - check if file is accessible before attempting analysis
    try:
        if not os.path.exists(full_path_str):
            if va_state.DEBUG_MODE:
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
            if va_state.DEBUG_MODE:
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
        if va_state.DEBUG_MODE:
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
    
    if va_state.DEBUG_MODE: log_debug(f"Processing: {full_path_str}", "DEBUG")
    
    # Retry logic with exponential backoff for transient failures
    max_retries = MAX_RETRIES
    retry_delay = RETRY_DELAY_INITIAL
    meta = None
    for attempt in range(max_retries + 1):
        try:
            # Check for abort before starting analysis
            if va_state.ABORT_SCAN:
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
                if va_state.DEBUG_MODE: log_debug(f"Retry {attempt + 1}/{max_retries} for {full_path_str} after {retry_delay}s", "WARNING")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                if va_state.DEBUG_MODE: log_debug(f"Max retries reached for {full_path_str}", "ERROR")
                meta = _create_error_result(f"Failed after {max_retries} retries: {str(e)}")
        except Exception as e:
            if attempt < max_retries:
                if va_state.DEBUG_MODE: log_debug(f"Retry {attempt + 1}/{max_retries} for {full_path_str} after {retry_delay}s: {e}", "WARNING")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                if va_state.DEBUG_MODE: log_debug(f"Max retries reached for {full_path_str}: {e}", "ERROR")
                meta = _create_error_result(f"Failed after {max_retries} retries: {str(e)}")
    
    if meta is None:
        meta = _create_error_result("Analysis failed")
    if meta.get('error'):
        _enrich_from_nfo_and_filename(full_path_str, meta)
    file_size = 0
    try:
        file_size = os.path.getsize(full_path_str)
    except OSError as e:
        if va_state.DEBUG_MODE: log_debug(f"Failed to get file size for {full_path_str}: {e}", "WARNING")

    # Note: scan_attempts will be calculated in run_scan based on previous attempts
    validation_flag = compute_validation_flag({
        "media_type": meta.get('media_type'),
        "show_title": meta.get('show_title'),
        "episode_title": meta.get('episode_title'),
        "movie_title": meta.get('movie_title'),
        "season": meta.get('season'),
        "episode": meta.get('episode')
    })
    quality_flag = compute_quality_anomaly_flag({
        "width": meta.get("width"), "height": meta.get("height"),
        "bitrate_mbps": meta.get("bitrate"), "video_codec": meta.get("video_codec"),
        "fps": meta.get("fps"),
    })
    file_mtime = None
    try:
        file_mtime = os.path.getmtime(full_path_str)
    except OSError:
        file_mtime = None
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
        "validation_flag": validation_flag,
        "quality_anomaly": quality_flag,
        "file_mtime": file_mtime,
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
        item.setdefault('quality_anomaly', None)
        item.setdefault('file_mtime', None)
    
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
                 scan_attempts, video_source, source_format, video_codec, is_3d, edition, year, media_type, show_title, season, episode, movie_title, episode_title, nfo_missing, missing, validation_flag, quality_anomaly, file_mtime, dup_group_key, dup_exact_key, dup_count) 
                VALUES (:filename, :category, :profile, :el_type, :container, :source_vol, :full_path, :last_scanned, 
                 :resolution, :bitrate_mbps, :scan_error, :is_hybrid, :is_source_hybrid, :secondary_hdr, :width, :height, 
                 :file_size, :bl_compatibility_id, :audio_codecs, :audio_langs, :audio_channels, :subtitles, :max_cll, :max_fall, :fps, :aspect_ratio,
                 :imdb_id, :tvdb_id, :tmdb_id, :rotten_id, :metacritic_id, :trakt_id,
                 :tvdb_series_id, :tvdb_episode_id, :imdb_series_id, :imdb_episode_id, :tmdb_series_id, :tmdb_episode_id,
                 :trakt_series_id, :trakt_episode_id, :rotten_series_id, :rotten_episode_id, :metacritic_series_id, :metacritic_episode_id,
                 :imdb_rating, :tvdb_rating, :tmdb_rating, :rotten_rating, :metacritic_rating, :trakt_rating,
                 :scan_attempts, :video_source, :source_format, :video_codec, :is_3d, :edition, :year, :media_type, :show_title, :season, :episode, :movie_title, :episode_title, :nfo_missing, :missing, :validation_flag, :quality_anomaly, :file_mtime, :dup_group_key, :dup_exact_key, :dup_count)""", sanitized_list)
            if duplicate_check_on_scan:
                recompute_duplicate_counts(conn)
            if va_state.DEBUG_MODE:
                for item in sanitized_list:
                    log_debug(f"Saved to DB: {item['filename']} -> {item['category']} {item['profile']} (error: {item.get('scan_error', 'None')})", "DEBUG")
    except sqlite3.Error as e:
        log_debug(f"Database error saving batch: {e}", "ERROR")
        if va_state.DEBUG_MODE:
            log_debug(f"Failed batch items: {[item.get('filename', 'unknown') for item in sanitized_list]}", "ERROR")


# --- compatibility barrel: scan + routes + db/queries own implementations ---
from video_analyzer.db.stats import (  # noqa: E402
    _audio_codec_counts_sql, _build_stats_sql, _compute_enriched_stats,
    _group_col_counts, _load_scan_folders, _path_counts_for_where,
    _secondary_hdr_counts, get_or_build_library_stats_bundle,
)
from video_analyzer.db.maintenance import cleanup_old_rpu_files, perform_cleanup_db  # noqa: E402
from video_analyzer.queries.filters import (  # noqa: E402
    build_filter_query, parse_advanced_search, parse_positive_int, parse_sort_order,
)
from video_analyzer.queries.videos_export import (  # noqa: E402
    _VIDEOS_COLUMN_NAMES, _VIDEOS_ROW_COLUMNS, _VIDEOS_SORT_MAP,
    _export_query_parts, _row_to_export_dict,
)
from video_analyzer.scan.pipeline import (  # noqa: E402
    analyze_files, cleanup_deleted_files, collect_files_to_scan, count_removed_files,
    finalize_scan, iter_bounded_scan_futures, iter_job_scan_paths, load_interrupted_job,
    load_processed_map, mark_scan_job_file, pending_scan_file_count, persist_pending_scan_paths,
    prepare_scan_paths, build_scan_paths_from_folders, parse_skip_rules,
    folder_matches_skip_rules, file_matches_skip_rules, record_seen_paths,
    reset_scan_seen_files, run_scan, take_pending_scan_batch, clear_scan_job_files,
)
from video_analyzer.routes import handlers as _routes  # noqa: E402


def __getattr__(name: str):
    if hasattr(_routes, name):
        return getattr(_routes, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
