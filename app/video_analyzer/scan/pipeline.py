from __future__ import annotations

import os
import pathlib
import json
import sqlite3
import time
import fnmatch
import signal
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from video_analyzer.config import (
    MAX_SCAN_ATTEMPTS, PROCESSED_MAP_CHUNK_SIZE, PROGRESS_UPDATE_INTERVAL,
    VIDEO_EXTENSIONS,
)
from video_analyzer.db.connection import get_db, get_db_readonly
from video_analyzer.state import (
    ACTIVE_PROCS, ACTIVE_SCAN_FILES, PAUSE_EVENT, PROGRESS, proc_lock, progress_lock,
)
from video_analyzer import core
from video_analyzer import state as va_state

log_debug = core.log_debug
wait_if_paused = core.wait_if_paused
save_batch_to_db = core.save_batch_to_db
log_failure = core.log_failure
update_scan_job = core.update_scan_job
create_scan_job = core.create_scan_job
clear_rpu_cache = core.clear_rpu_cache
setup_new_log_files = core.setup_new_log_files
cleanup_old_logs = core.cleanup_old_logs
record_scan_history = core.record_scan_history
get_mount_status = core.get_mount_status

_SEASON_DIR_RE = re.compile(r'^(season[\s._-]*\d+|s\d{1,2})$', re.IGNORECASE)

def reset_scan_seen_files() -> None:
    """Clear the per-scan seen-path table used for missing-file cleanup."""
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM scan_seen_files")
    except sqlite3.Error as e:
        log_debug(f"Could not reset scan_seen_files: {e}", "WARNING")


def record_seen_paths(paths: list[str]) -> None:
    if not paths:
        return
    try:
        with get_db() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO scan_seen_files (full_path) VALUES (?)",
                [(p,) for p in paths],
            )
    except sqlite3.Error as e:
        log_debug(f"Could not record seen paths: {e}", "WARNING")


def clear_scan_job_files(job_id: str | None = None) -> None:
    """Drop pending rows for a previous job, or all jobs when starting a full scan."""
    try:
        with get_db() as conn:
            if job_id:
                conn.execute("DELETE FROM scan_job_files WHERE job_id != ?", (job_id,))
            else:
                conn.execute("DELETE FROM scan_job_files")
    except sqlite3.Error as e:
        log_debug(f"Could not clear scan_job_files: {e}", "WARNING")


def persist_pending_scan_paths(job_id: str | None, paths: list[str]) -> None:
    if not job_id or not paths:
        return
    try:
        with get_db() as conn:
            conn.executemany(
                """INSERT OR IGNORE INTO scan_job_files (job_id, full_path, status)
                   VALUES (?, ?, 'pending')""",
                [(job_id, p) for p in paths],
            )
    except sqlite3.Error as e:
        log_debug(f"Could not persist pending scan paths: {e}", "WARNING")


def mark_scan_job_file(job_id: str | None, full_path: str, status: str) -> None:
    if not job_id or not full_path:
        return
    try:
        with get_db() as conn:
            conn.execute(
                "UPDATE scan_job_files SET status=? WHERE job_id=? AND full_path=?",
                (status, job_id, full_path),
            )
    except sqlite3.Error as e:
        if va_state.DEBUG_MODE:
            log_debug(f"Could not mark scan job file {full_path}: {e}", "DEBUG")


def pending_scan_file_count(job_id: str | None) -> int:
    if not job_id:
        return 0
    try:
        with get_db_readonly() as conn:
            row = conn.execute(
                """SELECT COUNT(*) FROM scan_job_files
                   WHERE job_id=? AND status IN ('pending', 'queued')""",
                (job_id,),
            ).fetchone()
            return int(row[0] or 0)
    except sqlite3.Error:
        return 0


def take_pending_scan_batch(job_id: str, limit: int) -> list[str]:
    """Claim a small batch of pending paths so analysis memory stays bounded."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT full_path FROM scan_job_files
               WHERE job_id=? AND status='pending' LIMIT ?""",
            (job_id, max(1, limit)),
        ).fetchall()
        paths = [str(r[0]) for r in rows]
        if paths:
            conn.executemany(
                "UPDATE scan_job_files SET status='queued' WHERE job_id=? AND full_path=?",
                [(job_id, p) for p in paths],
            )
        return paths


def iter_job_scan_paths(job_id: str, batch_size: int = 32):
    """Yield pending job paths without loading the full queue into memory."""
    while True:
        batch = take_pending_scan_batch(job_id, batch_size)
        if not batch:
            break
        for path in batch:
            yield pathlib.Path(path)


def load_interrupted_job(job_id: str) -> dict[str, Any] | None:
    try:
        with get_db_readonly() as conn:
            row = conn.execute(
                """SELECT job_id, status, options FROM scan_jobs WHERE job_id=?""",
                (job_id,),
            ).fetchone()
            if not row:
                return None
            try:
                options = json.loads(row[2] or "{}")
            except (TypeError, ValueError):
                options = {}
            pending = conn.execute(
                """SELECT COUNT(*) FROM scan_job_files
                   WHERE job_id=? AND status IN ('pending', 'queued')""",
                (job_id,),
            ).fetchone()[0]
            return {
                "job_id": row[0],
                "status": row[1],
                "options": options,
                "pending_count": int(pending or 0),
            }
    except sqlite3.Error:
        return None


def _last_scanned_timestamp(last_scanned: str | None) -> float | None:
    if not last_scanned:
        return None
    try:
        return datetime.strptime(str(last_scanned), "%Y-%m-%d %H:%M:%S").timestamp()
    except (TypeError, ValueError, OSError):
        return None


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
            rows = conn.execute(
                "SELECT full_path, file_size, scan_attempts, scan_error, last_scanned, file_mtime FROM videos LIMIT ? OFFSET ?",
                (chunk_size, offset),
            ).fetchall()
            if not rows:
                break
            processed_map.update({
                row[0]: {
                    'size': row[1],
                    'attempts': row[2] or 0,
                    'error': row[3],
                    'last_scanned': row[4],
                    'file_mtime': row[5],
                }
                for row in rows
            })
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
                          scan_extras: bool, changed_only: bool = False,
                          changed_after: float | None = None,
                          job_id: str | None = None,
                          keep_found_set: bool = True) -> tuple[list, set]:
    """
    Walk files and collect those that need analysis.

    Hybrid incremental (changed_only): always walk files (no directory-mtime prune).
    Skip analysis when the path exists, size matches, there is no scan_error, and
    file mtime is not newer than last_scanned. Force-rescan still analyzes all.

    When job_id is set, pending paths are persisted to scan_job_files and the
    in-memory Path list is not retained. Seen paths go to scan_seen_files.
    """
    files_to_scan = []
    all_found_files = set()
    total_seen = 0
    queued_count = 0
    last_vol_started = None
    vol_start_time = 0.0
    file_subs, file_globs, folder_rules = parse_skip_rules(skip_words)
    seen_batch: list[str] = []
    pending_batch: list[str] = []
    persist_queue = bool(job_id)

    reset_scan_seen_files()
    if persist_queue:
        clear_scan_job_files(job_id)

    with progress_lock:
        PROGRESS["file"] = "Scanning directories..."
    log_debug("[CRAWL] Starting directory scan...", "INFO")

    def flush_seen() -> None:
        if seen_batch:
            record_seen_paths(seen_batch)
            seen_batch.clear()

    def flush_pending() -> None:
        if pending_batch:
            persist_pending_scan_paths(job_id, pending_batch)
            pending_batch.clear()
        
    for path in scan_paths:
        wait_if_paused()
        if va_state.ABORT_SCAN:
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
                if va_state.ABORT_SCAN:
                    log_debug(f"[ABORT] Abort detected while scanning {root}, stopping directory walk", "INFO")
                    break
                dir_count += 1
                if dir_count <= 10 or dir_count % 100 == 0:
                    log_debug(f"[CRAWL] [{current_vol}] Traversing directory {dir_count}: {root}", "INFO")
                elapsed_vol = time.time() - vol_start_time
                show_found = elapsed_vol >= 3.0 or total_seen >= 1
                throttle = (total_seen == 0 and dir_count % 50 == 0) or (total_seen == 1) or (total_seen > 1 and total_seen % 500 == 0)
                if show_found and throttle:
                    elapsed = int(time.time() - start_time)
                    with progress_lock:
                        PROGRESS["file"] = f"Scanning [{current_vol}]: Found {total_seen} files ({queued_count} new)"
                        PROGRESS["last_duration"] = f"{elapsed}s"
                if os.path.isfile(os.path.join(root, '.scanignore')):
                    dirs[:] = []
                    continue
                if not scan_extras:
                    def should_skip_extras(parent_dir: str) -> bool:
                        try:
                            with os.scandir(parent_dir) as it:
                                for entry in it:
                                    name = entry.name
                                    if entry.is_file():
                                        if name.lower().endswith('.nfo'):
                                            return True
                                        if pathlib.Path(name).suffix.lower() in VIDEO_EXTENSIONS:
                                            return True
                                    elif entry.is_dir():
                                        if _SEASON_DIR_RE.match(name):
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
                            if va_state.DEBUG_MODE:
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
                        if va_state.DEBUG_MODE:
                            log_debug(f"Skipping file (IGNORE): {full_p}", "DEBUG")
                        continue
                    
                    try:
                        current_size = os.path.getsize(full_p)
                        if min_size > 0 and current_size < min_size:
                            if va_state.DEBUG_MODE:
                                log_debug(f"Skipping file (size < {min_size}): {full_p} ({current_size} bytes)", "DEBUG")
                            continue
                    except (OSError, PermissionError) as e:
                        if va_state.DEBUG_MODE:
                            log_debug(f"Error getting size for {full_p}: {e}", "DEBUG")
                        continue
                            
                    fp_str = os.fsdecode(os.fsencode(full_p))
                    seen_batch.append(fp_str)
                    if keep_found_set:
                        all_found_files.add(fp_str)
                    if len(seen_batch) >= 200:
                        flush_seen()
                    
                    existing = processed_map.get(fp_str)
                    should_scan = False
                    
                    if not existing:
                        should_scan = True
                        if va_state.DEBUG_MODE:
                            log_debug(f"New file to scan: {fp_str}", "DEBUG")
                    elif force_rescan:
                        should_scan = True
                        if va_state.DEBUG_MODE:
                            log_debug(f"Force rescan: {fp_str}", "DEBUG")
                    elif existing.get('attempts', 0) > MAX_SCAN_ATTEMPTS:
                        should_scan = False
                        if va_state.DEBUG_MODE:
                            log_debug(f"Skipping file (attempts > {MAX_SCAN_ATTEMPTS}): {fp_str}", "DEBUG")
                    elif existing.get('error'):
                        should_scan = True
                        if va_state.DEBUG_MODE:
                            log_debug(f"Rescanning file with error: {fp_str}", "DEBUG")
                    elif existing.get('size') != current_size:
                        should_scan = True
                        if va_state.DEBUG_MODE:
                            log_debug(f"File size changed: {fp_str} ({existing.get('size')} -> {current_size})", "DEBUG")
                    elif changed_only:
                        try:
                            file_mtime = os.path.getmtime(full_p)
                        except OSError:
                            file_mtime = None
                        last_ts = _last_scanned_timestamp(existing.get('last_scanned'))
                        if file_mtime is None or last_ts is None or file_mtime > last_ts:
                            should_scan = True
                            if va_state.DEBUG_MODE:
                                log_debug(f"File mtime newer than last scan: {fp_str}", "DEBUG")
                        else:
                            should_scan = False
                    
                    if should_scan:
                        queued_count += 1
                        if persist_queue:
                            pending_batch.append(fp_str)
                            if len(pending_batch) >= 200:
                                flush_pending()
                        else:
                            files_to_scan.append(pathlib.Path(fp_str))
                        if va_state.DEBUG_MODE and queued_count % 100 == 0:
                            log_debug(f"Added {queued_count} files to scan queue...", "DEBUG")
                    
                    if total_seen == 1 or total_seen % 500 == 0:
                        elapsed = int(time.time() - start_time)
                        with progress_lock:
                            PROGRESS["file"] = f"Scanning [{current_vol}]: Found {total_seen} files ({queued_count} new)"
                            PROGRESS["last_duration"] = f"{elapsed}s"
                        if va_state.DEBUG_MODE:
                            log_debug(f"[CRAWL] [{current_vol}] Found {total_seen} files ({queued_count} new) - {elapsed}s elapsed", "DEBUG")
        except (OSError, PermissionError) as e:
            log_debug(f"Error scanning {path}: {e}", "ERROR")

    flush_seen()
    flush_pending()
    return files_to_scan, all_found_files

def iter_bounded_scan_futures(executor: ThreadPoolExecutor, paths: list, max_inflight: int):
    """Yield completed scan futures while bounding queued work and memory."""
    pending = set()
    iterator = iter(paths)
    try:
        for _ in range(max(1, max_inflight)):
            try:
                pending.add(executor.submit(core.scan_file_worker, next(iterator)))
            except StopIteration:
                break
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                yield future
                try:
                    pending.add(executor.submit(core.scan_file_worker, next(iterator)))
                except StopIteration:
                    pass
    finally:
        for future in pending:
            future.cancel()

def analyze_files(files_to_scan: list, processed_map: dict, settings: dict, 
                  final_threads: int, start_time: float, job_id: str | None = None) -> dict:
    """
    Analyze files using ThreadPoolExecutor and return metrics.
    
    Args:
        files_to_scan: List of file paths to analyze (ignored when job_id has pending rows)
        processed_map: Dictionary of processed files for attempt tracking
        settings: Dictionary of scan settings
        final_threads: Number of threads to use
        start_time: Scan start time for progress updates
        job_id: Durable scan job whose pending paths should be consumed from SQLite
    
    Returns:
        Dictionary containing metrics_sum and metrics_count
    """
    path_source: Any = files_to_scan
    total_to_scan = len(files_to_scan)
    if job_id:
        queued = pending_scan_file_count(job_id)
        if queued:
            path_source = iter_job_scan_paths(job_id, max(8, final_threads * 2))
            total_to_scan = queued
    log_debug(f"[ANALYZING] {total_to_scan} files (New/Modified)...", "INFO")
    with progress_lock:
        PROGRESS["total"] = total_to_scan
        
    batch_buffer = []
    metrics_sum = {"bitrate": 0.0, "width": 0, "height": 0, "file_size": 0}
    metrics_count = {"bitrate": 0, "width": 0, "height": 0, "file_size": 0}
    progress_updates = {"current": 0, "failed_count": 0, "new_found": 0}
    duplicate_check_on_scan = str(settings.get('duplicate_check_on_scan', 'false')).lower() == 'true'
            
    with ThreadPoolExecutor(max_workers=final_threads) as executor:
        for f in iter_bounded_scan_futures(executor, path_source, final_threads * 2):
            wait_if_paused()
            if va_state.ABORT_SCAN:
                log_debug("[ABORT] Abort detected in analyze_files loop, stopping file processing", "INFO")
                break
            try:
                res = f.result()
                if va_state.DEBUG_MODE:
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
                    mark_scan_job_file(job_id, res['full_path'], 'error')
                    if va_state.DEBUG_MODE:
                        log_debug(f"File has error, will still be saved: {res['full_path']} - {res['scan_error']}", "DEBUG")
                else:
                    progress_updates["new_found"] += 1
                    mark_scan_job_file(job_id, res['full_path'], 'done')
                    if va_state.DEBUG_MODE:
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
                        job_progress = {
                            "current": PROGRESS.get("current", 0),
                            "total": PROGRESS.get("total", 0),
                            "failed": PROGRESS.get("failed_count", 0),
                            "new": PROGRESS.get("new_found", 0),
                            "file": PROGRESS.get("file", ""),
                        }
                    update_scan_job(va_state.ACTIVE_SCAN_JOB_ID, progress=job_progress)
                    # diag ts on va_state
                    now = time.time()
                    if now - va_state.DIAG_LOG_TS >= 5:
                        log_debug(
                            f"[SCAN_DIAG] current={PROGRESS.get('current', 0)}/{PROGRESS.get('total', 0)} "
                            f"new={PROGRESS.get('new_found', 0)} failed={PROGRESS.get('failed_count', 0)} "
                            f"batch_buffer={len(batch_buffer)}",
                            "INFO"
                        )
                        va_state.DIAG_LOG_TS = now
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

def _iter_missing_db_paths(conn: Any, target_vols: list | None, scan_paths: list,
                           all_found_files: set | None, use_seen_table: bool) -> list[str]:
    """Return DB paths under this scan that were not seen on disk."""
    if use_seen_table:
        if target_vols and len(target_vols) > 0:
            placeholders = ','.join('?' * len(target_vols))
            rows = conn.execute(
                f"""SELECT v.full_path FROM videos v
                    WHERE v.source_vol IN ({placeholders})
                      AND NOT EXISTS (
                          SELECT 1 FROM scan_seen_files s WHERE s.full_path = v.full_path
                      )""",
                tuple(target_vols),
            ).fetchall()
            return [r[0] for r in rows]
        clauses = []
        params: list[Any] = []
        for prefix in scan_paths:
            clauses.append("substr(v.full_path, 1, ?) = ?")
            params.extend([len(prefix), prefix])
        where_prefix = f"({' OR '.join(clauses)})" if clauses else "1=0"
        rows = conn.execute(
            f"""SELECT v.full_path FROM videos v
                WHERE {where_prefix}
                  AND NOT EXISTS (
                      SELECT 1 FROM scan_seen_files s WHERE s.full_path = v.full_path
                  )""",
            params,
        ).fetchall()
        return [r[0] for r in rows]

    found = all_found_files or set()
    if target_vols and len(target_vols) > 0:
        placeholders = ','.join('?' * len(target_vols))
        existing = {row[0] for row in conn.execute(
            f"SELECT full_path FROM videos WHERE source_vol IN ({placeholders})",
            tuple(target_vols),
        ).fetchall()}
    else:
        online_prefixes = tuple(scan_paths)
        existing = {
            r[0] for r in conn.execute("SELECT full_path FROM videos").fetchall()
            if r[0].startswith(online_prefixes)
        }
    return [f for f in existing if f not in found]


def count_removed_files(target_vols: list | None, scan_paths: list, all_found_files: set | None = None,
                        use_seen_table: bool = False) -> int:
    """
    Count files in DB that would be removed (no longer on disk).
    Does not modify the database.
    """
    with get_db() as conn:
        return len(_iter_missing_db_paths(conn, target_vols, scan_paths, all_found_files, use_seen_table))


def cleanup_deleted_files(target_vols: list | None, scan_paths: list, all_found_files: set | None = None,
                          remove_from_db: bool = True, use_seen_table: bool = False) -> int:
    """
    Remove or mark files from database that no longer exist on disk.
    """
    log_debug("🧹 Running cleanup...", "INFO")
    with get_db() as conn:
        to_del = _iter_missing_db_paths(conn, target_vols, scan_paths, all_found_files, use_seen_table)

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
        total_bytes = conn.execute(
            "SELECT COALESCE(SUM(file_size), 0) FROM videos WHERE COALESCE(missing, 0)=0"
        ).fetchone()[0] or 0
        duplicate_savings = conn.execute(
            """SELECT COALESCE(SUM(group_size - keep_size), 0) FROM (
                 SELECT dup_group_key, SUM(file_size) AS group_size,
                        MAX(file_size) AS keep_size
                 FROM videos
                 WHERE COALESCE(missing, 0)=0 AND dup_group_key IS NOT NULL
                 GROUP BY dup_group_key HAVING COUNT(*) > 1
               )"""
        ).fetchone()[0] or 0
        conn.execute(
            "INSERT INTO storage_snapshots (captured_at, total_bytes, duplicate_savings_bytes) VALUES (?, ?, ?)",
            (now, int(total_bytes), int(duplicate_savings))
        )
        conn.execute(
            "DELETE FROM storage_snapshots WHERE id NOT IN "
            "(SELECT id FROM storage_snapshots ORDER BY id DESC LIMIT 120)"
        )
            
    avg_bitrate = round(metrics_sum["bitrate"] / metrics_count["bitrate"], 2) if metrics_count["bitrate"] > 0 else 0
    avg_width = round(metrics_sum["width"] / metrics_count["width"]) if metrics_count["width"] > 0 else 0
    avg_height = round(metrics_sum["height"] / metrics_count["height"]) if metrics_count["height"] > 0 else 0
    avg_file_size_mb = round(metrics_sum["file_size"] / metrics_count["file_size"] / (1024 * 1024), 2) if metrics_count["file_size"] > 0 else 0
            
    with progress_lock:
        PROGRESS.update({"last_full_scan": now, "last_duration": dur, "scan_completed": True, "status": "idle", "paused": False})
        ACTIVE_SCAN_FILES.clear()
        PROGRESS["active_count"] = 0
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
    update_scan_job(
        va_state.ACTIVE_SCAN_JOB_ID,
        "completed",
        {"current": PROGRESS["total"], "total": PROGRESS["total"],
         "failed": PROGRESS["failed_count"], "new": PROGRESS["new_found"],
         "duration": dur}
    )
            
    log_debug(f"[SUCCESS] Finished: {dur}. Added: {PROGRESS['new_found']}. Errors: {PROGRESS['failed_count']}", "INFO")


def run_scan(thread_count: Optional[int] = None, target_vols: Optional[List[str]] = None, 
             force_rescan: bool = False, debug: bool = False, scan_mode: str = "all",
             scan_folder: dict | None = None, scan_scope: str = "all",
             resume_job_id: str | None = None, preclaimed: bool = False) -> None:
    """
    Main scan function that orchestrates the entire scanning process.
    
    This function coordinates database loading, file collection, analysis, and cleanup.
    It handles abort signals and ensures proper cleanup of resources.
    
    Args:
        thread_count: Number of worker threads to use for file analysis. If None, uses saved setting.
        target_vols: List of volume names to scan. If None, scans all mounted volumes.
        force_rescan: If True, resets scan attempts and rescans all files regardless of previous status.
        debug: If True, enables verbose debug logging throughout the scan process.
        resume_job_id: If set, skip crawl and analyze remaining pending paths from that job.
        preclaimed: True when POST /start already set PROGRESS status to scanning.
        
    Raises:
        RuntimeError: If scan is already in progress (race condition protection).
    """
    # scan flags live on video_analyzer.state
    changed_only = scan_scope == "changed"
    start_time = time.time()
    resume_info = load_interrupted_job(resume_job_id) if resume_job_id else None
    if resume_info and resume_info.get("pending_count", 0) > 0:
        options = resume_info.get("options") or {}
        thread_count = thread_count if thread_count is not None else options.get("thread_count")
        target_vols = target_vols if target_vols else options.get("target_vols") or None
        force_rescan = bool(options.get("force_rescan", force_rescan))
        scan_mode = options.get("scan_mode") or scan_mode
        scan_folder = scan_folder if scan_folder is not None else options.get("scan_folder")
        scan_scope = options.get("scan_scope") or scan_scope
        changed_only = scan_scope == "changed"
    
    # Check and set status atomically to prevent race condition.
    # /start may already have claimed scanning (preclaimed=True) so /progress
    # cannot look idle before this thread actually runs.
    already_running = False
    running_progress = (0, 0)
    with progress_lock:
        if PROGRESS["status"] == "scanning" and not preclaimed:
            already_running = True
            running_progress = (PROGRESS.get("current", 0), PROGRESS.get("total", 0))
        elif PROGRESS["status"] != "scanning":
            PROGRESS.update({"status": "scanning", "current": 0, "total": 0, "file": "Initializing...", "scan_completed": False, "new_found": 0, "removed": 0, "failed_count": 0, "warning_count": 0, "last_duration": "0s", "start_time": start_time, "active_count": 0})
            ACTIVE_SCAN_FILES.clear()
        elif preclaimed:
            PROGRESS["start_time"] = start_time
            PROGRESS["file"] = PROGRESS.get("file") or "Initializing..."
    if already_running:
        log_debug(f"[WARNING] Attempted to start scan while already scanning! Current progress: {running_progress[0]}/{running_progress[1]}", "WARNING")
        return
    
    va_state.ABORT_SCAN = False
    PAUSE_EVENT.set()
    with progress_lock:
        PROGRESS["paused"] = False
    va_state.DEBUG_MODE = debug
    try:
        if resume_info:
            va_state.ACTIVE_SCAN_JOB_ID = resume_info["job_id"]
            with get_db() as conn:
                conn.execute(
                    "UPDATE scan_jobs SET status='running', finished_at=NULL WHERE job_id=?",
                    (va_state.ACTIVE_SCAN_JOB_ID,),
                )
                conn.execute(
                    "UPDATE scan_job_files SET status='pending' WHERE job_id=? AND status='queued'",
                    (va_state.ACTIVE_SCAN_JOB_ID,),
                )
            with progress_lock:
                PROGRESS["job_id"] = va_state.ACTIVE_SCAN_JOB_ID
        else:
            va_state.ACTIVE_SCAN_JOB_ID = create_scan_job({
                "thread_count": thread_count,
                "target_vols": target_vols or [],
                "force_rescan": force_rescan,
                "scan_mode": scan_mode,
                "scan_folder": scan_folder,
                "scan_scope": scan_scope,
            })
            with progress_lock:
                PROGRESS["job_id"] = va_state.ACTIVE_SCAN_JOB_ID
    except sqlite3.Error as e:
        va_state.ACTIVE_SCAN_JOB_ID = None
        log_debug(f"Could not create durable scan job: {e}", "WARNING")
    
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
        if va_state.DEBUG_MODE: log_debug(f"Error reading thread setting: {e}")
    if thread_count: final_threads = int(thread_count)

    log_debug("[INIT] Initializing scan...", "INFO")
    
    try:
        with get_db() as conn:
            settings = dict(conn.execute("SELECT key, value FROM settings").fetchall())
        
        skip_words = [w.strip().lower() for w in settings.get('skip_words', '').split(',') if w.strip()]
        min_size = int(settings.get('min_size_mb', 0)) * 1024 * 1024
        scan_extras = str(settings.get('scan_extras', 'false')).lower() == 'true'
        
        log_debug(f"[STARTED] Scan started. Threads={final_threads}. Force={force_rescan}. Debug={va_state.DEBUG_MODE}", "INFO")
        
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
        changed_after = None
        if changed_only:
            try:
                prior_scan = settings.get("last_full_scan")
                changed_after = (
                    datetime.strptime(prior_scan, "%Y-%m-%d %H:%M:%S").timestamp()
                    if prior_scan else 0
                )
            except (TypeError, ValueError, OSError):
                changed_after = 0
        files_to_scan: list = []
        all_found_files: set = set()
        skip_crawl = bool(resume_info and resume_info.get("pending_count", 0) > 0)
        if skip_crawl:
            log_debug(f"[RESUME] Skipping crawl; {resume_info['pending_count']} pending paths", "INFO")
        else:
            files_to_scan, all_found_files = collect_files_to_scan(
                scan_paths, path_to_vol, processed_map, skip_words, min_size,
                force_rescan, start_time, scan_extras, changed_only, changed_after,
                job_id=va_state.ACTIVE_SCAN_JOB_ID, keep_found_set=False
            )
        
        use_seen_table = not skip_crawl
        removed = 0
        if not va_state.ABORT_SCAN and not skip_crawl:
            removed = count_removed_files(
                target_vols, scan_paths, all_found_files, use_seen_table=True
            )
        queued = pending_scan_file_count(va_state.ACTIVE_SCAN_JOB_ID) if va_state.ACTIVE_SCAN_JOB_ID else len(files_to_scan)
        total_found = queued
        try:
            with get_db_readonly() as conn:
                total_found = conn.execute("SELECT COUNT(*) FROM scan_seen_files").fetchone()[0] or queued
        except sqlite3.Error:
            total_found = len(all_found_files) or queued
        with progress_lock:
            PROGRESS["removed"] = removed
            PROGRESS["total_found"] = total_found
            PROGRESS["file"] = f"Found {total_found} ({queued} new / {removed} removed)"
        
        metrics = {"metrics_sum": {"bitrate": 0.0, "width": 0, "height": 0, "file_size": 0},
                   "metrics_count": {"bitrate": 0, "width": 0, "height": 0, "file_size": 0}}
        
        if not va_state.ABORT_SCAN and (files_to_scan or queued):
            metrics = analyze_files(
                files_to_scan, processed_map, settings, final_threads, start_time,
                job_id=va_state.ACTIVE_SCAN_JOB_ID,
            )
        
        if not va_state.ABORT_SCAN:
            remove_missing_from_db = str(settings.get('remove_missing_from_db', 'true')).lower() == 'true'
            if skip_crawl:
                removed = 0
            else:
                removed = cleanup_deleted_files(
                    target_vols, scan_paths, all_found_files,
                    remove_from_db=remove_missing_from_db, use_seen_table=use_seen_table,
                )
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
                        if va_state.DEBUG_MODE:
                            log_debug(f"Failed to kill process {p.pid}: {e}", "DEBUG")
            log_debug("[ABORT] User aborted.")
            dur = f"{int(time.time() - start_time)}s"
            with progress_lock:
                PROGRESS.update({"status": "idle", "file": "Aborted", "paused": False, "scan_completed": True, "last_duration": dur})
                ACTIVE_SCAN_FILES.clear()
                PROGRESS["active_count"] = 0
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
            update_scan_job(
                va_state.ACTIVE_SCAN_JOB_ID,
                "aborted",
                {"current": PROGRESS.get("current", 0),
                 "total": PROGRESS.get("total", 0),
                 "failed": PROGRESS.get("failed_count", 0),
                 "duration": dur}
            )
            if va_state.ACTIVE_SCAN_JOB_ID:
                try:
                    with get_db() as conn:
                        conn.execute(
                            "UPDATE scan_job_files SET status='pending' WHERE job_id=? AND status='queued'",
                            (va_state.ACTIVE_SCAN_JOB_ID,),
                        )
                except sqlite3.Error:
                    pass

    except Exception as e:
        log_debug(f"[ERROR] CRITICAL: {e}")
        import traceback; traceback.print_exc()
        with progress_lock:
            PROGRESS["status"] = "idle"
            PROGRESS["file"] = "Scan failed"
        update_scan_job(
            va_state.ACTIVE_SCAN_JOB_ID,
            "failed",
            {"current": PROGRESS.get("current", 0),
             "total": PROGRESS.get("total", 0),
             "error": str(e)}
        )

