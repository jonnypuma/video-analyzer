"""Scan control HTTP handlers."""
from __future__ import annotations

import os
import json
import sqlite3
import threading
import csv
import io
import time
import uuid
import zipfile
import pathlib
import shutil
import sys
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from flask import jsonify, make_response, render_template, request, send_file, Response, session

from video_analyzer.blueprint import bp
from video_analyzer import core
from video_analyzer import state as va_state
from video_analyzer.config import (
    BASE_DIR, DB_PATH, OUTPUT_DIR, RADARR_API_KEY, RADARR_URL, SONARR_API_KEY, SONARR_URL,
    VIDEO_EXTENSIONS, app_version_label,
)
from video_analyzer.db.connection import get_db, get_db_readonly
from video_analyzer.db.maintenance import perform_cleanup_db
from video_analyzer.db.schema import ensure_video_column
from video_analyzer.db.stats import (
    _audio_codec_counts_sql, _compute_enriched_stats, _group_col_counts,
    get_or_build_library_stats_bundle,
)
from video_analyzer.queries.filters import (
    build_filter_query, parse_advanced_search, parse_positive_int, parse_sort_order,
)
from video_analyzer.queries.videos_export import (
    _EXPORT_CHUNK_SIZE, _VIDEOS_COLUMN_NAMES, _VIDEOS_ROW_COLUMNS, _VIDEOS_SORT_MAP,
    _export_query_parts, _row_to_export_dict,
)
from video_analyzer.state import (
    APP_START_TIME, LIBRARY_STATS_CACHE, LOG_CACHE, PAUSE_EVENT, PROGRESS,
    ACTIVE_PROCS, ACTIVE_SCAN_FILES, library_stats_cache_lock, proc_lock, progress_lock,
)

log_debug = core.log_debug
run_command = core.run_command
reject_if_busy = core.reject_if_busy
get_mount_status = core.get_mount_status
get_tool_versions = core.get_tool_versions
_arr_service_status = core._arr_service_status
resolve_allowed_media_path = core.resolve_allowed_media_path
is_path_within_root = core.is_path_within_root
get_allowed_media_roots = core.get_allowed_media_roots
scan_file_worker = core.scan_file_worker
save_batch_to_db = core.save_batch_to_db
analyze_file_deep = core.analyze_file_deep
compute_validation_flag = core.compute_validation_flag
build_backfill_metadata = core.build_backfill_metadata
build_duplicate_group_key = core.build_duplicate_group_key
build_duplicate_exact_key = core.build_duplicate_exact_key
recompute_duplicate_counts = core.recompute_duplicate_counts
recompute_duplicate_group_keys_for_paths = core.recompute_duplicate_group_keys_for_paths
_queue_radarr_search = core._queue_radarr_search
_queue_sonarr_search = core._queue_sonarr_search
find_kodi_nfo_candidates = core.find_kodi_nfo_candidates
begin_scan_file = core.begin_scan_file
end_scan_file = core.end_scan_file

# --- ROUTES ---

_RESCAN_FILES_MAX_BATCH = 50

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

@bp.route('/api/scan_jobs')
def get_scan_jobs() -> Response:
    """Return recent durable scan jobs, including interrupted jobs after restart."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT job_id, status, started_at, finished_at, options, progress
                   FROM scan_jobs ORDER BY started_at DESC LIMIT 25"""
            ).fetchall()
            jobs = []
            for row in rows:
                try:
                    options = json.loads(row[4] or "{}")
                except (TypeError, ValueError):
                    options = {}
                try:
                    progress = json.loads(row[5] or "{}")
                except (TypeError, ValueError):
                    progress = {}
                pending = conn.execute(
                    """SELECT COUNT(*) FROM scan_job_files
                       WHERE job_id=? AND status IN ('pending', 'queued')""",
                    (row[0],),
                ).fetchone()[0]
                jobs.append({
                    "job_id": row[0], "status": row[1], "started_at": row[2],
                    "finished_at": row[3], "options": options, "progress": progress,
                    "pending_count": int(pending or 0),
                })
        return jsonify({"status": "ok", "jobs": jobs})
    except sqlite3.Error as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@bp.route('/api/storage_trends')
def get_storage_trends() -> Response:
    """Return retained storage and duplicate-savings snapshots."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT captured_at, total_bytes, duplicate_savings_bytes
                   FROM storage_snapshots ORDER BY id ASC"""
            ).fetchall()
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
        current = {
            "captured_at": "Current",
            "total_bytes": int(total_bytes),
            "duplicate_savings_bytes": int(duplicate_savings),
        }
        snapshots = [
            {"captured_at": row[0], "total_bytes": row[1],
             "duplicate_savings_bytes": row[2]}
            for row in rows
        ]
        if not snapshots or snapshots[-1] != current:
            snapshots.append(current)
        return jsonify({"status": "ok", "snapshots": snapshots})
    except sqlite3.Error as e:
        return jsonify({"status": "error", "message": str(e)}), 500

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
                if va_state.DEBUG_MODE:
                    log_debug(f"Cannot list directory {path}: {e}", "DEBUG")
        result.append({"name": v, "status": status, "path": path})
    return jsonify(result)

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
    scan_scope = (request.json.get('scan_scope') or 'all').lower()
    if scan_scope not in ('all', 'changed'):
        scan_scope = 'all'
    scan_folder = request.json.get('scan_folder')
    resume_job_id = request.json.get('resume_job_id') or None
    # Claim scanning in this request so /progress cannot still look idle
    # between Thread.start() and the worker's first lock.
    with progress_lock:
        if PROGRESS.get("status") == "scanning":
            file_msg = PROGRESS.get("file") or "in progress"
            return jsonify({
                "status": "busy",
                "message": f"A scan or heavy job is already running: {file_msg}",
            }), 400
        PROGRESS.update({
            "status": "scanning", "current": 0, "total": 0, "file": "Initializing...",
            "scan_completed": False, "new_found": 0, "removed": 0, "failed_count": 0,
            "warning_count": 0, "last_duration": "0s", "start_time": time.time(),
            "active_count": 0, "paused": False,
        })
        ACTIVE_SCAN_FILES.clear()
    va_state.ABORT_SCAN = False
    PAUSE_EVENT.set()
    thread_kwargs = {"preclaimed": True}
    if resume_job_id:
        thread_kwargs["resume_job_id"] = resume_job_id
    threading.Thread(
        target=core.run_scan,
        args=(threads, targets, force, debug, scan_mode, scan_folder, scan_scope),
        kwargs=thread_kwargs,
        daemon=True,
    ).start()
    return jsonify({"status": "started"})

@bp.route('/abort', methods=['POST'])
def abort() -> Response:
    """
    Abort the currently running scan.
    
    Immediately sets va_state.ABORT_SCAN flag and kills all active subprocesses.
    """
    # abort flag on va_state
    # Only log and process abort if a scan is actually running
    with progress_lock:
        is_scanning = PROGRESS.get("status") == "scanning"
    
    if not is_scanning:
        # If no scan is running, just return success without logging or setting va_state.ABORT_SCAN
        return jsonify({"status": "idle", "killed_processes": 0, "message": "No scan in progress"})
    
    log_debug("[ABORT] Abort requested by user", "INFO")
    va_state.ABORT_SCAN = True
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
