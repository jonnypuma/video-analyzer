"""HTTP health, logs, and error handlers."""
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from flask import jsonify, make_response, render_template, request, send_file, Response, session

from video_analyzer.blueprint import bp
from video_analyzer import core
from video_analyzer import state as va_state
from video_analyzer.config import (
    DB_PATH, OUTPUT_DIR, RADARR_API_KEY, RADARR_URL, SONARR_API_KEY, SONARR_URL,
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
    ACTIVE_PROCS, library_stats_cache_lock, proc_lock, progress_lock,
)

log_debug = core.log_debug
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

@bp.route('/')
def index():
    configured = (os.environ.get("BASIC_AUTH") or "").strip()
    auth_on = bool(configured and ":" in configured and configured.split(":", 1)[0])
    return render_template(
        'index.html',
        app_version_label=app_version_label(),
        auth_enabled=auth_on,
        csrf_token=session.get("csrf_token", ""),
    )

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

@bp.route('/download_log')
def download_log() -> Union[Response, Tuple[str, int]]:
    """
    Download the current scan activity log file.
    
    Returns:
        File download response if log file exists, or 404 error message if not found
    """
    if va_state.LOG_FILE and os.path.exists(va_state.LOG_FILE):
        return send_file(va_state.LOG_FILE, as_attachment=True, download_name=os.path.basename(va_state.LOG_FILE))
    return "No log found", 404

@bp.route('/download_failures')
def download_failures() -> Union[Response, Tuple[str, int]]:
    """Download the current scan failures CSV file."""
    if va_state.FAIL_FILE and os.path.exists(va_state.FAIL_FILE):
        return send_file(va_state.FAIL_FILE, as_attachment=True, download_name=os.path.basename(va_state.FAIL_FILE))
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
    if va_state.FAIL_FILE and os.path.exists(va_state.FAIL_FILE):
        try:
            with open(va_state.FAIL_FILE, 'r', encoding='utf-8', newline='') as f:
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
