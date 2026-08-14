"""Settings, presets, backup/restore, ARR, and DB-maintenance HTTP handlers."""
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
from video_analyzer.db.connection import get_db, get_db_readonly, invalidate_library_stats_cache
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
    APP_START_TIME, ARR_STATUS_CACHE, LIBRARY_STATS_CACHE, LOG_CACHE, PAUSE_EVENT, PROGRESS,
    ACTIVE_PROCS, library_stats_cache_lock, proc_lock, progress_lock,
)

log_debug = core.log_debug
apply_scan_schedule = core.apply_scan_schedule
_as_int = core._as_int
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

_RESTORE_ALLOWED_BASENAMES = frozenset({'processed_videos.db', 'settings.json'})

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

@bp.route('/api/scan_profiles', methods=['GET', 'POST', 'DELETE'])
def handle_scan_profiles() -> Response:
    """Manage named scan setting presets stored in the application database."""
    try:
        with get_db() as conn:
            raw = conn.execute(
                "SELECT value FROM settings WHERE key='scan_profiles'"
            ).fetchone()
            try:
                profiles = json.loads(raw[0]) if raw and raw[0] else []
            except (TypeError, ValueError):
                profiles = []
            if not isinstance(profiles, list):
                profiles = []

            if request.method == "GET":
                return jsonify({"status": "ok", "profiles": profiles})

            payload = request.get_json(silent=True) or {}
            name = str(payload.get("name") or "").strip()
            if not name or len(name) > 64:
                return jsonify({"status": "error", "message": "A profile name is required"}), 400
            if request.method == "DELETE":
                profiles = [p for p in profiles if p.get("name") != name]
            else:
                values = payload.get("settings") or {}
                if not isinstance(values, dict):
                    return jsonify({"status": "error", "message": "Invalid profile settings"}), 400
                profile = {"name": name, "settings": values}
                profiles = [p for p in profiles if p.get("name") != name]
                profiles.append(profile)
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('scan_profiles', ?)",
                (json.dumps(profiles),)
            )
            return jsonify({"status": "ok", "profiles": profiles})
    except (sqlite3.Error, TypeError, ValueError) as e:
        return jsonify({"status": "error", "message": str(e)}), 500

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
