"""Duplicate-group and delete HTTP handlers."""
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
