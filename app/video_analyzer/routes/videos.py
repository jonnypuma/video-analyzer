"""Video table, export, metadata, and anomaly HTTP handlers."""
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
        w_anom, p_anom = build_filter_query(args, exclude_key='anomaly')
        anomaly_yes = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_anom} AND quality_anomaly IS NOT NULL AND quality_anomaly != ''",
            p_anom,
        ).fetchone()[0]
        anomaly_no = conn.execute(
            f"SELECT COUNT(*) FROM videos WHERE {w_anom} AND (quality_anomaly IS NULL OR quality_anomaly = '')",
            p_anom,
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
            'special_anomaly': {'1': anomaly_yes, '0': anomaly_no},
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
        # api ts on va_state
        now = time.time()
        if PROGRESS.get("status") == "scanning" and now - va_state.API_LOG_TS >= 5:
            log_debug(
                f"[API_VIDEOS] total={total} rows={len(rows)} page={page} per_page={per_page}",
                "INFO",
            )
            va_state.API_LOG_TS = now
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

@bp.route('/api/anomalies')
def get_quality_anomalies() -> Response:
    """Return analyzed titles with codec/quality anomaly flags."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT full_path, filename, resolution, bitrate_mbps, video_codec,
                          quality_anomaly
                   FROM videos
                   WHERE quality_anomaly IS NOT NULL AND quality_anomaly != ''
                   ORDER BY filename COLLATE NOCASE"""
            ).fetchall()
        return jsonify({"status": "ok", "anomalies": [
            {"full_path": r[0], "filename": r[1], "resolution": r[2],
             "bitrate_mbps": r[3], "video_codec": r[4], "flags": r[5]}
            for r in rows
        ]})
    except sqlite3.Error as e:
        return jsonify({"status": "error", "message": str(e)}), 500

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

def update_validation_flag_for_path(conn: sqlite3.Connection, full_path: str) -> None:
    row = conn.execute(
        "SELECT media_type, show_title, episode_title, movie_title, season, episode FROM videos WHERE full_path=?",
        (full_path,)
    ).fetchone()
    if not row:
        return
    validation_flag = compute_validation_flag(dict(row))
    conn.execute("UPDATE videos SET validation_flag=? WHERE full_path=?", (validation_flag, full_path))
