"""Library ribbon/chart stats aggregations."""
from __future__ import annotations

import copy
import json
from typing import Any, Dict, List, Optional, Tuple

from video_analyzer.state import LIBRARY_STATS_CACHE, PROGRESS, library_stats_cache_lock

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
          COALESCE(SUM(CASE WHEN {ok} AND category = 'sdr_only' THEN 1 ELSE 0 END), 0) AS sdr,
          COALESCE(SUM(CASE WHEN quality_anomaly IS NOT NULL AND quality_anomaly != '' THEN 1 ELSE 0 END), 0) AS anomalies
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
        "anomalies": int(row["anomalies"] or 0),
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
