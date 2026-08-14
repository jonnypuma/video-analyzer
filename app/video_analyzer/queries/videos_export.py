"""Shared SELECT list / sort map for table API + CSV/JSON exports."""
from __future__ import annotations

from typing import Any, Dict

from video_analyzer.queries.filters import build_filter_query, parse_positive_int, parse_sort_order

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
    "dup_exact_key, dup_count, quality_anomaly"
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
    'anomaly': 'quality_anomaly',
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
