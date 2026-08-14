"""Versioned SQLite migrations. Callables receive a sqlite3 connection."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Callable

QUALITY_ANOMALY_TOKENS = (
    "low_bitrate_4k",
    "legacy_codec_4k",
    "low_bitrate_1080p",
    "unusual_frame_rate",
)

BASELINE_VIDEO_COLUMNS = {
    "filename": "TEXT",
    "category": "TEXT",
    "profile": "TEXT",
    "el_type": "TEXT",
    "container": "TEXT",
    "source_vol": "TEXT",
    "last_scanned": "TEXT",
    "resolution": "TEXT",
    "bitrate_mbps": "REAL",
    "scan_error": "TEXT",
    "is_hybrid": "INTEGER DEFAULT 0",
    "secondary_hdr": "TEXT",
    "width": "INTEGER",
    "height": "INTEGER",
    "file_size": "INTEGER",
    "bl_compatibility_id": "TEXT",
    "audio_codecs": "TEXT",
    "audio_langs": "TEXT",
    "audio_channels": "TEXT",
    "subtitles": "TEXT",
    "max_cll": "TEXT",
    "max_fall": "TEXT",
    "scan_attempts": "INTEGER DEFAULT 0",
    "fps": "REAL",
    "aspect_ratio": "TEXT",
    "imdb_id": "TEXT",
    "tvdb_id": "TEXT",
    "tmdb_id": "TEXT",
    "rotten_id": "TEXT",
    "metacritic_id": "TEXT",
    "trakt_id": "TEXT",
    "imdb_rating": "REAL",
    "tvdb_rating": "REAL",
    "tmdb_rating": "REAL",
    "rotten_rating": "REAL",
    "metacritic_rating": "REAL",
    "trakt_rating": "REAL",
    "video_source": "TEXT",
    "source_format": "TEXT",
    "video_codec": "TEXT",
    "is_3d": "INTEGER DEFAULT 0",
    "edition": "TEXT",
    "year": "INTEGER",
    "is_source_hybrid": "INTEGER DEFAULT 0",
    "media_type": "TEXT",
    "show_title": "TEXT",
    "season": "INTEGER",
    "episode": "INTEGER",
    "movie_title": "TEXT",
    "episode_title": "TEXT",
    "nfo_missing": "INTEGER DEFAULT 0",
    "missing": "INTEGER DEFAULT 0",
    "validation_flag": "TEXT",
    "dup_group_key": "TEXT",
    "dup_exact_key": "TEXT",
    "dup_count": "INTEGER DEFAULT 0",
    "tvdb_series_id": "TEXT",
    "tvdb_episode_id": "TEXT",
    "imdb_series_id": "TEXT",
    "imdb_episode_id": "TEXT",
    "tmdb_series_id": "TEXT",
    "tmdb_episode_id": "TEXT",
    "trakt_series_id": "TEXT",
    "trakt_episode_id": "TEXT",
    "rotten_series_id": "TEXT",
    "rotten_episode_id": "TEXT",
    "metacritic_series_id": "TEXT",
    "metacritic_episode_id": "TEXT",
}

BASELINE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_category ON videos (category)",
    "CREATE INDEX IF NOT EXISTS idx_vol ON videos (source_vol)",
    "CREATE INDEX IF NOT EXISTS idx_profile ON videos (profile)",
    "CREATE INDEX IF NOT EXISTS idx_container ON videos (container)",
    "CREATE INDEX IF NOT EXISTS idx_resolution ON videos (resolution)",
    "CREATE INDEX IF NOT EXISTS idx_scan_error ON videos (scan_error)",
    "CREATE INDEX IF NOT EXISTS idx_is_hybrid ON videos (is_hybrid)",
    "CREATE INDEX IF NOT EXISTS idx_last_scanned ON videos (last_scanned)",
    "CREATE INDEX IF NOT EXISTS idx_video_source ON videos (video_source)",
    "CREATE INDEX IF NOT EXISTS idx_source_format ON videos (source_format)",
    "CREATE INDEX IF NOT EXISTS idx_video_codec ON videos (video_codec)",
    "CREATE INDEX IF NOT EXISTS idx_is_3d ON videos (is_3d)",
    "CREATE INDEX IF NOT EXISTS idx_year ON videos (year)",
    "CREATE INDEX IF NOT EXISTS idx_media_type ON videos (media_type)",
    "CREATE INDEX IF NOT EXISTS idx_dup_group_key ON videos (dup_group_key)",
    "CREATE INDEX IF NOT EXISTS idx_dup_exact_key ON videos (dup_exact_key)",
    "CREATE INDEX IF NOT EXISTS idx_el_type ON videos (el_type)",
    "CREATE INDEX IF NOT EXISTS idx_secondary_hdr ON videos (secondary_hdr)",
    "CREATE INDEX IF NOT EXISTS idx_edition ON videos (edition)",
    "CREATE INDEX IF NOT EXISTS idx_missing ON videos (missing)",
    "CREATE INDEX IF NOT EXISTS idx_nfo_missing ON videos (nfo_missing)",
    "CREATE INDEX IF NOT EXISTS idx_is_source_hybrid ON videos (is_source_hybrid)",
    "CREATE INDEX IF NOT EXISTS idx_file_size ON videos (file_size)",
    "CREATE INDEX IF NOT EXISTS idx_bitrate_mbps ON videos (bitrate_mbps)",
    "CREATE INDEX IF NOT EXISTS idx_dup_count ON videos (dup_count)",
    "CREATE INDEX IF NOT EXISTS idx_category_lower ON videos (LOWER(category))",
    "CREATE INDEX IF NOT EXISTS idx_media_type_lower ON videos (LOWER(media_type))",
    "CREATE INDEX IF NOT EXISTS idx_vol_lower ON videos (LOWER(source_vol))",
    "CREATE INDEX IF NOT EXISTS idx_resolution_lower ON videos (LOWER(resolution))",
    "CREATE INDEX IF NOT EXISTS idx_secondary_hdr_lower ON videos (LOWER(secondary_hdr))",
    "CREATE INDEX IF NOT EXISTS idx_profile_lower ON videos (LOWER(profile))",
    "CREATE INDEX IF NOT EXISTS idx_el_type_lower ON videos (LOWER(el_type))",
    "CREATE INDEX IF NOT EXISTS idx_container_lower ON videos (LOWER(container))",
    "CREATE INDEX IF NOT EXISTS idx_edition_lower ON videos (LOWER(edition))",
    "CREATE INDEX IF NOT EXISTS idx_video_source_lower ON videos (LOWER(video_source))",
    "CREATE INDEX IF NOT EXISTS idx_source_format_lower ON videos (LOWER(source_format))",
    "CREATE INDEX IF NOT EXISTS idx_video_codec_lower ON videos (LOWER(video_codec))",
)

DEFAULT_SETTINGS = {
    "threads": "4",
    "skip_words": "trailer,sample",
    "min_size_mb": "50",
    "refresh_interval": "60",
    "notif_style": "modal",
    "force_rescan": "false",
    "column_order": "",
    "scan_folders": "[]",
    "scan_extras": "false",
    "debug_mode": "false",
    "remove_missing_from_db": "true",
    "duplicate_check_on_scan": "false",
}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, type_def: str) -> None:
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if col not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {type_def}")


def migration_001_baseline(conn: sqlite3.Connection) -> None:
    """Create current 3.0 schema (IF NOT EXISTS) so existing databases stamp cleanly."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS schema_migrations
           (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"""
    )
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS scan_history
           (id INTEGER PRIMARY KEY AUTOINCREMENT, entry TEXT, created_at TEXT)"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS storage_snapshots
           (id INTEGER PRIMARY KEY AUTOINCREMENT, captured_at TEXT NOT NULL,
            total_bytes INTEGER NOT NULL DEFAULT 0,
            duplicate_savings_bytes INTEGER NOT NULL DEFAULT 0)"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS scan_jobs
           (job_id TEXT PRIMARY KEY, status TEXT NOT NULL, started_at TEXT,
            finished_at TEXT, options TEXT, progress TEXT)"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS videos
           (filename TEXT, category TEXT, profile TEXT, el_type TEXT,
            container TEXT, source_vol TEXT, full_path TEXT PRIMARY KEY,
            last_scanned TEXT, resolution TEXT, bitrate_mbps REAL, scan_error TEXT,
            is_hybrid INTEGER DEFAULT 0, secondary_hdr TEXT,
            width INTEGER, height INTEGER, file_size INTEGER, bl_compatibility_id TEXT,
            audio_codecs TEXT, audio_langs TEXT, audio_channels TEXT, subtitles TEXT,
            max_cll TEXT, max_fall TEXT, fps REAL, aspect_ratio TEXT,
            imdb_id TEXT, tvdb_id TEXT, tmdb_id TEXT, rotten_id TEXT, metacritic_id TEXT, trakt_id TEXT,
            imdb_rating REAL, tvdb_rating REAL, tmdb_rating REAL, rotten_rating REAL,
            metacritic_rating REAL, trakt_rating REAL,
            scan_attempts INTEGER DEFAULT 0,
            video_source TEXT, source_format TEXT, video_codec TEXT,
            is_3d INTEGER DEFAULT 0, edition TEXT, year INTEGER,
            media_type TEXT, show_title TEXT, season INTEGER, episode INTEGER,
            movie_title TEXT, episode_title TEXT,
            nfo_missing INTEGER DEFAULT 0, missing INTEGER DEFAULT 0, validation_flag TEXT,
            dup_group_key TEXT, dup_exact_key TEXT, dup_count INTEGER DEFAULT 0)"""
    )
    for col, type_def in BASELINE_VIDEO_COLUMNS.items():
        _add_column_if_missing(conn, "videos", col, type_def)
    for sql in BASELINE_INDEXES:
        conn.execute(sql)
    for key, value in DEFAULT_SETTINGS.items():
        conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))


def migration_002_scan_and_quality(conn: sqlite3.Connection) -> None:
    """quality_anomaly, file_mtime, scan_job_files, scan_seen_files."""
    _add_column_if_missing(conn, "videos", "quality_anomaly", "TEXT")
    _add_column_if_missing(conn, "videos", "file_mtime", "REAL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_quality_anomaly ON videos (quality_anomaly)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_mtime ON videos (file_mtime)")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS scan_job_files
           (job_id TEXT NOT NULL, full_path TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            PRIMARY KEY (job_id, full_path))"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_job_files_status ON scan_job_files (job_id, status)")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS scan_seen_files
           (full_path TEXT PRIMARY KEY)"""
    )
    rows = conn.execute(
        "SELECT full_path, validation_flag FROM videos WHERE validation_flag IS NOT NULL AND validation_flag != ''"
    ).fetchall()
    for full_path, flag in rows:
        tokens = [t.strip() for t in str(flag).split(",") if t.strip()]
        quality = [t for t in tokens if t in QUALITY_ANOMALY_TOKENS]
        meta = [t for t in tokens if t not in QUALITY_ANOMALY_TOKENS]
        conn.execute(
            "UPDATE videos SET quality_anomaly=?, validation_flag=? WHERE full_path=?",
            (",".join(quality) or None, ",".join(meta) or None, full_path),
        )


MIGRATIONS: list[tuple[int, Callable[[sqlite3.Connection], None]]] = [
    (1, migration_001_baseline),
    (2, migration_002_scan_and_quality),
]


def current_schema_version(conn: sqlite3.Connection) -> int:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS schema_migrations
           (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"""
    )
    row = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()
    return int(row[0] or 0)


def apply_migrations(conn: sqlite3.Connection) -> int:
    """Apply pending migrations. Returns the resulting schema version."""
    applied = current_schema_version(conn)
    # Existing DBs created before this runner already have the 3.0 schema and
    # may already have a version=1 stamp from the old init_db path.
    has_videos = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='videos'"
    ).fetchone()
    if applied == 0 and has_videos:
        migration_001_baseline(conn)
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (1, _now()),
        )
        applied = 1
    for version, fn in MIGRATIONS:
        if version <= applied:
            continue
        fn(conn)
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (version, _now()),
        )
        applied = version
    return applied
