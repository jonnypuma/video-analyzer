"""Tests for versioned SQLite schema migrations."""
from __future__ import annotations

import sqlite3

from video_analyzer.db.migrations import apply_migrations, current_schema_version, MIGRATIONS


def test_empty_database_reaches_latest_schema(analyzer_mod, tmp_path, monkeypatch):
    db_path = tmp_path / "fresh.db"
    monkeypatch.setattr(analyzer_mod, "DB_PATH", str(db_path))
    import video_analyzer.core as core
    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    analyzer_mod.init_db()
    with analyzer_mod.get_db() as conn:
        version = current_schema_version(conn)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(videos)").fetchall()}
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert version == max(v for v, _ in MIGRATIONS)
    assert "quality_anomaly" in cols
    assert "file_mtime" in cols
    assert "scan_job_files" in tables
    assert "scan_seen_files" in tables


def test_existing_videos_table_stamps_baseline_then_upgrades(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE videos (full_path TEXT PRIMARY KEY, filename TEXT, validation_flag TEXT)")
    conn.execute(
        "INSERT INTO videos (full_path, filename, validation_flag) VALUES (?, ?, ?)",
        ("/a.mkv", "a.mkv", "movie_with_show_fields,low_bitrate_4k"),
    )
    conn.commit()
    version = apply_migrations(conn)
    conn.commit()
    assert version == max(v for v, _ in MIGRATIONS)
    stamped = [row[0] for row in conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()]
    assert stamped[0] == 1
    assert max(stamped) == version
    cols = {row[1] for row in conn.execute("PRAGMA table_info(videos)").fetchall()}
    assert "quality_anomaly" in cols
    row = conn.execute("SELECT validation_flag, quality_anomaly FROM videos WHERE full_path=?", ("/a.mkv",)).fetchone()
    assert row[0] == "movie_with_show_fields"
    assert row[1] == "low_bitrate_4k"
    conn.close()
