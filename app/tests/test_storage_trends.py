"""Tests for storage trend and duplicate-savings snapshots."""
from __future__ import annotations


def test_storage_trends_endpoint(analyzer_mod):
    analyzer_mod.init_db()
    with analyzer_mod.get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO videos (full_path, filename, file_size, dup_group_key, missing) VALUES (?, ?, ?, ?, 0)",
            ("/a.mkv", "a.mkv", 1000, "movie:title:example")
        )
        conn.execute(
            "INSERT OR REPLACE INTO videos (full_path, filename, file_size, dup_group_key, missing) VALUES (?, ?, ?, ?, 0)",
            ("/b.mkv", "b.mkv", 700, "movie:title:example")
        )
        conn.execute(
            """INSERT INTO storage_snapshots
               (captured_at, total_bytes, duplicate_savings_bytes)
               VALUES ('2026-08-02 00:00:00', 1000, 250)"""
        )
    response = analyzer_mod.app.test_client().get("/api/storage_trends")
    assert response.status_code == 200
    assert any(
        snapshot["duplicate_savings_bytes"] == 250
        for snapshot in response.json["snapshots"]
    )
    assert response.json["snapshots"][-1]["duplicate_savings_bytes"] == 700
    assert response.json["snapshots"][-1]["captured_at"] == "Current"
