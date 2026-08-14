"""Database initialization and lightweight column helpers."""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime

from video_analyzer.config import OUTPUT_DIR
from video_analyzer.db.connection import get_db
from video_analyzer.db.migrations import apply_migrations
from video_analyzer.state import PROGRESS, progress_lock


def ensure_video_column(col: str, type_def: str) -> None:
    """Ensure a column exists on videos table (safe for hot paths)."""
    from video_analyzer.core import log_debug

    try:
        with get_db() as conn:
            existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(videos)").fetchall()}
            if col not in existing_cols:
                log_debug(f"Migrating DB: Adding missing column '{col}'...", "WARNING")
                conn.execute(f"ALTER TABLE videos ADD COLUMN {col} {type_def}")
    except sqlite3.Error as e:
        log_debug(f"Migration Error: {e}", "ERROR")


def init_db() -> None:
    """Connect, enable WAL via get_db, apply migrations, restore last-scan ribbon."""
    from video_analyzer.core import get_mount_status, log_debug, recompute_duplicate_counts

    log_debug("Initializing Database...")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    with get_db() as conn:
        apply_migrations(conn)
        try:
            recompute_duplicate_counts(conn)
        except sqlite3.Error as e:
            log_debug(f"Could not recompute duplicate counts: {e}", "WARNING")

        interrupted = conn.execute(
            "SELECT job_id FROM scan_jobs WHERE status='running'"
        ).fetchall()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for row in interrupted:
            conn.execute(
                "UPDATE scan_jobs SET status='interrupted', finished_at=? WHERE job_id=?",
                (now, row[0]),
            )
            conn.execute(
                "UPDATE scan_job_files SET status='pending' WHERE job_id=? AND status='queued'",
                (row[0],),
            )
        if interrupted:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('last_interrupted_scan', ?)",
                (now,),
            )

        try:
            persisted = dict(conn.execute(
                "SELECT key, value FROM settings WHERE key IN ('last_full_scan', 'last_duration')"
            ).fetchall())
            with progress_lock:
                if persisted.get("last_full_scan"):
                    PROGRESS["last_full_scan"] = persisted["last_full_scan"]
                if persisted.get("last_duration"):
                    PROGRESS["last_duration"] = persisted["last_duration"]
        except Exception as e:
            log_debug(f"Could not restore last scan settings: {e}", "WARNING")

    log_debug("Database ready.")

    mounts = get_mount_status()
    if mounts:
        log_debug("--- VOLUME STATUS CHECK ---")
        for vol, path in mounts.items():
            log_debug(f"Volume {vol}: ONLINE ✅")
    else:
        log_debug("⚠️ No media volumes detected in root.")
