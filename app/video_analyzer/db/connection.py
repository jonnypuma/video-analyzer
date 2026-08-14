"""SQLite connection helpers."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Any

from video_analyzer.config import DB_PATH, DB_TIMEOUT
from video_analyzer.state import LIBRARY_STATS_CACHE, db_access_lock, library_stats_cache_lock


def invalidate_library_stats_cache() -> None:
    """Drop cached unfiltered library stats so the next meta request rebuilds them."""
    with library_stats_cache_lock:
        LIBRARY_STATS_CACHE["bundle"] = None


@contextmanager
def get_db() -> Any:
    """Context manager for database connections with commit/rollback and WAL."""
    with db_access_lock:
        conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            changed = conn.total_changes > 0
            conn.commit()
            if changed:
                invalidate_library_stats_cache()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


@contextmanager
def get_db_readonly() -> Any:
    """Read-only connection without the global write lock. Safe with WAL."""
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
    finally:
        conn.close()
