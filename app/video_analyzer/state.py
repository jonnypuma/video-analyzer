"""Public facade for state (implementation in core)."""
from __future__ import annotations
import video_analyzer.core as _core

PROGRESS = _core.PROGRESS
ABORT_SCAN = _core.ABORT_SCAN
PAUSE_EVENT = _core.PAUSE_EVENT
progress_lock = _core.progress_lock
db_access_lock = _core.db_access_lock
LIBRARY_STATS_CACHE = _core.LIBRARY_STATS_CACHE
library_stats_cache_lock = _core.library_stats_cache_lock
RPU_CACHE = _core.RPU_CACHE
rpu_cache_lock = _core.rpu_cache_lock
scheduler = _core.scheduler
LOG_CACHE = _core.LOG_CACHE
DEBUG_MODE = _core.DEBUG_MODE
ACTIVE_PROCS = _core.ACTIVE_PROCS
proc_lock = _core.proc_lock
