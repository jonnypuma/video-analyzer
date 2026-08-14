"""Mutable process-wide application state."""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any, Dict

APP_START_TIME = time.time()
ARR_STATUS_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": None}
TOOL_VERSION_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": None}
LIBRARY_STATS_CACHE: Dict[str, Any] = {"bundle": None}
library_stats_cache_lock = threading.Lock()
PROGRESS = {
    "status": "idle", "current": 0, "total": 0, "file": "Waiting...",
    "last_full_scan": "Never", "last_duration": "--",
    "scan_completed": False, "new_found": 0, "failed_count": 0, "last_duration": "0s",
    "eta": "", "start_time": 0, "paused": False, "warning_count": 0, "job_id": None,
}
ABORT_SCAN = False
ACTIVE_SCAN_JOB_ID: str | None = None
PAUSE_EVENT = threading.Event()
PAUSE_EVENT.set()
LOG_CACHE: list = []
DIAG_LOG_TS = 0.0
API_LOG_TS = 0.0
progress_lock = threading.Lock()
ACTIVE_SCAN_FILES: OrderedDict[str, str] = OrderedDict()
db_access_lock = threading.Lock()
LOG_FILE = ""
FAIL_FILE = ""
DEBUG_MODE = False
ACTIVE_PROCS: set = set()
proc_lock = threading.Lock()
RPU_CACHE: OrderedDict = OrderedDict()
rpu_cache_lock = threading.Lock()
scheduler = None
