"""Public facade for logging_util (implementation in core)."""
from __future__ import annotations
import video_analyzer.core as _core

log_debug = _core.log_debug
log_failure = _core.log_failure
setup_new_log_files = _core.setup_new_log_files
cleanup_old_logs = _core.cleanup_old_logs
