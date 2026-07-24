"""Public facade for worker (implementation in core)."""
from __future__ import annotations
import video_analyzer.core as _core

scan_file_worker = _core.scan_file_worker
save_batch_to_db = _core.save_batch_to_db
