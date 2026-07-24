"""Public facade for paths (implementation in core)."""
from __future__ import annotations
import video_analyzer.core as _core

get_mount_status = _core.get_mount_status
is_path_within_root = _core.is_path_within_root
get_allowed_media_roots = _core.get_allowed_media_roots
resolve_allowed_media_path = _core.resolve_allowed_media_path
