"""
WSGI entrypoint for Gunicorn (`analyzer:app`) and test imports.

Implementation lives in the `video_analyzer` package (3.0+).
"""
from __future__ import annotations

from video_analyzer import create_app
import video_analyzer.core as _core

app = create_app()

# Re-export symbols used by tests and scripts (compat with pre-3.0 monolith imports)
_parse_schedule_time = _core._parse_schedule_time
is_path_within_root = _core.is_path_within_root
resolve_allowed_media_path = _core.resolve_allowed_media_path
_zip_member_path_is_safe = _core._zip_member_path_is_safe
_validate_restore_zip_members = _core._validate_restore_zip_members
build_filter_query = _core.build_filter_query
parse_sort_order = _core.parse_sort_order
app_version = _core.app_version
app_version_label = _core.app_version_label
init_db = _core.init_db
get_db = _core.get_db
OUTPUT_DIR = _core.OUTPUT_DIR
DB_PATH = _core.DB_PATH
BASE_DIR = _core.BASE_DIR
PROGRESS = _core.PROGRESS


def __getattr__(name: str):
    if hasattr(_core, name):
        return getattr(_core, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6002)
