"""Public facade for config (implementation in core)."""
from __future__ import annotations
import video_analyzer.core as _core

OUTPUT_DIR = _core.OUTPUT_DIR
BASE_DIR = _core.BASE_DIR
DB_PATH = _core.DB_PATH
CHANGELOG_PATH = _core.CHANGELOG_PATH
VIDEO_EXTENSIONS = _core.VIDEO_EXTENSIONS
SYSTEM_DIRS = _core.SYSTEM_DIRS
DB_TIMEOUT = _core.DB_TIMEOUT
APP_VERSION_FALLBACK = _core.APP_VERSION_FALLBACK
RADARR_URL = _core.RADARR_URL
RADARR_API_KEY = _core.RADARR_API_KEY
SONARR_URL = _core.SONARR_URL
SONARR_API_KEY = _core.SONARR_API_KEY
app_version = _core.app_version
app_version_label = _core.app_version_label
