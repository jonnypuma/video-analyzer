"""Application paths, constants, and version helpers."""
from __future__ import annotations

import os
import re

OUTPUT_DIR = (os.environ.get("VIDEO_ANALYZER_OUTPUT") or "").strip() or "/output"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # app/
LOCAL_OUTPUT_FALLBACK = os.path.join(BASE_DIR, "results")
if not os.path.exists(OUTPUT_DIR) and os.path.exists(LOCAL_OUTPUT_FALLBACK):
    OUTPUT_DIR = LOCAL_OUTPUT_FALLBACK
DB_PATH = os.path.join(OUTPUT_DIR, "processed_videos.db")
CHANGELOG_PATH = os.path.join(BASE_DIR, "CHANGELOG.md")
if not os.path.exists(CHANGELOG_PATH):
    _changelog_alt = os.path.join(os.path.dirname(BASE_DIR), "CHANGELOG.md")
    if os.path.exists(_changelog_alt):
        CHANGELOG_PATH = _changelog_alt
VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".mpeg", ".mpg", ".mov", ".ts", ".m2ts", ".webm", ".wmv",
    ".obu", ".ivf", ".av1",
    ".hevc", ".h265", ".265",
    ".h264", ".264", ".avc",
    ".vvc", ".h266", ".266",
}
SYSTEM_DIRS = {
    "bin", "boot", "dev", "etc", "home", "lib", "lib64", "media", "mnt", "opt",
    "proc", "root", "run", "sbin", "srv", "sys", "tmp", "usr", "var", "app",
    "defaults", "config", "output",
}

DB_TIMEOUT = 120
PROCESSED_MAP_CHUNK_SIZE = 10000
MAX_RETRIES = 2
RETRY_DELAY_INITIAL = 1
RPU_CACHE_MAX_SIZE = 50000
LOG_CLEANUP_LIMIT = 5
MAX_SCAN_ATTEMPTS = 3
PROGRESS_UPDATE_INTERVAL = 10
SUBPROCESS_TIMEOUT = 30
MEDIAINFO_TIMEOUT = 120

APP_VERSION_FALLBACK = os.environ.get("APP_VERSION", "dev")
RADARR_URL = (os.environ.get("RADARR_URL") or "").strip().rstrip("/")
RADARR_API_KEY = (os.environ.get("RADARR_API_KEY") or "").strip()
SONARR_URL = (os.environ.get("SONARR_URL") or "").strip().rstrip("/")
SONARR_API_KEY = (os.environ.get("SONARR_API_KEY") or "").strip()


def app_version() -> str:
    """Return the latest semantic version listed in CHANGELOG.md."""
    try:
        with open(CHANGELOG_PATH, "r", encoding="utf-8") as handle:
            for line in handle:
                match = re.match(r"^##\s+v?(\d+\.\d+\.\d+)\s*$", line.strip(), re.IGNORECASE)
                if match:
                    return match.group(1)
    except OSError:
        pass
    return (APP_VERSION_FALLBACK or "dev").strip()


def app_version_label() -> str:
    version = app_version()
    return version if version.lower().startswith("v") or version == "dev" else f"v{version}"
