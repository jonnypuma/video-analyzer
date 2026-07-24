"""
Pytest bootstrap: set env BEFORE analyzer / video_analyzer import so OUTPUT_DIR / init are test-safe.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# Must run before any `import analyzer` / `video_analyzer`
_OUT = Path(tempfile.mkdtemp(prefix="va-test-output-"))
_OUT.mkdir(parents=True, exist_ok=True)
os.environ["VIDEO_ANALYZER_TESTING"] = "1"
os.environ["VIDEO_ANALYZER_OUTPUT"] = str(_OUT)

APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import pytest  # noqa: E402


@pytest.fixture
def media_root(tmp_path, monkeypatch):
    """Create a fake media mount and point SCAN_PATHS at it."""
    root = tmp_path / "movies"
    root.mkdir()
    monkeypatch.setenv("SCAN_PATHS", str(root))
    return root


@pytest.fixture
def analyzer_mod():
    """Import thin WSGI module (`analyzer:app`) after env is configured."""
    import analyzer
    return analyzer
