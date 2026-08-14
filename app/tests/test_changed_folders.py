"""Tests for changed-folder / skip-unchanged hybrid size+mtime scanning."""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta


def test_skip_unchanged_uses_size_and_mtime(analyzer_mod, tmp_path):
    analyzer_mod.init_db()
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    replaced_dir = tmp_path / "replaced"
    old_dir.mkdir()
    new_dir.mkdir()
    replaced_dir.mkdir()
    old_file = old_dir / "old.mkv"
    new_file = new_dir / "new.mkv"
    replaced_file = replaced_dir / "same.mkv"
    payload = b"video"
    old_file.write_bytes(payload)
    new_file.write_bytes(payload)
    replaced_file.write_bytes(payload)

    now = time.time()
    old_mtime = now - 200
    os.utime(old_file, (old_mtime, old_mtime))
    os.utime(replaced_file, (now + 10, now + 10))

    last_scanned = datetime.fromtimestamp(now - 50).strftime("%Y-%m-%d %H:%M:%S")
    processed = {
        str(old_file): {
            "size": len(payload), "attempts": 0, "error": None,
            "last_scanned": last_scanned, "file_mtime": old_mtime,
        },
        str(replaced_file): {
            "size": len(payload), "attempts": 0, "error": None,
            "last_scanned": last_scanned, "file_mtime": old_mtime,
        },
    }

    files, found = analyzer_mod.collect_files_to_scan(
        [str(tmp_path)], {str(tmp_path): "test"}, processed, [], 0, False,
        now, True, True, now - 50
    )
    names = sorted(path.name for path in files)
    assert names == ["new.mkv", "same.mkv"]
    assert str(old_file) in found
    assert str(new_file) in found
    assert str(replaced_file) in found


def test_newer_mtime_same_size_is_analyzed(analyzer_mod, tmp_path):
    analyzer_mod.init_db()
    folder = tmp_path / "lib"
    folder.mkdir()
    video = folder / "clip.mkv"
    video.write_bytes(b"abcd")
    now = time.time()
    os.utime(video, (now + 20, now + 20))
    last_scanned = datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S")
    processed = {
        str(video): {
            "size": 4, "attempts": 0, "error": None,
            "last_scanned": last_scanned, "file_mtime": now - 100,
        }
    }
    files, found = analyzer_mod.collect_files_to_scan(
        [str(tmp_path)], {str(tmp_path): "test"}, processed, [], 0, False,
        now, True, True, now
    )
    assert [path.name for path in files] == ["clip.mkv"]
    assert str(video) in found
