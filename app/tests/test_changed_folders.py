"""Tests for changed-folder scan scope."""
from __future__ import annotations

import os
import time


def test_changed_folder_scope_prunes_old_directories(analyzer_mod, tmp_path):
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    old_dir.mkdir()
    new_dir.mkdir()
    (old_dir / "old.mkv").write_bytes(b"video")
    (new_dir / "new.mkv").write_bytes(b"video")
    cutoff = time.time()
    old_time = cutoff - 100
    os.utime(old_dir, (old_time, old_time))
    os.utime(old_dir / "old.mkv", (old_time, old_time))
    os.utime(new_dir, (cutoff + 10, cutoff + 10))

    files, found = analyzer_mod.collect_files_to_scan(
        [str(tmp_path)], {str(tmp_path): "test"}, {}, [], 0, False,
        cutoff, True, True, cutoff
    )
    assert [path.name for path in files] == ["new.mkv"]
    assert str(new_dir / "new.mkv") in found
    assert str(old_dir / "old.mkv") not in found
