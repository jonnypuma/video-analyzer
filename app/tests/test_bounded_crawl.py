"""Tests for bounded crawl seen-path tracking."""
from __future__ import annotations


def test_seen_path_cleanup_matches_fake_tree(analyzer_mod, tmp_path):
    analyzer_mod.init_db()
    keep = tmp_path / "keep.mkv"
    gone = tmp_path / "gone.mkv"
    keep.write_bytes(b"keep")
    gone.write_bytes(b"gone")
    with analyzer_mod.get_db() as conn:
        conn.execute(
            """INSERT INTO videos (filename, full_path, source_vol, file_size, missing)
               VALUES (?, ?, ?, ?, 0)""",
            ("keep.mkv", str(keep), "test", 4),
        )
        conn.execute(
            """INSERT INTO videos (filename, full_path, source_vol, file_size, missing)
               VALUES (?, ?, ?, ?, 0)""",
            ("gone.mkv", str(gone), "test", 4),
        )
    gone.unlink()

    files, found = analyzer_mod.collect_files_to_scan(
        [str(tmp_path)], {str(tmp_path): "test"}, {}, [], 0, False,
        0, True, False, None, None, False
    )
    assert files == [] or all(path.name == "keep.mkv" for path in files)
    removed = analyzer_mod.count_removed_files(["test"], [str(tmp_path)], found, use_seen_table=True)
    assert removed == 1
    marked = analyzer_mod.cleanup_deleted_files(
        ["test"], [str(tmp_path)], found, remove_from_db=False, use_seen_table=True
    )
    assert marked == 1
    with analyzer_mod.get_db() as conn:
        row = conn.execute("SELECT missing FROM videos WHERE full_path=?", (str(gone),)).fetchone()
        assert row[0] == 1


def test_skip_extras_folder_does_not_crash_crawl(analyzer_mod, tmp_path):
    """scan_extras=False walks a movie extras/ dir and must import `re` for season matching."""
    analyzer_mod.init_db()
    movie = tmp_path / "Amelie (2001)"
    extras = movie / "extras"
    extras.mkdir(parents=True)
    (movie / "Amelie.mkv").write_bytes(b"vid")
    (movie / "Amelie.nfo").write_text("<movie></movie>")
    extra_file = extras / "behind.mkv"
    extra_file.write_bytes(b"extra")
    files, found = analyzer_mod.collect_files_to_scan(
        [str(tmp_path)], {str(tmp_path): "test"}, {}, [], 0, False,
        0, False, False,
    )
    names = [path.name for path in files]
    assert "Amelie.mkv" in names
    assert "behind.mkv" not in names
    assert str(extra_file) not in found
