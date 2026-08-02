"""Unit tests for in-flight scan-info label tracking."""
from __future__ import annotations


def test_active_scan_label_tracks_inflight(analyzer_mod):
    with analyzer_mod.progress_lock:
        analyzer_mod.ACTIVE_SCAN_FILES.clear()
        analyzer_mod.PROGRESS["file"] = "Initializing..."
        analyzer_mod.PROGRESS["active_count"] = 0

    analyzer_mod.begin_scan_file("/vol/a.mkv", "a.mkv")
    assert analyzer_mod.PROGRESS["file"] == "Analyzing: a.mkv"
    assert analyzer_mod.PROGRESS["active_count"] == 1

    analyzer_mod.begin_scan_file("/vol/b.mkv", "b.mkv")
    assert analyzer_mod.PROGRESS["file"] == "Analyzing (2): b.mkv (+1 more)"
    assert analyzer_mod.PROGRESS["active_count"] == 2

    analyzer_mod.end_scan_file("/vol/a.mkv", "a.mkv")
    assert analyzer_mod.PROGRESS["file"] == "Analyzing: b.mkv"
    assert analyzer_mod.PROGRESS["active_count"] == 1

    analyzer_mod.end_scan_file("/vol/b.mkv", "b.mkv")
    assert analyzer_mod.PROGRESS["file"] == "Done: b.mkv"
    assert analyzer_mod.PROGRESS["active_count"] == 0


def test_format_active_scan_label_empty_keeps_progress(analyzer_mod):
    with analyzer_mod.progress_lock:
        analyzer_mod.ACTIVE_SCAN_FILES.clear()
        analyzer_mod.PROGRESS["file"] = "Scanning directories..."
        assert analyzer_mod._format_active_scan_label() == "Scanning directories..."
        assert analyzer_mod._format_active_scan_label(last_completed="x.mkv") == "Done: x.mkv"
