"""Tests for bounded scan future scheduling."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor


def test_bounded_scan_futures_limit_queued_work(analyzer_mod):
    seen = []
    import video_analyzer.core as core

    def work(value):
        seen.append(value)
        return value * 2

    with ThreadPoolExecutor(max_workers=2) as executor:
        original = core.scan_file_worker
        core.scan_file_worker = work
        try:
            futures = core.iter_bounded_scan_futures(executor, list(range(7)), 4)
            results = [future.result() for future in futures]
        finally:
            core.scan_file_worker = original

    assert sorted(results) == [0, 2, 4, 6, 8, 10, 12]
    assert sorted(seen) == list(range(7))
