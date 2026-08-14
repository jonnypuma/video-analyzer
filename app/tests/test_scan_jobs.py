"""Tests for durable scan-job records."""
from __future__ import annotations


def test_scan_job_is_persisted_and_exposed(analyzer_mod):
    analyzer_mod.init_db()
    with analyzer_mod.get_db() as conn:
        assert conn.execute(
            "SELECT version FROM schema_migrations WHERE version=1"
        ).fetchone()
    job_id = analyzer_mod.create_scan_job({"scan_mode": "changed"})
    analyzer_mod.update_scan_job(job_id, progress={"current": 4, "total": 10})

    response = analyzer_mod.app.test_client().get("/api/scan_jobs")
    assert response.status_code == 200
    job = next(item for item in response.json["jobs"] if item["job_id"] == job_id)
    assert job["status"] == "running"
    assert job["options"]["scan_mode"] == "changed"
    assert job["progress"]["current"] == 4

    analyzer_mod.update_scan_job(job_id, "completed", {"current": 10, "total": 10})
    response = analyzer_mod.app.test_client().get("/api/scan_jobs")
    job = next(item for item in response.json["jobs"] if item["job_id"] == job_id)
    assert job["status"] == "completed"
    assert job["finished_at"]


def test_pending_paths_are_resumable_without_recrawl(analyzer_mod, monkeypatch):
    import video_analyzer.core as core
    import video_analyzer.scan.pipeline as pipeline

    analyzer_mod.init_db()
    job_id = analyzer_mod.create_scan_job({"scan_mode": "all"})
    analyzer_mod.persist_pending_scan_paths(job_id, ["/library/a.mkv", "/library/b.mkv"])
    analyzer_mod.update_scan_job(job_id, "interrupted")
    info = analyzer_mod.load_interrupted_job(job_id)
    assert info["status"] == "interrupted"
    assert info["pending_count"] == 2

    crawled = {"n": 0}

    def fake_collect(*args, **kwargs):
        crawled["n"] += 1
        return [], set()

    monkeypatch.setattr(pipeline, "collect_files_to_scan", fake_collect)
    monkeypatch.setattr(pipeline, "load_processed_map", lambda: {})
    monkeypatch.setattr(pipeline, "prepare_scan_paths", lambda *a, **k: ([], {}))
    monkeypatch.setattr(pipeline, "setup_new_log_files", lambda: None)
    monkeypatch.setattr(pipeline, "cleanup_old_logs", lambda: None)
    monkeypatch.setattr(pipeline, "cleanup_deleted_files", lambda *a, **k: 0)
    monkeypatch.setattr(pipeline, "count_removed_files", lambda *a, **k: 0)
    monkeypatch.setattr(pipeline, "finalize_scan", lambda *a, **k: None)
    analyzed = {"job_id": None}

    def fake_analyze(files, processed_map, settings, threads, start_time, job_id=None):
        analyzed["job_id"] = job_id
        analyzed["pending"] = pipeline.pending_scan_file_count(job_id)
        return {
            "metrics_sum": {"bitrate": 0.0, "width": 0, "height": 0, "file_size": 0},
            "metrics_count": {"bitrate": 0, "width": 0, "height": 0, "file_size": 0},
        }

    monkeypatch.setattr(pipeline, "analyze_files", fake_analyze)
    with core.progress_lock:
        core.PROGRESS["status"] = "idle"
    pipeline.run_scan(resume_job_id=job_id)
    assert crawled["n"] == 0
    assert analyzed["job_id"] == job_id
    assert analyzed["pending"] == 2
