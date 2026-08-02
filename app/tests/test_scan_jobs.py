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
