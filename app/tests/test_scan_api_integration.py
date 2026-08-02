"""Flask-client coverage for scan control API edge cases."""
from __future__ import annotations


def test_idle_scan_controls_are_safe(analyzer_mod):
    import video_analyzer.core as core

    with core.progress_lock:
        core.PROGRESS["status"] = "idle"
        core.PROGRESS["paused"] = False

    client = analyzer_mod.app.test_client()
    assert client.post("/abort").json["status"] == "idle"
    assert client.post("/pause").json == {"status": "idle", "paused": False}


def test_invalid_scan_request_is_normalized_without_starting_job(monkeypatch):
    import video_analyzer.core as core
    from video_analyzer import create_app

    called = []
    monkeypatch.setattr(core, "run_scan", lambda *args: called.append(args))
    class ImmediateThread:
        def __init__(self, target, args=(), daemon=False):
            self.target, self.args = target, args

        def start(self):
            self.target(*self.args)

    monkeypatch.setattr(core.threading, "Thread", ImmediateThread)
    with core.progress_lock:
        core.PROGRESS["status"] = "idle"
    response = create_app().test_client().post(
        "/start",
        json={"threads": 1, "scan_mode": "not-a-mode"},
    )
    assert response.status_code == 200
    assert response.json["status"] == "started"
    assert called == [(1, [], False, False, "all", None, "all")]
