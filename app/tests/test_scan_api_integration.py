"""Flask-client coverage for scan control API edge cases."""
from __future__ import annotations


def test_idle_scan_controls_are_safe(analyzer_mod):
    import video_analyzer.core as core

    with core.progress_lock:
        core.PROGRESS["status"] = "idle"
        core.PROGRESS["paused"] = False

    client = analyzer_mod.app.test_client()
    from tests.conftest import csrf_post
    assert csrf_post(client, "/abort").json["status"] == "idle"
    assert csrf_post(client, "/pause").json == {"status": "idle", "paused": False}


def test_invalid_scan_request_is_normalized_without_starting_job(monkeypatch):
    import video_analyzer.core as core
    import video_analyzer.routes.scan as scan_routes
    from video_analyzer import create_app

    called = []
    monkeypatch.setattr(core, "run_scan", lambda *args, **kwargs: called.append((args, kwargs)))
    class ImmediateThread:
        def __init__(self, target, args=(), kwargs=None, daemon=False):
            self.target, self.args, self.kwargs = target, args, kwargs or {}

        def start(self):
            self.target(*self.args, **self.kwargs)

    monkeypatch.setattr(scan_routes.threading, "Thread", ImmediateThread)
    with core.progress_lock:
        core.PROGRESS["status"] = "idle"
    from tests.conftest import csrf_post
    response = csrf_post(
        create_app().test_client(),
        "/start",
        json={"threads": 1, "scan_mode": "not-a-mode"},
    )
    assert response.status_code == 200
    assert response.json["status"] == "started"
    assert called == [((1, [], False, False, "all", None, "all"), {"preclaimed": True})]
    with core.progress_lock:
        core.PROGRESS["status"] = "idle"


def test_start_claims_scanning_before_worker_runs(analyzer_mod, monkeypatch):
    """POST /start must mark progress scanning even if the worker has not run yet."""
    import video_analyzer.core as core
    import video_analyzer.routes.scan as scan_routes

    started = []

    class DeferredThread:
        def __init__(self, target, args=(), kwargs=None, daemon=False):
            started.append({"target": target, "args": args, "kwargs": kwargs or {}})

        def start(self):
            return None

    monkeypatch.setattr(scan_routes.threading, "Thread", DeferredThread)
    with core.progress_lock:
        core.PROGRESS["status"] = "idle"
        core.PROGRESS["scan_completed"] = False
    from tests.conftest import csrf_post
    client = analyzer_mod.app.test_client()
    response = csrf_post(client, "/start", json={"threads": 1})
    assert response.status_code == 200
    assert response.json["status"] == "started"
    progress = client.get("/progress").json
    assert progress["status"] == "scanning"
    assert started and started[0]["kwargs"].get("preclaimed") is True
    with core.progress_lock:
        core.PROGRESS["status"] = "idle"


def test_preclaimed_run_scan_does_not_return_early(analyzer_mod, monkeypatch):
    """preclaimed=True must run the scan even if PROGRESS is already scanning."""
    import video_analyzer.core as core
    import video_analyzer.scan.pipeline as pipeline

    analyzer_mod.init_db()
    ran = {"n": 0}

    def fake_collect(*args, **kwargs):
        ran["n"] += 1
        return [], set()

    monkeypatch.setattr(pipeline, "collect_files_to_scan", fake_collect)
    monkeypatch.setattr(pipeline, "load_processed_map", lambda: {})
    monkeypatch.setattr(pipeline, "prepare_scan_paths", lambda *a, **k: ([], {}))
    monkeypatch.setattr(pipeline, "setup_new_log_files", lambda: None)
    monkeypatch.setattr(pipeline, "cleanup_old_logs", lambda: None)
    monkeypatch.setattr(pipeline, "cleanup_deleted_files", lambda *a, **k: 0)
    monkeypatch.setattr(pipeline, "count_removed_files", lambda *a, **k: 0)
    monkeypatch.setattr(pipeline, "finalize_scan", lambda *a, **k: None)

    with core.progress_lock:
        core.PROGRESS["status"] = "scanning"
        core.PROGRESS["file"] = "Initializing..."
        core.PROGRESS["scan_completed"] = False
    analyzer_mod.run_scan(thread_count=1, preclaimed=True)
    assert ran["n"] == 1
    with core.progress_lock:
        core.PROGRESS["status"] = "idle"
        core.PROGRESS["scan_completed"] = False


def test_run_scan_returns_early_if_already_scanning(analyzer_mod, monkeypatch):
    import video_analyzer.core as core
    import video_analyzer.scan.pipeline as pipeline

    ran = {"n": 0}
    monkeypatch.setattr(pipeline, "collect_files_to_scan", lambda *a, **k: ran.__setitem__("n", ran["n"] + 1) or ([], set()))
    monkeypatch.setattr(pipeline, "load_processed_map", lambda: {})
    monkeypatch.setattr(pipeline, "prepare_scan_paths", lambda *a, **k: ([], {}))
    monkeypatch.setattr(pipeline, "setup_new_log_files", lambda: None)

    with core.progress_lock:
        core.PROGRESS["status"] = "scanning"
        core.PROGRESS["file"] = "Initializing..."
        core.PROGRESS["scan_completed"] = False
    analyzer_mod.run_scan(thread_count=1)
    assert ran["n"] == 0
    with core.progress_lock:
        assert core.PROGRESS["status"] == "scanning"
        assert core.PROGRESS.get("scan_completed") is not True
        core.PROGRESS["status"] = "idle"
