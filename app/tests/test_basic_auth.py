"""Tests for optional BASIC_AUTH login protection, logout, and CSRF."""
from __future__ import annotations

from tests.conftest import csrf_post


def test_auth_disabled_by_empty_environment(analyzer_mod, monkeypatch):
    monkeypatch.delenv("BASIC_AUTH", raising=False)
    response = analyzer_mod.app.test_client().get("/login")
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")


def test_auth_off_still_serves_index(analyzer_mod, monkeypatch):
    monkeypatch.delenv("BASIC_AUTH", raising=False)
    response = analyzer_mod.app.test_client().get("/")
    assert response.status_code == 200
    assert b"csrf-token" in response.data
    assert b"Logout" not in response.data


def test_basic_auth_protects_pages_and_api(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH", "admin:s3cret")
    from video_analyzer import create_app

    app = create_app()
    client = app.test_client()

    page = client.get("/")
    assert page.status_code == 302
    assert "/login" in page.headers["Location"]
    assert client.get("/api/videos").status_code == 401

    login_page = client.get("/login")
    token = login_page.headers.get("X-CSRF-Token", "")
    login = client.post(
        "/login",
        data={"username": "admin", "password": "s3cret", "next": "/", "csrf_token": token},
        follow_redirects=False,
    )
    assert login.status_code == 302
    assert login.headers["Location"] == "/"
    home = client.get("/")
    assert home.status_code == 200
    assert b"Logout" in home.data
    assert client.get("/api/videos").status_code != 401


def test_basic_auth_rejects_wrong_password(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH", "admin:s3cret")
    from video_analyzer import create_app

    client = create_app().test_client()
    token = client.get("/login").headers.get("X-CSRF-Token", "")
    response = client.post(
        "/login",
        data={"username": "admin", "password": "wrong", "csrf_token": token},
    )
    assert response.status_code == 200
    assert b"not correct" in response.data


def test_logout_clears_session(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH", "admin:s3cret")
    from video_analyzer import create_app

    client = create_app().test_client()
    token = client.get("/login").headers.get("X-CSRF-Token", "")
    client.post(
        "/login",
        data={"username": "admin", "password": "s3cret", "next": "/", "csrf_token": token},
    )
    logout = client.get("/logout")
    assert logout.status_code == 302
    assert "/login" in logout.headers["Location"]
    assert client.get("/api/videos").status_code == 401


def test_csrf_rejects_mutating_request_without_token(analyzer_mod):
    client = analyzer_mod.app.test_client()
    client.get("/api/health")
    response = client.post("/abort")
    assert response.status_code == 403
    assert response.json["message"] == "Invalid CSRF token"


def test_csrf_accepts_header_token(analyzer_mod):
    client = analyzer_mod.app.test_client()
    response = csrf_post(client, "/abort")
    assert response.status_code == 200
    assert response.json["status"] == "idle"


def test_csrf_settings_post_succeeds(analyzer_mod):
    analyzer_mod.init_db()
    client = analyzer_mod.app.test_client()
    response = csrf_post(client, "/api/settings", json={"debug_mode": False})
    assert response.status_code == 200
    assert response.json["status"] == "success"


def test_settings_post_with_schedule_mode_succeeds(analyzer_mod):
    """UI save always sends mode/value; that path must call apply_scan_schedule."""
    analyzer_mod.init_db()
    client = analyzer_mod.app.test_client()
    response = csrf_post(
        client,
        "/api/settings",
        json={"mode": "manual", "value": "", "debug_mode": False},
    )
    assert response.status_code == 200, response.get_json()
    assert response.json["status"] == "success"


def test_csrf_accepts_json_body_token(analyzer_mod):
    analyzer_mod.init_db()
    client = analyzer_mod.app.test_client()
    token = client.get("/api/health").headers.get("X-CSRF-Token", "")
    response = client.post(
        "/api/settings",
        json={"debug_mode": False, "csrf_token": token},
    )
    assert response.status_code == 200
    assert response.json["status"] == "success"


def test_index_exposes_csrf_js_global(analyzer_mod):
    html = analyzer_mod.app.test_client().get("/").text
    assert "window.CSRF_TOKEN" in html
    assert 'meta name="csrf-token"' in html


def test_fetch_wrapper_refreshes_csrf_on_403(analyzer_mod):
    js = analyzer_mod.app.test_client().get("/static/js/core.js").text
    assert "function captureCsrfToken" in js
    assert "Invalid CSRF token" in js
    assert "/api/health" in js


def test_route_modules_bind_split_helpers(analyzer_mod):
    from video_analyzer.routes import scan as scan_routes
    from video_analyzer.routes import settings as settings_routes

    assert callable(settings_routes.apply_scan_schedule)
    assert callable(settings_routes._as_int)
    assert callable(settings_routes.invalidate_library_stats_cache)
    assert isinstance(settings_routes.ARR_STATUS_CACHE, dict)
    assert scan_routes.BASE_DIR
    assert callable(scan_routes.run_command)
    assert scan_routes.sys is not None
    assert scan_routes.signal is not None
