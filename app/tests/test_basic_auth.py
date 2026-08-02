"""Tests for optional BASIC_AUTH login protection."""
from __future__ import annotations


def test_auth_disabled_by_empty_environment(analyzer_mod, monkeypatch):
    monkeypatch.delenv("BASIC_AUTH", raising=False)
    response = analyzer_mod.app.test_client().get("/login")
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")


def test_basic_auth_protects_pages_and_api(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH", "admin:s3cret")
    from video_analyzer import create_app

    app = create_app()
    client = app.test_client()

    page = client.get("/")
    assert page.status_code == 302
    assert "/login" in page.headers["Location"]
    assert client.get("/api/videos").status_code == 401

    login = client.post(
        "/login",
        data={"username": "admin", "password": "s3cret", "next": "/"},
        follow_redirects=False,
    )
    assert login.status_code == 302
    assert login.headers["Location"] == "/"
    assert client.get("/api/videos").status_code != 401


def test_basic_auth_rejects_wrong_password(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH", "admin:s3cret")
    from video_analyzer import create_app

    client = create_app().test_client()
    response = client.post(
        "/login",
        data={"username": "admin", "password": "wrong"},
    )
    assert response.status_code == 200
    assert b"not correct" in response.data
