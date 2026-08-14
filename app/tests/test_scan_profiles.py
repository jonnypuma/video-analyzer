"""Tests for configurable scan profiles."""
from __future__ import annotations


def test_scan_profiles_can_be_saved_and_loaded(analyzer_mod):
    analyzer_mod.init_db()
    client = analyzer_mod.app.test_client()
    from tests.conftest import csrf_post, csrf_request
    response = csrf_post(
        client,
        "/api/scan_profiles",
        json={"name": "4K Movies", "settings": {"threads": "2", "min_size_mb": "100"}}
    )
    assert response.status_code == 200
    profiles = client.get("/api/scan_profiles").json["profiles"]
    profile = next(p for p in profiles if p["name"] == "4K Movies")
    assert profile["settings"]["min_size_mb"] == "100"

    response = csrf_request(client, "DELETE", "/api/scan_profiles", json={"name": "4K Movies"})
    assert response.status_code == 200
    assert all(p["name"] != "4K Movies" for p in response.json["profiles"])
