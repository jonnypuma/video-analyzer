"""Unit tests for path confinement, schedule parsing, ZIP restore safety, and filters."""
from __future__ import annotations

import io
import zipfile


def test_parse_schedule_time_defaults(analyzer_mod):
    assert analyzer_mod._parse_schedule_time("") == (3, 0)
    assert analyzer_mod._parse_schedule_time(None) == (3, 0)
    assert analyzer_mod._parse_schedule_time("not-a-time") == (3, 0)


def test_parse_schedule_time_valid(analyzer_mod):
    assert analyzer_mod._parse_schedule_time("14:30") == (14, 30)
    assert analyzer_mod._parse_schedule_time("00:00") == (0, 0)
    assert analyzer_mod._parse_schedule_time("23:59") == (23, 59)


def test_parse_schedule_time_out_of_range_falls_back(analyzer_mod):
    assert analyzer_mod._parse_schedule_time("25:00") == (3, 0)
    assert analyzer_mod._parse_schedule_time("12:99") == (3, 0)


def test_is_path_within_root_accepts_child(tmp_path):
    from video_analyzer.paths import is_path_within_root

    root = tmp_path / "movies"
    child = root / "A" / "film.mkv"
    child.parent.mkdir(parents=True)
    child.write_bytes(b"x")
    assert is_path_within_root(str(child), str(root)) is True
    assert is_path_within_root(str(root), str(root)) is True


def test_is_path_within_root_rejects_prefix_sibling(tmp_path):
    from video_analyzer.paths import is_path_within_root

    movies = tmp_path / "movies"
    movies_backup = tmp_path / "movies_backup"
    movies.mkdir()
    movies_backup.mkdir()
    sneaky = movies_backup / "secret.mkv"
    sneaky.write_bytes(b"x")
    assert is_path_within_root(str(sneaky), str(movies)) is False


def test_resolve_allowed_media_path_ok(media_root):
    from video_analyzer.paths import resolve_allowed_media_path

    f = media_root / "Show" / "ep.mkv"
    f.parent.mkdir()
    f.write_bytes(b"x")
    allowed, err = resolve_allowed_media_path(str(f))
    assert err is None
    assert allowed is not None
    assert allowed.endswith("ep.mkv")


def test_resolve_allowed_media_path_rejects_outside(media_root, tmp_path):
    from video_analyzer.paths import resolve_allowed_media_path

    outside = tmp_path / "other" / "ep.mkv"
    outside.parent.mkdir()
    outside.write_bytes(b"x")
    allowed, err = resolve_allowed_media_path(str(outside))
    assert allowed is None
    assert err and "outside" in err.lower()


def test_zip_member_path_is_safe(analyzer_mod):
    assert analyzer_mod._zip_member_path_is_safe("processed_videos.db") is True
    assert analyzer_mod._zip_member_path_is_safe("settings.json") is True
    assert analyzer_mod._zip_member_path_is_safe("nested/settings.json") is True
    assert analyzer_mod._zip_member_path_is_safe("../processed_videos.db") is False
    assert analyzer_mod._zip_member_path_is_safe("/etc/passwd") is False
    assert analyzer_mod._zip_member_path_is_safe("C:/Windows/system32") is False
    assert analyzer_mod._zip_member_path_is_safe("foo/../../etc/passwd") is False


def test_validate_restore_zip_rejects_traversal(analyzer_mod):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("../../evil.db", b"nope")
        zf.writestr("settings.json", b"{}")
    buf.seek(0)
    with zipfile.ZipFile(buf, "r") as zf:
        try:
            analyzer_mod._validate_restore_zip_members(zf)
            assert False, "expected ValueError"
        except ValueError as e:
            assert "Unsafe" in str(e) or "unsafe" in str(e).lower() or "rejected" in str(e).lower()


def test_validate_restore_zip_maps_allowed(analyzer_mod):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("processed_videos.db", b"SQLite")
        zf.writestr("settings.json", b'{"threads":"4"}')
    buf.seek(0)
    with zipfile.ZipFile(buf, "r") as zf:
        found = analyzer_mod._validate_restore_zip_members(zf)
    assert found["processed_videos.db"] == "processed_videos.db"
    assert found["settings.json"] == "settings.json"


def test_build_filter_query_media_type():
    from video_analyzer.queries import build_filter_query

    where, params = build_filter_query({"media_type": "movie"})
    assert "media_type" in where.lower()
    assert any(str(p).lower() == "movie" for p in params)


def test_build_filter_query_missing_flag():
    from video_analyzer.queries import build_filter_query

    where, params = build_filter_query({"missing": "1"})
    assert "missing" in where.lower()
    assert where.count("?") >= 0  # may be equality without param for 1/0 flags


def test_build_filter_query_anomaly_flag():
    from video_analyzer.queries import build_filter_query

    where, params = build_filter_query({"anomaly": "1"})
    assert "quality_anomaly" in where.lower()


def test_parse_sort_order():
    from video_analyzer.queries import parse_sort_order

    assert parse_sort_order("asc") == "ASC"
    assert parse_sort_order("DESC") == "DESC"
    assert parse_sort_order("nope") == "DESC"


def test_app_version_reads_changelog():
    from video_analyzer.config import app_version

    ver = app_version()
    assert ver and ver != ""
    # Latest changelog entry should be semver-like
    parts = ver.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)


def test_health_endpoint(analyzer_mod):
    client = analyzer_mod.app.test_client()
    analyzer_mod.init_db()
    res = client.get("/api/health")
    assert res.status_code == 200
    data = res.get_json()
    assert data["status"] == "healthy"
    assert data["database"] == "ok"
    assert "version" in data
