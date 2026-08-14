"""Tests for conservative codec/quality anomaly detection."""
from __future__ import annotations


def test_quality_anomaly_flags(analyzer_mod):
    flag = analyzer_mod.compute_quality_anomaly_flag({
        "width": 3840, "height": 2160, "bitrate_mbps": 4,
        "video_codec": "H.264", "fps": 24,
    })
    assert flag == "low_bitrate_4k,legacy_codec_4k"
    assert analyzer_mod.compute_quality_anomaly_flag({
        "width": 1920, "height": 1080, "bitrate_mbps": 8,
        "video_codec": "hevc", "fps": 24,
    }) is None


def test_anomaly_filter_sql_and_api(analyzer_mod):
    from video_analyzer.queries import build_filter_query

    analyzer_mod.init_db()
    with analyzer_mod.get_db() as conn:
        conn.execute(
            "INSERT INTO videos (full_path, filename, quality_anomaly) VALUES (?, ?, ?)",
            ("/lib/a.mkv", "a.mkv", "low_bitrate_4k"),
        )
        conn.execute(
            "INSERT INTO videos (full_path, filename) VALUES (?, ?)",
            ("/lib/b.mkv", "b.mkv"),
        )
    where, params = build_filter_query({"anomaly": "1"})
    assert "quality_anomaly" in where
    with analyzer_mod.get_db() as conn:
        rows = conn.execute(f"SELECT filename FROM videos WHERE {where}", params).fetchall()
    assert [row[0] for row in rows] == ["a.mkv"]

    client = analyzer_mod.app.test_client()
    response = client.get("/api/videos?anomaly=1")
    assert response.status_code == 200
    names = [row[0] for row in response.json["rows"]]
    assert names == ["a.mkv"]

    anomalies = client.get("/api/anomalies")
    assert anomalies.status_code == 200
    assert anomalies.json["anomalies"][0]["flags"] == "low_bitrate_4k"


def test_worker_writes_quality_anomaly_column(analyzer_mod):
    analyzer_mod.init_db()
    result = {
        "filename": "a.mkv", "category": "sdr_only", "profile": None, "el_type": None,
        "container": "mkv", "source_vol": "test", "full_path": "/lib/a.mkv",
        "last_scanned": "2026-08-14 00:00:00", "resolution": "4K", "bitrate_mbps": 4,
        "scan_error": None, "is_hybrid": 0, "is_source_hybrid": 0, "secondary_hdr": None,
        "width": 3840, "height": 2160, "file_size": 100, "bl_compatibility_id": None,
        "audio_codecs": None, "audio_langs": None, "audio_channels": None, "subtitles": None,
        "max_cll": None, "max_fall": None, "fps": 24, "aspect_ratio": None,
        "imdb_id": None, "tvdb_id": None, "tmdb_id": None, "rotten_id": None,
        "metacritic_id": None, "trakt_id": None,
        "tvdb_series_id": None, "tvdb_episode_id": None, "imdb_series_id": None,
        "imdb_episode_id": None, "tmdb_series_id": None, "tmdb_episode_id": None,
        "trakt_series_id": None, "trakt_episode_id": None, "rotten_series_id": None,
        "rotten_episode_id": None, "metacritic_series_id": None, "metacritic_episode_id": None,
        "imdb_rating": None, "tvdb_rating": None, "tmdb_rating": None, "rotten_rating": None,
        "metacritic_rating": None, "trakt_rating": None, "scan_attempts": 0,
        "video_source": None, "source_format": None, "video_codec": "hevc",
        "is_3d": 0, "edition": None, "year": None, "media_type": "movie",
        "show_title": None, "season": None, "episode": None, "movie_title": "A",
        "episode_title": None, "nfo_missing": 1, "missing": 0,
        "validation_flag": None, "quality_anomaly": "low_bitrate_4k", "file_mtime": 1.0,
        "dup_group_key": None, "dup_exact_key": None, "dup_count": 0,
    }
    analyzer_mod.save_batch_to_db([result], duplicate_check_on_scan=False)
    with analyzer_mod.get_db() as conn:
        row = conn.execute(
            "SELECT quality_anomaly, validation_flag, file_mtime FROM videos WHERE full_path=?",
            ("/lib/a.mkv",),
        ).fetchone()
    assert row[0] == "low_bitrate_4k"
    assert row[1] is None
    assert row[2] == 1.0
