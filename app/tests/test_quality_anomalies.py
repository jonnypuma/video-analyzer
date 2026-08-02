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
