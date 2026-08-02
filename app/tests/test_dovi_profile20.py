"""Dolby Vision Profile 20 (MV-HEVC stereo / dvwC) detection."""
from __future__ import annotations

from pathlib import Path

import pytest

SAMPLE = Path(__file__).resolve().parent / "DVprofile20.mp4"


@pytest.mark.skipif(not SAMPLE.is_file(), reason="DVprofile20.mp4 fixture missing")
def test_parse_isom_dovi_config_profile20():
    from video_analyzer.core import parse_isom_dovi_config

    info = parse_isom_dovi_config(str(SAMPLE))
    assert info is not None
    assert info.get("dovi_profile") == "20"
    assert info.get("box_type") == "dvwC"
    assert info.get("is_stereo") is True
    assert info.get("dovi_level") == "6"
    assert info.get("bl_compatibility_id") == "0"
    # RPU tools often report profile 5 for P20; container box must win in analyze_file_deep
    assert info.get("dovi_profile") != "5"


def test_decode_dovi_config_payload_profile20_bits():
    from video_analyzer.core import _decode_dovi_config_payload

    # Real dvwC payload prefix from DVprofile20.mp4: version 3.0, profile 20, level 6
    decoded = _decode_dovi_config_payload(bytes.fromhex("03002835"))
    assert decoded is not None
    assert decoded["dovi_profile"] == "20"
    assert decoded["dovi_level"] == "6"


def test_decode_dovi_config_payload_classic_profile5():
    from video_analyzer.core import _decode_dovi_config_payload

    # profile=5, level=6, rpu=1, el=0, bl=1 → tmp bits 0x0A35
    decoded = _decode_dovi_config_payload(bytes.fromhex("01000A35"))
    assert decoded is not None
    assert decoded["dovi_profile"] == "5"
