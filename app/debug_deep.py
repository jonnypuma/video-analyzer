import json
import os
import re
import subprocess
import sys
import tempfile
from typing import Any


def _run(cmd: list[str]) -> tuple[int, str, str]:
    """Run command and return (rc, stdout, stderr) as text."""
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.returncode, (p.stdout or ""), (p.stderr or "")


def _print_header(title: str) -> None:
    print()
    print("=" * 90)
    print(title)
    print("=" * 90)


def _print_cmd(cmd: list[str]) -> None:
    print("$ " + " ".join(f'"{c}"' if " " in c else c for c in cmd))


def _pretty_json_or_raw(text: str) -> str:
    try:
        return json.dumps(json.loads(text), indent=2)
    except Exception:
        return text


def _extract_ffprobe_dovi_side_data_from_streams(streams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for idx, stream in enumerate(streams or []):
        side_data = stream.get("side_data_list") or []
        for entry in side_data:
            blob = json.dumps(entry).lower()
            side_type = str(entry.get("side_data_type", "")).lower()
            if "dovi" in blob or "dolby vision" in blob or "dovi" in side_type:
                item = dict(entry)
                item["_video_stream_index"] = idx
                out.append(item)
    return out


def _mediainfo_video_tracks(mi_json_text: str) -> list[dict[str, Any]]:
    keys = [
        "ID",
        "StreamOrder",
        "Format",
        "CodecID",
        "Width",
        "Height",
        "BitRate",
        "HDR_Format",
        "HDR_Format_Profile",
        "HDR_Format_Compatibility",
        "HDR_Format_String",
        "HDR_Format_Settings",
        "HDR_Format_Version",
        "transfer_characteristics",
        "Transfer_Characteristics",
    ]
    try:
        data = json.loads(mi_json_text)
    except Exception:
        return [{"_parse_error": "Could not parse MediaInfo JSON", "_raw": mi_json_text}]
    tracks = (data.get("media") or {}).get("track") or []
    out = []
    for t in tracks:
        if t.get("@type") != "Video":
            continue
        out.append({k: t.get(k) for k in keys})
    return out


def _extract_mediainfo_hdr_fields(mi_json_text: str) -> dict[str, Any]:
    tracks = _mediainfo_video_tracks(mi_json_text)
    if not tracks:
        return {}
    # Prefer the first track that mentions Dolby Vision / dvhe / BL+EL; else first video.
    for t in tracks:
        blob = " ".join(str(t.get(k) or "") for k in t).lower()
        if any(tok in blob for tok in ("dolby vision", "dvhe", "dvh1", "dvav", "dva1", "bl+el", "rpu")):
            return t
    return tracks[0]


def _parse_profile_from_mi_fields(fields: dict[str, Any]) -> str | None:
    text = " ".join(str(fields.get(k) or "") for k in ("HDR_Format_Profile", "HDR_Format", "HDR_Format_Settings"))
    lower = text.lower()
    m = re.search(r"(?:dv(?:he|h1|av|a1)|dav1)\.(\d{2})(?:\.(\d{2}))?", lower)
    if not m:
        return None
    p = str(int(m.group(1)))
    c = m.group(2)
    if c is None:
        return p
    compat = str(int(c))
    if compat == "1":
        return f"{p}.1"
    if compat == "4":
        return f"{p}.4"
    return p


def _compat_from_mi_fields(fields: dict[str, Any]) -> str | None:
    compat = str(fields.get("HDR_Format_Compatibility") or "").upper()
    if "HLG" in compat:
        return "HLG"
    if "HDR10+" in compat or "HDR10PLUS" in compat:
        return "HDR10+"
    if "HDR10" in compat or "BLU-RAY" in compat:
        return "HDR10"
    return None


def _el_from_mi_fields(fields: dict[str, Any]) -> str | None:
    settings = str(fields.get("HDR_Format_Settings") or "").upper()
    if not settings.strip():
        return None
    if "FEL" in settings or "BL+EL" in settings:
        return "FEL"
    if "MEL" in settings or "BL+RPU" in settings:
        # BL+RPU without EL is MEL-style single-layer P7
        if "BL+EL" not in settings:
            return "MEL"
    return None


def _parse_dovi_tool_info_json(out_text: str) -> dict[str, Any] | None:
    start = out_text.find("{")
    if start == -1:
        return None
    try:
        return json.loads(out_text[start:])
    except Exception:
        return None


def _list_video_stream_indexes(path: str) -> list[int]:
    rc, out, _ = _run([
        "ffprobe", "-v", "error", "-select_streams", "v",
        "-show_entries", "stream=index,codec_name,codec_type,width,height,bit_rate",
        "-of", "json", path,
    ])
    if rc != 0 or not out.strip():
        return [0]
    try:
        data = json.loads(out)
    except Exception:
        return [0]
    streams = data.get("streams") or []
    # Use positional video indexes for -map 0:v:N (0..n-1), not absolute stream index.
    return list(range(len(streams))) if streams else [0]


def _extract_rpu_for_video_map(path: str, video_map_index: int | None, rpu_file: str) -> tuple[int, int, str, str]:
    """
    Extract RPU via ffmpeg|dovi_tool.
    video_map_index=None uses default video selection (analyzer legacy path).
    Returns (extract_rc, rpu_size, stdout, stderr).
    """
    if os.path.exists(rpu_file):
        try:
            os.remove(rpu_file)
        except Exception:
            pass
    ffmpeg_cmd = ["ffmpeg", "-i", path]
    if video_map_index is not None:
        ffmpeg_cmd += ["-map", f"0:v:{video_map_index}"]
    ffmpeg_cmd += ["-c:v", "copy", "-to", "2", "-f", "hevc", "-y", "-"]
    extract_cmd = ["dovi_tool", "extract-rpu", "-", "-o", rpu_file]
    p1 = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    p2 = subprocess.run(extract_cmd, stdin=p1.stdout, capture_output=True, text=True)
    if p1.stdout:
        p1.stdout.close()
    try:
        p1.wait(timeout=30)
    except Exception:
        pass
    size = os.path.getsize(rpu_file) if os.path.exists(rpu_file) else 0
    return p2.returncode, size, (p2.stdout or ""), (p2.stderr or "")


def _best_rpu_info(path: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Try default + each video map; return best dovi_tool info JSON and attempt log."""
    attempts: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None
    maps: list[int | None] = [None]
    for idx in _list_video_stream_indexes(path):
        if idx not in maps:
            maps.append(idx)

    for map_idx in maps:
        label = "default" if map_idx is None else f"0:v:{map_idx}"
        rpu_file = os.path.join(tempfile.gettempdir(), f"debug_rpu_{os.getpid()}_{label.replace(':', '_')}.bin")
        try:
            rc, size, out, err = _extract_rpu_for_video_map(path, map_idx, rpu_file)
            entry: dict[str, Any] = {
                "map": label,
                "extract_rc": rc,
                "rpu_size": size,
                "el_type": None,
                "dovi_profile": None,
            }
            if rc == 0 and size > 0:
                rc_info, out_info, err_info = _run(["dovi_tool", "info", "-i", rpu_file, "-f", "0"])
                entry["info_rc"] = rc_info
                if err_info.strip():
                    entry["info_stderr"] = err_info.strip()
                parsed = _parse_dovi_tool_info_json(out_info) if out_info.strip() else None
                if parsed:
                    entry["dovi_profile"] = parsed.get("dovi_profile")
                    entry["el_type"] = parsed.get("el_type")
                    if best is None:
                        best = parsed
                    elif str(parsed.get("el_type") or "").upper() == "FEL" and str(best.get("el_type") or "").upper() != "FEL":
                        best = parsed
            else:
                if out.strip():
                    entry["extract_stdout"] = out.strip()
                if err.strip():
                    entry["extract_stderr"] = err.strip()
            attempts.append(entry)
        finally:
            if os.path.exists(rpu_file):
                try:
                    os.remove(rpu_file)
                except Exception:
                    pass
    return best, attempts


def _build_interpretation(
    mi_fields: dict[str, Any],
    dovi_tool_data: dict[str, Any] | None,
    mi_tracks: list[dict[str, Any]] | None = None,
    video_stream_count: int = 1,
) -> dict[str, Any]:
    detected_profile = None
    detected_el = None
    detected_compat = None
    confidence = "low"
    evidence: list[str] = []

    if dovi_tool_data:
        raw_prof = dovi_tool_data.get("dovi_profile")
        raw_el = dovi_tool_data.get("el_type")
        if raw_prof is not None:
            detected_profile = str(raw_prof)
            evidence.append(f"RPU parse profile={detected_profile}")
        if raw_el:
            detected_el = str(raw_el)
            evidence.append(f"RPU parse el_type={detected_el}")
        confidence = "high"

    # Prefer DV-bearing MediaInfo track fields already selected by caller.
    candidates = [mi_fields] if mi_fields else []
    if mi_tracks:
        candidates = mi_tracks

    mi_profile = None
    mi_compat = None
    mi_el = None
    for fields in candidates:
        if not fields:
            continue
        p = _parse_profile_from_mi_fields(fields)
        c = _compat_from_mi_fields(fields)
        e = _el_from_mi_fields(fields)
        if p and not mi_profile:
            mi_profile = p
        if c and not mi_compat:
            mi_compat = c
        if e and (mi_el is None or (e == "FEL" and mi_el != "FEL")):
            mi_el = e

    if mi_profile:
        if not detected_profile:
            detected_profile = mi_profile
        evidence.append(f"MediaInfo profile hint={mi_profile}")
        if confidence != "high":
            confidence = "medium"
    if mi_compat:
        detected_compat = mi_compat
        evidence.append(f"MediaInfo compatibility={mi_compat}")
    if mi_el:
        if not detected_el:
            detected_el = mi_el
            evidence.append(f"MediaInfo EL hint={mi_el}")
        elif str(detected_el).upper() != mi_el:
            evidence.append(f"MediaInfo EL hint={mi_el} (RPU already set el_type)")

    if video_stream_count > 1:
        evidence.append(f"ffprobe video stream count={video_stream_count} (possible DT-DL)")

    # Apply analyzer-like normalization for P8/P10 when only base profile is known.
    if detected_profile in ("8", "10") and mi_compat:
        if mi_compat == "HLG":
            detected_profile = f"{detected_profile}.4"
        elif mi_compat in ("HDR10", "HDR10+"):
            detected_profile = f"{detected_profile}.1"

    detected_format = "dovi" if detected_profile else "unknown"
    if detected_format == "unknown" and mi_compat:
        evidence.append("No Dolby Vision profile/RPU found — looks like plain HDR base (filename FEL/DT-DL may be wrong)")

    return {
        "format": detected_format,
        "profile": detected_profile,
        "el_type": detected_el,
        "compatibility": detected_compat,
        "confidence": confidence,
        "evidence": evidence,
    }


def run_deep_debug(path: str) -> None:
    print(f"Analyzing file: {path}")
    if not os.path.exists(path):
        print("ERROR: File not found")
        return

    # Count / summarize all video streams early.
    cmd_all_v = [
        "ffprobe", "-v", "error", "-select_streams", "v",
        "-show_entries", "stream=index,codec_name,profile,width,height,bit_rate,color_transfer:stream_tags",
        "-of", "json", path,
    ]
    rc_all_v, out_all_v, err_all_v = _run(cmd_all_v)
    video_streams: list[dict[str, Any]] = []
    if rc_all_v == 0 and out_all_v.strip():
        try:
            video_streams = (json.loads(out_all_v).get("streams") or [])
        except Exception:
            video_streams = []

    pre_mi_tracks: list[dict[str, Any]] = []
    pre_mi_fields: dict[str, Any] = {}
    rc_mi_pre, out_mi_pre, _ = _run(["mediainfo", "--Output=JSON", path])
    if rc_mi_pre == 0 and out_mi_pre.strip():
        pre_mi_tracks = _mediainfo_video_tracks(out_mi_pre)
        pre_mi_fields = _extract_mediainfo_hdr_fields(out_mi_pre)

    pre_dovi_tool_data, pre_rpu_attempts = _best_rpu_info(path)
    summary = _build_interpretation(
        pre_mi_fields,
        pre_dovi_tool_data,
        mi_tracks=pre_mi_tracks,
        video_stream_count=len(video_streams) or 1,
    )

    _print_header("INTERPRETATION SUMMARY")
    print(f"Detected format: {summary['format']}")
    print(f"Detected profile: {summary['profile'] or 'unknown'}")
    print(f"Detected EL type: {summary['el_type'] or 'unknown'}")
    print(f"Base compatibility: {summary['compatibility'] or 'unknown'}")
    print(f"Confidence: {summary['confidence']}")
    print(f"Video streams (ffprobe): {len(video_streams)}")
    print(f"MediaInfo video tracks: {len(pre_mi_tracks)}")
    evidence = summary.get("evidence") or []
    if evidence:
        print("Evidence:")
        for ev in evidence:
            print(f"- {ev}")
    if pre_rpu_attempts:
        print("RPU extract attempts:")
        for att in pre_rpu_attempts:
            print(
                f"- map={att.get('map')} size={att.get('rpu_size')} "
                f"profile={att.get('dovi_profile')} el={att.get('el_type')}"
            )

    _print_header("TEST 0 - all video streams (ffprobe)")
    _print_cmd(cmd_all_v)
    print(f"Return code: {rc_all_v}")
    if out_all_v.strip():
        print(_pretty_json_or_raw(out_all_v))
    if err_all_v.strip():
        print("\nSTDERR:")
        print(err_all_v)
    if len(video_streams) <= 1:
        print("\nNOTE: Only one video stream found. True DT-DL usually has 2 HEVC video tracks.")
        print("If this file is labeled DT-DL FEL but has 1 stream and no RPU, the label is likely wrong.")

    # 1) ffprobe stream details (v:0 kept for compatibility)
    cmd1 = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries",
        "stream=codec_name,codec_long_name,profile,color_transfer,color_primaries,color_space,side_data_list:stream_tags",
        "-of", "json", path
    ]
    _print_header("TEST 1 - ffprobe stream + side_data (v:0)")
    _print_cmd(cmd1)
    rc1, out1, err1 = _run(cmd1)
    print(f"Return code: {rc1}")
    if out1.strip():
        print(_pretty_json_or_raw(out1))
    if err1.strip():
        print("\nSTDERR:")
        print(err1)

    # 2) ffprobe filtered DOVI side-data on ALL video streams
    _print_header("TEST 2 - ffprobe DOVI side_data (all video streams)")
    if not out_all_v.strip():
        print("No video streams listed, cannot filter side_data.")
    else:
        print("Filtered DOVI-related entries:")
        dovi_entries = _extract_ffprobe_dovi_side_data_from_streams(video_streams)
        print(json.dumps(dovi_entries, indent=2))

    # 3) MediaInfo key HDR fields for ALL video tracks
    cmd3 = ["mediainfo", "--Output=JSON", path]
    _print_header("TEST 3 - MediaInfo HDR fields (all video tracks)")
    _print_cmd(cmd3)
    rc3, out3, err3 = _run(cmd3)
    print(f"Return code: {rc3}")
    if out3.strip():
        print(json.dumps(pre_mi_tracks or _mediainfo_video_tracks(out3), indent=2))
    if err3.strip():
        print("\nSTDERR:")
        print(err3)

    # 4) direct dovi_tool info
    cmd4 = ["dovi_tool", "info", "-i", path, "-f", "0"]
    _print_header("TEST 4 - direct dovi_tool info")
    _print_cmd(cmd4)
    rc4, out4, err4 = _run(cmd4)
    print(f"Return code: {rc4}")
    if out4.strip():
        print(out4)
    if err4.strip():
        print("\nSTDERR:")
        print(err4)

    _print_header("TEST 4B - ffmpeg -> dovi_tool extract-rpu per video map")
    for att in pre_rpu_attempts:
        print(
            f"map={att.get('map')}: extract_rc={att.get('extract_rc')} "
            f"rpu_size={att.get('rpu_size')} profile={att.get('dovi_profile')} el={att.get('el_type')}"
        )
        if att.get("extract_stderr"):
            print(f"  extract stderr: {att['extract_stderr']}")
        if att.get("info_stderr"):
            print(f"  info stderr: {att['info_stderr']}")

    # Re-run default path with full command echo for parity with older logs.
    rpu_file = os.path.join(tempfile.gettempdir(), f"debug_rpu_{os.getpid()}.bin")
    try:
        ffmpeg_cmd = ["ffmpeg", "-i", path, "-c:v", "copy", "-to", "2", "-f", "hevc", "-y", "-"]
        extract_cmd = ["dovi_tool", "extract-rpu", "-", "-o", rpu_file]
        _print_cmd(ffmpeg_cmd)
        _print_cmd(extract_cmd)
        rc, size, out, err = _extract_rpu_for_video_map(path, None, rpu_file)
        print(f"extract-rpu return code: {rc}")
        if out.strip():
            print("\nextract-rpu STDOUT:")
            print(out)
        if err.strip():
            print("\nextract-rpu STDERR:")
            print(err)
        if rc == 0 and size > 0:
            cmd4b = ["dovi_tool", "info", "-i", rpu_file, "-f", "0"]
            _print_cmd(cmd4b)
            rc4b, out4b, err4b = _run(cmd4b)
            print(f"info return code: {rc4b}")
            if out4b.strip():
                print(out4b)
            if err4b.strip():
                print("\ninfo STDERR:")
                print(err4b)
        else:
            print(f"No usable RPU extracted from default map (size={size} bytes).")
    finally:
        if os.path.exists(rpu_file):
            try:
                os.remove(rpu_file)
            except Exception:
                pass


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 debug_deep.py /path/to/video.mkv")
        sys.exit(1)
    run_deep_debug(sys.argv[1])
