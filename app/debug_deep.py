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


def _extract_ffprobe_dovi_side_data(ffprobe_json_text: str) -> list[dict[str, Any]]:
    try:
        data = json.loads(ffprobe_json_text)
    except Exception:
        return []
    streams = data.get("streams") or []
    if not streams:
        return []
    side_data = streams[0].get("side_data_list") or []
    out = []
    for entry in side_data:
        blob = json.dumps(entry).lower()
        side_type = str(entry.get("side_data_type", "")).lower()
        if "dovi" in blob or "dolby vision" in blob or "dovi" in side_type:
            out.append(entry)
    return out


def _extract_mediainfo_hdr_fields(mi_json_text: str) -> dict[str, Any]:
    keys = [
        "Format",
        "CodecID",
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
        return {"_parse_error": "Could not parse MediaInfo JSON", "_raw": mi_json_text}
    tracks = (data.get("media") or {}).get("track") or []
    video = next((t for t in tracks if t.get("@type") == "Video"), {})
    return {k: video.get(k) for k in keys}


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
    if "HDR10" in compat:
        return "HDR10"
    return None


def _parse_dovi_tool_info_json(out_text: str) -> dict[str, Any] | None:
    start = out_text.find("{")
    if start == -1:
        return None
    try:
        return json.loads(out_text[start:])
    except Exception:
        return None


def _build_interpretation(mi_fields: dict[str, Any], dovi_tool_data_4b: dict[str, Any] | None) -> dict[str, Any]:
    detected_profile = None
    detected_el = None
    detected_compat = None
    confidence = "low"
    evidence: list[str] = []

    if dovi_tool_data_4b:
        raw_prof = dovi_tool_data_4b.get("dovi_profile")
        raw_el = dovi_tool_data_4b.get("el_type")
        if raw_prof is not None:
            detected_profile = str(raw_prof)
            evidence.append(f"RPU parse profile={detected_profile}")
        if raw_el:
            detected_el = str(raw_el)
            evidence.append(f"RPU parse el_type={detected_el}")
        confidence = "high"

    mi_profile = _parse_profile_from_mi_fields(mi_fields) if mi_fields else None
    mi_compat = _compat_from_mi_fields(mi_fields) if mi_fields else None
    if mi_profile:
        if not detected_profile:
            detected_profile = mi_profile
        evidence.append(f"MediaInfo profile hint={mi_profile}")
        if confidence != "high":
            confidence = "medium"
    if mi_compat:
        detected_compat = mi_compat
        evidence.append(f"MediaInfo compatibility={mi_compat}")

    # Apply analyzer-like normalization for P8/P10 when only base profile is known.
    if detected_profile in ("8", "10") and mi_compat:
        if mi_compat == "HLG":
            detected_profile = f"{detected_profile}.4"
        elif mi_compat in ("HDR10", "HDR10+"):
            detected_profile = f"{detected_profile}.1"

    detected_format = "dovi" if detected_profile else "unknown"
    return {
        "format": detected_format,
        "profile": detected_profile,
        "el_type": detected_el,
        "compatibility": detected_compat,
        "confidence": confidence,
        "evidence": evidence,
    }


def run_deep_debug(path: str) -> None:
    mi_fields: dict[str, Any] = {}
    dovi_tool_data_4b: dict[str, Any] | None = None

    print(f"Analyzing file: {path}")
    if not os.path.exists(path):
        print("ERROR: File not found")
        return

    # Precompute interpretation summary first so users see it before raw test output.
    pre_mi_fields: dict[str, Any] = {}
    pre_dovi_tool_data_4b: dict[str, Any] | None = None
    rc_mi_pre, out_mi_pre, _ = _run(["mediainfo", "--Output=JSON", path])
    if rc_mi_pre == 0 and out_mi_pre.strip():
        pre_mi_fields = _extract_mediainfo_hdr_fields(out_mi_pre)

    rpu_file_pre = os.path.join(tempfile.gettempdir(), f"debug_rpu_pre_{os.getpid()}.bin")
    try:
        if os.path.exists(rpu_file_pre):
            os.remove(rpu_file_pre)
        ffmpeg_cmd_pre = ["ffmpeg", "-i", path, "-c:v", "copy", "-to", "2", "-f", "hevc", "-y", "-"]
        extract_cmd_pre = ["dovi_tool", "extract-rpu", "-", "-o", rpu_file_pre]
        p1_pre = subprocess.Popen(ffmpeg_cmd_pre, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        p2_pre = subprocess.run(extract_cmd_pre, stdin=p1_pre.stdout, capture_output=True, text=True)
        if p1_pre.stdout:
            p1_pre.stdout.close()
        try:
            p1_pre.wait(timeout=30)
        except Exception:
            pass
        if p2_pre.returncode == 0 and os.path.exists(rpu_file_pre) and os.path.getsize(rpu_file_pre) > 0:
            rc_info_pre, out_info_pre, _ = _run(["dovi_tool", "info", "-i", rpu_file_pre, "-f", "0"])
            if rc_info_pre == 0 and out_info_pre.strip():
                pre_dovi_tool_data_4b = _parse_dovi_tool_info_json(out_info_pre)
    finally:
        if os.path.exists(rpu_file_pre):
            try:
                os.remove(rpu_file_pre)
            except Exception:
                pass

    summary = _build_interpretation(pre_mi_fields, pre_dovi_tool_data_4b)
    _print_header("INTERPRETATION SUMMARY")
    print(f"Detected format: {summary['format']}")
    print(f"Detected profile: {summary['profile'] or 'unknown'}")
    print(f"Detected EL type: {summary['el_type'] or 'unknown'}")
    print(f"Base compatibility: {summary['compatibility'] or 'unknown'}")
    print(f"Confidence: {summary['confidence']}")
    evidence = summary.get("evidence") or []
    if evidence:
        print("Evidence:")
        for ev in evidence:
            print(f"- {ev}")

    # 1) ffprobe stream details
    cmd1 = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries",
        "stream=codec_name,codec_long_name,profile,color_transfer,color_primaries,color_space,side_data_list:stream_tags",
        "-of", "json", path
    ]
    _print_header("TEST 1 - ffprobe stream + side_data")
    _print_cmd(cmd1)
    rc1, out1, err1 = _run(cmd1)
    print(f"Return code: {rc1}")
    if out1.strip():
        print(_pretty_json_or_raw(out1))
    if err1.strip():
        print("\nSTDERR:")
        print(err1)

    # 2) ffprobe filtered DOVI side-data only
    _print_header("TEST 2 - ffprobe DOVI side_data only")
    if not out1.strip():
        print("No output from TEST 1, cannot filter side_data.")
    else:
        try:
            raw = json.loads(out1)
            streams = raw.get("streams") or []
            side_data = streams[0].get("side_data_list") if streams else []
            print("Raw side_data_list:")
            print(json.dumps(side_data or [], indent=2))
        except Exception:
            pass
        print("\nFiltered DOVI-related entries:")
        dovi_entries = _extract_ffprobe_dovi_side_data(out1)
        print(json.dumps(dovi_entries, indent=2))

    # 3) MediaInfo key HDR fields
    cmd3 = ["mediainfo", "--Output=JSON", path]
    _print_header("TEST 3 - MediaInfo HDR fields")
    _print_cmd(cmd3)
    rc3, out3, err3 = _run(cmd3)
    print(f"Return code: {rc3}")
    if out3.strip():
        mi_fields = _extract_mediainfo_hdr_fields(out3)
        print(json.dumps(mi_fields, indent=2))
    if err3.strip():
        print("\nSTDERR:")
        print(err3)

    # 4) direct dovi_tool info + analyzer-equivalent extraction path
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

    _print_header("TEST 4B - ffmpeg -> dovi_tool extract-rpu -> dovi_tool info (analyzer path)")
    rpu_file = os.path.join(tempfile.gettempdir(), f"debug_rpu_{os.getpid()}.bin")
    try:
        if os.path.exists(rpu_file):
            os.remove(rpu_file)
        ffmpeg_cmd = ["ffmpeg", "-i", path, "-c:v", "copy", "-to", "2", "-f", "hevc", "-y", "-"]
        extract_cmd = ["dovi_tool", "extract-rpu", "-", "-o", rpu_file]
        _print_cmd(ffmpeg_cmd)
        _print_cmd(extract_cmd)
        p1 = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        p2 = subprocess.run(extract_cmd, stdin=p1.stdout, capture_output=True, text=True)
        if p1.stdout:
            p1.stdout.close()
        try:
            p1.wait(timeout=30)
        except Exception:
            pass
        print(f"extract-rpu return code: {p2.returncode}")
        if p2.stdout.strip():
            print("\nextract-rpu STDOUT:")
            print(p2.stdout)
        if p2.stderr.strip():
            print("\nextract-rpu STDERR:")
            print(p2.stderr)
        if p2.returncode == 0 and os.path.exists(rpu_file) and os.path.getsize(rpu_file) > 0:
            cmd4b = ["dovi_tool", "info", "-i", rpu_file, "-f", "0"]
            _print_cmd(cmd4b)
            rc4b, out4b, err4b = _run(cmd4b)
            print(f"info return code: {rc4b}")
            if out4b.strip():
                print(out4b)
                dovi_tool_data_4b = _parse_dovi_tool_info_json(out4b)
            if err4b.strip():
                print("\ninfo STDERR:")
                print(err4b)
        else:
            size = os.path.getsize(rpu_file) if os.path.exists(rpu_file) else 0
            print(f"No usable RPU extracted (size={size} bytes).")
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