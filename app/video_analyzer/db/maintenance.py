"""DB cleanup and leftover RPU temp-file maintenance."""
from __future__ import annotations

import glob
import json
import os
import tempfile

from video_analyzer.db.connection import get_db
from video_analyzer import state as va_state


def cleanup_old_rpu_files() -> None:
    """Clean up any leftover RPU temporary files from previous runs."""
    from video_analyzer.core import log_debug

    try:
        temp_dir = tempfile.gettempdir()
        for pattern in ['dovi_*_rpu.bin', 'temp_*_rpu.bin']:
            for temp_file in glob.glob(os.path.join(temp_dir, pattern)):
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                        if va_state.DEBUG_MODE:
                            log_debug(f"Cleaned up leftover RPU temp file: {temp_file}", "DEBUG")
                except OSError:
                    pass  # File may have been deleted already or is in use
    except (OSError, PermissionError) as e:
        if va_state.DEBUG_MODE:
            log_debug(f"Error cleaning up old RPU files: {e}", "WARNING")


def perform_cleanup_db(delete: bool) -> int:
    from video_analyzer.core import get_mount_status, is_path_within_root

    mounts = get_mount_status()
    online_vols = set(mounts.keys())
    with get_db() as conn:
        settings = dict(conn.execute("SELECT key, value FROM settings").fetchall())
        scan_folders = []
        try:
            scan_folders = json.loads(settings.get('scan_folders', '[]') or '[]')
        except (json.JSONDecodeError, TypeError):
            scan_folders = []
        allowed_bases = []
        if isinstance(scan_folders, list):
            for entry in scan_folders:
                if entry.get('muted'):
                    continue
                vol_name = (entry.get('volume') or '').strip()
                if not vol_name or vol_name not in mounts:
                    continue
                base = mounts.get(vol_name)
                rel_path = (entry.get('path') or '').strip()
                if rel_path:
                    candidate = rel_path
                    if not os.path.isabs(candidate):
                        candidate = os.path.join(base, rel_path.lstrip('/\\'))
                else:
                    candidate = base
                base_real = os.path.realpath(base)
                target_real = os.path.realpath(candidate)
                if is_path_within_root(target_real, base_real) and os.path.isdir(target_real):
                    allowed_bases.append(target_real)

        rows = conn.execute("SELECT full_path, source_vol FROM videos").fetchall()
        to_delete = []
        for row in rows:
            full_path = row["full_path"]
            vol = row["source_vol"]
            if vol not in online_vols:
                to_delete.append((full_path,))
                continue
            if allowed_bases:
                try:
                    real_path = os.path.realpath(full_path)
                except OSError:
                    to_delete.append((full_path,))
                    continue
                if not any(is_path_within_root(real_path, base) for base in allowed_bases):
                    to_delete.append((full_path,))
        if delete and to_delete:
            conn.executemany("DELETE FROM videos WHERE full_path=?", to_delete)
    return len(to_delete)
