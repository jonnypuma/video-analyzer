"""Video Analyzer application package."""
from __future__ import annotations

import os

from flask import Flask


def create_app() -> Flask:
    """Create Flask app, register routes blueprint, run startup init unless testing."""
    import video_analyzer.core as core
    from video_analyzer.routes import register_routes

    app = Flask(
        __name__,
        template_folder=os.path.join(core.BASE_DIR, "templates"),
        static_folder=os.path.join(core.BASE_DIR, "static"),
    )
    register_routes(app)

    testing = (os.environ.get("VIDEO_ANALYZER_TESTING") or "").strip().lower() in ("1", "true", "yes")
    if not testing:
        core.init_db()
        core.restore_scan_schedule_from_settings()
        core.cleanup_old_rpu_files()

    return app
