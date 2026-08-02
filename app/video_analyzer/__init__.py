"""Video Analyzer application package."""
from __future__ import annotations

import os
import hashlib
import hmac

from flask import Flask, jsonify, redirect, render_template, request, session, url_for


def create_app() -> Flask:
    """Create Flask app, register routes blueprint, run startup init unless testing."""
    import video_analyzer.core as core
    from video_analyzer.routes import register_routes

    app = Flask(
        __name__,
        template_folder=os.path.join(core.BASE_DIR, "templates"),
        static_folder=os.path.join(core.BASE_DIR, "static"),
    )
    basic_auth = (os.environ.get("BASIC_AUTH") or "").strip()
    app.secret_key = hashlib.sha256(
        ("video-analyzer-session:" + basic_auth).encode("utf-8")
    ).digest()

    def credentials() -> tuple[str, str] | None:
        configured = (os.environ.get("BASIC_AUTH") or "").strip()
        if not configured or ":" not in configured:
            return None
        username, password = configured.split(":", 1)
        if not username:
            return None
        return username, password

    @app.before_request
    def require_login():
        configured = credentials()
        if configured is None:
            return None
        if request.endpoint in {"login", "static"} or request.path == "/api/health":
            return None
        if session.get("authenticated") is True:
            return None
        if request.path.startswith("/api/"):
            return jsonify({"status": "error", "message": "Authentication required"}), 401
        return redirect(url_for("login", next=request.full_path))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        configured = credentials()
        if configured is None:
            return redirect(url_for("main.index"))
        next_url = request.args.get("next") or request.form.get("next") or "/"
        if not next_url.startswith("/") or next_url.startswith("//"):
            next_url = "/"
        error = None
        if request.method == "POST":
            username = request.form.get("username", "")
            password = request.form.get("password", "")
            if (
                hmac.compare_digest(username, configured[0])
                and hmac.compare_digest(password, configured[1])
            ):
                session["authenticated"] = True
                session["username"] = configured[0]
                return redirect(next_url)
            error = "That username or password is not correct."
        return render_template("login.html", error=error, next_url=next_url)

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    register_routes(app)

    testing = (os.environ.get("VIDEO_ANALYZER_TESTING") or "").strip().lower() in ("1", "true", "yes")
    if not testing:
        core.init_db()
        core.restore_scan_schedule_from_settings()
        core.cleanup_old_rpu_files()

    return app
