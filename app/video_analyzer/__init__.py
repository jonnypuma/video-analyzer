"""Video Analyzer application package."""
from __future__ import annotations

import hmac
import os
import secrets

from flask import Flask, jsonify, redirect, render_template, request, session, url_for


def _load_or_create_secret(output_dir: str) -> bytes:
    env = (os.environ.get("SECRET_KEY") or "").strip()
    if env:
        return env.encode("utf-8")
    path = os.path.join(output_dir, ".flask_secret")
    try:
        if os.path.exists(path):
            data = open(path, "rb").read().strip()
            if data:
                return data
    except OSError:
        pass
    secret = secrets.token_hex(32).encode("utf-8")
    try:
        os.makedirs(output_dir, exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(secret)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    except OSError:
        pass
    return secret


def create_app() -> Flask:
    """Create Flask app, register routes blueprint, run startup init unless testing."""
    import video_analyzer.core as core
    from video_analyzer.routes import register_routes

    app = Flask(
        __name__,
        template_folder=os.path.join(core.BASE_DIR, "templates"),
        static_folder=os.path.join(core.BASE_DIR, "static"),
    )
    app.secret_key = _load_or_create_secret(core.OUTPUT_DIR)
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

    def credentials() -> tuple[str, str] | None:
        configured = (os.environ.get("BASIC_AUTH") or "").strip()
        if not configured or ":" not in configured:
            return None
        username, password = configured.split(":", 1)
        if not username:
            return None
        return username, password

    def ensure_csrf_token() -> str:
        token = session.get("csrf_token")
        if not token:
            token = secrets.token_urlsafe(32)
            session["csrf_token"] = token
        return token

    def csrf_is_valid() -> bool:
        expected = session.get("csrf_token") or ""
        provided = (
            request.headers.get("X-CSRF-Token")
            or request.headers.get("X-CSRFToken")
            or request.form.get("csrf_token")
            or ""
        )
        if not provided:
            payload = request.get_json(silent=True)
            if isinstance(payload, dict):
                provided = str(payload.get("csrf_token") or "")
        if not expected or not provided:
            return False
        return hmac.compare_digest(str(provided), str(expected))

    @app.before_request
    def require_login_and_csrf():
        ensure_csrf_token()
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            if request.path != "/api/health" and not csrf_is_valid():
                if request.endpoint == "login":
                    return render_template(
                        "login.html",
                        error="Session expired. Please try again.",
                        next_url=request.args.get("next") or "/",
                        csrf_token=session.get("csrf_token", ""),
                    ), 403
                return jsonify({"status": "error", "message": "Invalid CSRF token"}), 403
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

    @app.after_request
    def add_csrf_header(response):
        token = session.get("csrf_token")
        if token:
            response.headers["X-CSRF-Token"] = token
        return response

    @app.context_processor
    def inject_auth_template_vars():
        configured = credentials()
        return {
            "auth_enabled": configured is not None,
            "csrf_token": session.get("csrf_token", ""),
        }

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
        return render_template(
            "login.html",
            error=error,
            next_url=next_url,
            csrf_token=session.get("csrf_token", ""),
        )

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
