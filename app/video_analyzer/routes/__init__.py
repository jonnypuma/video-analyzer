"""HTTP routes. Named modules register handlers on blueprint `bp`."""
from video_analyzer.blueprint import bp
from video_analyzer.routes import duplicates, health, scan, settings, videos  # noqa: F401


def register_routes(app):
    app.register_blueprint(bp)
