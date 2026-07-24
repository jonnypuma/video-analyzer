"""HTTP routes (Flask blueprint `video_analyzer.core.bp`)."""
from video_analyzer.core import bp


def register_routes(app):
    app.register_blueprint(bp)
