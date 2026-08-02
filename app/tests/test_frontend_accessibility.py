"""Frontend accessibility smoke checks for the dashboard shell."""
from __future__ import annotations


def test_dashboard_has_keyboard_and_live_progress_hooks(analyzer_mod):
    response = analyzer_mod.app.test_client().get("/")
    assert response.status_code == 200
    html = response.text
    assert 'class="skip-link"' in html
    assert 'id="library-content"' in html
    assert 'aria-live="polite"' in html
    assert 'role="progressbar"' in html
    assert 'aria-expanded="false"' in html
