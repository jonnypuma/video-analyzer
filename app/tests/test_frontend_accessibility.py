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


def test_anomaly_modal_and_filename_details_hooks(analyzer_mod):
    html = analyzer_mod.app.test_client().get("/").text
    assert 'id="anomaly-modal"' in html
    assert 'id="anomaly-modal-flags"' in html
    table_js = analyzer_mod.app.test_client().get("/static/js/table.js").text
    assert "showDetailsByIndex(" in table_js
    assert "showAnomalyDetails(" in table_js
    assert "showDetails('${rowJson}')" not in table_js
    modals_js = analyzer_mod.app.test_client().get("/static/js/modals.js").text
    assert "function showAnomalyDetails" in modals_js
    assert "low_bitrate_4k" in modals_js
    assert "legacy_codec_4k" in modals_js


def test_table_end_pad_precedes_delete_column(analyzer_mod):
    html = analyzer_mod.app.test_client().get("/").text
    pad_at = html.find('class="end-pad"')
    del_at = html.find('class="col-del"')
    assert pad_at != -1 and del_at != -1
    assert pad_at < del_at
    css = analyzer_mod.app.test_client().get("/static/css/app.css").text
    assert "th.end-pad, td.end-pad" in css
    assert "width: 15px !important;" in css
    core_js = analyzer_mod.app.test_client().get("/static/js/core.js").text
    assert "TABLE_END_PAD_PX = 15" in core_js


def test_scan_js_waits_for_progress_before_treating_idle_as_complete(analyzer_mod):
    js = analyzer_mod.app.test_client().get("/static/js/scan.js").text
    assert "pollSawScanning" in js
    assert "if (!res.ok)" in js
    assert "preclaimed" not in js
    assert "setTimeout(poll, 250)" in js
