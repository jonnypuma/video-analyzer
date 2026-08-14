// --- LOGS & SETTINGS ---
async function updateSettingsUI() {
    if (settingsLoading) return;
    settingsLoading = true;
    const wasLoaded = settingsLoaded;
    try {
        const res = await fetch('/api/settings');
        const data = await res.json();
        if(document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'SELECT') {
            document.getElementById('sched-mode').value = data.scan_mode || 'manual';
            if(data.scan_mode === 'daily') document.getElementById('sched-val-time').value = data.scan_value || '';
            if(data.scan_mode === 'interval') document.getElementById('sched-val-hours').value = data.scan_value || '';
            if(data.scan_mode === 'weekly') {
                const parts = parseSchedValueParts(data.scan_value);
                document.getElementById('sched-val-week').value = parts.primary || 'mon';
                document.getElementById('sched-val-time').value = parts.time || '03:00';
            }
            if(data.scan_mode === 'monthly') {
                const parts = parseSchedValueParts(data.scan_value);
                document.getElementById('sched-val-day').value = parts.primary || '1';
                document.getElementById('sched-val-time').value = parts.time || '03:00';
            }

            document.getElementById('skip-words').value = data.skip_words || '';
            document.getElementById('min-size').value = data.min_size_mb || '50';
            if (data.log_limit) document.getElementById('log-limit').value = data.log_limit;
            if (data.debug_mode !== undefined) {
                const debugEl = document.getElementById('chk-debug');
                if (debugEl) debugEl.checked = data.debug_mode === 'true' || data.debug_mode === true;
            }
            if (data.threads) document.getElementById('scan-threads').value = data.threads;
            if (data.refresh_interval) document.getElementById('scan-refresh').value = data.refresh_interval;
            if (data.notif_style) document.getElementById('notif-style').value = data.notif_style;
            if (data.batch_size) document.getElementById('batch-size').value = data.batch_size;
            if (data.rpu_fel_threshold) document.getElementById('rpu-threshold').value = data.rpu_fel_threshold;
            if (data.force_rescan !== undefined) {
                const forceMain = document.getElementById('chk-force');
                const forceModal = document.getElementById('chk-force-rescan');
                const checked = data.force_rescan === 'true' || data.force_rescan === true;
                if (forceMain) forceMain.checked = checked;
                if (forceModal) forceModal.checked = checked;
            }
            if (data.scan_extras !== undefined) {
                const extrasEl = document.getElementById('chk-scan-extras');
                if (extrasEl) extrasEl.checked = data.scan_extras === 'true' || data.scan_extras === true;
            }
            if (data.remove_missing_from_db !== undefined) {
                const rmEl = document.getElementById('chk-remove-missing');
                if (rmEl) rmEl.checked = data.remove_missing_from_db === 'true' || data.remove_missing_from_db === true;
            }
            if (data.duplicate_check_on_scan !== undefined) {
                const dupEl = document.getElementById('chk-dup-check-scan');
                if (dupEl) dupEl.checked = data.duplicate_check_on_scan === 'true' || data.duplicate_check_on_scan === true;
            }
            settingsCache = data;
            if (data.column_widths) {
                try {
                    const parsed = JSON.parse(data.column_widths);
                    if (parsed && typeof parsed === 'object' && Object.keys(parsed).length > 0) {
                        columnWidths = parsed;
                        localStorage.setItem('column_widths', JSON.stringify(parsed));
                    }
                } catch (e) {}
            }
            if (data.visible_cols) applyVisibleCols(data.visible_cols, true);
            if (data.column_order) { try { savedColumnOrder = JSON.parse(data.column_order); applyColumnOrder(savedColumnOrder); } catch (e) {} }
            applyStoredColumnWidths();
            requestAnimationFrame(() => { syncTableColgroup(); updateStickyHeader(); });
            if (data.sort_order && !sortInitialized) {
                setSortOrder(data.sort_order);
                sortInitialized = true;
            }
            if (data.scan_folders) {
                try {
                    const parsed = JSON.parse(data.scan_folders);
                    scanFolders = Array.isArray(parsed) ? parsed : [];
                } catch (e) {
                    scanFolders = [];
                }
            }
            updateScanButtonLabel();
            updateScanProfiles();
            
            // Never auto-calc - only user can change widths via manual resize or Reset
            toggleSchedInput();
            settingsLoaded = true;
        }
    } catch (e) { console.error("Failed to load settings", e); }
    settingsLoading = false;
}

async function triggerPulse(el) { el.classList.remove('pulse-success', 'pulse-error'); void el.offsetWidth; const success = await saveSettings(false); if (success) { el.classList.add('pulse-success'); } else { el.classList.add('pulse-error'); } setTimeout(() => { el.classList.remove('pulse-success', 'pulse-error'); }, 1000); }

async function saveSettings(animateButton = true) {
    let val = "";
    const mode = document.getElementById('sched-mode').value;
    if (mode === 'daily') val = document.getElementById('sched-val-time').value;
    else if (mode === 'interval') val = document.getElementById('sched-val-hours').value;
    else if (mode === 'weekly') {
        const dow = document.getElementById('sched-val-week').value || 'mon';
        const t = document.getElementById('sched-val-time').value || '03:00';
        val = `${dow}|${t}`;
    } else if (mode === 'monthly') {
        const day = document.getElementById('sched-val-day').value || '1';
        const t = document.getElementById('sched-val-time').value || '03:00';
        val = `${day}|${t}`;
    }

    const mediaTypeVal = (() => {
        try { return getMultiselectValue('media-type-filter'); } catch (e) { return ''; }
    })();
    const mediaKey = getMediaTypeKeyFromValue(mediaTypeVal);
    const visibleColsValue = getVisibleCols();
    const columnOrderValue = JSON.stringify(getColumnOrder());
    const currentWidths = getColumnWidths();
    if (!columnWidths) { try { const r = localStorage.getItem('column_widths'); if (r) columnWidths = JSON.parse(r); } catch (e) {} }
    const prevWidths = columnWidths || {};
    const columnWidthsValue = JSON.stringify({ ...prevWidths, ...currentWidths });
    const payload = {
        mode: mode, value: val,
        skip_words: document.getElementById('skip-words').value,
        min_size_mb: document.getElementById('min-size').value,
        log_limit: document.getElementById('log-limit').value,
        debug_mode: document.getElementById('chk-debug').checked,
        threads: document.getElementById('scan-threads').value,
        refresh_interval: document.getElementById('scan-refresh').value,
        visible_cols: visibleColsValue,
        column_widths: columnWidthsValue,
        sort_order: getSortOrder(),
        notif_style: document.getElementById('notif-style').value,
        batch_size: document.getElementById('batch-size').value,
        rpu_fel_threshold: document.getElementById('rpu-threshold').value,
        force_rescan: document.getElementById('chk-force')?.checked || false,
        column_order: columnOrderValue,
        scan_extras: document.getElementById('chk-scan-extras')?.checked || false,
        remove_missing_from_db: document.getElementById('chk-remove-missing')?.checked ?? true,
        duplicate_check_on_scan: document.getElementById('chk-dup-check-scan')?.checked || false
    };
    payload[`visible_cols_${mediaKey}`] = visibleColsValue;
    payload[`column_order_${mediaKey}`] = columnOrderValue;

    try {
        const headers = { 'Content-Type': 'application/json' };
        const csrf = (typeof getCsrfToken === 'function' ? getCsrfToken() : '') || '';
        if (csrf) headers['X-CSRF-Token'] = csrf;
        const res = await fetch('/api/settings', { method: 'POST', headers, body: JSON.stringify(payload), credentials: 'same-origin' });
        if (res.ok) {
            if (animateButton) animateSuccess('btn-save', 'Save', 'Saved');
            showToast("Settings saved successfully!");
            updateLogs();
            return true;
        } else {
            let detail = "Server returned " + res.status;
            try {
                const errBody = await res.json();
                if (errBody && errBody.message) detail += ": " + errBody.message;
            } catch (parseErr) { /* keep status-only message */ }
            throw new Error(detail);
        }
    } catch (e) {
        console.error("Save failed:", e);
        if (animateButton) animateFailure('btn-save', 'Save');
        setTimeout(updateLogs, 500);
        return false;
    }
}

async function updateScanProfiles() {
    const select = document.getElementById('scan-profile');
    if (!select) return;
    try {
        const response = await fetch('/api/scan_profiles');
        const data = await response.json();
        select.innerHTML = '<option value="">Default</option>';
        (data.profiles || []).forEach(profile => {
            const option = document.createElement('option');
            option.value = profile.name;
            option.textContent = profile.name;
            option.dataset.settings = JSON.stringify(profile.settings || {});
            select.appendChild(option);
        });
    } catch (e) { console.error('Failed to load scan profiles', e); }
}

function currentScanProfileSettings() {
    return {
        threads: document.getElementById('scan-threads')?.value,
        skip_words: document.getElementById('skip-words')?.value,
        min_size_mb: document.getElementById('min-size')?.value,
        scan_extras: document.getElementById('chk-scan-extras')?.checked || false,
        remove_missing_from_db: document.getElementById('chk-remove-missing')?.checked ?? true,
        duplicate_check_on_scan: document.getElementById('chk-dup-check-scan')?.checked || false
    };
}

async function saveScanProfile() {
    const name = window.prompt('Profile name');
    if (!name || !name.trim()) return;
    const response = await fetch('/api/scan_profiles', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name: name.trim(), settings: currentScanProfileSettings()})
    });
    if (response.ok) {
        await updateScanProfiles();
        document.getElementById('scan-profile').value = name.trim();
        showToast('Scan profile saved');
    }
}

function loadScanProfile() {
    const option = document.getElementById('scan-profile')?.selectedOptions[0];
    if (!option || !option.value) return;
    try {
        const settings = JSON.parse(option.dataset.settings || '{}');
        if (settings.threads) document.getElementById('scan-threads').value = settings.threads;
        if (settings.skip_words !== undefined) document.getElementById('skip-words').value = settings.skip_words;
        if (settings.min_size_mb !== undefined) document.getElementById('min-size').value = settings.min_size_mb;
        ['scan_extras', 'remove_missing_from_db', 'duplicate_check_on_scan'].forEach(key => {
            const id = {scan_extras:'chk-scan-extras', remove_missing_from_db:'chk-remove-missing', duplicate_check_on_scan:'chk-dup-check-scan'}[key];
            if (settings[key] !== undefined) document.getElementById(id).checked = Boolean(settings[key]);
        });
        showToast('Scan profile loaded');
    } catch (e) { console.error('Invalid scan profile', e); }
}

async function updateLogs() {
    try {
        const consoleElem = document.getElementById('debug-console');
        if (!consoleElem) return;
        const selection = window.getSelection();
        const hasSelection = selection && !selection.isCollapsed && (
            consoleElem.contains(selection.anchorNode) || consoleElem.contains(selection.focusNode)
        );
        if (hasSelection) return; // Don't disrupt text selection
        const wasAtBottom = (consoleElem.scrollTop + consoleElem.clientHeight) >= (consoleElem.scrollHeight - 8);

        const res = await fetch('/api/logs');
        const logs = await res.json();
        consoleElem.innerHTML = logs.map(l => {
            // Color based ONLY on the script's own log level tags and markers,
            // never on loose words that could appear in filenames/episode titles/NFO data
            let cls = 'log-default';
            if (/\[SUCCESS\]/.test(l)) cls = 'log-success';
            else if (/\(error:\s*none\b/i.test(l)) cls = 'log-success';
            else if (/\] \[WARNING\]/.test(l)) cls = 'log-warn';
            else if (/\] \[ERROR\]|\] \[CRITICAL\]|\[FAILURE\]/.test(l)) cls = 'log-err';
            else if (/scanned successfully|saved to db|finished scanning/i.test(l)) cls = 'log-success';
            // If line has failed=N where N > 0, highlight just that part red
            let text = escHtml(l);
            if (cls !== 'log-err') {
                text = text.replace(/failed=([1-9]\d*)/gi, '<span class="log-err">failed=$1</span>');
            }
            return `<span class="${cls}">${text}</span>`;
        }).join('<br>');
        if (wasAtBottom) consoleElem.scrollTop = consoleElem.scrollHeight;
    } catch (e) {}
}

function toggleReportSection(id) {
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.toggle('open');
}

async function updateReportDetails() {
    try {
        const res = await fetch('/api/failures?limit=200');
        const data = await res.json();
        const failList = document.getElementById('report-fail-list');
        const warnList = document.getElementById('report-warn-list');
        if (!failList || !warnList) return;
        const failures = data.failures || [];
        const warnings = data.warnings || [];
        const formatEntry = (entry, cls) => {
            const vol = entry.volume || 'Unknown';
            const path = entry.path || '';
            const name = entry.name || '';
            const msg = entry.message || '';
            const label = name ? `${name}` : (path || 'Unknown');
            const suffix = path ? ` | ${path}` : '';
            return `<div class="${cls}">${escHtml(vol)} - ${escHtml(label)}${escHtml(suffix)}${msg ? `: ${escHtml(msg)}` : ''}</div>`;
        };
        if (failures.length) {
            failList.innerHTML = `<div class="fail">Volume - Filename | Path: Error</div>` +
                failures.map(f => formatEntry(f, 'fail')).join('');
        } else {
            failList.innerHTML = '<div class="fail">No errors.</div>';
        }
        warnList.innerHTML = warnings.length
            ? warnings.map(w => formatEntry(w, 'warn')).join('')
            : '<div class="warn">No warnings.</div>';
    } catch (e) {
        // Silent fail to avoid blocking report modal
    }
}

let scanHistoryEntries = [];

async function openScanHistory() {
    const modal = document.getElementById('scan-history-modal');
    const list = document.getElementById('scan-history-list');
    if (!modal || !list) return;
    list.innerHTML = '<div class="warn">Loading...</div>';
    modal.style.display = 'block';
    try {
        const res = await fetch('/api/scan_history');
        const data = await res.json();
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || 'Failed to load history');
        }
        const entries = Array.isArray(data.entries) ? data.entries : [];
        scanHistoryEntries = entries;
        if (!entries.length) {
            list.innerHTML = '<div class="warn">No history yet.</div>';
            return;
        }
        list.innerHTML = entries.map((entry, idx) => {
            const ts = entry.timestamp || 'Unknown';
            const status = (entry.status || 'complete').toUpperCase();
            const dur = entry.duration || '--';
            const scanned = entry.scanned ?? 0;
            const added = entry.new ?? 0;
            const failed = entry.failed ?? 0;
            const warnings = entry.warnings ?? 0;
            const mode = entry.scan_mode || 'all';
            const folder = entry.scan_folder ? ` | ${entry.scan_folder}` : '';
            const vols = Array.isArray(entry.target_vols) && entry.target_vols.length
                ? ` | ${entry.target_vols.join(', ')}`
                : '';
            const cls = status === 'ABORTED' ? 'fail' : 'warn';
            return `<div class="${cls}" style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
                <div>${escHtml(ts)} | ${escHtml(status)} | ${escHtml(dur)} | scanned ${escHtml(scanned)} | new ${escHtml(added)} | failed ${escHtml(failed)} | warn ${escHtml(warnings)} | ${escHtml(mode)}${escHtml(vols)}${escHtml(folder)}</div>
                <button class="btn-grey" style="flex:0 0 auto; padding:4px 10px; font-size:0.75em;" onclick="openScanHistoryReport(${idx})">View report</button>
            </div>`;
        }).join('');
    } catch (e) {
        list.innerHTML = `<div class="fail">Failed to load history.</div>`;
    }
}

function openScanHistoryReport(index) {
    const entry = scanHistoryEntries[index];
    if (!entry) return;
    const statusEl = document.getElementById('rep-status');
    if (statusEl) {
        if (entry.status === 'aborted') {
            statusEl.innerText = "ABORTED";
            statusEl.style.color = "#e74c3c";
        } else {
            statusEl.innerText = "COMPLETE";
            statusEl.style.color = "#3498db";
        }
    }
    document.getElementById('rep-scanned').innerText = entry.scanned ?? 0;
    document.getElementById('rep-new').innerText = entry.new ?? 0;
    const repRemovedHist = document.getElementById('rep-removed');
    if (repRemovedHist) repRemovedHist.innerText = entry.removed ?? 0;
    const repRemovedNoteHist = document.getElementById('rep-removed-note');
    if (repRemovedNoteHist) repRemovedNoteHist.innerText = (entry.removed > 0 && entry.remove_missing_from_db === false) ? '(marked missing)' : '';
    document.getElementById('rep-failed').innerText = entry.failed ?? 0;
    document.getElementById('rep-warn').innerText = entry.warnings ?? 0;
    const repDupHist = document.getElementById('rep-dup');
    if (repDupHist) repDupHist.innerText = entry.duplicates ?? 0;
    const repDupNoteHist = document.getElementById('rep-dup-note');
    if (repDupNoteHist) repDupNoteHist.innerText = ((entry.duplicate_groups ?? 0) > 0) ? `(${entry.duplicate_groups} groups)` : '';
    document.getElementById('rep-time').innerText = formatDuration(entry.duration || '0s');
    document.getElementById('rep-date').innerText = entry.timestamp || '--';
    const offlineBox = document.getElementById('rep-offline-box');
    if (offlineBox) offlineBox.style.display = 'none';
    const failList = document.getElementById('report-fail-list');
    const warnList = document.getElementById('report-warn-list');
    if (failList) failList.innerHTML = '<div class="fail">Details not available for history.</div>';
    if (warnList) warnList.innerHTML = '<div class="warn">Details not available for history.</div>';
    document.getElementById('report-modal').style.display = 'block';
}

function closeScanHistory() {
    const modal = document.getElementById('scan-history-modal');
    if (modal) modal.style.display = 'none';
}

let exportScope = 'filtered';

function getExportScopeLabel(scope) {
    if (scope === 'all') return 'All';
    if (scope === 'movies') return 'Movies';
    if (scope === 'tv') return 'TV';
    if (scope === 'page') return 'Page';
    return 'Filtered';
}

function updateExportButtonText() {
    const format = document.querySelector('input[name="export-format"]:checked')?.value || 'csv';
    const btn = document.getElementById('btn-export');
    if (btn) {
        const fmtLabel = format === 'json' ? 'JSON' : 'CSV';
        const scopeLabel = getExportScopeLabel(exportScope);
        btn.innerHTML = `<span class="btn-export-content">
            <span class="btn-export-lines">
                <span>Export</span>
                <span>${fmtLabel}</span>
            </span>
            <span class="btn-export-scope">| <span>${scopeLabel}</span></span>
        </span>`;
    }
}

function toggleExportMenu(event) {
    if (event) event.stopPropagation();
    const menu = document.getElementById('export-mode-menu');
    if (!menu) return;
    menu.classList.toggle('active');
}

function selectExportScope(scope) {
    exportScope = scope || 'filtered';
    updateExportButtonText();
    const menu = document.getElementById('export-mode-menu');
    if (menu) menu.classList.remove('active');
}

function exportData() {
    const format = document.querySelector('input[name="export-format"]:checked')?.value || 'csv';
    showToast(format === 'json' ? 'Export started (JSON)' : 'Export started (CSV)');
    if (format === 'json') {
        exportJSON(exportScope);
    } else {
        exportCSV(exportScope);
    }
}

function buildExportFilters(scope) {
    const base = {
        sort: sortCol,
        order: sortOrder
    };
    if (scope === 'all') {
        return base;
    }
    if (scope === 'movies') {
        return { ...base, media_type: 'movie' };
    }
    if (scope === 'tv') {
        return { ...base, media_type: 'tv' };
    }
    const currentFilters = { ...activeFilters };
    currentFilters.search = document.getElementById('search-bar') ? document.getElementById('search-bar').value : '';
    try {
        currentFilters.category = getFormatFilterValue();
    } catch (e) {
        currentFilters.category = '';
    }
    try { currentFilters.volume = getMultiselectValue('vol-filter'); } catch (e) { currentFilters.volume = ''; }
    try { currentFilters.profile = getMultiselectValue('profile-filter'); } catch (e) { currentFilters.profile = ''; }
    try { currentFilters.el = getMultiselectValue('el-filter'); } catch (e) { currentFilters.el = ''; }
    try { currentFilters.container = getMultiselectValue('container-filter'); } catch (e) { currentFilters.container = ''; }
    try { currentFilters.media_type = getMultiselectValue('media-type-filter'); } catch (e) { currentFilters.media_type = ''; }
    try { currentFilters.is_hybrid = getMultiselectValue('hybrid-filter'); } catch (e) { currentFilters.is_hybrid = ''; }
    try { currentFilters.source_hybrid = getMultiselectValue('source-hybrid-filter'); } catch (e) { currentFilters.source_hybrid = ''; }
    try { currentFilters.secondary_hdr = getMultiselectValue('secondary-filter'); } catch (e) { currentFilters.secondary_hdr = ''; }
    try { currentFilters.status = getMultiselectValue('status-filter'); } catch (e) { currentFilters.status = ''; }
    try { currentFilters.nfo_missing = getMultiselectValue('nfo-filter'); } catch (e) { currentFilters.nfo_missing = ''; }
    try { currentFilters.missing = getMultiselectValue('missing-filter'); } catch (e) { currentFilters.missing = ''; }
    try { currentFilters.resolution = getMultiselectValue('res-filter'); } catch (e) { currentFilters.resolution = ''; }
    const sizeFilter = document.getElementById('size-filter-header') ? document.getElementById('size-filter-header').value : '';
    const sizeParsed = parseFilterValue(sizeFilter);
    currentFilters.size_op = sizeParsed.op;
    currentFilters.size_val = sizeParsed.value;
    const bitFilter = document.getElementById('bit-filter-header') ? document.getElementById('bit-filter-header').value : '';
    const bitParsed = parseFilterValue(bitFilter);
    currentFilters.bit_op = bitParsed.op;
    currentFilters.bit_val = bitParsed.value;
    try { currentFilters.audio = getMultiselectValue('audio-filter'); } catch (e) { currentFilters.audio = ''; }
    currentFilters.sort = sortCol;
    currentFilters.order = sortOrder;
    if (scope === 'page') {
        const perPageVal = document.getElementById('per-page-select') ? document.getElementById('per-page-select').value : '50';
        currentFilters.page = currentPage;
        currentFilters.per_page = perPageVal;
    }
    return currentFilters;
}

function exportCSV(scope = 'filtered') { 
    const currentFilters = buildExportFilters(scope);
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(currentFilters)) {
        if (value !== null && value !== undefined) {
            params.append(key, value.toString());
        }
    }
    window.location = '/download_csv?' + params.toString(); 
}

function exportJSON(scope = 'filtered') {
    const currentFilters = buildExportFilters(scope);
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(currentFilters)) {
        if (value !== null && value !== undefined) {
            params.append(key, value.toString());
        }
    }
    window.location = '/download_json?' + params.toString(); 
}

async function runDatabaseMaintenance() {
    if (!confirm('Run database maintenance (VACUUM and ANALYZE)? This may take a few moments...')) {
        return;
    }
    
    const btn = document.getElementById('maintenance-db-btn');
    const originalText = btn.innerText;
    btn.disabled = true;
    btn.innerText = 'Optimizing...';
    
    try {
        const response = await fetch('/api/db/maintenance', { method: 'POST' });
        const result = await response.json();
        
        if (response.ok) {
            showToast('Database maintenance completed!');
        } else {
            showToast('Database maintenance failed', {isError: true});
            console.error('Maintenance failed:', result.message || 'Unknown error');
        }
    } catch (e) {
        console.error('Maintenance error:', e);
        showToast('Database maintenance failed', {isError: true});
    } finally {
        btn.disabled = false;
        btn.innerText = originalText;
    }
}

async function backfillMetadata() {
    if (!confirm('Backfill missing metadata (year, titles, season/episode, type) from .nfo and filename?')) {
        return;
    }
    try {
        showToast('Backfilling metadata...');
        const response = await fetch('/api/backfill_metadata', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ fill_blanks_only: true })
        });
        const result = await response.json();
        if (response.ok) {
            showToast(`Backfill complete (${result.updated || 0} updated)`);
            loadData();
        } else {
            showToast('Backfill failed', {isError: true});
            console.error('Backfill failed:', result.message || 'Unknown error');
        }
    } catch (e) {
        console.error('Backfill error:', e);
        showToast('Backfill failed', {isError: true});
    }
}

async function backupDatabase() {
    try {
        showToast('Preparing backup...');
        const response = await fetch('/api/backup', { method: 'POST' });
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `video_analyzer_backup_${new Date().toISOString().slice(0,10)}.zip`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
            console.log('Backup downloaded successfully');
            showToast('Backup downloaded');
        } else {
            const error = await response.json();
            showToast('Backup failed', {isError: true});
            console.error('Backup failed:', error.message || 'Unknown error');
        }
    } catch (e) {
        console.error('Backup error:', e);
        showToast('Backup failed', {isError: true});
    }
}

async function restoreDatabase(fileInput) {
    const file = fileInput.files[0];
    if (!file) return;
    
    if (!file.name.toLowerCase().endsWith('.zip')) {
        showToast('Please select a ZIP file');
        fileInput.value = '';
        return;
    }
    
    if (!confirm('This will replace your current database and settings. Are you sure?')) {
        fileInput.value = '';
        return;
    }
    
    try {
        const formData = new FormData();
        formData.append('file', file);
        showToast('Restoring database...');
        
        const response = await fetch('/api/restore', {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        if (response.ok) {
            showToast(result.message || 'Database restored. Reloading...');
            fileInput.value = '';
            setTimeout(() => window.location.reload(), 1500);
        } else {
            showToast('Restore failed', {isError: true});
            console.error('Restore failed:', result.message || 'Unknown error');
            fileInput.value = '';
        }
    } catch (e) {
        console.error('Restore error:', e);
        showToast('Restore failed', {isError: true});
        fileInput.value = '';
    }
}

async function loadFilterPresetsList() {
    try {
        const response = await fetch('/api/filter_presets');
        if (response.ok) {
            const presets = await response.json();
            const select = document.getElementById('preset-select');
            if (select) {
                select.innerHTML = '<option value="">Select preset...</option>';
                for (const name of Object.keys(presets)) {
                    const option = document.createElement('option');
                    option.value = name;
                    option.textContent = name;
                    select.appendChild(option);
                }
            }
        }
    } catch (e) {
        console.error('Failed to load filter presets:', e);
    }
}

async function saveFilterPreset() {
    const nameInput = document.getElementById('preset-name-input');
    if (!nameInput) return;
    
    const presetName = nameInput.value.trim();
    if (!presetName) {
        alert('Please enter a preset name');
        return;
    }
    
    // Collect current filter values (same as exportCSV does)
    const currentFilters = { ...activeFilters };
    currentFilters.search = document.getElementById('search-bar') ? document.getElementById('search-bar').value : '';
    try { 
        currentFilters.category = getFormatFilterValue();
    } catch(e) { 
        currentFilters.category = ''; 
    }
    try { currentFilters.volume = getMultiselectValue('vol-filter'); } catch(e) { currentFilters.volume = ''; }
    try { currentFilters.profile = getMultiselectValue('profile-filter'); } catch(e) { currentFilters.profile = ''; }
    try { currentFilters.el = getMultiselectValue('el-filter'); } catch(e) { currentFilters.el = ''; }
    try { currentFilters.container = getMultiselectValue('container-filter'); } catch(e) { currentFilters.container = ''; }
    try { currentFilters.is_hybrid = getMultiselectValue('hybrid-filter'); } catch(e) { currentFilters.is_hybrid = ''; }
    try { currentFilters.source_hybrid = getMultiselectValue('source-hybrid-filter'); } catch(e) { currentFilters.source_hybrid = ''; }
    try { currentFilters.secondary_hdr = getMultiselectValue('secondary-filter'); } catch(e) { currentFilters.secondary_hdr = ''; }
    try { currentFilters.status = getMultiselectValue('status-filter'); } catch(e) { currentFilters.status = ''; }
    try { currentFilters.nfo_missing = getMultiselectValue('nfo-filter'); } catch(e) { currentFilters.nfo_missing = ''; }
    try { currentFilters.missing = getMultiselectValue('missing-filter'); } catch(e) { currentFilters.missing = ''; }
    try { currentFilters.anomaly = getMultiselectValue('anomaly-filter'); } catch(e) { currentFilters.anomaly = ''; }
    try { currentFilters.resolution = getMultiselectValue('res-filter'); } catch(e) { currentFilters.resolution = ''; }
    const sizeFilter = document.getElementById('size-filter-header') ? document.getElementById('size-filter-header').value : '';
    const sizeParsed = parseFilterValue(sizeFilter);
    currentFilters.size_op = sizeParsed.op;
    currentFilters.size_val = sizeParsed.value;
    const bitFilter = document.getElementById('bit-filter-header') ? document.getElementById('bit-filter-header').value : '';
    const bitParsed = parseFilterValue(bitFilter);
    currentFilters.bit_op = bitParsed.op;
    currentFilters.bit_val = bitParsed.value;
    try { currentFilters.audio = getMultiselectValue('audio-filter'); } catch(e) { currentFilters.audio = ''; }
    try { currentFilters.video_source = getMultiselectValue('video-source-filter'); } catch(e) { currentFilters.video_source = ''; }
    try { currentFilters.source_format = getMultiselectValue('source-format-filter'); } catch(e) { currentFilters.source_format = ''; }
    try { currentFilters.video_codec = getMultiselectValue('video-codec-filter'); } catch(e) { currentFilters.video_codec = ''; }
    try { currentFilters.is_3d = getMultiselectValue('is-3d-filter'); } catch(e) { currentFilters.is_3d = ''; }
    try { currentFilters.edition = getMultiselectValue('edition-filter'); } catch(e) { currentFilters.edition = ''; }
    currentFilters.sort = sortCol;
    currentFilters.order = sortOrder;
    
    try {
        const response = await fetch('/api/filter_presets', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ name: presetName, filters: currentFilters })
        });
        
        const result = await response.json();
        if (response.ok) {
            nameInput.value = '';
            await loadFilterPresetsList();
            alert(result.message || 'Preset saved successfully');
        } else {
            alert('Failed to save preset: ' + (result.message || 'Unknown error'));
        }
    } catch (e) {
        console.error('Save preset error:', e);
        alert('Failed to save preset: ' + e.message);
    }
}

async function loadFilterPreset() {
    const select = document.getElementById('preset-select');
    if (!select || !select.value) {
        alert('Please select a preset to load');
        return;
    }
    
    try {
        const response = await fetch('/api/filter_presets');
        if (response.ok) {
            const presets = await response.json();
            const filters = presets[select.value];
            if (!filters) {
                alert('Preset not found');
                return;
            }
            
            // Apply filters (similar to how loadData applies filters from URL params)
            if (filters.search) document.getElementById('search-bar').value = filters.search;
            if (filters.category) setFormatFilterValue(filters.category);
            if (filters.volume) setMultiselectValue('vol-filter', filters.volume, true);
            if (filters.profile) setMultiselectValue('profile-filter', filters.profile, true);
            if (filters.el) setMultiselectValue('el-filter', filters.el, true);
            if (filters.container) setMultiselectValue('container-filter', filters.container, true);
            if (filters.media_type) setMultiselectValue('media-type-filter', filters.media_type, true);
            ensureBinaryMultiselects();
            if (filters.is_hybrid !== undefined) setMultiselectValue('hybrid-filter', filters.is_hybrid, true);
            if (filters.source_hybrid !== undefined) setMultiselectValue('source-hybrid-filter', filters.source_hybrid, true);
            if (filters.secondary_hdr) setMultiselectValue('secondary-filter', filters.secondary_hdr, true);
            if (filters.status !== undefined) setMultiselectValue('status-filter', filters.status, true);
            if (filters.nfo_missing !== undefined) setMultiselectValue('nfo-filter', filters.nfo_missing, true);
            if (filters.missing !== undefined) setMultiselectValue('missing-filter', filters.missing, true);
            if (filters.anomaly !== undefined) setMultiselectValue('anomaly-filter', filters.anomaly, true);
            if (filters.resolution) setMultiselectValue('res-filter', filters.resolution, true);
            if (filters.size_op && filters.size_val) {
                const sizeVal = filters.size_op + filters.size_val;
                document.getElementById('size-filter-header').value = sizeVal;
            }
            if (filters.bit_op && filters.bit_val) {
                const bitVal = filters.bit_op + filters.bit_val;
                document.getElementById('bit-filter-header').value = bitVal;
            }
            if (filters.audio) setMultiselectValue('audio-filter', filters.audio, true);
            if (filters.video_source) setMultiselectValue('video-source-filter', filters.video_source, true);
            if (filters.source_format) setMultiselectValue('source-format-filter', filters.source_format, true);
            if (filters.video_codec) setMultiselectValue('video-codec-filter', filters.video_codec, true);
            if (filters.is_3d !== undefined) setMultiselectValue('is-3d-filter', filters.is_3d, true);
            if (filters.edition) setMultiselectValue('edition-filter', filters.edition, true);
            if (filters.sort) sortCol = filters.sort;
            if (filters.order) sortOrder = filters.order;
            
            // Reload data with new filters
            resetAndLoad();
            alert('Preset loaded successfully');
        } else {
            alert('Failed to load presets');
        }
    } catch (e) {
        console.error('Load preset error:', e);
        alert('Failed to load preset: ' + e.message);
    }
}

async function deleteFilterPreset() {
    const select = document.getElementById('preset-select');
    if (!select || !select.value) {
        alert('Please select a preset to delete');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete preset "${select.value}"?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/filter_presets/${encodeURIComponent(select.value)}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        if (response.ok) {
            await loadFilterPresetsList();
            alert(result.message || 'Preset deleted successfully');
        } else {
            alert('Failed to delete preset: ' + (result.message || 'Unknown error'));
        }
    } catch (e) {
        console.error('Delete preset error:', e);
        alert('Failed to delete preset: ' + e.message);
    }
}

updateStickyOffsets(); // CSS offsets for sticky control bars
// Initialize column resize
initColumnResize();
// Column picker must live inside the freeze-pane scrollport.
ensureColMenuInScrollport();
// Seed colgroup widths, then size the freeze-pane scrollport to the remaining viewport.
syncColgroupFromStoredWidths();
requestAnimationFrame(updateStickyHeader);

// Initialize all multiselects
const multiselectFilters = ['profile-filter', 'el-filter', 'vol-filter', 'container-filter', 'secondary-filter', 'res-filter', 'audio-filter'];
multiselectFilters.forEach(filterId => {
    const wrapper = document.getElementById(`${filterId}-wrapper`);
    if (wrapper) {
        const button = wrapper.querySelector('.multiselect-button');
        const dropdown = wrapper.querySelector('.multiselect-dropdown');
        const buttonText = button.querySelector('span:first-child');
        multiselectState[filterId] = { open: false, button, dropdown, buttonText, options: {}, labelMap: {} };
        
        button.onclick = (e) => {
            e.stopPropagation();
            toggleMultiselect(filterId);
        };
    }
});


// Init - Check for ongoing scan first, then load data
// Initial page load - ensure table loads with all files
function initializePage() {
    clearFilters(false); // Clear all filters without reloading
    // Small delay to ensure DOM is ready and filters are cleared
    setTimeout(() => {
loadData();
    }, 100);
}

fetch('/progress').then(r=>r.json()).then(d => {
    if(d.status === 'scanning') {
        // Reconnect to ongoing scan
        document.body.classList.add('scanning');
        scanStartTime = d.start_time || 0;
        if (scanStartTime > 0) {
            const srvDur = parseInt(d.last_duration.replace('s','')) || 0;
            const now = Date.now() / 1000;
            scanStartTime = now - srvDur;
        }
        // Still load initial data even if scan is in progress
        initializePage();
        poll(); // Start polling immediately
    } else {
        // No scan in progress, clear filters and load data normally
        initializePage();
        if (typeof checkInterruptedScan === 'function') checkInterruptedScan();
    }
}).catch(e => {
    console.error("Failed to check progress:", e);
    // Fallback to normal load
    initializePage();
});

setInterval(updateLogs, 2000);
setInterval(() => { if(document.body.classList.contains('scanning') && scanStartTime) { const diff = Math.floor((Date.now() / 1000) - scanStartTime); document.getElementById('stat-duration').innerText = formatDuration(diff); } }, 1000);
