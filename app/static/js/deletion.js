// --- DELETION ---
function toggleMaster(chk) {
    masterState = (masterState + 1) % 3;
    const chkEl = document.getElementById('master-chk');
    chkEl.className = 'col-chk ' + (masterState === 1 ? 'master-dash' : masterState === 2 ? 'master-x' : '');
    chkEl.checked = false; // Always uncheck the checkbox element itself
    if (masterState === 0) { selectedPaths.clear(); document.querySelectorAll('.row-chk').forEach(c => c.checked = false); } 
    else if (masterState === 1) { currentRows.forEach(p => selectedPaths.add(p)); document.querySelectorAll('.row-chk').forEach(c => c.checked = true); }
    else if (masterState === 2) { selectedPaths.clear(); document.querySelectorAll('.row-chk').forEach(c => c.checked = true); }
    updateDeleteBtn();
}

function toggleRow(chk, path) {
    if (chk.checked) selectedPaths.add(path); else selectedPaths.delete(path);
    if (masterState === 2) { masterState = 0; document.getElementById('master-chk').className = 'col-chk'; }
    updateDeleteBtn();
}

function updateDeleteBtn() {
    const wrapper = document.getElementById('main-action-wrapper');
    const btn = document.getElementById('btn-delete-action');
    const bulkBtn = document.getElementById('btn-bulk-edit');
    
    btn.classList.remove('btn-orange');

    if (selectedPaths.size > 0 || masterState === 2) {
        wrapper.classList.add('selection-active');
        if (masterState === 2) {
            btn.style.backgroundColor = '#e74c3c'; 
            btn.innerText = `DELETE ALL FILTERED`;
        } else {
            btn.style.backgroundColor = '#e67e22'; 
            btn.innerText = `DELETE (${selectedPaths.size})`;
        }
    } else {
        wrapper.classList.remove('selection-active');
    }

    if (bulkBtn) {
        const filteredCount = parseInt(document.getElementById('res-filtered')?.innerText || '0', 10);
        if (selectedPaths.size > 0) {
            bulkBtn.disabled = false;
            bulkBtn.innerText = `Bulk Edit/Rescan Selected (${selectedPaths.size})`;
            bulkBtn.style.display = '';
        } else if (masterState === 2) {
            bulkBtn.disabled = false;
            bulkBtn.innerText = `Bulk Edit/Rescan Filtered (${filteredCount})`;
            bulkBtn.style.display = '';
        } else {
            bulkBtn.disabled = true;
            bulkBtn.innerText = 'Bulk Edit/Rescan';
            bulkBtn.style.display = '';
        }
    }
}

function closeHealthModal() {
    const modal = document.getElementById('health-modal');
    if (modal) modal.style.display = 'none';
}

async function openHealthModal() {
    const modal = document.getElementById('health-modal');
    if (modal) modal.style.display = 'block';
    const statusEl = document.getElementById('health-status');
    const dbEl = document.getElementById('health-db');
    const scanEl = document.getElementById('health-scan');
    const uptimeEl = document.getElementById('health-uptime');
    const versionEl = document.getElementById('health-version');
    const latencyEl = document.getElementById('health-latency');
    const sonarrEl = document.getElementById('health-sonarr');
    const radarrEl = document.getElementById('health-radarr');
    const toolEls = {
        mediainfo: document.getElementById('health-tool-mediainfo'),
        ffmpeg: document.getElementById('health-tool-ffmpeg'),
        ffprobe: document.getElementById('health-tool-ffprobe'),
        dovi_tool: document.getElementById('health-tool-dovi'),
        python: document.getElementById('health-tool-python')
    };
    const errorEl = document.getElementById('health-error');

    if (statusEl) statusEl.textContent = 'Loading...';
    if (dbEl) dbEl.textContent = 'Loading...';
    if (scanEl) scanEl.textContent = 'Loading...';
    if (uptimeEl) uptimeEl.textContent = 'Loading...';
    if (versionEl) versionEl.textContent = 'Loading...';
    if (latencyEl) latencyEl.textContent = 'Loading...';
    if (sonarrEl) sonarrEl.textContent = 'Loading...';
    if (radarrEl) radarrEl.textContent = 'Loading...';
    Object.values(toolEls).forEach(el => {
        if (!el) return;
        el.textContent = 'Loading...';
        el.classList.remove('health-ok', 'health-warn', 'health-err');
    });
    if (errorEl) errorEl.style.display = 'none';

    const start = performance.now();
    try {
        const res = await fetch('/api/health');
        const latency = Math.round(performance.now() - start);
        const data = await res.json();

        if (statusEl) {
            statusEl.textContent = capitalizeFirst(data.status || 'unknown');
            statusEl.classList.remove('health-ok', 'health-warn', 'health-err');
            if (data.status === 'healthy') statusEl.classList.add('health-ok');
            else if (data.status === 'degraded') statusEl.classList.add('health-warn');
            else statusEl.classList.add('health-err');
        }
        if (dbEl) {
            dbEl.textContent = capitalizeFirst(data.database || 'unknown');
            dbEl.classList.remove('health-ok', 'health-warn', 'health-err');
            if (data.database === 'ok') dbEl.classList.add('health-ok');
            else dbEl.classList.add('health-err');
        }
        if (scanEl) scanEl.textContent = capitalizeFirst(data.scan_status || 'unknown');
        if (uptimeEl) uptimeEl.textContent = formatUptime(data.uptime_seconds);
        if (versionEl) versionEl.textContent = data.version || 'unknown';
        if (latencyEl) latencyEl.textContent = `${latency} ms`;
        const tools = data.tools || {};
        Object.entries(toolEls).forEach(([key, el]) => {
            if (!el) return;
            const info = tools[key] || {};
            el.textContent = info.installed ? (info.version || info.message || 'Installed') : (info.message || 'Not found');
            el.classList.remove('health-ok', 'health-warn', 'health-err');
            el.classList.add(info.installed ? 'health-ok' : 'health-warn');
        });
        if (sonarrEl) {
            const s = data.sonarr || {};
            sonarrEl.textContent = s.configured ? (s.ok ? s.message || 'Connected' : s.message || 'Error') : 'Not configured';
            sonarrEl.classList.remove('health-ok', 'health-warn', 'health-err');
            if (!s.configured) sonarrEl.classList.add('health-warn');
            else if (s.ok) sonarrEl.classList.add('health-ok');
            else sonarrEl.classList.add('health-err');
        }
        if (radarrEl) {
            const r = data.radarr || {};
            radarrEl.textContent = r.configured ? (r.ok ? r.message || 'Connected' : r.message || 'Error') : 'Not configured';
            radarrEl.classList.remove('health-ok', 'health-warn', 'health-err');
            if (!r.configured) radarrEl.classList.add('health-warn');
            else if (r.ok) radarrEl.classList.add('health-ok');
            else radarrEl.classList.add('health-err');
        }
    } catch (e) {
        if (errorEl) {
            errorEl.style.display = 'block';
            errorEl.textContent = `Failed to load health status: ${e.message}`;
        }
        if (statusEl) {
            statusEl.textContent = 'error';
            statusEl.classList.remove('health-ok', 'health-warn');
            statusEl.classList.add('health-err');
        }
        if (sonarrEl) sonarrEl.textContent = '—';
        if (radarrEl) radarrEl.textContent = '—';
        Object.values(toolEls).forEach(el => {
            if (el) el.textContent = '—';
        });
    }
}

function openCleanDbModal() {
    const modal = document.getElementById('clean-db-modal');
    if (modal) modal.style.display = 'block';
    updateCleanDbPreview();
}

async function updateCleanDbPreview() {
    const previewEl = document.getElementById('clean-db-preview');
    if (!previewEl) return;
    previewEl.innerText = 'Preview: Loading number of entries which will be removed. Please wait...';
    try {
        const res = await fetch('/api/cleanup_db_preview');
        const data = await res.json();
        if (!res.ok || data.status !== 'ok') throw new Error(data.message || 'Preview failed');
        const count = data.count ?? 0;
        previewEl.innerText = `Preview: ${count} entries will be removed.`;
    } catch (e) {
        console.error('Cleanup preview failed', e);
        previewEl.innerText = 'Preview: Unable to calculate entries to remove.';
    }
}

async function confirmCleanDb() {
    try {
        const res = await fetch('/api/cleanup_db', { method: 'POST' });
        const data = await res.json();
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || 'Cleanup failed');
        }
        showToast(`Cleaned ${data.deleted || 0} entries`);
        loadData();
    } catch (e) {
        console.error('Cleanup failed', e);
        showToast('Cleanup failed', {isError: true});
    } finally {
        const modal = document.getElementById('clean-db-modal');
        if (modal) modal.style.display = 'none';
    }
}

function closeDeleteModal() {
    document.getElementById('delete-modal').style.display = 'none';
    const confirmInput = document.getElementById('del-confirm-text');
    if (confirmInput) confirmInput.value = '';
}

function promptDelete(singlePath=null) {
    if (singlePath) { selectedPaths.clear(); selectedPaths.add(singlePath); }
    let count = (masterState === 2) ? parseInt(document.getElementById('res-filtered').innerText) : selectedPaths.size;
    if (!count || Number.isNaN(count)) count = selectedPaths.size;
    document.getElementById('del-count').innerText = count;
    const bulkWrap = document.getElementById('del-bulk-confirm-wrap');
    const confirmInput = document.getElementById('del-confirm-text');
    const needsTypedConfirm = masterState === 2 || count >= 50;
    if (bulkWrap) bulkWrap.style.display = needsTypedConfirm ? 'block' : 'none';
    if (confirmInput) confirmInput.value = '';
    document.getElementById('delete-modal').style.display = 'block';
}

async function confirmDelete() {
    const count = parseInt(document.getElementById('del-count').innerText, 10) || 0;
    const needsTypedConfirm = masterState === 2 || count >= 50;
    if (needsTypedConfirm) {
        const typed = (document.getElementById('del-confirm-text') || {}).value || '';
        if (typed.trim() !== 'DELETE') {
            showToast('Type DELETE to confirm this large deletion', {isError: true});
            return;
        }
    }

    const currentFilters = {
        search: document.getElementById('search-bar').value,
        category: getFormatFilterValue(),
        volume: getMultiselectValue('vol-filter'),
        profile: getMultiselectValue('profile-filter'),
        el: getMultiselectValue('el-filter'),
        container: getMultiselectValue('container-filter'),
        media_type: getMultiselectValue('media-type-filter'),
        is_hybrid: document.getElementById('hybrid-filter-header') ? document.getElementById('hybrid-filter-header').value : '',
        source_hybrid: document.getElementById('source-hybrid-filter-header') ? document.getElementById('source-hybrid-filter-header').value : '',
        secondary_hdr: getMultiselectValue('secondary-filter'),
        status: document.getElementById('status-filter-header') ? document.getElementById('status-filter-header').value : '',
        resolution: getMultiselectValue('res-filter'),
        size_op: (() => {
            const val = document.getElementById('size-filter-header') ? document.getElementById('size-filter-header').value : '';
            const parsed = parseFilterValue(val);
            return parsed.op;
        })(),
        size_val: (() => {
            const val = document.getElementById('size-filter-header') ? document.getElementById('size-filter-header').value : '';
            const parsed = parseFilterValue(val);
            return parsed.value;
        })(),
        bit_op: (() => {
            const val = document.getElementById('bit-filter-header') ? document.getElementById('bit-filter-header').value : '';
            const parsed = parseFilterValue(val);
            return parsed.op;
        })(),
        bit_val: (() => {
            const val = document.getElementById('bit-filter-header') ? document.getElementById('bit-filter-header').value : '';
            const parsed = parseFilterValue(val);
            return parsed.value;
        })(),
        audio: document.getElementById('audio-filter-header') ? document.getElementById('audio-filter-header').value : '',
        sort: sortCol,
        order: sortOrder
    };

    const payload = {
        paths: Array.from(selectedPaths),
        delete_all_filter: (masterState === 2),
        filters: currentFilters
    };
    try {
        const res = await fetch('/api/delete', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.message || `Delete failed (${res.status})`);
        showToast(`Deleted ${data.count || 0} database entr${(data.count === 1) ? 'y' : 'ies'}`);
        closeDeleteModal();
        selectedPaths.clear();
        masterState = 0;
        document.getElementById('master-chk').className = 'col-chk';
        updateDeleteBtn();
        loadData();
    } catch (e) {
        console.error('Delete failed', e);
        showToast(e.message || 'Delete failed', {isError: true});
    }
}
