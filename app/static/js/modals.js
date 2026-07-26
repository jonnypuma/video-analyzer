// --- NEW DETAILS MODAL LOGIC ---
function showDetails(rowDataStr) {
    try {
        const data = JSON.parse(decodeURIComponent(rowDataStr));
        document.getElementById('det-path').innerText = data.full_path;
        currentDetailsPath = data.full_path || '';
        const mediaTypeEl = document.getElementById('det-media-type');
        if (mediaTypeEl) {
            mediaTypeEl.value = data.media_type || '';
            currentDetailsMediaType = mediaTypeEl.value;
            updateDetailsTypeVisibility(mediaTypeEl.value);
            mediaTypeEl.onchange = () => updateDetailsTypeVisibility(mediaTypeEl.value);
        }
        const showTitleEl = document.getElementById('det-show-title');
        const episodeTitleEl = document.getElementById('det-episode-title');
        const movieTitleEl = document.getElementById('det-movie-title');
        const seasonEl = document.getElementById('det-season');
        const episodeEl = document.getElementById('det-episode');
        const yearEl = document.getElementById('det-year');
        const videoSourceEl = document.getElementById('det-video-source');
        const sourceFormatEl = document.getElementById('det-source-format');
        const mainHdrEl = document.getElementById('det-main-hdr');
        const secondaryHdrEl = document.getElementById('det-secondary-hdr');
        if (showTitleEl) showTitleEl.value = data.show_title || '';
        if (episodeTitleEl) episodeTitleEl.value = data.episode_title || '';
        if (movieTitleEl) movieTitleEl.value = data.movie_title || '';
        if (seasonEl) seasonEl.value = data.season ?? '';
        if (episodeEl) episodeEl.value = data.episode ?? '';
        if (yearEl) yearEl.value = data.year ?? '';
        if (videoSourceEl) videoSourceEl.value = data.video_source || '';
        if (sourceFormatEl) sourceFormatEl.value = data.source_format || '';
        if (mainHdrEl) mainHdrEl.value = data.category || '';
        if (secondaryHdrEl) secondaryHdrEl.value = data.secondary_hdr || '';
        currentDetailsMeta = {
            show_title: data.show_title || '',
            episode_title: data.episode_title || '',
            movie_title: data.movie_title || '',
            season: data.season ?? '',
            episode: data.episode ?? '',
            year: data.year ?? '',
            video_source: data.video_source || '',
            source_format: data.source_format || '',
            category: data.category || '',
            secondary_hdr: data.secondary_hdr || ''
        };
        const viewData = { ...data };
        delete viewData.full_path; 
        document.getElementById('det-json').innerText = JSON.stringify(viewData, null, 2);
        document.getElementById('details-modal').style.display = 'block';
    } catch(e) { console.error("Error showing details", e); }
}
function updateDetailsTypeVisibility(typeValue) {
    const showTitleRow = document.getElementById('det-show-title-row');
    const episodeTitleRow = document.getElementById('det-episode-title-row');
    const seasonEpisodeRow = document.getElementById('det-season-episode-row');
    const movieTitleRow = document.getElementById('det-movie-title-row');
    if (typeValue === 'movie') {
        if (showTitleRow) showTitleRow.style.display = 'none';
        if (episodeTitleRow) episodeTitleRow.style.display = 'none';
        if (seasonEpisodeRow) seasonEpisodeRow.style.display = 'none';
        if (movieTitleRow) movieTitleRow.style.display = '';
    } else if (typeValue === 'tv') {
        if (showTitleRow) showTitleRow.style.display = '';
        if (episodeTitleRow) episodeTitleRow.style.display = '';
        if (seasonEpisodeRow) seasonEpisodeRow.style.display = '';
        if (movieTitleRow) movieTitleRow.style.display = 'none';
    } else {
        if (showTitleRow) showTitleRow.style.display = '';
        if (episodeTitleRow) episodeTitleRow.style.display = '';
        if (seasonEpisodeRow) seasonEpisodeRow.style.display = '';
        if (movieTitleRow) movieTitleRow.style.display = '';
    }
}
async function saveDetailsMediaType() {
    const mediaTypeEl = document.getElementById('det-media-type');
    if (!mediaTypeEl || !currentDetailsPath) return;
    const newValue = mediaTypeEl.value || '';
    if (newValue === currentDetailsMediaType) return;
    try {
        const res = await fetch('/api/update_media_type', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ full_path: currentDetailsPath, media_type: newValue })
        });
        if (!res.ok) throw new Error(`Server Error: ${res.status}`);
        showToast('Type updated');
        currentDetailsMediaType = newValue;
        loadData();
    } catch (e) {
        console.error('Failed to update media type:', e);
        showToast('Failed to update type', {isError: true});
    }
}
async function saveDetailsMetadata() {
    if (!currentDetailsPath) return;
    const showTitleEl = document.getElementById('det-show-title');
    const episodeTitleEl = document.getElementById('det-episode-title');
    const movieTitleEl = document.getElementById('det-movie-title');
    const seasonEl = document.getElementById('det-season');
    const episodeEl = document.getElementById('det-episode');
    const yearEl = document.getElementById('det-year');
    const videoSourceEl = document.getElementById('det-video-source');
    const sourceFormatEl = document.getElementById('det-source-format');
    const mainHdrEl = document.getElementById('det-main-hdr');
    const secondaryHdrEl = document.getElementById('det-secondary-hdr');
    const payload = {
        full_path: currentDetailsPath,
        show_title: showTitleEl ? showTitleEl.value.trim() : '',
        episode_title: episodeTitleEl ? episodeTitleEl.value.trim() : '',
        movie_title: movieTitleEl ? movieTitleEl.value.trim() : '',
        season: seasonEl && seasonEl.value !== '' ? parseInt(seasonEl.value, 10) : null,
        episode: episodeEl && episodeEl.value !== '' ? parseInt(episodeEl.value, 10) : null,
        year: yearEl && yearEl.value !== '' ? parseInt(yearEl.value, 10) : null,
        video_source: videoSourceEl ? videoSourceEl.value.trim() : '',
        source_format: sourceFormatEl ? sourceFormatEl.value.trim() : '',
        category: mainHdrEl ? mainHdrEl.value.trim() : '',
        secondary_hdr: secondaryHdrEl ? secondaryHdrEl.value.trim() : ''
    };
    const changed = Object.keys(currentDetailsMeta).some(key => {
        return (currentDetailsMeta[key] ?? '') !== (payload[key] ?? '');
    });
    if (!changed) return;
    try {
        const res = await fetch('/api/update_metadata', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!res.ok) throw new Error(`Server Error: ${res.status}`);
        showToast('Details updated');
        currentDetailsMeta = {
            show_title: payload.show_title || '',
            episode_title: payload.episode_title || '',
            movie_title: payload.movie_title || '',
            season: payload.season ?? '',
            episode: payload.episode ?? '',
            year: payload.year ?? '',
            video_source: payload.video_source || '',
            source_format: payload.source_format || '',
            category: payload.category || '',
            secondary_hdr: payload.secondary_hdr || ''
        };
        loadData();
    } catch (e) {
        console.error('Failed to update details:', e);
        showToast('Failed to update details', {isError: true});
    }
}

function openBulkEdit() {
    closeContextMenu();
    if (selectedPaths.size === 0 && masterState !== 2) {
        showToast('Select rows to bulk edit');
        return;
    }
    const countEl = document.getElementById('bulk-edit-count');
    if (countEl) {
        const filteredCount = parseInt(document.getElementById('res-filtered')?.innerText || '0', 10);
        countEl.innerText = masterState === 2 ? filteredCount : selectedPaths.size;
    }
    const rescanBtn = document.getElementById('btn-bulk-rescan');
    if (rescanBtn) {
        const filteredCount = parseInt(document.getElementById('res-filtered')?.innerText || '0', 10);
        const count = masterState === 2 ? filteredCount : selectedPaths.size;
        rescanBtn.innerText = `Rescan Selected (${count})`;
    }
    const resetIds = [
        'bulk-media-type', 'bulk-show-title', 'bulk-episode-title', 'bulk-movie-title',
        'bulk-year', 'bulk-season', 'bulk-episode', 'bulk-video-source', 'bulk-source-format',
        'bulk-main-hdr', 'bulk-secondary-hdr'
    ];
    resetIds.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    const clearIds = [
        'bulk-clear-media-type', 'bulk-clear-show-title', 'bulk-clear-episode-title',
        'bulk-clear-movie-title', 'bulk-clear-year', 'bulk-clear-season', 'bulk-clear-episode',
        'bulk-clear-video-source', 'bulk-clear-source-format', 'bulk-clear-main-hdr', 'bulk-clear-secondary-hdr'
    ];
    clearIds.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.checked = false;
    });
    const modal = document.getElementById('bulk-edit-modal');
    if (modal) modal.style.display = 'block';
}

function closeBulkEdit() {
    const modal = document.getElementById('bulk-edit-modal');
    if (modal) modal.style.display = 'none';
}

function getCurrentFilterSnapshot() {
    // Re-read controls from the DOM so duplicates/bulk/rebuild match what the UI shows
    // (do not clear pending multiselect values).
    if (typeof syncActiveFiltersFromDom === 'function') {
        syncActiveFiltersFromDom(false);
    }
    return { ...activeFilters };
}


function setBulkEditStatus(processed, total) {
    const statusEl = document.getElementById('bulk-status-text');
    if (!statusEl) return;
    const base = 'Updating, please wait...';
    if (typeof total === 'number' && total > 0) {
        statusEl.textContent = `${base} ${processed} of ${total}`;
    } else {
        statusEl.textContent = base;
    }
}

async function applyBulkEdit() {
    if (selectedPaths.size === 0 && masterState !== 2) {
        showToast('Select rows to bulk edit');
        return;
    }
    const mediaTypeEl = document.getElementById('bulk-media-type');
    const clearMediaTypeEl = document.getElementById('bulk-clear-media-type');
    const showTitleEl = document.getElementById('bulk-show-title');
    const clearShowTitleEl = document.getElementById('bulk-clear-show-title');
    const episodeTitleEl = document.getElementById('bulk-episode-title');
    const clearEpisodeTitleEl = document.getElementById('bulk-clear-episode-title');
    const movieTitleEl = document.getElementById('bulk-movie-title');
    const clearMovieTitleEl = document.getElementById('bulk-clear-movie-title');
    const yearEl = document.getElementById('bulk-year');
    const clearYearEl = document.getElementById('bulk-clear-year');
    const seasonEl = document.getElementById('bulk-season');
    const clearSeasonEl = document.getElementById('bulk-clear-season');
    const episodeEl = document.getElementById('bulk-episode');
    const clearEpisodeEl = document.getElementById('bulk-clear-episode');
    const videoSourceEl = document.getElementById('bulk-video-source');
    const clearVideoSourceEl = document.getElementById('bulk-clear-video-source');
    const sourceFormatEl = document.getElementById('bulk-source-format');
    const clearSourceFormatEl = document.getElementById('bulk-clear-source-format');
    const mainHdrEl = document.getElementById('bulk-main-hdr');
    const clearMainHdrEl = document.getElementById('bulk-clear-main-hdr');
    const secondaryHdrEl = document.getElementById('bulk-secondary-hdr');
    const clearSecondaryHdrEl = document.getElementById('bulk-clear-secondary-hdr');

    const mediaType = mediaTypeEl ? mediaTypeEl.value.trim().toLowerCase() : '';
    const clearMediaType = clearMediaTypeEl ? clearMediaTypeEl.checked : false;
    const payload = {};
    const setIf = (key, value, clear) => {
        if (clear) {
            payload[key] = '';
            return;
        }
        if (value !== null && value !== undefined && value !== '') payload[key] = value;
    };
    setIf('show_title', showTitleEl ? showTitleEl.value.trim() : '', clearShowTitleEl ? clearShowTitleEl.checked : false);
    setIf('episode_title', episodeTitleEl ? episodeTitleEl.value.trim() : '', clearEpisodeTitleEl ? clearEpisodeTitleEl.checked : false);
    setIf('movie_title', movieTitleEl ? movieTitleEl.value.trim() : '', clearMovieTitleEl ? clearMovieTitleEl.checked : false);
    const clearYear = clearYearEl ? clearYearEl.checked : false;
    const yearVal = yearEl && yearEl.value !== '' ? parseInt(yearEl.value, 10) : null;
    if (clearYear) payload.year = null;
    else if (yearVal !== null && !Number.isNaN(yearVal)) payload.year = yearVal;
    const clearSeason = clearSeasonEl ? clearSeasonEl.checked : false;
    const seasonVal = seasonEl && seasonEl.value !== '' ? parseInt(seasonEl.value, 10) : null;
    if (clearSeason) payload.season = null;
    else if (seasonVal !== null && !Number.isNaN(seasonVal)) payload.season = seasonVal;
    const clearEpisode = clearEpisodeEl ? clearEpisodeEl.checked : false;
    const episodeVal = episodeEl && episodeEl.value !== '' ? parseInt(episodeEl.value, 10) : null;
    if (clearEpisode) payload.episode = null;
    else if (episodeVal !== null && !Number.isNaN(episodeVal)) payload.episode = episodeVal;
    setIf('video_source', videoSourceEl ? videoSourceEl.value.trim() : '', clearVideoSourceEl ? clearVideoSourceEl.checked : false);
    setIf('source_format', sourceFormatEl ? sourceFormatEl.value.trim() : '', clearSourceFormatEl ? clearSourceFormatEl.checked : false);
    setIf('category', mainHdrEl ? mainHdrEl.value.trim() : '', clearMainHdrEl ? clearMainHdrEl.checked : false);
    setIf('secondary_hdr', secondaryHdrEl ? secondaryHdrEl.value.trim() : '', clearSecondaryHdrEl ? clearSecondaryHdrEl.checked : false);

    const hasMetadata = Object.keys(payload).length > 0;
    const hasType = clearMediaType || mediaType === 'movie' || mediaType === 'tv';
    if (!hasMetadata && !hasType) {
        showToast('No fields set');
        return;
    }

    const paths = await getBulkTargetPaths();
    if (!paths.length) {
        showToast('No rows to bulk edit');
        return;
    }
    let failed = 0;
    const modal = document.getElementById('bulk-edit-modal');
    if (modal) modal.classList.add('busy');
    setBulkEditStatus(0, paths.length);
    try {
        const batchSize = 200;
        let processed = 0;
        for (let i = 0; i < paths.length; i += batchSize) {
            const batch = paths.slice(i, i + batchSize);
            try {
                const bulkPayload = {
                    paths: batch,
                    updates: payload
                };
                if (hasType) {
                    bulkPayload.media_type = clearMediaType ? '' : mediaType;
                }
                const res = await fetch('/api/bulk_update_metadata', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(bulkPayload)
                });
                const data = await res.json().catch(() => ({}));
                if (!res.ok || data.status !== 'ok') {
                    throw new Error(data.message || `Bulk update failed: ${res.status}`);
                }
            } catch (e) {
                console.error('Bulk edit batch failed', e);
                failed += batch.length;
            } finally {
                processed += batch.length;
                setBulkEditStatus(processed, paths.length);
            }
        }
    } finally {
        if (modal) modal.classList.remove('busy');
    }

    closeBulkEdit();
    loadData();
    if (failed > 0) {
        showToast(`Bulk edit completed with ${failed} failures`, {isError: true});
    } else {
        showToast('Bulk edit applied');
    }
}

async function bulkRescanSelected() {
    if (selectedPaths.size === 0 && masterState !== 2) {
        showToast('Select rows to rescan');
        return;
    }
    const modal = document.getElementById('bulk-edit-modal');
    if (modal) modal.classList.add('busy');
    const paths = await getBulkTargetPaths();
    if (!paths.length) {
        if (modal) modal.classList.remove('busy');
        showToast('No rows to rescan');
        return;
    }
    if (modal) modal.classList.remove('busy');
    closeBulkEdit();
    startInlineRescanProgress(paths.length);
    showToast(`Rescanning ${paths.length} file(s)...`);
    let failed = 0;
    try {
        const result = await rescanPathsInBatches(paths, {
            onProgress: (done, total) => updateInlineRescanProgress(done, total)
        });
        failed = result.failed;
    } catch (e) {
        console.error('Bulk rescan failed', e);
        failed = paths.length;
        showToast(e.message || 'Rescan failed', {isError: true});
        finishInlineRescanProgress();
        loadData();
        return;
    }
    finishInlineRescanProgress();
    loadData();
    if (failed > 0) {
        showToast(`Rescan completed with ${failed} failures`, {isError: true});
    } else {
        showToast('Rescan completed');
    }
}

/** Chunked client calls to POST /api/rescan_files (max 50 paths/request). */
async function rescanPathsInBatches(paths, { onProgress, chunkSize = 20, threads = 2 } = {}) {
    const list = Array.isArray(paths) ? paths.filter(Boolean) : [];
    const total = list.length;
    let failed = 0;
    let processed = 0;
    const size = Math.max(1, Math.min(50, chunkSize | 0 || 20));
    for (let i = 0; i < list.length; i += size) {
        const chunk = list.slice(i, i + size);
        const res = await fetch('/api/rescan_files', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ paths: chunk, threads })
        });
        const data = await res.json().catch(() => ({}));
        if (res.status === 409 || data.status === 'busy') {
            throw new Error(data.message || 'Busy: scan already running');
        }
        if (!res.ok && data.status !== 'ok' && data.status !== 'partial') {
            failed += chunk.length;
        } else {
            failed += Number(data.failed || 0);
        }
        processed += chunk.length;
        if (typeof onProgress === 'function') onProgress(processed, total);
    }
    return { failed, total, ok: total - failed };
}

async function getBulkTargetPaths() {
    if (masterState === 2) {
        const currentFilters = { ...activeFilters };
        currentFilters.search = document.getElementById('search-bar') ? document.getElementById('search-bar').value : '';
        try { currentFilters.category = getFormatFilterValue(); } catch(e) { currentFilters.category = ''; }
        try { currentFilters.volume = getMultiselectValue('vol-filter'); } catch(e) { currentFilters.volume = ''; }
        try { currentFilters.profile = getMultiselectValue('profile-filter'); } catch(e) { currentFilters.profile = ''; }
        try { currentFilters.el = getMultiselectValue('el-filter'); } catch(e) { currentFilters.el = ''; }
        try { currentFilters.container = getMultiselectValue('container-filter'); } catch(e) { currentFilters.container = ''; }
        try { currentFilters.media_type = getMultiselectValue('media-type-filter'); } catch(e) { currentFilters.media_type = ''; }
        try { currentFilters.is_hybrid = getMultiselectValue('hybrid-filter'); } catch(e) { currentFilters.is_hybrid = ''; }
        try { currentFilters.source_hybrid = getMultiselectValue('source-hybrid-filter'); } catch(e) { currentFilters.source_hybrid = ''; }
        try { currentFilters.secondary_hdr = getMultiselectValue('secondary-filter'); } catch(e) { currentFilters.secondary_hdr = ''; }
        try { currentFilters.status = getMultiselectValue('status-filter'); } catch(e) { currentFilters.status = ''; }
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
        try {
            const res = await fetch('/api/filter_paths', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filters: currentFilters })
            });
            const data = await res.json();
            return Array.isArray(data.paths) ? data.paths : [];
        } catch (e) {
            console.error('Failed to fetch filtered paths', e);
            return [];
        }
    }
    return Array.from(selectedPaths);
}

function startInlineRescanProgress(total) {
    document.body.classList.add('scanning');
    const progress = document.getElementById('progress-container');
    const bar = document.getElementById('progress-bar');
    const text = document.getElementById('progress-text');
    const sub = document.getElementById('progress-subtext');
    if (progress) progress.style.opacity = '1';
    if (bar) bar.style.width = '0%';
    if (text) text.textContent = `Rescanning 0/${total}`;
    if (sub) { sub.textContent = 'Bulk rescan in progress'; sub.style.opacity = '1'; }
    const info = document.getElementById('scan-info-box');
    if (info) info.innerText = 'BULK RESCAN';
}

function updateInlineRescanProgress(done, total) {
    const pct = total > 0 ? Math.round((done / total) * 100) : 0;
    const bar = document.getElementById('progress-bar');
    const text = document.getElementById('progress-text');
    if (bar) bar.style.width = `${pct}%`;
    if (text) text.textContent = `Rescanning ${done}/${total}`;
}

function finishInlineRescanProgress() {
    document.body.classList.remove('scanning');
    const sub = document.getElementById('progress-subtext');
    if (sub) sub.style.opacity = '0';
    const info = document.getElementById('scan-info-box');
    if (info) info.innerText = 'IDLE';
}

async function openFoldersModal() {
    await loadFolderVolumes();
    renderScanFolders();
    const modal = document.getElementById('folders-modal');
    if (modal) modal.style.display = 'block';
}

function closeFoldersModal() {
    const modal = document.getElementById('folders-modal');
    if (modal) modal.style.display = 'none';
}

async function loadFolderVolumes() {
    try {
        const res = await fetch('/api/pre_scan_check');
        const data = await res.json();
        folderVolumes = (data || []).filter(v => v.status === 'online');
        const select = document.getElementById('folders-volume-select');
        if (select) {
            select.innerHTML = folderVolumes.map(v => `<option value="${escAttr(v.name)}">${escHtml(v.name)}</option>`).join('');
            if (!folderBrowser.volume && folderVolumes.length > 0) {
                folderBrowser.volume = folderVolumes[0].name;
                folderBrowser.path = '';
            }
            if (folderBrowser.volume) select.value = folderBrowser.volume;
        }
        await browseFolders(folderBrowser.path || '');
    } catch (e) {
        console.error('Failed to load volumes', e);
        showToast('Failed to load volumes', {isError: true});
    }
}

async function onFoldersVolumeChange() {
    const select = document.getElementById('folders-volume-select');
    if (!select) return;
    folderBrowser.volume = select.value;
    folderBrowser.path = '';
    await browseFolders('');
}

async function browseFolders(path) {
    if (!folderBrowser.volume) return;
    try {
        const params = new URLSearchParams({ volume: folderBrowser.volume, path: path || '' });
        const res = await fetch(`/api/browse?${params}`);
        const data = await res.json();
        if (!res.ok || data.status !== 'ok') throw new Error(data.message || 'Browse failed');
        folderBrowser.path = data.path || '';
        const pathEl = document.getElementById('folders-current-path');
        if (pathEl) pathEl.innerText = '/' + (folderBrowser.path || '');
        const listEl = document.getElementById('folders-dir-list');
        if (listEl) {
            if (!data.dirs || data.dirs.length === 0) {
                listEl.innerHTML = '<div style="color:#666; font-size:0.85em;">No subfolders</div>';
            } else {
                listEl.innerHTML = data.dirs.map(d => {
                    const nextPath = (folderBrowser.path ? folderBrowser.path + '/' : '') + d;
                    const encoded = encodeURIComponent(nextPath);
                    return `<div class="folder-row">
                        <div class="folder-path" style="cursor:pointer;" onclick="browseFoldersEncoded('${encoded}')">${escHtml(d)}</div>
                        <div class="folder-actions">
                            <button class="btn-grey folder-item-btn" onclick="browseFoldersEncoded('${encoded}')">Open</button>
                        </div>
                    </div>`;
                }).join('');
            }
        }
    } catch (e) {
        console.error('Browse failed', e);
        showToast('Browse failed', {isError: true});
    }
}

function browseFoldersEncoded(encodedPath) {
    try {
        browseFolders(decodeURIComponent(encodedPath));
    } catch (e) {
        console.error('Invalid folder path', e);
    }
}

function browseFolderUp() {
    if (!folderBrowser.path) return;
    const parts = folderBrowser.path.split('/').filter(Boolean);
    parts.pop();
    browseFolders(parts.join('/'));
}

function addCurrentFolder() {
    if (!folderBrowser.volume) return;
    const path = folderBrowser.path || '';
    const exists = scanFolders.some(f => f.volume === folderBrowser.volume && (f.path || '') === path);
    if (!exists) {
        scanFolders.push({ volume: folderBrowser.volume, path: path, muted: false, type: 'auto' });
        renderScanFolders();
    }
}

function setFolderType(index, type) {
    if (!scanFolders[index]) return;
    scanFolders[index].type = type || 'auto';
}

function toggleFolderMute(index) {
    if (!scanFolders[index]) return;
    scanFolders[index].muted = !scanFolders[index].muted;
    renderScanFolders();
}

function removeScanFolder(index) {
    scanFolders.splice(index, 1);
    renderScanFolders();
}

function renderScanFolders() {
    const list = document.getElementById('folders-scan-list');
    if (!list) return;
    if (!scanFolders.length) {
        list.innerHTML = '<div style="color:#666; font-size:0.85em;">No folders added</div>';
        return;
    }
    list.innerHTML = scanFolders.map((f, idx) => {
        const pathText = `${f.volume}:${f.path ? '/' + f.path : '/'}`;
        const muted = !!f.muted;
        const type = f.type || 'auto';
        return `<div class="folder-row ${muted ? 'muted' : ''}">
            <select class="folder-type-select" onchange="setFolderType(${idx}, this.value)">
                <option value="auto" ${type === 'auto' ? 'selected' : ''}>Auto</option>
                <option value="tv" ${type === 'tv' ? 'selected' : ''}>TV</option>
                <option value="movie" ${type === 'movie' ? 'selected' : ''}>Movie</option>
            </select>
            <div class="folder-path">${escHtml(pathText)}</div>
            <div class="folder-actions">
                <button class="btn-grey folder-item-btn" onclick="toggleFolderMute(${idx})">${muted ? 'Scan' : 'Ignore'}</button>
                <button class="btn-orange folder-item-btn folder-remove-btn" onclick="removeScanFolder(${idx})">-</button>
            </div>
        </div>`;
    }).join('');
}

async function saveScanFolders() {
    try {
        const res = await fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ scan_folders: JSON.stringify(scanFolders) })
        });
        if (!res.ok) throw new Error(`Server Error: ${res.status}`);
        showToast('Scan folders saved');
        closeFoldersModal();
    } catch (e) {
        console.error('Failed to save scan folders', e);
        showToast('Failed to save scan folders', {isError: true});
    }
}
async function closeDetails() { 
    await saveDetailsMediaType();
    await saveDetailsMetadata();
    document.getElementById('details-modal').style.display = 'none'; 
    currentDetailsPath = '';
    currentDetailsMediaType = '';
    currentDetailsMeta = {};
}

async function rescanFile(fullPath) {
    if (!fullPath) {
        showToast("No file selected");
        return;
    }
    try {
        showToast("Rescanning...");
        const res = await fetch('/api/rescan_file', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ full_path: fullPath })
        });
        const data = await res.json();
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || 'Rescan failed');
        }
        showToast("File rescanned");
        closeDetails();
        loadData();
    } catch (e) {
        showToast("Rescan failed", {isError: true});
        console.error("Rescan failed:", e);
    }
}

function rescanFileFromModal() {
    rescanFile(currentDetailsPath);
}

function rescanSelectedFile() {
    if (selectedRowIndex < 0 || !currentRowData[selectedRowIndex]) {
        showToast("No file selected");
        return;
    }
    rescanFile(currentRowData[selectedRowIndex].full_path);
}
function copyDetails() {
    const txt = document.getElementById('det-json').innerText;
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(txt).then(() => {
            showToast("Copied to clipboard!");
        }).catch(err => {
            console.error('Failed to copy:', err);
            // Fallback for older browsers
            const textArea = document.createElement('textarea');
            textArea.value = txt;
            textArea.style.position = 'fixed';
            textArea.style.left = '-999999px';
            document.body.appendChild(textArea);
            textArea.select();
            try {
                document.execCommand('copy');
                showToast("Copied to clipboard!");
            } catch (e) {
                showToast("Failed to copy", {isError: true});
            }
            document.body.removeChild(textArea);
        });
    } else {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = txt;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        document.body.appendChild(textArea);
        textArea.select();
        try {
            document.execCommand('copy');
            showToast("Copied to clipboard!");
        } catch (e) {
            showToast("Failed to copy", {isError: true});
        }
        document.body.removeChild(textArea);
    }
}
