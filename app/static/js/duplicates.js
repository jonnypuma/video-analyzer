function setDuplicatesStatus(msg) {
    const el = document.getElementById('duplicates-status');
    if (el) el.textContent = msg || '';
}

function openDuplicatesModal() {
    closeContextMenu();
    duplicateSelectedPaths = new Set();
    duplicateMembers = [];
    duplicateActiveGroupId = '';
    const modal = document.getElementById('duplicates-modal');
    if (modal) modal.style.display = 'block';
    refreshDuplicateGroups();
}

function closeDuplicatesModal() {
    const modal = document.getElementById('duplicates-modal');
    if (modal) modal.style.display = 'none';
    // Drop modal tables so they cannot steal document.querySelector('thead') if any leftover callers exist.
    const container = document.getElementById('duplicates-groups-container');
    if (container) container.innerHTML = '';
    duplicateGroups = [];
    duplicateMembers = [];
    duplicateActiveGroupId = '';
    duplicateSelectedPaths = new Set();
}

async function openDuplicateGroupFromRow(groupKey, exactKey) {
    const logicalKey = (groupKey || '').trim();
    const exact = (exactKey || '').trim();
    const groupId = logicalKey
        ? `logical|${logicalKey}`
        : (exact ? `exact|${exact}` : '');
    if (!groupId) {
        showToast('No duplicate group key available for this row');
        return;
    }
    openDuplicatesModal();
    await loadDuplicateMembers(groupId);
}

function getDuplicatesFilterPayload() {
    const applyEl = document.getElementById('dup-apply-filters');
    const applyFilters = !!(applyEl && applyEl.checked);
    if (!applyFilters) return {};
    const snap = typeof getCurrentFilterSnapshot === 'function' ? getCurrentFilterSnapshot() : { ...activeFilters };
    // Drop empty values so the API treats this as an unfiltered query when nothing is set.
    const cleaned = {};
    Object.keys(snap || {}).forEach((key) => {
        const val = snap[key];
        if (val === null || val === undefined) return;
        if (typeof val === 'string' && val.trim() === '') return;
        cleaned[key] = val;
    });
    return cleaned;
}

async function rebuildDuplicates(includeExact = false) {
    setDuplicatesStatus(includeExact ? 'Rebuilding keys + exact fingerprints...' : 'Rebuilding keys...');
    try {
        const res = await fetch('/api/duplicates/rebuild', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                filters: getDuplicatesFilterPayload(),
                include_exact: !!includeExact
            })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || 'Failed rebuilding duplicate keys');
        }
        showToast(`Duplicate keys rebuilt (${data.updated || 0} rows)`);
        await refreshDuplicateGroups(true);
    } catch (e) {
        console.error('Failed to rebuild duplicates', e);
        showToast('Failed to rebuild duplicates', {isError: true});
        setDuplicatesStatus('Rebuild failed');
    }
}

async function refreshDuplicateGroups(showNotice = false) {
    setDuplicatesStatus('Loading duplicate groups...');
    try {
        const filters = getDuplicatesFilterPayload();
        const res = await fetch('/api/duplicates/groups', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filters })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || 'Failed loading duplicate groups');
        }
        duplicateGroups = Array.isArray(data.groups) ? data.groups : [];
        renderDuplicateGroups();
        const scope = data.filters_applied ? ' (table filters)' : ' (entire library)';
        if (showNotice) showToast(`Duplicate groups refreshed (${duplicateGroups.length})`);
        setDuplicatesStatus(`${duplicateGroups.length} duplicate groups${scope}`);
        if (duplicateActiveGroupId) {
            const stillExists = duplicateGroups.some(g => g.group_id === duplicateActiveGroupId);
            if (stillExists) {
                await loadDuplicateMembers(duplicateActiveGroupId);
            } else {
                duplicateActiveGroupId = '';
                duplicateMembers = [];
                duplicateSelectedPaths = new Set();
                renderDuplicateGroups();
            }
        }
    } catch (e) {
        console.error('Failed to load duplicate groups', e);
        showToast('Failed to load duplicate groups', {isError: true});
        setDuplicatesStatus('Load failed');
    }
}

function renderDuplicateGroups() {
    const container = document.getElementById('duplicates-groups-container');
    if (!container) return;
    if (!duplicateGroups.length) {
        container.innerHTML = '<div style="color:#888; font-size:0.9em; padding:8px 0;">No duplicate groups found for current filters.</div>';
        return;
    }
    const sortedGroups = getSortedDuplicateGroups();
    container.innerHTML = `
        <table class="duplicates-table">
            <thead>
                <tr>
                    <th class="dup-sortable" onclick="sortDuplicateGroups('group_type')">Type${dupSortMark('group', 'group_type')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateGroups('match_basis')">Match Basis${dupSortMark('group', 'match_basis')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateGroups('title')">Title${dupSortMark('group', 'title')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateGroups('file_count')">Count${dupSortMark('group', 'file_count')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateGroups('total_size')">Total Size${dupSortMark('group', 'total_size')}</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                ${sortedGroups.map(group => {
                    const groupId = group.group_id || '';
                    const isOpen = groupId === duplicateActiveGroupId;
                    const action = isOpen ? 'Hide Files' : 'View Files';
                    return `
                    <tr id="dup-group-row-${escAttr(groupId)}">
                        <td>${escHtml(group.group_type || '')}</td>
                        <td>${escHtml(group.match_basis || '')}</td>
                        <td>${escHtml(group.title || '')}</td>
                        <td>${escHtml(group.file_count || 0)}</td>
                        <td>${formatBytes(group.total_size || 0)}</td>
                        <td><button class="btn-blue dup-action-btn" onclick="toggleDuplicateMembers(decodeURIComponent('${escAttr(encodeURIComponent(groupId))}'))">${action}</button></td>
                    </tr>
                    ${isOpen ? `<tr><td colspan="6" style="background:#0b0b0b;">${renderDuplicateMembersInline()}</td></tr>` : ''}
                `;
                }).join('')}
            </tbody>
        </table>
    `;
}

function dupSortMark(which, col) {
    const cur = which === 'group' ? dupGroupSortCol : dupMemberSortCol;
    const ord = which === 'group' ? dupGroupSortOrder : dupMemberSortOrder;
    if (cur !== col) return '<span class="dup-sort-ind">⇅</span>';
    return `<span class="dup-sort-ind active">${ord === 'asc' ? '▲' : '▼'}</span>`;
}

function sortDuplicateGroups(col) {
    if (!col) return;
    if (dupGroupSortCol === col) {
        dupGroupSortOrder = dupGroupSortOrder === 'asc' ? 'desc' : 'asc';
    } else {
        dupGroupSortCol = col;
        dupGroupSortOrder = (col === 'title' || col === 'group_type' || col === 'match_basis') ? 'asc' : 'desc';
    }
    renderDuplicateGroups();
}

function sortDuplicateMembers(col) {
    if (!col) return;
    if (dupMemberSortCol === col) {
        dupMemberSortOrder = dupMemberSortOrder === 'asc' ? 'desc' : 'asc';
    } else {
        dupMemberSortCol = col;
        const textFirst = ['filename', 'full_path', 'source_vol', 'category', 'secondary_hdr', 'audio_codecs', 'video_codec', 'source_format', 'resolution', 'primary_hdr', 'last_scanned'];
        dupMemberSortOrder = textFirst.includes(col) ? 'asc' : 'desc';
    }
    renderDuplicateGroups();
}

function getSortedDuplicateGroups() {
    const list = Array.isArray(duplicateGroups) ? duplicateGroups.slice() : [];
    const col = dupGroupSortCol || 'file_count';
    const dir = dupGroupSortOrder === 'asc' ? 1 : -1;
    list.sort((a, b) => {
        let va = a ? a[col] : '';
        let vb = b ? b[col] : '';
        if (col === 'file_count' || col === 'total_size') {
            va = Number(va) || 0;
            vb = Number(vb) || 0;
            return (va - vb) * dir;
        }
        va = String(va || '').toLowerCase();
        vb = String(vb || '').toLowerCase();
        if (va < vb) return -1 * dir;
        if (va > vb) return 1 * dir;
        return 0;
    });
    return list;
}

function formatDupBitrate(mbps) {
    if (mbps === null || mbps === undefined || mbps === '') return '—';
    const n = Number(mbps);
    if (!Number.isFinite(n)) return escHtml(String(mbps));
    return `${n % 1 === 0 ? n.toFixed(0) : n.toFixed(1)} Mbps`;
}

function formatDupPrimaryHdr(member) {
    const cat = (member && member.category) || '';
    if (!cat) return '—';
    let label = cat;
    if (cat === 'dovi' || cat === 'dolby_vision') {
        const parts = ['DoVi'];
        if (member.profile) parts.push(`P${member.profile}`);
        if (member.el_type) parts.push(member.el_type);
        label = parts.join(' ');
    } else if (cat === 'hdr10plus') label = 'HDR10+';
    else if (cat === 'hdr10') label = 'HDR10';
    else if (cat === 'hlg') label = 'HLG';
    else if (cat === 'sdr_only') label = 'SDR';
    return label;
}

function getSortedDuplicateMembers() {
    const list = Array.isArray(duplicateMembers) ? duplicateMembers.slice() : [];
    if (!dupMemberSortCol) return list;
    const col = dupMemberSortCol;
    const dir = dupMemberSortOrder === 'asc' ? 1 : -1;
    list.sort((a, b) => {
        let va;
        let vb;
        if (col === 'primary_hdr') {
            va = formatDupPrimaryHdr(a).toLowerCase();
            vb = formatDupPrimaryHdr(b).toLowerCase();
        } else if (col === 'file_size' || col === 'bitrate_mbps') {
            va = Number(a && a[col]) || 0;
            vb = Number(b && b[col]) || 0;
            return (va - vb) * dir;
        } else {
            va = String((a && a[col]) || '').toLowerCase();
            vb = String((b && b[col]) || '').toLowerCase();
        }
        if (va < vb) return -1 * dir;
        if (va > vb) return 1 * dir;
        return 0;
    });
    return list;
}

async function toggleDuplicateMembers(groupId) {
    if (duplicateActiveGroupId && duplicateActiveGroupId === groupId) {
        duplicateActiveGroupId = '';
        duplicateMembers = [];
        duplicateSelectedPaths = new Set();
        dupMemberSortCol = '';
        renderDuplicateGroups();
        setDuplicatesStatus(`${duplicateGroups.length} duplicate groups`);
        return;
    }
    await loadDuplicateMembers(groupId);
}

async function loadDuplicateMembers(groupId) {
    duplicateActiveGroupId = groupId || '';
    duplicateSelectedPaths = new Set();
    duplicateMembers = [];
    dupMemberSortCol = '';
    renderDuplicateGroups();
    setDuplicatesStatus('Loading duplicate members...');
    try {
        const res = await fetch('/api/duplicates/members', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ group_id: duplicateActiveGroupId })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || 'Failed loading duplicate members');
        }
        duplicateMembers = Array.isArray(data.members) ? data.members : [];
        renderDuplicateGroups();
        setDuplicatesStatus(`${duplicateMembers.length} files in selected group`);
    } catch (e) {
        console.error('Failed to load duplicate members', e);
        showToast('Failed to load duplicate members', {isError: true});
        setDuplicatesStatus('Member load failed');
    }
}

function toggleDuplicatePath(path, checked) {
    if (!path) return;
    if (checked) duplicateSelectedPaths.add(path);
    else duplicateSelectedPaths.delete(path);
}

function toggleDuplicateAll(checked) {
    duplicateSelectedPaths = new Set();
    if (checked) {
        duplicateMembers.forEach(member => {
            if (member.full_path) duplicateSelectedPaths.add(member.full_path);
        });
    }
    renderDuplicateGroups();
}

async function copyTextToClipboard(text) {
    if (!text) return false;
    if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(text);
        return true;
    }
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    document.body.appendChild(textArea);
    textArea.select();
    try {
        const ok = document.execCommand('copy');
        document.body.removeChild(textArea);
        return !!ok;
    } catch (e) {
        document.body.removeChild(textArea);
        return false;
    }
}

async function copyDuplicatePath(path) {
    try {
        const ok = await copyTextToClipboard(path || '');
        showToast(ok ? 'Path copied' : 'Failed to copy path', {isError: !ok});
    } catch (e) {
        showToast('Failed to copy path', {isError: true});
    }
}

function getFolderPath(path) {
    const text = String(path || '');
    const idx = Math.max(text.lastIndexOf('/'), text.lastIndexOf('\\'));
    return idx > 0 ? text.slice(0, idx) : text;
}

async function copyDuplicateFolder(path) {
    await copyDuplicatePath(getFolderPath(path));
}

async function copySelectedDuplicatePaths() {
    if (!duplicateSelectedPaths.size) {
        showToast('Select duplicate rows first');
        return;
    }
    try {
        const ok = await copyTextToClipboard(Array.from(duplicateSelectedPaths).join('\n'));
        showToast(ok ? `Copied ${duplicateSelectedPaths.size} paths` : 'Failed to copy paths', {isError: !ok});
    } catch (e) {
        showToast('Failed to copy paths', {isError: true});
    }
}

async function rescanSelectedDuplicates() {
    const paths = Array.from(duplicateSelectedPaths);
    if (!paths.length) {
        showToast('Select duplicate rows to rescan');
        return;
    }
    showToast(`Rescanning ${paths.length} file(s)...`);
    try {
        const { failed, total } = await rescanPathsInBatches(paths);
        if (failed > 0) showToast(`Rescan done with ${failed}/${total} failures`, {isError: true});
        else showToast(`Rescanned ${total} file(s)`);
    } catch (e) {
        showToast(e.message || 'Rescan failed', {isError: true});
    }
    await refreshDuplicateGroups();
    loadData();
}

async function deleteSelectedDuplicates() {
    const paths = Array.from(duplicateSelectedPaths);
    if (!paths.length) {
        showToast('Select duplicate rows to delete');
        return;
    }
    openDupDeleteModal(paths);
}

let pendingDupDeletePaths = [];

function openDupDeleteModal(paths) {
    pendingDupDeletePaths = Array.isArray(paths) ? paths.slice() : [];
    const countEl = document.getElementById('dup-del-count');
    if (countEl) countEl.innerText = String(pendingDupDeletePaths.length);
    const filesChk = document.getElementById('dup-del-files');
    const foldersChk = document.getElementById('dup-del-folders');
    if (filesChk) filesChk.checked = false;
    if (foldersChk) { foldersChk.checked = false; foldersChk.disabled = true; }
    const preview = document.getElementById('dup-del-preview');
    if (preview) preview.style.display = 'none';
    const err = document.getElementById('dup-del-error');
    if (err) { err.style.display = 'none'; err.textContent = ''; }
    const modal = document.getElementById('dup-delete-modal');
    if (!modal) {
        showToast('Delete confirm dialog missing — hard-refresh the page', {isError: true});
        return;
    }
    modal.style.display = 'block';
}

function closeDupDeleteModal() {
    document.getElementById('dup-delete-modal').style.display = 'none';
    pendingDupDeletePaths = [];
}

async function onDupDeleteOptionsChange() {
    const filesChk = document.getElementById('dup-del-files');
    const foldersChk = document.getElementById('dup-del-folders');
    const preview = document.getElementById('dup-del-preview');
    const list = document.getElementById('dup-del-folder-list');
    if (!filesChk || !foldersChk) return;
    foldersChk.disabled = !filesChk.checked;
    if (!filesChk.checked) {
        foldersChk.checked = false;
        if (preview) preview.style.display = 'none';
        return;
    }
    if (!foldersChk.checked) {
        if (preview) preview.style.display = 'none';
        return;
    }
    if (preview) preview.style.display = 'block';
    if (list) list.innerHTML = 'Loading preview…';
    try {
        const res = await fetch('/api/delete/preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ paths: pendingDupDeletePaths })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.message || 'Preview failed');
        const folders = data.folders || [];
        if (!folders.length) {
            list.innerHTML = '<div class="blocked">No parent folders to evaluate.</div>';
            return;
        }
        list.innerHTML = folders.map(f => {
            const cls = f.ok ? 'ok' : 'blocked';
            const mark = f.ok ? 'WILL DELETE' : 'SKIP';
            const reason = f.reason ? ` — ${escHtml(f.reason)}` : '';
            return `<div class="${cls}"><strong>${mark}</strong>: ${escHtml(f.path || '')}${reason}</div>`;
        }).join('');
    } catch (e) {
        if (list) list.innerHTML = `<div class="blocked">${escHtml(e.message || 'Preview failed')}</div>`;
    }
}

async function confirmDupDelete() {
    const paths = pendingDupDeletePaths.slice();
    if (!paths.length) {
        closeDupDeleteModal();
        return;
    }
    const deleteFiles = !!(document.getElementById('dup-del-files') || {}).checked;
    const deleteFolders = !!(document.getElementById('dup-del-folders') || {}).checked;
    const btn = document.getElementById('dup-del-confirm-btn');
    if (btn) btn.disabled = true;
    const errEl = document.getElementById('dup-del-error');
    try {
        const res = await fetch('/api/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                paths,
                delete_files_on_disk: deleteFiles,
                delete_folders: deleteFolders
            })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.message || 'Delete failed');
        const parts = [`Removed ${data.count || 0} DB row(s)`];
        if (deleteFiles) parts.push(`${data.files_deleted || 0} file(s)`);
        if (deleteFolders) parts.push(`${data.folders_deleted || 0} folder(s)`);
        const skipped = (data.folders_skipped || []).length;
        const diskErr = (data.disk_errors || []).length;
        if (skipped) parts.push(`${skipped} folder(s) skipped`);
        if (diskErr) parts.push(`${diskErr} disk error(s)`);
        showToast(parts.join(' · '), { isError: diskErr > 0 });
        closeDupDeleteModal();
        duplicateSelectedPaths = new Set();
        await refreshDuplicateGroups();
        if (duplicateActiveGroupId) await loadDuplicateMembers(duplicateActiveGroupId);
        loadData();
    } catch (e) {
        console.error('Failed to delete duplicate rows', e);
        if (errEl) {
            errEl.style.display = 'block';
            errEl.textContent = e.message || 'Delete failed';
        }
        showToast(e.message || 'Delete failed', {isError: true});
    } finally {
        if (btn) btn.disabled = false;
    }
}

function renderDuplicateMembersInline() {
    if (!duplicateActiveGroupId) return '';
    if (!duplicateMembers.length) {
        return '<div style="color:#888; font-size:0.9em; padding:8px 0;">Loading files...</div>';
    }
    const members = getSortedDuplicateMembers();
    const allChecked = members.length > 0 && members.every(member => duplicateSelectedPaths.has(member.full_path));
    return `
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:8px;">
            <div style="color:#9aa; font-size:0.82em;">${members.length} file(s) in group</div>
            <div>
                <button class="btn-grey dup-action-btn" onclick="copySelectedDuplicatePaths()">Copy Selected Paths</button>
                <button class="btn-orange dup-action-btn" onclick="rescanSelectedDuplicates()">Rescan Selected</button>
                <button class="btn-red dup-action-btn" onclick="deleteSelectedDuplicates()">Delete Selected…</button>
            </div>
        </div>
        <p class="dup-delete-hint">Tick row checkboxes, then Delete Selected… — a confirm dialog offers optional delete-on-disk / delete-folder.</p>
        <div class="duplicates-members-scroll">
        <table class="duplicates-table duplicates-members-table">
            <thead>
                <tr>
                    <th><input type="checkbox" ${allChecked ? 'checked' : ''} onchange="toggleDuplicateAll(this.checked)"></th>
                    <th>Keep</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('filename')">Filename${dupSortMark('member', 'filename')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('full_path')">Path${dupSortMark('member', 'full_path')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('source_vol')">Volume${dupSortMark('member', 'source_vol')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('file_size')">Size${dupSortMark('member', 'file_size')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('resolution')">Res${dupSortMark('member', 'resolution')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('bitrate_mbps')">Bitrate${dupSortMark('member', 'bitrate_mbps')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('primary_hdr')">Primary HDR${dupSortMark('member', 'primary_hdr')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('secondary_hdr')">Secondary HDR${dupSortMark('member', 'secondary_hdr')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('audio_codecs')">Audio${dupSortMark('member', 'audio_codecs')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('video_codec')">Codec${dupSortMark('member', 'video_codec')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('source_format')">Format${dupSortMark('member', 'source_format')}</th>
                    <th class="dup-sortable" onclick="sortDuplicateMembers('last_scanned')">Last Scanned${dupSortMark('member', 'last_scanned')}</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                ${members.map(member => `
                    <tr>
                        <td><input type="checkbox" ${duplicateSelectedPaths.has(member.full_path) ? 'checked' : ''} onchange="toggleDuplicatePath(decodeURIComponent('${escAttr(encodeURIComponent(member.full_path || ''))}'), this.checked)"></td>
                        <td>${member.keep_recommended ? '<span class="dup-recommended">KEEP</span>' : ''}</td>
                        <td>${escHtml(member.filename || '')}</td>
                        <td title="${escAttr(member.full_path || '')}" style="max-width:280px; word-break:break-all;">${escHtml(member.full_path || '')}</td>
                        <td>${escHtml(member.source_vol || '')}</td>
                        <td>${formatBytes(member.file_size || 0)}</td>
                        <td>${escHtml(member.resolution || '')}</td>
                        <td>${formatDupBitrate(member.bitrate_mbps)}</td>
                        <td>${escHtml(formatDupPrimaryHdr(member))}</td>
                        <td>${escHtml(member.secondary_hdr || '—')}</td>
                        <td title="${escAttr(member.audio_codecs || '')}" style="max-width:160px; word-break:break-word;">${escHtml(member.audio_codecs || '—')}</td>
                        <td>${escHtml(member.video_codec || '')}</td>
                        <td>${escHtml(member.source_format || '')}</td>
                        <td>${escHtml(member.last_scanned || '')}</td>
                        <td>
                            <button class="btn-grey dup-action-btn" onclick="copyDuplicatePath(decodeURIComponent('${escAttr(encodeURIComponent(member.full_path || ''))}'))">Copy Path</button>
                            <button class="btn-grey dup-action-btn" onclick="copyDuplicateFolder(decodeURIComponent('${escAttr(encodeURIComponent(member.full_path || ''))}'))">Copy Folder</button>
                            <button class="btn-orange dup-action-btn" onclick="rescanFile(decodeURIComponent('${escAttr(encodeURIComponent(member.full_path || ''))}'))">Rescan</button>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
        </div>
    `;
}
