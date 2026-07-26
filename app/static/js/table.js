// --- COLUMN TOGGLE LOGIC ---
function updateColMenuPosition() {
    const menu = document.getElementById('colMenu');
    const rightPanel = menu?.querySelector('.col-menu-right-panel');
    const scrollableArea = menu?.querySelector('.col-menu-scrollable');
    const leftPanel = menu?.querySelector('.col-menu-left-panel');
    if (menu && rightPanel && scrollableArea && leftPanel) {
        const rightPanelWidth = rightPanel.offsetWidth;
        const leftPanelWidth = leftPanel.offsetWidth;
        const menuWidth = menu.offsetWidth;
        // Calculate max width for scrollable area: menu width - left panel - right panel - padding
        const maxScrollWidth = Math.max(100, menuWidth - leftPanelWidth - rightPanelWidth - 20); // 20px for margins/padding
        // Constrain scrollable area so it doesn't extend under the right panel
        scrollableArea.style.maxWidth = maxScrollWidth + 'px';
        scrollableArea.style.flexShrink = '1';
    }
}

function toggleColMenu() { 
    const menu = document.getElementById('colMenu');
    const btn = document.querySelector('#burger-btn .settings-toggle.table-mode');
    ensureColMenuInScrollport();
    const isActive = menu.classList.toggle('active');
    if (isActive) { 
        syncColMenuPosition();
        setTimeout(() => updateColMenuPosition(), 0);
        btn.classList.add('active'); 
        // Keep picker visible: jump scrollport to top so sticky menu + header are in view together.
        const host = getTableScrollHost();
        if (host) host.scrollTop = 0;
    } else { 
        btn.classList.remove('active'); 
        saveSettings(false); 
        syncColMenuPosition();
    }
}

function toggleMainMenu() {
    const menu = document.getElementById('mainMenu');
    const btn = document.getElementById('btn-main-menu');
    const isActive = menu.classList.toggle('active');
    if (isActive) {
        btn.classList.add('active');
    } else {
        btn.classList.remove('active');
    }
}

// Update sticky freeze-pane height / offsets on window resize and page scroll
window.addEventListener('resize', () => {
    updateStickyHeader();
    const menu = document.getElementById('colMenu');
    if (menu && menu.classList.contains('active')) {
        updateColMenuPosition();
    }
    // NEVER recalculate on window resize - preserve manual adjustments
    // If user wants to recalculate, they can toggle a column
});
let _stickyScrollRaf = 0;
window.addEventListener('scroll', () => {
    if (_stickyScrollRaf) return;
    _stickyScrollRaf = requestAnimationFrame(() => {
        _stickyScrollRaf = 0;
        // Offsets/menus only — do NOT grow table max-height on page scroll
        // (that pushed the console forever out of reach and broke jump-to-log).
        updateStickyOffsets();
        syncColMenuPosition();
        syncOpenMultiselectPositions();
    });
}, { passive: true });

function toggleCol(chk) {
    const target = chk.getAttribute('data-target');
    const table = document.querySelector('.table-wrap');
    if (chk.checked) { 
        table.classList.remove('hide-' + target); 
    } else { 
        table.classList.add('hide-' + target); 
    }
    applyCollapsedColumnCellStyles();
    requestAnimationFrame(() => { syncTableColgroup(); updateStickyHeader(); });
    console.log('[COLUMN_WIDTHS] Column toggled (colgroup sync, no stored width change):', target, 'checked:', chk.checked);
}

function applyVisibleCols(colList, skipRecalc = false) {
    if(!colList) return;
    const items = document.querySelectorAll('.col-chk-input');
    const table = document.querySelector('.table-wrap');
    const visibleSet = new Set(colList.split(','));
    
    items.forEach(chk => {
        const target = chk.getAttribute('data-target');
        if (visibleSet.has(target)) { chk.checked = true; table.classList.remove('hide-' + target); } 
        else { chk.checked = false; table.classList.add('hide-' + target); }
    });
    applyCollapsedColumnCellStyles();
    requestAnimationFrame(() => { syncTableColgroup(); updateStickyHeader(); });
}

function getVisibleCols() {
    const visible = [];
    document.querySelectorAll('.col-chk-input').forEach(chk => {
        if(chk.checked) visible.push(chk.getAttribute('data-target'));
    });
    return visible.join(',');
}

function getSortOrder() {
    return `${sortCol}:${sortOrder}`;
}

function setSortOrder(sortStr) {
    if (!sortStr) return;
    const parts = sortStr.split(':');
    if (parts.length === 2) {
        sortCol = parts[0];
        sortOrder = parts[1];
        // Update sort icons
        document.querySelectorAll('.sort-icon').forEach(e => e.innerText = '');
        const arrow = sortOrder === 'asc' ? '▲' : '▼';
        const icon = document.getElementById('sort-' + sortCol);
        if(icon) icon.innerText = arrow;
    }
}

function getColumnOrder() {
    const headers = getVideoTableHeaderCells();
    const order = [];
    headers.forEach(th => {
        const classes = Array.from(th.classList);
        const colClass = classes.find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
        if (colClass) order.push(colClass);
    });
    return order;
}

function applyColumnOrder(order) {
    if (!order || !order.length) return;
    const headerTable = getHeaderTable();
    const dataTable = getDataTable();
    if (!headerTable || !dataTable) return;
    const headerRow = headerTable.querySelector('thead tr');
    const bodyRows = dataTable.querySelectorAll('tbody tr');

    const reorderRow = (row) => {
        const cells = Array.from(row.children);
        const cellMap = {};
        cells.forEach(cell => {
            const classes = Array.from(cell.classList);
            if (classes.includes('col-chk')) cellMap['col-chk'] = cell;
            if (classes.includes('col-del')) cellMap['col-del'] = cell;
            const colClass = classes.find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
            if (colClass) cellMap[colClass] = cell;
        });
        row.innerHTML = '';
        if (cellMap['col-chk']) row.appendChild(cellMap['col-chk']);
        order.forEach(col => {
            if (cellMap[col]) row.appendChild(cellMap[col]);
        });
        // Append any new columns not present in saved order (keeps them visible)
        cells.forEach(cell => {
            const classes = Array.from(cell.classList);
            const colClass = classes.find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
            if (colClass && !order.includes(colClass) && cellMap[colClass]) {
                row.appendChild(cellMap[colClass]);
                delete cellMap[colClass];
            }
        });
        if (cellMap['col-del']) row.appendChild(cellMap['col-del']);
    };

    if (headerRow) reorderRow(headerRow);
    bodyRows.forEach(row => reorderRow(row));
    _stickyHeaderColWidths = null;
    requestAnimationFrame(() => {
        syncTableColgroup();
        updateStickyHeader();
    });
}

/** Force tbody column order to exactly match current header cell sequence (index-for-index). */
function alignBodyColumnsToHeaderOrder() {
    const headerRow = document.querySelector('#video-table thead tr');
    const tbody = document.getElementById('video-table-body');
    if (!headerRow || !tbody) return;
    const headerKeys = Array.from(headerRow.children).map(th => {
        if (th.classList.contains('col-chk')) return 'col-chk';
        if (th.classList.contains('col-del')) return 'col-del';
        return Array.from(th.classList).find(c => c.startsWith('col-')) || '';
    });
    if (!headerKeys.length) return;
    tbody.querySelectorAll('tr').forEach(row => {
        const cells = Array.from(row.children);
        if (!cells.length || cells.length !== headerKeys.length) return;
        const cellMap = {};
        cells.forEach(td => {
            if (td.classList.contains('col-chk')) cellMap['col-chk'] = td;
            else if (td.classList.contains('col-del')) cellMap['col-del'] = td;
            else {
                const cls = Array.from(td.classList).find(c => c.startsWith('col-'));
                if (cls) cellMap[cls] = td;
            }
        });
        // Rebuild row strictly by header order.
        row.innerHTML = '';
        headerKeys.forEach(k => {
            const td = cellMap[k];
            if (td) row.appendChild(td);
        });
    });
}

function initColumnDrag() {
    if (columnDragInitialized) return;
    const headers = getVideoTableHeaderCells();
    headers.forEach(th => {
        const classes = Array.from(th.classList);
        const colClass = classes.find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
        if (!colClass) return;
        th.draggable = true;
        th.style.cursor = 'grab';
        th.addEventListener('dragstart', (e) => {
            const isInteractiveTarget = !!e.target.closest('input, select, button, .multiselect-button, .multiselect-dropdown, .th-filter-row, .col-resize-handle');
            if (suppressHeaderDrag || isResizing || columnResizeActive || isInteractiveTarget) {
                e.preventDefault();
                return;
            }
            th.classList.add('dragging');
            e.dataTransfer.setData('text/plain', colClass);
        });
        th.addEventListener('dragend', () => {
            th.classList.remove('dragging');
        });
        th.addEventListener('dragover', (e) => {
            e.preventDefault();
        });
        th.addEventListener('drop', (e) => {
            e.preventDefault();
            const source = e.dataTransfer.getData('text/plain');
            const target = colClass;
            if (!source || !target || source === target) return;
            const currentOrder = getColumnOrder();
            const nextOrder = currentOrder.filter(c => c !== source);
            const targetIndex = nextOrder.indexOf(target);
            if (targetIndex >= 0) {
                nextOrder.splice(targetIndex, 0, source);
                applyColumnOrder(nextOrder);
                savedColumnOrder = nextOrder;
                saveSettings(false);
            }
        });
    });
    columnDragInitialized = true;
}

function updateTableScrollHeight() {
    const host = getTableScrollHost();
    if (!host) return;
    // Height must be stable across page scroll. Using live getBoundingClientRect().top
    // grew the table as the page scrolled and pushed .console-wrap out of reach.
    const pageNav = document.querySelector('.page-nav-wrap');
    const navH = pageNav ? Math.ceil(pageNav.getBoundingClientRect().height) : 72;
    const bottomReserve = navH + 240; // pagination + activity-log peek
    const ribbon = document.querySelector('.summary-ribbon');
    const controlStack = document.querySelector('.control-stack');
    const stickyChrome = (ribbon ? ribbon.offsetHeight : 0) + (controlStack ? controlStack.offsetHeight : 0);
    const available = window.innerHeight - stickyChrome - bottomReserve;
    const h = Math.max(240, Math.min(Math.floor(available), Math.floor(window.innerHeight * 0.7)));
    host.style.maxHeight = h + 'px';
    document.documentElement.style.setProperty('--table-scroll-max-height', `${h}px`);
}

function updateStickyHeader() {
    updateStickyOffsets();
    updateTableScrollHeight();
    syncColMenuPosition();
    syncOpenMultiselectPositions();
    if (formatMultiselectOpen) {
        const dropdown = document.getElementById('format-filter-dropdown');
        const button = document.getElementById('format-filter-button');
        positionFormatDropdown(dropdown, button);
    }
}

function updateStickyOffsets() {
    const ribbon = document.querySelector('.summary-ribbon');
    const controlStack = document.querySelector('.control-stack');
    const ribbonH = ribbon ? ribbon.offsetHeight : 0;
    const controlH = controlStack ? controlStack.offsetHeight : 0;
    document.documentElement.style.setProperty('--sticky-action-top', `${ribbonH}px`);
    document.documentElement.style.setProperty('--sticky-controls-top', `${ribbonH + controlH}px`);
}

let _stickyHeaderColWidths = null;
function syncTableColgroup(onlyUpdateColClass, newWidth, onlyUpdateColIndex) {
    const headerRow = getVideoTableHeaderRow();
    if (!getHeaderTable() || !headerRow) return;
    // Live resize: full logical rebuild with one column overridden (old partial path required _stickyHeaderColWidths and often skipped the drag).
    if (onlyUpdateColClass && typeof newWidth === 'number') {
        rebuildColgroupFromStoredColumnWidths(onlyUpdateColClass, newWidth, onlyUpdateColIndex);
        return;
    }
    // Full rebuild: global column_widths (+ defaults) only — never measure th.offsetWidth (causes drift on pin/filter).
    rebuildColgroupFromStoredColumnWidths(null, null, null);
}
function getTableContentWidth() {
    const colgroup = document.getElementById('table-colgroup');
    if (colgroup && colgroup.children.length > 0) {
        let sum = 0;
        for (let i = 0; i < colgroup.children.length; i++) {
            const w = parseFloat(colgroup.children[i].style.width) || 0;
            sum += w;
        }
        if (sum > 0) return sum;
    }
    return _stickyHeaderColWidths
        ? _stickyHeaderColWidths.reduce((a, b) => a + Math.max(0, Number(b) || 0), 0)
        : 0;
}
function clampTableScroll() {
    const host = getTableScrollHost();
    const dataTable = getDataTable();
    if (!host) return;
    const contentWidth = getTableContentWidth() || (dataTable ? dataTable.scrollWidth : 0);
    const maxScroll = Math.max(0, contentWidth - host.clientWidth);
    if (host.scrollLeft > maxScroll) host.scrollLeft = maxScroll;
}
function ensureColMenuInScrollport() {
    const menu = document.getElementById('colMenu');
    const host = getTableScrollHost();
    if (!menu || !host) return;
    if (menu.parentElement !== host) {
        host.insertBefore(menu, host.firstChild);
    }
}

function syncColMenuPosition() {
    const menu = document.getElementById('colMenu');
    const tableWrap = document.querySelector('.table-wrap');
    const host = getTableScrollHost();
    if (!menu || !tableWrap) return;
    ensureColMenuInScrollport();
    // Clear legacy fixed-position inline styles from older layout.
    menu.style.position = '';
    menu.style.top = '';
    menu.style.left = '';
    menu.style.right = '';
    menu.style.width = '';
    menu.style.transform = '';
    menu.style.zIndex = '';
    if (!menu.classList.contains('active')) {
        tableWrap.classList.remove('col-menu-open');
        return;
    }
    tableWrap.classList.add('col-menu-open');
    // Span the visible scrollport between frozen chk/del columns.
    const hostWidth = host ? host.clientWidth : tableWrap.clientWidth;
    const menuWidth = Math.max(0, hostWidth - 80);
    menu.style.width = `${Math.round(menuWidth)}px`;
    menu.style.left = '40px';
}

function initHeaderScrollbar() {
    const hScroll = getTableScrollHost();
    const headerScrollbar = document.getElementById('header-scrollbar');
    const headerScrollbarInner = document.getElementById('header-scrollbar-inner');
    const dataTable = getDataTable();
    if (!hScroll || !headerScrollbar || !headerScrollbarInner || !dataTable) return;
    if (hScroll.dataset.headerBarInit === '1') return;
    hScroll.dataset.headerBarInit = '1';
    let syncing = false;
    const recalc = () => {
        const contentWidth = getTableContentWidth() || dataTable.scrollWidth || 0;
        headerScrollbarInner.style.width = `${Math.max(0, Math.round(contentWidth))}px`;
    };
    const syncFromTable = () => {
        if (syncing) return;
        syncing = true;
        headerScrollbar.scrollLeft = hScroll.scrollLeft;
        syncing = false;
        // Keep open filter dropdowns / column menu aligned while the freeze-pane scrollport moves.
        syncColMenuPosition();
        syncOpenMultiselectPositions();
        if (formatMultiselectOpen) {
            const dropdown = document.getElementById('format-filter-dropdown');
            const button = document.getElementById('format-filter-button');
            positionFormatDropdown(dropdown, button);
        }
    };
    const syncFromHeader = () => {
        if (syncing) return;
        syncing = true;
        hScroll.scrollLeft = headerScrollbar.scrollLeft;
        syncing = false;
    };
    hScroll.addEventListener('scroll', syncFromTable, { passive: true });
    headerScrollbar.addEventListener('scroll', syncFromHeader, { passive: true });
    window.addEventListener('resize', () => { recalc(); syncFromTable(); updateStickyHeader(); });
    recalc();
    syncFromTable();
}

/** Persistable widths: from global column_widths + defaults only (never layout/computed style). */
function getColumnWidths() {
    const widths = {};
    const tableWrap = document.querySelector('.table-wrap');
    const headerTable = getHeaderTable();
    if (!headerTable) return widths;
    headerTable.querySelectorAll('thead th').forEach(th => {
        const classes = th.className.split(' ');
        const colClass = classes.find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
        if (!colClass) return;
        const computed = window.getComputedStyle(th);
        const isHidden = (tableWrap && tableWrap.classList.contains('hide-' + colClass)) || computed.display === 'none';
        if (isHidden) return;
        const px = getGlobalColumnWidthPx(colClass);
        widths[colClass] = px + 'px';
    });
    return widths;
}

function applyColumnWidths(widthsObj) {
    if (!widthsObj || typeof widthsObj !== 'object') {
        hasSavedWidths = false;
        isManualResize = false;
        return;
    }
    
    // Check if all visible columns have saved widths
    const headerTable = getHeaderTable();
    const tableWrap = document.querySelector('.table-wrap');
    if (headerTable && tableWrap) {
        const visibleColumns = [];
        const headers = headerTable.querySelectorAll('thead th');
        headers.forEach(th => {
            const classes = Array.from(th.classList);
            const colClass = classes.find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
            if (colClass) {
                const computed = window.getComputedStyle(th);
                const isHidden = tableWrap.classList.contains('hide-' + colClass) || computed.display === 'none';
                if (!isHidden) {
                    visibleColumns.push(colClass);
                }
            }
        });
        
        // Check if all visible columns have saved widths (and they're valid)
        const allHaveWidths = visibleColumns.length > 0 && visibleColumns.every(col => {
            const width = widthsObj[col];
            if (!width) return false;
            // Parse width value (could be "111px" or "111")
            const numWidth = parseFloat(width.toString().replace('px', ''));
            return numWidth > 0;
        });
        
        if (!allHaveWidths && visibleColumns.length > 0) {
            const missingCols = visibleColumns.filter(col => {
                const width = widthsObj[col];
                if (!width) return true;
                const numWidth = parseFloat(width.toString().replace('px', ''));
                return numWidth <= 0;
            });
            console.log('[COLUMN_WIDTHS] Saved widths missing or invalid for some columns, keeping saved widths.');
            console.log('[COLUMN_WIDTHS] Visible columns:', visibleColumns);
            console.log('[COLUMN_WIDTHS] Saved widths keys:', Object.keys(widthsObj));
            console.log('[COLUMN_WIDTHS] Missing/invalid columns:', missingCols);
        }
    }
    
    // Only set hasSavedWidths if there are actual widths to apply
    const hasWidths = Object.keys(widthsObj).length > 0;
    hasSavedWidths = hasWidths;
    isManualResize = false; // Not a manual resize, just loading saved widths
    // Do not set widths on cells — conflicts with <colgroup> + border-collapse. syncColgroupFromStoredWidths applies colgroup.
}

/** Lock OTHER columns — no-op; widths come from <colgroup>. */
function lockVisibleColumnWidths(_excludeColClass) {}

function getHeaderColClassByIndex(idx) {
    const row = document.querySelector('#video-table thead tr');
    if (!row || !(idx >= 0) || idx >= row.children.length) return '';
    const th = row.children[idx];
    if (!th) return '';
    return Array.from(th.classList).find(c => c.startsWith('col-')) || '';
}

function getColgroupWidthsSlice(centerIdx, radius = 2) {
    const cg = document.getElementById('table-colgroup');
    if (!cg || !cg.children || !cg.children.length) return [];
    const out = [];
    const start = Math.max(0, centerIdx - radius);
    const end = Math.min(cg.children.length - 1, centerIdx + radius);
    for (let i = start; i <= end; i++) {
        const w = parseFloat(cg.children[i].style.width) || 0;
        out.push({ i, cls: getHeaderColClassByIndex(i), w: Math.round(w) });
    }
    return out;
}

function logColResizeDebug(phase, payload) {
    if (!COL_RESIZE_DEBUG) return;
    colResizeDebugSeq += 1;
    const msg = {
        seq: colResizeDebugSeq,
        phase,
        locked_index: resizeColumnIndex,
        locked_class: getHeaderColClassByIndex(resizeColumnIndex),
        pointer_id: activeResizePointerId,
        is_resizing: isResizing,
        ...payload
    };
    try {
        console.log('[COL_RESIZE_DEBUG_JSON]', JSON.stringify(msg));
    } catch (_e) {
        console.log('[COL_RESIZE_DEBUG]', msg);
    }
}

function onColumnResizeMouseMove(e) {
    if (!isResizing || !resizeColumn) return;
    const colClass = Array.from(resizeColumn.classList).find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
    if (!colClass) return;
    if (!(resizeColumnIndex >= 0)) return;
    const diff = e.clientX - startX;
    const newWidth = Math.max(8, Math.round(startWidth + diff));
    columnResizeLastWidthPx = newWidth;
    ensureColumnWidthsObject();
    columnWidths[colClass] = newWidth + 'px';
    applyLiveResizeByHeaderIndex(resizeColumnIndex, newWidth);
    isManualResize = true;
    const now = Date.now();
    if (COL_RESIZE_DEBUG && (now - colResizeLastMoveLogTs > 120)) {
        colResizeLastMoveLogTs = now;
        const slice = getColgroupWidthsSlice(resizeColumnIndex, 3);
        const changed = [];
        if (Array.isArray(colResizePrevSlice) && colResizePrevSlice.length === slice.length) {
            for (let i = 0; i < slice.length; i++) {
                const a = colResizePrevSlice[i];
                const b = slice[i];
                if (a && b && a.i === b.i && a.w !== b.w) changed.push({ i: b.i, cls: b.cls, from: a.w, to: b.w });
            }
        }
        colResizePrevSlice = slice;
        logColResizeDebug('move', {
            client_x: Math.round(e.clientX || 0),
            diff: Math.round(diff),
            start_w: Math.round(startWidth),
            new_w: Math.round(newWidth),
            target_class_from_element: colClass,
            changed_indices: changed,
            colgroup_slice: slice
        });
    }
}

function onColumnResizeMouseUp() {
    if (isResizing) {
        const resizedCol = resizeColumn;
        isResizing = false;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.body.classList.remove('is-col-resizing');
        if (resizedCol) resizedCol.classList.remove('resize-active');
        resizeColumn = null;
        const resizedIndex = resizeColumnIndex;
        resizeColumnIndex = -1;
        activeResizePointerId = null;

        hasSavedWidths = true;
        isManualResize = false;
        manualResizeBlocked = true;

        const headerRow = getVideoTableHeaderRow();
        const resizedTh = (headerRow && resizedIndex >= 0 && resizedIndex < headerRow.children.length) ? headerRow.children[resizedIndex] : resizedCol;
        const resizedColClass = resizedTh ? Array.from(resizedTh.classList).find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del') : null;
        const resizedWidth = resizedColClass ? Math.max(8, Math.round(columnResizeLastWidthPx || 0)) : 0;
        ensureColumnWidthsObject();
        if (resizedColClass && resizedWidth > 0) columnWidths[resizedColClass] = resizedWidth + 'px';
        if (resizedIndex >= 0 && resizedWidth > 0) applyLiveResizeByHeaderIndex(resizedIndex, resizedWidth);
        snapshotAllColumnWidthsFromColgroup();
        const toSave = { ...columnWidths };
        try { localStorage.setItem('column_widths', JSON.stringify(toSave)); } catch (e) {}
        if (settingsCache) settingsCache.column_widths = JSON.stringify(toSave);
        const payload = { column_widths: JSON.stringify(toSave) };
        fetch('/api/settings', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        }).then(() => {
            console.log('[COLUMN_WIDTHS] Manual resize widths saved to server, automatic recalculation BLOCKED');
            hasSavedWidths = true;
        }).catch(err => {
            console.error('[COLUMN_WIDTHS] Failed to save widths:', err);
        });
        console.log('[COLUMN_WIDTHS] Manual resize completed - automatic recalculation is now BLOCKED');
        logColResizeDebug('up', {
            resized_index: resizedIndex,
            resized_class: resizedColClass || '',
            resized_width: Math.round(resizedWidth || 0),
            saved_width_keys: Object.keys(toSave || {}).length,
            colgroup_slice: getColgroupWidthsSlice(resizedIndex, 3)
        });
    }
    if (columnResizeActive) {
        setTimeout(() => { columnResizeActive = false; }, 150);
    }
    suppressHeaderDrag = false;
}

/**
 * Delegated mousedown (capture) so resize works when target is filter control inside <th>.
 * Wider hit target (12px); viewport delta for #table-h-scroll horizontal scroll.
 */
function initColumnResize() {
    if (columnResizeDelegated) return;
    columnResizeDelegated = true;
    const hasPointer = typeof window !== 'undefined' && 'PointerEvent' in window;
    if (!hasPointer) {
        document.addEventListener('mousemove', onColumnResizeMouseMove);
        document.addEventListener('mouseup', onColumnResizeMouseUp);
    }
    const startResizeForHeader = (th, e) => {
        if (!th || th.classList.contains('col-chk') || th.classList.contains('col-del')) return;
        const colClass = Array.from(th.classList).find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
        if (!colClass) return;
        e.preventDefault();
        e.stopPropagation();
        // Guard against any stale body/header mismatch before locking a resize index.
        alignBodyColumnsToHeaderOrder();
        suppressHeaderDrag = true;
        columnResizeActive = true;
        isResizing = true;
        resizeColumn = th;
        activeResizePointerId = (e.pointerId != null ? e.pointerId : null);
        // Do NOT run full sync here; that can re-distribute widths on pointerdown.
        // Only bootstrap if colgroup has not been built yet.
        const cg = document.getElementById('table-colgroup');
        if (!cg || !cg.children || !cg.children.length) syncTableColgroup();
        const headerRow = document.querySelector('#video-table thead tr');
        resizeColumnIndex = headerRow ? Array.from(headerRow.children).indexOf(th) : -1;
        // Critical: lock baseline to CURRENT visual colgroup widths before first move.
        ensureColumnWidthsObject();
        snapshotAllColumnWidthsFromColgroup();
        startX = e.clientX;
        const liveColWidths = readColPixelWidthsFromColgroup();
        const liveW = (liveColWidths && resizeColumnIndex >= 0 && resizeColumnIndex < liveColWidths.length)
            ? Math.round(liveColWidths[resizeColumnIndex] || 0)
            : 0;
        startWidth = Math.max(8, Math.round(liveW || th.getBoundingClientRect().width || th.offsetWidth || getGlobalColumnWidthPx(colClass)));
        columnWidths[colClass] = startWidth + 'px';
        columnResizeLastWidthPx = startWidth;
        colResizePrevSlice = getColgroupWidthsSlice(resizeColumnIndex, 3);
        th.classList.add('resize-active');
        document.body.classList.add('is-col-resizing');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
        logColResizeDebug('down', {
            client_x: Math.round(e.clientX || 0),
            client_y: Math.round(e.clientY || 0),
            grabbed_class: colClass,
            grabbed_index: resizeColumnIndex,
            th_left: Math.round((th.getBoundingClientRect() || {}).left || 0),
            th_right: Math.round((th.getBoundingClientRect() || {}).right || 0),
            start_w: Math.round(startWidth),
            colgroup_slice: getColgroupWidthsSlice(resizeColumnIndex, 3)
        });
        if (e.pointerId != null && th.setPointerCapture) {
            try { th.setPointerCapture(e.pointerId); } catch (_e) {}
        }
    };
    // Attach explicit handles to each data header so resize target is exact (no coordinate ambiguity).
    const headers = Array.from(document.querySelectorAll('#video-table thead th'))
        .filter(th => !th.classList.contains('col-chk') && !th.classList.contains('col-del'));
    headers.forEach(th => {
        if (th.querySelector(':scope > .col-resize-handle')) return;
        const handle = document.createElement('div');
        handle.className = 'col-resize-handle';
        handle.setAttribute('data-role', 'col-resize-handle');
        handle.setAttribute('draggable', 'false');
        const onHandleDown = (e) => startResizeForHeader(th, e);
        if (hasPointer) handle.addEventListener('pointerdown', onHandleDown, true);
        else handle.addEventListener('mousedown', onHandleDown, true);
        th.appendChild(handle);
    });
    document.addEventListener('pointermove', (e) => {
        if (!isResizing) return;
        if (activeResizePointerId != null && e.pointerId !== activeResizePointerId) return;
        onColumnResizeMouseMove(e);
    }, true);
    document.addEventListener('pointerup', (e) => {
        if (!isResizing) return;
        if (activeResizePointerId != null && e.pointerId !== activeResizePointerId) return;
        onColumnResizeMouseUp();
    }, true);
    document.addEventListener('pointercancel', (e) => {
        if (!isResizing) return;
        if (activeResizePointerId != null && e.pointerId !== activeResizePointerId) return;
        onColumnResizeMouseUp();
    }, true);
}

// --- COLUMN WIDTH CALCULATION ---
let hasSavedWidths = false;
let isManualResize = false; // Track if user is manually resizing
let manualResizeBlocked = false; // Block ALL automatic recalculations after manual resize

function calculateColumnWidths(forceRecalculate = false, allowOverrideManual = false) {
    // NEVER run if manual resize was used (user wants manual control)
    if (manualResizeBlocked && !allowOverrideManual) {
        console.log('[COLUMN_WIDTHS] Skipping - manual resize is active');
        return;
    }
    // NEVER run if we have saved widths (universal widths must be preserved)
    if (hasSavedWidths && !allowOverrideManual) {
        console.log('[COLUMN_WIDTHS] Skipping - saved widths exist, preserving universal widths');
        return;
    }
    if (forceRecalculate && allowOverrideManual) {
        manualResizeBlocked = false;
    }
    console.log('[COLUMN_WIDTHS] Calculating widths, forceRecalculate:', forceRecalculate, 'hasSavedWidths:', hasSavedWidths);
    
    const headerTable = getHeaderTable();
    const tableWrap = document.querySelector('.table-wrap');
    const tableScroll = getTableScrollHost();
    if (!headerTable || !tableWrap || !tableScroll) return;
    
    // Use table-wrap clientWidth (visible viewport), NOT inner table (which grows with table content)
    const tableWidth = tableWrap.clientWidth || tableWrap.offsetWidth;
    const checkboxWidth = 40; // col-chk width
    const hamburgerWidth = 40; // col-del width
    const availableWidth = Math.max(400, tableWidth - checkboxWidth - hamburgerWidth);
    
    // Column priority weights (higher = more space allocated)
    const columnWeights = {
        'col-file': 1.8,      // Filename gets most space (reduced from 3.0 to prevent hogging)
        'col-main': 1.2,      // Format
        'col-prof': 1.0,      // Profile
        'col-el': 0.8,        // EL Type
        'col-sec': 1.0,       // Secondary
        'col-res': 0.9,       // Resolution
        'col-bit': 0.8,       // Bitrate
        'col-size': 0.9,      // Size
        'col-width': 0.6,     // Width
        'col-height': 0.6,    // Height
        'col-vol': 0.7,       // Volume
        'col-cont': 0.6,      // Container
        'col-scan': 0.8,      // Scanned
        'col-stat': 0.7,     // Status
        'col-nfo': 0.6,      // NFO
        'col-missing': 0.6,  // Missing
        'col-dup': 0.6,      // Duplicate count
        'col-hyb': 0.6,       // Dual HDR
        'col-hybrid-src': 0.6, // Source Hybrid
        'col-audio': 1.0,     // Audio
        'col-audio-ch': 0.5,  // Audio Channels
        'col-audio-combined': 1.2,  // Audio Combined
        'col-sub': 0.7,       // Subtitles
        'col-cll': 0.7,       // MaxCLL
        'col-fall': 0.7,      // MaxFALL
        'col-video-source': 0.8,    // Source
        'col-source-format': 0.8,  // Source Format
        'col-video-codec': 0.8,    // Codec
        'col-is-3d': 0.5,     // 3D
        'col-edition': 0.9,   // Edition
        'col-year': 0.5,      // Year
        'col-media-type': 0.6, // Media Type
        'col-show-title': 1.2, // Show Title
        'col-season': 0.3,    // Season (narrow - 1-2 digits)
        'col-episode': 0.3,   // Episode (narrow - 1-3 digits)
        'col-movie-title': 1.2, // Movie Title
        'col-episode-title': 1.2, // Episode Title
        'col-aspect': 0.6,
        'col-imdb-id': 0.8,
        'col-tvdb-id': 0.8,
        'col-tmdb-id': 0.8,
        'col-rotten-id': 0.8,
        'col-metacritic-id': 0.8,
        'col-trakt-id': 0.8,
        'col-tvdb-series-id': 0.8,
        'col-tvdb-episode-id': 0.8,
        'col-imdb-series-id': 0.8,
        'col-imdb-episode-id': 0.8,
        'col-tmdb-series-id': 0.8,
        'col-tmdb-episode-id': 0.8,
        'col-trakt-series-id': 0.8,
        'col-trakt-episode-id': 0.8,
        'col-rotten-series-id': 0.8,
        'col-rotten-episode-id': 0.8,
        'col-metacritic-series-id': 0.8,
        'col-metacritic-episode-id': 0.8,
        'col-imdb-rating': 0.6,
        'col-tvdb-rating': 0.6,
        'col-tmdb-rating': 0.6,
        'col-rotten-rating': 0.6,
        'col-metacritic-rating': 0.6,
        'col-trakt-rating': 0.6
    };
    
    // Use ALL columns (not just visible) so widths stay consistent when switching All/Movie/TV
    // Only hide/show via CSS - column widths must not change
    const visibleColumns = [];
    const headers = headerTable.querySelectorAll('thead th');
    headers.forEach(th => {
        const classes = Array.from(th.classList);
        const colClass = classes.find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
        if (colClass) {
            visibleColumns.push(colClass);
        }
    });
    
    // Calculate total weight
    let totalWeight = 0;
    visibleColumns.forEach(col => {
        totalWeight += columnWeights[col] || 0.7; // Default weight if not found
    });
    
    if (totalWeight === 0) {
        console.warn('[COLUMN_WIDTHS] No visible columns found!');
        return; // No visible columns
    }
    
    console.log('[COLUMN_WIDTHS] Found', visibleColumns.length, 'visible columns:', visibleColumns.join(', '));
    
    // Calculate proportional widths first
    const calculatedWidths = {};
    const minWidths = {};
    let totalMinWidth = 0;
    
    // First pass: calculate min widths - keep them small to ensure all columns fit
    // Season/episode are narrow (1-3 digits), year/3d are 40px; file gets 100px; others 50px
    visibleColumns.forEach(col => {
        const minWidth = col === 'col-file' ? 100
            : (col.includes('year') || col.includes('3d') ? 40
            : (col === 'col-season' || col === 'col-episode' ? 40 : 50));
        minWidths[col] = minWidth;
        totalMinWidth += minWidth;
    });
    
    // Check if we have enough space for minimum widths
    if (totalMinWidth > availableWidth) {
        // Not enough space - distribute equally to ensure ALL columns are visible
        const equalWidth = Math.floor(availableWidth / visibleColumns.length);
        visibleColumns.forEach(col => {
            calculatedWidths[col] = Math.max(equalWidth, 30); // Minimum 30px per column
        });
    } else {
        // We have enough space - distribute remaining width proportionally but ensure ALL columns fit
        const remainingWidth = availableWidth - totalMinWidth;
        
        // Distribute remaining space proportionally
        visibleColumns.forEach(col => {
            const weight = columnWeights[col] || 0.7;
            const proportionalShare = (remainingWidth * weight) / totalWeight;
            calculatedWidths[col] = Math.floor(minWidths[col] + proportionalShare);
        });
        
        // CRITICAL: Ensure total EXACTLY matches available width (or is less)
        let totalCalculated = visibleColumns.reduce((sum, col) => sum + calculatedWidths[col], 0);
        
        if (totalCalculated > availableWidth) {
            // Total exceeds available - reduce proportionally to fit exactly
            const scaleFactor = availableWidth / totalCalculated;
            visibleColumns.forEach(col => {
                const scaled = Math.floor(calculatedWidths[col] * scaleFactor);
                // Ensure it's at least the minimum
                calculatedWidths[col] = Math.max(scaled, minWidths[col]);
            });
            // Recalculate total after scaling
            totalCalculated = visibleColumns.reduce((sum, col) => sum + calculatedWidths[col], 0);
        }
        
        // Final adjustment: if still over, trim from largest columns
        if (totalCalculated > availableWidth) {
            const diff = totalCalculated - availableWidth;
            const sortedCols = [...visibleColumns].sort((a, b) => calculatedWidths[b] - calculatedWidths[a]);
            let trimmed = 0;
            for (const col of sortedCols) {
                if (trimmed >= diff) break;
                if (calculatedWidths[col] > minWidths[col]) {
                    const trim = Math.min(calculatedWidths[col] - minWidths[col], diff - trimmed);
                    calculatedWidths[col] -= trim;
                    trimmed += trim;
                }
            }
        } else if (totalCalculated < availableWidth) {
            // If under, add to filename column
            const diff = availableWidth - totalCalculated;
            const fileCol = visibleColumns.find(col => col === 'col-file');
            if (fileCol) {
                calculatedWidths[fileCol] += diff;
            }
        }
    }
    
    // Final check: ensure total exactly equals availableWidth
    let totalCalculated = visibleColumns.reduce((sum, col) => sum + (calculatedWidths[col] || 0), 0);
    if (totalCalculated !== availableWidth && totalCalculated > 0 && visibleColumns.length > 0) {
        // Adjust to match exactly
        const diff = availableWidth - totalCalculated;
        if (diff > 0) {
            // Add to filename column if it exists, otherwise first column
            const fileCol = visibleColumns.find(col => col === 'col-file');
            if (fileCol) {
                calculatedWidths[fileCol] = (calculatedWidths[fileCol] || 0) + diff;
            } else {
                calculatedWidths[visibleColumns[0]] = (calculatedWidths[visibleColumns[0]] || 0) + diff;
            }
        } else {
            // Subtract from largest columns
            const sortedCols = [...visibleColumns].sort((a, b) => (calculatedWidths[b] || 0) - (calculatedWidths[a] || 0));
            let toRemove = Math.abs(diff);
            for (const col of sortedCols) {
                if (toRemove <= 0) break;
                const current = calculatedWidths[col] || 0;
                const min = minWidths[col] || 30;
                if (current > min) {
                    const remove = Math.min(current - min, toRemove);
                    calculatedWidths[col] = current - remove;
                    toRemove -= remove;
                }
            }
        }
    }
    
    // Apply widths - ensure ALL columns get a width
    let totalApplied = 0;
    visibleColumns.forEach(col => {
        const finalWidth = calculatedWidths[col] || 50; // Fallback to 50px if undefined
        totalApplied += finalWidth;
        
        // Apply to all cells with this class
        document.querySelectorAll(`th.${col}, td.${col}`).forEach(cell => {
            cell.style.width = finalWidth + 'px';
            cell.style.minWidth = '25px';
            cell.style.maxWidth = finalWidth + 'px';
        });
    });
    
    // Debug: Log results and verify columns are actually visible
    console.log(`[COLUMN_WIDTHS] Applied ${visibleColumns.length} columns: ${totalApplied}px / ${availableWidth}px`);
    console.log(`[COLUMN_WIDTHS] Column widths:`, visibleColumns.map(col => `${col}:${calculatedWidths[col] || 0}px`).join(', '));
    
    // Verify all columns are actually visible in the DOM
    visibleColumns.forEach(col => {
        const cells = document.querySelectorAll(`th.${col}, td.${col}`);
        if (cells.length === 0) {
            console.error(`[COLUMN_WIDTHS] ERROR: Column ${col} has no cells in DOM!`);
        } else {
            const firstCell = cells[0];
            const computedWidth = window.getComputedStyle(firstCell).width;
            const expectedWidth = calculatedWidths[col] + 'px';
            const isVisible = window.getComputedStyle(firstCell).display !== 'none';
            if (!isVisible) {
                console.error(`[COLUMN_WIDTHS] ERROR: Column ${col} is hidden (display: none)!`);
            } else if (computedWidth !== expectedWidth) {
                console.warn(`[COLUMN_WIDTHS] WARNING: Column ${col} width mismatch! Expected: ${expectedWidth}, Got: ${computedWidth}`);
            }
        }
    });
    
    if (totalApplied > availableWidth + 5) {
        console.error(`[COLUMN_WIDTHS] ERROR: Total ${totalApplied}px exceeds available ${availableWidth}px!`);
    } else {
        console.log(`[COLUMN_WIDTHS] SUCCESS: All ${visibleColumns.length} columns should be visible!`);
    }
}


// --- DATA LOADING & INTERACTION ---
const FILTER_RELOAD_MS = 300;
let filterReloadTimer = null;

/** Immediate page-1 reload (pagination/clear/flush). */
function resetAndLoadImmediate() {
    currentPage = 1;
    if (isLoading) {
        pendingReload = true;
        return;
    }
    loadData();
}

/** Debounced reload for filter UI — coalesces rapid checkbox/select changes. */
function resetAndLoad() {
    clearTimeout(filterReloadTimer);
    filterReloadTimer = setTimeout(() => {
        filterReloadTimer = null;
        resetAndLoadImmediate();
    }, FILTER_RELOAD_MS);
}

function debounceSearch() {
    clearTimeout(searchTimer);
    clearTimeout(filterReloadTimer);
    filterReloadTimer = null;
    searchTimer = setTimeout(resetAndLoadImmediate, 400);
}
function changePage(d) { jumpPage(currentPage + d); }
function jumpPage(v) { currentPage = Math.max(1, Math.min(totalPages, parseInt(v) || 1)); loadData(); }

function updateSelectedRowHighlight() {
    const tbody = document.getElementById('video-table-body');
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll('tr'));
    rows.forEach(r => r.classList.remove('keyboard-selected'));
    if (selectedRowIndex < 0 || selectedRowIndex >= rows.length) return;
    const row = rows[selectedRowIndex];
    if (row) {
        row.classList.add('keyboard-selected');
        row.scrollIntoView({ block: 'nearest' });
    }
}

function selectRowByIndex(index) {
    selectedRowIndex = Number(index);
    updateSelectedRowHighlight();
}

let contextMenuPath = '';

function handleRowClick(evt, index, path) {
    const rowIndex = Number(index);
    if (evt && (evt.ctrlKey || evt.metaKey)) {
        const row = evt.currentTarget;
        const chk = row ? row.querySelector('input.row-chk') : null;
        if (chk) {
            chk.checked = !chk.checked;
            toggleRow(chk, path);
        }
        selectRowByIndex(rowIndex);
        return;
    }
    // Second click on the same row clears the keyboard highlight
    if (selectedRowIndex === rowIndex) {
        selectedRowIndex = -1;
        updateSelectedRowHighlight();
        return;
    }
    selectRowByIndex(rowIndex);
}

function openRowContextMenu(evt, index, path) {
    if (!evt) return;
    evt.preventDefault();
    const row = evt.currentTarget;
    const chk = row ? row.querySelector('input.row-chk') : null;
    if (chk && !chk.checked) {
        chk.checked = true;
        toggleRow(chk, path);
    }
    selectRowByIndex(index);
    contextMenuPath = path;
    const menu = document.getElementById('row-context-menu');
    if (!menu) return;
    menu.style.left = `${evt.pageX}px`;
    menu.style.top = `${evt.pageY}px`;
    menu.classList.add('active');
    const removeBtn = document.getElementById('ctx-remove-db');
    if (removeBtn) {
        const count = selectedPaths.size;
        if (count > 1) removeBtn.innerText = `Remove ${count} item(s) from DB`;
        else removeBtn.innerText = 'Remove item from DB';
    }
    refreshArrContextStatus();
}

async function refreshArrContextStatus() {
    const sonarrDot = document.getElementById('ctx-sonarr-dot');
    const radarrDot = document.getElementById('ctx-radarr-dot');
    const btn = document.getElementById('ctx-arr-action');
    if (!sonarrDot || !radarrDot || !btn) return;
    sonarrDot.classList.remove('status-online', 'status-offline');
    radarrDot.classList.remove('status-online', 'status-offline');
    sonarrDot.title = 'Checking...';
    radarrDot.title = 'Checking...';
    btn.title = 'Checking ARR connectivity...';
    try {
        const res = await fetch('/api/arr_status');
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || `Status check failed (${res.status})`);
        }
        const sonarrOk = !!data?.sonarr?.ok;
        const radarrOk = !!data?.radarr?.ok;
        sonarrDot.classList.add(sonarrOk ? 'status-online' : 'status-offline');
        radarrDot.classList.add(radarrOk ? 'status-online' : 'status-offline');
        sonarrDot.title = `Sonarr: ${data?.sonarr?.message || 'Unknown'}`;
        radarrDot.title = `Radarr: ${data?.radarr?.message || 'Unknown'}`;
        btn.title = `Sonarr: ${data?.sonarr?.message || 'Unknown'} | Radarr: ${data?.radarr?.message || 'Unknown'}`;
    } catch (e) {
        sonarrDot.classList.add('status-offline');
        radarrDot.classList.add('status-offline');
        const tip = `ARR status check failed: ${e.message || e}`;
        sonarrDot.title = tip;
        radarrDot.title = tip;
        btn.title = tip;
    }
}

function closeContextMenu() {
    const menu = document.getElementById('row-context-menu');
    if (menu) menu.classList.remove('active');
}

function contextRescan() {
    closeContextMenu();
    if (selectedPaths.size > 1) {
        bulkRescanSelected();
        return;
    }
    if (contextMenuPath) {
        rescanFile(contextMenuPath);
    }
}

async function contextArrSearchReplace() {
    closeContextMenu();
    let paths = [];
    if (masterState === 2 || selectedPaths.size > 1) {
        paths = await getBulkTargetPaths();
    } else if (contextMenuPath) {
        paths = [contextMenuPath];
    }
    if (!paths.length) {
        showToast('No rows selected');
        return;
    }
    showToast(`Queuing ARR search for ${paths.length} item(s)...`);
    try {
        const res = await fetch('/api/arr_search_replace', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ paths })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || `ARR request failed (${res.status})`);
        }
        console.log('[ARR] Response:', data);
        if (data.failed > 0) {
            const failed = (data.results || []).filter(r => r.status === 'error');
            const firstMsg = failed[0]?.message || 'Unknown error';
            showToast(`ARR: ${data.success} queued, ${data.failed} failed. First: ${firstMsg}`, {isError: true});
            console.warn('[ARR] Failures:', failed);
        } else {
            showToast(`ARR queued for ${data.success} item(s)`);
        }
    } catch (e) {
        console.error('[ARR] Error:', e);
        showToast(`ARR failed: ${e.message || e}`, {isError: true});
    }
}

function contextCopyPath() {
    closeContextMenu();
    if (!contextMenuPath) return;
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(contextMenuPath).then(() => showToast('Path copied'));
    } else {
        showToast('Copy not supported', {isError: true});
    }
}

function contextRemoveFromDb() {
    closeContextMenu();
    if (selectedPaths.size > 1) {
        promptDelete();
        return;
    }
    if (contextMenuPath) {
        promptDelete(contextMenuPath);
    }
}

function closeDeepDebugModal() {
    const modal = document.getElementById('deep-debug-modal');
    if (modal) modal.style.display = 'none';
}

let nfoModalContent = '';
async function openNfoModal(fullPath) {
    if (!fullPath) return;
    const modal = document.getElementById('nfo-modal');
    const pathEl = document.getElementById('nfo-modal-path');
    const contentEl = document.getElementById('nfo-modal-content');
    if (!modal || !pathEl || !contentEl) return;
    pathEl.textContent = fullPath;
    contentEl.textContent = 'Loading...';
    nfoModalContent = '';
    modal.style.display = 'flex';
    try {
        const res = await fetch('/api/nfo_content?path=' + encodeURIComponent(fullPath));
        const data = await res.json();
        if (data.status === 'ok') {
            nfoModalContent = data.content || '';
            contentEl.textContent = nfoModalContent || '(empty)';
            if (data.nfo_path) pathEl.textContent = data.nfo_path;
        } else {
            contentEl.textContent = 'Error: ' + (data.message || 'Failed to load NFO');
        }
    } catch (e) {
        contentEl.textContent = 'Error: ' + (e.message || 'Request failed');
    }
}
async function copyNfoContent() {
    const contentEl = document.getElementById('nfo-modal-content');
    const txt = contentEl ? (contentEl.textContent || '').replace(/^\(empty\)$/, '').trim() : '';
    await copyTextWithFallback(txt, 'Copied to clipboard');
}
function closeNfoModal() {
    const modal = document.getElementById('nfo-modal');
    if (modal) modal.style.display = 'none';
}

async function contextDeepDebug() {
    closeContextMenu();
    if (!contextMenuPath) {
        showToast('No file selected');
        return;
    }
    try {
        showToast('Running deep debug...');
        const res = await fetch('/api/debug_deep', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ full_path: contextMenuPath })
        });
        const data = await res.json();
        if (!res.ok || data.status !== 'ok') {
            throw new Error(data.message || 'Deep debug failed');
        }
        const pathEl = document.getElementById('deep-debug-path');
        const metaEl = document.getElementById('deep-debug-meta');
        const outEl = document.getElementById('deep-debug-output');
        if (pathEl) pathEl.textContent = contextMenuPath;
        if (metaEl) metaEl.textContent = `Return code: ${data.return_code}`;
        if (outEl) outEl.textContent = data.output || '(no output)';
        const modal = document.getElementById('deep-debug-modal');
        if (modal) modal.style.display = 'block';
    } catch (e) {
        console.error('Deep debug failed:', e);
        showToast(`Deep debug failed: ${e.message || e}`, {isError: true});
    }
}

async function copyTextWithFallback(txt, okMessage) {
    if (!txt) {
        showToast('No output to copy');
        return;
    }
    try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(txt);
            showToast(okMessage);
            return;
        }
    } catch (e) {
        // Fall through to legacy fallback.
    }

    // Fallback for contexts where Clipboard API is unavailable/blocked.
    const ta = document.createElement('textarea');
    ta.value = txt;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    ta.style.pointerEvents = 'none';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    let copied = false;
    try {
        copied = document.execCommand('copy');
    } catch (e) {
        copied = false;
    } finally {
        document.body.removeChild(ta);
    }
    showToast(copied ? okMessage : 'Copy failed (use Ctrl+C)', copied ? {} : {isError: true});
}

function extractDeepDebugSummary(txt) {
    if (!txt) return '';
    const marker = 'INTERPRETATION SUMMARY';
    const idx = txt.indexOf(marker);
    if (idx < 0) return '';
    const after = txt.slice(idx);
    // Summary block ends at the next test section divider.
    const nextTestIdx = after.indexOf('TEST 1 -');
    if (nextTestIdx > 0) {
        return after.slice(0, nextTestIdx).trim();
    }
    // Fallback: return from marker onward if test marker is missing.
    return after.trim();
}

async function copyDeepDebugSummary() {
    const outEl = document.getElementById('deep-debug-output');
    const txt = outEl ? outEl.textContent || '' : '';
    const summary = extractDeepDebugSummary(txt);
    if (!summary) {
        showToast('No interpretation summary found');
        return;
    }
    await copyTextWithFallback(summary, 'Summary copied');
}

async function copyDeepDebugOutput() {
    const outEl = document.getElementById('deep-debug-output');
    const txt = outEl ? outEl.textContent || '' : '';
    await copyTextWithFallback(txt, 'Debug output copied');
}

function selectAllRows() {
    closeContextMenu();
    masterState = 1;
    const chkEl = document.getElementById('master-chk');
    if (chkEl) chkEl.className = 'col-chk master-dash';
    currentRows.forEach(p => selectedPaths.add(p));
    document.querySelectorAll('.row-chk').forEach(c => c.checked = true);
    updateDeleteBtn();
}

function selectAllFilteredRows() {
    closeContextMenu();
    masterState = 2;
    const chkEl = document.getElementById('master-chk');
    if (chkEl) chkEl.className = 'col-chk master-x';
    selectedPaths.clear();
    document.querySelectorAll('.row-chk').forEach(c => c.checked = false);
    updateDeleteBtn();
}

function deselectAllRows() {
    closeContextMenu();
    masterState = 0;
    const chkEl = document.getElementById('master-chk');
    if (chkEl) chkEl.className = 'col-chk';
    selectedPaths.clear();
    document.querySelectorAll('.row-chk').forEach(c => c.checked = false);
    updateDeleteBtn();
}

function moveRowSelection(delta) {
    const tbody = document.getElementById('video-table-body');
    const rows = tbody ? tbody.querySelectorAll('tr') : [];
    if (!rows.length) return;
    if (selectedRowIndex < 0) selectedRowIndex = 0;
    selectedRowIndex = Math.max(0, Math.min(rows.length - 1, selectedRowIndex + delta));
    updateSelectedRowHighlight();
}

function isTypingTarget(target) {
    if (!target) return false;
    const tag = target.tagName;
    return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || target.isContentEditable;
}

function formatSearchValue(value) {
    if (value.includes(' ')) {
        return `"${value}"`;
    }
    return value;
}

function getSearchSuggestionValues(token) {
    const opts = lastFilterOptions || {};
    const values = {
        'year': [],
        'source': Object.keys(opts.video_sources || {}),
        'format': Object.keys(opts.source_formats || {}),
        'codec': Object.keys(opts.video_codecs || {}),
        'res': Object.keys(opts.resolutions || {}),
        'resolution': Object.keys(opts.resolutions || {}),
        'category': Object.keys(opts.categories || {}),
        'cat': Object.keys(opts.categories || {}),
        'volume': Object.keys(opts.volumes || {}),
        'vol': Object.keys(opts.volumes || {}),
        'container': Object.keys(opts.containers || {}),
        'edition': Object.keys(opts.editions || {}),
        'type': ['movie', 'tv'],
        'media_type': ['movie', 'tv'],
        'status': ['ok', 'failed'],
        'hybrid': ['1', '0'],
        '3d': ['1', '0'],
        'nfo': ['missing', 'found']
    };
    return values[token] || [];
}

let searchSuggestionIndex = -1;

function getSearchSuggestionItems() {
    return Array.from(document.querySelectorAll('#search-suggestions .search-suggestion'));
}

function setActiveSearchSuggestion(index) {
    const items = getSearchSuggestionItems();
    if (items.length === 0) return;
    items.forEach((el, i) => {
        el.classList.toggle('active', i === index);
    });
    const active = items[index];
    if (active) {
        active.scrollIntoView({ block: 'nearest' });
    }
}

function updateSearchSuggestions() {
    const input = document.getElementById('search-bar');
    const suggestionsEl = document.getElementById('search-suggestions');
    if (!input || !suggestionsEl) return;
    const value = input.value || '';
    const parts = value.split(/\s+/);
    const last = parts[parts.length - 1] || '';
    const tokens = ['year', 'source', 'format', 'codec', 'res', 'resolution', 'category', 'cat', 'volume', 'vol', 'container', 'edition', 'type', 'media_type', 'status', 'hybrid', '3d', 'nfo'];

    let suggestions = [];
    if (last.includes(':')) {
        const [tokenRaw, ...rest] = last.split(':');
        const token = tokenRaw.toLowerCase();
        const partial = rest.join(':');
        const values = getSearchSuggestionValues(token);
        suggestions = values
            .map(v => formatSearchValue(v))
            .filter(v => v.toLowerCase().startsWith(partial.toLowerCase()))
            .map(v => `${token}:${v}`);
    } else {
        suggestions = tokens
            .filter(t => t.startsWith(last.toLowerCase()))
            .map(t => `${t}:`);
    }

    if (suggestions.length === 0) {
        suggestionsEl.style.display = 'none';
        suggestionsEl.innerHTML = '';
        searchSuggestionIndex = -1;
        return;
    }

    suggestionsEl.innerHTML = suggestions.slice(0, 20).map(s => `<div class="search-suggestion" data-value="${escAttr(s)}">${escHtml(s)}</div>`).join('');
    suggestionsEl.style.display = 'block';
    searchSuggestionIndex = -1;
}

function applySearchSuggestion(value) {
    const input = document.getElementById('search-bar');
    const suggestionsEl = document.getElementById('search-suggestions');
    if (!input) return;
    const parts = input.value.split(/\s+/);
    parts[parts.length - 1] = value;
    input.value = parts.join(' ') + ' ';
    if (suggestionsEl) {
        suggestionsEl.style.display = 'none';
        suggestionsEl.innerHTML = '';
    }
    searchSuggestionIndex = -1;
    debounceSearch();
}

async function refreshChartsOnly() {
    try {
        const params = new URLSearchParams({ sort: sortCol, order: sortOrder, ...activeFilters, include_options: '0' });
        const res = await fetch(`/api/videos/meta?${params}`);
        if (!res.ok) {
            notifyVideosMetaFailure(new Error(`HTTP ${res.status}`));
            return;
        }
        const data = await res.json();
        if (data && data.stats) {
            applyVideosMeta(data);
        }
    } catch (e) {
        console.error('Failed to refresh charts:', e);
        notifyVideosMetaFailure(e);
    }
}

function applyVideosMeta(meta) {
    if (!meta || !meta.stats) return;
    const mt = (activeFilters.media_type || '').trim().toLowerCase();
    const ribbonStats = (mt === 'movie' || mt === 'tv') && meta.stats_media_scoped
        ? meta.stats_media_scoped
        : meta.stats;
    lastStats = ribbonStats;
    lastStatsFiltered = meta.stats_filtered;
    lastFullStats = meta.stats;
    if (meta.filter_options) {
        lastFilterOptions = meta.filter_options;
        updateFilterDropdowns(meta.filter_options);
    }
    applyStatsToUI(ribbonStats, meta.total_items != null ? meta.total_items : undefined, meta.stats);
    updateCharts(ribbonStats, meta.stats_filtered, lastFilterOptions);
}

const FILTER_OPTIONS_REFRESH_MS = 900;
let filterOptionsTimer = null;
let filterOptionsSeq = 0;
let lastMetaFailureToastAt = 0;

async function retryVideosMeta() {
    try {
        const perPageVal = document.getElementById('per-page-select')
            ? document.getElementById('per-page-select').value
            : '50';
        const params = new URLSearchParams({
            page: currentPage,
            per_page: perPageVal,
            sort: sortCol,
            order: sortOrder,
            ...activeFilters,
            include_options: '0'
        });
        const res = await fetch(`/api/videos/meta?${params}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const meta = await res.json();
        applyVideosMeta(meta);
        scheduleFilterOptionsRefresh();
        showToast('Dashboard stats reloaded');
    } catch (e) {
        console.error('Meta retry failed:', e);
        notifyVideosMetaFailure(e);
    }
}

function notifyVideosMetaFailure(err) {
    const now = Date.now();
    if (now - lastMetaFailureToastAt < 2500) return;
    lastMetaFailureToastAt = now;
    const detail = (err && err.message) ? String(err.message) : 'request failed';
    showToast(`Stats/charts failed to load (${detail})`, {
        isError: true,
        duration: 12000,
        actionLabel: 'Retry',
        onAction: () => { retryVideosMeta(); }
    });
}

/** Refresh dropdown facet counts after filters settle (separate from hot stats path). */
function scheduleFilterOptionsRefresh() {
    clearTimeout(filterOptionsTimer);
    const seq = ++filterOptionsSeq;
    filterOptionsTimer = setTimeout(async () => {
        filterOptionsTimer = null;
        try {
            const params = new URLSearchParams({ ...activeFilters, include_options: '1' });
            const res = await fetch(`/api/videos/meta?${params}`);
            if (!res.ok || seq !== filterOptionsSeq) return;
            const meta = await res.json();
            if (seq !== filterOptionsSeq) return;
            if (meta && meta.filter_options) {
                lastFilterOptions = meta.filter_options;
                updateFilterDropdowns(meta.filter_options);
                if (lastStats) updateCharts(lastStats, lastStatsFiltered, lastFilterOptions);
            }
        } catch (e) {
            console.error('Failed to refresh filter options:', e);
        }
    }, FILTER_OPTIONS_REFRESH_MS);
}

function toggleCharts() {
    const view = document.getElementById('db-view');
    const btn = document.getElementById('btn-toggle-charts');
    if (view.style.display === 'none' || view.style.display === '') {
        view.style.display = 'grid'; 
        btn.innerHTML = 'Charts ⏷'; 
        if (lastStats) {
            updateCharts(lastStats, lastStatsFiltered, lastFilterOptions);
        } else {
            refreshChartsOnly();
        }
        requestAnimationFrame(() => {
            window.dispatchEvent(new Event('resize'));
            [formatChart, secChart, resChart, volChart].forEach(c => {
                if (c && typeof c.resize === 'function') c.resize();
            });
        });
    } else {
        view.style.display = 'none'; 
        btn.innerHTML = 'Charts ⏵';
    }
}

function toggleFormatView() {
    const mainView = document.getElementById('view-main-fmt');
    const secView = document.getElementById('view-sec-fmt');
    if (mainView.classList.contains('active')) {
        mainView.classList.remove('active'); mainView.classList.add('hidden');
        secView.classList.remove('hidden'); secView.classList.add('active');
    } else {
        secView.classList.remove('active'); secView.classList.add('hidden');
        mainView.classList.remove('hidden'); mainView.classList.add('active');
    }
}

function toggleChartMode() {
    chartMode = chartMode === 'total' ? 'filtered' : 'total';
    const toggle = document.getElementById('chart-mode-toggle');
    if (toggle) {
        toggle.innerText = chartMode === 'total' ? 'Totals' : 'Filtered';
    }
    if (lastStats) {
        updateCharts(lastStats, lastStatsFiltered, lastFilterOptions);
    }
}

function toggleBarChartMode() {
    barChartMode = barChartMode === 'volumes' ? 'paths' : 'volumes';
    const title = document.getElementById('vol-chart-title');
    if (title) title.innerText = barChartMode === 'volumes' ? 'VOLUMES' : 'PATHS';
    if (lastStats) {
        updateCharts(lastStats, lastStatsFiltered, lastFilterOptions);
    }
}

function applyStatsToUI(stats, totalItems, fullStats, mediaTypeOverride) {
    if (!stats) return;
    document.getElementById('stat-total').innerText = stats.total ?? 0;
    document.getElementById('stat-failed').innerText = stats.failed ?? 0;
    document.getElementById('stat-hybrid').innerText = stats.hybrid ?? 0;
    const srcHybridEl = document.getElementById('stat-source-hybrid');
    if (srcHybridEl) srcHybridEl.innerText = stats.source_hybrid ?? 0;
    document.getElementById('stat-dovi').innerText = stats.dovi ?? 0;
    document.getElementById('stat-hdr10plus').innerText = stats.hdr10plus ?? 0;
    document.getElementById('stat-hdr10').innerText = stats.hdr10 ?? 0;
    document.getElementById('stat-hlg').innerText = stats.hlg ?? 0;
    document.getElementById('stat-sdr').innerText = stats.sdr ?? 0;
    document.getElementById('stat-fel').innerText = stats.dovi_p7_fel ?? 0;
    document.getElementById('stat-mel').innerText = stats.dovi_p7_mel ?? 0;
    document.getElementById('stat-p5').innerText = stats.dovi_p5 || 0;
    document.getElementById('stat-p81').innerText = stats.dovi_p81 || 0;
    document.getElementById('stat-p84').innerText = stats.dovi_p84 || 0;
    // Bare profile "10" (no compat hint) folds into P10.1 — same idea as bare P8 → chart P8.1
    document.getElementById('stat-p101').innerText = (stats.dovi_p101 || 0) + (stats.dovi_p10 || 0);
    document.getElementById('stat-p104').innerText = stats.dovi_p104 || 0;
    const p20El = document.getElementById('stat-p20');
    if (p20El) p20El.innerText = stats.dovi_p20 || 0;
    document.getElementById('res-total-display').innerText = stats.total ?? 0;
    if (typeof totalItems === 'number') {
        document.getElementById('res-filtered').innerText = totalItems;
    }
    // Idle: show last scan date. During scan the live timer owns #stat-duration.
    if (!document.body.classList.contains('scanning')) {
        setLastScanDisplay(stats.last_full_scan || 'Never');
    }
    // Total size badge: use fullStats (data.stats) which has total_size_all/movie/tv
    const badge = document.getElementById('total-size-badge');
    if (badge && fullStats) {
        const mt = (mediaTypeOverride !== undefined ? String(mediaTypeOverride) : (typeof activeFilters !== 'undefined' && activeFilters.media_type || '')).trim().toLowerCase();
        let bytes = fullStats.total_size_all ?? 0;
        let sizeClass = 'size-all';
        if (mt === 'movie') { bytes = fullStats.total_size_movie ?? fullStats.total_size_all ?? 0; sizeClass = 'size-movie'; }
        else if (mt === 'tv') { bytes = fullStats.total_size_tv ?? fullStats.total_size_all ?? 0; sizeClass = 'size-tv'; }
        badge.className = 'total-size-badge ' + sizeClass;
        badge.innerText = 'Total size on disk(s): ' + formatSize(bytes);
    }
}

async function loadData() {
    // Don't load data if we're in the middle of clearing filters
    if (isClearingFilters) {
        console.log(`[DEBUG] loadData(): Skipping because isClearingFilters=true`);
        return;
    }
    if (isLoading) return;
    isLoading = true;
    const loadingBackdrop = document.getElementById('table-loading-backdrop');
    const loadingOverlay = document.getElementById('table-loading-overlay');
    if (loadingBackdrop) loadingBackdrop.style.display = 'block';
    if (loadingOverlay) loadingOverlay.style.display = 'block';
    document.body.classList.add('loading-table');
    
    syncActiveFiltersFromDom(true);
    const normalizedMediaType = normalizeMediaTypeFilter(activeFilters.media_type);
    updateMediaTypeButtons(normalizedMediaType);
    // Only change column visibility for Movies/TV. NEVER touch widths when filtering.
    applyMediaTypeColumnVisibility(normalizedMediaType);
    if (!columnWidths) { try { const r = localStorage.getItem('column_widths'); if (r) columnWidths = JSON.parse(r); } catch (e) {} }

    const perPageVal = document.getElementById('per-page-select') ? document.getElementById('per-page-select').value : '50';
    const tbody = document.getElementById('video-table-body');
    tbody.innerHTML = '<tr><td colspan="52" style="text-align:center; padding: 20px;">Loading...</td></tr>';
    const loadId = ++loadDataSeq;
    try {
        const params = new URLSearchParams({ page: currentPage, per_page: perPageVal, sort: sortCol, order: sortOrder, ...activeFilters });
        // Rows first (fast); meta stats without facet counts; options refresh on a slower schedule.
        const videosPromise = fetch(`/api/videos?${params}`);
        const metaParams = new URLSearchParams({ page: currentPage, per_page: perPageVal, sort: sortCol, order: sortOrder, ...activeFilters, include_options: '0' });
        const metaPromise = fetch(`/api/videos/meta?${metaParams}`);

        const res = await videosPromise;
        if (loadId !== loadDataSeq) return;
        if (!res.ok) throw new Error(`Server Error: ${res.status}`);
        const data = await res.json();
        if (loadId !== loadDataSeq) return;
        if (!data || !data.rows) {
            throw new Error("Invalid API response: missing data or rows");
        }

        // Immediate filtered/library counts from the light response
        document.getElementById('res-filtered').innerText = data.total_items;
        if (data.library_total != null) {
            document.getElementById('res-total-display').innerText = data.library_total;
        }
        
        // Only load settings once; avoid recalculating widths on every data refresh
        if (!settingsLoaded) {
            await updateSettingsUI();
            if (loadId !== loadDataSeq) return;
        }
        updateExportButtonText(); // Initialize export button text
        loadFilterPresetsList(); // Load filter presets list
        
        // Update sort icon display
        document.querySelectorAll('.sort-icon').forEach(e => e.innerText = '');
        const arrow = sortOrder === 'asc' ? '▲' : '▼';
        const icon = document.getElementById('sort-' + sortCol);
        if(icon) icon.innerText = arrow;

        if (data.rows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="52" style="text-align:center; padding: 20px;">No results found</td></tr>';
        } else {
            currentRowData = [];
            currentRowDataEncoded = [];
            currentRows = [];
            tbody.innerHTML = data.rows.map((row, rowIndex) => {
                const hasSourceHybrid = row.length >= 51;
                const off = (row.length >= 64) ? 1 : 0;  // +1 when missing column present
                const idx = {
                    filename: 0, category: 1, profile: 2, el_type: 3,
                    container: 4, source_vol: 5, full_path: 6, last_scanned: 7,
                    resolution: 8, bitrate: 9, scan_error: 10, is_hybrid: 11,
                    is_source_hybrid: hasSourceHybrid ? 12 : -1,
                    secondary_hdr: hasSourceHybrid ? 13 : 12,
                    width: hasSourceHybrid ? 14 : 13,
                    height: hasSourceHybrid ? 15 : 14,
                    file_size: hasSourceHybrid ? 16 : 15,
                    bl_id: hasSourceHybrid ? 17 : 16,
                    audio_codecs: hasSourceHybrid ? 18 : 17,
                    audio_langs: hasSourceHybrid ? 19 : -1,
                    audio_channels: hasSourceHybrid ? 20 : 18,
                    subtitles: hasSourceHybrid ? 21 : 19,
                    max_cll: hasSourceHybrid ? 22 : 20,
                    max_fall: hasSourceHybrid ? 23 : 21,
                    video_source: hasSourceHybrid ? 24 : 22,
                    source_format: hasSourceHybrid ? 25 : 23,
                    video_codec: hasSourceHybrid ? 26 : 24,
                    is_3d: hasSourceHybrid ? 27 : 25,
                    edition: hasSourceHybrid ? 28 : 26,
                    year: hasSourceHybrid ? 29 : 27,
                    media_type: hasSourceHybrid ? 30 : 28,
                    show_title: hasSourceHybrid ? 31 : 29,
                    season: hasSourceHybrid ? 32 : 30,
                    episode: hasSourceHybrid ? 33 : 31,
                    movie_title: hasSourceHybrid ? 34 : 32,
                    episode_title: hasSourceHybrid ? 35 : 33,
                    nfo_missing: hasSourceHybrid ? 36 : 34,
                    missing: off ? (hasSourceHybrid ? 37 : 35) : -1,
                    fps: (hasSourceHybrid ? 37 : 35) + off,
                    aspect_ratio: (hasSourceHybrid ? 38 : 36) + off,
                    imdb_id: (hasSourceHybrid ? 39 : 37) + off,
                    tvdb_id: (hasSourceHybrid ? 40 : 38) + off,
                    tmdb_id: (hasSourceHybrid ? 41 : 39) + off,
                    rotten_id: (hasSourceHybrid ? 42 : 40) + off,
                    metacritic_id: (hasSourceHybrid ? 43 : 41) + off,
                    trakt_id: (hasSourceHybrid ? 44 : 42) + off,
                    tvdb_series_id: (hasSourceHybrid ? 45 : 43) + off,
                    tvdb_episode_id: (hasSourceHybrid ? 46 : 44) + off,
                    imdb_series_id: (hasSourceHybrid ? 47 : 45) + off,
                    imdb_episode_id: (hasSourceHybrid ? 48 : 46) + off,
                    tmdb_series_id: (hasSourceHybrid ? 49 : 47) + off,
                    tmdb_episode_id: (hasSourceHybrid ? 50 : 48) + off,
                    trakt_series_id: (hasSourceHybrid ? 51 : 49) + off,
                    trakt_episode_id: (hasSourceHybrid ? 52 : 50) + off,
                    rotten_series_id: (hasSourceHybrid ? 53 : 51) + off,
                    rotten_episode_id: (hasSourceHybrid ? 54 : 52) + off,
                    metacritic_series_id: (hasSourceHybrid ? 55 : 53) + off,
                    metacritic_episode_id: (hasSourceHybrid ? 56 : 54) + off,
                    imdb_rating: (hasSourceHybrid ? 57 : 55) + off,
                    tvdb_rating: (hasSourceHybrid ? 58 : 56) + off,
                    tmdb_rating: (hasSourceHybrid ? 59 : 57) + off,
                    rotten_rating: (hasSourceHybrid ? 60 : 58) + off,
                    metacritic_rating: (hasSourceHybrid ? 61 : 59) + off,
                    trakt_rating: (hasSourceHybrid ? 62 : 60) + off,
                    dup_group_key: (hasSourceHybrid ? 63 : 61) + off,
                    dup_exact_key: (hasSourceHybrid ? 64 : 62) + off,
                    dup_count: (hasSourceHybrid ? 65 : 63) + off
                };
                const rowData = {
                    filename: row[idx.filename], category: row[idx.category], profile: row[idx.profile], el_type: row[idx.el_type],
                    container: row[idx.container], source_vol: row[idx.source_vol], full_path: row[idx.full_path], last_scanned: row[idx.last_scanned],
                    resolution: row[idx.resolution], bitrate: row[idx.bitrate], scan_error: row[idx.scan_error], is_hybrid: row[idx.is_hybrid],
                    is_source_hybrid: idx.is_source_hybrid >= 0 ? row[idx.is_source_hybrid] : 0,
                    secondary_hdr: row[idx.secondary_hdr], width: row[idx.width], height: row[idx.height], file_size: row[idx.file_size],
                    bl_id: row[idx.bl_id], audio_codecs: row[idx.audio_codecs], audio_langs: idx.audio_langs >= 0 ? row[idx.audio_langs] : null, audio_channels: row[idx.audio_channels], subtitles: row[idx.subtitles], max_cll: row[idx.max_cll], max_fall: row[idx.max_fall],
                    video_source: row[idx.video_source], source_format: row[idx.source_format], video_codec: row[idx.video_codec], is_3d: row[idx.is_3d], edition: row[idx.edition], year: row[idx.year],
                    media_type: row[idx.media_type], show_title: row[idx.show_title], season: row[idx.season], episode: row[idx.episode], movie_title: row[idx.movie_title], episode_title: row[idx.episode_title],
                    nfo_missing: row[idx.nfo_missing],
                    fps: row[idx.fps], aspect_ratio: row[idx.aspect_ratio],
                    imdb_id: row[idx.imdb_id], tvdb_id: row[idx.tvdb_id], tmdb_id: row[idx.tmdb_id], rotten_id: row[idx.rotten_id], metacritic_id: row[idx.metacritic_id], trakt_id: row[idx.trakt_id],
                    tvdb_series_id: idx.tvdb_series_id >= 0 ? row[idx.tvdb_series_id] : null, tvdb_episode_id: idx.tvdb_episode_id >= 0 ? row[idx.tvdb_episode_id] : null,
                    imdb_series_id: idx.imdb_series_id >= 0 ? row[idx.imdb_series_id] : null, imdb_episode_id: idx.imdb_episode_id >= 0 ? row[idx.imdb_episode_id] : null,
                    tmdb_series_id: idx.tmdb_series_id >= 0 ? row[idx.tmdb_series_id] : null, tmdb_episode_id: idx.tmdb_episode_id >= 0 ? row[idx.tmdb_episode_id] : null,
                    trakt_series_id: idx.trakt_series_id >= 0 ? row[idx.trakt_series_id] : null, trakt_episode_id: idx.trakt_episode_id >= 0 ? row[idx.trakt_episode_id] : null,
                    rotten_series_id: idx.rotten_series_id >= 0 ? row[idx.rotten_series_id] : null, rotten_episode_id: idx.rotten_episode_id >= 0 ? row[idx.rotten_episode_id] : null,
                    metacritic_series_id: idx.metacritic_series_id >= 0 ? row[idx.metacritic_series_id] : null, metacritic_episode_id: idx.metacritic_episode_id >= 0 ? row[idx.metacritic_episode_id] : null,
                    imdb_rating: row[idx.imdb_rating], tvdb_rating: row[idx.tvdb_rating], tmdb_rating: row[idx.tmdb_rating], rotten_rating: row[idx.rotten_rating], metacritic_rating: row[idx.metacritic_rating], trakt_rating: row[idx.trakt_rating],
                    dup_group_key: row[idx.dup_group_key], dup_exact_key: row[idx.dup_exact_key], dup_count: row[idx.dup_count]
                };
                const rowJson = encodeURIComponent(JSON.stringify(rowData));
                currentRowData.push(rowData);
                currentRowDataEncoded.push(rowJson);
                
                currentRows.push(row[6]); 
                const hasError = row[10]; 
                const isDualHdr = row[idx.is_hybrid] === 1;
                const isSourceHybrid = idx.is_source_hybrid >= 0 ? row[idx.is_source_hybrid] === 1 : false;
                const nfoMissing = row[idx.nfo_missing] === 1;
                const nfoHtml = row[idx.nfo_missing] == null
                    ? '-'
                    : (nfoMissing
                        ? '<span style="color:#e67e22; font-weight:bold;">Missing</span>'
                        : '<span class="nfo-found-clickable" style="color:#2ecc71; font-weight:bold; cursor:pointer; text-decoration:underline;" data-full-path="' + escAttr(row[idx.full_path] || '') + '" onclick="event.stopPropagation(); openNfoModal(this.getAttribute(\'data-full-path\'))">Found</span>');
                const secHdr = row[idx.secondary_hdr];
                const resText = row[idx.resolution] || '-';
                const vidW = row[idx.width] || 0;
                const vidH = row[idx.height] || 0;
                const vidSize = row[idx.file_size] || 0;
                const dateDisplay = row[7] ? row[7].split(' ')[0] : '--';
                const audioTxt = row[idx.audio_codecs] || '-';
                const audioChTxt = row[idx.audio_channels] || '-';
                const audioLangTxt = idx.audio_langs >= 0 ? (row[idx.audio_langs] || '-') : '-';
                const audioCodecs = audioTxt && audioTxt !== '-' ? audioTxt.split(',').map(v => v.trim()).filter(Boolean) : [];
                const audioChs = audioChTxt && audioChTxt !== '-' ? audioChTxt.split(',').map(v => v.trim()).filter(Boolean) : [];
                const audioLangs = audioLangTxt && audioLangTxt !== '-' ? audioLangTxt.split(',').map(v => v.trim()).filter(Boolean) : [];
                const derivedChs = audioCodecs.map(codec => {
                    const match = codec.match(/(\d+(?:\.\d+)?)(?:\s*(?:ch|channels?))?\b/i);
                    return match ? match[1] : '';
                });
                const audioCombinedTxt = audioCodecs.length
                    ? audioCodecs.map((codec, idx) => {
                        const ch = audioChs[idx] || derivedChs[idx];
                        const lang = audioLangs[idx];
                        if (lang && ch) return `${codec} (${lang}, ${ch}ch)`;
                        if (lang) return `${codec} (${lang})`;
                        if (ch) return `${codec} (${ch}ch)`;
                        return codec;
                    }).join(', ')
                    : '-';
                const subTxt = row[idx.subtitles] ? String(row[idx.subtitles]).substring(0, 10) + (String(row[idx.subtitles]).length>10?'...':'') : '-';
                const cllTxt = row[idx.max_cll] ? row[idx.max_cll] : '-';
                const fallTxt = row[idx.max_fall] ? row[idx.max_fall] : '-';
                const aspectTxt = row[idx.aspect_ratio] || '-';
                const imdbIdTxt = row[idx.imdb_id] || '-';
                const tvdbIdTxt = row[idx.tvdb_id] || '-';
                const tmdbIdTxt = row[idx.tmdb_id] || '-';
                const rottenIdTxt = row[idx.rotten_id] || '-';
                const metaIdTxt = row[idx.metacritic_id] || '-';
                const traktIdTxt = row[idx.trakt_id] || '-';
                const tvdbSeriesTxt = idx.tvdb_series_id >= 0 ? (row[idx.tvdb_series_id] || '-') : '-';
                const tvdbEpisodeTxt = idx.tvdb_episode_id >= 0 ? (row[idx.tvdb_episode_id] || '-') : '-';
                const imdbSeriesTxt = idx.imdb_series_id >= 0 ? (row[idx.imdb_series_id] || '-') : '-';
                const imdbEpisodeTxt = idx.imdb_episode_id >= 0 ? (row[idx.imdb_episode_id] || '-') : '-';
                const tmdbSeriesTxt = idx.tmdb_series_id >= 0 ? (row[idx.tmdb_series_id] || '-') : '-';
                const tmdbEpisodeTxt = idx.tmdb_episode_id >= 0 ? (row[idx.tmdb_episode_id] || '-') : '-';
                const traktSeriesTxt = idx.trakt_series_id >= 0 ? (row[idx.trakt_series_id] || '-') : '-';
                const traktEpisodeTxt = idx.trakt_episode_id >= 0 ? (row[idx.trakt_episode_id] || '-') : '-';
                const rottenSeriesTxt = idx.rotten_series_id >= 0 ? (row[idx.rotten_series_id] || '-') : '-';
                const rottenEpisodeTxt = idx.rotten_episode_id >= 0 ? (row[idx.rotten_episode_id] || '-') : '-';
                const metaSeriesTxt = idx.metacritic_series_id >= 0 ? (row[idx.metacritic_series_id] || '-') : '-';
                const metaEpisodeTxt = idx.metacritic_episode_id >= 0 ? (row[idx.metacritic_episode_id] || '-') : '-';
                const imdbRtTxt = row[idx.imdb_rating] ?? '-';
                const tvdbRtTxt = row[idx.tvdb_rating] ?? '-';
                const tmdbRtTxt = row[idx.tmdb_rating] ?? '-';
                const rottenRtTxt = row[idx.rotten_rating] ?? '-';
                const metaRtTxt = row[idx.metacritic_rating] ?? '-';
                const traktRtTxt = row[idx.trakt_rating] ?? '-';
                const dupCount = Number(row[idx.dup_count] || 0);
                const dupGroupKey = row[idx.dup_group_key] || '';
                const dupExactKey = row[idx.dup_exact_key] || '';
                const dupBadge = dupCount > 1
                    ? `<span class="badge badge-dup" onclick="event.stopPropagation(); openDuplicateGroupFromRow(decodeURIComponent('${escAttr(encodeURIComponent(dupGroupKey))}'), decodeURIComponent('${escAttr(encodeURIComponent(dupExactKey))}'))" title="Open duplicate group">x${dupCount}</span>`
                    : '-';

                let mainBadge = '', profileBadge = '-', elBadge = '-', secBadge = '-', dualHdrHtml = '<span style="opacity:0.3">No</span>', sourceHybridHtml = '<span style="opacity:0.3">No</span>';
                let statusText = `<span class="badge badge-ok" onclick="setFilter('status', 'ok')">OK</span>`;
                
                if (hasError) {
                    const safeError = (hasError || '').replace(/"/g, '&quot;');
                    statusText = `<span class="badge badge-fail" title="${safeError}" style="cursor: help;" onclick="setFilter('status', 'failed')">FAILED ⓘ</span>`;
                    mainBadge = '<span style="color:#888">-</span>';
                } else {
                    let clickFn = '';
                    if (row[1] === 'dovi') {
                        const profVal = row[2] ? String(row[2]) : '';
                        clickFn = `filterBadge('dovi')`;
                        mainBadge = `<span class="badge badge-dovi" onclick="${clickFn}" title="Filter: Dolby Vision" style="cursor:pointer">Dolby Vision</span>`;
                        if (profVal) {
                            let profCls = 'badge-p7';
                            if (profVal === '5') profCls = 'badge-p5';
                            else if (profVal === '8.1') profCls = 'badge-p8';
                            else if (profVal === '8.4') profCls = 'badge-p84';
                            else if (profVal === '8') profCls = 'badge-p8';
                            else if (profVal === '10') profCls = 'badge-p10';
                            else if (profVal === '10.1') profCls = 'badge-p101';
                            else if (profVal === '10.4') profCls = 'badge-p104';
                            else if (profVal === '20') profCls = 'badge-p20';
                            const profLabel = `P${profVal}`;
                            const profClick = `filterBadge('dovi', '${profVal}')`;
                            profileBadge = `<span class="badge ${profCls}" onclick="${profClick}" title="Filter: DV ${profLabel}" style="cursor:pointer">${profLabel}</span>`;
                        }
                    } else {
                        let cls = `badge-${row[1]}`, txt = (row[1] || '').toUpperCase().replace('_ONLY', '');
                        if (row[1] === 'hdr10plus') txt = 'HDR10Plus';
                        clickFn = `filterBadge('${row[1] || ''}')`;
                        mainBadge = `<span class="badge ${cls}" onclick="${clickFn}" title="Filter: ${txt}" style="cursor:pointer">${txt}</span>`;
                    }
                    if (secHdr) {
                        const secHdrText = String(secHdr || '');
                        let cls = `badge-${secHdrText.toLowerCase().replace('+', 'plus').replace(' ', '')}`;
                        let txt = secHdrText.replace('+', 'Plus');
                        const secClick = `filterBadge(null, null, null, '${secHdrText.replace(/'/g, "\\'")}')`;
                        secBadge = `<span class="badge ${cls}" onclick="${secClick}" title="Filter Secondary: ${txt}" style="cursor:pointer">${txt}</span>`;
                    }
                    const elType = row[3] ? String(row[3]).toUpperCase() : '';
                    if (elType === 'FEL' || elType === 'MEL') {
                        const elCls = elType === 'FEL' ? 'badge-el-fel' : 'badge-el-mel';
                        const elClick = `filterBadge(null, null, '${elType}')`;
                        elBadge = `<span class="badge ${elCls}" onclick="${elClick}" title="Filter EL: ${elType}" style="cursor:pointer">${elType}</span>`;
                    } else if (elType) {
                        elBadge = elType;
                    }
                    if (isDualHdr) dualHdrHtml = `<span style="color:#ffb6c1; font-weight:bold;">Yes</span>`;
                    if (isSourceHybrid) sourceHybridHtml = `<span style="color:#A0DE14; font-weight:bold;">Yes</span>`;
                }

                const isChecked = selectedPaths.has(row[6]) ? 'checked' : '';
                const safePath = String(row[idx.full_path] ?? '').replace(/'/g, "\\'");
                const resIconHtml = resText !== '-' ? `<div class="res-icon-wrapper" style="display:inline-block;" onclick="setFilter('resolution', '${String(resText).replace(/'/g, "\\'")}')"><img src="/static/${String(resText).toLowerCase()}.png" alt="${resText}" style="height:28px; vertical-align:middle;" onerror="this.outerHTML='<span>'+this.alt+'</span>'"></div>` : '-';

                return `<tr data-row-index="${rowIndex}" onclick="handleRowClick(event, ${rowIndex}, '${safePath}')" oncontextmenu="openRowContextMenu(event, ${rowIndex}, '${safePath}')">
                        <td class="col-chk"><input type="checkbox" class="row-chk" ${isChecked} onclick="event.stopPropagation(); toggleRow(this, '${safePath}')"></td>
                        <td title="${escAttr(row[idx.full_path])}" class="col-file">
                            <div class="file-name" style="cursor:pointer; color:#fff;" onclick="event.stopPropagation(); showDetails('${rowJson}')">${escHtml(row[0])}</div>
                            <div class="file-path">${escHtml(row[idx.full_path])}</div>
                        </td>
                        <td class="td-center col-hyb">${dualHdrHtml}</td>
                        <td class="td-center col-hybrid-src">${sourceHybridHtml}</td>
                        <td class="td-center col-main">${mainBadge}</td>
                        <td class="td-center col-prof">${profileBadge}</td>
                        <td class="td-center col-el hide-col-el">${elBadge}</td>
                        <td class="td-center col-sec">${secBadge}</td>
                        <td class="td-center col-res">${resIconHtml}</td>
                        <td class="td-center col-width" style="font-family:monospace; font-size:0.8em; color:#aaa;">${vidW || '-'}</td>
                        <td class="td-center col-height" style="font-family:monospace; font-size:0.8em; color:#aaa;">${vidH || '-'}</td>
                        <td class="col-size" style="font-family:monospace; color:#ccc;">${formatBytes(vidSize)}</td>
                        <td class="col-bit">${row[idx.bitrate] ? `${escHtml(row[idx.bitrate])} Mbps` : 'N/A'}</td>
                        <td class="col-vol">${escHtml(row[idx.source_vol] || '')}</td>
                        <td class="td-center col-cont" style="font-family:monospace; font-size:0.85em; color:#aaa; text-transform:uppercase;">${escTextOrDash(row[idx.container])}</td>
                        <td class="col-scan" style="opacity:0.4; font-size:0.8em">${dateDisplay}</td>
                        <td class="td-center col-stat">${statusText}</td>
                        <td class="td-center col-nfo">${nfoHtml}</td>
                        <td class="td-center col-missing">${idx.missing >= 0 && row[idx.missing] === 1 ? '<span style="color:#e67e22; font-weight:bold;">Yes</span>' : '-'}</td>
                        <td class="td-center col-dup">${dupBadge}</td>
                        <td class="col-audio hide-col-audio" title="${escAttr(audioTxt)}">${escHtml(audioTxt)}</td>
                        <td class="td-center col-audio-ch hide-col-audio-ch">${escHtml(audioChTxt)}</td>
                        <td class="col-audio-combined hide-col-audio-combined" title="${escAttr(audioCombinedTxt)}">${escHtml(audioCombinedTxt)}</td>
                        <td class="col-sub hide-col-sub" title="${escAttr(row[20])}">${escHtml(subTxt)}</td>
                        <td class="col-cll hide-col-cll" title="MaxCLL / FALL">${escTextOrDash(cllTxt)}</td>
                        <td class="col-fall hide-col-fall" title="MaxFALL">${escTextOrDash(fallTxt)}</td>
                        <td class="td-center col-video-source hide-col-video-source">${escTextOrDash(row[idx.video_source])}</td>
                        <td class="td-center col-source-format hide-col-source-format">${escTextOrDash(row[idx.source_format])}</td>
                        <td class="td-center col-video-codec hide-col-video-codec">${escTextOrDash(row[idx.video_codec])}</td>
                        <td class="td-center col-is-3d hide-col-is-3d">${row[idx.is_3d] === 1 ? 'YES' : '-'}</td>
                        <td class="col-edition hide-col-edition">${escTextOrDash(row[idx.edition])}</td>
                        <td class="td-center col-year hide-col-year">${escTextOrDash(row[idx.year])}</td>
                        <td class="td-center col-media-type hide-col-media-type">${row[idx.media_type] ? escHtml(row[idx.media_type].toString().toUpperCase()) : '-'}</td>
                        <td class="col-show-title hide-col-show-title" title="${escAttr(row[idx.show_title])}">${escHtml(row[idx.show_title]) || '-'}</td>
                        <td class="td-center col-season hide-col-season">${escTextOrDash(row[idx.season])}</td>
                        <td class="td-center col-episode hide-col-episode">${escTextOrDash(row[idx.episode])}</td>
                        <td class="col-movie-title hide-col-movie-title" title="${escAttr(row[idx.movie_title])}">${escHtml(row[idx.movie_title]) || '-'}</td>
                        <td class="col-episode-title hide-col-episode-title" title="${escAttr(row[idx.episode_title])}">${escHtml(row[idx.episode_title]) || '-'}</td>
                        <td class="td-center col-aspect hide-col-aspect">${escTextOrDash(aspectTxt)}</td>
                        <td class="td-center col-imdb-id hide-col-imdb-id" title="${escAttr(imdbIdTxt)}">${escTextOrDash(imdbIdTxt)}</td>
                        <td class="td-center col-tvdb-id hide-col-tvdb-id" title="${escAttr(tvdbIdTxt)}">${escTextOrDash(tvdbIdTxt)}</td>
                        <td class="td-center col-tmdb-id hide-col-tmdb-id" title="${escAttr(tmdbIdTxt)}">${escTextOrDash(tmdbIdTxt)}</td>
                        <td class="td-center col-rotten-id hide-col-rotten-id" title="${escAttr(rottenIdTxt)}">${escTextOrDash(rottenIdTxt)}</td>
                        <td class="td-center col-metacritic-id hide-col-metacritic-id" title="${escAttr(metaIdTxt)}">${escTextOrDash(metaIdTxt)}</td>
                        <td class="td-center col-trakt-id hide-col-trakt-id" title="${escAttr(traktIdTxt)}">${escTextOrDash(traktIdTxt)}</td>
                        <td class="td-center col-tvdb-series-id hide-col-tvdb-series-id" title="${escAttr(tvdbSeriesTxt)}">${escTextOrDash(tvdbSeriesTxt)}</td>
                        <td class="td-center col-tvdb-episode-id hide-col-tvdb-episode-id" title="${escAttr(tvdbEpisodeTxt)}">${escTextOrDash(tvdbEpisodeTxt)}</td>
                        <td class="td-center col-imdb-series-id hide-col-imdb-series-id" title="${escAttr(imdbSeriesTxt)}">${escTextOrDash(imdbSeriesTxt)}</td>
                        <td class="td-center col-imdb-episode-id hide-col-imdb-episode-id" title="${escAttr(imdbEpisodeTxt)}">${escTextOrDash(imdbEpisodeTxt)}</td>
                        <td class="td-center col-tmdb-series-id hide-col-tmdb-series-id" title="${escAttr(tmdbSeriesTxt)}">${escTextOrDash(tmdbSeriesTxt)}</td>
                        <td class="td-center col-tmdb-episode-id hide-col-tmdb-episode-id" title="${escAttr(tmdbEpisodeTxt)}">${escTextOrDash(tmdbEpisodeTxt)}</td>
                        <td class="td-center col-trakt-series-id hide-col-trakt-series-id" title="${escAttr(traktSeriesTxt)}">${escTextOrDash(traktSeriesTxt)}</td>
                        <td class="td-center col-trakt-episode-id hide-col-trakt-episode-id" title="${escAttr(traktEpisodeTxt)}">${escTextOrDash(traktEpisodeTxt)}</td>
                        <td class="td-center col-rotten-series-id hide-col-rotten-series-id" title="${escAttr(rottenSeriesTxt)}">${escTextOrDash(rottenSeriesTxt)}</td>
                        <td class="td-center col-rotten-episode-id hide-col-rotten-episode-id" title="${escAttr(rottenEpisodeTxt)}">${escTextOrDash(rottenEpisodeTxt)}</td>
                        <td class="td-center col-metacritic-series-id hide-col-metacritic-series-id" title="${escAttr(metaSeriesTxt)}">${escTextOrDash(metaSeriesTxt)}</td>
                        <td class="td-center col-metacritic-episode-id hide-col-metacritic-episode-id" title="${escAttr(metaEpisodeTxt)}">${escTextOrDash(metaEpisodeTxt)}</td>
                        <td class="td-center col-imdb-rating hide-col-imdb-rating">${escTextOrDash(imdbRtTxt)}</td>
                        <td class="td-center col-tvdb-rating hide-col-tvdb-rating">${escTextOrDash(tvdbRtTxt)}</td>
                        <td class="td-center col-tmdb-rating hide-col-tmdb-rating">${escTextOrDash(tmdbRtTxt)}</td>
                        <td class="td-center col-rotten-rating hide-col-rotten-rating">${escTextOrDash(rottenRtTxt)}</td>
                        <td class="td-center col-metacritic-rating hide-col-metacritic-rating">${escTextOrDash(metaRtTxt)}</td>
                        <td class="td-center col-trakt-rating hide-col-trakt-rating">${escTextOrDash(traktRtTxt)}</td>
                        <td class="col-del"><button class="trash-btn" onclick="event.stopPropagation(); promptDelete('${safePath}')">🗑</button></td>
                    </tr>`;
            }).join('');
        }
        if (savedColumnOrder) {
            applyColumnOrder(savedColumnOrder);
        }
        // Ensure visual data columns always follow current header order exactly.
        alignBodyColumnsToHeaderOrder();
        applyCollapsedColumnCellStyles();
        applyStoredColumnWidths();
        // Deterministic width sync now (not double-RAF) so top-of-table always uses global colgroup widths.
        syncColgroupFromStoredWidths();
        requestAnimationFrame(updateStickyHeader);
        initColumnDrag();
        initHeaderScrollbar();

        totalPages = data.total_pages;
        document.getElementById('page-jump').value = data.page;
        document.getElementById('pageTotalDisplay').innerText = totalPages;
        document.getElementById('res-filtered').innerText = data.total_items;
        if (data.library_total != null) {
            document.getElementById('res-total-display').innerText = data.library_total;
        }
        document.getElementById('master-chk').style.visibility = data.rows.length > 0 ? 'visible' : 'hidden';
        updateDeleteBtn();
        
        // Don't re-apply visible columns here; it forces a width recalculation
        // and overrides saved column widths on reload.
        if (selectedRowIndex >= data.rows.length) {
            selectedRowIndex = data.rows.length - 1;
        }
        updateSelectedRowHighlight();

        // Table is ready — drop the loading overlay before waiting on heavy meta.
        isLoading = false;
        const loadingBackdropDone = document.getElementById('table-loading-backdrop');
        if (loadingBackdropDone) loadingBackdropDone.style.display = 'none';
        if (loadingOverlay) loadingOverlay.style.display = 'none';
        document.body.classList.remove('loading-table');

        try {
            const metaRes = await metaPromise;
            if (loadId !== loadDataSeq) return;
            if (metaRes.ok) {
                const meta = await metaRes.json();
                if (loadId !== loadDataSeq) return;
                applyVideosMeta(meta);
                scheduleFilterOptionsRefresh();
            } else {
                notifyVideosMetaFailure(new Error(`HTTP ${metaRes.status}`));
            }
        } catch (metaErr) {
            console.error('Failed to load videos meta:', metaErr);
            if (loadId === loadDataSeq) notifyVideosMetaFailure(metaErr);
        }

    } catch (e) {
        console.error(e);
        const tbody = document.getElementById('video-table-body');
        if (tbody) {
            tbody.innerHTML = `<tr><td colspan="52" style="text-align:center; padding: 20px; color:#e74c3c; font-weight:bold;">Error Loading Data: ${e.message}</td></tr>`;
        }
    } finally {
        if (loadId === loadDataSeq) {
            isLoading = false;
            const loadingBackdrop = document.getElementById('table-loading-backdrop');
            if (loadingBackdrop) loadingBackdrop.style.display = 'none';
            if (loadingOverlay) loadingOverlay.style.display = 'none';
            document.body.classList.remove('loading-table');
        }
        if (pendingReload) {
            pendingReload = false;
            resetAndLoadImmediate();
        }
    }
}

window.addEventListener('keydown', (e) => {
    if (isTypingTarget(e.target)) return;
    const key = e.key;
    if (key === 'j' || key === 'ArrowDown') {
        e.preventDefault();
        moveRowSelection(1);
        return;
    }
    if (key === 'k' || key === 'ArrowUp') {
        e.preventDefault();
        moveRowSelection(-1);
        return;
    }
    if (key === 'Enter') {
        if (selectedRowIndex >= 0 && currentRowDataEncoded[selectedRowIndex]) {
            e.preventDefault();
            showDetails(currentRowDataEncoded[selectedRowIndex]);
        }
        return;
    }
    if (key === 'r' || key === 'R') {
        if (selectedRowIndex >= 0 && currentRowData[selectedRowIndex]) {
            e.preventDefault();
            rescanSelectedFile();
        }
        return;
    }
});

document.addEventListener('click', (e) => {
    const suggestionsEl = document.getElementById('search-suggestions');
    if (!suggestionsEl) return;
    if (!e.target.closest('.search-wrap')) {
        suggestionsEl.style.display = 'none';
    }
});

document.addEventListener('click', (e) => {
    const menu = document.getElementById('export-mode-menu');
    if (menu && !e.target.closest('#export-group')) {
        menu.classList.remove('active');
    }
});

document.addEventListener('click', (e) => {
    if (!e.target.closest('#row-context-menu')) {
        closeContextMenu();
    }
});

document.addEventListener('input', (e) => {
    if (e.target && e.target.id === 'search-bar') {
        updateSearchSuggestions();
    }
});

document.addEventListener('keydown', (e) => {
    const target = e.target;
    if (!target || target.id !== 'search-bar') return;
    const suggestionsEl = document.getElementById('search-suggestions');
    const items = getSearchSuggestionItems();
    const isOpen = suggestionsEl && suggestionsEl.style.display === 'block' && items.length > 0;
    if (!isOpen) return;
    if (e.key === 'ArrowDown') {
        e.preventDefault();
        searchSuggestionIndex = (searchSuggestionIndex + 1) % items.length;
        setActiveSearchSuggestion(searchSuggestionIndex);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        searchSuggestionIndex = (searchSuggestionIndex - 1 + items.length) % items.length;
        setActiveSearchSuggestion(searchSuggestionIndex);
    } else if (e.key === 'Enter') {
        if (searchSuggestionIndex >= 0 && items[searchSuggestionIndex]) {
            e.preventDefault();
            const value = items[searchSuggestionIndex].getAttribute('data-value');
            if (value) applySearchSuggestion(value);
        }
    } else if (e.key === 'Tab') {
        const pickIndex = searchSuggestionIndex >= 0 ? searchSuggestionIndex : 0;
        const item = items[pickIndex];
        if (item) {
            e.preventDefault();
            const value = item.getAttribute('data-value');
            if (value) applySearchSuggestion(value);
        }
    } else if (e.key === 'Escape') {
        if (suggestionsEl) {
            suggestionsEl.style.display = 'none';
            suggestionsEl.innerHTML = '';
        }
        searchSuggestionIndex = -1;
    }
});

document.addEventListener('click', (e) => {
    const target = e.target;
    if (target && target.classList && target.classList.contains('search-suggestion')) {
        const value = target.getAttribute('data-value');
        if (value) {
            applySearchSuggestion(value);
        }
    }
});
