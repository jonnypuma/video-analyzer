// --- INTERACTION FUNCTIONS ---
function toggleConfigMode() {
    const btn = document.getElementById('btn-settings-anim');
    const grpSched = document.getElementById('group-schedule');
    const grpFilt = document.getElementById('group-filters');

    btn.classList.toggle('active');
    if (configMode === 'schedule') {
        configMode = 'filters';
        grpSched.classList.add('hidden');
        grpFilt.classList.remove('hidden');
    } else {
        configMode = 'schedule';
        grpFilt.classList.add('hidden');
        grpSched.classList.remove('hidden');
    }
}

async function toggleScanMenu(event) {
    if (event) event.stopPropagation();
    const menu = document.getElementById('scan-mode-menu');
    if (!menu) return;
    menu.classList.toggle('active');
    if (menu.classList.contains('active')) {
        if (!scanFolders || scanFolders.length === 0) {
            await refreshScanFolders();
        }
        renderScanSubmenus();
    }
}

function selectScanMode(mode) {
    scanMode = mode || 'all';
    scanFolderTarget = null;
    updateScanButtonLabel();
    const menu = document.getElementById('scan-mode-menu');
    if (menu) menu.classList.remove('active');
}

function selectScanFolder(mode, encodedPath) {
    scanMode = mode || 'all';
    try {
        scanFolderTarget = JSON.parse(decodeURIComponent(encodedPath));
    } catch (e) {
        scanFolderTarget = null;
    }
    updateScanButtonLabel();
    const menu = document.getElementById('scan-mode-menu');
    if (menu) menu.classList.remove('active');
}

function updateScanButtonLabel() {
    const mainEl = document.getElementById('scan-label-main');
    const subEl = document.getElementById('scan-label-sub');
    if (!mainEl || !subEl) return;
    if (scanMode === 'tv') mainEl.innerText = 'Scan | TV';
    else if (scanMode === 'movie') mainEl.innerText = 'Scan | Movie';
    else mainEl.innerText = 'Scan | All';
    if (scanFolderTarget && scanFolderTarget.volume) {
        const pathText = scanFolderTarget.path ? `/${scanFolderTarget.path}` : '/';
        subEl.innerText = `Target: ${scanFolderTarget.volume}${pathText}`;
        subEl.style.display = '';
    } else {
        subEl.innerText = '';
        subEl.style.display = 'none';
    }
}

function renderScanSubmenus() {
    const tvMenu = document.getElementById('scan-tv-submenu');
    const movieMenu = document.getElementById('scan-movie-submenu');
    if (!tvMenu || !movieMenu) return;
    const tvFolders = scanFolders.filter(f => !f.muted && (f.type || 'auto') === 'tv');
    const movieFolders = scanFolders.filter(f => !f.muted && (f.type || 'auto') === 'movie');
    const renderItems = (folders, mode) => {
        if (!scanFolders.length) {
            return `<div class="scan-menu-label" style="cursor:default; color:#777;">No folders configured</div>`;
        }
        if (!folders.length) {
            return `<div class="scan-menu-label" style="cursor:default; color:#777;">No ${mode.toUpperCase()} folders (set type in Scan Folders)</div>`;
        }
        return folders.map(f => {
            const payload = encodeURIComponent(JSON.stringify({ volume: f.volume, path: f.path || '' }));
            const label = `${f.volume}${f.path ? '/' + f.path : '/'}`;
            return `<button onclick="selectScanFolder('${mode}', '${payload}')">${escHtml(label)}</button>`;
        }).join('');
    };
    tvMenu.innerHTML = renderItems(tvFolders, 'tv');
    movieMenu.innerHTML = renderItems(movieFolders, 'movie');
}

async function refreshScanFolders() {
    try {
        const res = await fetch('/api/settings');
        const data = await res.json();
        if (data.scan_folders) {
            const parsed = JSON.parse(data.scan_folders);
            scanFolders = Array.isArray(parsed) ? parsed : [];
        }
    } catch (e) {
        console.error('Failed to refresh scan folders', e);
    }
}

document.addEventListener('click', (e) => {
    const menu = document.getElementById('scan-mode-menu');
    if (!menu) return;
    if (!e.target.closest('.btn-group-scan')) {
        menu.classList.remove('active');
    }
});

function toggleSchedInput() {
    const mode = document.getElementById('sched-mode').value;
    const valContainer = document.getElementById('sched-val-container');
    Array.from(valContainer.children).forEach(el => el.style.display = 'none');
    
    if (mode === 'daily') {
        document.getElementById('sched-val-time').style.display = 'block';
    } else if (mode === 'interval') {
        document.getElementById('sched-val-hours').style.display = 'block';
    } else if (mode === 'weekly') {
        document.getElementById('sched-val-week').style.display = 'inline-block';
        document.getElementById('sched-val-time').style.display = 'block';
    } else if (mode === 'monthly') {
        document.getElementById('sched-val-day').style.display = 'inline-block';
        document.getElementById('sched-val-time').style.display = 'block';
    }
}

function parseSchedValueParts(raw) {
    const text = String(raw || '');
    const idx = text.indexOf('|');
    if (idx < 0) return { primary: text, time: '' };
    return { primary: text.slice(0, idx), time: text.slice(idx + 1) };
}

// --- MULTISELECT FORMAT FILTER ---
let formatMultiselectOpen = false;

function positionFormatDropdown(dropdown, button) {
    if (!dropdown || !button) return;
    const rect = button.getBoundingClientRect();
    dropdown.style.position = 'fixed';
    dropdown.style.left = `${Math.round(rect.left)}px`;
    dropdown.style.minWidth = `${Math.round(rect.width)}px`;
    const dropHeight = dropdown.offsetHeight || 0;
    let top = rect.bottom + 4;
    if (top + dropHeight > window.innerHeight - 8) {
        top = Math.max(8, rect.top - dropHeight - 4);
    }
    dropdown.style.top = `${Math.round(top)}px`;
    dropdown.style.zIndex = '1200';
}

function toggleFormatMultiselect(event) {
    event.stopPropagation();
    const dropdown = document.getElementById('format-filter-dropdown');
    const button = document.getElementById('format-filter-button');
    formatMultiselectOpen = !formatMultiselectOpen;
    dropdown.classList.toggle('active', formatMultiselectOpen);
    button.classList.toggle('active', formatMultiselectOpen);
    if (formatMultiselectOpen) {
        positionFormatDropdown(dropdown, button);
    } else {
        dropdown.style.position = '';
        dropdown.style.left = '';
        dropdown.style.top = '';
        dropdown.style.minWidth = '';
        dropdown.style.zIndex = '';
    }
    updateHeaderClipForDropdowns();
}

function toggleAllFormats(checkbox) {
    const checkboxes = document.querySelectorAll('#format-filter-dropdown input[type="checkbox"]:not(#format-chk-all)');
    checkboxes.forEach(cb => cb.checked = checkbox.checked);
    updateFormatFilter();
}

function updateFormatFilter(doReload = true) {
    const allCheckbox = document.getElementById('format-chk-all');
    const checkboxes = Array.from(document.querySelectorAll('#format-filter-dropdown input[type="checkbox"]:not(#format-chk-all)'));
    const selected = checkboxes.filter(cb => cb.checked).map(cb => cb.value);
    
    // Update "All" checkbox state
    if (allCheckbox) {
        allCheckbox.checked = selected.length === 0 || selected.length === checkboxes.length;
    }
    
    // Update button text
    const buttonText = document.getElementById('format-filter-text');
    if (buttonText) {
        if (selected.length === 0 || selected.length === checkboxes.length) {
            buttonText.textContent = 'All';
        } else if (selected.length === 1) {
            const labels = { 'dovi': 'Dolby Vision', 'hdr10plus': 'HDR10+', 'hdr10': 'HDR10', 'hlg': 'HLG', 'sdr_only': 'SDR', '__blank__': 'Blanks' };
            buttonText.textContent = labels[selected[0]] || selected[0];
        } else {
            buttonText.textContent = `Format (${selected.length})`;
        }
    }
    
    // Apply filter only if doReload is true
    if (doReload) {
        resetAndLoad();
    }
}

function closeFormatMultiselect() {
    formatMultiselectOpen = false;
    document.getElementById('format-filter-dropdown').classList.remove('active');
    document.getElementById('format-filter-button').classList.remove('active');
    const dropdown = document.getElementById('format-filter-dropdown');
    if (dropdown) {
        dropdown.style.position = '';
        dropdown.style.left = '';
        dropdown.style.top = '';
        dropdown.style.minWidth = '';
        dropdown.style.zIndex = '';
    }
    updateHeaderClipForDropdowns();
}

function getFormatFilterValue() {
    // Check pending value first (set by setFormatFilterValue), then fall back to checkboxes
    if (pendingFormatValue) {
        console.log(`[DEBUG] getFormatFilterValue(): Using pendingFormatValue = "${pendingFormatValue}"`);
        return pendingFormatValue;
    }
    const checkboxes = Array.from(document.querySelectorAll('#format-filter-dropdown input[type="checkbox"]:not(#format-chk-all)'));
    const selected = checkboxes.filter(cb => cb.checked).map(cb => cb.value);
    const result = selected.length === 0 || selected.length === checkboxes.length ? '' : selected.join(',');
    console.log(`[DEBUG] getFormatFilterValue(): Found ${checkboxes.length} checkboxes, ${selected.length} checked, pendingFormatValue = "${pendingFormatValue}", result = "${result}"`);
    return result;
}

function setFormatFilterValue(value, skipExtraReload = false) {
    // Store pending value to persist through dropdown updates
    if (value && value !== '') {
        pendingFormatValue = value;
    } else {
        pendingFormatValue = null;
    }
    
    // Clear all first
    document.querySelectorAll('#format-filter-dropdown input[type="checkbox"]').forEach(cb => cb.checked = false);
    const allCheckbox = document.getElementById('format-chk-all');
    if (allCheckbox) allCheckbox.checked = true;
    
    if (!value || value === '') {
        updateFormatFilter(false);
        return;
    }
    
    const values = value.split(',').map(v => v.trim());
    let foundAny = false;
    values.forEach(val => {
        // Map 'sdr_only' to 'sdr' for checkbox ID
        const checkboxId = val === 'sdr_only' ? 'format-chk-sdr' : (val === '__blank__' ? 'format-chk-blank' : `format-chk-${val}`);
        const checkbox = document.getElementById(checkboxId);
        if (checkbox) {
            checkbox.checked = true;
            foundAny = true;
            if (allCheckbox) allCheckbox.checked = false;
        }
    });
    
    // Update UI without reloading
    updateFormatFilter(false);
    
    // If we found checkboxes, trigger reload after a small delay to ensure state is set
    // skipExtraReload: caller (e.g. applyRibbonFilter) will call resetAndLoad itself to avoid double load
    if (foundAny && !skipExtraReload) {
        setTimeout(() => {
            // Ensure checkboxes are still set before reloading
            const checkboxes = Array.from(document.querySelectorAll('#format-filter-dropdown input[type="checkbox"]:not(#format-chk-all)'));
            const checked = checkboxes.filter(cb => cb.checked);
            if (checked.length === 0) {
                // Re-set checkboxes if they were cleared
                values.forEach(val => {
                    const checkboxId = val === 'sdr_only' ? 'format-chk-sdr' : `format-chk-${val}`;
                    const checkbox = document.getElementById(checkboxId);
                    if (checkbox) checkbox.checked = true;
                });
            }
            resetAndLoad();
        }, 50);
    }
}

// Store pending format filter value to persist through dropdown updates
let pendingFormatValue = null;

function updateFormatMultiselect(categories) {
    if (!categories) return;
    // Check for pending value first, then fall back to current checkbox state
    // Don't clear pendingFormatValue here - let getFormatFilterValue() handle it
    const currentValue = getFormatFilterValue();
    const currentValues = currentValue ? currentValue.split(',').map(v => v.trim()) : [];
    
    const formatMap = {
        'dovi': 'Dolby Vision',
        'hdr10plus': 'HDR10+',
        'hdr10': 'HDR10',
        'hlg': 'HLG',
        'sdr_only': 'SDR'
    };
    
    // Update labels with counts
    Object.keys(formatMap).forEach(key => {
        const checkbox = document.getElementById(`format-chk-${key}`);
        const label = document.querySelector(`label[for="format-chk-${key}"]`);
        if (checkbox && label) {
            const count = categories[key] !== undefined ? categories[key] : 0;
            label.textContent = `${formatMap[key]} (${count})`;
            
            // Preserve checked state if it was selected
            if (currentValues.includes(key)) {
                checkbox.checked = true;
            }
        }
    });

    const blankCheckbox = document.getElementById('format-chk-blank');
    const blankLabel = document.querySelector('label[for="format-chk-blank"]');
    if (blankCheckbox && blankLabel) {
        const blankCount = (lastFilterBlanks && lastFilterBlanks.category !== undefined) ? lastFilterBlanks.category : 0;
        blankLabel.textContent = `Blanks (${blankCount})`;
        if (currentValues.includes('__blank__')) {
            blankCheckbox.checked = true;
        }
    }
    
    // Update "All" checkbox state
    const allCheckbox = document.getElementById('format-chk-all');
    const checkboxes = Array.from(document.querySelectorAll('#format-filter-dropdown input[type="checkbox"]:not(#format-chk-all)'));
    const selected = checkboxes.filter(cb => cb.checked);
    if (allCheckbox) {
        allCheckbox.checked = selected.length === 0 || selected.length === checkboxes.length;
    }
    
    // Update button text without reloading
    updateFormatFilter(false);
}

// Close multiselect when clicking outside
document.addEventListener('click', (e) => {
    if (formatMultiselectOpen && !e.target.closest('.multiselect-wrapper')) {
        closeFormatMultiselect();
    }
});

// --- GENERIC MULTISELECT SYSTEM ---
const multiselectState = {};

function positionMultiselectDropdown(state) {
    if (!state || !state.dropdown || !state.button) return;
    const rect = state.button.getBoundingClientRect();
    state.dropdown.style.position = 'fixed';
    state.dropdown.style.left = `${Math.round(rect.left)}px`;
    state.dropdown.style.minWidth = `${Math.round(rect.width)}px`;
    const dropHeight = state.dropdown.offsetHeight || 0;
    let top = rect.bottom + 4;
    if (top + dropHeight > window.innerHeight - 8) {
        top = Math.max(8, rect.top - dropHeight - 4);
    }
    state.dropdown.style.top = `${Math.round(top)}px`;
}

function initMultiselect(filterId, options, labelMap = {}) {
    const wrapper = document.querySelector(`#${filterId}-wrapper`);
    if (!wrapper) return;
    
    const button = wrapper.querySelector('.multiselect-button');
    const dropdown = wrapper.querySelector('.multiselect-dropdown');
    const buttonText = button.querySelector('span:first-child');
    
    multiselectState[filterId] = { open: false, button, dropdown, buttonText, options, labelMap };
    
    button.onclick = (e) => {
        e.stopPropagation();
        toggleMultiselect(filterId);
    };
    
    // Setup "All" checkbox
    const allCheckbox = dropdown.querySelector(`#${filterId}-chk-all`);
    if (allCheckbox) {
        allCheckbox.onchange = () => {
            const checkboxes = dropdown.querySelectorAll(`input[type="checkbox"]:not(#${filterId}-chk-all)`);
            checkboxes.forEach(cb => cb.checked = allCheckbox.checked);
            updateMultiselectFilter(filterId);
        };
    }
    
    // Setup individual checkboxes
    dropdown.querySelectorAll(`input[type="checkbox"]:not(#${filterId}-chk-all)`).forEach(cb => {
        cb.onchange = () => updateMultiselectFilter(filterId);
    });
}

function toggleMultiselect(filterId) {
    const state = multiselectState[filterId];
    if (!state) return;
    
    state.open = !state.open;
    state.dropdown.classList.toggle('active', state.open);
    state.button.classList.toggle('active', state.open);
    if (state.open) {
        positionMultiselectDropdown(state);
    } else {
        state.dropdown.style.position = '';
        state.dropdown.style.left = '';
        state.dropdown.style.top = '';
        state.dropdown.style.minWidth = '';
    }
    updateHeaderClipForDropdowns();
}

function syncOpenMultiselectPositions() {
    Object.values(multiselectState).forEach((state) => {
        if (state && state.open) {
            positionMultiselectDropdown(state);
        }
    });
}

function closeMultiselect(filterId) {
    const state = multiselectState[filterId];
    if (!state) return;
    
    state.open = false;
    state.dropdown.classList.remove('active');
    state.button.classList.remove('active');
    state.dropdown.style.position = '';
    state.dropdown.style.left = '';
    state.dropdown.style.top = '';
    state.dropdown.style.minWidth = '';
    updateHeaderClipForDropdowns();
}

function updateHeaderClipForDropdowns() {
    const thead = getVideoTableThead();
    if (!thead) return;
    const anyOpen = Object.values(multiselectState).some(s => s.open) || formatMultiselectOpen;
    thead.classList.toggle('no-clip', anyOpen);
}

function getMultiselectValue(filterId) {
    const state = multiselectState[filterId];
    if (!state) {
        console.log(`[DEBUG] getMultiselectValue(${filterId}): No state found`);
        return '';
    }
    
    // Check pending value first (set by setMultiselectValue), then fall back to checkboxes
    if (state.pendingValue) {
        console.log(`[DEBUG] getMultiselectValue(${filterId}): Using pendingValue = "${state.pendingValue}"`);
        return state.pendingValue;
    }
    
    const checkboxes = Array.from(state.dropdown.querySelectorAll(`input[type="checkbox"]:not(#${filterId}-chk-all)`));
    const selected = checkboxes.filter(cb => cb.checked).map(cb => cb.value);
    const result = selected.length === 0 || selected.length === checkboxes.length ? '' : selected.join(',');
    console.log(`[DEBUG] getMultiselectValue(${filterId}): Found ${checkboxes.length} checkboxes, ${selected.length} checked, result = "${result}"`);
    return result;
}

function getMultiselectValueAndClearPending(filterId) {
    const value = getMultiselectValue(filterId);
    const state = multiselectState[filterId];
    if (state && state.pendingValue && value === state.pendingValue) {
        state.pendingValue = null;
    }
    return value;
}

function setMultiselectValue(filterId, value, skipReload = false) {
    const state = multiselectState[filterId];
    if (!state) return;
    
    // Store the value in state so it persists through dropdown rebuilds
    if (value && value !== '') {
        state.pendingValue = value;
    } else {
        state.pendingValue = null;
    }
    
    // Clear all first
    state.dropdown.querySelectorAll(`input[type="checkbox"]`).forEach(cb => cb.checked = false);
    const allCheckbox = state.dropdown.querySelector(`#${filterId}-chk-all`);
    
    if (!value || value === '') {
        // If no value, set "All" to checked
        if (allCheckbox) allCheckbox.checked = true;
        if (!skipReload) {
            updateMultiselectFilter(filterId);
        } else {
            // Just update button text without reloading
            state.buttonText.textContent = 'All';
        }
        return;
    }
    
    // Set specific values - do NOT check "All" checkbox
    const values = value.split(',').map(v => v.trim());
    let foundAny = false;
    values.forEach(val => {
        // Try to find checkbox with exact value match (escape special characters in val)
        const escapedVal = val.replace(/"/g, '&quot;');
        let checkbox = state.dropdown.querySelector(`input[value="${escapedVal}"]`);
        
        // If not found, try without escaping (in case the value in HTML is not escaped)
        if (!checkbox) {
            checkbox = state.dropdown.querySelector(`input[value="${val}"]`);
        }
        
        // If still not found, try to find by ID
        if (!checkbox) {
            checkbox = state.dropdown.querySelector(`#${filterId}-chk-${val.replace(/\./g, '\\.')}`);
        }
        
        // If not found, create it (for cases where dropdown hasn't been populated yet)
        if (!checkbox) {
            // Create a temporary checkbox option for this value
            const optionDiv = document.createElement('div');
            optionDiv.className = 'multiselect-option';
            optionDiv.onclick = (e) => e.stopPropagation();
            const safeId = val.replace(/[^a-zA-Z0-9]/g, '_');
            optionDiv.innerHTML = `<input type="checkbox" id="${filterId}-chk-${safeId}" value="${escAttr(val)}" checked>
                <label for="${filterId}-chk-${safeId}">${escHtml(val)}</label>`;
            state.dropdown.appendChild(optionDiv);
            checkbox = optionDiv.querySelector(`input[value="${escapedVal}"]`);
            
            // Re-initialize event handler for this checkbox
            if (checkbox) {
                checkbox.onchange = () => updateMultiselectFilter(filterId);
            }
        }
        
        if (checkbox) {
            checkbox.checked = true;
            foundAny = true;
        }
    });
    
    // Only check "All" if no specific values were found (shouldn't happen, but safety check)
    if (!foundAny && allCheckbox) {
        allCheckbox.checked = true;
    }
    
    // Don't clear pendingValue if skipReload is true - let loadData() use it
    // Only clear pendingValue when we're not skipping reload (normal user interaction)
    if (!skipReload) {
        // Clear pendingValue now that checkboxes are successfully set
        if (foundAny && state.pendingValue && state.pendingValue === value) {
            state.pendingValue = null;
        }
        updateMultiselectFilter(filterId);
    } else {
        // Keep pendingValue set so loadData() can read it
        // Just update button text without reloading
        const selected = values;
        if (selected.length === 1) {
            state.buttonText.textContent = selected[0] === '__blank__' ? 'Blanks' : (state.labelMap[selected[0]] || selected[0]);
        } else {
            const filterName = filterId.replace('-filter', '').replace(/-/g, ' ');
            state.buttonText.textContent = `${filterName} (${selected.length})`;
        }
    }
}

function updateMultiselectOptions(filterId, options, labelMap = {}, blankCount = null) {
    if (!options) return;
    const dropdown = document.getElementById(`${filterId}-dropdown`);
    if (!dropdown) return;
    
    if (!multiselectState[filterId]) {
        initMultiselect(filterId, options, labelMap);
    } else if (labelMap && Object.keys(labelMap).length) {
        multiselectState[filterId].labelMap = { ...multiselectState[filterId].labelMap, ...labelMap };
    }

    // Check for pending value first (set by setMultiselectValue), then fall back to current value
    const state = multiselectState[filterId];
    let currentValue = '';
    if (state && state.pendingValue) {
        currentValue = state.pendingValue;
        // DON'T clear pending value here - let loadData() use it and clear it
        // pendingValue will be cleared by setMultiselectValue after checkboxes are set
        // or by loadData() after reading it
    } else {
        currentValue = getMultiselectValue(filterId);
    }
    const currentValues = currentValue ? currentValue.split(',').map(v => v.trim()) : [];
    
    let items = Array.isArray(options) ? options : Object.keys(options);
    items = items.filter(item => item !== '__blank__');
    let html = `<div class="multiselect-option" onclick="event.stopPropagation()">
        <input type="checkbox" id="${filterId}-chk-all" checked>
        <label for="${filterId}-chk-all">All</label>
    </div>`;

    const includeBlanks = blankCount !== false;
    if (includeBlanks) {
        const blanksLabel = (blankCount === null || blankCount === undefined) ? 'Blanks' : `Blanks (${blankCount})`;
        html += `<div class="multiselect-option" onclick="event.stopPropagation()">
            <input type="checkbox" id="${filterId}-chk-blank" value="__blank__">
            <label for="${filterId}-chk-blank">${blanksLabel}</label>
        </div>
        <div class="multiselect-divider"></div>`;
    } else {
        html += `<div class="multiselect-divider"></div>`;
    }
    
    items.forEach(key => {
        const val = key.toString();
        let display = labelMap[val] || val.toUpperCase();
        if(val === 'sdr_only') display = 'SDR';
        if(val === 'hdr10plus') display = 'HDR10+';
        if(val === 'none') display = 'None';
        
        const count = !Array.isArray(options) && options[val] !== undefined ? options[val] : 0;
        // Always include items that are currently selected, even if count is 0.
        // Binary filters (blankCount === false) always keep all option keys visible.
        if (!Array.isArray(options) && count === 0 && !currentValues.includes(val) && includeBlanks) return;
        
        display += ` (${count})`;
        const checked = currentValues.includes(val) ? 'checked' : '';
        const safeId = `${filterId}-chk-${val.replace(/[^a-zA-Z0-9_-]/g, '_')}`;
        html += `<div class="multiselect-option" onclick="event.stopPropagation()">
            <input type="checkbox" id="${safeId}" value="${escAttr(val)}" ${checked}>
            <label for="${safeId}">${escHtml(display)}</label>
        </div>`;
    });
    
    dropdown.innerHTML = html;
    
    // Re-initialize event handlers
    const allCheckbox = dropdown.querySelector(`#${filterId}-chk-all`);
    if (allCheckbox) {
        allCheckbox.onchange = () => {
            const checkboxes = dropdown.querySelectorAll(`input[type="checkbox"]:not(#${filterId}-chk-all)`);
            checkboxes.forEach(cb => cb.checked = allCheckbox.checked);
            updateMultiselectFilter(filterId);
        };
    }
    
    dropdown.querySelectorAll(`input[type="checkbox"]:not(#${filterId}-chk-all)`).forEach(cb => {
        cb.onchange = () => updateMultiselectFilter(filterId);
    });
    
    // If there was a pending value, ensure it's applied (checkboxes should already be checked from HTML, but verify)
    if (currentValues.length > 0) {
        currentValues.forEach(val => {
            const escapedVal = val.replace(/"/g, '&quot;');
            let checkbox = dropdown.querySelector(`input[value="${escapedVal}"]`);
            if (!checkbox) {
                checkbox = dropdown.querySelector(`input[value="${val}"]`);
            }
            if (checkbox) {
                checkbox.checked = true;
                if (allCheckbox) allCheckbox.checked = false;
            }
        });
    }
    
    // Update button text without reloading data (avoid page reset during refresh)
    updateMultiselectFilter(filterId, true);
}

function updateMultiselectFilter(filterId, skipReload = false) {
    const state = multiselectState[filterId];
    if (!state) return;
    
    const allCheckbox = state.dropdown.querySelector(`#${filterId}-chk-all`);
    const checkboxes = Array.from(state.dropdown.querySelectorAll(`input[type="checkbox"]:not(#${filterId}-chk-all)`));
    const selected = checkboxes.filter(cb => cb.checked).map(cb => cb.value);
    
    // Update "All" checkbox state
    if (allCheckbox) {
        allCheckbox.checked = selected.length === 0 || selected.length === checkboxes.length;
    }
    
    // Update button text
    if (selected.length === 0 || selected.length === checkboxes.length) {
        state.buttonText.textContent = 'All';
    } else if (selected.length === 1) {
        const label = selected[0] === '__blank__' ? 'Blanks' : (state.labelMap[selected[0]] || selected[0]);
        state.buttonText.textContent = label;
    } else {
        const filterName = filterId.replace('-filter', '').replace(/-/g, ' ');
        state.buttonText.textContent = `${filterName} (${selected.length})`;
    }
    
    // Apply filter but don't close dropdown unless skipReload is set
    if (!skipReload) {
        resetAndLoad();
    }
}

// Close all multiselects when clicking outside
document.addEventListener('click', (e) => {
    Object.keys(multiselectState).forEach(filterId => {
        const state = multiselectState[filterId];
        if (state.open && !e.target.closest(`#${filterId}-wrapper`)) {
            closeMultiselect(filterId);
        }
    });
});

// --- FILTER LOGIC ---
function filterValueSet(val) {
    if (!val) return new Set();
    return new Set(String(val).split(',').map(v => v.trim()).filter(Boolean));
}

function ensureBinaryMultiselects() {
    const specs = [
        ['hybrid-filter', { '1': 'Yes', '0': 'No' }],
        ['source-hybrid-filter', { '1': 'Yes', '0': 'No' }],
        ['status-filter', { 'ok': 'OK', 'failed': 'Failed' }],
        ['nfo-filter', { '1': 'Missing', '0': 'Found' }],
        ['missing-filter', { '1': 'Yes', '0': 'No' }],
        ['is-3d-filter', { '1': '3D', '0': '2D' }],
    ];
    specs.forEach(([id, labelMap]) => {
        if (!multiselectState[id] && document.getElementById(`${id}-wrapper`)) {
            initMultiselect(id, Object.keys(labelMap), labelMap);
        }
    });
}

function updateRibbonActiveState() {
    ensureBinaryMultiselects();
    const formatVals = filterValueSet(activeFilters.category || '');
    const profileVals = filterValueSet(activeFilters.profile || '');
    const elVals = filterValueSet(activeFilters.el || '');
    const hybridVal = activeFilters.is_hybrid || '';
    const srcHybridVal = activeFilters.source_hybrid || '';
    const statusVal = activeFilters.status || '';

    document.querySelectorAll('.stat-card[data-ribbon]').forEach(card => {
        const spec = card.getAttribute('data-ribbon') || '';
        const colon = spec.indexOf(':');
        if (colon < 0) {
            card.classList.remove('is-active');
            return;
        }
        const type = spec.slice(0, colon);
        const value = spec.slice(colon + 1);
        let active = false;
        if (type === 'format') {
            active = formatVals.size === 1 && formatVals.has(value);
        } else if (type === 'dovi_prof') {
            if (value === '10.1') {
                // Ribbon count folds bare "10" into P10.1
                active = elVals.size === 0 && profileVals.size > 0
                    && [...profileVals].every(p => p === '10.1' || p === '10');
            } else {
                active = profileVals.size === 1 && profileVals.has(value) && elVals.size === 0;
            }
        } else if (type === 'el') {
            active = elVals.size === 1 && elVals.has(value) && profileVals.has('7');
        } else if (type === 'hybrid') {
            active = hybridVal === value;
        } else if (type === 'source_hybrid') {
            active = srcHybridVal === value;
        } else if (type === 'status') {
            active = statusVal === value;
        }
        card.classList.toggle('is-active', !!active);
    });
}

/** Highlight table header columns that currently have a filter applied. */
function updateColumnFilterActiveState() {
    const table = document.getElementById('video-table');
    if (!table) return;

    const sizeActive = !!(activeFilters.size_val && String(activeFilters.size_val).trim());
    const bitActive = !!(activeFilters.bit_val && String(activeFilters.bit_val).trim());
    const searchActive = !!(activeFilters.search && String(activeFilters.search).trim());

    const mapping = [
        ['col-file', searchActive],
        ['col-hyb', !!(activeFilters.is_hybrid)],
        ['col-hybrid-src', !!(activeFilters.source_hybrid)],
        ['col-main', !!(activeFilters.category)],
        ['col-prof', !!(activeFilters.profile)],
        ['col-el', !!(activeFilters.el)],
        ['col-sec', !!(activeFilters.secondary_hdr)],
        ['col-res', !!(activeFilters.resolution)],
        ['col-size', sizeActive],
        ['col-bit', bitActive],
        ['col-vol', !!(activeFilters.volume)],
        ['col-cont', !!(activeFilters.container)],
        ['col-stat', !!(activeFilters.status)],
        ['col-nfo', !!(activeFilters.nfo_missing)],
        ['col-missing', !!(activeFilters.missing)],
        ['col-audio', !!(activeFilters.audio)],
        ['col-video-source', !!(activeFilters.video_source)],
        ['col-source-format', !!(activeFilters.source_format)],
        ['col-video-codec', !!(activeFilters.video_codec)],
        ['col-is-3d', !!(activeFilters.is_3d)],
        ['col-edition', !!(activeFilters.edition)],
        ['col-media-type', !!(activeFilters.media_type)],
    ];

    mapping.forEach(([colClass, on]) => {
        table.querySelectorAll(`thead th.${colClass}`).forEach(th => {
            th.classList.toggle('is-filtered', !!on);
        });
    });
}

function applyRibbonFilter(type, value) {
    ensureBinaryMultiselects();
    const currentMediaType = (() => {
        try { return getMultiselectValue('media-type-filter'); } catch (e) { return ''; }
    })();
    
    // Build mergePending so clearFilters restores these after clearing (avoids race / double-click)
    let mergePending = null;
    if (type === 'format') mergePending = { format: value };
    else if (type === 'el') mergePending = { profile: '7', el: value };
    else if (type === 'dovi_prof') mergePending = { profile: value === '10.1' ? '10.1,10' : value };
    else if (type === 'hybrid') mergePending = { hybrid: value };
    else if (type === 'source_hybrid') mergePending = { source_hybrid: value };
    else if (type === 'status') mergePending = { status: value };
    
    clearFilters(false, mergePending);

    setMultiselectValue('media-type-filter', currentMediaType || '', true);
    const normalizedType = normalizeMediaTypeFilter(currentMediaType);
    updateMediaTypeButtons(normalizedType);
    applyMediaTypeColumnVisibility(normalizedType);
    
    if (type === 'format' || type === 'el' || type === 'dovi_prof') {
        setMultiselectValue('status-filter', 'ok', true);
    }
    
    // Set filter UI and load after DOM settles (150ms for reliability; setFormatFilterValue skips its own reload)
    setTimeout(() => {
        if (type === 'format') {
            setFormatFilterValue(value, true);
            resetAndLoad();
        }
        else if (type === 'status') {
            setMultiselectValue('status-filter', value, true);
            resetAndLoad();
        }
        else if (type === 'hybrid') {
            setMultiselectValue('hybrid-filter', value, true);
            resetAndLoad();
        }
        else if (type === 'source_hybrid') {
            setMultiselectValue('source-hybrid-filter', value, true);
            resetAndLoad();
        }
        else if (type === 'el') {
            setMultiselectValue('profile-filter', '7', true);
            setMultiselectValue('el-filter', value, true);
            resetAndLoad();
        }
        else if (type === 'dovi_prof') {
            setMultiselectValue('profile-filter', value === '10.1' ? '10.1,10' : value, true);
            resetAndLoad();
        }
        syncActiveFiltersFromDom(false);
    }, 150);
}

/** Sync `activeFilters` from current DOM controls (and optional pending multiselect values). */
function syncActiveFiltersFromDom(clearPending = false) {
    ensureBinaryMultiselects();
    const readMulti = clearPending ? getMultiselectValueAndClearPending : getMultiselectValue;
    activeFilters.search = document.getElementById('search-bar') ? document.getElementById('search-bar').value : '';
    try {
        const formatValue = getFormatFilterValue();
        activeFilters.category = formatValue;
        if (clearPending && pendingFormatValue && formatValue === pendingFormatValue) {
            pendingFormatValue = null;
        }
    } catch (e) {
        activeFilters.category = '';
    }
    try { activeFilters.volume = readMulti('vol-filter'); } catch (e) { activeFilters.volume = ''; }
    try { activeFilters.profile = readMulti('profile-filter'); } catch (e) { activeFilters.profile = ''; }
    try { activeFilters.el = readMulti('el-filter'); } catch (e) { activeFilters.el = ''; }
    try { activeFilters.container = readMulti('container-filter'); } catch (e) { activeFilters.container = ''; }
    try { activeFilters.is_hybrid = readMulti('hybrid-filter'); } catch (e) { activeFilters.is_hybrid = ''; }
    try { activeFilters.source_hybrid = readMulti('source-hybrid-filter'); } catch (e) { activeFilters.source_hybrid = ''; }
    try { activeFilters.secondary_hdr = readMulti('secondary-filter'); } catch (e) { activeFilters.secondary_hdr = ''; }
    try { activeFilters.status = readMulti('status-filter'); } catch (e) { activeFilters.status = ''; }
    try { activeFilters.nfo_missing = readMulti('nfo-filter'); } catch (e) { activeFilters.nfo_missing = ''; }
    try { activeFilters.missing = readMulti('missing-filter'); } catch (e) { activeFilters.missing = ''; }
    try { activeFilters.resolution = readMulti('res-filter'); } catch (e) { activeFilters.resolution = ''; }
    try { activeFilters.video_source = readMulti('video-source-filter'); } catch (e) { activeFilters.video_source = ''; }
    try { activeFilters.source_format = readMulti('source-format-filter'); } catch (e) { activeFilters.source_format = ''; }
    try { activeFilters.video_codec = readMulti('video-codec-filter'); } catch (e) { activeFilters.video_codec = ''; }
    try { activeFilters.is_3d = readMulti('is-3d-filter'); } catch (e) { activeFilters.is_3d = ''; }
    try { activeFilters.edition = readMulti('edition-filter'); } catch (e) { activeFilters.edition = ''; }
    try { activeFilters.media_type = readMulti('media-type-filter'); } catch (e) { activeFilters.media_type = ''; }
    const sizeFilter = document.getElementById('size-filter-header') ? document.getElementById('size-filter-header').value : '';
    const sizeParsed = parseFilterValue(sizeFilter);
    activeFilters.size_op = sizeParsed.op;
    activeFilters.size_val = sizeParsed.value;
    const bitFilter = document.getElementById('bit-filter-header') ? document.getElementById('bit-filter-header').value : '';
    const bitParsed = parseFilterValue(bitFilter);
    activeFilters.bit_op = bitParsed.op;
    activeFilters.bit_val = bitParsed.value;
    try { activeFilters.audio = readMulti('audio-filter'); } catch (e) { activeFilters.audio = ''; }
    updateRibbonActiveState();
    updateColumnFilterActiveState();
    return activeFilters;
}

function clearFilters(doReload = true, mergePending = null) {
    console.log(`[DEBUG] clearFilters(doReload=${doReload})`);
    
    // Set flag to prevent loadData() from running during clearing
    isClearingFilters = true;
    
    // Clear all pending values FIRST before clearing filters (mergePending will restore at end)
    pendingFormatValue = null;
    const profileState = multiselectState['profile-filter'];
    if (profileState) profileState.pendingValue = null;
    const elState = multiselectState['el-filter'];
    if (elState) elState.pendingValue = null;
    const secondaryState = multiselectState['secondary-filter'];
    if (secondaryState) secondaryState.pendingValue = null;
    const volState = multiselectState['vol-filter'];
    if (volState) volState.pendingValue = null;
    const containerState = multiselectState['container-filter'];
    if (containerState) containerState.pendingValue = null;
    const resState = multiselectState['res-filter'];
    if (resState) resState.pendingValue = null;
    const audioState = multiselectState['audio-filter'];
    if (audioState) audioState.pendingValue = null;
    const videoSourceState = multiselectState['video-source-filter'];
    if (videoSourceState) videoSourceState.pendingValue = null;
    const sourceFormatState = multiselectState['source-format-filter'];
    if (sourceFormatState) sourceFormatState.pendingValue = null;
    const videoCodecState = multiselectState['video-codec-filter'];
    if (videoCodecState) videoCodecState.pendingValue = null;
    const editionState = multiselectState['edition-filter'];
    if (editionState) editionState.pendingValue = null;
    const mediaTypeState = multiselectState['media-type-filter'];
    if (mediaTypeState) mediaTypeState.pendingValue = null;
    ['hybrid-filter', 'source-hybrid-filter', 'status-filter', 'nfo-filter', 'missing-filter', 'is-3d-filter'].forEach(id => {
        const st = multiselectState[id];
        if (st) st.pendingValue = null;
    });
    
    const searchBar = document.getElementById('search-bar');
    if (searchBar) searchBar.value = '';
    
    // Clear format multiselect
    setFormatFilterValue('');
    
    // Clear all multiselect filters
    setMultiselectValue('vol-filter', '', true);
    setMultiselectValue('profile-filter', '', true);
    setMultiselectValue('el-filter', '', true);
    setMultiselectValue('container-filter', '', true);
    setMultiselectValue('secondary-filter', '', true);
    setMultiselectValue('res-filter', '', true);
    setMultiselectValue('audio-filter', '', true);
    setMultiselectValue('video-source-filter', '', true);
    setMultiselectValue('source-format-filter', '', true);
    setMultiselectValue('video-codec-filter', '', true);
    setMultiselectValue('edition-filter', '', true);
    setMultiselectValue('media-type-filter', '', true);
    ensureBinaryMultiselects();
    setMultiselectValue('hybrid-filter', '', true);
    setMultiselectValue('source-hybrid-filter', '', true);
    setMultiselectValue('status-filter', '', true);
    setMultiselectValue('nfo-filter', '', true);
    setMultiselectValue('missing-filter', '', true);
    setMultiselectValue('is-3d-filter', '', true);
    
    // Clear text filters
    const headerFilters = ['size-filter-header', 'bit-filter-header'];
    headerFilters.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            if (el.tagName === 'SELECT' || el.tagName === 'INPUT') {
                el.value = '';
            }
        }
    });
    
    // Restore pending values for ribbon/badge (avoids race where clearFilters wipes them before setTimeout)
    if (mergePending) {
        if (mergePending.format) pendingFormatValue = mergePending.format;
        if (mergePending.profile && profileState) profileState.pendingValue = mergePending.profile;
        if (mergePending.el && elState) elState.pendingValue = mergePending.el;
        const secondaryState = multiselectState['secondary-filter'];
        if (mergePending.secondary && secondaryState) secondaryState.pendingValue = mergePending.secondary;
        if (mergePending.hybrid && multiselectState['hybrid-filter']) multiselectState['hybrid-filter'].pendingValue = mergePending.hybrid;
        if (mergePending.source_hybrid && multiselectState['source-hybrid-filter']) multiselectState['source-hybrid-filter'].pendingValue = mergePending.source_hybrid;
        if (mergePending.status && multiselectState['status-filter']) multiselectState['status-filter'].pendingValue = mergePending.status;
    }

    document.querySelectorAll('.stat-card.is-active').forEach(card => card.classList.remove('is-active'));
    document.querySelectorAll('#video-table thead th.is-filtered').forEach(th => th.classList.remove('is-filtered'));
    
    // Force a synchronous DOM update by reading a property
    // This ensures all DOM changes are applied before we continue
    void document.body.offsetHeight;

    // Keep activeFilters in sync with cleared DOM (avoids stale snapshots for duplicates/bulk).
    syncActiveFiltersFromDom(false);
    
    // Clear the flag synchronously at the end (not in setTimeout)
    // The flag prevents loadData() during clearing, and we clear it immediately after all clearing operations
    isClearingFilters = false;
    
    if(doReload) {
        // Use setTimeout to ensure all DOM updates are complete before reloading
        setTimeout(() => {
            console.log(`[DEBUG] clearFilters: Calling resetAndLoadImmediate()`);
            clearTimeout(filterReloadTimer);
            filterReloadTimer = null;
            resetAndLoadImmediate();
        }, 50);
    }
}

function parseFilterValue(val) {
    if (!val) return { op: '', value: '' };
    val = val.trim();
    // Match operators: >=, <=, ==, >, <, =
    const match = val.match(/^(>=|<=|==|>|<|=)(.+)$/);
    if (match) {
        let op = match[1];
        if (op === '==') op = '=';
        return { op: op, value: match[2].trim() };
    }
    return { op: '=', value: val };
}

function setFilter(type, val) {
     ensureBinaryMultiselects();
     if (type === 'format') {
         setFormatFilterValue(val);
         resetAndLoad();
         return;
     }
     if (type === 'resolution') {
         setMultiselectValue('res-filter', val);
         resetAndLoad();
         return;
     }
     if (type === 'el') {
         setMultiselectValue('el-filter', val);
         resetAndLoad();
         return;
     }
     if (type === 'status') {
         setMultiselectValue('status-filter', val);
         resetAndLoad();
         return;
     }
     if (type === 'hybrid') {
         setMultiselectValue('hybrid-filter', val);
         resetAndLoad();
         return;
     }
     if (type === 'source_hybrid') {
         setMultiselectValue('source-hybrid-filter', val);
         resetAndLoad();
         return;
     }
     if (type === 'dovi_prof') {
         setMultiselectValue('profile-filter', val);
         resetAndLoad();
         return;
     }
}

function normalizeMediaTypeFilter(val) {
    if (!val) return '';
    const parts = val.split(',').map(v => v.trim()).filter(Boolean);
    if (parts.length !== 1) return '';
    if (parts[0] === 'movie' || parts[0] === 'tv') return parts[0];
    return '';
}

function getMediaTypeKeyFromValue(val) {
    const normalized = normalizeMediaTypeFilter(val);
    return normalized || 'all';
}

function captureColumnPrefsForKey(key) {
    if (!key) return;
    const visibleCols = getVisibleCols();
    const order = getColumnOrder();
    const widths = getColumnWidths();
    const mergedWidths = { ...(columnWidths || {}), ...widths };
    visibleColsByMediaKey[key] = visibleCols;
    columnOrderByMediaKey[key] = order;
    columnWidths = mergedWidths;
    try { localStorage.setItem('column_widths', JSON.stringify(mergedWidths)); } catch (e) {}
    if (!settingsCache) settingsCache = {};
    settingsCache[`visible_cols_${key}`] = visibleCols;
    settingsCache[`column_order_${key}`] = JSON.stringify(order);
    settingsCache.column_widths = JSON.stringify(mergedWidths);
    const payload = {
        [`visible_cols_${key}`]: visibleCols,
        [`column_order_${key}`]: JSON.stringify(order),
        column_widths: JSON.stringify(mergedWidths)
    };
    fetch('/api/settings', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    }).catch(() => {});
}

/** Apply stored widths to cells when rebuilding table. Does NOT recalculate. */
function applyStoredColumnWidths() {
    if (!columnWidths) {
        try { const r = localStorage.getItem('column_widths'); if (r) columnWidths = JSON.parse(r); } catch (e) {}
    }
    if (columnWidths) {
        applyColumnWidths(columnWidths);
        hasSavedWidths = true;
    }
}

function getMinColWidth(colClass) {
    if (!colClass) return 30;
    if (colClass === 'col-file') return 30;
    if (colClass === 'col-season' || colClass === 'col-episode' || colClass.includes('year') || colClass.includes('3d')) return 28;
    return 30;
}

/** Single-table layout inside ONE scrollport (#table-h-scroll) with sticky freeze-pane header. */
function getVideoTable() { return document.getElementById('video-table'); }
function getHeaderTable() { return getVideoTable(); }
function getDataTable() { return getVideoTable(); }
function getTableScrollHost() { return document.getElementById('table-h-scroll'); }
/** Always scope to #video-table — never document.querySelector('thead'), which matches the duplicates modal table first. */
function getVideoTableHeaderRow() {
    const table = getVideoTable();
    return table ? table.querySelector('thead tr') : null;
}
function getVideoTableHeaderCells() {
    const table = getVideoTable();
    return table ? table.querySelectorAll('thead th') : [];
}
function getVideoTableThead() {
    const table = getVideoTable();
    return table ? table.querySelector('thead') : null;
}
function getTableColgroups() {
    const cg = document.getElementById('table-colgroup');
    return cg ? [cg] : [];
}

function resetColumnWidths() {
    columnWidths = null;
    try { localStorage.removeItem('column_widths'); } catch (e) {}
    if (settingsCache) delete settingsCache.column_widths;
    hasSavedWidths = false;
    manualResizeBlocked = false;
    const table = getHeaderTable();
    if (!table) return;
    const widths = {};
    const colWidthsForSync = [];
    table.querySelectorAll('thead th').forEach(th => {
        const colClass = Array.from(th.classList).find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
        if (th.classList.contains('col-chk') || th.classList.contains('col-del')) {
            colWidthsForSync.push(40);
        } else if (colClass) {
            const w = getMinColWidth(colClass);
            widths[colClass] = w + 'px';
            colWidthsForSync.push(w);
        }
    });
    applyColumnWidths(widths);
    syncColgroupFromWidths(colWidthsForSync);
    requestAnimationFrame(() => {
        requestAnimationFrame(() => {
            applyColumnWidths(widths);
            syncTableColgroup();
            updateStickyHeader();
            const w = getColumnWidths();
            if (Object.keys(w).length) {
                columnWidths = w;
                try { localStorage.setItem('column_widths', JSON.stringify(w)); } catch (e) {}
                fetch('/api/settings', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ column_widths: JSON.stringify(w) }) }).catch(() => {});
            }
        });
    });
    showToast('Column widths reset to minimum');
}
/** col-chk / col-del are always 40px — detect by class, not column index (column reorder breaks index 0 / last). */
function clampChkDelWidthsFromHeaderRow(headerRow, colWidths) {
    const FIXED = 40;
    if (!headerRow || !colWidths || !colWidths.length) return colWidths || [];
    const out = colWidths.slice();
    const n = Math.min(headerRow.children.length, out.length);
    for (let i = 0; i < n; i++) {
        const th = headerRow.children[i];
        if (th && (th.classList.contains('col-chk') || th.classList.contains('col-del'))) out[i] = FIXED;
    }
    return out;
}
/** Pinned header must match body: read actual px from <colgroup> (avoids stale _stickyHeaderColWidths / rebuild skew). */
function readColPixelWidthsFromColgroup() {
    const cg = document.getElementById('table-colgroup');
    if (!cg || !cg.children.length) return null;
    const out = [];
    for (let i = 0; i < cg.children.length; i++) {
        const raw = cg.children[i].style.width || '';
        const w = parseFloat(String(raw).replace(/px/gi, '')) || 0;
        out.push(w);
    }
    return out;
}
/** After a resize, persist every visible column's width from <colgroup> so storage matches the grid (avoids pin/scroll jumps). */
function snapshotAllColumnWidthsFromColgroup() {
    const headerRow = getVideoTableHeaderRow();
    const tableWrap = document.querySelector('.table-wrap');
    const cols = readColPixelWidthsFromColgroup();
    if (!headerRow || !tableWrap || !cols || cols.length !== headerRow.children.length) return;
    ensureColumnWidthsObject();
    for (let i = 0; i < cols.length; i++) {
        const th = headerRow.children[i];
        if (!th) continue;
        const colClass = Array.from(th.classList).find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
        if (!colClass || isColumnLayoutCollapsed(colClass, tableWrap)) continue;
        const w = cols[i];
        if (w > 0) columnWidths[colClass] = Math.round(w) + 'px';
    }
}
/** Remove inline width constraints so <colgroup> is the only column-width source (avoids th/td fighting collapse). */
function clearVideoTableCellWidthStyles() {
    const table = getHeaderTable();
    if (!table) return;
    table.querySelectorAll('thead th, tbody td').forEach(cell => {
        cell.style.removeProperty('width');
        cell.style.removeProperty('min-width');
        cell.style.removeProperty('max-width');
    });
}
/**
 * After <colgroup> sync: clear all th/td inline widths (resize, applyColumnWidths, etc. leave !important junk).
 * Unpinned: layout from colgroup + table-layout:fixed only. Pinned thead: reinforce th widths (fixed breaks grid).
 */
function afterColgroupSyncCellStyles(headerRow, colWidthsArr) {
    clearVideoTableCellWidthStyles();
}
function applyCollapsedColumnCellStyles() {
    const table = document.getElementById('video-table');
    const tableWrap = document.querySelector('.table-wrap');
    const headerRow = table ? table.querySelector('thead tr') : null;
    if (!table || !tableWrap || !headerRow) return;
    const applyCollapsed = (cell, collapsed) => {
        if (!cell) return;
        if (collapsed) {
            // Keep cell in the table grid (display:table-cell) but collapse geometry to 0px.
            cell.style.setProperty('display', 'table-cell', 'important');
            cell.style.setProperty('visibility', 'hidden', 'important');
            cell.style.setProperty('pointer-events', 'none', 'important');
            cell.style.setProperty('padding-left', '0px', 'important');
            cell.style.setProperty('padding-right', '0px', 'important');
            cell.style.setProperty('border-left-width', '0px', 'important');
            cell.style.setProperty('border-right-width', '0px', 'important');
            cell.style.setProperty('width', '0px', 'important');
            cell.style.setProperty('min-width', '0px', 'important');
            cell.style.setProperty('max-width', '0px', 'important');
            return;
        }
        cell.style.removeProperty('display');
        cell.style.removeProperty('visibility');
        cell.style.removeProperty('pointer-events');
        cell.style.removeProperty('padding-left');
        cell.style.removeProperty('padding-right');
        cell.style.removeProperty('border-left-width');
        cell.style.removeProperty('border-right-width');
    };
    Array.from(headerRow.children).forEach(th => {
        if (!th || th.classList.contains('col-chk') || th.classList.contains('col-del')) return;
        const colClass = Array.from(th.classList).find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
        if (!colClass) return;
        const collapsed = isColumnLayoutCollapsed(colClass, tableWrap);
        applyCollapsed(th, collapsed);
        table.querySelectorAll(`tbody td.${colClass}`).forEach(td => applyCollapsed(td, collapsed));
    });
}
function enforceCellWidthsFromColgroupByIndex() {
    const table = document.getElementById('video-table');
    const headerRow = table ? table.querySelector('thead tr') : null;
    const widths = readColPixelWidthsFromColgroup();
    if (!table || !headerRow || !widths || !widths.length) return;
    const n = Math.min(headerRow.children.length, widths.length);
    const applyCell = (cell, w) => {
        if (!cell) return;
        if (!(w > 0)) {
            cell.style.removeProperty('width');
            cell.style.removeProperty('min-width');
            cell.style.removeProperty('max-width');
            return;
        }
        const px = `${Math.round(w)}px`;
        cell.style.setProperty('width', px, 'important');
        cell.style.setProperty('min-width', px, 'important');
        cell.style.setProperty('max-width', px, 'important');
    };
    // Build class -> width map from current header index sequence.
    const classWidthMap = {};
    for (let i = 0; i < n; i++) {
        const th = headerRow.children[i];
        const colClass = th
            ? Array.from(th.classList).find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del')
            : '';
        if (colClass) classWidthMap[colClass] = widths[i];
        applyCell(th, widths[i]);
    }
    // Apply by class across the full table (more robust than strict row child index assumptions).
    Object.keys(classWidthMap).forEach(colClass => {
        const w = classWidthMap[colClass];
        table.querySelectorAll(`tbody td.${colClass}`).forEach(td => applyCell(td, w));
    });
    // Keep fixed edge columns pinned at 40px.
    table.querySelectorAll('th.col-chk, td.col-chk, th.col-del, td.col-del').forEach(cell => applyCell(cell, 40));
}
function syncColgroupFromWidths(colWidths) {
    if (!colWidths || !colWidths.length) return;
    const colgroups = getTableColgroups();
    if (!colgroups.length) return;
    const FIXED_WIDTH = 40; // col-chk and col-del must never expand
    const headerRow = getVideoTableHeaderRow();
    colgroups.forEach(colgroup => {
        while (colgroup.children.length < colWidths.length) colgroup.appendChild(document.createElement('col'));
        while (colgroup.children.length > colWidths.length) colgroup.lastChild.remove();
        for (let i = 0; i < colWidths.length; i++) {
            const c = colgroup.children[i];
            const th = headerRow && headerRow.children[i];
            const isFixedCol = th && (th.classList.contains('col-chk') || th.classList.contains('col-del'));
            const w = isFixedCol ? FIXED_WIDTH : (colWidths[i] || 0);
            c.style.width = w + 'px';
            if (w <= 0) {
                c.style.minWidth = '0px';
                c.style.maxWidth = '0px';
            } else {
                c.style.minWidth = w + 'px';
                c.style.maxWidth = w + 'px';
            }
        }
    });
    _stickyHeaderColWidths = clampChkDelWidthsFromHeaderRow(headerRow, colWidths);
    // Lock table width to exact colgroup sum so browser cannot redistribute spare space between columns.
    const table = getHeaderTable();
    if (table && _stickyHeaderColWidths && _stickyHeaderColWidths.length) {
        const total = _stickyHeaderColWidths.reduce((a, b) => a + Math.max(0, Number(b) || 0), 0);
        table.style.width = total + 'px';
        table.style.minWidth = total + 'px';
        table.style.maxWidth = total + 'px';
    }
    afterColgroupSyncCellStyles(headerRow, _stickyHeaderColWidths);
    applyCollapsedColumnCellStyles();
    enforceCellWidthsFromColgroupByIndex();
    clampTableScroll();
}
function applyLiveResizeByHeaderIndex(colIndex, newWidth) {
    if (!(colIndex >= 0) || !(newWidth > 0)) return;
    const colgroups = getTableColgroups();
    if (!colgroups.length) return;
    const px = Math.max(8, Math.round(newWidth));
    colgroups.forEach(cg => {
        const c = cg.children[colIndex];
        if (!c) return;
        c.style.width = px + 'px';
        c.style.minWidth = px + 'px';
        c.style.maxWidth = px + 'px';
    });
    if (_stickyHeaderColWidths && _stickyHeaderColWidths.length > colIndex) {
        _stickyHeaderColWidths[colIndex] = px;
    }
    const table = getHeaderTable();
    if (table) {
        const total = getTableContentWidth();
        if (total > 0) {
            table.style.width = total + 'px';
            table.style.minWidth = total + 'px';
            table.style.maxWidth = total + 'px';
        }
    }
    enforceCellWidthsFromColgroupByIndex();
    clampTableScroll();
}

function ensureColumnWidthsObject() {
    if (!columnWidths) { try { const r = localStorage.getItem('column_widths'); if (r) columnWidths = JSON.parse(r); } catch (e) {} }
    if (!columnWidths || typeof columnWidths !== 'object') columnWidths = {};
}
/** Pixel width for a data column: saved column_widths only, else getMinColWidth. Never read layout from the DOM. */
function getGlobalColumnWidthPx(colClass) {
    ensureColumnWidthsObject();
    if (!colClass) return 30;
    const raw = columnWidths[colClass];
    if (raw != null && raw !== '') {
        const w = parseFloat(String(raw).replace(/px/gi, ''));
        if (Number.isFinite(w) && w > 0) return Math.round(w);
    }
    return getMinColWidth(colClass);
}
/**
 * Columns removed from layout (display:none) must use <col> width 0 or thead/tbody misalign.
 * Saved column_widths are unchanged; only runtime colgroup reflects collapse.
 */
function isColumnLayoutCollapsed(colClass, tableWrap) {
    if (!colClass || !tableWrap) return false;
    if (tableWrap.classList.contains('hide-' + colClass)) return true;
    if (tableWrap.classList.contains('media-filter-movie')) {
        return colClass === 'col-show-title' || colClass === 'col-season' || colClass === 'col-episode' || colClass === 'col-episode-title';
    }
    if (tableWrap.classList.contains('media-filter-tv')) {
        return colClass === 'col-movie-title';
    }
    return false;
}
/**
 * Rebuild <colgroup> from global column_widths only. Pin, scroll, filter must not change stored widths.
 * overrideClass + overridePx: only while user drags a resize handle.
 */
function rebuildColgroupFromStoredColumnWidths(overrideClass, overridePx, overrideIndex) {
    const headerRow = getVideoTableHeaderRow();
    const tableWrap = document.querySelector('.table-wrap');
    if (!getHeaderTable() || !headerRow) return;
    ensureColumnWidthsObject();
    const colWidths = [];
    for (let i = 0; i < headerRow.children.length; i++) {
        const th = headerRow.children[i];
        if (th && (th.classList.contains('col-chk') || th.classList.contains('col-del'))) {
            colWidths.push(40);
        } else if (th) {
            const colClass = Array.from(th.classList).find(c => c.startsWith('col-') && c !== 'col-chk' && c !== 'col-del');
            let w = 0;
            if (typeof overrideIndex === 'number' && overrideIndex === i && typeof overridePx === 'number' && overridePx > 0) {
                w = Math.round(overridePx);
            } else if (colClass && overrideClass && colClass === overrideClass && typeof overridePx === 'number' && overridePx > 0) {
                w = Math.round(overridePx);
            } else if (colClass) {
                w = getGlobalColumnWidthPx(colClass);
            }
            if (colClass && isColumnLayoutCollapsed(colClass, tableWrap)) w = 0;
            colWidths.push(w);
        } else {
            colWidths.push(0);
        }
    }
    if (colWidths.length > 0) syncColgroupFromWidths(colWidths);
}

/** After data load / filter — same as full rebuild from storage (no DOM measurement). */
function syncColgroupFromStoredWidths() {
    rebuildColgroupFromStoredColumnWidths(null, null, null);
}

function applyMediaTypeColumnVisibility(type) {
    const tableWrap = document.querySelector('.table-wrap');
    if (!tableWrap) return;
    tableWrap.classList.remove('media-filter-movie', 'media-filter-tv', 'media-filter-all');
    if (type === 'movie') {
        tableWrap.classList.add('media-filter-movie');
    } else if (type === 'tv') {
        tableWrap.classList.add('media-filter-tv');
    } else {
        tableWrap.classList.add('media-filter-all');
    }
    applyCollapsedColumnCellStyles();
}

function updateMediaTypeButtons(type) {
    const group = document.getElementById('media-type-btn-group');
    if (!group) return;
    const buttons = group.querySelectorAll('.media-type-btn');
    buttons.forEach(btn => {
        const btnType = btn.getAttribute('data-type') || '';
        const active = (type === '' && btnType === 'all') || (btnType === type);
        btn.classList.toggle('inactive', !active);
    });
}

function setMediaTypeButton(type) {
    const value = type === 'movie' ? 'movie' : (type === 'tv' ? 'tv' : '');
    activeFilters.media_type = value;
    setMultiselectValue('media-type-filter', value, true);
    updateMultiselectFilter('media-type-filter', true);
    const normalized = normalizeMediaTypeFilter(value);
    updateMediaTypeButtons(normalized);
    applyMediaTypeColumnVisibility(normalized);
    requestAnimationFrame(() => { syncTableColgroup(); updateStickyHeader(); });
    resetAndLoad();  // Filter data by type, not just hide columns
}

function filterBadge(cat, prof, elType, sec) {
    console.log(`[DEBUG] filterBadge(${cat}, ${prof}, ${elType}, ${sec})`);
    
    const currentMediaType = (() => {
        try { return getMultiselectValue('media-type-filter'); } catch (e) { return ''; }
    })();
    
    const mergePending = {};
    if (cat && cat !== 'sec') mergePending.format = cat;
    if (prof) mergePending.profile = prof;
    if (elType) mergePending.el = elType;
    if (sec) mergePending.secondary = sec;
    
    if (prof || elType) {
        ensureBinaryMultiselects();
        setMultiselectValue('status-filter', 'ok', true);
    }
    
    clearFilters(false, Object.keys(mergePending).length ? mergePending : null);
    
    setMultiselectValue('media-type-filter', currentMediaType || '', true);
    const normalizedType = normalizeMediaTypeFilter(currentMediaType);
    updateMediaTypeButtons(normalizedType);
    applyMediaTypeColumnVisibility(normalizedType);
    requestAnimationFrame(() => { syncTableColgroup(); updateStickyHeader(); });
    
    setTimeout(() => {
        if (cat && cat !== 'sec') setFormatFilterValue(cat, true);
        if (prof) setMultiselectValue('profile-filter', prof, true);
        if (elType) setMultiselectValue('el-filter', elType, true);
        if (sec) setMultiselectValue('secondary-filter', sec, true);
        resetAndLoad();
    }, 150);
}

function sortBy(col, event) {
    if (columnResizeActive) {
        return;
    }
    // Prevent event bubbling if called from filter row elements
    if (event) {
        event.stopPropagation();
    }
    if (sortCol === col) { sortOrder = (sortOrder === 'asc') ? 'desc' : 'asc'; } 
    else { sortCol = col; sortOrder = 'desc'; }
    sortInitialized = true;
    document.querySelectorAll('.sort-icon').forEach(e => e.innerText = '');
    const arrow = sortOrder === 'asc' ? '▲' : '▼';
    const icon = document.getElementById('sort-' + col);
    if(icon) icon.innerText = arrow;
    loadData();
}

function updatePalette() { if(lastStats) updateCharts(lastStats, lastStatsFiltered, lastFilterOptions); }

function animateSuccess(btnId, origText, tempText) {
    const btn = document.getElementById(btnId);
    btn.innerText = tempText || "Saved!";
    btn.classList.add('action-success');
    setTimeout(() => {
        btn.classList.remove('action-success');
        btn.innerText = origText;
    }, 1500);
}

function animateFailure(btnId, origText) {
    const btn = document.getElementById(btnId);
    btn.innerText = "Error!";
    btn.classList.add('action-fail');
    setTimeout(() => {
        btn.classList.remove('action-fail');
        btn.innerText = origText;
    }, 1500);
}
