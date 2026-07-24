// --- CHART LOGIC ---
function updateCharts(statsTotal, statsFiltered, filterOpts) {
    const stats = chartMode === 'filtered' && statsFiltered ? statsFiltered : statsTotal;
    if (!stats || !stats.vol_labels) return;

    const barLabels = (barChartMode === 'paths' && stats.path_labels && stats.path_labels.length) ? stats.path_labels : stats.vol_labels;
    const barData = (barChartMode === 'paths' && stats.path_data && stats.path_data.length) ? stats.path_data : stats.vol_data;

    const paletteKey = document.getElementById('palette-select').value;
    let currentPalette = PALETTES[paletteKey] || PALETTES.all;
    // If we need more colors than available in the palette, generate them dynamically
    if (barLabels.length > currentPalette.length) {
        const generatedColors = generateColors(barLabels.length, paletteKey === 'all' ? 'neon' : paletteKey);
        currentPalette = [...currentPalette, ...generatedColors.slice(currentPalette.length)];
    }
    const volColors = barLabels.map((_, i) => currentPalette[i]);
    const resColors = stats.res_labels.map(label => RES_COLORS[label] || '#555555');

    if (formatChart) formatChart.destroy();
    formatChart = new Chart(document.getElementById('formatChart'), {
        type: 'doughnut',
        data: {
            labels: ['DV P7 FEL', 'DV P7 MEL', 'DV P8.1', 'DV P8.4', 'DV P5', 'DV P10.1', 'DV P10.4', 'DV P10', 'HDR10+', 'HDR10', 'HLG', 'SDR'],
            datasets: [{ 
                data: [stats.dovi_p7_fel, stats.dovi_p7_mel, (stats.dovi_p81 + stats.dovi_p8), stats.dovi_p84, stats.dovi_p5, stats.dovi_p101, stats.dovi_p104, stats.dovi_p10, stats.hdr10plus, stats.hdr10, stats.hlg, stats.sdr], 
                backgroundColor: ['#a55eea', '#5f27cd', '#e74c3c', '#fd79a8', '#27ae60', '#A1BC98', '#D2DCB6', '#778873', '#f1c40f', '#e67e22', '#3498db', '#555555'], borderWidth: 0 
            }]
        },
        options: { 
            responsive: true, maintainAspectRatio: false,
            onClick: (evt, elements) => {
                if (elements.length > 0) {
                    const label = formatChart.data.labels[elements[0].index];
                    if (label.includes('DV')) {
                        if (label.includes('FEL')) setFilter('el', 'FEL');
                        else if (label.includes('MEL')) setFilter('el', 'MEL');
                        else if (label.includes('P5')) { setFilter('format', 'dovi'); const el = document.getElementById('profile-filter-header'); if (el) el.value = '5'; resetAndLoad(); }
                        else if (label.includes('P8.1')) { setFilter('format', 'dovi'); const el = document.getElementById('profile-filter-header'); if (el) el.value = '8.1'; resetAndLoad(); }
                        else if (label.includes('P8.4')) { setFilter('format', 'dovi'); const el = document.getElementById('profile-filter-header'); if (el) el.value = '8.4'; resetAndLoad(); }
                        else if (label.includes('P10.1')) { setFilter('format', 'dovi'); const el = document.getElementById('profile-filter-header'); if (el) el.value = '10.1'; resetAndLoad(); }
                        else if (label.includes('P10.4')) { setFilter('format', 'dovi'); const el = document.getElementById('profile-filter-header'); if (el) el.value = '10.4'; resetAndLoad(); }
                        else if (label.includes('P10')) { setFilter('format', 'dovi'); const el = document.getElementById('profile-filter-header'); if (el) el.value = '10'; resetAndLoad(); }
                    } else if (label === 'HDR10+') setFilter('format', 'hdr10plus');
                    else if (label === 'HDR10') setFilter('format', 'hdr10');
                    else if (label === 'HLG') setFilter('format', 'hlg');
                    else if (label === 'SDR') setFilter('format', 'sdr_only');
                }
            },
            plugins: { legend: { position: 'right', labels: { color: '#fff', font: { size: 10 }, padding: 10 } }, title: { display: false } } 
        }
    });

    if (secChart) secChart.destroy();
    let secLabels = [], secData = [], secColors = [];
    const secColorMap = { 'HDR10+': '#f1c40f', 'HDR10': '#e67e22', 'HLG': '#3498db', 'none': '#333' };
    if (stats.secondary_hdrs) {
        Object.entries(stats.secondary_hdrs).forEach(([key, val]) => {
            if (key !== 'none') { secLabels.push(key.toUpperCase()); secData.push(val); secColors.push(secColorMap[key] || '#999'); }
        });
    }
    secChart = new Chart(document.getElementById('secChart'), {
        type: 'doughnut',
        data: { labels: secLabels, datasets: [{ data: secData, backgroundColor: secColors, borderWidth: 0 }] },
        options: { 
            responsive: true, maintainAspectRatio: false,
            onClick: (evt, elements) => { if (elements.length > 0) { const label = secChart.data.labels[elements[0].index]; let val = label; if(label === 'HDR10+') val = 'HDR10+'; const el = document.getElementById('secondary-filter-header'); if (el) el.value = val; resetAndLoad(); } },
            plugins: { legend: { position: 'right', labels: { color: '#fff', font: { size: 10 }, padding: 10 } }, title: { display: false } } 
        }
    });
    
    if (resChart) resChart.destroy();
    resChart = new Chart(document.getElementById('resChart'), {
        type: 'doughnut', 
        data: { labels: stats.res_labels, datasets: [{ data: stats.res_data, backgroundColor: resColors, borderWidth: 0 }] },
        options: { 
            responsive: true, maintainAspectRatio: false,
            onClick: (evt, elements) => { if (elements.length > 0) { setFilter('resolution', stats.res_labels[elements[0].index]); } },
            plugins: { legend: { position: 'right', labels: { color: '#fff', font: { size: 10 }, padding: 10 } }, title: { display: false } } 
        }
    });

    if (volChart) volChart.destroy();
    const barTitle = document.getElementById('vol-chart-title');
    if (barTitle) barTitle.innerText = barChartMode === 'paths' ? 'PATHS' : 'VOLUMES';
    volChart = new Chart(document.getElementById('volChart'), {
        type: 'bar',
        data: { labels: barLabels, datasets: [{ label: 'Files', data: barData, backgroundColor: volColors }] },
        options: { 
            responsive: true, maintainAspectRatio: false, layout: { padding: { top: 40 } }, 
            onClick: (evt, elements) => { 
                if (elements.length > 0 && barChartMode === 'volumes') { 
                    const el = document.getElementById('vol-filter-header'); 
                    if (el) el.value = stats.vol_labels[elements[0].index]; 
                    resetAndLoad(); 
                } 
            },
            scales: { y: { beginAtZero: true, grid: { color: '#222' } }, x: { ticks: { color: '#888', font: { size: 9 } } } }, 
            plugins: { legend: { display: false }, title: { display: false } } 
        }
    });
}

function updateFilterDropdowns(opts) {
    if (!opts) return;
    
    // Update format multiselect
    lastFilterOptions = opts;
    lastFilterBlanks = opts.blank_counts || {};
    updateFormatMultiselect(opts.categories);
    
    // Update all other multiselects
    updateMultiselectOptions('profile-filter', opts.profiles, {}, lastFilterBlanks.profile);
    updateMultiselectOptions('el-filter', opts.el_types, {}, lastFilterBlanks.el);
    updateMultiselectOptions('vol-filter', opts.volumes, {}, lastFilterBlanks.volume);
    updateMultiselectOptions('container-filter', opts.containers, {}, lastFilterBlanks.container);
    updateMultiselectOptions('secondary-filter', opts.secondary_hdrs, {}, lastFilterBlanks.secondary_hdr);
    updateMultiselectOptions('res-filter', opts.resolutions, {}, lastFilterBlanks.resolution);
    updateMultiselectOptions('audio-filter', opts.audio_codecs, {}, lastFilterBlanks.audio);
    updateMultiselectOptions('video-source-filter', opts.video_sources, {}, lastFilterBlanks.video_source);
    updateMultiselectOptions('source-format-filter', opts.source_formats, {}, lastFilterBlanks.source_format);
    updateMultiselectOptions('video-codec-filter', opts.video_codecs, {}, lastFilterBlanks.video_codec);
    updateMultiselectOptions('edition-filter', opts.editions, {}, lastFilterBlanks.edition);
    updateMultiselectOptions('media-type-filter', opts.media_types, { movie: 'MOVIE', tv: 'TV' }, lastFilterBlanks.media_type);
    
    // Update single-select dropdowns (hybrid, status)
    const update = (id, options) => {
        if (!options) return; 
        const el = document.getElementById(id); if (!el) return; 
        const current = el.value; 
        let items = Array.isArray(options) ? options : Object.keys(options);
        if (current && !Array.isArray(options) && options[current] === undefined) { options[current] = 0; }
        if (current && !Array.isArray(options) && !items.includes(current)) { items.push(current); }
        let html = `<option value="">All</option>`;
        html += items.map(k => {
            let val = k.toString();
            let display = val.toUpperCase();
            if(val === 'sdr_only') display = 'SDR';
            if(val === 'hdr10plus') display = 'HDR10+';
            if(val === 'none') display = 'None'; 
            let count = (opts && !Array.isArray(options) && options[val] !== undefined) ? options[val] : 0;
            if (!Array.isArray(options)) { 
                if (count === 0 && val !== current) return ''; 
                display += ` (${count})`; 
            }
            return `<option value="${val}" ${val == current ? 'selected' : ''}>${display}</option>`;
        }).join('');
        if (current && current !== '' && !Array.isArray(options)) {
            const hasCurrentValue = html.includes(`value="${current}"`);
            if (!hasCurrentValue) {
                let display = current.toUpperCase();
                if(current === 'sdr_only') display = 'SDR';
                if(current === 'hdr10plus') display = 'HDR10+';
                if(current === 'none') display = 'None';
                html += `<option value="${current}" selected>${display} (0)</option>`;
            }
        }
        el.innerHTML = html; 
        if (current) el.value = current; 
    }; 

    const hybEl = document.getElementById('hybrid-filter-header');
    if (hybEl) {
        const hybCur = hybEl.value; const hybCounts = opts.special_hybrid || { '1': 0, '0': 0 };
        let hybHtml = `<option value="">All</option>`;
        // Always include current value even if count is 0
        if (hybCur === "1" || hybCounts['1'] > 0) hybHtml += `<option value="1" ${hybCur === "1" ? "selected" : ""}>Yes (${hybCounts['1'] || 0})</option>`;
        if (hybCur === "0" || hybCounts['0'] > 0) hybHtml += `<option value="0" ${hybCur === "0" ? "selected" : ""}>No (${hybCounts['0'] || 0})</option>`;
        hybEl.innerHTML = hybHtml;
        if (hybCur) hybEl.value = hybCur;
    }

    const srcHybEl = document.getElementById('source-hybrid-filter-header');
    if (srcHybEl) {
        const srcCur = srcHybEl.value; const srcCounts = opts.special_source_hybrid || { '1': 0, '0': 0 };
        let srcHtml = `<option value="">All</option>`;
        if (srcCur === "1" || srcCounts['1'] > 0) srcHtml += `<option value="1" ${srcCur === "1" ? "selected" : ""}>Yes (${srcCounts['1'] || 0})</option>`;
        if (srcCur === "0" || srcCounts['0'] > 0) srcHtml += `<option value="0" ${srcCur === "0" ? "selected" : ""}>No (${srcCounts['0'] || 0})</option>`;
        srcHybEl.innerHTML = srcHtml;
        if (srcCur) srcHybEl.value = srcCur;
    }
    
    const d3dEl = document.getElementById('is-3d-filter-header');
    if (d3dEl) {
        const d3dCur = d3dEl.value; const d3dCounts = opts.special_is_3d || { '1': 0, '0': 0 };
        let d3dHtml = `<option value="">All</option>`;
        // Always include current value even if count is 0
        if (d3dCur === "1" || d3dCounts['1'] > 0) d3dHtml += `<option value="1" ${d3dCur === "1" ? "selected" : ""}>3D (${d3dCounts['1'] || 0})</option>`;
        if (d3dCur === "0" || d3dCounts['0'] > 0) d3dHtml += `<option value="0" ${d3dCur === "0" ? "selected" : ""}>2D (${d3dCounts['0'] || 0})</option>`;
        d3dEl.innerHTML = d3dHtml;
        if (d3dCur) d3dEl.value = d3dCur;
    }

    const statEl = document.getElementById('status-filter-header');
    if (statEl) {
        const statCur = statEl.value; const statCounts = opts.special_status || { 'ok': 0, 'failed': 0 };
        let statHtml = `<option value="">All</option>`;
        // Always include current value even if count is 0
        if (statCur === "ok" || statCounts['ok'] > 0) statHtml += `<option value="ok" ${statCur === "ok" ? "selected" : ""}>OK (${statCounts['ok'] || 0})</option>`;
        if (statCur === "failed" || statCounts['failed'] > 0) statHtml += `<option value="failed" ${statCur === "failed" ? "selected" : ""}>Failed (${statCounts['failed'] || 0})</option>`;
        statEl.innerHTML = statHtml;
        if (statCur) statEl.value = statCur;
    }

    const missingEl = document.getElementById('missing-filter-header');
    if (missingEl) {
        const missingCur = missingEl.value; const missingCounts = opts.special_missing || { '1': 0, '0': 0 };
        let missingHtml = `<option value="">All</option>`;
        if (missingCur === "1" || missingCounts['1'] > 0) missingHtml += `<option value="1" ${missingCur === "1" ? "selected" : ""}>Yes (${missingCounts['1'] || 0})</option>`;
        if (missingCur === "0" || missingCounts['0'] > 0) missingHtml += `<option value="0" ${missingCur === "0" ? "selected" : ""}>No (${missingCounts['0'] || 0})</option>`;
        missingEl.innerHTML = missingHtml;
        if (missingCur) missingEl.value = missingCur;
    }
}
