// --- CHART LOGIC ---
let storageTrendChart = null;

async function openStorageTrends() {
    const modal = document.getElementById('storage-trend-modal');
    const canvas = document.getElementById('storageTrendChart');
    const empty = document.getElementById('storage-trend-empty');
    if (!modal || !canvas) return;
    modal.style.display = 'block';
    try {
        const response = await fetch('/api/storage_trends');
        if (!response.ok) throw new Error(`Storage trends request failed (${response.status})`);
        const data = await response.json();
        if (storageTrendChart) storageTrendChart.destroy();
        const snapshots = data.snapshots || [];
        if (empty) {
            empty.textContent = snapshots.length
                ? ''
                : 'No completed scan snapshots yet. Finish a scan to begin this history.';
        }
        storageTrendChart = new Chart(canvas, {
            type: 'line',
            data: {
                labels: snapshots.map(item => item.captured_at),
                datasets: [
                    {label: 'Library size', data: snapshots.map(item => item.total_bytes / 1073741824), borderColor: '#3498db', backgroundColor: 'rgba(52,152,219,.15)', fill: true, tension: .25, yAxisID: 'yTotal'},
                    {label: 'Duplicate savings', data: snapshots.map(item => item.duplicate_savings_bytes / 1073741824), borderColor: '#2ecc71', backgroundColor: 'transparent', tension: .25, yAxisID: 'ySavings'}
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                scales: {
                    yTotal: {
                        type: 'linear', position: 'left', beginAtZero: true,
                        title: {display: true, text: 'Total size (GB)', color: '#3498db'},
                        grid: {color: '#222'}, ticks: {color: '#3498db'}
                    },
                    ySavings: {
                        type: 'linear', position: 'right', beginAtZero: true,
                        title: {display: true, text: 'Duplicate savings (GB)', color: '#2ecc71'},
                        grid: {drawOnChartArea: false}, ticks: {color: '#2ecc71'}
                    },
                    x: {ticks: {color: '#aaa'}}
                },
                plugins: {legend: {labels: {color: '#fff'}}}
            }
        });
    } catch (error) {
        console.error('Failed to load storage trends', error);
        if (empty) empty.textContent = 'Storage trend data is currently unavailable.';
    }
}

function closeStorageTrends() {
    const modal = document.getElementById('storage-trend-modal');
    if (modal) modal.style.display = 'none';
}

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
            labels: ['DV P7 FEL', 'DV P7 MEL', 'DV P8.1', 'DV P8.4', 'DV P5', 'DV P10.1', 'DV P10.4', 'DV P20', 'HDR10+', 'HDR10', 'HLG', 'SDR'],
            datasets: [{ 
                data: [stats.dovi_p7_fel, stats.dovi_p7_mel, (stats.dovi_p81 + stats.dovi_p8), stats.dovi_p84, stats.dovi_p5, (stats.dovi_p101 + (stats.dovi_p10 || 0)), stats.dovi_p104, (stats.dovi_p20 || 0), stats.hdr10plus, stats.hdr10, stats.hlg, stats.sdr], 
                backgroundColor: ['#a55eea', '#5f27cd', '#e74c3c', '#fd79a8', '#27ae60', '#A1BC98', '#D2DCB6', '#6c5ce7', '#f1c40f', '#e67e22', '#3498db', '#555555'], borderWidth: 0 
            }]
        },
        options: { 
            responsive: true, maintainAspectRatio: false,
            onClick: (evt, elements) => {
                if (elements.length > 0) {
                    const label = formatChart.data.labels[elements[0].index];
                    if (label.includes('DV')) {
                        if (label.includes('FEL')) applyRibbonFilter('el', 'FEL');
                        else if (label.includes('MEL')) applyRibbonFilter('el', 'MEL');
                        else if (label.includes('P5')) applyRibbonFilter('dovi_prof', '5');
                        else if (label.includes('P8.1')) applyRibbonFilter('dovi_prof', '8.1');
                        else if (label.includes('P8.4')) applyRibbonFilter('dovi_prof', '8.4');
                        else if (label.includes('P10.1')) applyRibbonFilter('dovi_prof', '10.1');
                        else if (label.includes('P10.4')) applyRibbonFilter('dovi_prof', '10.4');
                        else if (label.includes('P20')) applyRibbonFilter('dovi_prof', '20');
                    } else if (label === 'HDR10+') applyRibbonFilter('format', 'hdr10plus');
                    else if (label === 'HDR10') applyRibbonFilter('format', 'hdr10');
                    else if (label === 'HLG') applyRibbonFilter('format', 'hlg');
                    else if (label === 'SDR') applyRibbonFilter('format', 'sdr_only');
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
            onClick: (evt, elements) => {
                if (elements.length > 0) {
                    const label = secChart.data.labels[elements[0].index];
                    let val = label;
                    if (label === 'HDR10+') val = 'HDR10+';
                    setMultiselectValue('secondary-filter', val);
                    resetAndLoad();
                }
            },
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
                    setMultiselectValue('vol-filter', stats.vol_labels[elements[0].index]);
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

    // Binary / status filters (checkbox multiselects; no Blanks row)
    updateMultiselectOptions('hybrid-filter', opts.special_hybrid || { '1': 0, '0': 0 }, { '1': 'Yes', '0': 'No' }, false);
    updateMultiselectOptions('source-hybrid-filter', opts.special_source_hybrid || { '1': 0, '0': 0 }, { '1': 'Yes', '0': 'No' }, false);
    updateMultiselectOptions('is-3d-filter', opts.special_is_3d || { '1': 0, '0': 0 }, { '1': '3D', '0': '2D' }, false);
    updateMultiselectOptions('status-filter', opts.special_status || { 'ok': 0, 'failed': 0 }, { 'ok': 'OK', 'failed': 'Failed' }, false);
    updateMultiselectOptions('missing-filter', opts.special_missing || { '1': 0, '0': 0 }, { '1': 'Yes', '0': 'No' }, false);
    updateMultiselectOptions('anomaly-filter', opts.special_anomaly || { '1': 0, '0': 0 }, { '1': 'Yes', '0': 'No' }, false);
    updateMultiselectOptions('nfo-filter', { '1': 0, '0': 0 }, { '1': 'Missing', '0': 'Found' }, false);
}
