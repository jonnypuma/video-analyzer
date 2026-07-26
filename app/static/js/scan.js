// --- WARNING POPUP FOR MISSING VOLUMES ---
async function startScan(mode = scanMode) {
    try {
        const res = await fetch('/api/pre_scan_check');
        const data = await res.json();
        
        const hasIssues = data.some(v => v.status !== 'online');
        if (!hasIssues) {
            const force = document.getElementById('chk-force').checked;
            triggerScan([], force, mode); 
            return;
        }

        const container = document.getElementById('vol-list-container');
        container.innerHTML = data.map(v => {
            const isOnline = v.status === 'online';
            const cls = isOnline ? 'status-online' : 'status-offline';
            return `
                <label class="vol-item">
                    <div class="vol-status-dot ${cls}"></div>
                    <div class="vol-name">${v.name} (${v.status.toUpperCase()})</div>
                    <input type="checkbox" class="vol-chk" value="${v.name}" ${isOnline ? 'checked' : ''}>
                </label>
            `;
        }).join('');
        
        document.getElementById('volume-modal').style.display = 'block';
    } catch(e) { console.error("Pre-scan check failed", e); alert("Error checking volumes."); }
}

async function confirmScan(fromModal = false, mode = scanMode) {
    const checkboxes = document.querySelectorAll('.vol-chk:checked');
    const selected = Array.from(checkboxes).map(c => c.value);
    if (fromModal && selected.length === 0) { alert("Please select at least one volume."); return; }
    
    // Modal overrides
    let force = document.getElementById('chk-force').checked;
    if(fromModal) {
        force = document.getElementById('chk-force-rescan').checked;
    }

    document.getElementById('volume-modal').style.display = 'none';
    triggerScan(selected, force, mode);
}

async function triggerScan(targets, force, mode = scanMode) {
    document.body.classList.add('scanning');
    document.getElementById('scan-info-box').innerHTML = `STARTING <div class="spinner"></div>`;
    const durEl = document.getElementById('stat-duration');
    if (durEl) durEl.innerText = formatDuration(0);
    const debug = document.getElementById('chk-debug').checked;
    // Initialize scanStartTime immediately to start timer right away
    scanStartTime = Date.now() / 1000;
    
    try { 
        await fetch('/start', { 
            method: 'POST', 
            headers: {'Content-Type': 'application/json'}, 
            body: JSON.stringify({ 
                threads: document.getElementById('scan-threads').value,
                targets: targets,
                force_rescan: force,
                debug_mode: debug,
                scan_mode: mode,
                scan_folder: scanFolderTarget
            }) 
        }); 
        setTimeout(poll, 500); 
    } catch (e) { 
        document.body.classList.remove('scanning'); 
        scanStartTime = 0;
        alert("Failed to start scan."); 
    }
}

async function abortScan() {
    if(confirm("Stop scan?")) {
        console.log("[DEBUG] Abort button clicked - sending abort request to server");
        // Update UI immediately, don't wait for server response
        const btn = document.getElementById('btn-abort');
        if(btn) { 
            btn.disabled = true; 
            btn.innerText = "Stopping..."; 
        }
        const scanInfoBox = document.getElementById('scan-info-box');
        if(scanInfoBox) {
            scanInfoBox.innerHTML = `STOPPING <div class="spinner"></div>`;
        }
        isAnalyzingFiles = false; // Clear flag on abort
        stopRefreshInterval(); // Stop refresh interval on abort
        
        try {
            const response = await fetch('/abort', { method: 'POST' });
            const data = await response.json();
            console.log(`[DEBUG] Abort request acknowledged by server:`, data);
            // Continue polling to see when abort completes
            setTimeout(poll, 1000);
        } catch (e) {
            console.error("[ERROR] Abort failed:", e);
            // Don't show alert, just log - UI is already updated
        }
    }
}


// Start/stop refresh interval timer for table updates during file analysis
function startRefreshInterval() {
    // Always stop existing timer first to avoid duplicates
    if (refreshIntervalTimer) {
        clearInterval(refreshIntervalTimer);
        refreshIntervalTimer = null;
    }
    const intervalEl = document.getElementById('scan-refresh');
    const intervalSeconds = parseInt(intervalEl?.value || 60) * 1000;
    refreshIntervalTimer = setInterval(() => {
        // Only refresh if we're scanning, analyzing files (not directory scanning), and not clearing/loading
        // Double-check isAnalyzingFiles flag to prevent refresh during directory scanning
        if (document.body.classList.contains('scanning') && isAnalyzingFiles && !isClearingFilters && !isLoading) {
            loadData();
        }
    }, intervalSeconds);
}

function stopRefreshInterval() {
    if (refreshIntervalTimer) {
        clearInterval(refreshIntervalTimer);
        refreshIntervalTimer = null;
    }
}

function applyPauseState(paused) {
    const container = document.getElementById('progress-container');
    const textEl = document.getElementById('progress-text');
    const subEl = document.getElementById('progress-subtext');
    if (!container || !textEl || !subEl) return;
    if (paused) {
        container.classList.add('paused');
        textEl.innerText = 'PAUSED';
        subEl.innerText = 'Click to continue analyzing';
    } else {
        container.classList.remove('paused');
        subEl.innerText = '';
    }
}

async function togglePause() {
    if (!document.body.classList.contains('scanning')) return;
    try {
        const res = await fetch('/pause', { method: 'POST' });
        const data = await res.json();
        applyPauseState(!!data.paused);
    } catch (e) {
        console.error("Failed to toggle pause:", e);
    }
}

async function poll() {
    try {
        const res = await fetch('/progress');
        const data = await res.json();
        if (data.status === 'scanning') {
            document.body.classList.add('scanning');
            // Sync scanStartTime with server only on first poll or if timer hasn't started yet
            if (!scanStartTime) {
                if (data.start_time && data.start_time > 0) {
                    // Server provides Unix timestamp, convert to relative start time
                    const srvDur = parseInt(data.last_duration.replace('s','')) || 0;
                    const now = Date.now() / 1000;
                    scanStartTime = now - srvDur;
                } else {
                    // Fallback: initialize from duration if start_time not available
                    const srvDur = parseInt(data.last_duration.replace('s','')) || 0;
                    scanStartTime = (Date.now() / 1000) - srvDur;
                }
            }
            // Don't sync after initial setup - let the client-side timer run continuously
            let pct = data.total > 0 ? Math.round((data.current / data.total) * 100) : 0;
            document.getElementById('progress-bar').style.width = pct + '%';
            if (data.paused) {
                applyPauseState(true);
            } else {
                applyPauseState(false);
                document.getElementById('progress-text').innerText = `${pct}% (${data.current}/${data.total})`;
            }
            document.getElementById('scan-info-box').innerHTML = `${data.file} <div class="spinner"></div>`;
            
            // Start refresh interval only when analyzing files (total > 0), not during directory scanning
            if (data.total > 0) {
                if (!isAnalyzingFiles) {
                    isAnalyzingFiles = true; // Transition into analysis phase
                    startRefreshInterval(); // Start refresh timer once
                }
            } else {
                if (isAnalyzingFiles) {
                    isAnalyzingFiles = false; // Transition out of analysis phase
                    stopRefreshInterval(); // Stop refresh timer during directory scanning
                }
            }
            
            // Update ribbon counts frequently; update charts/filters less often
            if (data.total > 0) {
                const now = Date.now();
                const shouldUpdateCharts = !lastFilterUpdate || (now - lastFilterUpdate >= 30000);
                // Fetch stats without affecting the timer
                    fetch(`/api/videos/meta?${new URLSearchParams({ ...activeFilters, include_options: shouldUpdateCharts ? '1' : '0' })}`)
                        .then(r => r.json())
                        .then(d => {
                            if (d.stats) {
                                applyVideosMeta(d);
                            }
                            if (shouldUpdateCharts) {
                                lastFilterUpdate = now;
                            }
                        })
                        .catch(e => console.error("Failed to update filter options:", e));
            }
            
            // Only refresh table data during scan, don't call loadData() which resets duration timer
            // Table will refresh when scan completes
            setTimeout(poll, 1000);
        } else { 
            // Stop refresh interval when scan is not active
            isAnalyzingFiles = false; // Clear flag when scan completes
            stopRefreshInterval();
            applyPauseState(false);
            if (document.body.classList.contains('scanning')) {
                document.body.classList.remove('scanning');
                scanStartTime = 0; // Reset timer when scan completes
                lastFilterUpdate = 0; // Reset filter update timer
                document.getElementById('scan-info-box').innerText = "IDLE";
                if (data.last_full_scan) setLastScanDisplay(data.last_full_scan);
                const btn = document.getElementById('btn-abort');
                if(btn) { btn.disabled = false; btn.innerText = "Abort Scan"; }
                loadData(); 
                updateLogs(); // Update console log when scan completes
                if (data.last_report && data.scan_completed) {
                    const style = document.getElementById('notif-style').value;
                    if (style === 'toast') {
                        showToast(`Scan Complete! New: ${data.last_report.new}`);
                        fetch('/clear_completed', { method: 'POST' });
                    } else {
                        const statusEl = document.getElementById('rep-status');
                        if (statusEl) {
                            if (data.last_report.aborted) {
                                statusEl.innerText = "ABORTED";
                                statusEl.style.color = "#e74c3c";
                            } else {
                                statusEl.innerText = "COMPLETE";
                                statusEl.style.color = "#3498db";
                            }
                        }
                        document.getElementById('report-modal').style.display = 'block';
                        document.getElementById('rep-scanned').innerText = data.last_report.scanned;
                        document.getElementById('rep-new').innerText = data.last_report.new;
                        const repRemoved = document.getElementById('rep-removed');
                        if (repRemoved) repRemoved.innerText = data.last_report.removed ?? 0;
                        const repRemovedNote = document.getElementById('rep-removed-note');
                        if (repRemovedNote) repRemovedNote.innerText = (data.last_report.removed > 0 && data.last_report.remove_missing_from_db === false) ? '(marked missing)' : '';
                        document.getElementById('rep-failed').innerText = data.last_report.failed;
                        document.getElementById('rep-warn').innerText = data.last_report.warnings ?? 0;
                        const repDup = document.getElementById('rep-dup');
                        if (repDup) repDup.innerText = data.last_report.duplicates ?? 0;
                        const repDupNote = document.getElementById('rep-dup-note');
                        if (repDupNote) repDupNote.innerText = ((data.last_report.duplicate_groups ?? 0) > 0) ? `(${data.last_report.duplicate_groups} groups)` : '';
                        document.getElementById('rep-time').innerText = formatDuration(data.last_report.duration);
                        document.getElementById('rep-date').innerText = data.last_report.date || '--';
                        updateReportDetails();
                        const offlineBox = document.getElementById('rep-offline-box');
                        const offlineList = document.getElementById('rep-offline-list');
                        if (data.last_report.offline && data.last_report.offline.length > 0) { offlineBox.style.display = 'block'; offlineList.innerText = data.last_report.offline.join('\n'); } else { offlineBox.style.display = 'none'; }
                        fetch('/clear_completed', { method: 'POST' });
                    }
                }
            }
        }
    } catch (e) { 
        console.error("Poll error:", e);
        // Continue polling even on error to handle temporary network issues
        setTimeout(poll, 1000);
    }
}
