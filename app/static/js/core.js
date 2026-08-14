// --- GLOBAL VARIABLES ---
let currentPage = 1, totalPages = 1, searchTimer, formatChart, volChart, resChart, secChart;
let lastFilterBlanks = {};
let configMode = 'schedule';
let scanStartTime = 0; 
let lastFilterUpdate = 0; // Track last filter update time (independent of duration timer) 
let selectedPaths = new Set();
let masterState = 0; 
let currentRows = [];
let sortCol = 'scan', sortOrder = 'desc';
let sortInitialized = false;
let savedColumnOrder = null;
let columnDragInitialized = false;
let settingsCache = null;
let columnWidths = null;
(function () {
    try {
        const raw = localStorage.getItem('column_widths');
        if (raw) {
            const p = JSON.parse(raw);
            if (p && typeof p === 'object' && Object.keys(p).length > 0) columnWidths = p;
        }
    } catch (e) {}
})();
/** Column resize state */
let isResizing = false;
let resizeColumn = null;
let startX = 0;
let startWidth = 0;
let columnResizeLastWidthPx = 0;
let columnResizeActive = false;
let columnResizeDelegated = false;
let suppressHeaderDrag = false;
let resizeColumnIndex = -1;
let activeResizePointerId = null;
const COL_RESIZE_DEBUG = false;
let colResizeDebugSeq = 0;
let colResizeLastMoveLogTs = 0;
let colResizePrevSlice = null;
const columnOrderByMediaKey = {};
const visibleColsByMediaKey = {};
let lastStats = null; 
let lastStatsFiltered = null;
let lastFullStats = null;
let lastFilterOptions = {};
let chartMode = 'total';
let barChartMode = 'volumes';
let settingsLoaded = false;
let settingsLoading = false;
let activeFilters = { search: '', category: '', volume: '', profile: '', el: '', container: '', is_hybrid: '', source_hybrid: '', secondary_hdr: '', status: '', resolution: '', size_op: '', size_val: '', bit_op: '', bit_val: '', audio: '', media_type: '', nfo_missing: '', missing: '', anomaly: '', is_3d: '' };
let isLoading = false;
let pendingReload = false;
let isClearingFilters = false; // Flag to prevent loadData() during clearFilters
let loadDataSeq = 0; // Ignore stale async responses from older filter loads
let lastScanStatus = "idle"; 
let refreshIntervalTimer = null; // Timer for periodic table refresh during file analysis
let isAnalyzingFiles = false; // Flag to track if we're in file analysis phase (total > 0) 
let selectedRowIndex = -1;
let scanFolders = [];
let folderVolumes = [];
let folderBrowser = { volume: '', path: '' };
let scanMode = 'all';
let scanFolderTarget = null;
let currentRowData = [];
let currentRowDataEncoded = [];
let currentDetailsPath = '';
let currentDetailsMediaType = '';
let currentDetailsMeta = {};
let duplicateGroups = [];
let duplicateActiveGroupId = '';
let duplicateMembers = [];
let duplicateSelectedPaths = new Set();
let dupGroupSortCol = 'file_count';
let dupGroupSortOrder = 'desc';
let dupMemberSortCol = '';
let dupMemberSortOrder = 'desc';

// --- CSRF: send token on mutating fetch calls ---
(function wrapFetchWithCsrf() {
    const originalFetch = window.fetch.bind(window);
    const mutating = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

    function readCsrfToken() {
        const meta = document.querySelector('meta[name="csrf-token"]');
        return (meta && meta.getAttribute('content')) || window.CSRF_TOKEN || '';
    }
    function writeCsrfToken(token) {
        if (!token) return;
        window.CSRF_TOKEN = token;
        let meta = document.querySelector('meta[name="csrf-token"]');
        if (!meta) {
            meta = document.createElement('meta');
            meta.setAttribute('name', 'csrf-token');
            document.head.appendChild(meta);
        }
        meta.setAttribute('content', token);
    }
    function captureCsrfToken(res) {
        if (!res || !res.headers || typeof res.headers.get !== 'function') return;
        const token = res.headers.get('X-CSRF-Token') || res.headers.get('X-CSRFToken');
        if (token) writeCsrfToken(token);
    }
    function resolveMethod(input, init) {
        if (init && init.method) return String(init.method).toUpperCase();
        if (typeof Request !== 'undefined' && input instanceof Request) {
            return String(input.method || 'GET').toUpperCase();
        }
        return 'GET';
    }
    function attachCsrf(init) {
        const token = readCsrfToken();
        const headers = new Headers(init.headers || {});
        if (token) {
            headers.set('X-CSRF-Token', token);
        }
        init.headers = headers;
        if (init.credentials == null) init.credentials = 'same-origin';
    }

    window.getCsrfToken = readCsrfToken;

    window.fetch = async function (input, init) {
        init = init ? { ...init } : {};
        const method = resolveMethod(input, init);
        if (mutating.has(method)) attachCsrf(init);
        let res = await originalFetch(input, init);
        captureCsrfToken(res);
        if (res.status === 403 && mutating.has(method)) {
            let csrfReject = true;
            try {
                const body = await res.clone().json();
                csrfReject = !body || body.message === 'Invalid CSRF token';
            } catch (e) { /* retry once if the 403 body is not JSON */ }
            if (csrfReject) {
                try {
                    const probe = await originalFetch('/api/health', { credentials: 'same-origin' });
                    captureCsrfToken(probe);
                } catch (e) { /* keep original 403 if refresh fails */ }
                attachCsrf(init);
                res = await originalFetch(input, init);
                captureCsrfToken(res);
            }
        }
        return res;
    };
})();

// Color generation function - generates distinct colors using HSL color space
function generateColors(count, paletteStyle = 'vibrant') {
    const colors = [];
    // HSL parameters for different styles
    const styleParams = {
        neon:    { sMin: 70, sMax: 100, lMin: 45, lMax: 70 },  // Bright, high contrast
        vintage: { sMin: 40, sMax: 75, lMin: 40, lMax: 70 },   // Muted, warm
        pastel:  { sMin: 20, sMax: 55, lMin: 65, lMax: 88 },   // Soft, light
        nature:  { sMin: 35, sMax: 75, lMin: 35, lMax: 65 },   // Earthy
        coffee:  { sMin: 35, sMax: 70, lMin: 35, lMax: 65 }    // Warm neutrals
    };
    const params = styleParams[paletteStyle] || styleParams.neon;
    
    // Generate colors evenly distributed around the hue circle
    for (let i = 0; i < count; i++) {
        const hue = (i * 360 / count) % 360;  // Evenly distribute around hue circle
        const saturation = params.sMin + (i % 3) * (params.sMax - params.sMin) / 2;  // Vary saturation
        const lightness = params.lMin + (i % 2) * (params.lMax - params.lMin);  // Vary lightness
        // Convert HSL to RGB
        const h = hue / 360;
        const s = saturation / 100;
        const l = lightness / 100;
        const c = (1 - Math.abs(2 * l - 1)) * s;
        const x = c * (1 - Math.abs((h * 6) % 2 - 1));
        const m = l - c / 2;
        let r, g, b;
        if (h < 1/6) { r = c; g = x; b = 0; }
        else if (h < 2/6) { r = x; g = c; b = 0; }
        else if (h < 3/6) { r = 0; g = c; b = x; }
        else if (h < 4/6) { r = 0; g = x; b = c; }
        else if (h < 5/6) { r = x; g = 0; b = c; }
        else { r = c; g = 0; b = x; }
        r = Math.round((r + m) * 255);
        g = Math.round((g + m) * 255);
        b = Math.round((b + m) * 255);
        colors.push(`#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`);
    }
    return colors;
}

// PALETTES - now using predefined colors for up to 30 volumes, then generating dynamically
const PALETTES = {
    vintage: ['#1A3263', '#547792', '#FAB95B', '#E8E2DB', '#3F9AAE', '#79C9C5', '#FFE2AF', '#F96E5B', '#8A8635', '#AA2B1D', '#CC561E', '#F3CF7A'],
    pastel:  ['#576A8F', '#B7BDF7', '#FFF8DE', '#FF7444', '#574964', '#9F8383', '#C8AAAA', '#FFDAB3', '#EA7B7B', '#D25353', '#9E3B3B', '#FFEAD3', '#5A9CB5', '#FACE68', '#FAAC68', '#FA6868', '#7F55B1', '#9B7EBD', '#F49BAB', '#FFE1E0'],
    neon:    ['#FFFADC', '#B6F500', '#A4DD00', '#98CD00', '#410445', '#A5158C', '#FF2DF1', '#F6DC43', '#362F4F', '#5B23FF', '#008BFF', '#E4FF30', '#00F7FF', '#B0FFFA', '#FF0087', '#FF7DB0'],
    nature:  ['#40513B', '#628141', '#E5D9B6', '#E67E22', '#313647', '#435663', '#A3B087', '#FFF8D4', '#A8BBA3', '#B87C4C', '#C4A484', '#F7F1DE', '#3F7D58', '#EFEFEF', '#EF9651', '#EC5228'],
    coffee:  ['#7B542F', '#B6771D', '#FF9D00', '#FFCF71', '#706D54', '#A08963', '#C9B194', '#DBDBDB', '#3E3F29', '#7D8D86', '#BCA88D', '#F1F0E4', '#7C444F', '#9F5255', '#E16A54', '#F39E60']
};
PALETTES.all = [...PALETTES.vintage, ...PALETTES.pastel, ...PALETTES.neon, ...PALETTES.nature, ...PALETTES.coffee];

const RES_COLORS = { '4K': '#f1c40f', '1080p': '#3498db', '720p': '#2ecc71', 'SD': '#95a5a6' };

function formatDuration(raw) {
    if (!raw) return "00:00:00";
    const sec = parseInt(raw.toString().replace('s', ''));
    if (isNaN(sec)) return "00:00:00";
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    return [h, m, s].map(v => v < 10 ? "0" + v : v).join(":");
}

const LAST_SCAN_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** Format server last_full_scan ("YYYY-MM-DD HH:MM:SS" or Never) as "12 Jul 2026". */
function formatLastScanDate(raw) {
    if (!raw || raw === 'Never' || raw === '--') return 'Never';
    const m = String(raw).match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!m) return String(raw);
    const day = parseInt(m[3], 10);
    const month = LAST_SCAN_MONTHS[parseInt(m[2], 10) - 1] || m[2];
    return `${day} ${month} ${m[1]}`;
}

function setLastScanDisplay(raw) {
    const el = document.getElementById('stat-last-scan');
    if (el) el.innerText = formatLastScanDate(raw);
}

function formatSize(bytes) {
    if (bytes == null || bytes === undefined || isNaN(bytes)) return '—';
    const n = Number(bytes);
    if (n === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.min(Math.floor(Math.log10(n) / 3), units.length - 1);
    const v = n / Math.pow(1000, i);
    return (i >= 2 ? v.toFixed(2) : Math.round(v)) + ' ' + units[i];
}

function formatUptime(seconds) {
    const sec = parseInt(seconds, 10);
    if (isNaN(sec) || sec < 0) return 'Unknown';
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    return [h, m, s].map(v => v < 10 ? `0${v}` : v).join(":");
}

function capitalizeFirst(value) {
    const text = (value ?? '').toString();
    if (!text) return '';
    return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatBytes(bytes, decimals = 2) {
    if (!+bytes) return '0 B';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
}

function escHtml(val) {
    return String(val ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function escAttr(val) {
    return escHtml(val)
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function escTextOrDash(val) {
    const text = val ?? '';
    return String(text) === '' ? '-' : escHtml(text);
}

const TABLE_EDGE_FIXED_PX = 40;
const TABLE_END_PAD_PX = 15;
function isTableChromeCell(el) {
    if (!el || !el.classList) return false;
    return el.classList.contains('col-chk') || el.classList.contains('col-del') || el.classList.contains('end-pad');
}
function tableChromeWidthPx(el) {
    if (el && el.classList && el.classList.contains('end-pad')) return TABLE_END_PAD_PX;
    return TABLE_EDGE_FIXED_PX;
}

// --- SCROLL TO CONSOLE ---
function scrollToConsole() {
    const consoleEl = document.querySelector('.console-wrap');
    if (!consoleEl) return;
    // Prefer scrollIntoView so sticky chrome / dynamic layout cannot leave us short.
    consoleEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// --- FILTER CAROUSEL SCROLLING ---
function scrollFilters(direction) {
    const container = document.getElementById('filter-scroll-area');
    const amount = 150; // Scroll 150px per click
    container.scrollBy({ left: amount * direction, behavior: 'smooth' });
}


// --- TOAST LOGIC ---
let toastTimer = null;
function showToast(msg, options = {}) {
    const t = document.getElementById('toast-notif');
    const isError = options.isError === true;
    const duration = options.duration ?? (isError ? 10000 : 4000);
    t.replaceChildren();
    const text = document.createElement('span');
    text.textContent = msg;
    t.appendChild(text);
    if (options.actionLabel && typeof options.onAction === 'function') {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'toast-action-btn';
        btn.textContent = options.actionLabel;
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            t.classList.remove('show');
            if (toastTimer) { clearTimeout(toastTimer); toastTimer = null; }
            try { options.onAction(); } catch (err) { console.error(err); }
        });
        t.appendChild(btn);
    }
    t.classList.toggle('toast-error', isError);
    t.classList.add('show');
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { t.classList.remove('show'); toastTimer = null; }, duration);
    if (isError) {
        fetch('/api/log_client_error', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({message: msg}) }).catch(() => {});
    }
}
