const ANNUAL_CHART_KEYS = [
  '주가 (원)', '시가총액', 'PER', 'PBR', 'ROE (%)', 'EPS (원)',
  '부채비율 (%)', '영업이익률 (%)', '배당수익률 (%)', '주당배당금 (원)'
];

const WEEKLY_CHART_KEYS = [
  '주가', 'PER', 'PBR', '배당수익률 (%)',
  '시가총액', 'EPS (원)', 'ROE (%)', '부채비율 (%)', '영업이익률 (%)'
];

const CHART_COLORS = [
  '#e11d48','#2563eb','#7c3aed','#059669','#d97706',
  '#dc2626','#0891b2','#4f46e5','#c026d3'
];
const PER_DISPLAY_MAX = 100;

const GUEST_RECENT_KEY = 'guest_recent';
const GUEST_RECENT_MAX = 20;

// Render markdown to sanitized HTML. Uses DOMPurify when loaded; falls
// back to escaped plain text if either lib is missing. Callers must use
// this instead of `marked.parse(...)` directly because AI / user inputs
// can inject raw HTML otherwise.
function _renderSafeMarkdown(mdText) {
  if (typeof marked === 'undefined') return '';
  const html = marked.parse(mdText);
  if (typeof DOMPurify !== 'undefined') return DOMPurify.sanitize(html);
  // DOMPurify failed to load (offline?) — strip all tags as the safe
  // fallback; users see plain text instead of rendered markdown.
  const tmp = document.createElement('div');
  tmp.innerHTML = html;
  return (tmp.textContent || '').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function getGuestRecent() {
  try { return JSON.parse(localStorage.getItem(GUEST_RECENT_KEY)) || []; } catch { return []; }
}

function saveGuestRecent(stockCode, corpName) {
  const list = getGuestRecent().filter(i => i.stock_code !== stockCode);
  list.unshift({ stock_code: stockCode, corp_name: corpName });
  if (list.length > GUEST_RECENT_MAX) list.length = GUEST_RECENT_MAX;
  localStorage.setItem(GUEST_RECENT_KEY, JSON.stringify(list));
}

function removeGuestRecent(stockCode) {
  const list = getGuestRecent().filter(i => i.stock_code !== stockCode);
  localStorage.setItem(GUEST_RECENT_KEY, JSON.stringify(list));
}

function flashEl(el) {
  if (!el) return;
  el.classList.remove('flash-update');
  void el.offsetWidth;
  el.classList.add('flash-update');
}

let charts = {};
let searchTimeout = null;
let selectedIdx = -1;
let currentAbortController = null;
let recentListLoading = false;
let recentListItems = [];
let activeStockCode = null;
let activeIndicators = {};
let activeQuoteSnapshot = {};
let authConfig = null;
let currentUser = null;
let googleButtonRetryTimer = null;
let googleAuthInitialized = false;
let googleButtonRetryCount = 0;
let currentUserPreference = null;
let preferenceSaving = false;
let activeTab = 'recent';
const API_BASE_URL = (APP_CONFIG.apiBaseUrl || '').replace(/\/$/, '');
const IS_GITHUB_PAGES_SITE = window.location.hostname.endsWith('github.io');
const REPORT_LOCAL_CACHE_TTL_MS = 6 * 60 * 60 * 1000;

function buildApiUrl(path) {
  return `${API_BASE_URL}${path}`;
}

function apiFetch(path, options = {}) {
  const init = {
    credentials: 'include',
    ...options,
  };
  if (options.headers) {
    init.headers = { ...options.headers };
  }
  return fetch(buildApiUrl(path), init);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function safeExternalUrl(value) {
  if (!value) return '';
  try {
    const url = new URL(value, window.location.href);
    return ['http:', 'https:'].includes(url.protocol) ? url.href : '';
  } catch {
    return '';
  }
}

function buildReportPdfUrl(pdfUrl) {
  if (!pdfUrl) return '';
  return buildApiUrl(`/api/report-pdf?url=${encodeURIComponent(pdfUrl)}`);
}

function getReportCacheKey(stockCode) {
  return `report_cache:${stockCode}`;
}

function loadReportCache(stockCode) {
  try {
    const raw = localStorage.getItem(getReportCacheKey(stockCode));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    if (!parsed.savedAt || (Date.now() - parsed.savedAt) > REPORT_LOCAL_CACHE_TTL_MS) return null;
    return parsed;
  } catch {
    return null;
  }
}

function saveReportCache(stockCode, payload) {
  try {
    localStorage.setItem(getReportCacheKey(stockCode), JSON.stringify({
      ...payload,
      savedAt: Date.now(),
    }));
  } catch (e) { console.warn(e); }
}

function sameReport(a, b) {
  if (!a || !b) return false;
  return a.date === b.date && a.title === b.title && a.firm === b.firm && a.pdf_url === b.pdf_url;
}

function hasApiConfiguration() {
  return !IS_GITHUB_PAGES_SITE || Boolean(API_BASE_URL);
}

function requireApiConfiguration() {
  if (!hasApiConfiguration()) {
    throw new Error('GitHub Pages에서는 app-config.js의 apiBaseUrl에 FastAPI 서버 주소를 설정해야 합니다.');
  }
}

// --- Toast notification system ---
function showToast(message, type = 'error', duration = 4000) {
  let container = document.getElementById('toastContainer');
  if (!container) {
    container = document.createElement('div');
    container.id = 'toastContainer';
    container.style.cssText = 'position:fixed;top:16px;right:16px;z-index:10000;display:flex;flex-direction:column;gap:8px;pointer-events:none;';
    document.body.appendChild(container);
  }
  const toast = document.createElement('div');
  const colors = { error: '#dc2626', success: '#059669', warning: '#d97706', info: '#2563eb' };
  toast.style.cssText = `background:${colors[type] || colors.error};color:white;padding:10px 16px;border-radius:8px;font-size:13px;max-width:360px;box-shadow:0 4px 12px rgba(0,0,0,0.15);pointer-events:auto;cursor:pointer;opacity:0;transition:opacity 0.2s;`;
  toast.textContent = message;
  toast.addEventListener('click', () => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 200); });
  container.appendChild(toast);
  requestAnimationFrame(() => { toast.style.opacity = '1'; });
  setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 200); }, duration);
}

// --- Chart library lazy loader (ECharts for tablet+, uPlot for mobile) ---
const USE_UPLOT = window.innerWidth < 768;
let _chartLibLoaded = false;
let _chartLibLoading = null;

function _showChartLoading() {
  document.querySelectorAll('.chart-canvas-wrap, .pf-nav-chart-container').forEach(el => {
    if (el.querySelector('.chart-loading')) return;
    const loader = document.createElement('div');
    loader.className = 'chart-loading';
    loader.innerHTML = '<div class="chart-loading-dot"></div><div class="chart-loading-dot"></div><div class="chart-loading-dot"></div>';
    el.appendChild(loader);
  });
}
function _hideChartLoading() {
  document.querySelectorAll('.chart-loading').forEach(el => el.remove());
}

function loadChartLib() {
  if (_chartLibLoaded) return Promise.resolve();
  if (_chartLibLoading) return _chartLibLoading;
  _showChartLoading();
  const src = USE_UPLOT
    ? 'https://cdn.jsdelivr.net/npm/uplot@1.6.31/dist/uPlot.iife.min.js'
    : 'https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js';
  const promises = [new Promise((resolve, reject) => {
    const s = document.createElement('script');
    s.src = src;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error('Chart lib load failed'));
    document.head.appendChild(s);
  })];
  if (USE_UPLOT) {
    promises.push(new Promise((resolve, reject) => {
      const link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = 'https://cdn.jsdelivr.net/npm/uplot@1.6.31/dist/uPlot.min.css';
      link.onload = () => resolve();
      link.onerror = () => reject();
      document.head.appendChild(link);
    }));
  }
  _chartLibLoading = Promise.all(promises).then(() => { _chartLibLoaded = true; _hideChartLoading(); });
  return _chartLibLoading;
}
// Backwards compat alias
const loadECharts = loadChartLib;

/**
 * Map a return % to a blue-red color.
 * Negative → blue (#2563eb), zero → gray (#9ca3af), positive → red (#dc2626)
 * Clamped to [-range, +range] for full saturation.
 */
function returnToColor(pct, range = 20) {
  if (pct == null) return '#9ca3af';
  const t = Math.max(-1, Math.min(1, pct / range)); // -1 ~ +1
  const abs = Math.abs(t);
  // gray(156,163,175) → blue(37,99,235) or red(220,38,38)
  const gray = [156, 163, 175];
  const blue = [37, 99, 235];
  const red = [220, 38, 38];
  const target = t < 0 ? blue : red;
  const r = Math.round(gray[0] + (target[0] - gray[0]) * abs);
  const g = Math.round(gray[1] + (target[1] - gray[1]) * abs);
  const b = Math.round(gray[2] + (target[2] - gray[2]) * abs);
  return `#${r.toString(16).padStart(2,'0')}${g.toString(16).padStart(2,'0')}${b.toString(16).padStart(2,'0')}`;
}

function _hexToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1,3), 16), g = parseInt(hex.slice(3,5), 16), b = parseInt(hex.slice(5,7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

/**
 * Unified chart creation — ECharts on tablet+, uPlot on mobile.
 * Returns an object with .dispose() and .resize() methods.
 * @param {HTMLElement} container - div element
 * @param {Object} opts - { labels, values, color, smooth, fill, yMin, tooltipPrefix, yFormatter, connectNulls }
 */
function createLineChart(container, opts) {
  const { labels, values, color = '#3b82f6', smooth = 0.3, fill = true,
          yMin, tooltipPrefix = '', yFormatter, connectNulls = false, dataZoom = false } = opts;

  if (USE_UPLOT) {
    return _createUPlotChart(container, opts);
  } else {
    return _createEChartsChart(container, opts);
  }
}

function _createEChartsChart(container, opts) {
  const { labels, values, color = '#3b82f6', smooth = 0.3, fill = true,
          yMin, tooltipPrefix = '', yFormatter, tooltipFormatter, connectNulls = false, dataZoom = false } = opts;
  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#333';

  const ec = echarts.init(container);
  const zoomComponents = dataZoom ? [
    { type: 'slider', height: 22, bottom: 4, borderColor: gridColor, fillerColor: _hexToRgba(color, 0.12),
      handleStyle: { color }, textStyle: { color: textColor, fontSize: 10 },
      labelFormatter: (_, val) => labels[Math.round(val)] || '' },
    // Wheel-zoom disabled — page scroll kept getting hijacked when the
    // cursor happened to be over the chart. Slider drag + click-drag pan
    // still work.
    { type: 'inside', zoomOnMouseWheel: false, moveOnMouseWheel: false },
  ] : [];
  const bottomPad = dataZoom ? 56 : 24;
  ec.setOption({
    grid: { left: yFormatter ? 65 : 50, right: 12, top: 10, bottom: bottomPad },
    dataZoom: zoomComponents,
    xAxis: {
      type: 'category',
      data: labels,
      axisLine: { lineStyle: { color: gridColor } },
      axisLabel: { color: textColor, fontSize: 10 },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value',
      min: yMin,
      axisLine: { show: false },
      axisLabel: { color: textColor, fontSize: 10, formatter: yFormatter },
      splitLine: { lineStyle: { color: gridColor, width: 0.5 } },
    },
    tooltip: {
      trigger: 'axis',
      formatter(params) {
        const p = params[0];
        const val = p.value == null || p.value === '-' ? 'N/A' : (tooltipPrefix + (tooltipFormatter ? tooltipFormatter(p.value) : Number(p.value).toLocaleString()));
        return `${labels[p.dataIndex]}<br/>${val}`;
      },
    },
    series: [{
      type: 'line',
      data: values.map(v => v === null ? '-' : v),
      smooth,
      symbol: values.length > 30 ? 'none' : 'circle',
      symbolSize: values.length > 60 ? 0 : 4,
      lineStyle: { color, width: 2 },
      itemStyle: { color },
      areaStyle: fill ? {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: _hexToRgba(color, 0.25) },
          { offset: 1, color: _hexToRgba(color, 0.0) },
        ]),
      } : undefined,
      connectNulls,
    }],
  });
  return ec;
}

function _createUPlotChart(container, opts) {
  const { labels, values, color = '#3b82f6', fill = true, yMin, yFormatter } = opts;
  container.innerHTML = '';
  const w = container.clientWidth;
  const h = container.clientHeight || 220;

  // uPlot needs numeric x-axis — use index
  const xData = labels.map((_, i) => i);
  const yData = values.map(v => v === null ? null : v);
  const data = [xData, yData];

  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#e0e0e0';

  const uOpts = {
    width: w,
    height: h,
    cursor: { show: true, drag: { x: false, y: false } },
    select: { show: false },
    legend: { show: false },
    axes: [
      {
        stroke: textColor,
        grid: { stroke: gridColor, width: 0.5 },
        values: (_, splits) => splits.map(i => labels[Math.round(i)] || ''),
        font: '10px sans-serif',
        ticks: { stroke: gridColor, width: 0.5 },
      },
      {
        stroke: textColor,
        grid: { stroke: gridColor, width: 0.5 },
        values: yFormatter ? (_, splits) => splits.map(v => yFormatter(v)) : undefined,
        font: '10px sans-serif',
        ticks: { stroke: gridColor, width: 0.5 },
      },
    ],
    scales: {
      y: { min: yMin },
    },
    series: [
      {},
      {
        stroke: color,
        width: 2,
        fill: fill ? _hexToRgba(color, 0.12) : undefined,
        points: { show: values.length <= 30, size: 4, stroke: color, fill: color },
      },
    ],
  };

  const chart = new uPlot(uOpts, data, container);
  // Return ECharts-compatible interface
  return {
    dispose() { chart.destroy(); },
    resize() { chart.setSize({ width: container.clientWidth, height: container.clientHeight || h }); },
    _uplot: chart,
  };
}

function updateAnalyticsAuthState() {
  if (IS_LOCALHOST || typeof gtag !== 'function') return;
  gtag('set', 'user_properties', {
    login_state: currentUser ? 'logged_in' : 'guest',
  });
}
