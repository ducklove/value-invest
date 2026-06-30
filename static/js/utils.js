// ROE 는 PBR 바로 다음, 주당배당금(주당배당액)은 배당수익률 바로 앞에 배치.
const ANNUAL_CHART_KEYS = [
  '주가 (원)', '시가총액', 'PER', 'PBR', 'ROE (%)', 'EPS (원)',
  '부채비율 (%)', '영업이익률 (%)', '주당배당금 (원)', '배당수익률 (%)'
];

const WEEKLY_CHART_KEYS = [
  '주가', 'PER', 'PBR', 'ROE (%)', '시가총액', 'EPS (원)',
  '부채비율 (%)', '영업이익률 (%)', '주당배당금 (원)', '배당수익률 (%)'
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

function quoteIsUsable(q) {
  return !!q && q.price !== null && q.price !== undefined && q._stale !== true;
}

function quotePriceOrNull(q) {
  return q && q.price !== null && q.price !== undefined ? q.price : null;
}

function quoteSnapshotDateValue(q) {
  const raw = q && q.date;
  if (!raw) return null;
  const text = String(raw);
  let parsed = NaN;
  if (/^\d{8}$/.test(text)) {
    parsed = Date.UTC(Number(text.slice(0, 4)), Number(text.slice(4, 6)) - 1, Number(text.slice(6, 8)));
  } else {
    parsed = Date.parse(text);
  }
  return Number.isFinite(parsed) ? parsed : null;
}

function quoteSnapshotTimeValue(q) {
  if (!q) return null;
  const rawTs = q.ts ?? q.fetched_at ?? q.fetchedAt ?? q._receivedAt;
  if (rawTs === null || rawTs === undefined || rawTs === '') return null;
  if (typeof rawTs === 'number' || /^\d+(\.\d+)?$/.test(String(rawTs))) {
    const numeric = Number(rawTs);
    if (!Number.isFinite(numeric)) return null;
    return numeric < 10_000_000_000 ? numeric * 1000 : numeric;
  }
  const parsed = Date.parse(String(rawTs));
  return Number.isFinite(parsed) ? parsed : null;
}

function quoteSourceRank(q) {
  if (!q || q._stale === true) return 0;
  const source = String(q.source || q._source || '').toLowerCase();
  if (source.includes('ws')) return 4;
  if (source.includes('rest') || source.includes('quote')) return 3;
  if (source.includes('history')) return 1;
  return 2;
}

// 랭크 강등 보호 시간 — 서버(services/portfolio/quotes.py의
// QUOTE_RANK_PROTECT_SECONDS)와 같은 값·같은 이유. 늦게 도착한 REST 응답이
// 더 새로운 WS 틱을 되돌리는 레이스만 막고, 보호가 끝나면 시각 비교로
// 넘어가 WS 틱이 끊긴 종목도 폴링 시세로 계속 갱신되게 한다.
const QUOTE_RANK_PROTECT_MS = 20_000;

function quoteSnapshotIsRecent(q) {
  const ts = quoteSnapshotTimeValue(q);
  if (ts === null) return true; // 시각을 모르면 보수적으로 보호 유지
  return Date.now() - ts < QUOTE_RANK_PROTECT_MS;
}

function shouldAcceptQuoteSnapshot(current, incoming) {
  if (!incoming || incoming.price === null || incoming.price === undefined) return false;
  if (incoming._stale === true && quoteIsUsable(current)) return false;
  const currentDate = quoteSnapshotDateValue(current);
  const incomingDate = quoteSnapshotDateValue(incoming);
  if (currentDate !== null && incomingDate !== null) {
    if (incomingDate < currentDate) return false;
    if (incomingDate > currentDate) return true;
  }
  const currentRank = quoteSourceRank(current);
  const incomingRank = quoteSourceRank(incoming);
  if (incomingRank > currentRank) return true;
  // 랭크 강등은 현재 시세가 보호 시간 안에 있을 때만 거부.
  if (incomingRank < currentRank && quoteSnapshotIsRecent(current)) return false;

  const currentTime = quoteSnapshotTimeValue(current);
  const incomingTime = quoteSnapshotTimeValue(incoming);
  if (currentTime !== null && incomingTime !== null) {
    if (incomingTime < currentTime) return false;
    if (incomingTime > currentTime) return true;
  }
  return true;
}

function quoteValuePresent(value) {
  return value !== null && value !== undefined && value !== '';
}

function mergeQuoteSupplementalFields(current, incoming) {
  const next = { ...(current || {}) };
  if (!incoming || incoming._stale === true) return next;
  const fields = ['previous_close', 'trade_value'];
  fields.forEach(field => {
    if (!quoteValuePresent(next[field]) && quoteValuePresent(incoming[field])) {
      next[field] = incoming[field];
    }
  });
  const price = Number(next.price);
  const previousClose = Number(next.previous_close);
  if (Number.isFinite(price) && Number.isFinite(previousClose) && previousClose !== 0) {
    if (!quoteValuePresent(next.change)) {
      next.change = price - previousClose;
    }
    if (!quoteValuePresent(next.change_pct)) {
      next.change_pct = (price - previousClose) / previousClose * 100;
    }
  } else {
    ['change', 'change_pct'].forEach(field => {
      if (!quoteValuePresent(next[field]) && quoteValuePresent(incoming[field])) {
        next[field] = incoming[field];
      }
    });
  }
  return next;
}

function mergeQuoteSnapshot(current, incoming) {
  if (!shouldAcceptQuoteSnapshot(current, incoming)) return mergeQuoteSupplementalFields(current, incoming);
  const next = { ...(current || {}), ...(incoming || {}) };
  if (!incoming || incoming._stale !== true) delete next._stale;
  return next;
}

function quoteSnapshotDisplayChanged(before, after) {
  const fields = ['date', 'price', 'previous_close', 'change', 'change_pct', 'trade_value', '_stale'];
  return fields.some(field => (before || {})[field] !== (after || {})[field]);
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
const APP_CONFIG_DATA = window.APP_CONFIG || {};
const API_BASE_URL = (APP_CONFIG_DATA.apiBaseUrl || '').replace(/\/$/, '');
const APP_INTEGRATIONS = APP_CONFIG_DATA.integrations || {};
const IS_GITHUB_PAGES_SITE = window.location.hostname.endsWith('github.io');
const REPORT_LOCAL_CACHE_TTL_MS = 6 * 60 * 60 * 1000;

function buildApiUrl(path) {
  return `${API_BASE_URL}${path}`;
}

function getIntegrationConfig(key) {
  return APP_INTEGRATIONS[key] || {};
}

function getIntegrationEndpoint(key, endpointKey, pathFallback = '') {
  const config = getIntegrationConfig(key);
  if (config[endpointKey]) return config[endpointKey];
  return pathFallback ? buildIntegrationUrl(key, pathFallback) : '';
}

function buildIntegrationUrl(key, path = '', query = {}) {
  const baseUrl = getIntegrationConfig(key).baseUrl || '';
  if (!baseUrl) return '';
  try {
    const url = new URL(baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`);
    const cleanPath = String(path || '').replace(/^\/+/, '');
    if (cleanPath) {
      url.pathname = `${url.pathname.replace(/\/+$/, '')}/${cleanPath}`;
    }
    Object.entries(query || {}).forEach(([name, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        url.searchParams.set(name, value);
      }
    });
    return url.toString();
  } catch (e) {
    console.warn(e);
    return '';
  }
}

function openIntegration(key, path = '', query = {}) {
  // 새 탭으로 열리는 연결 대시보드가 현재 앱 테마(라이트/다크)로 뜨도록 전달.
  const theme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  const url = buildIntegrationUrl(key, path, { theme, ...query });
  if (!url) {
    showToast('Integration URL is not configured.', 'warning');
    return;
  }
  window.open(url, '_blank', 'noopener');
}

// apiFetch 기본 타임아웃. 일반 JSON API 는 20초가 지나면 AbortController 로
// 중단한다. 호출별로 { timeoutMs: n } 으로 조정(0/null 이면 비활성).
const API_FETCH_TIMEOUT_MS = 20000;

function _apiTimeoutError(timeoutMs) {
  const seconds = Math.round(Number(timeoutMs || 0) / 1000);
  const message = seconds > 0
    ? `요청 시간이 ${seconds}초를 초과했습니다.`
    : '요청 시간이 초과되었습니다.';
  if (typeof DOMException === 'function') {
    return new DOMException(message, 'TimeoutError');
  }
  const err = new Error(message);
  err.name = 'TimeoutError';
  return err;
}

function apiFetch(path, options = {}) {
  const { stream = false, timeoutMs = API_FETCH_TIMEOUT_MS, ...fetchOptions } = options;
  const method = String(fetchOptions.method || 'GET').toUpperCase();
  const isAdminMutation = String(path || '').startsWith('/api/admin/')
    && ['POST', 'PUT', 'PATCH', 'DELETE'].includes(method);
  const init = {
    credentials: 'include',
    ...fetchOptions,
  };
  if (fetchOptions.headers || isAdminMutation) {
    init.headers = { ...(isAdminMutation ? {
      'Content-Type': 'application/json',
      'X-Requested-With': 'fetch',
    } : {}), ...(fetchOptions.headers || {}) };
  }
  // SSE/스트리밍 응답({ stream: true } — AI 분석, 위키 Q&A 등)은 20초보다
  // 오래 열려 있어야 하므로 타임아웃을 걸지 않는다. 호출자가 직접 signal 을
  // 넘긴 경우(분석 취소 등)도 호출자의 수명 관리를 그대로 따른다.
  if (stream || fetchOptions.signal || !timeoutMs) {
    return fetch(buildApiUrl(path), init);
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(_apiTimeoutError(timeoutMs)), timeoutMs);
  init.signal = controller.signal;
  return fetch(buildApiUrl(path), init).finally(() => clearTimeout(timer));
}

function _isAbortError(error) {
  return !!error && (error.name === 'AbortError' || error.name === 'TimeoutError');
}

// API 오류 공통 처리 — 콘솔에는 항상 컨텍스트와 함께 남긴다.
// 사용자가 직접 누른 동작(기본)은 토스트로 알리고('저장 실패: ...' 톤),
// 백그라운드 갱신/선택적 보강은 { silent: true } 로 로그만 남긴다.
function reportApiError(error, context, options = {}) {
  const label = context || '요청';
  console.warn(`[api] ${label} 실패`, error);
  if (options.silent) return;
  const detail = _isAbortError(error)
    ? '요청 시간이 초과되었습니다.'
    : (error && error.message) || '네트워크 오류';
  showToast(`${label} 실패: ${detail}`);
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
const CHART_LIB_TIMEOUT_MS = 12000;

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

function _loadScriptWithFallback(urls, globalName) {
  return new Promise((resolve, reject) => {
    let index = 0;
    const tryNext = () => {
      if (globalName && window[globalName]) {
        resolve();
        return;
      }
      if (index >= urls.length) {
        reject(new Error(`Chart lib load failed: ${globalName || urls[0]}`));
        return;
      }
      const script = document.createElement('script');
      let settled = false;
      const cleanup = () => {
        settled = true;
        clearTimeout(timer);
      };
      const timer = setTimeout(() => {
        if (settled) return;
        cleanup();
        script.remove();
        index += 1;
        tryNext();
      }, CHART_LIB_TIMEOUT_MS);
      script.src = urls[index];
      script.async = true;
      script.onload = () => {
        if (settled) return;
        cleanup();
        resolve();
      };
      script.onerror = () => {
        if (settled) return;
        cleanup();
        script.remove();
        index += 1;
        tryNext();
      };
      document.head.appendChild(script);
    };
    tryNext();
  });
}

function _loadStylesheetWithFallback(urls) {
  return new Promise(resolve => {
    let index = 0;
    const tryNext = () => {
      if (index >= urls.length) {
        resolve();
        return;
      }
      const link = document.createElement('link');
      let settled = false;
      const cleanup = () => {
        settled = true;
        clearTimeout(timer);
      };
      const timer = setTimeout(() => {
        if (settled) return;
        cleanup();
        link.remove();
        index += 1;
        tryNext();
      }, CHART_LIB_TIMEOUT_MS);
      link.rel = 'stylesheet';
      link.href = urls[index];
      link.onload = () => {
        if (settled) return;
        cleanup();
        resolve();
      };
      link.onerror = () => {
        if (settled) return;
        cleanup();
        link.remove();
        index += 1;
        tryNext();
      };
      document.head.appendChild(link);
    };
    tryNext();
  });
}

function loadChartLib(options = {}) {
  const silent = !!options.silent;
  const globalName = USE_UPLOT ? 'uPlot' : 'echarts';
  if (_chartLibLoaded || window[globalName]) {
    _chartLibLoaded = true;
    if (!silent) _hideChartLoading();
    return Promise.resolve();
  }
  if (_chartLibLoading) {
    if (!silent) _showChartLoading();
    return _chartLibLoading.finally(() => {
      if (!silent) _hideChartLoading();
    });
  }
  if (!silent) _showChartLoading();
  const scriptUrls = USE_UPLOT
    ? [
        'https://cdn.jsdelivr.net/npm/uplot@1.6.31/dist/uPlot.iife.min.js',
        'https://unpkg.com/uplot@1.6.31/dist/uPlot.iife.min.js',
      ]
    : [
        'https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js',
        'https://cdnjs.cloudflare.com/ajax/libs/echarts/5.6.0/echarts.min.js',
        'https://unpkg.com/echarts@5.6.0/dist/echarts.min.js',
      ];
  const promises = [_loadScriptWithFallback(scriptUrls, globalName)];
  if (USE_UPLOT) {
    promises.push(_loadStylesheetWithFallback([
      'https://cdn.jsdelivr.net/npm/uplot@1.6.31/dist/uPlot.min.css',
      'https://unpkg.com/uplot@1.6.31/dist/uPlot.min.css',
    ]));
  }
  _chartLibLoading = Promise.all(promises)
    .then(() => {
      if (!window[globalName]) throw new Error(`Chart lib loaded without ${globalName}`);
      _chartLibLoaded = true;
    })
    .catch(err => {
      _chartLibLoading = null;
      throw err;
    })
    .finally(() => {
      if (!silent) _hideChartLoading();
    });
  return _chartLibLoading;
}
function preloadChartLib() {
  return loadChartLib({ silent: true }).catch(err => {
    console.warn('Chart preload failed; will retry on demand.', err);
  });
}
function scheduleChartPreload() {
  if (_chartLibLoaded || _chartLibLoading) return;
  const run = () => preloadChartLib();
  if (typeof requestIdleCallback === 'function') {
    requestIdleCallback(run, { timeout: 1500 });
  } else {
    setTimeout(run, 800);
  }
}
// echarts 전용 로더. 모바일(USE_UPLOT)에서 밸류에이션 차트는 uPlot 을 쓰지만,
// 증권사 목표가 차트는 echarts API(scatter+legend+tooltip+클릭)에 의존하므로
// loadChartLib 와 별개로 echarts 를 보장 로드한다.
let _echartsLoading = null;
function loadEcharts() {
  if (typeof window !== 'undefined' && window.echarts) return Promise.resolve();
  if (_echartsLoading) return _echartsLoading;
  _echartsLoading = _loadScriptWithFallback([
    'https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js',
    'https://cdnjs.cloudflare.com/ajax/libs/echarts/5.6.0/echarts.min.js',
    'https://unpkg.com/echarts@5.6.0/dist/echarts.min.js',
  ], 'echarts')
    .then(() => { if (!window.echarts) throw new Error('echarts load failed'); })
    .catch(err => { _echartsLoading = null; throw err; });
  return _echartsLoading;
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
// echarts 폭0 자가복구 — grid 가 display:none→grid 로 막 바뀐 직후처럼 레이아웃
// reflow 전에 init 되면 캔버스가 0 크기로 굳어 차트가 안 보인다(다른 종목을 갔다
// 와야 보이던 증상). 컨테이너가 실제 크기를 가지는 순간 한 번 resize 해서 그린다.
function _autoResizeOnLayout(ec, container) {
  if (container.clientWidth && container.clientHeight) return;  // 이미 정상
  let done = false, ro = null;
  const fix = () => {
    if (done) return;
    if (container.clientWidth && container.clientHeight) {
      done = true;
      try { ec.resize(); } catch (e) {}
      if (ro) ro.disconnect();
    }
  };
  if (typeof ResizeObserver !== 'undefined') {
    ro = new ResizeObserver(fix);
    ro.observe(container);
  }
  requestAnimationFrame(fix);  // ResizeObserver 미지원/누락 대비 폴백 한 프레임
}

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
  _autoResizeOnLayout(ec, container);
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

  // 컨테이너가 아직 레이아웃되지 않아(예: 뷰가 display:none 인 상태로 렌더) 폭이 0이면
  // uPlot 이 0 폭으로 굳어 빈 차트가 된다. ResizeObserver 로 컨테이너가 실제 폭을 갖는
  // 순간(뷰가 보일 때·회전·창 크기 변경) 자동으로 다시 사이즈를 맞춰 자가 치유한다.
  let _ro = null;
  if (typeof ResizeObserver !== 'undefined') {
    _ro = new ResizeObserver(() => {
      const cw = container.clientWidth;
      if (cw <= 0) return;
      const ch = container.clientHeight || h;
      // 폭이 바뀌면 다시 맞춘다. 그리고 0폭으로 생성됐다가 뷰가 보이며 폭이 생긴
      // 경우 setSize 만으로는 시리즈(선)가 다시 그려지지 않을 때가 있어 redraw 로
      // 강제 재그리기한다(크기 동일해도 1회 페인트 보정).
      if (cw !== chart.width || ch !== chart.height) chart.setSize({ width: cw, height: ch });
      chart.redraw();
    });
    _ro.observe(container);
  }

  // Return ECharts-compatible interface
  return {
    dispose() { if (_ro) { _ro.disconnect(); _ro = null; } chart.destroy(); },
    resize() {
      const cw = container.clientWidth;
      if (cw <= 0) return;
      chart.setSize({ width: cw, height: container.clientHeight || h });
      chart.redraw();
    },
    _uplot: chart,
  };
}

function updateAnalyticsAuthState() {
  if (IS_LOCALHOST || typeof gtag !== 'function') return;
  gtag('set', 'user_properties', {
    login_state: currentUser ? 'logged_in' : 'guest',
  });
}

// ── 접근성: 차트 대체 텍스트 (ST-05) ─────────────────────────────────
// echarts 는 canvas 로 렌더링하므로 스크린 리더가 내용을 읽지 못한다.
// describeChart() 는 차트 컨테이너에 role="img" + aria-label(요약 문구)을
// 붙이고, 선택적으로 핵심 데이터를 숨김 표(.sr-only-chart-table)로 제공해
// 보조기기가 수치까지 읽을 수 있게 한다. 텍스트/표는 이미 차트가 그려진
// 컨테이너의 자식으로 들어가므로 시각적 렌더링에 영향을 주지 않는다(sr-only).
const CHART_A11Y_TABLE_CLASS = 'sr-only-chart-table';

function describeChart(container, label, options) {
  if (!container) return;
  // canvas 자체는 보조기기가 무시하도록 aria-hidden. 대체 텍스트는 컨테이너에.
  const canvas = container.querySelector('canvas, svg');
  if (canvas) canvas.setAttribute('aria-hidden', 'true');
  container.setAttribute('role', 'img');
  if (label) container.setAttribute('aria-label', label);

  // 핵심 데이터를 숨김 표로 제공(옵션). 이미 있으면 갱신.
  const rows = (options && options.rows) || null;
  let table = container.querySelector('.' + CHART_A11Y_TABLE_CLASS);
  if (!rows) {
    if (table) table.remove();
    return;
  }
  if (!table) {
    table = document.createElement('table');
    table.className = CHART_A11Y_TABLE_CLASS;
    container.appendChild(table);
  }
  // 헤더 + 본문을 안전하게 구성(모든 값을 텍스트 노드로).
  table.replaceChildren();
  if (options && options.caption) {
    const cap = document.createElement('caption');
    cap.textContent = options.caption;
    table.appendChild(cap);
  }
  if (options && options.headers && options.headers.length) {
    const thead = document.createElement('thead');
    const tr = document.createElement('tr');
    for (const h of options.headers) {
      const th = document.createElement('th');
      th.textContent = String(h ?? '');
      tr.appendChild(th);
    }
    thead.appendChild(tr);
    table.appendChild(thead);
  }
  const tbody = document.createElement('tbody');
  for (const row of rows) {
    const tr = document.createElement('tr');
    for (const cell of row) {
      const td = document.createElement('td');
      td.textContent = String(cell ?? '');
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
}
