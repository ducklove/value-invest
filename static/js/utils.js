const ANNUAL_CHART_KEYS = [
  '주가 (원)', '시가총액 (억원)', 'PER', 'PBR', 'ROE (%)', 'EPS (원)',
  '부채비율 (%)', '영업이익률 (%)', '배당수익률 (%)', '주당배당금 (원)'
];

const WEEKLY_CHART_KEYS = [
  '주간 주가', '주간 PER', '주간 PBR', '주간 배당수익률'
];

const CHART_COLORS = [
  '#e11d48','#2563eb','#7c3aed','#059669','#d97706',
  '#dc2626','#0891b2','#4f46e5','#c026d3'
];
const PER_DISPLAY_MAX = 100;

const GUEST_RECENT_KEY = 'guest_recent';
const GUEST_RECENT_MAX = 20;

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

function updateAnalyticsAuthState() {
  if (IS_LOCALHOST || typeof gtag !== 'function') return;
  gtag('set', 'user_properties', {
    login_state: currentUser ? 'logged_in' : 'guest',
  });
}
