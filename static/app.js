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

// --- WebSocket Quote Manager ---
const QuoteManager = {
  ws: null,
  connected: false,
  reconnectTimer: null,
  subscriptions: {},
  overflowCodes: [],
  overflowTimer: null,
  generalPollTimer: null,
  wsActive: false,      // true when this session owns the active WS slot
  onQuote: null,

  connect() {
    if (this.ws) return;
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws/quotes`;
    try { this.ws = new WebSocket(url); } catch { this._scheduleReconnect(); return; }
    this.ws.onopen = () => {
      this.connected = true;
      // Request takeover immediately (server will check if occupied)
      this.ws.send(JSON.stringify({ action: 'takeover' }));
    };
    this.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'quote' && msg.code && this.onQuote) this.onQuote(msg.code, msg);
        else if (msg.type === 'subscriptions') {
          this.overflowCodes = msg.rest || [];
          this._fetchInitialQuotes(msg.ws || []);
          this._startOverflowPolling();
        }
        else if (msg.type === 'ws_status') {
          if (msg.occupied && !msg.active) {
            // Another session has the WS — ask user
            this._promptTakeover();
          } else if (msg.active) {
            // We are now the active subscriber
            this.wsActive = true;
            this._sendSubscriptions();
          }
        }
        else if (msg.type === 'ws_taken_over') {
          // Another session took over — fall back to polling
          this.wsActive = false;
          this._showTakenOverBanner();
        }
      } catch {}
    };
    this.ws.onclose = (ev) => {
      this.connected = false;
      this.wsActive = false;
      this.ws = null;
      // Don't reconnect if we were kicked by takeover
      if (ev.code === 4001) return;
      this._scheduleReconnect();
    };
    this.ws.onerror = () => {};
    this._startGeneralPolling();
  },

  disconnect() {
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.overflowTimer) { clearInterval(this.overflowTimer); this.overflowTimer = null; }
    if (this.generalPollTimer) { clearInterval(this.generalPollTimer); this.generalPollTimer = null; }
    if (this.ws) { this.ws.close(); this.ws = null; }
    this.connected = false;
    this.wsActive = false;
  },

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => { this.reconnectTimer = null; this.connect(); }, 5000);
  },

  _promptTakeover() {
    if (confirm('다른 세션에서 실시간 시세를 사용 중입니다.\n이 세션에서 실시간 시세를 사용하시겠습니까?\n\n(취소 시 1분 간격 폴링으로 동작합니다)')) {
      if (this.connected && this.ws) {
        this.ws.send(JSON.stringify({ action: 'takeover' }));
      }
    }
    // If cancelled, keep polling-only mode (generalPollTimer handles it)
  },

  _showTakenOverBanner() {
    // Brief visual notification
    const banner = document.createElement('div');
    banner.textContent = '다른 세션이 실시간 시세를 가져갔습니다. 1분 간격 폴링으로 전환됩니다.';
    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;padding:10px;background:#e67e22;color:white;text-align:center;z-index:9999;font-size:13px;';
    document.body.appendChild(banner);
    setTimeout(() => banner.remove(), 5000);
  },

  updateSubscriptions(requested) {
    this.subscriptions = requested;
    this._sendSubscriptions();
  },

  _sendSubscriptions() {
    if (!this.connected || !this.ws || !this.wsActive) return;
    this.ws.send(JSON.stringify({ action: 'subscribe', requested: this.subscriptions }));
  },

  async _fetchQuotes(codes) {
    if (!codes.length) return;
    await Promise.allSettled(codes.map(async code => {
      const resp = await apiFetch(`/api/asset-quote/${code}`);
      if (!resp.ok) return;
      const q = await resp.json();
      if (this.onQuote) this.onQuote(code, { code, price: q.price, change: q.change, change_pct: q.change_pct, previous_close: q.previous_close, date: q.date });
    }));
  },

  async _fetchInitialQuotes(wsCodes) {
    const needsFetch = wsCodes.filter(code => {
      const pf = portfolioItems.find(i => i.stock_code === code);
      if (pf && pf.quote && pf.quote.price != null) return false;
      return true;
    });
    await this._fetchQuotes(needsFetch);
  },

  async _pollOverflow() {
    await this._fetchQuotes(this.overflowCodes);
  },

  _startOverflowPolling() {
    if (this.overflowTimer) clearInterval(this.overflowTimer);
    if (!this.overflowCodes.length) return;
    this._pollOverflow();
    this.overflowTimer = setInterval(() => this._pollOverflow(), 30_000);
  },

  // 60초 간격 전체 폴링 — WS 활성 여부와 무관하게 항상 동작
  _startGeneralPolling() {
    if (this.generalPollTimer) clearInterval(this.generalPollTimer);
    this.generalPollTimer = setInterval(() => this._pollAll(), 60_000);
  },

  async _pollAll() {
    // WS가 활성이면 WS 코드는 이미 실시간이므로 overflow만 폴링
    if (this.wsActive) return;
    // WS 비활성 — 모든 구독 코드를 REST로 폴링
    const allCodes = new Set();
    for (const codes of Object.values(this.subscriptions)) {
      for (const c of codes) allCodes.add(c);
    }
    if (allCodes.size) await this._fetchQuotes([...allCodes]);
  },
};

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
  } catch {}
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

function updateAnalyticsAuthState() {
  if (IS_LOCALHOST || typeof gtag !== 'function') return;
  gtag('set', 'user_properties', {
    login_state: currentUser ? 'logged_in' : 'guest',
  });
}

async function loadAuthConfig() {
  if (!hasApiConfiguration()) {
    authConfig = { enabled: false, googleClientId: '' };
    return authConfig;
  }

  try {
    const resp = await apiFetch('/api/auth/config');
    authConfig = await resp.json();
  } catch (error) {
    authConfig = { enabled: false, googleClientId: '' };
  }
  return authConfig;
}

async function loadCurrentUser() {
  if (!hasApiConfiguration()) {
    currentUser = null;
    return currentUser;
  }

  try {
    const resp = await apiFetch('/api/auth/me');
    if (!resp.ok) {
      currentUser = null;
      return currentUser;
    }
    const data = await resp.json();
    currentUser = data.user || null;
  } catch (error) {
    currentUser = null;
  }
  return currentUser;
}

async function syncAuthState(options = {}) {
  const { refreshRecentList = false, refreshPreference = false } = options;
  await loadCurrentUser();
  renderAuthState();
  if (refreshPreference) {
    await refreshActivePreference();
  }
  if (refreshRecentList) {
    await loadRecentList();
  }
}

function switchTab(tab) {
  activeTab = tab;
  document.querySelectorAll('.sidebar-tab').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tab);
  });
  loadRecentList();
}

function updateSidebarTabs() {
  const tabs = document.getElementById('sidebarTabs');
  if (!tabs) return;
  tabs.style.display = currentUser ? 'flex' : 'none';
}

function buildGoogleLoginUri() {
  return buildApiUrl('/api/auth/google/callback');
}

function buildLoginPageUrl() {
  const path = `/login?return_to=${encodeURIComponent(window.location.pathname + window.location.search + window.location.hash)}`;
  if (IS_GITHUB_PAGES_SITE && API_BASE_URL) {
    return `${API_BASE_URL.replace(/\/$/, '')}${path}`;
  }
  return path;
}

function normalizeUserPreference(preference) {
  return {
    is_starred: Boolean(preference?.is_starred),
    note: preference?.note || '',
    updated_at: preference?.updated_at || null,
  };
}

function setPreferenceStatus(message = '', tone = '') {
  const status = document.getElementById('preferenceStatus');
  if (!status) return;
  status.className = 'personalization-status';
  if (tone) {
    status.classList.add(tone);
  }
  status.textContent = message;
}

function renderUserPreference() {
  const panel = document.getElementById('personalizationPanel');
  const favoriteBtn = document.getElementById('favoriteBtn');
  const saveNoteBtn = document.getElementById('saveNoteBtn');
  const note = document.getElementById('preferenceNote');
  if (!panel || !favoriteBtn || !saveNoteBtn || !note) return;

  if (!activeStockCode) {
    panel.style.display = 'none';
    return;
  }

  panel.style.display = 'block';
  currentUserPreference = normalizeUserPreference(currentUserPreference);
  const isLoggedIn = Boolean(currentUser);

  favoriteBtn.classList.toggle('active', currentUserPreference.is_starred);
  favoriteBtn.textContent = currentUserPreference.is_starred ? '관심중' : '관심종목';
  favoriteBtn.disabled = !isLoggedIn || preferenceSaving;
  saveNoteBtn.disabled = !isLoggedIn || preferenceSaving;
  note.disabled = !isLoggedIn || preferenceSaving;
  note.value = currentUserPreference.note || '';

  if (!isLoggedIn) {
    setPreferenceStatus('로그인하면 관심종목과 개인 메모를 저장할 수 있습니다.', 'warning');
  } else if (!document.getElementById('preferenceStatus').textContent) {
    setPreferenceStatus('관심종목에 추가하면 관심 목록 탭에서 모아볼 수 있습니다.');
  }
}

async function refreshActivePreference() {
  if (!activeStockCode || !currentUser) {
    currentUserPreference = normalizeUserPreference(null);
    renderUserPreference();
    return;
  }

  try {
    const resp = await apiFetch(`/api/preferences/${activeStockCode}`);
    if (!resp.ok) {
      throw new Error('개인화 설정을 불러오지 못했습니다.');
    }
    const data = await resp.json();
    currentUserPreference = normalizeUserPreference(data.user_preference);
    setPreferenceStatus('');
  } catch (error) {
    currentUserPreference = normalizeUserPreference(null);
    setPreferenceStatus(error.message || '개인화 설정을 불러오지 못했습니다.', 'warning');
  }
  renderUserPreference();
}

async function saveUserPreference(changes, successMessage) {
  if (!currentUser || !activeStockCode || preferenceSaving) return;
  preferenceSaving = true;
  renderUserPreference();
  setPreferenceStatus('저장 중...');

  try {
    const resp = await apiFetch(`/api/preferences/${activeStockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(changes),
    });
    const data = await resp.json();
    if (!resp.ok) {
      throw new Error(data.detail || '개인화 설정을 저장하지 못했습니다.');
    }
    currentUserPreference = normalizeUserPreference(data.user_preference);
    renderUserPreference();
    setPreferenceStatus(successMessage, 'saved');
    loadRecentList();
  } catch (error) {
    renderUserPreference();
    setPreferenceStatus(error.message || '개인화 설정을 저장하지 못했습니다.', 'warning');
  } finally {
    preferenceSaving = false;
    renderUserPreference();
  }
}

async function toggleFavorite() {
  if (!currentUser) {
    renderUserPreference();
    return;
  }
  const nextValue = !normalizeUserPreference(currentUserPreference).is_starred;
  currentUserPreference = { ...normalizeUserPreference(currentUserPreference), is_starred: nextValue };
  renderUserPreference();
  await saveUserPreference(
    { is_starred: nextValue },
    nextValue ? '관심종목에 추가했습니다.' : '관심종목에서 제거했습니다.',
  );
}

async function autoStarCurrentStock() {
  if (!currentUser || !activeStockCode) return;
  try {
    const resp = await apiFetch(`/api/preferences/${activeStockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ is_starred: true }),
    });
    if (resp.ok) {
      const data = await resp.json();
      currentUserPreference = normalizeUserPreference(data.user_preference);
      renderUserPreference();
    }
  } catch {}
}

async function savePreferenceNote() {
  if (!currentUser) {
    renderUserPreference();
    return;
  }
  const note = document.getElementById('preferenceNote').value;
  currentUserPreference = { ...normalizeUserPreference(currentUserPreference), note };
  await saveUserPreference({ note }, '메모를 저장했습니다.');
}

function scheduleGoogleButtonRender() {
  if (googleButtonRetryTimer !== null) return;
  googleButtonRetryTimer = window.setTimeout(() => {
    googleButtonRetryTimer = null;
    googleButtonRetryCount += 1;
    renderGoogleButton();
  }, 300);
}

async function submitGoogleCredential(credential) {
  const resp = await apiFetch('/api/auth/google', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ credential }),
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(data.detail || 'Google 로그인에 실패했습니다.');
  }
  currentUser = data.user || null;
  return data;
}

async function handleGoogleCredentialResponse(response) {
  const credential = response?.credential;
  if (!credential) {
    alert('Google 로그인 토큰을 받지 못했습니다.');
    return;
  }

  try {
    await submitGoogleCredential(credential);
    renderAuthState();
    await refreshActivePreference();
    await loadRecentList();
    trackEvent('login_success', { provider: 'google', method: 'popup' });
  } catch (error) {
    trackEvent('login_error', { provider: 'google', reason: 'popup_callback' });
    alert(error.message || 'Google 로그인에 실패했습니다.');
  }
}

function renderGoogleButton() {
  const container = document.getElementById('googleSignInButton');
  if (!container) return;

  if (currentUser || !authConfig?.enabled || !authConfig?.googleClientId) {
    container.innerHTML = '';
    return;
  }

  if (IS_GITHUB_PAGES_SITE && API_BASE_URL) {
    container.innerHTML = `<a class="auth-login-link" href="${escapeHtml(buildLoginPageUrl())}">서버 버전에서 로그인</a>`;
    return;
  }

  if (!window.google?.accounts?.id) {
    if (googleButtonRetryCount >= 5) {
      container.innerHTML = `<a class="auth-login-link" href="${escapeHtml(buildLoginPageUrl())}">로그인 페이지에서 계속</a>`;
      return;
    }
    scheduleGoogleButtonRender();
    return;
  }

  if (!googleAuthInitialized) {
    window.google.accounts.id.initialize({
      client_id: authConfig.googleClientId,
      auto_select: false,
      callback: handleGoogleCredentialResponse,
    });
    googleAuthInitialized = true;
  }

  container.innerHTML = '';
  window.google.accounts.id.renderButton(container, {
    theme: 'outline',
    size: 'large',
    shape: 'pill',
    text: 'signin_with',
    width: 280,
    locale: 'ko',
  });
}

function renderAuthState() {
  const statusTitle = document.getElementById('authStatusTitle');
  const statusDetail = document.getElementById('authStatusDetail');
  const authUser = document.getElementById('authUser');
  const loginLink = document.getElementById('authLoginLink');
  const avatar = document.getElementById('authAvatar');
  const name = document.getElementById('authUserName');
  const email = document.getElementById('authUserEmail');

  updateSidebarTabs();
  updateAnalyticsAuthState();

  if (currentUser) {
    statusTitle.textContent = '내 계정으로 최근 분석을 저장 중입니다';
    statusDetail.textContent = '최근 검색, 관심종목, 개인 메모가 내 Google 계정 기준으로 저장됩니다.';
    authUser.style.display = 'grid';
    loginLink.style.display = 'none';
    avatar.src = currentUser.picture || 'data:image/gif;base64,R0lGODlhAQABAAAAACw=';
    name.textContent = currentUser.name || currentUser.email;
    email.textContent = currentUser.email || '';
  } else if (authConfig?.enabled) {
    statusTitle.textContent = '로그인해 최근 분석을 저장하세요';
    statusDetail.textContent = IS_GITHUB_PAGES_SITE
      ? 'GitHub Pages에서는 서버 버전으로 이동해 로그인한 뒤 개인화 기능을 사용할 수 있습니다.'
      : 'Google로 로그인하면 최근 검색, 관심종목, 개인 메모를 내 계정 기준으로 관리할 수 있습니다.';
    authUser.style.display = 'none';
    loginLink.href = buildLoginPageUrl();
    loginLink.style.display = 'inline-flex';
  } else {
    statusTitle.textContent = 'Google 로그인이 아직 설정되지 않았습니다';
    statusDetail.textContent = '서버 설정이 완료되면 계정별 최근 분석 저장을 사용할 수 있습니다.';
    authUser.style.display = 'none';
    loginLink.style.display = 'none';
  }

  renderGoogleButton();

  // Mobile: hide sidebar when logged in, show compact auth indicator
  const mobileAuth = document.getElementById('mobileAuthStatus');
  if (currentUser && window.innerWidth <= 900) {
    document.body.classList.add('mobile-auth');
    mobileAuth.style.display = 'flex';
    document.getElementById('mobileAuthAvatar').src = currentUser.picture || '';
    document.getElementById('mobileAuthName').textContent = currentUser.name || currentUser.email;
  } else {
    document.body.classList.remove('mobile-auth');
    mobileAuth.style.display = 'none';
  }
}

async function logout() {
  try {
    await apiFetch('/api/auth/logout', { method: 'POST' });
  } catch (error) {
  } finally {
    if (window.google?.accounts?.id) {
      window.google.accounts.id.disableAutoSelect();
    }
    currentUser = null;
    renderAuthState();
    refreshActivePreference();
    trackEvent('logout', { provider: 'google' });
    loadRecentList();
  }
}

function consumeAuthRedirectResult() {
  const url = new URL(window.location.href);
  const auth = url.searchParams.get('auth');
  const authError = url.searchParams.get('auth_error');
  if (!auth && !authError) return;

  url.searchParams.delete('auth');
  url.searchParams.delete('auth_error');
  window.history.replaceState({}, '', url.toString());

  if (auth === 'success') {
    trackEvent('login_success', { provider: 'google', method: 'redirect' });
    return;
  }

  trackEvent('login_error', { provider: 'google', reason: authError || 'unknown' });
  if (authError === 'csrf') {
    alert('Google 로그인 보안 검증에 실패했습니다. 다시 시도해 주세요.');
  } else if (authError === 'not_configured') {
    alert('서버 설정이 아직 완료되지 않았습니다.');
  } else {
    alert('Google 로그인에 실패했습니다.');
  }
}

async function initAuth() {
  await loadAuthConfig();
  await syncAuthState({ refreshPreference: true });
  consumeAuthRedirectResult();
}

// Theme
function toggleTheme() {
  const html = document.documentElement;
  const next = html.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  html.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
  Object.values(charts).forEach(c => c.update());
  trackEvent('theme_toggle', { theme: next });
}
(function initTheme() {
  const saved = localStorage.getItem('theme');
  if (saved) document.documentElement.setAttribute('data-theme', saved);
})();

// Search
const searchInput = document.getElementById('searchInput');
const dropdown = document.getElementById('dropdown');

function updateActiveItem() {
  const items = dropdown.querySelectorAll('.dropdown-item[data-stock]');
  items.forEach((el, i) => el.classList.toggle('active', i === selectedIdx));
  if (selectedIdx >= 0 && items[selectedIdx]) {
    items[selectedIdx].scrollIntoView({ block: 'nearest' });
  }
}

searchInput.addEventListener('input', () => {
  clearTimeout(searchTimeout);
  selectedIdx = -1;
  const q = searchInput.value.trim();
  if (q.length < 1) { dropdown.classList.remove('show'); return; }
  searchTimeout = setTimeout(() => doSearch(q), 250);
});

searchInput.addEventListener('keydown', (e) => {
  const items = dropdown.querySelectorAll('.dropdown-item[data-stock]');
  if (e.key === 'Escape') { dropdown.classList.remove('show'); selectedIdx = -1; return; }
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    if (items.length > 0) { selectedIdx = Math.min(selectedIdx + 1, items.length - 1); updateActiveItem(); }
    return;
  }
  if (e.key === 'ArrowUp') {
    e.preventDefault();
    if (items.length > 0) { selectedIdx = Math.max(selectedIdx - 1, 0); updateActiveItem(); }
    return;
  }
  if (e.key === 'Enter') {
    e.preventDefault();
    dropdown.classList.remove('show');
    if (selectedIdx >= 0 && items[selectedIdx]) {
      items[selectedIdx].click();
    } else if (items.length > 0) {
      items[0].click();
    } else {
      const q = searchInput.value.trim();
      if (q.length > 0) doSearchAndAnalyze(q);
    }
    selectedIdx = -1;
  }
});

document.addEventListener('click', (e) => {
  if (!e.target.closest('.search-container')) dropdown.classList.remove('show');
});

async function doSearchAndAnalyze(q) {
  try {
    requireApiConfiguration();
    const resp = await apiFetch(`/api/search?q=${encodeURIComponent(q)}`);
    const data = await resp.json();
    if (data.length > 0) {
      searchInput.value = data[0].corp_name;
      trackEvent('stock_select', { stock_code: data[0].stock_code, source: 'enter' });
      analyzeStock(data[0].stock_code);
    }
  } catch (error) {
    alert(error.message || '검색 중 오류가 발생했습니다.');
  }
}

async function doSearch(q) {
  try {
    requireApiConfiguration();
    const resp = await apiFetch(`/api/search?q=${encodeURIComponent(q)}`);
    const data = await resp.json();
    dropdown.innerHTML = '';
    if (data.length === 0) {
      dropdown.innerHTML = '<div class="dropdown-item" style="color:var(--text-secondary)">검색 결과 없음</div>';
    } else {
      data.forEach(item => {
        const div = document.createElement('div');
        div.className = 'dropdown-item';
        div.dataset.stock = item.stock_code;
        const name = document.createElement('span');
        name.textContent = item.corp_name;
        const code = document.createElement('span');
        code.style.color = 'var(--text-secondary)';
        code.textContent = item.stock_code;
        div.append(name, code);
        div.addEventListener('click', () => {
          dropdown.classList.remove('show');
          searchInput.value = item.corp_name;
          trackEvent('stock_select', { stock_code: item.stock_code, source: 'dropdown' });
          analyzeStock(item.stock_code);
        });
        dropdown.appendChild(div);
      });
    }
    trackEvent('search_results', { result_count: data.length });
    dropdown.classList.add('show');
  } catch (error) {
    dropdown.innerHTML = `<div class="dropdown-item" style="color:var(--text-secondary)">${escapeHtml(error.message || '검색 중 오류가 발생했습니다.')}</div>`;
    dropdown.classList.add('show');
  }
}

// Analyze
const STEP_LABELS = {
  start: '분석 시작',
  dart_start: 'DART 재무제표 수집',
  dart_fetch: 'DART 재무제표 조회',
  dart_done: 'DART 수집 완료',
  dart_error: 'DART 조회 실패',
  market_start: '시장 데이터 및 파생 지표 계산',
  market_done: '시장 데이터 수집 완료',
  market_error: '시장 데이터 조회 실패',
  saving: '캐시 저장',
  analyzing: '지표 계산',
};

function getSeriesCoverage(series) {
  const years = (series || [])
    .filter(item => item && item.value !== null && item.value !== undefined)
    .map(item => item.year)
    .filter(year => Number.isFinite(year));
  if (years.length === 0) return '';
  const startYear = Math.min(...years);
  const endYear = Math.max(...years);
  return startYear === endYear ? String(startYear) : `${startYear}-${endYear}`;
}

function getWeeklyDateCoverage(series) {
  const dates = (series || [])
    .filter(item => item && item.value !== null && item.value !== undefined && item.date)
    .map(item => item.date)
    .sort();
  if (dates.length === 0) return null;
  return {
    startDate: dates[0],
    endDate: dates[dates.length - 1],
  };
}

function formatWeeklySectionTitle(weeklyIndicators) {
  const coverage = getWeeklyDateCoverage(weeklyIndicators?.['주간 주가'] || weeklyIndicators?.['주간 PER'] || []);
  if (!coverage) return '주간 밸류에이션';
  return `주간 밸류에이션 (${coverage.startDate} ~ ${coverage.endDate})`;
}

function getLatestIndicatorValue(series) {
  const entries = (series || []).filter(item => item && item.value !== null && item.value !== undefined && Number.isFinite(Number(item.value)));
  if (entries.length === 0) return null;
  return Number(entries[entries.length - 1].value);
}

function formatMetricNumber(value, suffix = '') {
  return value === null || value === undefined || !Number.isFinite(value)
    ? 'N/A'
    : `${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${suffix}`;
}

function getLatestDerivedBps(indicators) {
  const priceByYear = new Map((indicators['주가 (원)'] || []).map(item => [item.year, Number(item.value)]));
  const pbrSeries = indicators['PBR'] || [];

  for (let index = pbrSeries.length - 1; index >= 0; index -= 1) {
    const item = pbrSeries[index];
    const pbr = Number(item?.value);
    const price = priceByYear.get(item?.year);
    if (Number.isFinite(price) && Number.isFinite(pbr) && pbr > 0) {
      return price / pbr;
    }
  }

  return null;
}

function getCurrentValuationMetrics(indicators, quoteSnapshot) {
  const currentPrice = Number(quoteSnapshot?.price);
  if (!Number.isFinite(currentPrice) || currentPrice <= 0) {
    return { per: null, pbr: null, dividendYield: null };
  }

  const latestEps = getLatestIndicatorValue(indicators['EPS (원)']);
  const latestDps = getLatestIndicatorValue(indicators['주당배당금 (원)']);
  const latestBps = getLatestDerivedBps(indicators);

  return {
    per: latestEps && latestEps > 0 ? currentPrice / latestEps : null,
    pbr: latestBps && latestBps > 0 ? currentPrice / latestBps : null,
    dividendYield: latestDps !== null ? (latestDps / currentPrice) * 100 : null,
  };
}

function renderCurrentValuationSummary(indicators, quoteSnapshot) {
  const metrics = getCurrentValuationMetrics(indicators, quoteSnapshot);
  return [
    { label: 'PER', value: formatMetricNumber(metrics.per) },
    { label: 'PBR', value: formatMetricNumber(metrics.pbr) },
    { label: '배당수익률', value: formatMetricNumber(metrics.dividendYield, '%') },
  ].map(item => (
    `<div class="valuation-card"><span class="valuation-label">${item.label}</span><span class="valuation-value">${item.value}</span></div>`
  )).join('');
}

function renderQuoteSnapshot(quoteSnapshot, indicators = activeIndicators) {
  const quoteSummary = document.getElementById('quoteSummary');
  const quotePrice = document.getElementById('quotePrice');
  const quoteChange = document.getElementById('quoteChange');
  const quoteDate = document.getElementById('quoteDate');
  const coverageNote = document.getElementById('coverageNote');
  const quote = quoteSnapshot || {};

  if (quote.price !== null && quote.price !== undefined) {
    quoteSummary.style.display = 'flex';
    quotePrice.textContent = `${Number(quote.price).toLocaleString()}원`;
    const change = Number(quote.change || 0);
    const changePct = quote.change_pct;
    const changePrefix = change > 0 ? '+' : '';
    quoteChange.textContent = changePct !== null && changePct !== undefined
      ? `${changePrefix}${change.toLocaleString()}원 (${changePrefix}${Number(changePct).toLocaleString()}%)`
      : '변동 정보 없음';
    quoteChange.className = 'quote-change';
    quoteChange.classList.add(change > 0 ? 'up' : change < 0 ? 'down' : 'flat');
    quoteDate.textContent = quote.date ? `${quote.date} 기준` : '';
  } else {
    quoteSummary.style.display = 'none';
    quotePrice.textContent = '';
    quoteChange.textContent = '';
    quoteDate.textContent = '';
  }

  coverageNote.innerHTML = renderCurrentValuationSummary(indicators || {}, quote);
}

function isPerChart(key) {
  return key === 'PER' || key === '주간 PER';
}

function normalizeChartValue(value) {
  return value === null || value === undefined ? null : Number(value);
}

function buildChartSeries(key, series) {
  const labelField = Object.prototype.hasOwnProperty.call(series[0], 'date') ? 'date' : 'year';
  const labels = series.map(item => item[labelField]);
  const rawValues = series.map(item => normalizeChartValue(item.value));
  let values = rawValues.slice();
  let note = '';
  let spanGaps = true;

  if (isPerChart(key)) {
    values = rawValues.map(value => {
      if (value === null || !Number.isFinite(value)) return null;
      if (value <= 0 || Math.abs(value) > PER_DISPLAY_MAX) return null;
      return value;
    });
    note = `표시 기준: 음수 PER와 ${PER_DISPLAY_MAX}배 초과 구간은 추세 왜곡을 막기 위해 제외합니다.`;
    spanGaps = false;
  }

  return { labelField, labels, values, rawValues, note, spanGaps };
}

function shouldUseZeroBaseline(values) {
  const validValues = (values || []).filter(value => value !== null && Number.isFinite(value));
  return validValues.length > 0 && validValues.every(value => value >= 0);
}

function formatWeeklyTickLabel(value) {
  if (typeof value !== 'string') return value;
  const match = value.match(/^(\d{4})-(\d{2})-\d{2}$/);
  if (!match) return value;
  return `${match[1].slice(-2)}.${match[2]}`;
}

function renderChartGrid(container, chartKeys, indicatorMap, gridColor, tickColor, prefix) {
  container.innerHTML = '';

  chartKeys.forEach((key, i) => {
    const series = indicatorMap[key] || [];
    if (series.length === 0) return;

    const { labelField, labels, values, rawValues, note, spanGaps } = buildChartSeries(key, series);
    const isWeeklyChart = labelField === 'date';
    const zeroBaseline = shouldUseZeroBaseline(values);

    const card = document.createElement('div');
    card.className = 'chart-card';
    card.innerHTML = `<h3>${key}</h3><div class="chart-canvas-wrap"><canvas id="${prefix}-chart-${i}"></canvas></div>`;
    if (note) {
      const noteEl = document.createElement('div');
      noteEl.className = 'chart-note';
      noteEl.textContent = note;
      card.appendChild(noteEl);
    }
    container.appendChild(card);

    const ctx = document.getElementById(`${prefix}-chart-${i}`).getContext('2d');
    charts[key] = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: key,
          data: values,
          borderColor: CHART_COLORS[i % CHART_COLORS.length],
          backgroundColor: CHART_COLORS[i % CHART_COLORS.length] + '22',
          borderWidth: 2.5,
          pointRadius: isWeeklyChart ? 0 : 4,
          pointHoverRadius: isWeeklyChart ? 4 : 6,
          pointHitRadius: isWeeklyChart ? 12 : 8,
          pointBackgroundColor: CHART_COLORS[i % CHART_COLORS.length],
          fill: !isWeeklyChart,
          tension: 0.3,
          spanGaps,
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
          mode: 'nearest',
          axis: 'xy',
          intersect: true,
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (ctx) => {
                const rawValue = rawValues[ctx.dataIndex];
                if (ctx.parsed.y === null) {
                  if (isPerChart(key) && rawValue !== null && Number.isFinite(rawValue)) {
                    return `${key}: 표시 제외 (${rawValue.toLocaleString()})`;
                  }
                  return 'N/A';
                }
                return `${key}: ${ctx.parsed.y.toLocaleString()}`;
              }
            }
          }
        },
        scales: {
          x: {
            grid: { color: gridColor },
            ticks: {
              color: tickColor,
              maxTicksLimit: labelField === 'date' ? 8 : 12,
              callback: (_, index) => labelField === 'date' ? formatWeeklyTickLabel(labels[index]) : labels[index],
            }
          },
          y: {
            min: zeroBaseline ? 0 : undefined,
            grid: { color: gridColor },
            ticks: { color: tickColor }
          }
        }
      }
    });
  });
}

function resetProgress() {
  document.getElementById('progressBar').style.width = '0%';
  document.getElementById('progressSteps').innerHTML = '';
  document.getElementById('loadingDetail').textContent = '';
}

function addStep(text, cls) {
  const steps = document.getElementById('progressSteps');
  const div = document.createElement('div');
  div.className = cls || '';
  div.textContent = text;
  steps.appendChild(div);
  steps.scrollTop = steps.scrollHeight;
}

function markLastStepDone() {
  const steps = document.getElementById('progressSteps');
  const active = steps.querySelector('.active:last-child');
  if (active) { active.classList.remove('active'); active.classList.add('done'); active.textContent = '\u2713 ' + active.textContent; }
}

function cancelAnalysis() {
  if (currentAbortController) {
    currentAbortController.abort();
    currentAbortController = null;
  }
}

async function analyzeStock(stockCode) {
  try {
    requireApiConfiguration();
  } catch (error) {
    alert(error.message);
    return;
  }

  // 이전 분석 진행중이면 취소
  if (currentAbortController) currentAbortController.abort();
  currentAbortController = new AbortController();
  const signal = currentAbortController.signal;

  const overlay = document.getElementById('loadingOverlay');
  const loadingText = document.getElementById('loadingText');
  const loadingDetail = document.getElementById('loadingDetail');
  const progressBar = document.getElementById('progressBar');
  const cancelBtn = document.getElementById('cancelBtn');

  overlay.classList.add('show');
  cancelBtn.style.display = 'inline-block';
  resetProgress();
  loadingText.textContent = '데이터를 분석하고 있습니다...';

  try {
    trackEvent('analysis_start', { stock_code: stockCode });
    const resp = await apiFetch(`/api/analyze/${stockCode}`, { signal });
    const contentType = resp.headers.get('content-type') || '';

    // 캐시 히트: 일반 JSON 응답
    if (contentType.includes('application/json')) {
      if (!resp.ok) {
        const err = await resp.json();
        alert(err.detail || '분석 실패');
        return;
      }
      const data = await resp.json();
      renderResult(data);
      if (!currentUser) saveGuestRecent(data.stock_code, data.corp_name);
      if (activeTab === 'starred' && currentUser && !data.user_preference?.is_starred) {
        await autoStarCurrentStock();
      }
      loadRecentList();
      return;
    }

    // SSE 스트리밍 응답
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let resultData = null;
    let lastDartStep = '';
    let eventType = '';

    // signal로 취소 시 reader도 정리
    signal.addEventListener('abort', () => { reader.cancel(); });

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const payload = JSON.parse(line.slice(6));

          if (eventType === 'progress') {
            const step = payload.step;
            loadingText.textContent = payload.message;

            if (step === 'dart_fetch') {
              const pct = Math.round((payload.current / payload.total) * 60);
              progressBar.style.width = pct + '%';
              loadingDetail.textContent = `${payload.current} / ${payload.total}`;
              if (lastDartStep !== 'dart_fetch') {
                markLastStepDone();
                addStep('DART 재무제표 연도별 조회 중...', 'active');
              }
              lastDartStep = 'dart_fetch';
            } else if (step === 'dart_done' || step === 'dart_error') {
              markLastStepDone();
              progressBar.style.width = '60%';
              loadingDetail.textContent = '';
              addStep(payload.message, step === 'dart_done' ? 'done' : '');
              lastDartStep = step;
            } else if (step === 'market_start') {
              markLastStepDone();
              progressBar.style.width = '65%';
              addStep(payload.message, 'active');
            } else if (step === 'market_done' || step === 'market_error') {
              markLastStepDone();
              progressBar.style.width = '85%';
              addStep(payload.message, step === 'market_done' ? 'done' : '');
            } else if (step === 'saving') {
              progressBar.style.width = '90%';
              addStep(payload.message, 'active');
            } else if (step === 'analyzing') {
              markLastStepDone();
              progressBar.style.width = '95%';
              addStep(payload.message, 'active');
            } else if (step === 'start') {
              addStep(payload.message, 'active');
            }
          } else if (eventType === 'result') {
            progressBar.style.width = '100%';
            markLastStepDone();
            addStep('분석 완료!', 'done');
            resultData = payload;
          } else if (eventType === 'error') {
            alert(payload.message || '분석 실패');
          }
          eventType = '';
        }
      }
    }

    // 스트림 종료 후 버퍼에 남은 데이터 처리
    if (buffer.trim()) {
      const remainingLines = buffer.split('\n');
      for (const line of remainingLines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const payload = JSON.parse(line.slice(6));
          if (eventType === 'result') {
            resultData = payload;
          }
          eventType = '';
        }
      }
    }

    if (resultData) {
      await new Promise(r => setTimeout(r, 300));
      renderResult(resultData);
      if (!currentUser) saveGuestRecent(resultData.stock_code, resultData.corp_name);
      if (activeTab === 'starred' && currentUser && !resultData.user_preference?.is_starred) {
        await autoStarCurrentStock();
      }
      loadRecentList();
    }
  } catch (e) {
    if (e.name === 'AbortError') {
      // 사용자가 취소함 - 무시
    } else {
      alert('서버 오류: ' + e.message);
    }
  } finally {
    currentAbortController = null;
    overlay.classList.remove('show');
  }
}

function renderResult(data) {
  // Company info
  const infoEl = document.getElementById('companyInfo');
  infoEl.style.display = 'block';
  activeStockCode = data.stock_code;
  activeIndicators = data.indicators || {};
  currentUserPreference = normalizeUserPreference(data.user_preference);
  document.getElementById('companyName').textContent = `${data.corp_name} (${data.stock_code})`;
  const cachedText = data.cached ? `캐시됨 (${new Date(data.analyzed_at).toLocaleDateString('ko-KR')})` : '신규 분석 완료';
  document.getElementById('companyMeta').textContent = cachedText;
  renderUserPreference();
  renderQuoteSnapshot(data.quote_snapshot || {}, activeIndicators);
  trackEvent('analysis_complete', { stock_code: data.stock_code, cached: String(Boolean(data.cached)) });

  // Load reports asynchronously
  loadReports(data.stock_code);

  // Hide empty state, show charts
  document.getElementById('emptyState').style.display = 'none';
  const weeklyTitle = document.getElementById('weeklySectionTitle');
  const weeklyGrid = document.getElementById('weeklyChartsGrid');
  const annualTitle = document.getElementById('annualSectionTitle');
  const grid = document.getElementById('chartsGrid');
  const hasWeeklyCharts = WEEKLY_CHART_KEYS.some(key => (data.weekly_indicators?.[key] || []).length > 0);
  weeklyTitle.textContent = formatWeeklySectionTitle(data.weekly_indicators || {});
  weeklyTitle.style.display = hasWeeklyCharts ? 'block' : 'none';
  weeklyGrid.style.display = hasWeeklyCharts ? 'grid' : 'none';
  annualTitle.style.display = 'block';
  grid.style.display = 'grid';
  weeklyGrid.innerHTML = '';
  grid.innerHTML = '';

  // Destroy existing charts
  Object.values(charts).forEach(c => c.destroy());
  charts = {};

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const gridColor = isDark ? 'rgba(148,163,184,0.15)' : 'rgba(0,0,0,0.06)';
  const tickColor = isDark ? '#94a3b8' : '#666';

  if (hasWeeklyCharts) {
    renderChartGrid(weeklyGrid, WEEKLY_CHART_KEYS, data.weekly_indicators || {}, gridColor, tickColor, 'weekly');
  }
  renderChartGrid(grid, ANNUAL_CHART_KEYS, data.indicators || {}, gridColor, tickColor, 'annual');
  _updateQuoteSubscriptions();
}

// Recent list
async function loadRecentList() {
  updateSidebarTabs();
  if (!hasApiConfiguration()) {
    document.getElementById('recentList').innerHTML = '<div style="color:var(--text-secondary);font-size:13px;">GitHub Pages에서는 API 서버 연결 후 최근 분석 목록을 불러옵니다.</div>';
    return;
  }

  if (recentListLoading) return;
  recentListLoading = true;

  try {
    const container = document.getElementById('recentList');
    const tab = currentUser ? activeTab : 'recent';

    if (currentUser) {
      const resp = await apiFetch(`/api/cache/list?include_quotes=true&tab=${tab}`);
      const data = await resp.json();
      recentListItems = Array.isArray(data) ? data.slice() : [];
    } else {
      recentListItems = getGuestRecent();
    }
    if (recentListItems.length === 0) {
      const emptyMsg = currentUser
        ? (tab === 'starred' ? '관심종목이 없습니다. 분석 화면에서 관심종목을 추가하세요.' : '최근 검색한 종목이 없습니다.')
        : '아직 분석한 종목이 없습니다.';
      container.innerHTML = `<div style="color:var(--text-secondary);font-size:13px;">${emptyMsg}</div>`;
      return;
    }
    container.innerHTML = '';
    recentListItems.forEach((item, index) => {
      const wrapper = document.createElement('div');
      wrapper.className = 'sidebar-item';
      wrapper.dataset.index = index;

      if (currentUser) {
        wrapper.draggable = true;
        wrapper.addEventListener('dragstart', (e) => {
          wrapper.classList.add('dragging');
          e.dataTransfer.effectAllowed = 'move';
          e.dataTransfer.setData('text/plain', String(index));
        });
        wrapper.addEventListener('dragend', () => {
          wrapper.classList.remove('dragging');
          container.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
        });
        wrapper.addEventListener('dragover', (e) => {
          e.preventDefault();
          e.dataTransfer.dropEffect = 'move';
          const dragging = container.querySelector('.dragging');
          if (dragging !== wrapper) wrapper.classList.add('drag-over');
        });
        wrapper.addEventListener('dragleave', () => {
          wrapper.classList.remove('drag-over');
        });
        wrapper.addEventListener('drop', (e) => {
          e.preventDefault();
          wrapper.classList.remove('drag-over');
          const fromIndex = parseInt(e.dataTransfer.getData('text/plain'), 10);
          const toIndex = parseInt(wrapper.dataset.index, 10);
          if (fromIndex !== toIndex && !isNaN(fromIndex) && !isNaN(toIndex)) {
            dropRecentItem(fromIndex, toIndex);
          }
        });
      }

      const info = document.createElement('div');
      info.className = 'info';
      info.addEventListener('click', () => { switchView('analysis'); analyzeStock(item.stock_code); });

      const name = document.createElement('div');
      name.className = 'name';
      name.textContent = item.corp_name;
      const nameRow = document.createElement('div');
      nameRow.className = 'name-row';
      nameRow.appendChild(name);

      const badges = document.createElement('div');
      badges.className = 'badges';
      if (item.is_starred && activeTab !== 'starred') {
        const badge = document.createElement('span');
        badge.className = 'sidebar-badge star';
        badge.textContent = '관심';
        badges.appendChild(badge);
      }
      if (item.note) {
        const badge = document.createElement('span');
        badge.className = 'sidebar-badge note';
        badge.textContent = '메모';
        badges.appendChild(badge);
      }
      if (badges.childElementCount > 0) {
        nameRow.appendChild(badges);
      }

      const quote = item.quote_snapshot || {};
      const quotePrice = document.createElement('div');
      quotePrice.className = 'quote-price';
      const quoteChange = document.createElement('div');
      quoteChange.className = 'quote-change';

      if (quote.price !== null && quote.price !== undefined) {
        quotePrice.textContent = Number(quote.price).toLocaleString();
        const change = Number(quote.change || 0);
        const changePct = quote.change_pct;
        const changeClass = change > 0 ? 'up' : change < 0 ? 'down' : 'flat';
        quoteChange.classList.add(changeClass);
        if (changePct !== null && changePct !== undefined) {
          quoteChange.textContent = `${change > 0 ? '+' : ''}${Number(changePct).toLocaleString()}%`;
        }
      }

      info.append(nameRow, quotePrice, quoteChange);
      wrapper.appendChild(info);

      const button = document.createElement('button');
      button.className = 'delete-btn';
      button.title = activeTab === 'starred' ? '관심 해제' : '삭제';
      button.innerHTML = '&times;';
      button.addEventListener('click', (event) => {
        event.stopPropagation();
        if (currentUser) {
          deleteCache(item.stock_code);
        } else {
          removeGuestRecent(item.stock_code);
          loadRecentList();
        }
      });
      wrapper.appendChild(button);
      container.appendChild(wrapper);
    });
  } catch (e) {
  } finally {
    recentListLoading = false;
    _updateQuoteSubscriptions();
  }
}

function refreshRecentList() {
  loadRecentList();
}

async function deleteCache(stockCode) {
  try {
    const resp = await apiFetch(`/api/cache/${stockCode}?tab=${activeTab}`, { method: 'DELETE' });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      throw new Error(data.detail || '삭제하지 못했습니다.');
    }
    loadRecentList();
  } catch (e) {
    alert(e.message || '삭제하지 못했습니다.');
  }
}

async function saveRecentOrder(stockCodes) {
  const resp = await apiFetch('/api/cache/order', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ stock_codes: stockCodes, tab: activeTab }),
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(data.detail || '순서를 저장하지 못했습니다.');
  }
}

async function moveRecentItem(index, delta) {
  if (!currentUser) return;
  const nextIndex = index + delta;
  if (nextIndex < 0 || nextIndex >= recentListItems.length) return;

  const nextItems = recentListItems.slice();
  const [moved] = nextItems.splice(index, 1);
  nextItems.splice(nextIndex, 0, moved);
  recentListItems = nextItems;

  try {
    await saveRecentOrder(nextItems.map(item => item.stock_code));
    await loadRecentList();
  } catch (error) {
    alert(error.message || '순서를 저장하지 못했습니다.');
    await loadRecentList();
  }
}

async function dropRecentItem(fromIndex, toIndex) {
  if (!currentUser) return;
  const nextItems = recentListItems.slice();
  const [moved] = nextItems.splice(fromIndex, 1);
  nextItems.splice(toIndex, 0, moved);
  recentListItems = nextItems;

  try {
    await saveRecentOrder(nextItems.map(item => item.stock_code));
    await loadRecentList();
  } catch (error) {
    alert(error.message || '순서를 저장하지 못했습니다.');
    await loadRecentList();
  }
}

// Reports
let allReports = [];
let reportDisplayCount = 20;
let reportsRequestId = 0;

function getRecommBadge(recomm) {
  if (!recomm) return '';
  const r = recomm.toUpperCase();
  if (r.includes('BUY') || r.includes('매수') || r.includes('강력')) return `<span class="badge badge-buy">${recomm}</span>`;
  if (r.includes('SELL') || r.includes('매도') || r.includes('비중축소')) return `<span class="badge badge-sell">${recomm}</span>`;
  return `<span class="badge badge-hold">${recomm}</span>`;
}

function renderReportsTable(reports, limit) {
  const tbody = document.getElementById('reportsBody');
  const slice = reports.slice(0, limit);
  tbody.innerHTML = slice.map(r => {
    const safeTitle = escapeHtml(r.title);
    const safeSummary = r.summary ? `<div class="report-summary">${escapeHtml(r.summary)}</div>` : '';
    const safeAnalyst = escapeHtml(r.analyst);
    const safeFirm = escapeHtml(r.firm_short || r.firm);
    const safeDate = escapeHtml(r.date);
    const safeHref = safeExternalUrl(buildReportPdfUrl(r.pdf_url) || r.source_url);
    const titleLink = safeHref
      ? `<a href="${safeHref}" target="_blank" rel="noopener noreferrer">${safeTitle}</a>`
      : safeTitle;
    const safeReadHref = safeExternalUrl(r.source_url);
    const linkNote = safeReadHref
      ? `<div class="report-link-note"><a href="${safeReadHref}" target="_blank" rel="noopener noreferrer">네이버 리서치 본문 페이지</a></div>`
      : '';
    const targetPrc = r.target_price ? Number(r.target_price.replace(/,/g, '')).toLocaleString() + '원' : '-';
    return `<tr>
      <td class="report-date">${safeDate}</td>
      <td class="report-firm">${safeFirm}</td>
      <td class="report-title">${titleLink}${safeSummary}${linkNote}<div style="font-size:11px;color:var(--text-secondary);margin-top:2px;">${safeAnalyst}</div></td>
      <td>${getRecommBadge(escapeHtml(r.recommendation))}</td>
      <td class="report-target">${escapeHtml(targetPrc)}</td>
    </tr>`;
  }).join('');

  const moreBtn = document.getElementById('reportsMore');
  moreBtn.style.display = reports.length > limit ? 'block' : 'none';
  moreBtn.textContent = `더 보기 (${limit}/${reports.length}건)`;
}

function showMoreReports() {
  reportDisplayCount += 20;
  renderReportsTable(allReports, reportDisplayCount);
}

async function loadReports(stockCode) {
  const section = document.getElementById('reportsSection');
  const loading = document.getElementById('reportsLoading');
  const table = document.getElementById('reportsTable');
  const countEl = document.getElementById('reportCount');
  const requestId = ++reportsRequestId;

  section.style.display = 'block';
  loading.style.display = 'block';
  loading.textContent = '최신 리포트를 불러오는 중...';
  table.style.display = 'none';
  countEl.textContent = '';
  allReports = [];
  reportDisplayCount = 20;

  if (!hasApiConfiguration()) {
    loading.style.display = 'block';
    loading.textContent = 'GitHub Pages에서는 API 서버 연결 후 리포트를 불러올 수 있습니다.';
    return;
  }

  const localCache = loadReportCache(stockCode);
  let latestReport = localCache?.latestReport || null;
  let renderedFromCache = false;

  if (Array.isArray(localCache?.reports) && localCache.reports.length > 0) {
    allReports = localCache.reports;
    countEl.textContent = `(최근 3년, ${allReports.length}건 · 캐시)`;
    table.style.display = 'table';
    loading.style.display = 'none';
    renderReportsTable(allReports, reportDisplayCount);
    renderedFromCache = true;
  } else if (latestReport) {
    allReports = [latestReport];
    countEl.textContent = '(최신 1건 · 캐시)';
    table.style.display = 'table';
    loading.style.display = 'none';
    renderReportsTable(allReports, 1);
    renderedFromCache = true;
  }

  try {
    const latestResp = await apiFetch(`/api/reports/${stockCode}/latest?refresh=1`);
    const latestData = await latestResp.json();
    if (requestId !== reportsRequestId) return;
    const networkLatest = latestData.report || null;
    const latestChanged = latestData.changed ?? !sameReport(latestReport, networkLatest);

    if (networkLatest) {
      latestReport = networkLatest;
      saveReportCache(stockCode, {
        latestReport,
        reports: Array.isArray(localCache?.reports) ? localCache.reports : [],
      });
    }

    if (networkLatest && (!renderedFromCache || latestChanged)) {
      allReports = [latestReport];
      loading.style.display = 'none';
      countEl.textContent = latestChanged ? '(최신 1건 갱신됨)' : '(최신 1건 확인됨)';
      table.style.display = 'table';
      renderReportsTable(allReports, 1);
    }
  } catch (e) {}

  try {
    const shouldRefreshFullReports =
      !Array.isArray(localCache?.reports) ||
      localCache.reports.length === 0 ||
      !sameReport(localCache.latestReport, latestReport);

    if (!shouldRefreshFullReports) {
      return;
    }

    const resp = await apiFetch(`/api/reports/${stockCode}`);
    const data = await resp.json();
    if (requestId !== reportsRequestId) return;

    allReports = data.reports || [];
    loading.style.display = 'none';

    if (allReports.length === 0) {
      if (latestReport) {
        countEl.textContent = '(최신 1건만 표시)';
        table.style.display = 'table';
        renderReportsTable([latestReport], 1);
      } else {
        loading.style.display = 'block';
        loading.textContent = '증권사 리포트가 없습니다.';
      }
      return;
    }

    countEl.textContent = `(최근 3년, ${allReports.length}건)`;
    table.style.display = 'table';
    renderReportsTable(allReports, reportDisplayCount);
    saveReportCache(stockCode, { latestReport: allReports[0] || latestReport, reports: allReports });
  } catch (e) {
    if (requestId !== reportsRequestId) return;
    if (latestReport) {
      loading.style.display = 'none';
      countEl.textContent = '(최신 1건만 표시)';
      table.style.display = 'table';
      renderReportsTable([latestReport], 1);
    } else {
      loading.textContent = '리포트를 불러오지 못했습니다.';
    }
  }
}

// --- Portfolio ---
let activeView = 'analysis';
let portfolioItems = [];
let portfolioLoading = false;
let pfSearchTimeout = null;
let pfEditingCode = null;
let pfSortKey = 'marketValue';
let pfSortAsc = false;
let pfGroups = [];        // [{group_name, sort_order, is_default}, ...]
let pfGroupFilter = null; // null = all selected, Set of group_names = filtered
let pfGroupSort = true;   // independent group sort toggle
let pfBenchmarkQuotes = {}; // benchmark_code -> {change_pct, name}
let pfMonthEndValue = null; // total_value at end of previous month
const PF_QUOTE_REFRESH_MS = 60_000;

function switchView(view) {
  activeView = view;
  document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.view === view));
  const analysisView = document.getElementById('analysisView');
  const portfolioView = document.getElementById('portfolioView');
  analysisView.style.display = view === 'analysis' ? 'block' : 'none';
  portfolioView.style.display = view === 'portfolio' ? 'block' : 'none';
  const activeEl = view === 'analysis' ? analysisView : portfolioView;
  activeEl.classList.remove('fade-in');
  void activeEl.offsetWidth;
  activeEl.classList.add('fade-in');
  if (view === 'portfolio') {
    loadPortfolio();
  }
  _updateQuoteSubscriptions();
}

async function loadPortfolio() {
  if (portfolioLoading) return;
  portfolioLoading = true;
  try {
    const resp = await apiFetch('/api/portfolio');
    if (!resp.ok) {
      if (resp.status === 401) {
        document.getElementById('pfEmpty').textContent = '로그인이 필요합니다.';
        document.getElementById('pfEmpty').style.display = 'block';
        document.getElementById('pfTable').style.display = 'none';
        return;
      }
      return;
    }
    const freshItems = await resp.json();
    // Load groups (fast), restore cached benchmark names from localStorage
    try {
      const gResp = await apiFetch('/api/portfolio/groups');
      if (gResp.ok) pfGroups = await gResp.json();
    } catch {}
    // Restore benchmark names from localStorage cache for instant display
    try {
      const cached = JSON.parse(localStorage.getItem('pfBenchmarkNames') || '{}');
      for (const [k, v] of Object.entries(cached)) {
        pfBenchmarkQuotes[k] = { ...(pfBenchmarkQuotes[k] || {}), name: v };
      }
    } catch {}
    // Fetch benchmark quotes in background (don't block initial render)
    apiFetch('/api/portfolio/month-end-value').then(async r => {
      if (!r.ok) return;
      const snap = await r.json();
      pfMonthEndValue = snap.total_value ?? null;
      renderPortfolio();
    }).catch(() => {});
    apiFetch('/api/portfolio/benchmark-quotes').then(async r => {
      if (!r.ok) return;
      const fresh = await r.json();
      for (const [k, v] of Object.entries(fresh)) pfBenchmarkQuotes[k] = v;
      // Save names to localStorage
      const names = {};
      for (const [k, v] of Object.entries(pfBenchmarkQuotes)) { if (v.name) names[k] = v.name; }
      try { localStorage.setItem('pfBenchmarkNames', JSON.stringify(names)); } catch {}
      renderPortfolio();
    }).catch(() => {});
    // Preserve existing quotes from previous load
    const prevQuotes = {};
    portfolioItems.forEach(i => { if (i.quote && i.quote.price != null) prevQuotes[i.stock_code] = i.quote; });
    portfolioItems = freshItems.map(item => {
      if (!item.quote || item.quote.price == null) item.quote = prevQuotes[item.stock_code] || item.quote;
      return item;
    });
    renderPortfolio();
    _updateQuoteSubscriptions();
  } catch {} finally {
    portfolioLoading = false;
  }
}

function pfSort(key) {
  if (key === 'group') {
    pfGroupSort = !pfGroupSort;
  } else if (pfSortKey === key) {
    if (!pfSortAsc) {
      pfSortKey = null;
      pfSortAsc = true;
    } else {
      pfSortAsc = false;
    }
  } else {
    pfSortKey = key;
    pfSortAsc = key === 'name';
  }
  renderPortfolio();
}

function pfGetGroup(item) {
  return item.group_name || '기타';
}

function pfToggleGroupFilter(groupName) {
  if (pfGroupFilter === null) {
    pfGroupFilter = new Set([groupName]);
  } else if (pfGroupFilter.has(groupName)) {
    pfGroupFilter.delete(groupName);
    if (pfGroupFilter.size === 0) pfGroupFilter = null;
  } else {
    pfGroupFilter.add(groupName);
    if (pfGroups.length && pfGroupFilter.size === pfGroups.length) pfGroupFilter = null;
  }
  renderPortfolio();
}

function renderPortfolio() {
  const tbody = document.getElementById('pfBody');
  const tfoot = document.getElementById('pfFoot');
  const summary = document.getElementById('pfSummary');
  const table = document.getElementById('pfTable');
  const empty = document.getElementById('pfEmpty');

  // Show/hide filter bar and update counts
  const filterBar = document.getElementById('pfFilterBar');
  if (filterBar) {
    filterBar.style.display = portfolioItems.length ? 'flex' : 'none';
    if (portfolioItems.length && pfGroups.length) {
      const counts = {};
      pfGroups.forEach(g => counts[g.group_name] = 0);
      portfolioItems.forEach(i => {
        const gn = pfGetGroup(i);
        if (counts[gn] !== undefined) counts[gn]++;
        else counts[gn] = 1;
      });
      filterBar.innerHTML = pfGroups.map(g => {
        const active = pfGroupFilter === null || pfGroupFilter.has(g.group_name);
        return `<button class="pf-filter-btn${active ? ' active' : ''}" onclick="pfToggleGroupFilter('${escapeHtml(g.group_name)}')">${escapeHtml(g.group_name)} (${counts[g.group_name] || 0})</button>`;
      }).join('') + `<button class="pf-filter-btn pf-group-manage-btn" onclick="openGroupModal()" title="그룹 관리">\u2699</button>`;
    }
  }

  if (!portfolioItems.length) {
    table.style.display = 'none';
    empty.style.display = 'block';
    empty.textContent = '포트폴리오가 비어 있습니다. 위 검색창에서 종목을 추가하세요.';
    summary.innerHTML = '';
    return;
  }

  const allRows = portfolioItems.map(item => {
    const q = item.quote || {};
    const cur = item.currency || 'KRW';
    const price = q.price ?? null;
    const change = q.change ?? 0;
    const changePct = q.change_pct ?? null;
    const qty = item.quantity;
    const avgPrice = item.avg_price; // already in KRW
    const invested = qty * avgPrice;
    const marketValue = price !== null ? qty * price : null;
    const rawReturn = avgPrice > 0 && price !== null ? ((price - avgPrice) / avgPrice * 100) : null;
    const returnPct = rawReturn !== null && qty < 0 ? -rawReturn : rawReturn;
    const dailyPnl = price !== null ? qty * change : 0;
    return { ...item, cur, price, change, changePct, qty, avgPrice, invested, marketValue, returnPct, dailyPnl };
  });

  // Total market value across ALL items (for weight calculation)
  let grandTotalMarketValue = 0;
  allRows.forEach(r => { if (r.marketValue !== null) grandTotalMarketValue += r.marketValue; });

  // Apply group filter
  const rows = pfGroupFilter === null ? allRows : allRows.filter(r => pfGroupFilter.has(pfGetGroup(r)));

  if (!rows.length) {
    table.style.display = 'none';
    empty.style.display = 'block';
    empty.textContent = '해당 분류의 종목이 없습니다.';
    summary.innerHTML = '';
    return;
  }
  table.style.display = 'table';
  empty.style.display = 'none';

  let totalInvested = 0, totalMarketValue = 0, totalDailyPnl = 0;
  rows.forEach(r => {
    totalInvested += r.invested;
    if (r.marketValue !== null) totalMarketValue += r.marketValue;
    totalDailyPnl += r.dailyPnl;
  });

  // Sort rows: group sort (primary, if on) + column sort (secondary)
  if (pfGroupSort || pfSortKey) {
    const grpOrder = {};
    if (pfGroupSort) pfGroups.forEach((g, i) => grpOrder[g.group_name] = i);
    rows.sort((a, b) => {
      // Primary: group sort
      if (pfGroupSort) {
        const ga = grpOrder[pfGetGroup(a)] ?? 999;
        const gb = grpOrder[pfGetGroup(b)] ?? 999;
        if (ga !== gb) return ga - gb;
      }
      // Secondary: column sort
      if (pfSortKey) {
        let va, vb;
        if (pfSortKey === 'name') {
          va = a.stock_name; vb = b.stock_name;
          return pfSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
        }
        va = a[pfSortKey] ?? -Infinity;
        vb = b[pfSortKey] ?? -Infinity;
        return pfSortAsc ? va - vb : vb - va;
      }
      return 0;
    });
  }
  if (pfEditingCode) {
    const idx = rows.findIndex(r => r.stock_code === pfEditingCode);
    if (idx > 0) {
      const [editing] = rows.splice(idx, 1);
      rows.unshift(editing);
    }
  }

  // Update sort arrows in header
  document.querySelectorAll('.pf-sortable').forEach(th => {
    const key = th.dataset.sort;
    const existing = th.querySelector('.pf-sort-arrow');
    if (existing) existing.remove();
    const isActive = key === 'group' ? pfGroupSort : pfSortKey === key;
    if (isActive) {
      const arrow = document.createElement('span');
      arrow.className = 'pf-sort-arrow';
      arrow.textContent = key === 'group' ? ' \u25BC' : (pfSortAsc ? ' \u25B2' : ' \u25BC');
      th.appendChild(arrow);
    }
  });

  const totalReturnPct = totalInvested > 0 ? ((totalMarketValue - totalInvested) / totalInvested * 100) : 0;
  const prevTotalValue = totalMarketValue - totalDailyPnl;
  const dailyReturnPct = prevTotalValue > 0 ? (totalDailyPnl / prevTotalValue * 100) : 0;

  // Monthly return (vs end of previous month)
  const monthlyReturnPct = pfMonthEndValue && pfMonthEndValue > 0
    ? ((totalMarketValue - pfMonthEndValue) / pfMonthEndValue * 100) : null;
  const monthlyPnl = pfMonthEndValue != null ? totalMarketValue - pfMonthEndValue : null;

  // Summary cards
  summary.innerHTML = `
    <div class="pf-summary-card">
      <div class="pf-summary-label">총 평가금액</div>
      <div class="pf-summary-value">${fmtKrw(totalMarketValue)}</div>
      <div class="pf-summary-sub">투자 ${fmtKrw(totalInvested)}</div>
    </div>
    <div class="pf-summary-card">
      <div class="pf-summary-label">총 수익률</div>
      <div class="pf-summary-value ${returnClass(totalReturnPct)}">${fmtPct(totalReturnPct)}</div>
      <div class="pf-summary-sub ${returnClass(totalMarketValue - totalInvested)}">${fmtSignedKrw(totalMarketValue - totalInvested)}</div>
    </div>
    <div class="pf-summary-card">
      <div class="pf-summary-label">월간 수익률</div>
      <div class="pf-summary-value ${returnClass(monthlyReturnPct)}">${monthlyReturnPct !== null ? fmtPct(monthlyReturnPct) : '-'}</div>
      <div class="pf-summary-sub ${returnClass(monthlyPnl)}">${monthlyPnl !== null ? fmtSignedKrw(monthlyPnl) : '-'}</div>
    </div>
    <div class="pf-summary-card">
      <div class="pf-summary-label">일간 수익률</div>
      <div class="pf-summary-value ${returnClass(dailyReturnPct)}">${fmtPct(dailyReturnPct)}</div>
      <div class="pf-summary-sub ${returnClass(totalDailyPnl)}">${fmtSignedKrw(totalDailyPnl)}</div>
    </div>`;

  // Table body
  tbody.innerHTML = rows.map((r, i) => {
    const weight = grandTotalMarketValue > 0 && r.marketValue !== null ? (r.marketValue / grandTotalMarketValue * 100) : 0;
    const isEditing = pfEditingCode === r.stock_code;
    const isCash = r.stock_code.startsWith('CASH_');
    const isSpecialFloat = ['KRX_GOLD', 'CRYPTO_BTC', 'CRYPTO_ETH'].includes(r.stock_code) || isCash;
    const curTag = r.stock_code === 'KRX_GOLD' ? ' <span class="pf-stock-code">원/g</span>' : r.cur !== 'KRW' ? ` <span class="pf-stock-code">${r.cur}</span>` : '';
    const qtyStep = isSpecialFloat ? 'any' : '1';
    const qtyDecimals = r.stock_code === 'KRX_GOLD' ? 2 : isCash ? 2 : 8;
    const fmtQty = isSpecialFloat ? (v => v !== null && v !== undefined ? Number(v).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: qtyDecimals}) : '-') : fmtNum;

    const groupOpts = pfGroups.map(g => `<option value="${escapeHtml(g.group_name)}"${g.group_name === pfGetGroup(r) ? ' selected' : ''}>${escapeHtml(g.group_name)}</option>`).join('');

    if (isEditing) {
      return `<tr data-code="${r.stock_code}">
        <td><a href="#" class="pf-stock-link" onclick="pfGoAnalyze('${r.stock_code}',event);return false;"><strong>${escapeHtml(r.stock_name)}</strong></a> <span class="pf-stock-code">${r.stock_code}</span>${curTag}</td>
        <td class="pf-col-group"><select class="pf-group-select" onchange="pfChangeGroup('${r.stock_code}', this.value)">${groupOpts}</select></td>
        <td class="pf-col-num">${fmtChangePct(r.changePct, r.change)}</td>
        <td class="pf-col-num pf-col-benchmark">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
        <td class="pf-col-num pf-col-buyprice"><input class="pf-edit-input" id="pfEditPrice" value="${r.avgPrice}" type="number" step="1"></td>
        <td class="pf-col-num">${r.price !== null ? fmtNum(r.price) : '-'}</td>
        <td class="pf-col-num"><input class="pf-edit-input" id="pfEditQty" value="${r.qty}" type="number" step="${qtyStep}"></td>
        <td class="pf-col-num"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
        <td class="pf-col-num">${r.marketValue !== null ? fmtNum(Math.round(r.marketValue)) : '-'}</td>
        <td class="pf-col-num pf-col-weight">${fmtPct(weight)}</td>
        <td class="pf-col-act"><div class="pf-row-actions">
          <button class="pf-row-btn save" onclick="savePortfolioEdit('${r.stock_code}','${escapeHtml(r.stock_name)}')" title="저장">✓</button>
          <button class="pf-row-btn cancel" onclick="cancelPortfolioEdit()" title="취소">✕</button>
        </div></td>
      </tr>`;
    }
    return `<tr draggable="true" data-code="${r.stock_code}">
      <td><a href="#" class="pf-stock-link" onclick="pfGoAnalyze('${r.stock_code}',event);return false;"><strong>${escapeHtml(r.stock_name)}</strong></a> <span class="pf-stock-code">${r.stock_code}</span>${curTag}</td>
      <td class="pf-col-group"><select class="pf-group-select" onchange="pfChangeGroup('${r.stock_code}', this.value)">${groupOpts}</select></td>
      <td class="pf-col-num">${fmtChangePct(r.changePct, r.change)}</td>
      <td class="pf-col-num pf-col-benchmark" onclick="pfShowBenchmarkPicker('${r.stock_code}', this)">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
      <td class="pf-col-num pf-col-buyprice">${fmtNum(r.avgPrice)}</td>
      <td class="pf-col-num">${r.price !== null ? fmtNum(r.price) : '-'}</td>
      <td class="pf-col-num">${fmtQty(r.qty)}</td>
      <td class="pf-col-num"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
      <td class="pf-col-num">${r.marketValue !== null ? fmtNum(Math.round(r.marketValue)) : '-'}</td>
      <td class="pf-col-num pf-col-weight">${fmtPct(weight)}</td>
      <td class="pf-col-act"><div class="pf-row-actions">
        <button class="pf-row-btn edit" onclick="startPortfolioEdit('${r.stock_code}')" title="편집">✎</button>
        <button class="pf-row-btn delete" onclick="deletePortfolioItem('${r.stock_code}')" title="삭제">✕</button>
      </div></td>
    </tr>`;
  }).join('');

  // Footer
  tfoot.innerHTML = `<tr>
    <td>합계</td>
    <td class="pf-col-group"></td>
    <td class="pf-col-num">${fmtChangePct(dailyReturnPct, totalDailyPnl)}</td>
    <td class="pf-col-benchmark"></td>
    <td class="pf-col-num pf-col-buyprice">${fmtNum(Math.round(totalInvested))}</td>
    <td></td>
    <td></td>
    <td class="pf-col-num"><span class="pf-return ${returnClass(totalReturnPct)}">${fmtPct(totalReturnPct)}</span></td>
    <td class="pf-col-num">${fmtNum(Math.round(totalMarketValue))}</td>
    <td class="pf-col-num pf-col-weight">${fmtPct(grandTotalMarketValue > 0 ? totalMarketValue / grandTotalMarketValue * 100 : 0)}</td>
    <td class="pf-col-act"></td>
  </tr>`;

  // Drag-and-drop on rows (manual order only)
  if (!pfSortKey && !pfGroupSort && currentUser) {
    tbody.querySelectorAll('tr[draggable]').forEach(tr => {
      tr.addEventListener('dragstart', (e) => {
        tr.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', tr.dataset.code);
      });
      tr.addEventListener('dragend', () => {
        tr.classList.remove('dragging');
        tbody.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
      });
      tr.addEventListener('dragover', (e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        if (!tr.classList.contains('dragging')) tr.classList.add('drag-over');
      });
      tr.addEventListener('dragleave', () => tr.classList.remove('drag-over'));
      tr.addEventListener('drop', (e) => {
        e.preventDefault();
        tr.classList.remove('drag-over');
        const fromCode = e.dataTransfer.getData('text/plain');
        const toCode = tr.dataset.code;
        if (fromCode && toCode && fromCode !== toCode) pfDropRow(fromCode, toCode);
      });
    });
  } else {
    tbody.querySelectorAll('tr[draggable]').forEach(tr => tr.removeAttribute('draggable'));
  }
}

function returnClass(val) {
  if (val === null || val === undefined) return '';
  return val > 0 ? 'pf-return positive' : val < 0 ? 'pf-return negative' : '';
}
function fmtNum(n) { return n !== null && n !== undefined ? Number(n).toLocaleString() : '-'; }
function fmtKrw(n) { return n !== null ? Number(Math.round(n)).toLocaleString() : '-'; }
function fmtSignedKrw(n) {
  if (n === null) return '-';
  const r = Math.round(n);
  return (r > 0 ? '+' : '') + r.toLocaleString();
}
function fmtPct(n) {
  if (n === null || n === undefined) return '-';
  return (n > 0 ? '+' : '') + n.toFixed(2) + '%';
}
const _BENCHMARK_PRESETS = [
  {code: 'IDX_KOSPI', name: '코스피'},
  {code: 'IDX_KOSDAQ', name: '코스닥'},
  {code: 'IDX_SP500', name: 'S&P500'},
  {code: 'FX_USDKRW', name: 'USD/KRW'},
];

function fmtBenchmarkPct(benchmarkCode) {
  if (!benchmarkCode) return '<span class="pf-benchmark-val">-</span>';
  const bq = pfBenchmarkQuotes[benchmarkCode];
  // For stock benchmarks (e.g., common stock for preferred), check regular quote cache
  if (!bq && benchmarkCode.length === 6) {
    const item = portfolioItems.find(i => i.stock_code === benchmarkCode);
    if (item && item.quote) {
      const pct = item.quote.change_pct;
      if (pct !== null && pct !== undefined) {
        const cls = pct > 0 ? 'positive' : pct < 0 ? 'negative' : '';
        return `<span class="pf-benchmark-val pf-return ${cls}">${fmtPct(pct)}</span>`;
      }
    }
  }
  if (!bq || bq.change_pct === null || bq.change_pct === undefined) return '<span class="pf-benchmark-val">-</span>';
  const pct = bq.change_pct;
  const cls = pct > 0 ? 'positive' : pct < 0 ? 'negative' : '';
  return `<span class="pf-benchmark-val pf-return ${cls}">${fmtPct(pct)}</span>`;
}

function benchmarkName(code) {
  if (!code) return '';
  let name;
  // Check if benchmark-quotes API returned a name
  const bq = pfBenchmarkQuotes[code];
  if (bq && bq.name && bq.name !== code) name = bq.name;
  else {
    const preset = _BENCHMARK_PRESETS.find(p => p.code === code);
    if (preset) name = preset.name;
    else {
      const item = portfolioItems.find(i => i.stock_code === code);
      name = item ? item.stock_name : code;
    }
  }
  // Truncate to 5 characters for display
  return name.length > 5 ? name.slice(0, 5) + '..' : name;
}

function fmtChangePct(pct, change) {
  if (pct === null || pct === undefined) return '-';
  const cls = change > 0 ? 'positive' : change < 0 ? 'negative' : '';
  return `<span class="pf-return ${cls}">${fmtPct(pct)}</span>`;
}

async function pfDropRow(fromCode, toCode) {
  const fromIdx = portfolioItems.findIndex(i => i.stock_code === fromCode);
  const toIdx = portfolioItems.findIndex(i => i.stock_code === toCode);
  if (fromIdx < 0 || toIdx < 0) return;
  const next = portfolioItems.slice();
  const [moved] = next.splice(fromIdx, 1);
  next.splice(toIdx, 0, moved);
  portfolioItems = next;
  renderPortfolio();
  try {
    await apiFetch('/api/portfolio/order', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_codes: next.map(i => i.stock_code) }),
    });
  } catch (e) {
    await loadPortfolio();
  }
}

async function pfChangeGroup(stockCode, groupName) {
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  try {
    const resp = await apiFetch(`/api/portfolio/${stockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_name: item.stock_name,
        quantity: item.quantity,
        avg_price: item.avg_price,
        group_name: groupName,
      }),
    });
    if (!resp.ok) throw new Error('그룹 변경 실패');
    item.group_name = groupName;
    renderPortfolio();
  } catch (e) { alert(e.message); }
}

function pfShowBenchmarkPicker(stockCode, td) {
  // Close any existing picker
  document.querySelectorAll('.pf-benchmark-picker').forEach(el => el.remove());
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  const picker = document.createElement('div');
  picker.className = 'pf-benchmark-picker';
  const presets = _BENCHMARK_PRESETS.map(p =>
    `<div class="pf-bm-option${item.benchmark_code === p.code ? ' selected' : ''}" onclick="pfSetBenchmark('${stockCode}','${p.code}')">${p.name}</div>`
  ).join('');
  picker.innerHTML = `
    ${presets}
    <div class="pf-bm-custom">
      <input class="pf-bm-input" placeholder="종목코드" onkeydown="if(event.key==='Enter')pfSetBenchmark('${stockCode}',this.value)">
    </div>
    <div class="pf-bm-option pf-bm-reset" onclick="pfSetBenchmark('${stockCode}','')">기본값으로</div>
  `;
  td.style.position = 'relative';
  td.appendChild(picker);
  const input = picker.querySelector('.pf-bm-input');
  if (input) input.focus();
  // Close on outside click
  setTimeout(() => {
    const close = (e) => {
      if (!picker.contains(e.target)) { picker.remove(); document.removeEventListener('click', close); }
    };
    document.addEventListener('click', close);
  }, 0);
}

async function pfSetBenchmark(stockCode, benchmarkCode) {
  document.querySelectorAll('.pf-benchmark-picker').forEach(el => el.remove());
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  try {
    const resp = await apiFetch(`/api/portfolio/${stockCode}/benchmark`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ benchmark_code: benchmarkCode || null }),
    });
    if (!resp.ok) throw new Error('벤치마크 변경 실패');
    const data = await resp.json();
    item.benchmark_code = data.effective_benchmark;
    if (data.benchmark_quote || data.benchmark_name) {
      pfBenchmarkQuotes[data.effective_benchmark] = {
        ...(pfBenchmarkQuotes[data.effective_benchmark] || {}),
        ...data.benchmark_quote,
        name: data.benchmark_name || data.effective_benchmark,
      };
    }
    renderPortfolio();
  } catch (e) { alert(e.message); }
}

function startPortfolioEdit(stockCode) {
  pfEditingCode = stockCode;
  renderPortfolio();
  const priceInput = document.getElementById('pfEditPrice');
  if (priceInput) priceInput.focus();
}

function cancelPortfolioEdit() {
  pfEditingCode = null;
  renderPortfolio();
}

async function savePortfolioEdit(stockCode, stockName) {
  const qty = parseFloat(document.getElementById('pfEditQty').value);
  const price = parseFloat(document.getElementById('pfEditPrice').value);
  if (isNaN(qty) || qty === 0 || isNaN(price) || price < 0) {
    alert('수량과 매입가를 올바르게 입력해 주세요.');
    return;
  }
  try {
    const resp = await apiFetch(`/api/portfolio/${stockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_name: stockName, quantity: qty, avg_price: price }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '저장 실패');
    }
    // Update local item without full reload
    const item = portfolioItems.find(i => i.stock_code === stockCode);
    if (item) {
      item.quantity = qty;
      item.avg_price = price;
      item.stock_name = stockName;
    }
    pfEditingCode = null;
    renderPortfolio();
  } catch (e) { alert(e.message); }
}

async function deletePortfolioItem(stockCode) {
  try {
    const resp = await apiFetch(`/api/portfolio/${stockCode}`, { method: 'DELETE' });
    if (!resp.ok) throw new Error('삭제 실패');
    portfolioItems = portfolioItems.filter(i => i.stock_code !== stockCode);
    renderPortfolio();
  } catch (e) { alert(e.message); }
}

// Portfolio add - search
(function initPfSearch() {
  document.addEventListener('DOMContentLoaded', () => {
    const input = document.getElementById('pfAddInput');
    const dropdown = document.getElementById('pfDropdown');
    if (!input || !dropdown) return;

    input.addEventListener('input', () => {
      clearTimeout(pfSearchTimeout);
      const raw = input.value.trim();
      if (raw.length < 1) { dropdown.classList.remove('show'); return; }

      const q = raw.replace(/(우[A-Z0-9]?|우)$/, '').trim() || raw;
      const wantPref = q !== raw;

      pfSearchTimeout = setTimeout(async () => {
        try {
          // Special asset matching
          const specialAssets = [
            { code: 'KRX_GOLD', name: 'KRX 금현물', keywords: ['금', '금현물', 'krx금', 'krx_gold', 'gold'] },
            { code: 'CRYPTO_BTC', name: '비트코인', keywords: ['btc', '비트코인', 'bitcoin'] },
            { code: 'CRYPTO_ETH', name: '이더리움', keywords: ['eth', '이더리움', 'ethereum'] },
            { code: 'CASH_KRW', name: '원화', keywords: ['krw', '원화', '현금', '원'] },
            { code: 'CASH_USD', name: '미국 달러', keywords: ['usd', '달러', '미국달러', 'dollar'] },
            { code: 'CASH_EUR', name: '유로', keywords: ['eur', '유로', 'euro'] },
            { code: 'CASH_JPY', name: '일본 엔', keywords: ['jpy', '엔', '일본엔', 'yen'] },
            { code: 'CASH_CNY', name: '중국 위안', keywords: ['cny', '위안', '중국위안', 'yuan'] },
            { code: 'CASH_HKD', name: '홍콩 달러', keywords: ['hkd', '홍콩달러'] },
            { code: 'CASH_GBP', name: '영국 파운드', keywords: ['gbp', '파운드', 'pound'] },
            { code: 'CASH_AUD', name: '호주 달러', keywords: ['aud', '호주달러'] },
            { code: 'CASH_CAD', name: '캐나다 달러', keywords: ['cad', '캐나다달러'] },
            { code: 'CASH_CHF', name: '스위스 프랑', keywords: ['chf', '프랑', '스위스프랑'] },
            { code: 'CASH_VND', name: '베트남 동', keywords: ['vnd', '베트남동', '동'] },
            { code: 'CASH_TWD', name: '대만 달러', keywords: ['twd', '대만달러'] },
          ];
          const qLower = raw.toLowerCase();
          const matchedSpecial = specialAssets.filter(a => a.keywords.some(k => qLower.includes(k)) || a.code.toLowerCase() === qLower);

          const resp = await apiFetch(`/api/search?q=${encodeURIComponent(q)}`);
          const results = await resp.json();
          if (!results.length && !matchedSpecial.length) {
            // No domestic results — try as foreign ticker
            if (/^[A-Z0-9]/i.test(raw) && /[A-Z]/i.test(raw)) {
              const r2 = await apiFetch(`/api/portfolio/resolve-name?code=${encodeURIComponent(raw.trim())}`);
              const d = await r2.json();
              if (d.stock_name) {
                const resolvedCode = d.stock_code || raw.trim();
                dropdown.innerHTML = `<div class="dropdown-item" data-code="${resolvedCode}" data-name="${escapeHtml(d.stock_name)}">${escapeHtml(d.stock_name)} <span style="color:var(--text-secondary)">${resolvedCode}</span></div>`;
                dropdown.classList.add('show');
                dropdown.querySelectorAll('.dropdown-item').forEach(el => {
                  el.addEventListener('click', () => pfAddFromSearch(el.dataset.code, el.dataset.name));
                });
                return;
              }
            }
            dropdown.classList.remove('show'); return;
          }

          let items;
          if (wantPref) {
            // Resolve preferred stock names from backend
            const prefCodes = results.flatMap(r => {
              const base = r.stock_code;
              if (!base.endsWith('0')) return [];
              return ['5','7','8','9','K','L'].map(s => base.slice(0,-1) + s);
            });
            const resolved = await Promise.all(
              prefCodes.map(async c => {
                try {
                  const r2 = await apiFetch(`/api/portfolio/resolve-name?code=${c}`);
                  const d = await r2.json();
                  return d.stock_name ? { code: c, name: d.stock_name } : null;
                } catch { return null; }
              })
            );
            items = resolved.filter(Boolean);
          } else {
            items = results.map(r => ({ code: r.stock_code, name: r.corp_name }));
          }

          // Prepend matched special assets
          const specialItems = matchedSpecial.map(a => ({ code: a.code, name: a.name }));
          items = [...specialItems, ...items.filter(i => !specialItems.some(s => s.code === i.code))];
          if (!items.length) { dropdown.classList.remove('show'); return; }
          dropdown.innerHTML = items.map(r =>
            `<div class="dropdown-item" data-code="${r.code}" data-name="${escapeHtml(r.name)}">${escapeHtml(r.name)} <span style="color:var(--text-secondary)">${r.code}</span></div>`
          ).join('');
          dropdown.classList.add('show');
          dropdown.querySelectorAll('.dropdown-item').forEach(el => {
            el.addEventListener('click', () => pfAddFromSearch(el.dataset.code, el.dataset.name));
          });
        } catch {}
      }, 200);
    });

    input.addEventListener('keydown', async (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        dropdown.classList.remove('show');
        const q = input.value.trim();
        if (!q) return;
        const resp = await apiFetch(`/api/portfolio/resolve-name?code=${encodeURIComponent(q)}`);
        const data = await resp.json();
        const resolvedCode = data.stock_code || q;
        pfAddFromSearch(resolvedCode, data.stock_name || q);
      }
    });

    document.addEventListener('click', (e) => {
      if (!input.contains(e.target) && !dropdown.contains(e.target)) dropdown.classList.remove('show');
    });
  });
})();

async function pfAddFromSearch(code, name) {
  document.getElementById('pfDropdown').classList.remove('show');
  document.getElementById('pfAddInput').value = '';
  const existing = portfolioItems.find(i => i.stock_code === code);
  if (existing) {
    startPortfolioEdit(code);
    return;
  }
  try {
    // stock_name empty → backend resolves via Naver Finance
    const resp = await apiFetch(`/api/portfolio/${code}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_name: '', quantity: 1, avg_price: 0 }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '추가 실패');
    }
    pfEditingCode = code;
    await loadPortfolio();
  } catch (e) { alert(e.message); }
}

function _isPreferredStock(code) {
  return /^[0-9]{5}[^0]$/.test(code) || /^[0-9]{5}[A-Z]$/.test(code);
}

let _HOLDING_CODES = new Set([
  '000670','000880','002790','003380','004360','004700','004800',
  '005810','006120','024800','028260','030530','032830',
  '036710','051910','058650','402340',
]);

(function _refreshHoldingCodes() {
  try {
    const cached = JSON.parse(localStorage.getItem('holdingCodes') || '{}');
    if (cached.codes) _HOLDING_CODES = new Set(cached.codes);
    // Refresh once per day
    if (cached.ts && Date.now() - cached.ts < 86400000) return;
  } catch {}
  fetch('https://ducklove.github.io/holding_value/api/holdings.json')
    .then(r => r.json())
    .then(data => {
      const codes = data.items.map(i => i.holdingCode).filter(Boolean);
      if (codes.length) {
        _HOLDING_CODES = new Set(codes);
        localStorage.setItem('holdingCodes', JSON.stringify({ codes, ts: Date.now() }));
      }
    }).catch(() => {});
})();

function pfGoAnalyze(stockCode, e) {
  // Special assets, cash & foreign stocks: no analysis support
  if (['KRX_GOLD', 'CRYPTO_BTC', 'CRYPTO_ETH'].includes(stockCode) || stockCode.startsWith('CASH_')) return;
  const isKorean = stockCode.length === 6 && /^\d{5}/.test(stockCode);
  if (!isKorean) return;

  if (_isPreferredStock(stockCode)) {
    const commonCode = stockCode.slice(0, -1) + '0';
    _showPrefMenu(stockCode, commonCode, e);
    return;
  }
  if (_HOLDING_CODES.has(stockCode)) {
    _showHoldingMenu(stockCode, e);
    return;
  }
  switchView('analysis');
  analyzeStock(stockCode);
}

function _showPrefMenu(prefCode, commonCode, e) {
  // Remove any existing menu
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  const menu = document.createElement('div');
  menu.className = 'pf-pref-menu';
  menu.innerHTML = `
    <div class="pf-pref-item" data-action="common">본주 분석 (${commonCode})</div>
    <div class="pf-pref-item" data-action="spread">우선주 괴리율 대시보드</div>
  `;
  document.body.appendChild(menu);
  // Position near click or element
  const rect = e && e.target ? e.target.getBoundingClientRect() : { left: 100, bottom: 100 };
  menu.style.left = rect.left + 'px';
  menu.style.top = (rect.bottom + 4) + 'px';

  menu.querySelector('[data-action="common"]').addEventListener('click', () => {
    menu.remove();
    switchView('analysis');
    analyzeStock(commonCode);
  });
  menu.querySelector('[data-action="spread"]').addEventListener('click', () => {
    menu.remove();
    window.open(`https://ducklove.github.io/common_preferred_spread/?code=${prefCode}`, '_blank');
  });
  // Close on outside click
  setTimeout(() => {
    document.addEventListener('click', function close(ev) {
      if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close); }
    });
  }, 0);
}

function _showHoldingMenu(stockCode, e) {
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  const menu = document.createElement('div');
  menu.className = 'pf-pref-menu';
  menu.innerHTML = `
    <div class="pf-pref-item" data-action="analysis">본주 분석</div>
    <div class="pf-pref-item" data-action="holding">자회사 비율 추이</div>
  `;
  document.body.appendChild(menu);
  const rect = e && e.target ? e.target.getBoundingClientRect() : { left: 100, bottom: 100 };
  menu.style.left = rect.left + 'px';
  menu.style.top = (rect.bottom + 4) + 'px';
  menu.querySelector('[data-action="analysis"]').addEventListener('click', () => {
    menu.remove();
    switchView('analysis');
    analyzeStock(stockCode);
  });
  menu.querySelector('[data-action="holding"]').addEventListener('click', () => {
    menu.remove();
    window.open(`https://ducklove.github.io/holding_value/?code=${stockCode}`, '_blank');
  });
  setTimeout(() => {
    document.addEventListener('click', function close(ev) {
      if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close); }
    });
  }, 0);
}

// --- Group management modal ---
function openGroupModal() {
  document.getElementById('pfGroupModal').style.display = 'flex';
  renderGroupModalBody();
}

function closeGroupModal() {
  document.getElementById('pfGroupModal').style.display = 'none';
}

const _PIE_COLORS = ['#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f','#edc948','#b07aa1','#ff9da7','#9c755f','#bab0ac'];

function _drawGroupPie(stats, grandMV) {
  const canvas = document.getElementById('pfGroupPie');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const size = 180;
  canvas.width = size * dpr;
  canvas.height = size * dpr;
  canvas.style.width = size + 'px';
  canvas.style.height = size + 'px';
  ctx.scale(dpr, dpr);

  const cx = size / 2, cy = size / 2, r = 70;
  const slices = pfGroups.map((g, i) => {
    const s = stats[g.group_name] || { mv: 0 };
    return { name: g.group_name, value: s.mv, color: _PIE_COLORS[i % _PIE_COLORS.length] };
  }).filter(s => s.value > 0);

  if (!slices.length || grandMV <= 0) {
    ctx.fillStyle = 'var(--text-secondary)';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('데이터 없음', cx, cy);
    return;
  }

  let angle = -Math.PI / 2;
  slices.forEach(s => {
    const sweep = (s.value / grandMV) * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, r, angle, angle + sweep);
    ctx.closePath();
    ctx.fillStyle = s.color;
    ctx.fill();
    // Label
    if (sweep > 0.15) {
      const mid = angle + sweep / 2;
      const lx = cx + Math.cos(mid) * (r * 0.6);
      const ly = cy + Math.sin(mid) * (r * 0.6);
      const pct = (s.value / grandMV * 100).toFixed(0) + '%';
      ctx.fillStyle = '#fff';
      ctx.font = 'bold 11px sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(pct, lx, ly);
    }
    angle += sweep;
  });

  // Legend below
  const ly = size - 8;
  let lx = 8;
  ctx.font = '10px sans-serif';
  ctx.textBaseline = 'bottom';
  slices.forEach(s => {
    ctx.fillStyle = s.color;
    ctx.fillRect(lx, ly - 8, 8, 8);
    ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text').trim() || '#333';
    ctx.textAlign = 'left';
    const label = s.name.length > 4 ? s.name.slice(0, 4) + '..' : s.name;
    ctx.fillText(label, lx + 10, ly);
    lx += ctx.measureText(label).width + 18;
  });
}

function renderGroupModalBody() {
  const body = document.getElementById('pfGroupModalBody');
  // Compute per-group stats
  const stats = {};
  let grandMV = 0;
  portfolioItems.forEach(i => {
    const gn = pfGetGroup(i);
    if (!stats[gn]) stats[gn] = { cnt: 0, invested: 0, mv: 0, dailyPnl: 0 };
    const s = stats[gn];
    const q = i.quote || {};
    const price = q.price ?? null;
    const change = q.change ?? 0;
    const qty = i.quantity;
    const avgPrice = i.avg_price;
    s.cnt++;
    s.invested += qty * avgPrice;
    if (price !== null) { s.mv += qty * price; grandMV += qty * price; }
    if (price !== null) s.dailyPnl += qty * change;
  });
  const defaultCount = pfGroups.filter(x => x.is_default).length;
  const rowsHtml = pfGroups.map((g, i) => {
    const s = stats[g.group_name] || { cnt: 0, invested: 0, mv: 0, dailyPnl: 0 };
    const weight = grandMV > 0 ? (s.mv / grandMV * 100) : 0;
    const returnPct = s.invested > 0 ? ((s.mv - s.invested) / s.invested * 100) : 0;
    const prevMV = s.mv - s.dailyPnl;
    const dailyPct = prevMV > 0 ? (s.dailyPnl / prevMV * 100) : 0;
    const canDelete = !g.is_default || defaultCount > 3;
    const delBtn = canDelete
      ? `<button class="pf-grp-del" onclick="deleteGroup('${escapeHtml(g.group_name)}')" title="삭제">&times;</button>`
      : '';
    return `<tr class="pf-grp-tr" draggable="true" data-grp-idx="${i}">
      <td class="pf-grp-td-drag"><span class="pf-grp-drag" title="드래그하여 순서 변경">&#x2630;</span></td>
      <td class="pf-grp-td-name"><input class="pf-grp-name" value="${escapeHtml(g.group_name)}" data-orig="${escapeHtml(g.group_name)}" onblur="renameGroup(this)"></td>
      <td class="pf-grp-td-num">${s.cnt}</td>
      <td class="pf-grp-td-num">${weight.toFixed(1)}%</td>
      <td class="pf-grp-td-num">${fmtNum(Math.round(s.mv))}</td>
      <td class="pf-grp-td-num"><span class="${returnClass(returnPct)}">${fmtPct(returnPct)}</span></td>
      <td class="pf-grp-td-num"><span class="${returnClass(dailyPct)}">${fmtPct(dailyPct)}</span></td>
      <td class="pf-grp-td-num"><span class="${returnClass(s.dailyPnl)}">${fmtSignedKrw(s.dailyPnl)}</span></td>
      <td class="pf-grp-td-act">${delBtn}</td>
    </tr>`;
  }).join('');
  body.innerHTML = `<div class="pf-grp-layout">
    <div class="pf-grp-table-wrap"><table class="pf-grp-table">
      <thead><tr>
        <th></th><th>그룹명</th><th>종목</th><th>비중</th><th>평가금액</th><th>수익률</th><th>일간</th><th>일간수익</th><th></th>
      </tr></thead>
      <tbody>${rowsHtml}</tbody>
    </table></div>
    <div class="pf-grp-pie-wrap"><canvas id="pfGroupPie" width="180" height="180"></canvas></div>
  </div>`;
  // Draw pie chart
  _drawGroupPie(stats, grandMV);
  // Drag-and-drop for group reorder
  body.querySelectorAll('.pf-grp-tr[draggable]').forEach(row => {
    row.addEventListener('dragstart', e => {
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', row.dataset.grpIdx);
      row.classList.add('dragging');
    });
    row.addEventListener('dragend', () => row.classList.remove('dragging'));
    row.addEventListener('dragover', e => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; if (!row.classList.contains('dragging')) row.classList.add('drag-over'); });
    row.addEventListener('dragleave', () => row.classList.remove('drag-over'));
    row.addEventListener('drop', async e => {
      e.preventDefault();
      row.classList.remove('drag-over');
      const fromIdx = parseInt(e.dataTransfer.getData('text/plain'));
      const toIdx = parseInt(row.dataset.grpIdx);
      if (isNaN(fromIdx) || isNaN(toIdx) || fromIdx === toIdx) return;
      const [moved] = pfGroups.splice(fromIdx, 1);
      pfGroups.splice(toIdx, 0, moved);
      renderGroupModalBody();
      renderPortfolio();
      try {
        await apiFetch('/api/portfolio/groups-order', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ group_names: pfGroups.map(g => g.group_name) }),
        });
      } catch {}
    });
  });
}

async function addNewGroup() {
  const input = document.getElementById('pfNewGroupInput');
  const name = input.value.trim();
  if (!name) return;
  try {
    const resp = await apiFetch('/api/portfolio/groups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '추가 실패');
    }
    const result = await resp.json();
    pfGroups.push(result);
    input.value = '';
    renderGroupModalBody();
    renderPortfolio();
  } catch (e) { alert(e.message); }
}

async function renameGroup(inputEl) {
  const orig = inputEl.dataset.orig;
  const newName = inputEl.value.trim();
  if (!newName || newName === orig) {
    inputEl.value = orig;
    return;
  }
  try {
    const resp = await apiFetch(`/api/portfolio/groups/${encodeURIComponent(orig)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ new_name: newName }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '변경 실패');
    }
    const g = pfGroups.find(g => g.group_name === orig);
    if (g) g.group_name = newName;
    portfolioItems.forEach(i => { if (i.group_name === orig) i.group_name = newName; });
    if (pfGroupFilter && pfGroupFilter.has(orig)) {
      pfGroupFilter.delete(orig);
      pfGroupFilter.add(newName);
    }
    inputEl.dataset.orig = newName;
    renderPortfolio();
  } catch (e) {
    alert(e.message);
    inputEl.value = orig;
  }
}

async function deleteGroup(groupName) {
  const counts = {};
  portfolioItems.forEach(i => {
    const g = pfGetGroup(i);
    counts[g] = (counts[g] || 0) + 1;
  });
  const cnt = counts[groupName] || 0;
  if (cnt > 0 && !confirm(`"${groupName}" 그룹에 ${cnt}개 종목이 있습니다. 삭제하면 기본 그룹으로 이동합니다. 삭제할까요?`)) return;
  try {
    const resp = await apiFetch(`/api/portfolio/groups/${encodeURIComponent(groupName)}`, { method: 'DELETE' });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '삭제 실패');
    }
    pfGroups = pfGroups.filter(g => g.group_name !== groupName);
    if (pfGroupFilter) pfGroupFilter.delete(groupName);
    await loadPortfolio();
    renderGroupModalBody();
  } catch (e) { alert(e.message); }
}

// --- Market Bar ---
const MB_DEFAULT_CODES = ['KOSPI', 'KOSDAQ', 'USD_KRW', 'CMDT_GC', 'NIGHT_FUTURES'];
const MB_MAX = 10;
const MB_LS_KEY = 'market_bar_codes';
let mbCodes = [];
let mbCatalog = {};
let mbLoaded = false;
let mbPickerOpen = false;
let mbDragFrom = -1;

function _mbGetCodes() {
  try { const v = JSON.parse(localStorage.getItem(MB_LS_KEY)); if (Array.isArray(v)) return v; } catch {}
  return null;
}
function _mbSaveCodes() {
  localStorage.setItem(MB_LS_KEY, JSON.stringify(mbCodes));
  if (currentUser) apiFetch('/api/settings/market-bar', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ codes: mbCodes }) }).catch(() => {});
}
async function _mbLoadCodes() {
  if (currentUser) {
    try {
      const resp = await apiFetch('/api/settings/market-bar');
      if (resp.ok) { const d = await resp.json(); if (d.codes) { mbCodes = d.codes; localStorage.setItem(MB_LS_KEY, JSON.stringify(mbCodes)); return; } }
    } catch {}
  }
  mbCodes = _mbGetCodes() || MB_DEFAULT_CODES.slice();
}
async function _mbLoadCatalog() {
  try {
    const resp = await apiFetch('/api/market-indicators');
    if (resp.ok) mbCatalog = await resp.json();
  } catch {}
}

function _mbRenderBar(dataMap) {
  const bar = document.getElementById('marketBar');
  if (!bar) return;
  let html = '';
  mbCodes.forEach((code, idx) => {
    const cat = mbCatalog[code];
    const label = cat ? cat.label : code;
    const d = dataMap ? dataMap[code] : null;
    const r = idx;  // row index
    let valHtml = '-', chgHtml = '';
    if (d && d.value) {
      const rawPct = (d.change_pct || '').replace(/[-+%]/g, '');
      const isDown = d.direction === 'down';
      const cls = isDown ? 'mi-down' : (d.direction === 'up' ? 'mi-up' : '');
      const sign = isDown ? '-' : (d.direction === 'up' ? '+' : '');
      const chgVal = d.change ? `${sign}${d.change}` : '';
      const chgPct = rawPct ? `(${sign}${rawPct}%)` : '';
      valHtml = d.value;
      chgHtml = `<span class="${cls}">${chgVal} ${chgPct}</span>`;
    }
    html += `<span class="mi-label" draggable="true" data-idx="${r}">${escapeHtml(label)}</span>`;
    html += `<span class="mi-val" data-idx="${r}">${valHtml}</span>`;
    html += `<span class="mi-chg" data-idx="${r}">${chgHtml}</span>`;
    html += `<button class="mi-del" data-code="${code}" title="삭제">&times;</button>`;
  });
  if (mbCodes.length < MB_MAX) {
    html += `<div class="mi-add" id="mbAddBtn">+ 항목 추가</div>`;
  }
  bar.innerHTML = html;

  // Event: delete
  bar.querySelectorAll('.mi-del').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      mbCodes = mbCodes.filter(c => c !== btn.dataset.code);
      _mbSaveCodes();
      loadMarketSummary();
    });
  });

  // Event: drag reorder (on label spans)
  bar.querySelectorAll('.mi-label[draggable]').forEach(lbl => {
    lbl.addEventListener('dragstart', (e) => { mbDragFrom = parseInt(lbl.dataset.idx); e.dataTransfer.effectAllowed = 'move'; });
    lbl.addEventListener('dragend', () => { bar.querySelectorAll('.mi-drop-target').forEach(el => el.classList.remove('mi-drop-target')); });
  });
  bar.querySelectorAll('[data-idx]').forEach(cell => {
    cell.addEventListener('dragover', (e) => { e.preventDefault(); const r = parseInt(cell.dataset.idx); bar.querySelectorAll(`[data-idx="${r}"]`).forEach(c => c.classList.add('mi-drop-target')); });
    cell.addEventListener('dragleave', (e) => { const r = parseInt(cell.dataset.idx); bar.querySelectorAll(`[data-idx="${r}"]`).forEach(c => c.classList.remove('mi-drop-target')); });
    cell.addEventListener('drop', (e) => {
      e.preventDefault();
      bar.querySelectorAll('.mi-drop-target').forEach(el => el.classList.remove('mi-drop-target'));
      const to = parseInt(cell.dataset.idx);
      if (mbDragFrom !== to && mbDragFrom >= 0) {
        const [item] = mbCodes.splice(mbDragFrom, 1);
        mbCodes.splice(to, 0, item);
        _mbSaveCodes();
        loadMarketSummary();
      }
    });
  });

  // Event: row hover → show delete button
  bar.querySelectorAll('[data-idx]').forEach(cell => {
    cell.addEventListener('mouseenter', () => {
      const r = cell.dataset.idx;
      const dels = bar.querySelectorAll('.mi-del');
      dels[parseInt(r)]?.classList.add('visible');
    });
    cell.addEventListener('mouseleave', () => {
      bar.querySelectorAll('.mi-del.visible').forEach(d => d.classList.remove('visible'));
    });
  });
  bar.querySelectorAll('.mi-del').forEach(btn => {
    btn.addEventListener('mouseenter', () => btn.classList.add('visible'));
    btn.addEventListener('mouseleave', () => btn.classList.remove('visible'));
  });

  // Event: add button
  const addBtn = document.getElementById('mbAddBtn');
  if (addBtn) addBtn.addEventListener('click', () => _mbTogglePicker());

  if (mbLoaded) flashEl(bar);
  mbLoaded = true;
}

function _mbTogglePicker() {
  const existing = document.getElementById('mbPicker');
  if (existing) { existing.remove(); mbPickerOpen = false; return; }
  mbPickerOpen = true;

  const bar = document.getElementById('marketBar');
  const picker = document.createElement('div');
  picker.id = 'mbPicker';
  picker.className = 'mb-picker';

  const categories = {};
  for (const [code, info] of Object.entries(mbCatalog)) {
    const cat = info.category;
    if (!categories[cat]) categories[cat] = [];
    categories[cat].push({ code, label: info.label });
  }

  let html = '';
  for (const [cat, items] of Object.entries(categories)) {
    html += `<div class="mb-pick-cat">${escapeHtml(cat)}</div>`;
    items.forEach(item => {
      const disabled = mbCodes.includes(item.code);
      html += `<div class="mb-pick-item${disabled ? ' disabled' : ''}" data-code="${item.code}">${escapeHtml(item.label)}</div>`;
    });
  }
  picker.innerHTML = html;
  bar.appendChild(picker);

  picker.querySelectorAll('.mb-pick-item:not(.disabled)').forEach(el => {
    el.addEventListener('click', () => {
      mbCodes.push(el.dataset.code);
      _mbSaveCodes();
      picker.remove();
      mbPickerOpen = false;
      loadMarketSummary();
    });
  });

  // Close on outside click
  setTimeout(() => {
    const closeHandler = (e) => { if (!picker.contains(e.target) && e.target.id !== 'mbAddBtn') { picker.remove(); mbPickerOpen = false; document.removeEventListener('click', closeHandler); } };
    document.addEventListener('click', closeHandler);
  }, 0);
}

async function loadMarketSummary() {
  try {
    if (!mbCodes.length) await _mbLoadCodes();
    const resp = await apiFetch(`/api/market-summary?codes=${mbCodes.join(',')}`);
    if (!resp.ok) return;
    const dataMap = await resp.json();
    _mbRenderBar(dataMap);
  } catch {}
}

function toggleCsvPanel() {
  const panel = document.getElementById('pfCsvPanel');
  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

async function submitCsv(mode) {
  const text = document.getElementById('pfCsvInput').value.trim();
  if (!text) { alert('CSV 데이터를 입력해 주세요.'); return; }

  if (mode === 'replace' && !confirm('기존 포트폴리오를 모두 삭제하고 새로 등록합니다. 계속할까요?')) return;

  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const items = [];
  const errors = [];

  for (let i = 0; i < lines.length; i++) {
    const parts = lines[i].split(/[,\t]/).map(s => s.trim());
    if (parts.length < 3) { errors.push(`행 ${i+1}: 종목코드,매입가,수량 3개 필드가 필요합니다.`); continue; }
    const [code, priceStr, qtyStr] = parts;
    const price = Number(priceStr);
    const qty = parseFloat(qtyStr);
    if (!code) { errors.push(`행 ${i+1}: 종목코드가 비어 있습니다.`); continue; }
    if (isNaN(price) || price < 0) { errors.push(`행 ${i+1}: 매입가가 올바르지 않습니다.`); continue; }
    if (isNaN(qty) || qty === 0) { errors.push(`행 ${i+1}: 수량은 0이 아닌 값이어야 합니다.`); continue; }
    items.push({ stock_code: code, avg_price: price, quantity: qty });
  }

  if (errors.length) { alert(errors.join('\n')); return; }
  if (!items.length) { alert('등록할 종목이 없습니다.'); return; }

  const btns = document.querySelectorAll('.pf-csv-btn');
  btns.forEach(b => b.disabled = true);

  try {
    const resp = await apiFetch('/api/portfolio/bulk', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode, items }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '등록 실패');
    document.getElementById('pfCsvInput').value = '';
    document.getElementById('pfCsvPanel').style.display = 'none';
    alert(`${data.imported}개 종목이 ${mode === 'replace' ? '교체' : '추가'} 등록되었습니다.`);
    await loadPortfolio();
  } catch (e) {
    alert(e.message);
  } finally {
    btns.forEach(b => b.disabled = false);
  }
}

// --- Quote subscription management ---
function _updateQuoteSubscriptions() {
  const requested = { portfolio: [], benchmark: [], sidebar: [], analysis: [] };
  portfolioItems.forEach(item => {
    requested.portfolio.push(item.stock_code);
    if (item.benchmark_code) requested.benchmark.push(item.benchmark_code);
  });
  recentListItems.forEach(item => requested.sidebar.push(item.stock_code));
  if (activeStockCode) requested.analysis.push(activeStockCode);
  QuoteManager.updateSubscriptions(requested);
}

let _pfRenderQueued = false;
QuoteManager.onQuote = function(code, q) {
  // 1) 분석 뷰 활성 종목
  if (code === activeStockCode && q.price != null) {
    renderQuoteSnapshot({
      date: q.date, price: q.price, previous_close: q.previous_close,
      change: q.change, change_pct: q.change_pct,
    }, activeIndicators);
    flashEl(document.getElementById('quoteSummary'));
  }
  // 2) 포트폴리오 종목
  const pfItem = portfolioItems.find(i => i.stock_code === code);
  if (pfItem && q.price != null) {
    pfItem.quote = { price: q.price, change: q.change, change_pct: q.change_pct, previous_close: q.previous_close, date: q.date };
    if (!pfEditingCode && activeView === 'portfolio' && !_pfRenderQueued) {
      _pfRenderQueued = true;
      requestAnimationFrame(() => { _pfRenderQueued = false; renderPortfolio(); });
    }
  }
  // 3) 사이드바
  const sbItem = recentListItems.find(i => i.stock_code === code);
  if (sbItem && q.price != null) {
    sbItem.quote_snapshot = { price: q.price, change: q.change, change_pct: q.change_pct };
  }
};

// Init
async function initApp() {
  await initAuth();
  await loadRecentList();
  await _mbLoadCatalog();
  await _mbLoadCodes();
  loadMarketSummary();
  setInterval(loadMarketSummary, 60_000);
  QuoteManager.connect();
  _updateQuoteSubscriptions();
  trackEvent('app_ready', { auth_state: currentUser ? 'logged_in' : 'guest' });
  // Mobile + logged in → default to portfolio view
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  if (code) {
    switchView('analysis');
    analyzeStock(code.trim());
  } else if (currentUser && window.innerWidth <= 900) {
    switchView('portfolio');
  }
}

initApp();

window.addEventListener('pageshow', () => {
  syncAuthState({ refreshRecentList: true, refreshPreference: true });
});

window.addEventListener('focus', () => {
  syncAuthState({ refreshRecentList: true, refreshPreference: true });
});

document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    syncAuthState({ refreshRecentList: true, refreshPreference: true });
  }
});

// --- Portfolio Performance Tab ---
let pfActiveTab = 'holdings';

function pfSwitchTab(tab) {
  pfActiveTab = tab;
  document.querySelectorAll('.pf-tab').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === tab));
  const holdingsTab = document.getElementById('pfHoldingsTab');
  const performanceTab = document.getElementById('pfPerformanceTab');
  holdingsTab.style.display = tab === 'holdings' ? '' : 'none';
  performanceTab.style.display = tab === 'performance' ? '' : 'none';
  const activeEl = tab === 'holdings' ? holdingsTab : performanceTab;
  activeEl.classList.remove('fade-in');
  void activeEl.offsetWidth;
  activeEl.classList.add('fade-in');
  if (tab === 'performance') loadPerformanceData();
}

async function loadPerformanceData() {
  const dateInput = document.getElementById('pfCfDate');
  if (dateInput && !dateInput.value) dateInput.value = new Date().toISOString().slice(0, 10);
  try {
    const [navResp, cfResp] = await Promise.all([
      apiFetch('/api/portfolio/nav-history'),
      apiFetch('/api/portfolio/cashflows'),
    ]);
    const navData = navResp.ok ? await navResp.json() : [];
    const cfData = cfResp.ok ? await cfResp.json() : [];
    renderNavChart(navData);
    renderNavReturns(navData);
    renderCashflows(cfData);
  } catch {}
}

function renderNavChart(data) {
  const canvas = document.getElementById('pfNavCanvas');
  if (!canvas || !data.length) {
    if (canvas) {
      const ctx = canvas.getContext('2d');
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary');
      ctx.font = '14px sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('스냅샷 데이터가 없습니다. "스냅샷 저장" 버튼을 눌러 첫 스냅샷을 생성하세요.', canvas.width / 2, canvas.height / 2);
    }
    return;
  }
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = 300 * dpr;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = '300px';
  ctx.scale(dpr, dpr);
  const W = rect.width, H = 300;
  ctx.clearRect(0, 0, W, H);

  const pad = { top: 20, right: 20, bottom: 40, left: 60 };
  const cw = W - pad.left - pad.right;
  const ch = H - pad.top - pad.bottom;

  const navs = data.map(d => d.nav);
  const minNav = Math.min(...navs) * 0.995;
  const maxNav = Math.max(...navs) * 1.005;
  const range = maxNav - minNav || 1;

  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const lineColor = getComputedStyle(document.documentElement).getPropertyValue('--primary').trim() || '#3b82f6';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#333';

  // Grid
  ctx.strokeStyle = gridColor;
  ctx.lineWidth = 0.5;
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + (ch / 4) * i;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cw, y); ctx.stroke();
    const val = maxNav - (range / 4) * i;
    ctx.fillStyle = textColor; ctx.font = '11px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(val.toFixed(1), pad.left - 8, y + 4);
  }

  // Base line at 1000
  if (minNav <= 1000 && maxNav >= 1000) {
    const baseY = pad.top + ch * (1 - (1000 - minNav) / range);
    ctx.strokeStyle = '#888'; ctx.lineWidth = 1; ctx.setLineDash([4, 4]);
    ctx.beginPath(); ctx.moveTo(pad.left, baseY); ctx.lineTo(pad.left + cw, baseY); ctx.stroke();
    ctx.setLineDash([]);
  }

  // NAV line
  ctx.strokeStyle = lineColor; ctx.lineWidth = 2;
  ctx.beginPath();
  data.forEach((d, i) => {
    const x = pad.left + (cw / Math.max(data.length - 1, 1)) * i;
    const y = pad.top + ch * (1 - (d.nav - minNav) / range);
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.stroke();

  // Fill area
  const lastX = pad.left + cw;
  const baseLineY = pad.top + ch;
  ctx.lineTo(lastX, baseLineY);
  ctx.lineTo(pad.left, baseLineY);
  ctx.closePath();
  ctx.fillStyle = lineColor.replace(')', ', 0.1)').replace('rgb', 'rgba');
  ctx.fill();

  // X labels
  ctx.fillStyle = textColor; ctx.font = '10px sans-serif'; ctx.textAlign = 'center';
  const step = Math.max(1, Math.floor(data.length / 8));
  data.forEach((d, i) => {
    if (i % step === 0 || i === data.length - 1) {
      const x = pad.left + (cw / Math.max(data.length - 1, 1)) * i;
      ctx.fillText(d.date.slice(5), x, H - pad.bottom + 16);
    }
  });
}

function renderNavReturns(data) {
  const el = document.getElementById('pfNavReturns');
  if (!el || !data.length) { if (el) el.innerHTML = ''; return; }
  const latest = data[data.length - 1];
  const baseNav = 1000;
  const totalReturn = ((latest.nav / baseNav) - 1) * 100;

  function retSince(daysAgo) {
    if (data.length < 2) return null;
    const target = new Date();
    target.setDate(target.getDate() - daysAgo);
    const targetStr = target.toISOString().slice(0, 10);
    let prev = data[0];
    for (const d of data) { if (d.date <= targetStr) prev = d; else break; }
    return ((latest.nav / prev.nav) - 1) * 100;
  }

  const periods = [
    { label: '전일', val: data.length >= 2 ? ((latest.nav / data[data.length - 2].nav) - 1) * 100 : null },
    { label: '1주', val: retSince(7) },
    { label: '1개월', val: retSince(30) },
    { label: '3개월', val: retSince(90) },
    { label: '전체', val: totalReturn },
  ];
  el.innerHTML = periods.map(p => {
    if (p.val === null) return '';
    const cls = p.val > 0 ? 'pf-return positive' : p.val < 0 ? 'pf-return negative' : '';
    return `<div class="pf-nav-ret-card"><div class="pf-nav-ret-label">${p.label}</div><div class="pf-nav-ret-value ${cls}">${p.val > 0 ? '+' : ''}${p.val.toFixed(2)}%</div></div>`;
  }).join('');
}

function renderCashflows(data) {
  const tbody = document.getElementById('pfCfBody');
  if (!tbody) return;
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text-secondary);">입출금 내역이 없습니다.</td></tr>';
    return;
  }
  tbody.innerHTML = data.map(cf => `<tr>
    <td>${cf.date}</td>
    <td>${cf.type === 'deposit' ? '입금' : '출금'}</td>
    <td class="pf-col-num">${fmtNum(Math.round(cf.amount))}원</td>
    <td class="pf-col-num">${cf.nav_at_time ? cf.nav_at_time.toFixed(2) : '-'}</td>
    <td class="pf-col-num">${cf.units_change ? (cf.units_change > 0 ? '+' : '') + cf.units_change.toFixed(2) : '-'}</td>
    <td>${cf.memo || ''}</td>
    <td><button class="pf-row-btn delete" onclick="deleteCashflow(${cf.id})">X</button></td>
  </tr>`).join('');
}

async function addCashflow() {
  const type = document.getElementById('pfCfType').value;
  const date = document.getElementById('pfCfDate').value;
  const amount = parseFloat(document.getElementById('pfCfAmount').value);
  const memo = document.getElementById('pfCfMemo').value.trim();
  if (!amount || amount <= 0) { alert('금액을 입력해 주세요.'); return; }
  try {
    const resp = await apiFetch('/api/portfolio/cashflows', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, date: date || undefined, amount, memo: memo || undefined }),
    });
    if (!resp.ok) { const d = await resp.json().catch(() => ({})); throw new Error(d.detail || '등록 실패'); }
    document.getElementById('pfCfAmount').value = '';
    document.getElementById('pfCfMemo').value = '';
    loadPerformanceData();
  } catch (e) { alert(e.message); }
}

async function deleteCashflow(id) {
  if (!confirm('이 입출금 내역을 삭제할까요?')) return;
  try {
    await apiFetch(`/api/portfolio/cashflows/${id}`, { method: 'DELETE' });
    loadPerformanceData();
  } catch (e) { alert(e.message); }
}

