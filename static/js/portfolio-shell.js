// Portfolio shell: shared state, columns, view switching, NPS entrypoint.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
// --- Portfolio ---
let activeView = 'analysis';
// Holdings list now lives in PfStore.items (portfolio-store.js).
let portfolioLoading = false;
let pfSearchTimeout = null;
let pfEditingCode = null;
let pfSavingEditCode = null;
let pfSortKey = null;
let pfSortAsc = false;
let pfGroups = [];        // [{group_name, sort_order, is_default}, ...]
let pfGroupFilter = null; // null = all selected, Set of group_names = filtered
let pfPortfolioSearchText = '';
let pfGroupSort = false;  // independent group sort toggle
let pfPendingManualOrderCodes = null;
let pfManualOrderRevision = 0;
let pfManualOrderSaveInFlight = false;
let pfManualOrderKeepTimer = null;
// Benchmark quotes now live in PfStore.benchmarkQuotes (portfolio-store.js).
let pfMonthEndSnap = null; // {total_value, nav, fx_usdkrw, ...} at end of previous month
let pfMonthEndStockValues = {}; // stock_code -> market_value at month end
let pfYearStartSnap = null; // {date, total_value, fx_usdkrw, ...} for first snapshot of this year
let pfYearStartStockValues = {}; // stock_code -> market_value at year start
// NAV history now lives in PfStore.navHistory (portfolio-store.js).
let pfIntradayData = []; // [{ts, total_value}, ...]
let pfPrevDaySnapshot = null; // {total_value, fx_usdkrw, stock_values, today_net_cashflow}
let pfCurrency = 'KRW'; // 'KRW' or 'USD'
let pfFxRate = null; // USD/KRW rate
const PF_QUOTE_REFRESH_MS = 60_000;
let _pfPointerGuardUntil = 0;
const PF_SIMPLE_MODE_KEY = 'pf_mobile_simple_mode';
let pfSimpleMode = false;
// 컴팩트 보기: 종목명을 한 줄로, 태그·순서이동 핸들을 숨기고 행 간격을 좁힌다.
// 모바일 전용인 pf-mobile-simple 과 달리 데스크톱에서도 동작하는 보기 옵션.
const PF_COMPACT_ROWS_KEY = 'pf_compact_rows';
let pfCompactRows = false;
try { pfCompactRows = localStorage.getItem(PF_COMPACT_ROWS_KEY) === '1'; } catch (e) {}

function pfBenchmarkQuoteHasChange(q) {
  return q && q.change_pct !== null && q.change_pct !== undefined && q.change_pct !== '';
}

function pfMergeBenchmarkQuote(code, incoming) {
  if (!code || !incoming || typeof incoming !== 'object') return false;
  const current = PfStore.benchmarkQuotes[code] || {};
  const merged = { ...current };
  const incomingHasChange = pfBenchmarkQuoteHasChange(incoming);
  const currentHasChange = pfBenchmarkQuoteHasChange(current);
  const canReplaceChange = incomingHasChange && (incoming._stale !== true || !currentHasChange);

  for (const [key, value] of Object.entries(incoming)) {
    if (value === null || value === undefined || value === '') continue;
    if (['change_pct', 'change', 'direction', 'value'].includes(key) && !canReplaceChange) continue;
    merged[key] = value;
  }

  PfStore.benchmarkQuotes[code] = merged;
  return true;
}

function _pfMarkPointerInteraction(ms = 450) {
  _pfPointerGuardUntil = performance.now() + ms;
}

function _pfIsPointerInteractionActive() {
  return performance.now() < _pfPointerGuardUntil;
}

function _pfIsSimpleModeViewport() {
  return window.matchMedia('(max-width: 900px), (max-height: 520px) and (max-width: 1180px)').matches;
}

function _pfShouldAutoUseSimpleMode() {
  return window.matchMedia('(max-width: 760px), (max-height: 520px) and (max-width: 1180px)').matches;
}

function _mobileFixedView() {
  // 과거에는 로그인한 모바일 사용자를 포트폴리오에 강제 고정했지만, 하단 탭바로
  // 4개 뷰를 자유롭게 오가도록 바뀌면서 lock 을 해제한다. 첫 진입 기본값(로그인 시
  // 포트폴리오)은 initApp 에서 별도로 처리하므로 여기서 강제할 필요가 없다.
  return null;
}

function pfSyncMobileFixedView() {
  const lockedView = _mobileFixedView();
  document.body.classList.toggle('mobile-fixed-portfolio', lockedView === 'portfolio');
  document.body.classList.toggle('mobile-fixed-analysis', lockedView === 'analysis');
  if (lockedView && activeView !== lockedView) {
    switchView(lockedView, { allowMobileLockOverride: true });
  }
}

function _pfLoadSimpleModePreference() {
  try {
    const raw = localStorage.getItem(PF_SIMPLE_MODE_KEY);
    if (raw === '1') return true;
    if (raw === '0') return false;
  } catch (e) {}
  return null;
}

function _pfSaveSimpleModePreference(enabled) {
  try { localStorage.setItem(PF_SIMPLE_MODE_KEY, enabled ? '1' : '0'); } catch (e) {}
}

function _pfApplySimpleMode(enabled, { persist = false } = {}) {
  const compactViewport = _pfIsSimpleModeViewport();
  const active = compactViewport && !!enabled;
  pfSimpleMode = active;
  document.body.classList.toggle('pf-mobile-simple', active);
  if (active && typeof pfSwitchTab === 'function' && typeof pfActiveTab !== 'undefined' && pfActiveTab !== 'holdings') {
    pfSwitchTab('holdings');
  }
  if (persist) _pfSaveSimpleModePreference(active);

  const toggle = document.getElementById('pfSimpleToggle');
  if (toggle) {
    toggle.hidden = !compactViewport;
    toggle.classList.toggle('active', active);
    toggle.setAttribute('aria-pressed', active ? 'true' : 'false');
    toggle.textContent = active ? '일반' : '간편';
    toggle.title = active ? '일반 보기' : '모바일 간편 보기';
  }
}

function pfSyncSimpleModeForViewport() {
  const preference = _pfLoadSimpleModePreference();
  _pfApplySimpleMode(preference === null ? _pfShouldAutoUseSimpleMode() : preference);
  pfSyncMobileFixedView();
}

function pfToggleSimpleMode() {
  _pfApplySimpleMode(!pfSimpleMode, { persist: true });
}

(function initPfSimpleMode() {
  const onReady = () => {
    pfSyncSimpleModeForViewport();
    window.addEventListener('resize', pfSyncSimpleModeForViewport);
    window.addEventListener('orientationchange', pfSyncSimpleModeForViewport);
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onReady);
  } else {
    onReady();
  }
})();

// --- Column visibility ---
// `defaultVisible: false` 는 "처음 방문하는 사용자에게 기본 숨김".
// 기존 사용자가 이미 localStorage 에 visibility 선택을 저장해둔 경우엔
// 그 선택이 우선 (_pfGetColVisibility 로직 참조).
const PF_COL_DEFS = [
  { key: 'group',     cls: 'pf-col-group',     label: '그룹' },
  { key: 'curprice',  cls: 'pf-col-curprice',   label: '현재가' },
  { key: 'benchmark', cls: 'pf-col-benchmark',  label: '벤치마크' },
  { key: 'invested',  cls: 'pf-col-invested',   label: '거래대금',  defaultVisible: false },
  { key: 'buyprice',  cls: 'pf-col-buyprice',   label: '매입가' },
  { key: 'target',    cls: 'pf-col-target',     label: '목표가',     defaultVisible: false },
  { key: 'achiev',    cls: 'pf-col-achiev',     label: '달성률',     defaultVisible: false },
  { key: 'qty',       cls: 'pf-col-qty',        label: '수량' },
  { key: 'return',    cls: 'pf-col-return',      label: '수익률' },
  { key: 'mktval',    cls: 'pf-col-mktval',     label: '평가금액' },
  { key: 'dividend',  cls: 'pf-col-dividend',   label: '배당액' },
  { key: 'divyield',  cls: 'pf-col-divyield',   label: '배당수익률' },
  { key: 'weight',    cls: 'pf-col-weight',      label: '비중' },
  { key: 'date',      cls: 'pf-col-date',       label: '등록일자',  defaultVisible: false },
];
let _pfColStyleEl = null;

function _pfLoadColVisibility() {
  try { return JSON.parse(localStorage.getItem('pf_col_vis') || 'null'); } catch { return null; }
}
function _pfSaveColVisibility(vis) {
  localStorage.setItem('pf_col_vis', JSON.stringify(vis));
}
function _pfGetColVisibility() {
  // Stored per-user overrides take priority; columns not yet known to
  // the stored map (newly added) fall back to their defaultVisible
  // setting. This lets us ship a new column hidden-by-default without
  // wiping existing users' customizations. defaultVisible defaults to
  // true when unspecified so bulk of the columns don't need the flag.
  const stored = _pfLoadColVisibility() || {};
  const result = {};
  for (const c of PF_COL_DEFS) {
    if (stored[c.key] !== undefined) {
      result[c.key] = !!stored[c.key];
    } else {
      result[c.key] = c.defaultVisible !== false;
    }
  }
  return result;
}
function _pfApplyColVisibility(vis) {
  if (!_pfColStyleEl) {
    _pfColStyleEl = document.createElement('style');
    document.head.appendChild(_pfColStyleEl);
  }
  const rules = PF_COL_DEFS
    .filter(c => !vis[c.key])
    .map(c => `.${c.cls} { display: none !important; }`)
    .join('\n');
  _pfColStyleEl.textContent = rules;
}
function pfToggleCol(key, checked) {
  const vis = _pfGetColVisibility();
  vis[key] = checked;
  _pfSaveColVisibility(vis);
  _pfApplyColVisibility(vis);
  // Sync checkbox UI
  const wrap = document.getElementById('pfColToggles');
  if (wrap) {
    wrap.querySelectorAll('input[type="checkbox"]').forEach(cb => {
      const k = PF_COL_DEFS.find(c => c.label === cb.parentElement.textContent.trim());
      if (k) cb.checked = vis[k.key];
    });
  }
}
function _pfApplyCompactRows(enabled) {
  pfCompactRows = !!enabled;
  document.body.classList.toggle('pf-compact-rows', pfCompactRows);
  const cb = document.getElementById('pfCompactToggle');
  if (cb && cb.checked !== pfCompactRows) cb.checked = pfCompactRows;
}
function pfToggleCompactRows(checked) {
  _pfApplyCompactRows(checked);
  try { localStorage.setItem(PF_COMPACT_ROWS_KEY, pfCompactRows ? '1' : '0'); } catch (e) {}
}
(function initPfCompactRows() {
  const apply = () => _pfApplyCompactRows(pfCompactRows);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', apply);
  } else {
    apply();
  }
})();

let _pfColTogglesRendered = false;
function _pfRenderColToggles() {
  const vis = _pfGetColVisibility();
  _pfApplyColVisibility(vis);
  if (_pfColTogglesRendered) return;
  const wrap = document.getElementById('pfColToggles');
  if (!wrap) return;
  const compactToggle =
    `<label class="pf-compact-toggle" title="태그·순서이동 아이콘을 숨기고 종목명을 한 줄로, 행 간격을 좁게 표시">`
    + `<input type="checkbox" id="pfCompactToggle" class="js-pf-compact-toggle"${pfCompactRows ? ' checked' : ''}> 컴팩트</label>`
    + `<span class="pf-col-toggle-sep" aria-hidden="true"></span>`;
  const colToggles = PF_COL_DEFS.map(c =>
    `<label><input type="checkbox" class="js-pf-col-toggle" data-col-key="${escapeHtml(c.key)}" ${vis[c.key] ? 'checked' : ''}> ${c.label}</label>`
  ).join('');
  wrap.innerHTML = compactToggle + colToggles;
  _pfColTogglesRendered = true;
}

function switchView(view, options = {}) {
  const lockedView = options.allowMobileLockOverride ? null : _mobileFixedView();
  if (lockedView && view !== lockedView) view = lockedView;
  activeView = view;
  // 데스크톱 상단 탭(.nav-btn)과 모바일 하단 탭바(.mnav-btn) 활성 상태를 함께 동기화.
  document.querySelectorAll('.nav-btn, .mnav-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.view === view));
  const investingView = document.getElementById('investingView');
  const analysisView = document.getElementById('analysisView');
  const portfolioView = document.getElementById('portfolioView');
  const npsView = document.getElementById('npsView');
  const labsView = document.getElementById('labsView');
  const insightsView = document.getElementById('insightsView');
  if (investingView) investingView.style.display = view === 'investing' ? 'block' : 'none';
  analysisView.style.display = view === 'analysis' ? 'block' : 'none';
  portfolioView.style.display = view === 'portfolio' ? 'block' : 'none';
  if (npsView) npsView.style.display = view === 'nps' ? 'block' : 'none';
  if (labsView) labsView.style.display = view === 'labs' ? 'block' : 'none';
  if (insightsView) insightsView.style.display = view === 'insights' ? 'block' : 'none';
  const activeEl = view === 'investing' ? investingView
                  : view === 'analysis' ? analysisView
                  : view === 'portfolio' ? portfolioView
                  : view === 'labs' ? labsView
                  : view === 'insights' ? insightsView
                  : npsView;
  if (activeEl) {
    activeEl.classList.remove('fade-in');
    void activeEl.offsetWidth;
    activeEl.classList.add('fade-in');
  }
  if (view === 'investing' && typeof loadInvestingDashboard === 'function') {
    loadInvestingDashboard();
  } else if (view === 'portfolio') {
    loadPortfolio();
  } else if (view === 'nps') {
    loadNpsView();
  } else if (view === 'insights' && typeof loadInsightsBoard === 'function') {
    loadInsightsBoard();
  }
  _updateQuoteSubscriptions();
  // 모바일에서 탭 전환 시 이전 뷰의 스크롤 위치가 남아 본문이 중간에서 시작하는
  // 어색함을 막기 위해 최상단으로 이동.
  if (typeof isCompactMobileViewport === 'function' && isCompactMobileViewport()) {
    window.scrollTo(0, 0);
  }
}

let _npsLoaded = false;
async function loadNpsView() {
  const container = document.getElementById('npsContent');
  if (!container) return;
  if (_npsLoaded) return;
  try {
    const resp = await apiFetch('/api/nps/html');
    if (!resp.ok) throw new Error('NPS 데이터를 불러올 수 없습니다.');
    container.innerHTML = await resp.text();
    _npsLoaded = true;
    // Execute inline scripts in the inserted HTML
    container.querySelectorAll('script').forEach(oldScript => {
      const newScript = document.createElement('script');
      newScript.textContent = oldScript.textContent;
      oldScript.replaceWith(newScript);
    });
  } catch (e) {
    container.innerHTML = `<div style="text-align:center;padding:40px;color:var(--text-secondary);">${escapeHtml(e.message)}</div>`;
    console.warn(e);
  }
}
