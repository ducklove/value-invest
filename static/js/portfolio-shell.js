// Portfolio shell: shared state, columns, view switching, NPS entrypoint.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
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
let pfMonthEndSnap = null; // {total_value, nav, fx_usdkrw, ...} at end of previous month
let pfMonthEndStockValues = {}; // stock_code -> market_value at month end
let pfYearStartSnap = null; // {date, total_value, fx_usdkrw, ...} for first snapshot of this year
let pfYearStartStockValues = {}; // stock_code -> market_value at year start
let pfNavHistory = []; // [{date, nav, total_value, total_invested, total_units}, ...]
let pfIntradayData = []; // [{ts, total_value}, ...]
let pfPrevDaySnapshot = null; // {total_value, fx_usdkrw, stock_values, today_net_cashflow}
let pfCurrency = 'KRW'; // 'KRW' or 'USD'
let pfFxRate = null; // USD/KRW rate
const PF_QUOTE_REFRESH_MS = 60_000;
let _pfPointerGuardUntil = 0;

function _pfMarkPointerInteraction(ms = 450) {
  _pfPointerGuardUntil = performance.now() + ms;
}

function _pfIsPointerInteractionActive() {
  return performance.now() < _pfPointerGuardUntil;
}

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
let _pfColTogglesRendered = false;
function _pfRenderColToggles() {
  const vis = _pfGetColVisibility();
  _pfApplyColVisibility(vis);
  if (_pfColTogglesRendered) return;
  const wrap = document.getElementById('pfColToggles');
  if (!wrap) return;
  wrap.innerHTML = PF_COL_DEFS.map(c =>
    `<label><input type="checkbox" class="js-pf-col-toggle" data-col-key="${escapeHtml(c.key)}" ${vis[c.key] ? 'checked' : ''}> ${c.label}</label>`
  ).join('');
  _pfColTogglesRendered = true;
}

function switchView(view) {
  activeView = view;
  document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.view === view));
  const analysisView = document.getElementById('analysisView');
  const portfolioView = document.getElementById('portfolioView');
  const npsView = document.getElementById('npsView');
  const labsView = document.getElementById('labsView');
  const backtestView = document.getElementById('backtestView');
  const insightsView = document.getElementById('insightsView');
  analysisView.style.display = view === 'analysis' ? 'block' : 'none';
  portfolioView.style.display = view === 'portfolio' ? 'block' : 'none';
  if (npsView) npsView.style.display = view === 'nps' ? 'block' : 'none';
  if (labsView) labsView.style.display = view === 'labs' ? 'block' : 'none';
  if (backtestView) backtestView.style.display = view === 'backtest' ? 'block' : 'none';
  if (insightsView) insightsView.style.display = view === 'insights' ? 'block' : 'none';
  const activeEl = view === 'analysis' ? analysisView
                  : view === 'portfolio' ? portfolioView
                  : view === 'labs' ? labsView
                  : view === 'backtest' ? backtestView
                  : view === 'insights' ? insightsView
                  : npsView;
  if (activeEl) {
    activeEl.classList.remove('fade-in');
    void activeEl.offsetWidth;
    activeEl.classList.add('fade-in');
  }
  if (view === 'portfolio') {
    loadPortfolio();
  } else if (view === 'nps') {
    loadNpsView();
  } else if (view === 'insights' && typeof loadInsightsBoard === 'function') {
    loadInsightsBoard();
  }
  _updateQuoteSubscriptions();
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
