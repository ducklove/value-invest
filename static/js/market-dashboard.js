// 투자정보 (Investing dashboard): public market overview.
//
// Built entirely from already-public endpoints — /api/market-indicators
// (catalog: code -> {label, category}) and /api/market-summary?codes=...
// (values: code -> {value, change, change_pct, direction}). Renders the
// indicators grouped by category into #marketDashboard. No auth required.
//
// Phase 1 covers the indicator grid (지수·해외증시·원자재·환율·금리·야간선물).
// Crypto / news / 수급·시총·업종 widgets are layered on in later steps.

let _mdCatalog = null; // {code: {label, category}}
let _mdLoadedOnce = false;
let _mdInFlight = null;

// Display order for category groups; unknown categories fall to the end.
const MD_CATEGORY_ORDER = ['국내 지수', '해외 지수', '원자재', '환율', '채권', '야간선물'];

function _mdChange(d) {
  // Mirror the market-bar contract: direction up/down/flat, change_pct like "1.23%".
  const rawPct = String(d.change_pct || '').replace(/[-+%]/g, '');
  const isDown = d.direction === 'down';
  const isUp = d.direction === 'up';
  const cls = isDown ? 'md-down' : (isUp ? 'md-up' : 'md-flat');
  const sign = isDown ? '-' : (isUp ? '+' : '');
  const chgVal = d.change ? `${sign}${String(d.change)}` : '';
  const chgPct = rawPct ? `(${sign}${rawPct}%)` : '';
  return { cls, text: [chgVal, chgPct].filter(Boolean).join(' ') };
}

function _mdGroupByCategory(catalog) {
  const groups = {};
  for (const [code, meta] of Object.entries(catalog || {})) {
    const cat = (meta && meta.category) || '기타';
    (groups[cat] = groups[cat] || []).push(code);
  }
  const cats = Object.keys(groups).sort((a, b) => {
    const ia = MD_CATEGORY_ORDER.indexOf(a);
    const ib = MD_CATEGORY_ORDER.indexOf(b);
    return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
  });
  return cats.map((cat) => ({ category: cat, codes: groups[cat] }));
}

// Naver-style information architecture: prominent 주요 지수 hero in the main
// column, 해외 증시 below it, and the lighter indicator strips (환율·원자재·
// 금리·야간선물) in a right rail. Phase 2 ranking/수급/업종/뉴스 sections and a
// future 경제캘린더 slot into these columns without changing this skeleton.
const MD_HERO_CATEGORIES = ['국내 지수'];
const MD_MAIN_CATEGORIES = ['해외 지수'];

function _mdCardHtml(code, catalog, dataMap, variant) {
  const meta = catalog[code] || {};
  const label = meta.label || code;
  const d = dataMap ? dataMap[code] : null;
  let valHtml = '-';
  let chgHtml = '';
  if (d && d.value) {
    const c = _mdChange(d);
    valHtml = escapeHtml(String(d.value));
    chgHtml = c.text ? `<span class="md-chg ${c.cls}">${escapeHtml(c.text)}</span>` : '';
  }
  if (variant === 'hero') {
    return `<div class="md-hero-card">`
      + `<div class="md-hero-label">${escapeHtml(label)}</div>`
      + `<div class="md-hero-val">${valHtml}</div>${chgHtml}</div>`;
  }
  return `<div class="md-row">`
    + `<span class="md-row-label">${escapeHtml(label)}</span>`
    + `<span class="md-row-val">${valHtml}</span>${chgHtml}</div>`;
}

function _mdSectionHtml(category, codes, catalog, dataMap, variant) {
  const body = variant === 'hero'
    ? `<div class="md-hero">${codes.map((c) => _mdCardHtml(c, catalog, dataMap, 'hero')).join('')}</div>`
    : `<div class="md-rows">${codes.map((c) => _mdCardHtml(c, catalog, dataMap, 'list')).join('')}</div>`;
  return `<section class="md-section${variant === 'hero' ? ' md-hero-section' : ''}">`
    + `<h3 class="md-section-title">${escapeHtml(category)}</h3>${body}</section>`;
}

function _mdRenderDashboard(catalog, dataMap) {
  // The two-column shell is stable HTML in index.html; we only fill the
  // indicator slots so sibling widgets (movers, 수급, 뉴스) aren't disturbed.
  const mainEl = document.getElementById('mdIndMain');
  const railEl = document.getElementById('mdIndRail');
  if (!mainEl || !railEl) return;
  const groups = _mdGroupByCategory(catalog);
  if (!groups.length) {
    mainEl.innerHTML = '<div class="md-loading">표시할 지표가 없습니다.</div>';
    return;
  }
  const main = [];
  const rail = [];
  for (const { category, codes } of groups) {
    const isHero = MD_HERO_CATEGORIES.includes(category);
    const html = _mdSectionHtml(category, codes, catalog, dataMap, isHero ? 'hero' : 'list');
    (isHero || MD_MAIN_CATEGORIES.includes(category) ? main : rail).push(html);
  }
  mainEl.innerHTML = main.join('');
  railEl.innerHTML = rail.join('');
}

async function loadInvestingDashboard(refresh = false) {
  // Sibling widgets load independently so a slow/failed indicator fetch never
  // blocks them (and vice versa).
  if (typeof loadMarketMovers === 'function') loadMarketMovers();
  if (typeof loadSectors === 'function') loadSectors();
  if (_mdInFlight) return _mdInFlight;
  _mdInFlight = (async () => {
    try {
      if (!_mdCatalog || refresh) {
        const cr = await apiFetch('/api/market-indicators');
        if (cr.ok) _mdCatalog = await cr.json();
      }
      const catalog = _mdCatalog || {};
      const codes = Object.keys(catalog);
      if (!codes.length) return;
      const sr = await apiFetch('/api/market-summary?codes=' + encodeURIComponent(codes.join(',')));
      const dataMap = sr.ok ? await sr.json() : {};
      _mdRenderDashboard(catalog, dataMap);
      _mdLoadedOnce = true;
    } catch (e) {
      console.warn('investing dashboard load failed', e);
      const mainEl = document.getElementById('mdIndMain');
      if (mainEl && !_mdLoadedOnce) {
        mainEl.innerHTML = '<div class="md-loading">시장 지표를 불러오지 못했습니다.</div>';
      }
    } finally {
      _mdInFlight = null;
    }
  })();
  return _mdInFlight;
}


// --- 시장 랭킹 (market movers): 시총상위 / 거래상위 / 급상승 / 급하락 ---
const MV_TABS = [
  { kind: 'market_cap', label: '시총상위' },
  { kind: 'volume', label: '거래상위' },
  { kind: 'rising', label: '급상승' },
  { kind: 'falling', label: '급하락' },
];
let _mvKind = 'market_cap';
let _mvMarket = 'kospi';
let _mvInFlight = false;

function _mvRenderShell(root) {
  const tabs = MV_TABS.map((t) =>
    `<button class="mv-tab${t.kind === _mvKind ? ' active' : ''}" data-kind="${t.kind}">${escapeHtml(t.label)}</button>`
  ).join('');
  const markets = [['kospi', '코스피'], ['kosdaq', '코스닥']].map(([m, label]) =>
    `<button class="mv-mkt${m === _mvMarket ? ' active' : ''}" data-market="${m}">${label}</button>`
  ).join('');
  root.innerHTML = '<section class="md-section mv-section">'
    + '<div class="mv-head"><h3 class="md-section-title">시장 랭킹</h3>'
    + `<div class="mv-mkts">${markets}</div></div>`
    + `<div class="mv-tabs">${tabs}</div>`
    + '<div class="mv-body"><div class="md-loading">불러오는 중...</div></div>'
    + '</section>';
  root.querySelectorAll('.mv-tab').forEach((b) =>
    b.addEventListener('click', () => { _mvKind = b.dataset.kind; loadMarketMovers(); }));
  root.querySelectorAll('.mv-mkt').forEach((b) =>
    b.addEventListener('click', () => { _mvMarket = b.dataset.market; loadMarketMovers(); }));
}

function _mvRenderRows(root, items) {
  const body = root.querySelector('.mv-body');
  if (!body) return;
  if (!items.length) {
    body.innerHTML = '<div class="md-loading">표시할 종목이 없습니다.</div>';
    return;
  }
  const showMetric = _mvKind === 'market_cap' || _mvKind === 'volume';
  body.innerHTML = items.map((it) => {
    const dirCls = it.direction === 'up' ? 'md-up' : (it.direction === 'down' ? 'md-down' : 'md-flat');
    const metric = showMetric && it.metric
      ? `<span class="mv-metric">${escapeHtml(String(it.metric))}</span>` : '';
    return `<button class="mv-row" data-code="${escapeHtml(String(it.code || ''))}">`
      + `<span class="mv-rank">${escapeHtml(String(it.rank || ''))}</span>`
      + `<span class="mv-name">${escapeHtml(String(it.name || ''))}</span>`
      + `<span class="mv-price">${escapeHtml(String(it.price || '-'))}</span>`
      + `<span class="mv-chg ${dirCls}">${escapeHtml(String(it.change_pct || ''))}</span>`
      + `${metric}</button>`;
  }).join('');
  body.querySelectorAll('.mv-row').forEach((b) =>
    b.addEventListener('click', () => {
      const code = b.dataset.code;
      if (!code) return;
      if (typeof switchView === 'function') switchView('analysis');
      if (typeof analyzeStock === 'function') analyzeStock(code);
    }));
}

// --- 업종별 등락 (sector performance) — rail widget ---
let _secInFlight = false;

function _secRenderRows(root, items) {
  if (!items.length) {
    root.innerHTML = '<section class="md-section"><h3 class="md-section-title">업종별 등락</h3>'
      + '<div class="md-loading">표시할 업종이 없습니다.</div></section>';
    return;
  }
  const rows = items.map((it) => {
    const dirCls = it.direction === 'up' ? 'md-up' : (it.direction === 'down' ? 'md-down' : 'md-flat');
    return `<div class="sec-row">`
      + `<span class="sec-name">${escapeHtml(String(it.name || ''))}</span>`
      + `<span class="sec-chg ${dirCls}">${escapeHtml(String(it.change_pct || ''))}</span></div>`;
  }).join('');
  root.innerHTML = '<section class="md-section"><h3 class="md-section-title">업종별 등락</h3>'
    + `<div class="sec-rows">${rows}</div></section>`;
}

async function loadSectors() {
  const root = document.getElementById('marketSectors');
  if (!root || _secInFlight) return;
  _secInFlight = true;
  try {
    const r = await apiFetch('/api/market/sectors?limit=12');
    const data = r.ok ? await r.json() : { sectors: [] };
    _secRenderRows(root, data.sectors || []);
  } catch (e) {
    console.warn('sectors load failed', e);
  } finally {
    _secInFlight = false;
  }
}

async function loadMarketMovers() {
  const root = document.getElementById('marketMovers');
  if (!root || _mvInFlight) return;
  _mvInFlight = true;
  _mvRenderShell(root);
  try {
    const r = await apiFetch(`/api/market/movers?kind=${encodeURIComponent(_mvKind)}&market=${encodeURIComponent(_mvMarket)}&limit=10`);
    const data = r.ok ? await r.json() : { items: [] };
    _mvRenderRows(root, data.items || []);
  } catch (e) {
    console.warn('market movers load failed', e);
    const body = root.querySelector('.mv-body');
    if (body) body.innerHTML = '<div class="md-loading">불러오지 못했습니다.</div>';
  } finally {
    _mvInFlight = false;
  }
}
