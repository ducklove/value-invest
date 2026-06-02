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
  // abs/pct 분리: 좁은 화면(모바일)에서는 절대값을 숨기고 등락%만 노출한다.
  return { cls, text: [chgVal, chgPct].filter(Boolean).join(' '), abs: chgVal, pct: chgPct };
}

// 투자정보 대시보드 표시에서만 제외할 코드 (데이터 수집·금일시황 등 다른 경로는 유지).
// KOSPI200 은 국내 지수 카드에서 자리가 어색하고 중요도가 낮아 숨긴다.
const MD_HIDDEN_CODES = new Set(['KOSPI200']);

function _mdGroupByCategory(catalog) {
  const groups = {};
  for (const [code, meta] of Object.entries(catalog || {})) {
    if (MD_HIDDEN_CODES.has(code)) continue;
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

// 국내 지수 hero 카드 우상단에 띄울 네이버 일간 미니 차트(코드=네이버 심볼).
// down 변형 파일은 없고 _end_up_tablet.png 만 존재하며, 이미지 내용 자체가
// 당일 등락을 반영한다. 캐시버스팅을 5분 단위로 둬 최신 차트를 받되 과도한
// 재요청은 피한다.
const MD_MINI_CHART = new Set(['KOSPI', 'KOSDAQ']);

function _miniChartHtml(code) {
  if (!MD_MINI_CHART.has(code)) return '';
  const bust = Math.floor(Date.now() / 300000);
  const url = `https://ssl.pstatic.net/imgfinance/chart/mobile/mini/${code}_end_up_tablet.png?${bust}`;
  return `<img class="md-hero-chart" src="${escapeHtml(url)}" alt="${escapeHtml(code)} 일간 추이"`
    + ` loading="lazy" onerror="this.style.display='none'">`;
}

function _mdCardHtml(code, catalog, dataMap, variant) {
  const meta = catalog[code] || {};
  const label = meta.label || code;
  const d = dataMap ? dataMap[code] : null;
  let valHtml = '-';
  let chgHtml = '';      // hero: 절대값+%(full)
  // list: 전일대비를 못 구한 항목도 자리(정렬)는 지키고 '-' placeholder 로 표시.
  let rowChgHtml = '<span class="md-chg md-flat">-</span>';
  if (d && d.value) {
    const c = _mdChange(d);
    valHtml = escapeHtml(String(d.value));
    chgHtml = c.text ? `<span class="md-chg ${c.cls}">${escapeHtml(c.text)}</span>` : '';
    if (c.text) {
      rowChgHtml = `<span class="md-chg ${c.cls}">`
        + (c.abs ? `<span class="md-chg-abs">${escapeHtml(c.abs)} </span>` : '')
        + `<span class="md-chg-pct">${escapeHtml(c.pct)}</span></span>`;
    }
  }
  if (variant === 'hero') {
    // 카드 안에 해당 시장 수급 슬롯을 둔다. 캐시값이 있으면 즉시 채우고,
    // loadInvestorFlows()가 최신값으로 갱신한다(없으면 빈 슬롯).
    const flow = _mdFlows ? _mdFlows[String(code).toLowerCase()] : null;
    const flowSlot = `<div class="md-card-flow" data-flow-code="${escapeHtml(String(code))}">${_cardFlowHtml(flow)}</div>`;
    return `<div class="md-hero-card">`
      + _miniChartHtml(code)
      + `<div class="md-hero-label">${escapeHtml(label)}</div>`
      + `<div class="md-hero-val">${valHtml}</div>${chgHtml}`
      + flowSlot + `</div>`;
  }
  return `<div class="md-row">`
    + `<span class="md-row-label">${escapeHtml(label)}</span>`
    + `<span class="md-row-val">${valHtml}</span>${rowChgHtml}</div>`;
}

// 최근 투자자별 순매수(개인/외국인/기관). 각 국내 지수 카드(코스피/코스닥)
// 안에 해당 시장 수급을 넣는다. {kospi, kosdaq} 형태이며 코드를 소문자화해
// 매칭한다(KOSPI→kospi).
let _mdFlows = null;
let _flowsInFlight = false;

function _cardFlowHtml(flow) {
  if (!flow) return '';
  const actors = [['individual', '개인'], ['foreign', '외국인'], ['institution', '기관']];
  const rows = actors.map(([k, name]) => {
    const d = flow[k] || {};
    const cls = d.direction === 'up' ? 'md-up' : (d.direction === 'down' ? 'md-down' : 'md-flat');
    return `<div class="cf-row"><span class="cf-actor">${name}</span>`
      + `<span class="cf-val ${cls}">${escapeHtml(String(d.value || '-'))}</span></div>`;
  }).join('');
  return `<div class="cf-head">순매수<span class="cf-date">${escapeHtml(String(flow.date || ''))} · 억</span></div>${rows}`;
}

async function loadInvestorFlows() {
  if (_flowsInFlight) return;
  _flowsInFlight = true;
  try {
    const r = await apiFetch('/api/market/investor-flows');
    const data = r.ok ? await r.json() : {};
    _mdFlows = data.flows || null;
    document.querySelectorAll('.md-card-flow[data-flow-code]').forEach((el) => {
      const f = _mdFlows ? _mdFlows[String(el.dataset.flowCode).toLowerCase()] : null;
      el.innerHTML = _cardFlowHtml(f);
    });
  } catch (e) {
    console.warn('investor flows load failed', e);
  } finally {
    _flowsInFlight = false;
  }
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
  if (typeof loadMarketNews === 'function') loadMarketNews();
  if (typeof loadExternalInsights === 'function') loadExternalInsights();
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
      // 수급 슬롯은 hero 섹션과 함께 생성되므로 렌더 직후 채운다.
      if (typeof loadInvestorFlows === 'function') loadInvestorFlows();
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

// --- 분석 도구 (external insights) — 외부 GitHub Pages 도구 요약 허브 ---
// 지주사 NAV 디스카운트 / 우선주 괴리율 / 김치프리미엄. public JSON 요약을
// 한 섹션에 카드로 묶고, 항목 클릭 시 해당 도구로(새 탭, 가능하면 deep-link).
let _extInFlight = false;

function _extSafeUrl(url) {
  return /^https?:\/\//.test(String(url || '')) ? String(url) : '#';
}

function _extPct(v, signed) {
  if (v === null || v === undefined || v === '') return '-';
  const n = Number(v);
  if (!isFinite(n)) return escapeHtml(String(v));
  return (signed && n > 0 ? '+' : '') + n.toFixed(signed ? 2 : 1) + '%';
}

function _extLinkRows(rows, valKey, baseUrl, useCode) {
  return (rows || []).map((r) => {
    // holding 도구만 ?code= deep-link 지원. 그 외엔 도구 홈으로.
    const href = useCode && r.code ? `${baseUrl}?code=${encodeURIComponent(r.code)}` : baseUrl;
    return `<a class="ext-row" href="${escapeHtml(_extSafeUrl(href))}" target="_blank" rel="noopener noreferrer">`
      + `<span class="ext-name">${escapeHtml(String(r.name || r.code || ''))}</span>`
      + `<span class="ext-val">${escapeHtml(_extPct(r[valKey]))}</span></a>`;
  }).join('');
}

function _extCard(title, url, subText, bodyHtml) {
  return '<div class="ext-card">'
    + `<div class="ext-head"><span>${escapeHtml(title)}</span>`
    + `<a href="${escapeHtml(_extSafeUrl(url))}" target="_blank" rel="noopener noreferrer" class="ext-more" title="도구 열기">↗</a></div>`
    + (subText ? `<div class="ext-sub">${escapeHtml(subText)}</div>` : '')
    + `<div class="ext-rows">${bodyHtml}</div></div>`;
}

function _extRender(root, data) {
  const cards = [];
  const h = data && data.holding;
  if (h && (h.top || []).length) {
    const sub = h.averageRatio != null ? `평균 ${_extPct(h.averageRatio)} · 보유가치/시총` : '보유가치/시총';
    cards.push(_extCard('지주사 저평가', h.url, sub, _extLinkRows(h.top, 'ratio', h.url, true)));
  }
  const s = data && data.spread;
  if (s && (s.top || []).length) {
    const sub = s.averageSpread != null ? `평균 괴리율 ${_extPct(s.averageSpread)}` : '우선주 괴리율';
    cards.push(_extCard('우선주 괴리율', s.url, sub, _extLinkRows(s.top, 'spread', s.url, false)));
  }
  const p = data && data.spac;
  if (p && (p.top || []).length) {
    // spac-hunter 는 ?code= deep-link 지원 → useCode=true.
    const sub = p.averageAnnualizedReturn != null ? `평균 연환산 ${_extPct(p.averageAnnualizedReturn)}` : '연환산 기대수익률';
    cards.push(_extCard('스팩 기대수익', p.url, sub, _extLinkRows(p.top, 'annualizedReturn', p.url, true)));
  }
  const g = data && data.goldGap;
  if (g && (g.assets || []).length) {
    const rows = g.assets.map((a) => {
      const n = Number(a.gap);
      const cls = isFinite(n) ? (n > 0 ? 'md-up' : (n < 0 ? 'md-down' : 'md-flat')) : 'md-flat';
      return `<a class="ext-row" href="${escapeHtml(_extSafeUrl(a.link || g.url))}" target="_blank" rel="noopener noreferrer">`
        + `<span class="ext-name">${escapeHtml(String(a.label || a.key || ''))}</span>`
        + `<span class="ext-val ${cls}">${escapeHtml(_extPct(a.gap, true))}</span></a>`;
    }).join('');
    cards.push(_extCard('김치프리미엄', g.url, '국내가 vs 국제가', rows));
  }
  if (!cards.length) {
    root.innerHTML = '';
    return;
  }
  root.innerHTML = '<section class="md-section ext-section">'
    + '<h3 class="md-section-title">분석 도구</h3>'
    + `<div class="ext-grid">${cards.join('')}</div></section>`;
}

async function loadExternalInsights() {
  const root = document.getElementById('externalTools');
  if (!root || _extInFlight) return;
  _extInFlight = true;
  try {
    const r = await apiFetch('/api/external/insights');
    const data = r.ok ? await r.json() : {};
    _extRender(root, data);
  } catch (e) {
    console.warn('external insights load failed', e);
  } finally {
    _extInFlight = false;
  }
}

// --- 주요 뉴스 (market news) — main-column widget ---
let _newsInFlight = false;

function _newsRender(root, items) {
  if (!items.length) {
    root.innerHTML = '<section class="md-section"><h3 class="md-section-title">주요 뉴스</h3>'
      + '<div class="md-loading">표시할 뉴스가 없습니다.</div></section>';
    return;
  }
  const rows = items.map((it) => {
    const meta = [it.source, it.date].filter(Boolean).map((s) => escapeHtml(String(s))).join(' · ');
    const summ = it.summary
      ? `<div class="news-summary">${escapeHtml(String(it.summary))}</div>` : '';
    const url = String(it.url || '');
    const safeUrl = /^https?:\/\//.test(url) ? url : '#';
    return `<a class="news-item" href="${escapeHtml(safeUrl)}" target="_blank" rel="noopener noreferrer">`
      + `<div class="news-title">${escapeHtml(String(it.title || ''))}</div>`
      + summ
      + (meta ? `<div class="news-meta">${meta}</div>` : '')
      + '</a>';
  }).join('');
  root.innerHTML = '<section class="md-section"><h3 class="md-section-title">주요 뉴스</h3>'
    + `<div class="news-list">${rows}</div></section>`;
}

async function loadMarketNews() {
  const root = document.getElementById('marketNews');
  if (!root || _newsInFlight) return;
  _newsInFlight = true;
  try {
    const r = await apiFetch('/api/market/news?limit=8');
    const data = r.ok ? await r.json() : { news: [] };
    _newsRender(root, data.news || []);
  } catch (e) {
    console.warn('market news load failed', e);
  } finally {
    _newsInFlight = false;
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
