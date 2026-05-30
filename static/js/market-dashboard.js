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

function _mdRenderDashboard(catalog, dataMap) {
  const root = document.getElementById('marketDashboard');
  if (!root) return;
  const groups = _mdGroupByCategory(catalog);
  let html = '';
  for (const { category, codes } of groups) {
    let cards = '';
    for (const code of codes) {
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
      cards += `<div class="md-card">`
        + `<div class="md-card-label">${escapeHtml(label)}</div>`
        + `<div class="md-card-val">${valHtml}</div>`
        + `${chgHtml}`
        + `</div>`;
    }
    html += `<section class="md-group">`
      + `<h3 class="md-group-title">${escapeHtml(category)}</h3>`
      + `<div class="md-cards">${cards}</div>`
      + `</section>`;
  }
  root.innerHTML = html || '<div class="md-loading">표시할 지표가 없습니다.</div>';
}

async function loadInvestingDashboard(refresh = false) {
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
      const root = document.getElementById('marketDashboard');
      if (root && !_mdLoadedOnce) {
        root.innerHTML = '<div class="md-loading">시장 지표를 불러오지 못했습니다.</div>';
      }
    } finally {
      _mdInFlight = null;
    }
  })();
  return _mdInFlight;
}
