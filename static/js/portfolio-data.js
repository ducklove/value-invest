// Portfolio data loading, sorting/filtering state, and in-place quote row updates.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
let _pfNavHistoryPromise = null;
let _pfTodayStatePromise = null;
const _PF_PORTFOLIO_SNAPSHOT_KEY = 'valueInvestPortfolioSnapshot:v2';
const _PF_PORTFOLIO_SNAPSHOT_QUOTE_TTL_MS = 2 * 60 * 1000;

async function pfLoadNavHistory({ force = false } = {}) {
  if (!force && Array.isArray(PfStore.navHistory) && PfStore.navHistory.length) return PfStore.navHistory;
  if (_pfNavHistoryPromise) return _pfNavHistoryPromise;
  _pfNavHistoryPromise = (async () => {
    const resp = await apiFetch('/api/portfolio/nav-history');
    if (!resp.ok) {
      const message = resp.status === 401
        ? '로그인 후 심층 분석 데이터를 확인할 수 있습니다.'
        : `NAV history request failed (${resp.status})`;
      const err = new Error(message);
      err.status = resp.status;
      throw err;
    }
    const rows = await resp.json();
    PfStore.navHistory = Array.isArray(rows) ? rows : [];
    return PfStore.navHistory;
  })().finally(() => {
    _pfNavHistoryPromise = null;
  });
  return _pfNavHistoryPromise;
}

async function pfRefreshTodayState({ force = false, render = true } = {}) {
  if (_pfTodayStatePromise && !force) return _pfTodayStatePromise;
  _pfTodayStatePromise = (async () => {
    const [snapshotResult, intradayResult] = await Promise.allSettled([
      apiFetch('/api/portfolio/prev-day-snapshot', { cache: 'no-store' }),
      apiFetch('/api/portfolio/intraday', { cache: 'no-store' }),
    ]);
    let updated = false;
    if (snapshotResult.status === 'fulfilled' && snapshotResult.value.ok) {
      PfStore.snapshots.prevDay = await snapshotResult.value.json();
      updated = true;
    }
    if (intradayResult.status === 'fulfilled' && intradayResult.value.ok) {
      PfStore.snapshots.intraday = await intradayResult.value.json();
      updated = true;
    }
    if (updated && render) renderPortfolio();
    return { updated };
  })().catch(e => {
    reportApiError(e, '오늘 수익 데이터', { silent: true });
    return { updated: false };
  }).finally(() => {
    _pfTodayStatePromise = null;
  });
  return _pfTodayStatePromise;
}

async function loadPortfolio({ force = false } = {}) {
  if (PfStore.loading) return;
  const loadOrderRevision = PfStore.manualOrder.revision;
  const preservePendingManualOrder = !!PfStore.manualOrder.pendingCodes;
  PfStore.loading = true;
  try {
    _restorePortfolioSnapshotForFastPaint();
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
      if (gResp.ok) PfStore.groups = await gResp.json();
    } catch (e) { console.warn(e); }
    // Restore benchmark names from localStorage cache for instant display
    try {
      const cached = JSON.parse(localStorage.getItem('pfBenchmarkNames') || '{}');
      for (const [k, v] of Object.entries(cached)) {
        PfStore.benchmarkQuotes[k] = { ...(PfStore.benchmarkQuotes[k] || {}), name: v };
      }
    } catch (e) { console.warn(e); }
    // Refresh the 22:00 settlement baseline before the fresh portfolio render.
    // Cashflows mutate CASH_KRW immediately, so Today must combine the fresh
    // holdings and the fresh cashflow-adjusted baseline in the same paint.
    const todayStatePromise = pfRefreshTodayState({ force: true, render: false }).catch(() => ({ updated: false }));
    apiFetch('/api/portfolio/month-end-value').then(async r => {
      if (!r.ok) return;
      const snap = await r.json();
      PfStore.snapshots.monthEnd = snap && snap.total_value ? snap : null;
      PfStore.snapshots.monthEndStockValues = snap.stock_values || {};
      renderPortfolio();
    }).catch(e => reportApiError(e, '월말 평가액', { silent: true }));
    apiFetch('/api/portfolio/year-start-value').then(async r => {
      if (!r.ok) return;
      const snap = await r.json();
      PfStore.snapshots.yearStart = snap && snap.total_value ? snap : null;
      PfStore.snapshots.yearStartStockValues = (snap && snap.stock_values) || {};
      renderPortfolio();
    }).catch(e => reportApiError(e, '연초 평가액', { silent: true }));
    pfLoadNavHistory({ force: true }).then(() => {
      renderPortfolio();
    }).catch(e => reportApiError(e, 'NAV 히스토리', { silent: true }));
    apiFetch('/api/portfolio/benchmark-quotes').then(async r => {
      if (!r.ok) return;
      const fresh = await r.json();
      for (const [k, v] of Object.entries(fresh)) pfMergeBenchmarkQuote(k, v);
      // Save names to localStorage
      const names = {};
      for (const [k, v] of Object.entries(PfStore.benchmarkQuotes)) { if (v.name) names[k] = v.name; }
      try { localStorage.setItem('pfBenchmarkNames', JSON.stringify(names)); } catch (e) { console.warn(e); }
      renderPortfolio();
    }).catch(e => reportApiError(e, '벤치마크 시세', { silent: true }));
    // Preserve existing quotes from previous load
    const prevQuotes = {};
    PfStore.items.forEach(i => { if (quoteIsUsable(i.quote)) prevQuotes[i.stock_code] = i.quote; });
    let nextPortfolioItems = freshItems.map(item => {
      const prevQuote = prevQuotes[item.stock_code];
      if (quoteIsUsable(prevQuote) && quoteIsUsable(item.quote)) {
        item.quote = mergeQuoteSnapshot(prevQuote, item.quote);
      } else if (!quoteIsUsable(item.quote)) {
        item.quote = prevQuote || item.quote;
      }
      return item;
    });
    if (PfStore.manualOrder.pendingCodes && (preservePendingManualOrder || PfStore.manualOrder.revision > loadOrderRevision)) {
      nextPortfolioItems = pfApplyManualOrder(nextPortfolioItems, PfStore.manualOrder.pendingCodes);
    }
    PfStore.items = nextPortfolioItems;
    await todayStatePromise;
    _savePortfolioSnapshot(PfStore.items);
    renderPortfolio();
    _updateQuoteSubscriptions();
  } catch (e) { console.warn(e); } finally {
    PfStore.loading = false;
  }
}

function _restorePortfolioSnapshotForFastPaint() {
  if (PfStore.items.length) return;
  try {
    const snapshot = JSON.parse(localStorage.getItem(_PF_PORTFOLIO_SNAPSHOT_KEY) || 'null');
    if (!snapshot || !Array.isArray(snapshot.items) || !snapshot.items.length) return;
    const savedAt = Number(snapshot.savedAt || 0);
    const quotesExpired = !savedAt || (Date.now() - savedAt) > _PF_PORTFOLIO_SNAPSHOT_QUOTE_TTL_MS;
    PfStore.items = snapshot.items.map(item => {
      if (!quotesExpired || !item.quote) return item;
      return { ...item, quote: { ...item.quote, _stale: true } };
    });
    renderPortfolio();
    _updateQuoteSubscriptions();
  } catch (e) { console.warn(e); }
}

function _savePortfolioSnapshot(items) {
  try {
    localStorage.setItem(_PF_PORTFOLIO_SNAPSHOT_KEY, JSON.stringify({
      savedAt: Date.now(),
      items: Array.isArray(items) ? items.slice(0, 300) : [],
    }));
  } catch (e) { console.warn(e); }
}

function pfApplyManualOrder(items, orderedCodes) {
  if (!Array.isArray(items) || !items.length || !Array.isArray(orderedCodes) || !orderedCodes.length) return items;
  const rank = new Map();
  orderedCodes.forEach((code, index) => {
    const normalized = String(code || '').trim();
    if (normalized && !rank.has(normalized)) rank.set(normalized, index);
  });
  if (!rank.size) return items;
  return items.map((item, index) => ({ item, index })).sort((a, b) => {
    const ar = rank.has(a.item.stock_code) ? rank.get(a.item.stock_code) : Number.POSITIVE_INFINITY;
    const br = rank.has(b.item.stock_code) ? rank.get(b.item.stock_code) : Number.POSITIVE_INFINITY;
    return ar === br ? a.index - b.index : ar - br;
  }).map(entry => entry.item);
}

function pfSort(key) {
  // 클릭 순환: 없음 → 내림차순 → 올림차순 → 없음. 모든 컬럼 동일.
  // 이전 구현은 첫 클릭 후 desc → none 을 반복하며 asc 단계로 아예
  // 넘어가지 않는 버그가 있었다.
  if (key === 'group') {
    PfStore.sort.groupSort = !PfStore.sort.groupSort;
  } else if (PfStore.sort.key === key) {
    if (!PfStore.sort.asc) {
      // 현재 내림차순 → 올림차순
      PfStore.sort.asc = true;
    } else {
      // 현재 올림차순 → 해제
      PfStore.sort.key = null;
      PfStore.sort.asc = true;
    }
  } else {
    PfStore.sort.key = key;
    PfStore.sort.asc = false;   // 첫 클릭은 내림차순 (이름 포함 통일)
  }
  renderPortfolio();
}

function pfGetGroup(item) {
  return item.group_name || '기타';
}

function pfGetTags(item) {
  return Array.isArray(item?.tags) ? item.tags : [];
}

function pfNormalizeSearchText(value) {
  return String(value || '').trim().toLowerCase().replace(/^#/, '');
}

function pfRowMatchesSearch(item, query = PfStore.filters.searchText) {
  const text = pfNormalizeSearchText(query);
  if (!text) return true;
  const tokens = text.split(/[\s,]+/).map(t => t.trim()).filter(Boolean);
  if (!tokens.length) return true;
  const haystack = [
    item?.stock_name,
    item?.stock_code,
    pfGetGroup(item),
    ...pfGetTags(item),
  ].map(v => String(v || '').toLowerCase()).join(' ');
  return tokens.every(token => haystack.includes(token));
}

function pfSetPortfolioSearchText(value) {
  PfStore.filters.searchText = String(value || '').trim();
  renderPortfolio();
}

function _renderPortfolioRowTags(tags) {
  const safeTags = Array.isArray(tags) ? tags.filter(Boolean).slice(0, 3) : [];
  if (!safeTags.length) return '';
  const more = Array.isArray(tags) && tags.length > safeTags.length
    ? `<span class="pf-stock-tag more">+${tags.length - safeTags.length}</span>`
    : '';
  return `<div class="pf-stock-tags">${safeTags.map(tag => {
    const safeTag = escapeHtml(tag);
    return `<button type="button" class="pf-stock-tag js-pf-open-tag-summary" data-tag="${safeTag}" title="#${safeTag}">${safeTag}</button>`;
  }).join('')}${more}</div>`;
}

function pfToggleGroupFilter(groupName) {
  if (PfStore.filters.group === null) {
    PfStore.filters.group = new Set([groupName]);
  } else if (PfStore.filters.group.has(groupName)) {
    PfStore.filters.group.delete(groupName);
    if (PfStore.filters.group.size === 0) PfStore.filters.group = null;
  } else {
    PfStore.filters.group.add(groupName);
    if (PfStore.groups.length && PfStore.filters.group.size === PfStore.groups.length) PfStore.filters.group = null;
  }
  renderPortfolio();
}

// WS tick 마다 renderPortfolio() 로 tbody 전체를 교체하면 마우스 커서
// 아래 tr 이 매 tick 재생성되면서 :hover transition 이 fresh 시작 →
// '커서 있는 행이 깜빡임' 문제. 영향 받는 셀만 in-place 로 덮어 쓰고
// flash 클래스를 그 행에만 붙여 갱신된 행만 번쩍이게 한다.
function updatePortfolioRowQuote(code, shouldFlash = true) {
  const tbody = document.getElementById('pfBody');
  if (!tbody) return;
  // Hot path (runs per quote tick): let the browser match the row by attribute
  // instead of materializing every tr[data-code] and scanning in JS. Querying
  // live each call keeps it correct across re-render / reorder / edit — no
  // persistent index to invalidate. CSS.escape guards codes with dots etc.
  const tr = tbody.querySelector(`tr[data-code="${CSS.escape(code)}"]`);
  if (!tr) return;
  if (tr.querySelector('input.pf-edit-input')) return;
  const item = PfStore.items.find(i => i.stock_code === code);
  if (!item) return;

  const q = item.quote || {};
  const price = quotePriceOrNull(q);
  const change = price !== null ? (q.change ?? 0) : 0;
  const changePct = price !== null ? (q.change_pct ?? null) : null;
  const qty = item.quantity;
  const avgPrice = pfAvgPriceKrw(item);
  const marketValue = price !== null ? qty * price : null;
  const rawReturn = avgPrice > 0 && price !== null ? ((price - avgPrice) / avgPrice * 100) : null;
  const returnPct = rawReturn !== null && qty < 0 ? -rawReturn : rawReturn;
  const tradingValue = (price !== null && q.trade_value !== undefined && q.trade_value !== null) ? Number(q.trade_value) : null;
  const trailingDps = item.trailing_dps ?? null;
  const dividendYield = (trailingDps !== null && price !== null && price > 0 && qty > 0)
    ? (trailingDps / price * 100) : null;
  // _computeTargetPrice 호출 — renderPortfolio 경로와 동일 모양의 첫 인자
  // + PfStore.items (각 item 의 quote.price 로 조회). pfItem.quote 는
  // 이미 onQuote 에서 최신값으로 업데이트된 상태.
  const rowLike = { ...item, price };
  const targetPrice = _computeTargetPrice(rowLike, PfStore.items);
  const achievementPct = (targetPrice != null && targetPrice > 0 && price != null)
    ? (price / targetPrice * 100) : null;

  const setText = (sel, txt) => { const el = tr.querySelector(sel); if (el) el.textContent = txt; };
  const setHtml = (sel, html) => { const el = tr.querySelector(sel); if (el) el.innerHTML = html; };

  setText('.pf-col-curprice', price !== null ? pfFmtPortfolioValue(price) : '-');
  setHtml('.pf-col-changepct', fmtChangePct(changePct, change));
  setHtml('.pf-col-return', `<span class="pf-return ${returnClass(returnPct)}">${returnPct !== null ? fmtPct(returnPct) : '-'}</span>`);
  setText('.pf-col-mktval', marketValue !== null ? pfFmtPortfolioValue(marketValue) : '-');
  setText('.pf-col-invested', tradingValue !== null ? fmtKrw(tradingValue) : '-');
  setText('.pf-col-divyield', dividendYield !== null ? fmtPct(dividendYield, false) : '-');
  setText('.pf-col-target', targetPrice !== null ? pfFmtPortfolioValue(targetPrice) : '-');
  setText('.pf-col-achiev', achievementPct !== null ? fmtPct(achievementPct, false) : '-');

  if (shouldFlash) flashEl(tr);
}

// 벤치마크 tick 전용: 같은 benchmark_code 를 쓰는 모든 행의 벤치마크
// 셀만 갱신. renderPortfolio 전체 재호출 없이.
function updatePortfolioBenchmarkCells(code) {
  const tbody = document.getElementById('pfBody');
  if (!tbody) return;
  // Build a code→item index once per call (O(N)) instead of a linear
  // PfStore.items.find per row (which made this O(N²) per benchmark tick).
  // Local-only, rebuilt each call, so there is no stale-index risk.
  const byCode = new Map(PfStore.items.map(i => [i.stock_code, i]));
  tbody.querySelectorAll('tr[data-code]').forEach(tr => {
    if (tr.querySelector('input.pf-edit-input')) return;
    const pfCode = tr.dataset.code;
    const item = byCode.get(pfCode);
    if (!item || item.benchmark_code !== code) return;
    const cell = tr.querySelector('.pf-col-benchmark');
    if (cell) cell.innerHTML = fmtBenchmarkPct(code) + `<span class="pf-benchmark-name">${escapeHtml(benchmarkName(code || ''))}</span>`;
  });
}
