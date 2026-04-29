// Portfolio data loading, sorting/filtering state, and in-place quote row updates.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
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
    } catch (e) { console.warn(e); }
    // Restore benchmark names from localStorage cache for instant display
    try {
      const cached = JSON.parse(localStorage.getItem('pfBenchmarkNames') || '{}');
      for (const [k, v] of Object.entries(cached)) {
        pfBenchmarkQuotes[k] = { ...(pfBenchmarkQuotes[k] || {}), name: v };
      }
    } catch (e) { console.warn(e); }
    // Fetch benchmark quotes in background (don't block initial render)
    apiFetch('/api/portfolio/prev-day-snapshot').then(async r => {
      if (!r.ok) return;
      pfPrevDaySnapshot = await r.json();
      renderPortfolio();
    }).catch(() => {});
    apiFetch('/api/portfolio/month-end-value').then(async r => {
      if (!r.ok) return;
      const snap = await r.json();
      pfMonthEndSnap = snap && snap.total_value ? snap : null;
      pfMonthEndStockValues = snap.stock_values || {};
      renderPortfolio();
    }).catch(() => {});
    apiFetch('/api/portfolio/year-start-value').then(async r => {
      if (!r.ok) return;
      const snap = await r.json();
      pfYearStartSnap = snap && snap.total_value ? snap : null;
      pfYearStartStockValues = (snap && snap.stock_values) || {};
      renderPortfolio();
    }).catch(() => {});
    apiFetch('/api/portfolio/nav-history').then(async r => {
      if (!r.ok) return;
      pfNavHistory = await r.json();
      renderPortfolio();
    }).catch(() => {});
    apiFetch('/api/portfolio/intraday').then(async r => {
      if (!r.ok) return;
      pfIntradayData = await r.json();
      renderPortfolio();
    }).catch(() => {});
    apiFetch('/api/portfolio/benchmark-quotes').then(async r => {
      if (!r.ok) return;
      const fresh = await r.json();
      for (const [k, v] of Object.entries(fresh)) pfBenchmarkQuotes[k] = v;
      // Save names to localStorage
      const names = {};
      for (const [k, v] of Object.entries(pfBenchmarkQuotes)) { if (v.name) names[k] = v.name; }
      try { localStorage.setItem('pfBenchmarkNames', JSON.stringify(names)); } catch (e) { console.warn(e); }
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
  } catch (e) { console.warn(e); } finally {
    portfolioLoading = false;
  }
}

function pfSort(key) {
  // 클릭 순환: 없음 → 내림차순 → 올림차순 → 없음. 모든 컬럼 동일.
  // 이전 구현은 첫 클릭 후 desc → none 을 반복하며 asc 단계로 아예
  // 넘어가지 않는 버그가 있었다.
  if (key === 'group') {
    pfGroupSort = !pfGroupSort;
  } else if (pfSortKey === key) {
    if (!pfSortAsc) {
      // 현재 내림차순 → 올림차순
      pfSortAsc = true;
    } else {
      // 현재 올림차순 → 해제
      pfSortKey = null;
      pfSortAsc = true;
    }
  } else {
    pfSortKey = key;
    pfSortAsc = false;   // 첫 클릭은 내림차순 (이름 포함 통일)
  }
  renderPortfolio();
}

function pfGetGroup(item) {
  return item.group_name || '기타';
}

function pfGetTags(item) {
  return Array.isArray(item?.tags) ? item.tags : [];
}

function _renderPortfolioRowTags(tags) {
  const safeTags = Array.isArray(tags) ? tags.filter(Boolean).slice(0, 3) : [];
  if (!safeTags.length) return '';
  const more = Array.isArray(tags) && tags.length > safeTags.length
    ? `<span class="pf-stock-tag more">+${tags.length - safeTags.length}</span>`
    : '';
  return `<div class="pf-stock-tags">${safeTags.map(tag => `<span class="pf-stock-tag">${escapeHtml(tag)}</span>`).join('')}${more}</div>`;
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

// WS tick 마다 renderPortfolio() 로 tbody 전체를 교체하면 마우스 커서
// 아래 tr 이 매 tick 재생성되면서 :hover transition 이 fresh 시작 →
// '커서 있는 행이 깜빡임' 문제. 영향 받는 셀만 in-place 로 덮어 쓰고
// flash 클래스를 그 행에만 붙여 갱신된 행만 번쩍이게 한다.
function updatePortfolioRowQuote(code, shouldFlash = true) {
  const tbody = document.getElementById('pfBody');
  if (!tbody) return;
  let tr = null;
  const rows = tbody.querySelectorAll('tr[data-code]');
  for (const t of rows) {
    if (t.dataset.code === code) { tr = t; break; }
  }
  if (!tr) return;
  if (tr.querySelector('input.pf-edit-input')) return;
  const item = portfolioItems.find(i => i.stock_code === code);
  if (!item) return;

  const q = item.quote || {};
  const price = q.price ?? null;
  const change = q.change ?? 0;
  const changePct = q.change_pct ?? null;
  const qty = item.quantity;
  const avgPrice = item.avg_price;
  const marketValue = price !== null ? qty * price : null;
  const rawReturn = avgPrice > 0 && price !== null ? ((price - avgPrice) / avgPrice * 100) : null;
  const returnPct = rawReturn !== null && qty < 0 ? -rawReturn : rawReturn;
  const tradingValue = (q.trade_value !== undefined && q.trade_value !== null) ? Number(q.trade_value) : null;
  const trailingDps = item.trailing_dps ?? null;
  const dividendYield = (trailingDps !== null && price !== null && price > 0 && qty > 0)
    ? (trailingDps / price * 100) : null;
  // _computeTargetPrice 호출 — renderPortfolio 경로와 동일 모양의 첫 인자
  // + portfolioItems (각 item 의 quote.price 로 조회). pfItem.quote 는
  // 이미 onQuote 에서 최신값으로 업데이트된 상태.
  const rowLike = { ...item, price };
  const targetPrice = _computeTargetPrice(rowLike, portfolioItems);
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
  tbody.querySelectorAll('tr[data-code]').forEach(tr => {
    if (tr.querySelector('input.pf-edit-input')) return;
    const pfCode = tr.dataset.code;
    const item = portfolioItems.find(i => i.stock_code === pfCode);
    if (!item || item.benchmark_code !== code) return;
    const cell = tr.querySelector('.pf-col-benchmark');
    if (cell) cell.innerHTML = fmtBenchmarkPct(code) + `<span class="pf-benchmark-name">${escapeHtml(benchmarkName(code || ''))}</span>`;
  });
}
