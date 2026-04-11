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

// --- Column visibility ---
const PF_COL_DEFS = [
  { key: 'group',     cls: 'pf-col-group',     label: '그룹' },
  { key: 'benchmark', cls: 'pf-col-benchmark',  label: '벤치마크' },
  { key: 'buyprice',  cls: 'pf-col-buyprice',   label: '매입가' },
  { key: 'curprice',  cls: 'pf-col-curprice',   label: '현재가' },
  { key: 'qty',       cls: 'pf-col-qty',        label: '수량' },
  { key: 'return',    cls: 'pf-col-return',      label: '수익률' },
  { key: 'mktval',    cls: 'pf-col-mktval',     label: '평가금액' },
  { key: 'weight',    cls: 'pf-col-weight',      label: '비중' },
];
let _pfColStyleEl = null;

function _pfLoadColVisibility() {
  try { return JSON.parse(localStorage.getItem('pf_col_vis') || 'null'); } catch { return null; }
}
function _pfSaveColVisibility(vis) {
  localStorage.setItem('pf_col_vis', JSON.stringify(vis));
}
function _pfGetColVisibility() {
  return _pfLoadColVisibility() || Object.fromEntries(PF_COL_DEFS.map(c => [c.key, true]));
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
    `<label><input type="checkbox" ${vis[c.key] ? 'checked' : ''} onchange="pfToggleCol('${c.key}', this.checked)"> ${c.label}</label>`
  ).join('');
  _pfColTogglesRendered = true;
}

function switchView(view) {
  activeView = view;
  document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.view === view));
  const analysisView = document.getElementById('analysisView');
  const portfolioView = document.getElementById('portfolioView');
  const npsView = document.getElementById('npsView');
  const backtestView = document.getElementById('backtestView');
  analysisView.style.display = view === 'analysis' ? 'block' : 'none';
  portfolioView.style.display = view === 'portfolio' ? 'block' : 'none';
  if (npsView) npsView.style.display = view === 'nps' ? 'block' : 'none';
  if (backtestView) backtestView.style.display = view === 'backtest' ? 'block' : 'none';
  const activeEl = view === 'analysis' ? analysisView
                  : view === 'portfolio' ? portfolioView
                  : view === 'backtest' ? backtestView
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
  _pfRenderColToggles();
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
      // Build a fingerprint to avoid unnecessary DOM rebuilds (prevents click loss during rAF re-renders)
      const fingerprint = pfGroups.map(g => {
        const active = pfGroupFilter === null || pfGroupFilter.has(g.group_name);
        return `${g.group_name}:${counts[g.group_name] || 0}:${active ? 1 : 0}`;
      }).join('|');
      if (filterBar.dataset.fingerprint !== fingerprint) {
        filterBar.dataset.fingerprint = fingerprint;
        filterBar.innerHTML = '';
        // Set up delegated click handler once
        if (!filterBar.dataset.delegated) {
          filterBar.dataset.delegated = '1';
          filterBar.addEventListener('click', (e) => {
            const btn = e.target.closest('.pf-filter-btn');
            if (!btn) return;
            if (btn.classList.contains('pf-group-manage-btn')) { openGroupModal(); return; }
            const gn = btn.dataset.groupName;
            if (gn) pfToggleGroupFilter(gn);
          });
        }
        pfGroups.forEach(g => {
          const active = pfGroupFilter === null || pfGroupFilter.has(g.group_name);
          const btn = document.createElement('button');
          btn.className = 'pf-filter-btn' + (active ? ' active' : '');
          btn.dataset.groupName = g.group_name;
          btn.textContent = g.group_name + ' ';
          const cnt = document.createElement('span');
          cnt.className = 'pf-filter-cnt';
          cnt.textContent = counts[g.group_name] || 0;
          btn.appendChild(cnt);
          filterBar.appendChild(btn);
        });
        const gearBtn = document.createElement('button');
        gearBtn.className = 'pf-filter-btn pf-group-manage-btn';
        gearBtn.title = '그룹 관리';
        gearBtn.textContent = '\u2699';
        filterBar.appendChild(gearBtn);
      }
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

  // Check if all quotes are loaded
  const allQuotesLoaded = allRows.every(r => r.price !== null);

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

  // FX helper: convert KRW value using a specific snapshot's FX rate, or current rate
  const _fxConv = (krwVal, snap) => {
    if (pfCurrency !== 'USD') return krwVal;
    const rate = snap && snap.fx_usdkrw ? snap.fx_usdkrw : pfFxRate;
    return rate && rate > 0 ? krwVal / rate : pfFx(krwVal);
  };
  const _currentFxVal = _fxConv(totalMarketValue, null); // today uses current rate
  const _currentFxInvested = _fxConv(totalInvested, null);

  const totalReturnPct = _currentFxInvested > 0 ? ((_currentFxVal - _currentFxInvested) / _currentFxInvested * 100) : 0;

  // NAV adjusted for currency mode: USD NAV = KRW NAV / FX
  const isFiltered = pfGroupFilter !== null;
  const _navAdj = (nav, fx) => {
    if (pfCurrency !== 'USD' || !nav) return nav;
    const rate = fx && fx > 0 ? fx : pfFxRate;
    return rate && rate > 0 ? nav / rate : nav;
  };

  // Latest NAV — use live total_value / total_units if snapshot is stale
  const latestSnap = pfNavHistory.length ? pfNavHistory[pfNavHistory.length - 1] : null;
  const _today = new Date().toISOString().slice(0, 10);
  const _snapIsStale = latestSnap && latestSnap.date < _today;
  const _curNavKrw = (_snapIsStale && latestSnap.total_units && totalMarketValue > 0)
    ? totalMarketValue / latestSnap.total_units
    : (latestSnap ? latestSnap.nav : null);
  const curNav = _navAdj(_curNavKrw, pfFxRate);

  // --- Daily return ---
  // NAV-based (big text)
  const prevDayNav = _navAdj(
    pfPrevDaySnapshot ? pfPrevDaySnapshot.nav : null,
    pfPrevDaySnapshot ? pfPrevDaySnapshot.fx_usdkrw : null
  );
  const dailyNavPct = prevDayNav && curNav ? ((curNav / prevDayNav - 1) * 100) : null;
  // Value-based PnL (small text)
  let totalDailyPnlDisplay = 0;
  if (pfPrevDaySnapshot && pfPrevDaySnapshot.total_value) {
    const _prevFxVal = pfCurrency === 'USD' && pfPrevDaySnapshot.fx_usdkrw && pfPrevDaySnapshot.fx_usdkrw > 0
      ? pfPrevDaySnapshot.total_value / pfPrevDaySnapshot.fx_usdkrw
      : pfPrevDaySnapshot.total_value;
    const _fxCashflow = pfCurrency === 'USD'
      ? (pfPrevDaySnapshot.today_net_cashflow || 0) / (pfFxRate || 1)
      : (pfPrevDaySnapshot.today_net_cashflow || 0);
    totalDailyPnlDisplay = _currentFxVal - _prevFxVal - _fxCashflow;
  }
  // For table footer daily column, use NAV-based if available
  let dailyReturnPct = dailyNavPct ?? 0;

  // --- Monthly return (MTD) ---
  // FX-adjusted snap value helper
  const _snapToFxVal = snap => {
    if (!snap || !snap.total_value) return null;
    if (pfCurrency === 'USD' && snap.fx_usdkrw && snap.fx_usdkrw > 0) return snap.total_value / snap.fx_usdkrw;
    return snap.total_value;
  };
  // NAV-based (big text)
  const monthEndNav = _navAdj(
    pfMonthEndSnap ? pfMonthEndSnap.nav : null,
    pfMonthEndSnap ? pfMonthEndSnap.fx_usdkrw : null
  );
  const monthlyNavPct = monthEndNav && curNav ? ((curNav / monthEndNav - 1) * 100) : null;
  // Value-based PnL (small text)
  const _mtdBaseVal = _snapToFxVal(pfMonthEndSnap);
  const _mtdPnl = _mtdBaseVal != null ? _currentFxVal - _mtdBaseVal : null;
  const monthlyReturnPct = monthlyNavPct;

  // --- YTD return ---
  // NAV-based (big text)
  const yearStartSnap = pfYearStartSnap || null;
  const baseNav = _navAdj(
    yearStartSnap ? yearStartSnap.nav : null,
    yearStartSnap ? yearStartSnap.fx_usdkrw : null
  );
  const ytdReturnPct = baseNav && curNav ? ((curNav / baseNav - 1) * 100) : null;
  // Value-based PnL (small text)
  const _ytdBaseVal = _snapToFxVal(yearStartSnap);
  const _ytdPnl = _ytdBaseVal != null ? _currentFxVal - _ytdBaseVal : null;

  // Date labels for summary cards
  const _now = new Date();
  const _timeLabel = `${String(_now.getHours()).padStart(2,'0')}:${String(_now.getMinutes()).padStart(2,'0')}`;
  const _todayLabel = `${_now.getFullYear()}/${String(_now.getMonth()+1).padStart(2,'0')}/${String(_now.getDate()).padStart(2,'0')}`;
  const _mtdLabel = `${_now.getFullYear()}/${String(_now.getMonth()+1).padStart(2,'0')}`;
  const _ytdLabel = `${_now.getFullYear()}`;

  // Summary cards: Total, Today, MTD, YTD — show '-' until all quotes loaded
  const _l = allQuotesLoaded;
  const _loadingCount = allRows.filter(r => r.price === null).length;
  const _loadingSub = !_l ? `<span style="opacity:0.5">시세 로딩 중 (${allRows.length - _loadingCount}/${allRows.length})</span>` : '';
  // Format helpers — values passed here are already FX-converted
  const _fmtUsdVal = v => '$' + Number(v).toLocaleString(undefined, {minimumFractionDigits: 3, maximumFractionDigits: 3});
  const _fv = v => pfCurrency === 'USD' ? _fmtUsdVal(v) : fmtKrw(v);
  const _fsv = v => {
    if (pfCurrency === 'USD') return (v >= 0 ? '+' : '') + _fmtUsdVal(v);
    return fmtSignedKrw(v);
  };
  // Today's daily PnL in selected currency
  const _dailyPnlFx = pfCurrency === 'USD' ? pfFx(totalDailyPnl) : totalDailyPnl;

  summary.innerHTML = `
    <div class="pf-summary-card">
      <div class="pf-summary-text">
        <div class="pf-summary-label">Total <span class="pf-summary-date">${_timeLabel}</span></div>
        <div class="pf-summary-value">${_l ? _fv(_currentFxVal) : '-'}</div>
        <div class="pf-summary-sub">${_l ? '투자금액 ' + _fv(_currentFxInvested) : _loadingSub}</div>
      </div>
    </div>
    <div class="pf-summary-card">
      <div class="pf-summary-text">
        <div class="pf-summary-label">Today <span class="pf-summary-date">${_todayLabel}</span></div>
        <div class="pf-summary-value ${_l ? returnClass(dailyNavPct) : ''}">${_l ? fmtPct(dailyNavPct ?? 0) : '-'}</div>
        <div class="pf-summary-sub ${_l ? returnClass(totalDailyPnlDisplay) : ''}">${_l ? _fsv(totalDailyPnlDisplay) : ''}</div>
      </div>
      <canvas class="pf-sparkline" id="sparkDaily"></canvas>
    </div>
    <div class="pf-summary-card">
      <div class="pf-summary-text">
        <div class="pf-summary-label">MTD <span class="pf-summary-date">${_mtdLabel}</span></div>
        <div class="pf-summary-value ${_l ? returnClass(monthlyNavPct) : ''}">${_l ? (monthlyNavPct !== null ? fmtPct(monthlyNavPct) : '-') : '-'}</div>
        <div class="pf-summary-sub ${_l ? returnClass(_mtdPnl) : ''}">${_l ? (_mtdPnl !== null ? _fsv(_mtdPnl) : '-') : ''}</div>
      </div>
      <canvas class="pf-sparkline" id="sparkMonthly"></canvas>
    </div>
    <div class="pf-summary-card">
      <div class="pf-summary-text">
        <div class="pf-summary-label">YTD <span class="pf-summary-date">${_ytdLabel}</span></div>
        <div class="pf-summary-value ${_l ? returnClass(ytdReturnPct) : ''}">${_l ? (ytdReturnPct !== null ? fmtPct(ytdReturnPct) : '-') : '-'}</div>
        <div class="pf-summary-sub ${_l ? returnClass(_ytdPnl) : ''}">${_l ? (_ytdPnl !== null ? _fsv(_ytdPnl) : '-') : ''}</div>
      </div>
      <canvas class="pf-sparkline" id="sparkTotalReturn"></canvas>
    </div>`;
  _renderSummarySparklines(_l ? grandTotalMarketValue : null);

  // Table body — apply FX conversion to price columns
  const _fp = v => {
    const cv = pfFx(v);
    return pfCurrency === 'USD'
      ? '$' + Number(cv).toLocaleString(undefined, {minimumFractionDigits: 3, maximumFractionDigits: 3})
      : fmtNum(Math.round(cv));
  };
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
        <td class="pf-col-num pf-col-changepct">${fmtChangePct(r.changePct, r.change)}</td>
        <td class="pf-col-num pf-col-benchmark">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
        <td class="pf-col-num pf-col-buyprice"><input class="pf-edit-input" id="pfEditPrice" value="${r.avgPrice}" type="number" step="1"></td>
        <td class="pf-col-num pf-col-curprice">${r.price !== null ? _fp(r.price) : '-'}</td>
        <td class="pf-col-num pf-col-return"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
        <td class="pf-col-num pf-col-qty"><input class="pf-edit-input" id="pfEditQty" value="${r.qty}" type="number" step="${qtyStep}"></td>
        <td class="pf-col-num pf-col-mktval">${r.marketValue !== null ? _fp(r.marketValue) : '-'}</td>
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
      <td class="pf-col-num pf-col-changepct">${fmtChangePct(r.changePct, r.change)}</td>
      <td class="pf-col-num pf-col-benchmark" onclick="pfShowBenchmarkPicker('${r.stock_code}', this)">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
      <td class="pf-col-num pf-col-buyprice">${_fp(r.avgPrice)}</td>
      <td class="pf-col-num pf-col-curprice">${r.price !== null ? _fp(r.price) : '-'}</td>
      <td class="pf-col-num pf-col-return"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
      <td class="pf-col-num pf-col-qty">${fmtQty(r.qty)}</td>
      <td class="pf-col-num pf-col-mktval">${r.marketValue !== null ? _fp(r.marketValue) : '-'}</td>
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
    <td class="pf-col-num pf-col-changepct">${fmtChangePct(dailyReturnPct, totalDailyPnl)}</td>
    <td class="pf-col-benchmark"></td>
    <td class="pf-col-num pf-col-buyprice">${_fp(totalInvested)}</td>
    <td class="pf-col-curprice"></td>
    <td class="pf-col-num pf-col-return"><span class="pf-return ${returnClass(totalReturnPct)}">${fmtPct(totalReturnPct)}</span></td>
    <td class="pf-col-qty"></td>
    <td class="pf-col-num pf-col-mktval">${_fp(totalMarketValue)}</td>
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

function _drawSparkline(canvasId, values, color, maxSlots, align) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);

  const slots = maxSlots || Math.max(values.length, 1);
  const offset = align === 'left' ? 0 : slots - values.length;
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 0;
  const pad = 2;

  // Include 0 in range so zero line is always visible
  const minZ = Math.min(min, 0);
  const maxZ = Math.max(max, 0);
  const rangeZ = maxZ - minZ || 1;

  // Zero line — always draw
  const zeroY = pad + (1 - (0 - minZ) / rangeZ) * (h - pad * 2);
  ctx.beginPath();
  ctx.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#e0e0e0';
  ctx.lineWidth = 0.5;
  ctx.moveTo(0, zeroY);
  ctx.lineTo(w, zeroY);
  ctx.stroke();

  // Data line
  if (values.length > 1) {
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    values.forEach((v, i) => {
      const x = ((i + offset) / (slots - 1)) * w;
      const y = pad + (1 - (v - minZ) / rangeZ) * (h - pad * 2);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }
}

function _renderSummarySparklines(currentTotalValue) {
  // 총 수익률 — 52주 (약 252 거래일) 누적 수익률 추이
  if (pfNavHistory.length > 1) {
    const last365 = pfNavHistory.slice(-365);
    const returnPcts = last365.map(d => d.total_invested > 0 ? ((d.total_value - d.total_invested) / d.total_invested * 100) : 0);
    const lastReturn = returnPcts[returnPcts.length - 1] || 0;
    _drawSparkline('sparkTotalReturn', returnPcts, lastReturn >= 0 ? '#dc2626' : '#2563eb', 252, 'right');
  } else {
    _drawSparkline('sparkTotalReturn', [], '#dc2626', 252, 'right');
  }

  // 월간 수익률 — pfMonthEndSnap?.total_value 대비 일별 total_value 변동률 (%)
  if (pfNavHistory.length > 0 && pfMonthEndSnap?.total_value && pfMonthEndSnap?.total_value > 0) {
    const thisMonth = new Date().toISOString().slice(0, 7);
    const monthData = pfNavHistory.filter(d => d.date >= thisMonth);
    if (monthData.length > 0) {
      const monthPcts = monthData.map(d => ((d.total_value / pfMonthEndSnap?.total_value) - 1) * 100);
      const lastPct = monthPcts[monthPcts.length - 1];
      _drawSparkline('sparkMonthly', monthPcts,
        lastPct >= 0 ? '#dc2626' : '#2563eb', 22, 'left');
    } else {
      _drawSparkline('sparkMonthly', [], '#dc2626', 22, 'left');
    }
  } else {
    _drawSparkline('sparkMonthly', [], '#dc2626', 22, 'left');
  }

  // 일간 수익률 — 30분 간격 변동률 (%) + 현재 실시간 값 append
  if (pfIntradayData.length > 1) {
    const baseValue = pfIntradayData[0].total_value;
    const dayPcts = pfIntradayData.map(d => ((d.total_value / baseValue) - 1) * 100);
    if (currentTotalValue && baseValue > 0) {
      dayPcts.push(((currentTotalValue / baseValue) - 1) * 100);
    }
    const lastPct = dayPcts[dayPcts.length - 1];
    _drawSparkline('sparkDaily', dayPcts, lastPct >= 0 ? '#dc2626' : '#2563eb', 28, 'left');
  } else {
    _drawSparkline('sparkDaily', [], '#dc2626', 28, 'left');
  }
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
  } catch (e) { showToast(e.message); }
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
  } catch (e) { showToast(e.message); }
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
    showToast('수량과 매입가를 올바르게 입력해 주세요.');
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
  } catch (e) { showToast(e.message); }
}

async function deletePortfolioItem(stockCode) {
  try {
    const resp = await apiFetch(`/api/portfolio/${stockCode}`, { method: 'DELETE' });
    if (!resp.ok) throw new Error('삭제 실패');
    portfolioItems = portfolioItems.filter(i => i.stock_code !== stockCode);
    renderPortfolio();
  } catch (e) { showToast(e.message); }
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
        } catch (e) { console.warn(e); }
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
  } catch (e) { showToast(e.message); }
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
  } catch (e) { console.warn(e); }
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
  const prevStockVals = (pfPrevDaySnapshot && pfPrevDaySnapshot.stock_values) || {};
  portfolioItems.forEach(i => {
    const gn = pfGetGroup(i);
    if (!stats[gn]) stats[gn] = { cnt: 0, invested: 0, mv: 0, prevMV: 0 };
    const s = stats[gn];
    const q = i.quote || {};
    const price = q.price ?? null;
    const qty = i.quantity;
    const avgPrice = i.avg_price;
    s.cnt++;
    s.invested += qty * avgPrice;
    if (price !== null) { s.mv += qty * price; grandMV += qty * price; }
    s.prevMV += (prevStockVals[i.stock_code] ?? 0);
  });
  const defaultCount = pfGroups.filter(x => x.is_default).length;
  const rowsHtml = pfGroups.map((g, i) => {
    const s = stats[g.group_name] || { cnt: 0, invested: 0, mv: 0, prevMV: 0 };
    const weight = grandMV > 0 ? (s.mv / grandMV * 100) : 0;
    const returnPct = s.invested > 0 ? ((s.mv - s.invested) / s.invested * 100) : 0;
    const dailyPnl = s.prevMV > 0 ? (s.mv - s.prevMV) : 0;
    const dailyPct = s.prevMV > 0 ? (dailyPnl / s.prevMV * 100) : 0;
    const canDelete = !g.is_default || defaultCount > 3;
    const delBtn = canDelete
      ? `<button class="pf-grp-del" data-grp-name="${escapeHtml(g.group_name)}" title="삭제">&times;</button>`
      : '';
    return `<tr class="pf-grp-tr" draggable="true" data-grp-idx="${i}">
      <td class="pf-grp-td-drag"><span class="pf-grp-drag" title="드래그하여 순서 변경">&#x2630;</span></td>
      <td class="pf-grp-td-name"><input class="pf-grp-name" value="${escapeHtml(g.group_name)}" data-orig="${escapeHtml(g.group_name)}" onblur="renameGroup(this)"></td>
      <td class="pf-grp-td-num">${s.cnt}</td>
      <td class="pf-grp-td-num">${weight.toFixed(1)}%</td>
      <td class="pf-grp-td-num">${fmtNum(Math.round(s.mv))}</td>
      <td class="pf-grp-td-num"><span class="${returnClass(returnPct)}">${fmtPct(returnPct)}</span></td>
      <td class="pf-grp-td-num"><span class="${returnClass(dailyPct)}">${fmtPct(dailyPct)}</span></td>
      <td class="pf-grp-td-num"><span class="${returnClass(dailyPnl)}">${fmtSignedKrw(dailyPnl)}</span></td>
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
      } catch (e) { console.warn(e); }
    });
  });
  // Delete buttons — read group name from data attribute
  body.querySelectorAll('.pf-grp-del[data-grp-name]').forEach(btn => {
    btn.addEventListener('click', () => deleteGroup(btn.dataset.grpName));
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
  } catch (e) { showToast(e.message); }
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
    showToast(e.message);
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
  } catch (e) { showToast(e.message); }
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
  try { const v = JSON.parse(localStorage.getItem(MB_LS_KEY)); if (Array.isArray(v)) return v; } catch (e) { console.warn(e); }
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
    } catch (e) { console.warn(e); }
  }
  mbCodes = _mbGetCodes() || MB_DEFAULT_CODES.slice();
}
async function _mbLoadCatalog() {
  try {
    const resp = await apiFetch('/api/market-indicators');
    if (resp.ok) mbCatalog = await resp.json();
  } catch (e) { console.warn(e); }
}

function _mbIsHidden(code) {
  if (code === 'NIGHT_FUTURES') {
    const h = new Date().getHours();
    if (h >= 9 && h < 18) return true;
  }
  return false;
}

function _mbRenderBar(dataMap) {
  const bar = document.getElementById('marketBar');
  if (!bar) return;
  let html = '';
  mbCodes.forEach((code, idx) => {
    if (_mbIsHidden(code)) return;
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
  } catch (e) { console.warn(e); }
}

function toggleCsvPanel() {
  const panel = document.getElementById('pfCsvPanel');
  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

async function submitCsv(mode) {
  const text = document.getElementById('pfCsvInput').value.trim();
  if (!text) { showToast('CSV 데이터를 입력해 주세요.'); return; }

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

  if (errors.length) { showToast(errors.join('\n')); return; }
  if (!items.length) { showToast('등록할 종목이 없습니다.'); return; }

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
    showToast(`${data.imported}개 종목이 ${mode === 'replace' ? '교체' : '추가'} 등록되었습니다.`, 'success');
    await loadPortfolio();
  } catch (e) {
    showToast(e.message);
  } finally {
    btns.forEach(b => b.disabled = false);
  }
}

// --- Currency conversion ---
function pfFx(krwValue) {
  if (pfCurrency === 'USD' && pfFxRate && pfFxRate > 0) return krwValue / pfFxRate;
  return krwValue;
}
function pfFxSymbol() { return pfCurrency === 'USD' ? '$' : ''; }
function pfFxUnit() { return pfCurrency === 'USD' ? 'M' : '억'; }
function pfFxDivisor() { return pfCurrency === 'USD' ? 1e6 : 1e8; }

async function _ensureFxRate() {
  if (pfFxRate) return;
  try {
    const resp = await apiFetch('/api/asset-quote/CASH_USD');
    if (resp.ok) {
      const d = await resp.json();
      if (d.price) pfFxRate = d.price; // KRW per 1 USD
    }
  } catch (e) { console.warn(e); }
}

function pfSetCurrency(currency) {
  pfCurrency = currency;
  document.querySelectorAll('.pf-currency-btn').forEach(b => b.classList.toggle('active', b.dataset.currency === currency));
  const refresh = () => {
    renderPortfolio();
    if (pfActiveTab === 'performance') loadPerformanceData();
  };
  if (currency === 'USD') {
    _ensureFxRate().then(refresh);
  } else {
    refresh();
  }
}

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
  if (dateInput && !dateInput.value) {
    const now = new Date();
    dateInput.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
  }
  try {
    const [navResp, cfResp] = await Promise.all([
      apiFetch('/api/portfolio/nav-history'),
      apiFetch('/api/portfolio/cashflows'),
    ]);
    const navData = navResp.ok ? await navResp.json() : [];
    const cfData = cfResp.ok ? await cfResp.json() : [];
    if (!USE_UPLOT) renderTreemap();
    renderNavChart(navData);
    renderValueChart(navData);
    renderNavReturns(navData);
    renderCashflows(cfData);
  } catch (e) { console.warn(e); }
}

let _treemapInstance = null;

async function renderTreemap() {
  const container = document.getElementById('pfTreemap');
  if (!container) return;
  if (_treemapInstance) { _treemapInstance.dispose(); _treemapInstance = null; }

  // ECharts required for treemap
  if (typeof echarts === 'undefined') {
    await new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js';
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    });
  }

  // Wait one frame for container layout
  await new Promise(r => requestAnimationFrame(r));

  if (!portfolioItems.length) {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">포트폴리오가 비어 있습니다.</div>';
    return;
  }

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';

  // Build treemap data grouped by pfGetGroup (exclude negative qty)
  const groups = {};
  portfolioItems.forEach(item => {
    if (item.quantity <= 0) return;
    const gn = pfGetGroup(item);
    if (!groups[gn]) groups[gn] = [];
    const q = item.quote || {};
    const price = q.price ?? null;
    const qty = item.quantity;
    const mv = price !== null ? qty * price : qty * item.avg_price;
    if (mv <= 0) return;
    const changePct = q.change_pct ?? null;
    groups[gn].push({
      name: item.stock_name,
      value: mv,
      changePct,
      code: item.stock_code,
    });
  });

  // Map changePct to color: blue(-) → gray(0) → red(+)
  function _pctToColor(pct) {
    if (pct === null || pct === undefined) return isDark ? '#475569' : '#9ca3af';
    const clamped = Math.max(-5, Math.min(5, pct));
    const t = (clamped + 5) / 10; // 0~1
    let r, g, b;
    if (t < 0.5) {
      const s = t / 0.5;
      r = Math.round(37 + (148 - 37) * s);
      g = Math.round(99 + (163 - 99) * s);
      b = Math.round(235 + (184 - 235) * s);
    } else {
      const s = (t - 0.5) / 0.5;
      r = Math.round(148 + (220 - 148) * s);
      g = Math.round(163 + (38 - 163) * s);
      b = Math.round(184 + (38 - 184) * s);
    }
    return `rgb(${r},${g},${b})`;
  }

  // Compute grand total for weight %
  let grandTotal = 0;
  Object.values(groups).forEach(items => items.forEach(it => { grandTotal += it.value; }));

  // Muted color for group header based on changePct
  function _pctToHeaderColor(pct) {
    if (pct === null || pct === undefined) return isDark ? '#1e293b' : '#f3f4f6';
    const clamped = Math.max(-3, Math.min(3, pct));
    const abs = Math.abs(clamped) / 3; // 0~1
    if (clamped < 0) {
      // Blue tint
      return isDark
        ? `rgba(37,99,235,${0.08 + abs * 0.2})`
        : `rgba(37,99,235,${0.04 + abs * 0.12})`;
    } else {
      // Red tint
      return isDark
        ? `rgba(220,38,38,${0.08 + abs * 0.2})`
        : `rgba(220,38,38,${0.04 + abs * 0.12})`;
    }
  }

  function _pctToHeaderTextColor(pct) {
    if (pct === null || pct === undefined) return isDark ? '#94a3b8' : '#6b7280';
    if (pct < 0) return isDark ? '#93c5fd' : '#2563eb';
    if (pct > 0) return isDark ? '#fca5a5' : '#dc2626';
    return isDark ? '#94a3b8' : '#6b7280';
  }

  const treeData = Object.entries(groups).map(([gn, items]) => {
    // Group-level weighted daily change
    const grpTotal = items.reduce((s, it) => s + it.value, 0);
    let grpChangePct = null;
    const withPct = items.filter(it => it.changePct !== null);
    if (withPct.length > 0 && grpTotal > 0) {
      grpChangePct = withPct.reduce((s, it) => s + (it.changePct * it.value), 0) / grpTotal;
    }
    const grpWeight = grandTotal > 0 ? (grpTotal / grandTotal * 100) : 0;

    return {
      name: gn,
      changePct: grpChangePct,
      weight: grpWeight,
      itemStyle: { color: _pctToHeaderColor(grpChangePct), borderColor: 'transparent' },
      upperLabel: {
        color: _pctToHeaderTextColor(grpChangePct),
        backgroundColor: _pctToHeaderColor(grpChangePct),
      },
      children: items.map(it => ({
        name: it.name,
        value: it.value,
        changePct: it.changePct,
        weight: grandTotal > 0 ? (it.value / grandTotal * 100) : 0,
        code: it.code,
        itemStyle: { color: _pctToColor(it.changePct) },
      })),
    };
  });

  const ec = echarts.init(container);
  _treemapInstance = ec;

  const _fmtPct = v => v !== null && v !== undefined ? (v > 0 ? '+' : '') + v.toFixed(2) + '%' : '-';

  ec.setOption({
    tooltip: {
      formatter(info) {
        const d = info.data;
        const cpStr = _fmtPct(d.changePct);
        const wStr = d.weight !== undefined ? d.weight.toFixed(1) + '%' : '';
        const val = Number(info.value).toLocaleString();
        return `<strong>${escapeHtml(info.name)}</strong><br/>평가: ${val}<br/>비중: ${wStr}<br/>일간: ${cpStr}`;
      },
    },
    series: [{
      type: 'treemap',
      left: 0, right: 0, top: 0, bottom: 0,
      roam: false,
      nodeClick: false,
      breadcrumb: { show: false },
      itemStyle: {
        borderColor: isDark ? '#334155' : '#e5e7eb',
        borderWidth: 1,
      },
      upperLabel: { show: false },
      levels: [
        {
          // Level 0: root — hide upperLabel
          upperLabel: { show: false },
          itemStyle: { borderWidth: 0 },
        },
        {
          // Level 1: group — header tinted by performance
          itemStyle: {
            borderColor: isDark ? '#475569' : '#d1d5db',
            borderWidth: 2,
          },
          upperLabel: {
            show: true,
            height: 22,
            fontSize: 11,
            fontWeight: 600,
            padding: [2, 8],
            formatter(params) {
              const d = params.data;
              if (d.changePct === null || d.changePct === undefined) return params.name;
              return `${params.name}  ${_fmtPct(d.changePct)}`;
            },
          },
        },
        {
          // Stock level
          itemStyle: {
            borderColor: isDark ? 'rgba(255,255,255,0.08)' : 'rgba(255,255,255,0.6)',
            borderWidth: 1,
          },
          label: {
            show: true,
            formatter(params) {
              const cp = params.data.changePct;
              const cpStr = cp !== null && cp !== undefined ? (cp > 0 ? '+' : '') + cp.toFixed(2) + '%' : '';
              return `{name|${params.name}}\n{pct|${cpStr}}`;
            },
            rich: {
              name: { fontSize: 11, fontWeight: 600, color: '#fff', lineHeight: 16 },
              pct: { fontSize: 10, color: 'rgba(255,255,255,0.8)', lineHeight: 14 },
            },
          },
        },
      ],
      data: treeData,
    }],
  });
}

let _navChartInstance = null;

async function renderNavChart(data) {
  const container = document.getElementById('pfNavChart');
  if (!container) return;
  if (_navChartInstance) { _navChartInstance.dispose(); _navChartInstance = null; }
  await loadChartLib();

  if (!data.length) {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">스냅샷 데이터가 없습니다.</div>';
    return;
  }

  // In USD mode, NAV_USD = NAV_KRW / FX so it reflects both portfolio and currency performance
  const navValues = data.map(d => {
    if (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) return d.nav / d.fx_usdkrw;
    return d.nav;
  });

  // Color based on YoY
  const last365 = data.slice(-365);
  const yoyPct = last365.length > 1
    ? ((navValues[navValues.length - 1] / navValues[navValues.length - last365.length]) - 1) * 100 : 0;
  const navColor = returnToColor(yoyPct);

  _navChartInstance = createLineChart(container, {
    labels: data.map(d => d.date),
    values: navValues,
    color: navColor,
    tooltipPrefix: 'NAV ',
    dataZoom: true,
  });
}

let _valueChartInstance = null;

async function renderValueChart(data) {
  const container = document.getElementById('pfValueChart');
  if (!container) return;
  if (_valueChartInstance) { _valueChartInstance.dispose(); _valueChartInstance = null; }
  await loadChartLib();

  // Stats cards
  const statsEl = document.getElementById('pfValueStats');
  if (statsEl) statsEl.innerHTML = '';

  if (!data.length) {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">스냅샷 데이터가 없습니다.</div>';
    return;
  }

  // Convert values using per-day FX rate when available
  const fxValues = data.map(d => {
    if (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) return d.total_value / d.fx_usdkrw;
    if (pfCurrency === 'USD') return pfFx(d.total_value); // fallback to current rate
    return d.total_value;
  });

  // Color based on MoM
  const last30 = fxValues.slice(-30);
  const momPct = last30.length > 1
    ? ((fxValues[fxValues.length - 1] / last30[0]) - 1) * 100 : 0;
  const valColor = returnToColor(momPct, 10);

  const div = pfFxDivisor();
  const unit = pfFxUnit();
  const sym = pfFxSymbol();

  _valueChartInstance = createLineChart(container, {
    labels: data.map(d => d.date),
    values: fxValues.map(v => Math.round(v)),
    color: valColor,
    yFormatter: v => sym + (v / div).toFixed(pfCurrency === 'USD' ? 2 : 0) + unit,
    dataZoom: true,
  });

  // Value stats cards — use FX-converted values
  if (statsEl) {
    const fxLast365 = fxValues.slice(-365);
    const min52 = Math.min(...fxLast365);
    const max52 = Math.max(...fxLast365);

    // YoY (using FX-adjusted values)
    const yoyPct = fxLast365.length > 1
      ? ((fxValues[fxValues.length - 1] / fxLast365[0]) - 1) * 100 : null;

    // CAGR using FX-adjusted values
    const valTotalDays = data.length > 1 ? (new Date(data[data.length - 1].date) - new Date(data[0].date)) / 86400000 : 0;
    const valTotalYears = valTotalDays / 365;
    const _latestFxVal = fxValues[fxValues.length - 1];
    const _firstFxVal = fxValues[0];
    const acctReturn = valTotalYears > 0 && _firstFxVal > 0
      ? ((_latestFxVal - _firstFxVal) / _firstFxVal * 100) / valTotalYears : null;

    const fmtVal = v => pfCurrency === 'USD' ? '$' + Number(v.toFixed(0)).toLocaleString() : fmtKrw(Math.round(v));
    const items = [
      { label: '52주 최저', val: fmtVal(min52) },
      { label: '52주 최고', val: fmtVal(max52) },
      { label: 'YoY', val: yoyPct !== null ? fmtPct(yoyPct) : '-', cls: returnClass(yoyPct) },
      { label: 'CAGR', val: acctReturn !== null ? fmtPct(acctReturn) : '-', cls: returnClass(acctReturn) },
    ];
    statsEl.innerHTML = items.map(p =>
      `<div class="pf-nav-ret-card"><div class="pf-nav-ret-label">${p.label}</div><div class="pf-nav-ret-value ${p.cls || ''}">${p.val}</div></div>`
    ).join('');
  }
}

function renderNavReturns(data) {
  const el = document.getElementById('pfNavReturns');
  if (!el || !data.length) { if (el) el.innerHTML = ''; return; }

  // FX-adjusted NAV values
  const _nav = d => {
    if (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) return d.nav / d.fx_usdkrw;
    return d.nav;
  };

  const latest = data[data.length - 1];
  const latestNav = _nav(latest);
  const firstNav = _nav(data[0]);

  // 52-week range
  const last365 = data.slice(-365);
  const navs52 = last365.map(d => _nav(d));
  const min52 = Math.min(...navs52);
  const max52 = Math.max(...navs52);

  // YoY
  const oneYearAgo = last365.length >= 252 ? last365[0] : (last365.length > 0 ? last365[0] : null);
  const yoyPct = oneYearAgo ? ((latestNav / _nav(oneYearAgo)) - 1) * 100 : null;

  // Annualized return
  const totalDays = data.length > 1 ? (new Date(latest.date) - new Date(data[0].date)) / 86400000 : 0;
  const totalYears = totalDays / 365;
  const annualizedPct = totalYears > 0
    ? ((latestNav - firstNav) / firstNav * 100) / totalYears : null;

  const items = [
    { label: '52주 최저', val: min52.toFixed(2) },
    { label: '52주 최고', val: max52.toFixed(2) },
    { label: 'YoY', val: yoyPct !== null ? fmtPct(yoyPct) : '-', cls: returnClass(yoyPct) },
    { label: 'CAGR', val: annualizedPct !== null ? fmtPct(annualizedPct) : '-', cls: returnClass(annualizedPct) },
  ];
  el.innerHTML = items.map(p =>
    `<div class="pf-nav-ret-card"><div class="pf-nav-ret-label">${p.label}</div><div class="pf-nav-ret-value ${p.cls || ''}">${p.val}</div></div>`
  ).join('');
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
  if (!amount || amount <= 0) { showToast('금액을 입력해 주세요.'); return; }
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
  } catch (e) { showToast(e.message); }
}

async function deleteCashflow(id) {
  if (!confirm('이 입출금 내역을 삭제할까요?')) return;
  try {
    await apiFetch(`/api/portfolio/cashflows/${id}`, { method: 'DELETE' });
    loadPerformanceData();
  } catch (e) { showToast(e.message); }
}
