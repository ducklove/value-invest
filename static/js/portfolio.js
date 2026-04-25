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
// `defaultVisible: false` 는 "처음 방문하는 사용자에게 기본 숨김".
// 기존 사용자가 이미 localStorage 에 visibility 선택을 저장해둔 경우엔
// 그 선택이 우선 (_pfGetColVisibility 로직 참조).
const PF_COL_DEFS = [
  { key: 'group',     cls: 'pf-col-group',     label: '그룹' },
  { key: 'benchmark', cls: 'pf-col-benchmark',  label: '벤치마크' },
  { key: 'invested',  cls: 'pf-col-invested',   label: '거래대금',  defaultVisible: false },
  { key: 'buyprice',  cls: 'pf-col-buyprice',   label: '매입가' },
  { key: 'curprice',  cls: 'pf-col-curprice',   label: '현재가' },
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
function updatePortfolioRowQuote(code) {
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

  setText('.pf-col-curprice', price !== null ? _fp(price) : '-');
  setHtml('.pf-col-changepct', fmtChangePct(changePct, change));
  setHtml('.pf-col-return', `<span class="pf-return ${returnClass(returnPct)}">${returnPct !== null ? fmtPct(returnPct) : '-'}</span>`);
  setText('.pf-col-mktval', marketValue !== null ? _fp(marketValue) : '-');
  setText('.pf-col-invested', tradingValue !== null ? fmtKrw(tradingValue) : '-');
  setText('.pf-col-divyield', dividendYield !== null ? fmtPct(dividendYield, false) : '-');
  setText('.pf-col-target', targetPrice !== null ? _fp(targetPrice) : '-');
  setText('.pf-col-achiev', achievementPct !== null ? fmtPct(achievementPct, false) : '-');

  flashEl(tr);
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
    // Derived columns added on the client so sorting and live edits use
    // the same values the row cells render:
    //   dividendAmount — trailing dividend per share × quantity.
    //                    pfSort() compares numbers directly, so null
    //                    sinks to the bottom via the shared -Infinity
    //                    fallback (no extra sort plumbing needed).
    //   createdAtSort — created_at as YYYY-MM-DD for lexicographic
    //                    ascending = oldest first, matching user intent.
    // 거래대금 = 시장 누적 거래대금 (원). quote.trade_value 는 현재 한국
    // 주식 경로 (KIS WS + HTTP) 에서만 전달. 해외/현금/금/크립토는
    // null → UI 에서 '-' 로 렌더. 추후 yfinance / Naver world / KIS
    // 해외 경로에서도 채우면 자동 적용됨.
    const tradingValue = (q.trade_value !== undefined && q.trade_value !== null)
      ? Number(q.trade_value)
      : null;
    const trailingDps = item.trailing_dps ?? null;
    // Short / 선물매도 포지션 (qty < 0) 은 실제로는 배당락 만큼 현금
    // 지급 의무가 있어 금융 수학적으로는 음수 배당이 맞지만, 포트폴리오
    // 대시보드에서는 "내가 수취할 연간 배당" 이라는 단순한 의미로 쓰이는
    // 편이 일반적. 따라서 음수·0 수량에는 배당 적용 없음으로 '-' 표시.
    const dividendAmount = (trailingDps !== null && qty > 0) ? trailingDps * qty : null;
    // 시가배당률 = 직전 연간 배당 ÷ 현재가. price 도 quote 에서 이미 KRW
    // 환산된 값이라 trailingDps(KRW) 와 단위가 맞음. 현재가 누락이거나
    // 음수·0 포지션은 계산 의미 없음.
    const dividendYield = (trailingDps !== null && price !== null && price > 0 && qty > 0)
      ? (trailingDps / price * 100)
      : null;
    const createdAtSort = item.created_at ? item.created_at.slice(0, 10) : '';
    return {
      ...item, cur, price, change, changePct, qty, avgPrice,
      invested, marketValue, returnPct, dailyPnl,
      tradingValue,
      trailingDps, dividendAmount, dividendYield, createdAtSort,
    };
  });

  // 목표가 / 달성률 — _computeTargetPrice 가 portfolio 전체를 본다
  // (우선주의 보통주 lookup, 지주사 자회사 lookup) 라 row build 후 두
  // 번째 패스에서 일괄 부여. 외부 quote (포트폴리오에 없는 보통주 /
  // 자회사) 가 필요한 종목들의 코드를 한 번에 모아 백그라운드 fetch.
  // 응답 도착 시 renderPortfolio 재호출되어 자동 갱신.
  const _externalCodesNeeded = new Set();
  const _portCodes = new Set(allRows.map(r => r.stock_code));
  for (const r of allRows) {
    // 우선주: 보통주 quote 가 포트폴리오에 없으면 외부 fetch 필요
    if (r.target_price == null && _isPreferredStock(r.stock_code)) {
      const commonCode = r.stock_code.slice(0, -1) + '0';
      if (!_portCodes.has(commonCode)) _externalCodesNeeded.add(commonCode);
    }
    // 지주사: 자회사 quote 들
    const meta = _HOLDING_META[r.stock_code];
    if (meta && r.target_price == null) {
      for (const sub of meta.subsidiaries || []) {
        if (sub.code && !_portCodes.has(sub.code)) _externalCodesNeeded.add(sub.code);
      }
    }
  }
  if (_externalCodesNeeded.size) _ensureExternalQuotes([..._externalCodesNeeded]);

  for (const r of allRows) {
    r.targetPrice = _computeTargetPrice(r, allRows);
    r.targetSource = _targetPriceSource(r);
    r.achievementPct = (r.targetPrice != null && r.targetPrice > 0 && r.price != null)
      ? (r.price / r.targetPrice * 100)
      : null;
  }

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

  let totalInvested = 0, totalMarketValue = 0, totalDailyPnl = 0, totalDividend = 0;
  rows.forEach(r => {
    totalInvested += r.invested;
    if (r.marketValue !== null) totalMarketValue += r.marketValue;
    totalDailyPnl += r.dailyPnl;
    if (r.dividendAmount !== null) totalDividend += r.dividendAmount;
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
      // Secondary: column sort. null/NaN/빈값은 방향 무관 항상 맨 아래.
      // 이전엔 `?? -Infinity` 로 치환해서 desc 에선 적절히 뒤로 갔지만
      // asc 에선 맨 앞에 몰려 "정렬 안된 것처럼" 보이는 버그가 있었다.
      if (pfSortKey) {
        if (pfSortKey === 'name') {
          const va = a.stock_name || '';
          const vb = b.stock_name || '';
          if (!va && vb) return 1;
          if (va && !vb) return -1;
          return pfSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
        }
        if (pfSortKey === 'createdAtSort') {
          const va = a.createdAtSort || '';
          const vb = b.createdAtSort || '';
          if (!va && vb) return 1;
          if (va && !vb) return -1;
          return pfSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
        }
        const aVal = a[pfSortKey];
        const bVal = b[pfSortKey];
        const aMissing = aVal == null || Number.isNaN(aVal);
        const bMissing = bVal == null || Number.isNaN(bVal);
        if (aMissing && bMissing) return 0;
        if (aMissing) return 1;
        if (bMissing) return -1;
        return pfSortAsc ? aVal - bVal : bVal - aVal;
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
  const _isUsd = pfCurrency === 'USD' && pfFxRate && pfFxRate > 0;
  const _fxConv = (krwVal, snap) => {
    if (!_isUsd) return krwVal;
    const rate = snap && snap.fx_usdkrw ? snap.fx_usdkrw : pfFxRate;
    return rate && rate > 0 ? krwVal / rate : krwVal;
  };
  const _currentFxVal = _fxConv(totalMarketValue, null); // today uses current rate
  const _currentFxInvested = _fxConv(totalInvested, null);

  const totalReturnPct = _currentFxInvested !== 0 ? ((_currentFxVal - _currentFxInvested) / Math.abs(_currentFxInvested) * 100) : 0;

  // NAV adjusted for currency mode: USD NAV = KRW NAV / FX
  const isFiltered = pfGroupFilter !== null;
  const _navAdj = (nav, fx) => {
    if (!_isUsd || !nav) return nav;
    const rate = fx && fx > 0 ? fx : pfFxRate;
    return rate && rate > 0 ? nav / rate : nav;
  };

  // FX-adjusted snap value helper
  const _snapToFxVal = (snap, field = 'total_value') => {
    if (!snap || !snap[field]) return null;
    if (_isUsd && snap.fx_usdkrw && snap.fx_usdkrw > 0) return snap[field] / snap.fx_usdkrw;
    return snap[field];
  };

  // --- Group-filtered ratio: what fraction of total does the filtered group represent ---
  // Returns null when historical per-stock data is unavailable — callers
  // should display "-" rather than approximate with current composition,
  // which would be biased if the group's weight has shifted.
  const _groupRatio = (snap) => {
    if (!isFiltered || !snap) return 1;
    const sv = snap.stock_values || {};
    const allTotal = Object.values(sv).reduce((a, b) => a + b, 0);
    if (!allTotal || allTotal <= 0) return null;
    let filteredTotal = 0;
    rows.forEach(r => { filteredTotal += (sv[r.stock_code] ?? 0); });
    return filteredTotal / allTotal;
  };

  // Helper: compute return % and PnL for a period, group-aware
  const _periodReturn = (snap, navField) => {
    if (!snap) return { pct: null, pnl: null };
    const ratio = _groupRatio(snap);
    if (isFiltered) {
      // Under a filter, if no historical per-stock data is available for
      // this reference date, return null so the card renders "-" instead
      // of falling through to whole-portfolio NAV (which would misleadingly
      // show the entire portfolio's return for the filtered view).
      if (ratio === null) return { pct: null, pnl: null };
      const baseVal = _fxConv(snap.total_value * ratio, snap);
      const pnl = _currentFxVal - baseVal;
      const pct = baseVal > 0 ? (pnl / baseVal * 100) : null;
      return { pct, pnl };
    }
    // Whole portfolio: NAV-based % + total value PnL
    const nav = _navAdj(snap[navField || 'nav'], snap.fx_usdkrw);
    const pct = nav && curNav ? ((curNav / nav - 1) * 100) : null;
    const baseVal = _snapToFxVal(snap);
    const pnl = baseVal != null ? _currentFxVal - baseVal : null;
    return { pct, pnl };
  };

  // --- Compute current NAV ---
  const latestSnap = pfNavHistory.length ? pfNavHistory[pfNavHistory.length - 1] : null;
  // Snapshots are dated in server local time (KST). Use browser LOCAL date
  // so the comparison isn't off by a day between 00:00–09:00 KST (when UTC
  // date is still the previous day and toISOString() would mis-stale-check).
  const _d = new Date();
  const _today = `${_d.getFullYear()}-${String(_d.getMonth()+1).padStart(2,'0')}-${String(_d.getDate()).padStart(2,'0')}`;
  const _snapIsStale = latestSnap && latestSnap.date < _today;
  const _curNavKrw = (_snapIsStale && latestSnap.total_units && totalMarketValue > 0)
    ? totalMarketValue / latestSnap.total_units
    : (latestSnap ? latestSnap.nav : null);
  const curNav = _navAdj(_curNavKrw, pfFxRate);

  // --- Daily return ---
  const _daily = _periodReturn(pfPrevDaySnapshot, 'nav');
  let dailyNavPct = _daily.pct;
  let totalDailyPnlDisplay = _daily.pnl ?? 0;
  // Subtract cashflow for daily (whole portfolio only)
  if (!isFiltered && pfPrevDaySnapshot && pfPrevDaySnapshot.today_net_cashflow) {
    totalDailyPnlDisplay -= _fxConv(pfPrevDaySnapshot.today_net_cashflow, null);
  }
  // For filtered views, derive daily from live quotes instead of the
  // snapshot so the total row matches the weighted average of visible
  // rows exactly. Snapshot-based ratio can drift from the quote's
  // "prev close" due to 22:00 vs market-close timing, causing the
  // filtered total to fall outside the constituent row range.
  if (isFiltered && totalMarketValue > 0) {
    const prevMV = totalMarketValue - totalDailyPnl;
    if (prevMV > 0) {
      dailyNavPct = (totalDailyPnl / prevMV) * 100;
      totalDailyPnlDisplay = _fxConv(totalDailyPnl, null);
    }
  }
  let dailyReturnPct = dailyNavPct ?? 0;

  // --- Monthly return (MTD) ---
  const _mtd = _periodReturn(pfMonthEndSnap, 'nav');
  const monthlyNavPct = _mtd.pct;
  const _mtdPnl = _mtd.pnl;
  const monthlyReturnPct = monthlyNavPct;

  // --- YTD return ---
  const yearStartSnap = pfYearStartSnap || null;
  const _ytd = _periodReturn(yearStartSnap, 'nav');
  const ytdReturnPct = _ytd.pct;
  const _ytdPnl = _ytd.pnl;

  // Date labels for summary cards
  const _now = new Date();
  const _timeLabel = `${String(_now.getHours()).padStart(2,'0')}:${String(_now.getMinutes()).padStart(2,'0')}`;
  // Today card: the baseline is the previous-day 22:00 KST snapshot,
  // not "today". Showing today's date here was misleading — the value
  // actually describes change vs that prior snapshot. We surface the
  // snapshot's real timestamp so "Today" is unambiguous.
  //
  // 22:00 is the snapshot_nav cron cadence; portfolio_snapshots stores
  // only a date column, but the time is a hard contract of that job.
  // For filtered views we fall back to live-quote previous_close math
  // which conceptually shares the same baseline date.
  const _todayBaseDate = pfPrevDaySnapshot && pfPrevDaySnapshot.date;
  // Compact "MM/DD HH시 기준" — year omitted (always current or just-passed
  // year), snapshot_nav cron is 22:00 KST so the hour is a hard contract.
  // pfPrevDaySnapshot.date is YYYY-MM-DD from the server; slice instead
  // of Date() parsing to avoid timezone-off-by-one when the browser
  // timezone doesn't match KST.
  const _todayLabel = _todayBaseDate
    ? `${_todayBaseDate.slice(5, 7)}/${_todayBaseDate.slice(8, 10)} 22시 기준`
    : '기준 없음';
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
    if (pfCurrency === 'USD') {
      const abs = Math.round(Math.abs(v));
      const sign = v > 0 ? '+' : v < 0 ? '-' : '';
      return sign + '$' + abs.toLocaleString();
    }
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
        <div class="pf-summary-value ${_l ? returnClass(dailyNavPct) : ''}">${_l ? (dailyNavPct !== null ? fmtPct(dailyNavPct) : '-') : '-'}</div>
        <div class="pf-summary-sub ${_l ? returnClass(totalDailyPnlDisplay) : ''}">${_l && dailyNavPct !== null ? _fsv(totalDailyPnlDisplay) : ''}</div>
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
  // 편집 중 input 의 사용자 입력(값/포커스/커서)을 re-render 전후로 보존.
  // tbody.innerHTML 재할당은 모든 <input> 을 재생성하므로, QuoteManager
  // WebSocket tick 이나 benchmark polling 이 돌 때마다 편집 중인 값이
  // DB 값으로 덮어써지는 문제가 있었다. 목표가 × 버튼으로 input 을
  // 비워도 즉시 '자동 계산 값' 으로 복원되던 증상이 대표적.
  // 따라서 DOM 교체 전에 현재 값을 snapshot 하고, 교체 후에 복원한다.
  const _editInputIds = ['pfEditPrice', 'pfEditTarget', 'pfEditQty', 'pfEditCreatedAt'];
  const _preservedEdit = pfEditingCode ? {} : null;
  if (_preservedEdit) {
    for (const id of _editInputIds) {
      const el = document.getElementById(id);
      if (!el) continue;
      let selStart = null, selEnd = null;
      // type=number 는 selectionStart 접근 시 DOMException 발생할 수 있음
      try { selStart = el.selectionStart; selEnd = el.selectionEnd; } catch (e) {}
      _preservedEdit[id] = {
        value: el.value,
        focused: el === document.activeElement,
        selStart, selEnd,
      };
    }
  }

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

    const liveDotE = QuoteManager.isLive(r.stock_code) ? '<span class="ws-live-dot" title="실시간"></span>' : '';
    const safeCode = escapeHtml(r.stock_code);
    if (isEditing) {
      return `<tr data-code="${safeCode}">
        <td><a href="#" class="pf-stock-link js-pf-analyze"><strong>${escapeHtml(r.stock_name)}</strong></a> <span class="pf-stock-code">${safeCode}</span>${curTag}${liveDotE}</td>
        <td class="pf-col-group"><select class="pf-group-select js-pf-group">${groupOpts}</select></td>
        <td class="pf-col-num pf-col-changepct">${fmtChangePct(r.changePct, r.change)}</td>
        <td class="pf-col-num pf-col-benchmark">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
        <td class="pf-col-num pf-col-invested">${r.tradingValue !== null ? fmtKrw(r.tradingValue) : '-'}</td>
        <td class="pf-col-num pf-col-buyprice"><input class="pf-edit-input" id="pfEditPrice" value="${r.avgPrice}" type="number" step="1"></td>
        <td class="pf-col-num pf-col-curprice">${r.price !== null ? _fp(r.price) : '-'}</td>
        <td class="pf-col-num pf-col-target"><span class="pf-target-edit-wrap"><input class="pf-edit-input" id="pfEditTarget" value="${r.target_price ?? ''}" type="number" step="any" placeholder="자동"><button type="button" class="pf-target-clear js-pf-target-clear" title="목표가 즉시 비우기 (자동 계산 복귀)">×</button></span></td>
        <td class="pf-col-num pf-col-achiev">${r.achievementPct !== null ? fmtPct(r.achievementPct, false) : '-'}</td>
        <td class="pf-col-num pf-col-return"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
        <td class="pf-col-num pf-col-qty"><input class="pf-edit-input" id="pfEditQty" value="${r.qty}" type="number" step="${qtyStep}"></td>
        <td class="pf-col-num pf-col-mktval">${r.marketValue !== null ? _fp(r.marketValue) : '-'}</td>
        <td class="pf-col-num pf-col-dividend">${r.dividendAmount !== null ? _fp(r.dividendAmount) : '-'}</td>
        <td class="pf-col-num pf-col-divyield">${r.dividendYield !== null ? fmtPct(r.dividendYield, false) : '-'}</td>
        <td class="pf-col-num pf-col-weight">${fmtPct(weight)}</td>
        <td class="pf-col-date"><input class="pf-edit-input" id="pfEditCreatedAt" value="${r.createdAtSort || ''}" type="date"></td>
        <td class="pf-col-act"><div class="pf-row-actions">
          <button class="pf-row-btn save js-pf-save" title="저장">✓</button>
          <button class="pf-row-btn cancel js-pf-cancel" title="취소">✕</button>
        </div></td>
      </tr>`;
    }
    return `<tr draggable="true" data-code="${safeCode}">
      <td><a href="#" class="pf-stock-link js-pf-analyze"><strong>${escapeHtml(r.stock_name)}</strong></a> <span class="pf-stock-code">${safeCode}</span>${curTag}${liveDotE}</td>
      <td class="pf-col-group"><select class="pf-group-select js-pf-group">${groupOpts}</select></td>
      <td class="pf-col-num pf-col-changepct">${fmtChangePct(r.changePct, r.change)}</td>
      <td class="pf-col-num pf-col-benchmark js-pf-bench-picker">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
      <td class="pf-col-num pf-col-invested">${r.tradingValue !== null ? fmtKrw(r.tradingValue) : '-'}</td>
      <td class="pf-col-num pf-col-buyprice">${_fp(r.avgPrice)}</td>
      <td class="pf-col-num pf-col-curprice">${r.price !== null ? _fp(r.price) : '-'}</td>
      <td class="pf-col-num pf-col-target">${r.targetPrice !== null ? _fp(r.targetPrice) : '-'}</td>
      <td class="pf-col-num pf-col-achiev">${r.achievementPct !== null ? fmtPct(r.achievementPct, false) : '-'}</td>
      <td class="pf-col-num pf-col-return"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
      <td class="pf-col-num pf-col-qty">${fmtQty(r.qty)}</td>
      <td class="pf-col-num pf-col-mktval">${r.marketValue !== null ? _fp(r.marketValue) : '-'}</td>
      <td class="pf-col-num pf-col-dividend">${r.dividendAmount !== null ? _fp(r.dividendAmount) : '-'}</td>
      <td class="pf-col-num pf-col-divyield">${r.dividendYield !== null ? fmtPct(r.dividendYield, false) : '-'}</td>
      <td class="pf-col-num pf-col-weight">${fmtPct(weight)}</td>
      <td class="pf-col-date">${r.createdAtSort || '-'}</td>
      <td class="pf-col-act"><div class="pf-row-actions">
        <button class="pf-row-btn edit js-pf-edit" title="편집">✎</button>
        <button class="pf-row-btn delete js-pf-delete" title="삭제">✕</button>
      </div></td>
    </tr>`;
  }).join('');

  // snapshot 복원 — DOM 교체 직후 편집 input 의 값/포커스/커서 복귀.
  if (_preservedEdit) {
    for (const [id, snap] of Object.entries(_preservedEdit)) {
      const el = document.getElementById(id);
      if (!el) continue;
      el.value = snap.value;
      if (snap.focused) {
        el.focus();
        try {
          if (snap.selStart !== null && snap.selEnd !== null) {
            el.setSelectionRange(snap.selStart, snap.selEnd);
          }
        } catch (e) {}
      }
    }
  }

  // Footer
  tfoot.innerHTML = `<tr>
    <td>합계</td>
    <td class="pf-col-group"></td>
    <td class="pf-col-num pf-col-changepct">${fmtChangePct(dailyReturnPct, totalDailyPnl)}</td>
    <td class="pf-col-benchmark"></td>
    <td class="pf-col-invested"></td>
    <td class="pf-col-num pf-col-buyprice"></td>
    <td class="pf-col-curprice"></td>
    <td class="pf-col-target"></td>
    <td class="pf-col-achiev"></td>
    <td class="pf-col-num pf-col-return"><span class="pf-return ${returnClass(totalReturnPct)}">${fmtPct(totalReturnPct)}</span></td>
    <td class="pf-col-qty"></td>
    <td class="pf-col-num pf-col-mktval">${_fp(totalMarketValue)}</td>
    <td class="pf-col-num pf-col-dividend">${totalDividend > 0 ? _fp(totalDividend) : '-'}</td>
    <td class="pf-col-num pf-col-divyield">${totalDividend > 0 && totalMarketValue > 0 ? fmtPct(totalDividend / totalMarketValue * 100, false) : '-'}</td>
    <td class="pf-col-num pf-col-weight">${fmtPct(grandTotalMarketValue > 0 ? totalMarketValue / grandTotalMarketValue * 100 : 0)}</td>
    <td class="pf-col-date"></td>
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

  // 0% 을 데이터 범위의 경계에 자연스럽게 배치. 전부 양수면 0% 이 맨
  // 아래, 전부 음수면 맨 위.
  const minZ = Math.min(min, 0);
  const maxZ = Math.max(max, 0);
  const rangeZ = maxZ - minZ || 1;

  // v → y 좌표 변환. 기준선·데이터 라인이 동일 수식 사용.
  const yFor = (v) => pad + (1 - (v - minZ) / rangeZ) * (h - pad * 2);
  const zeroY = yFor(0);

  // 0% 기준선 — 연한 점선.
  ctx.save();
  ctx.beginPath();
  ctx.strokeStyle = '#64748b';
  ctx.lineWidth = 1;
  ctx.setLineDash([3, 3]);
  ctx.globalAlpha = 0.5;
  ctx.moveTo(0, zeroY);
  ctx.lineTo(w, zeroY);
  ctx.stroke();
  ctx.restore();

  if (values.length > 1) {
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    values.forEach((v, i) => {
      const x = ((i + offset) / (slots - 1)) * w;
      const y = yFor(v);
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
    // Local date, not UTC — between 00:00 and 09:00 KST toISOString()
    // returns the previous day, so the 1st-2nd of a month would drop
    // the whole sparkline to last month.
    const _d = new Date();
    const thisMonth = `${_d.getFullYear()}-${String(_d.getMonth()+1).padStart(2,'0')}`;
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

  // 기준값 = 전일 22:00 결산값 (pfPrevDaySnapshot.total_value). sparkline
  // 각 점 = (value / prev22 - 1) × 100. 기준선(0%) 은 _drawSparkline 이
  // 자동으로 그림.
  //
  // server snapshot_intraday 가 한 틱에서 일부 종목 가격 fetch 에 실패
  // 하면 그 종목만 avg_price 로 fallback 되어 total_value 가 비정상적
  // 으로 한참 낮게 저장되는 케이스 있음 → sparkline 에 실제로는 없던
  // 급락 dip 이 찍혀 사용자 체감 "잘못된 곳에 뭐가 있다". 인접 포인트
  // 대비 ±3% 이상 튀는 점은 outlier 로 간주해 제외.
  const _prevClose = (pfPrevDaySnapshot && pfPrevDaySnapshot.total_value > 0)
    ? pfPrevDaySnapshot.total_value
    : null;
  if (!_prevClose) {
    _drawSparkline('sparkDaily', [], '#dc2626', 28, 'left');
  } else {
    const raw = [];
    for (const d of pfIntradayData) {
      if (!d || !d.total_value) continue;
      if (d.ts && d.ts.endsWith('T00:00')) continue;   // baseline 제외
      raw.push((d.total_value / _prevClose - 1) * 100);
    }
    if (currentTotalValue) raw.push((currentTotalValue / _prevClose - 1) * 100);
    // Outlier 필터: 현재값 부호와 반대 부호인 점은 일시적 데이터 오류
    // (일부 종목 가격 fetch 실패로 total_value 잘못 저장) 로 간주해 제외.
    // 포트폴리오 전체가 30 분 사이 부호를 뒤집을 정도로 움직이는 건
    // 극히 드묾 — 반대 부호 점 하나가 있다면 데이터 문제일 가능성이
    // 훨씬 큼. 사용자가 '오늘 항상 + 였다' 고 확신하는 케이스와 일치.
    const lastRaw = raw.length ? raw[raw.length - 1] : 0;
    const dayPcts = lastRaw >= 0
      ? raw.filter(p => p >= 0)
      : raw.filter(p => p <= 0);
    const lastPct = dayPcts.length ? dayPcts[dayPcts.length - 1] : 0;
    _drawSparkline('sparkDaily', dayPcts, lastPct >= 0 ? '#dc2626' : '#2563eb', 28, 'left');
  }
}
function fmtNum(n) { return n !== null && n !== undefined ? Number(n).toLocaleString() : '-'; }
function fmtKrw(n) {
  if (n === null || n === undefined) return '-';
  const a = Math.abs(n);
  if (a >= 1e12) { const v = n / 1e12; const d = a >= 1e15 ? 0 : a >= 1e14 ? 1 : a >= 1e13 ? 2 : 3; return v.toFixed(d) + '조'; }
  if (a >= 1e8)  { const v = n / 1e8;  const d = a >= 1e11 ? 0 : a >= 1e10 ? 1 : a >= 1e9 ? 2 : 3;  return v.toFixed(d) + '억'; }
  return Number(Math.round(n)).toLocaleString();
}
function fmtSignedKrw(n) {
  if (n === null) return '-';
  return (n > 0 ? '+' : '') + fmtKrw(n);
}
// signed=false 면 양수에 '+' 를 붙이지 않는다. 달성률·배당수익률·비중
// 같은 '절대 퍼센트' 는 +가 어색하고, 수익률·변동률 같은 '변화 퍼센트'
// 만 +를 보여준다. 기본값은 true(기존 동작) — 호출부에서 명시.
function fmtPct(n, signed = true) {
  if (n === null || n === undefined) return '-';
  const prefix = signed && n > 0 ? '+' : '';
  return prefix + n.toFixed(2) + '%';
}
const _BENCHMARK_PRESETS = [
  {code: 'IDX_KOSPI', name: '코스피'},
  {code: 'IDX_KOSDAQ', name: '코스닥'},
  {code: 'IDX_SP500', name: 'S&P500'},
  {code: 'GOLD', name: '금'},
  {code: 'AGG', name: '미국 종합채권'},
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
  const cls = pct > 0 ? 'positive' : pct < 0 ? 'negative' : '';
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
  picker.dataset.code = stockCode;
  const presets = _BENCHMARK_PRESETS.map(p =>
    `<div class="pf-bm-option js-pf-bench-set${item.benchmark_code === p.code ? ' selected' : ''}" data-bench="${escapeHtml(p.code)}">${escapeHtml(p.name)}</div>`
  ).join('');
  picker.innerHTML = `
    ${presets}
    <div class="pf-bm-custom">
      <input class="pf-bm-input" placeholder="종목코드">
    </div>
    <div class="pf-bm-option pf-bm-reset js-pf-bench-set" data-bench="">기본값으로</div>
  `;
  td.style.position = 'relative';
  td.appendChild(picker);
  const input = picker.querySelector('.pf-bm-input');
  if (input) {
    input.focus();
    // Listener scoped to this picker instance — removed with the node when
    // picker is closed, so no global accumulation.
    input.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter') pfSetBenchmark(stockCode, input.value);
    });
  }
  // Close on outside click. Use { once: true } isn't safe because the
  // first click may be inside; instead remove explicitly when closed.
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
  // When called from the delegated handler the name is looked up locally
  // rather than smuggled through the DOM as a JS-string literal.
  if (stockName === undefined) {
    const existing = portfolioItems.find(i => i.stock_code === stockCode);
    stockName = existing ? existing.stock_name : '';
  }
  // 등록일자는 optional — 비워두면 서버가 기존 값 유지. Input[type=date]
  // 는 YYYY-MM-DD 또는 빈 문자열을 돌려주므로 그대로 전달.
  const createdAtEl = document.getElementById('pfEditCreatedAt');
  const createdAt = createdAtEl ? createdAtEl.value.trim() : '';
  const body = { stock_name: stockName, quantity: qty, avg_price: price };
  if (createdAt) body.created_at = createdAt;
  // 목표가 input — 비워두면 명시 null 로 보내 자동 계산으로 되돌리고,
  // 숫자 있으면 수동 override 로 저장. PUT 에 'target_price' 키가
  // 있으면 서버는 항상 처리 (sentinel preserve 는 키 미전달 시).
  const tgtEl = document.getElementById('pfEditTarget');
  if (tgtEl) {
    const tgtRaw = tgtEl.value.trim();
    body.target_price = tgtRaw === '' ? null : parseFloat(tgtRaw);
  }
  try {
    const resp = await apiFetch(`/api/portfolio/${stockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '저장 실패');
    }
    const data = await resp.json().catch(() => ({}));
    // Update local item without full reload
    const item = portfolioItems.find(i => i.stock_code === stockCode);
    if (item) {
      item.quantity = qty;
      item.avg_price = price;
      item.stock_name = stockName;
      // Server may have normalized or kept created_at — trust its echo.
      if (data.created_at) item.created_at = data.created_at;
      // target_price 도 server 응답을 trust — null/숫자 그대로.
      if ('target_price' in data) item.target_price = data.target_price;
      if ('target_price_disabled' in data) item.target_price_disabled = !!data.target_price_disabled;
    }
    pfEditingCode = null;
    renderPortfolio();
  } catch (e) { showToast(e.message); }
}

// × 버튼 핸들러 — 목표가를 '명시적으로 비움' 상태로 만든다. DB 에
// target_price_disabled=1, target_price=NULL 을 저장. 자동 계산도
// bypass 되어 UI 는 '-' 로 고정. 다시 표시하려면 사용자가 직접 숫자를
// 입력하면 disabled 플래그가 자동 해제된다.
async function clearPortfolioTargetPrice(stockCode) {
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  // 이미 disabled 면 중복 요청 불필요.
  if (item.target_price_disabled) {
    const tgtEl = document.getElementById('pfEditTarget');
    if (tgtEl) tgtEl.value = '';
    return;
  }
  try {
    const resp = await apiFetch(`/api/portfolio/${stockCode}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_name: item.stock_name,
        quantity: item.quantity,
        avg_price: item.avg_price,
        target_price: null,
        target_price_disabled: true,
      }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '목표가 초기화 실패');
    }
    const data = await resp.json().catch(() => ({}));
    if ('target_price' in data) item.target_price = data.target_price;
    else item.target_price = null;
    if ('target_price_disabled' in data) item.target_price_disabled = !!data.target_price_disabled;
    else item.target_price_disabled = 1;
    // 편집 모드면 input 도 즉시 비우기.
    const tgtEl = document.getElementById('pfEditTarget');
    if (tgtEl) tgtEl.value = '';
    renderPortfolio();
    showToast('목표가를 비웠습니다. (- 로 표시)', 'success');
  } catch (e) {
    showToast(e.message);
  }
}

async function deletePortfolioItem(stockCode) {
  // Other destructive actions in this file (group delete, cashflow delete,
  // CSV replace) all confirm first; this one was the outlier, so a
  // misclick on the ✕ in a dense table silently wiped a holding. Look up
  // the display name so the operator sees which stock they're about to
  // remove, not just an opaque code.
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  const displayName = item && item.stock_name
    ? `${item.stock_name} (${stockCode})`
    : stockCode;
  if (!confirm(`"${displayName}" 를 포트폴리오에서 삭제할까요?`)) return;
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

    // Submit the current input — shared by the Enter key and the
    // explicit "등록" button. Resolves the typed text to a canonical
    // stock_code via the backend (so typing "삼성전자" works just as
    // well as "005930") and then falls through to pfAddFromSearch.
    const submitAdd = async () => {
      dropdown.classList.remove('show');
      const q = input.value.trim();
      if (!q) return;
      const resp = await apiFetch(`/api/portfolio/resolve-name?code=${encodeURIComponent(q)}`);
      const data = await resp.json();
      const resolvedCode = data.stock_code || q;
      pfAddFromSearch(resolvedCode, data.stock_name || q);
    };

    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        submitAdd();
      }
    });

    const addBtn = document.getElementById('pfAddBtn');
    if (addBtn) {
      addBtn.addEventListener('click', submitAdd);
    }

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

let _PREFERRED_PAIR_BY_CODE = {};

function _isPreferredStock(code) {
  return Boolean(_PREFERRED_PAIR_BY_CODE[code]) || /^[0-9]{5}[^0]$/.test(code) || /^[0-9]{5}[A-Z]$/.test(code);
}

// 외부 quote 캐시 — 포트폴리오에 없지만 목표가 계산에 필요한 종목들
// (우선주의 보통주, 지주사의 자회사) quote 를 별도로 받아 보관. price
// 는 /api/asset-quotes 가 KRW 환산해서 돌려주므로 단위 일치.
let _EXTERNAL_QUOTE_CACHE = {};   // code → { price, ts }
const _EXTERNAL_QUOTE_TTL = 60 * 1000;
let _externalFetchInflight = false;

async function _ensureExternalQuotes(codes) {
  // 인자: 임의 종목 코드 리스트. 캐시에 없거나 stale 한 것만 fetch.
  // inflight guard 로 같은 렌더 사이클의 중복 호출 방지.
  if (_externalFetchInflight) return;
  const needed = new Set();
  const now = Date.now();
  for (const code of codes) {
    if (!code) continue;
    const cached = _EXTERNAL_QUOTE_CACHE[code];
    if (!cached || (now - cached.ts) > _EXTERNAL_QUOTE_TTL) {
      needed.add(code);
    }
  }
  if (!needed.size) return;
  _externalFetchInflight = true;
  try {
    const resp = await apiFetch('/api/asset-quotes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ codes: [...needed] }),
    });
    if (!resp.ok) return;
    const data = await resp.json();
    for (const [code, q] of Object.entries(data)) {
      if (q && q.price != null) {
        _EXTERNAL_QUOTE_CACHE[code] = { price: Number(q.price), ts: Date.now() };
      }
    }
    // 새 quote 도착 → 의존 row (우선주 / 지주사) 의 목표가 다시 그리기
    if (typeof renderPortfolio === 'function') renderPortfolio();
  } catch (e) {
    console.warn('external quote fetch failed', e);
  } finally {
    _externalFetchInflight = false;
  }
}

// 목표가 계산. 반환:
//   - null  → '-' 표시 (CASH_ 등 의미 없음, 또는 명시적 비움)
//   - 숫자 → 그 값. 계산 출처는 _targetPriceSource 가 별도 알려줌.
function _computeTargetPrice(item, allItems) {
  const code = item.stock_code;
  // 현금/통화: 목표가 개념 없음
  if (code.startsWith('CASH_')) return null;
  // 사용자가 × 로 명시적 비움 → 자동 계산도 하지 않고 '-' 로 표시
  if (item.target_price_disabled) return null;
  // 사용자 수동 override 우선
  if (item.target_price != null) return Number(item.target_price);

  // 우선주 → 보통주 현재가
  if (_isPreferredStock(code)) {
    const commonCode = code.slice(0, -1) + '0';
    const commonItem = allItems.find(i => i.stock_code === commonCode);
    const commonPrice = commonItem?.quote?.price;
    if (commonPrice != null) return Number(commonPrice);
    // 보통주가 포트폴리오에 없으면 _EXTERNAL_QUOTE_CACHE 에서 시도
    // (편의상 같은 캐시 재활용 — 다른 데서도 fetch 가능).
    const cached = _EXTERNAL_QUOTE_CACHE[commonCode];
    if (cached && cached.price != null) return cached.price;
    // 보통주 가격 모름 → fallback 으로 매입가 × 1.3
    return item.avg_price * 1.3;
  }

  // 지주사 → NAV per share = Σ(자회사 price × sharesHeld) / (total - treasury)
  const meta = _HOLDING_META[code];
  if (meta && meta.totalShares > 0) {
    let subTotal = 0;
    let allHave = true;
    for (const sub of meta.subsidiaries || []) {
      const cached = _EXTERNAL_QUOTE_CACHE[sub.code];
      const inPort = allItems.find(i => i.stock_code === sub.code);
      const subPrice = inPort?.quote?.price ?? cached?.price;
      if (subPrice == null) { allHave = false; break; }
      subTotal += Number(subPrice) * (sub.sharesHeld || 0);
    }
    if (allHave && subTotal > 0) {
      const free = meta.totalShares - (meta.treasuryShares || 0);
      if (free > 0) return subTotal / free;
    }
    // 자회사 quote 미로딩 → 일단 매입가 × 1.3 (다음 렌더에 자연 갱신)
    return item.avg_price * 1.3;
  }

  // 그 외 일반 종목 → 매입가 × 1.3
  return item.avg_price * 1.3;
}

function _targetPriceSource(item) {
  const code = item.stock_code;
  if (code.startsWith('CASH_')) return 'cash';
  if (item.target_price_disabled) return 'disabled';
  if (item.target_price != null) return 'manual';
  if (_isPreferredStock(code)) return 'preferred';
  if (_HOLDING_META[code]) return 'holding';
  return 'default';
}

function _initPreferredPairsFromConfig() {
  const pairsByCode = getIntegrationConfig('preferredSpread').pairsByPreferredCode || {};
  _PREFERRED_PAIR_BY_CODE = pairsByCode && typeof pairsByCode === 'object' ? pairsByCode : {};
}

_initPreferredPairsFromConfig();

function _preferredCommonCodeFor(code) {
  const pair = _PREFERRED_PAIR_BY_CODE[code];
  if (pair && pair.commonCode) return pair.commonCode;
  return code.slice(0, -1) + '0';
}

function _goldGapInfoForCode(code) {
  const config = getIntegrationConfig('goldGap');
  const assetByCode = config.assetByPortfolioCode || {};
  const fallbackAsset = {
    KRX_GOLD: 'gold',
    CRYPTO_BTC: 'bitcoin',
  }[code] || '';
  const asset = assetByCode[code] || fallbackAsset;
  if (!asset) return { asset: '', label: '', title: '' };
  const assetConfig = (config.assets && config.assets[asset]) || {};
  const latestGap = Number(assetConfig.latestGapPct);
  const hasLatestGap = Number.isFinite(latestGap);
  const label = hasLatestGap ? `Gap ${latestGap >= 0 ? '+' : ''}${latestGap.toFixed(1)}%` : 'Gap';
  const titleParts = [assetConfig.label || asset, 'gap dashboard'];
  if (hasLatestGap) titleParts.push(`latest ${latestGap.toFixed(2)}%`);
  if (assetConfig.latestDate) titleParts.push(assetConfig.latestDate);
  return { asset, label, title: titleParts.join(' · ') };
}

function _openGoldGapDashboard(asset) {
  openIntegration('goldGap', '', { asset });
}

function _isKoreanAnalysisCode(code) {
  return typeof code === 'string' && code.length === 6 && /^\d{5}/.test(code);
}

function _hasAssetInsight(code) {
  return Boolean(code);
}

function _analysisAction(stockCode, label = '분석 화면', hint = '재무/밸류에이션 분석') {
  return {
    id: `analysis-${stockCode}`,
    label,
    hint,
    run: () => {
      switchView('analysis');
      analyzeStock(stockCode);
    },
  };
}

function _portfolioLinkActions(stockCode) {
  const actions = [];
  if (_isKoreanAnalysisCode(stockCode)) {
    if (_isPreferredStock(stockCode)) {
      const commonCode = _preferredCommonCodeFor(stockCode);
      actions.push(_analysisAction(commonCode, `본주 분석 (${commonCode})`));
      actions.push({
        id: 'preferred-spread',
        label: '우선주 괴리율',
        hint: '보통주 대비 스프레드',
        run: () => openIntegration('preferredSpread', '', { code: stockCode }),
      });
    } else {
      actions.push(_analysisAction(stockCode));
      if (_HOLDING_CODES.has(stockCode)) {
        actions.push({
          id: 'holding-value',
          label: '자회사 비율 추이',
          hint: 'Holding Value 대시보드',
          run: () => openIntegration('holdingValue', '', { code: stockCode }),
        });
      }
    }
  }
  if (_hasAssetInsight(stockCode)) {
    actions.push({
      id: 'insight',
      label: '투자 인사이트',
      hint: '가격 추세, 벤치마크, 시장 지표',
      run: () => pfOpenAssetInsight(stockCode),
    });
  }
  const goldGapInfo = _goldGapInfoForCode(stockCode);
  if (goldGapInfo.asset) {
    actions.push({
      id: 'gold-gap',
      label: goldGapInfo.label || 'Gap',
      hint: goldGapInfo.title || 'Gap 대시보드',
      run: () => _openGoldGapDashboard(goldGapInfo.asset),
    });
  }
  return actions;
}

function _runOrShowPortfolioLinks(stockCode, e) {
  const actions = _portfolioLinkActions(stockCode);
  if (actions.length === 0) return false;
  if (actions.length === 1) {
    actions[0].run();
    return true;
  }
  _showPortfolioLinkMenu(actions, e);
  return true;
}

function _showPortfolioLinkMenu(actions, e) {
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  const menu = document.createElement('div');
  menu.className = 'pf-pref-menu pf-link-menu';
  menu.innerHTML = actions.map((action, idx) => `
    <div class="pf-pref-item" data-action-idx="${idx}">
      <strong>${escapeHtml(action.label)}</strong>
      <span>${escapeHtml(action.hint || '')}</span>
    </div>
  `).join('');
  document.body.appendChild(menu);
  const rect = e && e.target ? e.target.getBoundingClientRect() : { left: 100, bottom: 100 };
  menu.style.left = rect.left + 'px';
  menu.style.top = (rect.bottom + 4) + 'px';
  menu.querySelectorAll('[data-action-idx]').forEach(el => {
    el.addEventListener('click', () => {
      const action = actions[Number(el.dataset.actionIdx)];
      menu.remove();
      if (action) action.run();
    });
  });
  setTimeout(() => {
    document.addEventListener('click', function close(ev) {
      if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close); }
    });
  }, 0);
}

let pfAssetInsightCode = null;

function pfCloseAssetInsight() {
  const modal = document.getElementById('pfAssetInsightModal');
  if (modal) modal.style.display = 'none';
  pfAssetInsightCode = null;
}

async function pfOpenAssetInsight(stockCode) {
  const modal = document.getElementById('pfAssetInsightModal');
  const title = document.getElementById('pfAssetInsightTitle');
  const body = document.getElementById('pfAssetInsightBody');
  if (!modal || !body || !stockCode) return;
  pfAssetInsightCode = stockCode;
  modal.style.display = 'flex';
  if (title) title.textContent = '투자 인사이트';
  body.innerHTML = '<div class="pf-insight-loading">자산 데이터를 불러오는 중입니다...</div>';

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 15000);
  try {
    const resp = await apiFetch(`/api/portfolio/asset-insight/${encodeURIComponent(stockCode)}`, { signal: controller.signal });
    if (!resp.ok) {
      let detail = '';
      try {
        const err = await resp.json();
        detail = err.detail || err.message || '';
      } catch (e) {}
      throw new Error(detail || `인사이트 로드 실패 (${resp.status})`);
    }
    const data = await resp.json();
    if (pfAssetInsightCode !== stockCode) return;
    const profile = data.profile || {};
    if (title) title.textContent = `${profile.name || stockCode} 투자 인사이트`;
    body.innerHTML = _renderAssetInsight(data);
  } catch (e) {
    if (pfAssetInsightCode !== stockCode) return;
    const message = e.name === 'AbortError'
      ? '인사이트 조회가 15초를 넘겨 중단되었습니다. 잠시 후 다시 열면 캐시된 데이터로 더 빨라질 수 있습니다.'
      : (e.message || '인사이트를 불러오지 못했습니다.');
    body.innerHTML = `<div class="pf-insight-error">${escapeHtml(message)}</div>`;
  } finally {
    clearTimeout(timeoutId);
  }
}

function _insightNum(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function _fmtInsightPct(value, signed = true) {
  const n = _insightNum(value);
  return n === null ? '-' : fmtPct(n, signed);
}

function _fmtInsightSignedAmount(value, currency = '') {
  const n = _insightNum(value);
  if (n === null) return '-';
  const prefix = n > 0 ? '+' : '';
  return `${prefix}${_fmtInsightAmount(n, currency)}`;
}

function _fmtInsightAmount(value, currency = '') {
  const n = _insightNum(value);
  if (n === null) return '-';
  const cur = String(currency || '').toUpperCase();
  if (!cur || cur === 'KRW') return fmtKrw(Math.round(n));
  const digits = Math.abs(n) >= 100 ? 2 : 4;
  return `${cur} ${n.toLocaleString(undefined, { maximumFractionDigits: digits })}`;
}

function _fmtInsightPrice(value, currency = '') {
  const n = _insightNum(value);
  if (n === null) return '-';
  const cur = String(currency || '').toUpperCase();
  if (!cur || cur === 'KRW') return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return `${cur} ${n.toLocaleString(undefined, { maximumFractionDigits: 4 })}`;
}

function _insightClass(value) {
  const n = _insightNum(value);
  if (n === null) return '';
  return n > 0 ? 'positive' : n < 0 ? 'negative' : '';
}

function _renderInsightCard(label, value, sub = '', cls = '') {
  return `<div class="pf-insight-card">
    <div class="pf-insight-card-label">${escapeHtml(label)}</div>
    <div class="pf-insight-card-value ${cls}">${escapeHtml(value)}</div>
    ${sub ? `<div class="pf-insight-card-sub">${escapeHtml(sub)}</div>` : ''}
  </div>`;
}

function _renderAssetInsight(data) {
  const profile = data.profile || {};
  const position = data.position || {};
  const metrics = data.metrics || {};
  const returns = metrics.returns || {};
  const volatility = metrics.volatility || {};
  const benchmark = data.benchmark || {};
  const benchmarkReturns = benchmark.returns || {};
  const relativeReturns = benchmark.relativeReturns || {};
  const quality = data.dataQuality || {};
  const code = profile.code || '';
  const historyCurrency = (
    profile.currency ||
    quality.historyCurrency ||
    (code === 'KRX_GOLD' || String(code).startsWith('CASH_') ? 'KRW' : '')
  );
  const positionCurrency = 'KRW';

  const cards = [
    _renderInsightCard('현재가', _fmtInsightPrice(position.currentPrice, positionCurrency), benchmark.dayChangePct !== null && benchmark.dayChangePct !== undefined ? `벤치마크 오늘 ${_fmtInsightPct(benchmark.dayChangePct)}` : ''),
    _renderInsightCard('평가금액', _fmtInsightAmount(position.marketValue, positionCurrency), `투입 ${_fmtInsightAmount(position.invested, positionCurrency)}`),
    _renderInsightCard('보유 수익률', _fmtInsightPct(position.returnPct), _fmtInsightSignedAmount(position.pnl, positionCurrency), _insightClass(position.returnPct)),
    _renderInsightCard('오늘 손익', _fmtInsightPct(position.dailyChangePct), _fmtInsightSignedAmount(position.dailyPnl, positionCurrency), _insightClass(position.dailyPnl)),
    _renderInsightCard('최근 3개월', _fmtInsightPct(returns['3m']), `벤치마크 대비 ${_fmtInsightPct(relativeReturns['3m'])}`, _insightClass(returns['3m'])),
    _renderInsightCard('60일 변동성', _fmtInsightPct(volatility['60d'], false), '연율화 기준'),
    _renderInsightCard('최대 낙폭', _fmtInsightPct(metrics.maxDrawdownPct), '최근 1년 가격 기준', _insightClass(metrics.maxDrawdownPct)),
    _renderInsightCard('52주 고점 대비', _fmtInsightPct(metrics.fromHigh52Pct), `저점 대비 ${_fmtInsightPct(metrics.fromLow52Pct)}`, _insightClass(metrics.fromHigh52Pct)),
  ].join('');

  const windows = [
    ['1m', '1개월'],
    ['3m', '3개월'],
    ['6m', '6개월'],
    ['1y', '1년'],
  ];
  const returnRows = windows.map(([key, label]) => `
    <tr>
      <td>${label}</td>
      <td class="${_insightClass(returns[key])}">${_fmtInsightPct(returns[key])}</td>
      <td class="${_insightClass(benchmarkReturns[key])}">${_fmtInsightPct(benchmarkReturns[key])}</td>
      <td class="${_insightClass(relativeReturns[key])}">${_fmtInsightPct(relativeReturns[key])}</td>
    </tr>
  `).join('');

  const signals = Array.isArray(data.signals) && data.signals.length ? data.signals : [{
    level: 'neutral',
    title: '추가 경고 신호 없음',
    body: '현재 확보된 가격/벤치마크 데이터 기준으로는 큰 이상 신호가 보이지 않습니다.',
  }];
  const signalHtml = signals.map(s => `
    <div class="pf-insight-signal ${escapeHtml(s.level || 'neutral')}">
      <strong>${escapeHtml(s.title || '')}</strong>
      <span>${escapeHtml(s.body || '')}</span>
    </div>
  `).join('');

  const macro = Array.isArray(data.macro) ? data.macro : [];
  const macroHtml = macro.length ? macro.map(m => `
    <div class="pf-insight-macro-item">
      <span>${escapeHtml(m.label || m.code || '')}</span>
      <strong>${escapeHtml(m.value || '-')}</strong>
      <em class="${m.direction === 'up' ? 'positive' : m.direction === 'down' ? 'negative' : ''}">${escapeHtml(m.changePct || m.change || '')}</em>
    </div>
  `).join('') : '<div class="pf-insight-empty">연동된 시장 지표가 없습니다.</div>';

  const goldGap = data.goldGap;
  const goldGapHtml = goldGap ? `
    <div class="pf-insight-gold-gap">
      <div>
        <span class="pf-insight-section-kicker">Gold Gap</span>
        <strong>${escapeHtml(goldGap.label || goldGap.asset || '')}</strong>
        <p>최근 괴리율 ${_fmtInsightPct(goldGap.latestGapPct)}${goldGap.latestDate ? ` · ${escapeHtml(goldGap.latestDate)}` : ''}</p>
      </div>
      <button class="pf-insight-link js-pf-gold-gap" data-gap-asset="${escapeHtml(goldGap.asset || '')}">대시보드 열기</button>
    </div>
  ` : '';

  return `
    <div class="pf-insight-hero">
      <div>
        <div class="pf-insight-kicker">${escapeHtml(code)}</div>
        <h4>${escapeHtml(profile.name || code)}</h4>
        <p>${escapeHtml(profile.assetClassLabel || '기타 자산')} 자산을 가격 추세, 벤치마크, 매크로 지표 기준으로 빠르게 점검합니다.</p>
      </div>
      <div class="pf-insight-chips">
        <span>${escapeHtml(profile.assetClassLabel || '자산')}</span>
        <span>${escapeHtml(historyCurrency || '통화 미확인')}</span>
        <span>BM ${escapeHtml(benchmark.name || profile.benchmarkName || '-')}</span>
        <span>${Number(quality.historyPoints || 0).toLocaleString()} pts</span>
      </div>
    </div>
    <div class="pf-insight-grid">${cards}</div>
    <div class="pf-insight-two-col">
      <section class="pf-insight-section">
        <div class="pf-insight-section-title">수익률 비교</div>
        <table class="pf-insight-table">
          <thead><tr><th>기간</th><th>자산</th><th>벤치마크</th><th>초과</th></tr></thead>
          <tbody>${returnRows}</tbody>
        </table>
      </section>
      <section class="pf-insight-section">
        <div class="pf-insight-section-title">체크 포인트</div>
        <div class="pf-insight-signals">${signalHtml}</div>
      </section>
    </div>
    <section class="pf-insight-section">
      <div class="pf-insight-section-title">시장 배경</div>
      <div class="pf-insight-macro">${macroHtml}</div>
    </section>
    ${goldGapHtml}
  `;
}

let _HOLDING_CODES = new Set([
  '000670','000880','002790','003380','004360','004700','004800',
  '005810','006120','024800','028260','030530','032830',
  '036710','051910','058650','402340',
]);
// 지주사 코드 → 메타 (자회사 + 발행주식수). NAV per share 계산에 사용.
// holdings.json 의 단순 코드 Set 외에 subsidiaries / totalShares /
// treasuryShares 도 캐시. localStorage 키도 holdingCodes (Set 호환) 와
// 새 holdingMeta 두 가지 병행 — 옛 키만 있는 사용자도 호환되도록 폴백.
let _HOLDING_META = {};

function _applyHoldingPayload(data, persist) {
  const codes = (data.items || []).map(i => i.holdingCode).filter(Boolean);
  const meta = {};
  for (const it of (data.items || [])) {
    if (!it.holdingCode) continue;
    meta[it.holdingCode] = {
      totalShares: it.holdingTotalShares || 0,
      treasuryShares: it.holdingTreasuryShares || 0,
      subsidiaries: (it.subsidiaries || [])
        .filter(s => s.code && s.sharesHeld != null)
        .map(s => ({ code: s.code, sharesHeld: s.sharesHeld })),
    };
  }
  if (!codes.length) return false;
  _HOLDING_CODES = new Set(codes);
  _HOLDING_META = meta;
  if (persist) {
    localStorage.setItem('holdingCodes', JSON.stringify({ codes, ts: Date.now() }));
    localStorage.setItem('holdingMeta', JSON.stringify({ meta, ts: Date.now() }));
  }
  return true;
}

function _applyHoldingIntegrationConfig() {
  const config = getIntegrationConfig('holdingValue');
  if (Array.isArray(config.items) && _applyHoldingPayload({ items: config.items }, false)) return true;
  let applied = false;
  if (Array.isArray(config.codes) && config.codes.length) {
    _HOLDING_CODES = new Set(config.codes);
    applied = true;
  }
  if (config.meta && typeof config.meta === 'object') {
    _HOLDING_META = config.meta;
    applied = true;
  }
  return applied;
}

(function _refreshHoldingCodes() {
  const hasConfig = _applyHoldingIntegrationConfig();
  try {
    const codeCache = JSON.parse(localStorage.getItem('holdingCodes') || '{}');
    const metaCache = JSON.parse(localStorage.getItem('holdingMeta') || '{}');
    if (!hasConfig) {
      if (codeCache.codes) _HOLDING_CODES = new Set(codeCache.codes);
      if (metaCache.meta) _HOLDING_META = metaCache.meta;
    }
    if (hasConfig || (codeCache.ts && metaCache.ts && Date.now() - Math.min(codeCache.ts, metaCache.ts) < 86400000)) return;
  } catch (e) { console.warn(e); }
  const holdingsUrl = getIntegrationEndpoint('holdingValue', 'holdingsUrl', 'api/holdings.json');
  if (!holdingsUrl) return;
  fetch(holdingsUrl)
    .then(r => r.json())
    .then(data => { _applyHoldingPayload(data, true); })
    .catch(() => {});
})();

function pfGoAnalyze(stockCode, e) {
  if (_runOrShowPortfolioLinks(stockCode, e)) {
    return;
  }
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
    openIntegration('preferredSpread', '', { code: prefCode });
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
    openIntegration('holdingValue', '', { code: stockCode });
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
  // Compute per-group stats.
  // prevMV is derived from the LIVE quote (qty × (price - change)) rather
  // than from yesterday's snapshot, so composition drift (qty changes,
  // items added since the snapshot) cannot push group% outside the range
  // of its constituents. Missing-quote items are excluded from both sides.
  const stats = {};
  let grandMV = 0;
  portfolioItems.forEach(i => {
    const gn = pfGetGroup(i);
    if (!stats[gn]) stats[gn] = { cnt: 0, invested: 0, mv: 0, prevMV: 0 };
    const s = stats[gn];
    const q = i.quote || {};
    const price = q.price ?? null;
    const change = q.change ?? 0;
    const qty = i.quantity;
    const avgPrice = i.avg_price;
    s.cnt++;
    s.invested += qty * avgPrice;
    if (price !== null) {
      const mv = qty * price;
      s.mv += mv;
      s.prevMV += qty * (price - change);
      grandMV += mv;
    }
  });
  const defaultCount = pfGroups.filter(x => x.is_default).length;
  const rowsHtml = pfGroups.map((g, i) => {
    const s = stats[g.group_name] || { cnt: 0, invested: 0, mv: 0, prevMV: 0 };
    const weight = grandMV > 0 ? (s.mv / grandMV * 100) : 0;
    const returnPct = s.invested !== 0 ? ((s.mv - s.invested) / Math.abs(s.invested) * 100) : 0;
    const dailyPnl = s.prevMV !== 0 ? (s.mv - s.prevMV) : 0;
    const dailyPct = s.prevMV !== 0 ? (dailyPnl / Math.abs(s.prevMV) * 100) : 0;
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

async function _pollBenchmarkQuotes() {
  try {
    const r = await apiFetch('/api/portfolio/benchmark-quotes');
    if (!r.ok) return;
    const fresh = await r.json();
    for (const [k, v] of Object.entries(fresh)) pfBenchmarkQuotes[k] = v;
    // 전체 재렌더 대신 벤치마크 셀만 업데이트 — 이래야 WS tick 으로 in-
    // place 갱신된 다른 셀들이 60초 polling 때마다 뒤집히지 않음.
    if (activeView === 'portfolio') {
      for (const k of Object.keys(fresh)) updatePortfolioBenchmarkCells(k);
    }
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

// --- AI Analysis ---
let _aiModelsLoaded = false;
async function _loadAiModels() {
  if (_aiModelsLoaded) return;
  // Show model picker only for admin
  if (typeof currentUser === 'undefined' || !currentUser || !currentUser.is_admin) return;
  const picker = document.getElementById('pfAiModelPicker');
  if (!picker) return;
  picker.style.display = '';
  try {
    const resp = await apiFetch('/api/portfolio/ai-models');
    if (!resp.ok) return;
    const data = await resp.json();
    const input = document.getElementById('pfAiModelInput');
    const datalist = document.getElementById('pfAiModelList');
    input.value = data.default || '';
    datalist.innerHTML = data.models.map(m =>
      `<option value="${m.id}">${m.name} ($${m.prompt_price.toFixed(2)}/$${m.completion_price.toFixed(2)} per 1M)</option>`
    ).join('');
    _aiModelsLoaded = true;
  } catch {}
}

async function runAiAnalysis() {
  _loadAiModels();
  _ensureFxRate();
  const btn = document.getElementById('pfAiBtn');
  const result = document.getElementById('pfAiResult');
  const tokens = document.getElementById('pfAiTokens');
  btn.disabled = true;
  btn.textContent = '분석 중...';
  result.textContent = '';
  tokens.textContent = '';

  const modelInput = document.getElementById('pfAiModelInput');
  const selectedModel = modelInput ? modelInput.value.trim() : '';
  const queryInput = document.getElementById('pfAiQuery');
  const userQuery = queryInput ? queryInput.value.trim() : '';
  const payload = {};
  if (selectedModel) payload.model = selectedModel;
  if (userQuery) payload.query = userQuery;
  const body = JSON.stringify(payload);

  let mdText = '';
  try {
    const resp = await apiFetch('/api/portfolio/ai-analysis', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${resp.status}`);
    }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const d = JSON.parse(line.slice(6));
          if (d.content) {
            mdText += d.content;
            // Live preview: render markdown as it streams.
            // Sanitize with DOMPurify — prompt injection via portfolio
            // names could otherwise cause the model to echo raw HTML.
            if (typeof marked !== 'undefined') {
              result.innerHTML = _renderSafeMarkdown(mdText);
            } else {
              result.textContent = mdText;
            }
          }
          if (d.done) {
            const model = d.model ? ` · ${d.model}` : '';
            const costUsd = Number(d.cost || 0);
            const costKrw = costUsd && pfFxRate ? Math.round(costUsd * pfFxRate) : null;
            const cost = costUsd ? ` · ${costKrw !== null ? costKrw.toLocaleString() + '원' : '$' + costUsd.toFixed(6)}` : '';
            const wikiN = Number(d.wiki_used || 0);
            const wikiTag = wikiN > 0 ? ` · 리포트 ${wikiN}건 참조` : '';
            tokens.textContent = `입력 ${d.input_tokens?.toLocaleString() || '?'} / 출력 ${d.output_tokens?.toLocaleString() || '?'} 토큰${cost}${model}${wikiTag}`;
          }
        } catch {}
      }
    }
    // Final render
    if (typeof marked !== 'undefined' && mdText) result.innerHTML = _renderSafeMarkdown(mdText);
  } catch (e) {
    result.textContent = '분석 실패: ' + e.message;
  }
  btn.disabled = false;
  btn.textContent = '분석 실행';
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
  if (tab === 'performance') { loadPerformanceData(); _loadAiModels(); }
}

// 영역지도 팝업 — 기존에는 보유종목 탭 안에서 테이블/영역지도 토글 뷰로
// 존재했으나, 영역지도만 단독으로 보면 썰렁해 보이고 테이블과 전환하며
// 보는 니즈도 낮아 모달로 전환. ESC / backdrop / ✕ 모두 닫기 지원.
function pfOpenTreemap() {
  const modal = document.getElementById('pfTreemapModal');
  if (!modal) return;
  modal.style.display = 'flex';
  // ECharts 는 컨테이너가 화면에 보여진 뒤 init/resize 해야 크기를 맞게
  // 측정함. display 전환 → 레이아웃 확정 → 측정 순서 보장 위해 두 번째
  // rAF 에서 렌더 (첫 rAF 는 style flush, 두 번째에 실제 크기가 확정).
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      if (!USE_UPLOT) renderTreemap();
    });
  });
  document.addEventListener('keydown', _pfTreemapEscHandler);
}

function pfCloseTreemap() {
  const modal = document.getElementById('pfTreemapModal');
  if (!modal) return;
  modal.style.display = 'none';
  document.removeEventListener('keydown', _pfTreemapEscHandler);
  // ECharts 인스턴스 해제 — 다음 open 에서 다시 그림. 메모리 관리 겸
  // 닫은 뒤 브라우저 창 리사이즈 때 hidden 컨테이너에 대고 resize 가
  // 호출되지 않도록. ResizeObserver 도 함께 해제.
  if (_treemapInstance) { _treemapInstance.dispose(); _treemapInstance = null; }
  if (_treemapResizeObserver) { _treemapResizeObserver.disconnect(); _treemapResizeObserver = null; }
}

function _pfTreemapEscHandler(e) {
  if (e.key === 'Escape') pfCloseTreemap();
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
    // treemap 은 이제 보유종목 탭 소관 — 여기서 호출 안 함
    renderNavChart(navData);
    renderValueChart(navData);
    renderNavReturns(navData);
    renderCashflows(cfData);
  } catch (e) { console.warn(e); }
}

let _treemapInstance = null;
// 모달 open 시점에 flex 레이아웃이 아직 확정 안 돼 container height 가
// 0 에 가까운 상태로 echarts.init 이 불리면 treemap 이 상단 일부에만
// 그려지는 증상이 있음. ResizeObserver 로 container 크기 변화를 잡아
// 자동 ec.resize() 해주면 레이아웃 확정 순간 바로 교정된다.
let _treemapResizeObserver = null;

async function renderTreemap() {
  const container = document.getElementById('pfTreemap');
  if (!container) return;
  if (_treemapInstance) { _treemapInstance.dispose(); _treemapInstance = null; }
  if (_treemapResizeObserver) { _treemapResizeObserver.disconnect(); _treemapResizeObserver = null; }

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

  // Container 가 flex 레이아웃 완료 전이라 처음 init 크기가 잘못
  // 잡혔더라도, ResizeObserver 가 실제 확정 크기를 감지해 즉시 resize.
  // 모달 open 시 '위쪽 반만 그려지는' 증상의 근본 대책.
  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => {
      if (_treemapInstance) _treemapInstance.resize();
    });
    ro.observe(container);
    _treemapResizeObserver = ro;
  }

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
              const wStr = d.weight !== undefined ? ` (${d.weight.toFixed(1)}%)` : '';
              if (d.changePct === null || d.changePct === undefined) return params.name + wStr;
              return `${params.name}  ${_fmtPct(d.changePct)}${wStr}`;
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
// 탭 전환 등으로 container 크기가 늦게 확정될 때 ECharts 가 0 크기로
// init 되어 차트가 아예 안 보이는 증상 대응. treemap 과 동일 패턴.
let _navChartResizeObserver = null;
let _valueChartResizeObserver = null;
let _navChartData = [];  // cached for benchmark overlay
let _benchCache = {};    // code -> [{date, close}]

const _BENCH_COLORS = { KOSPI: '#e74c3c', SP500: '#2563eb', GOLD: '#f59e0b' };
const _BENCH_LABELS = { KOSPI: '코스피', SP500: 'S&P 500', GOLD: '금' };

function _getSelectedBenchmarks() {
  return Array.from(document.querySelectorAll('.pf-bench-chip input[value]:checked')).map(el => el.value);
}

async function onBenchToggle() {
  const codes = _getSelectedBenchmarks();
  if (!_navChartData.length) return;
  // Fetch any uncached benchmarks
  const startDate = _navChartData[0].date;
  const toFetch = codes.filter(c => !_benchCache[c]);
  if (toFetch.length) {
    const results = await Promise.all(toFetch.map(c =>
      apiFetch(`/api/portfolio/benchmark-history?code=${c}&start=${startDate}`).then(r => r.ok ? r.json() : []).catch(() => [])
    ));
    toFetch.forEach((c, i) => { _benchCache[c] = results[i]; });
  }
  // Preserve the current zoom window across re-render. renderNavChart()
  // disposes the chart instance and recreates it from scratch, which would
  // otherwise reset dataZoom to 0~100 — i.e. the user loses their 3M/6M/1Y
  // selection just for checking a benchmark box.
  let preservedZoom = null;
  if (_navChartInstance) {
    try {
      const opt = _navChartInstance.getOption();
      const dz = opt?.dataZoom?.[0];
      if (dz && (dz.start != null || dz.end != null)) {
        preservedZoom = { start: dz.start ?? 0, end: dz.end ?? 100 };
      }
    } catch (_) { /* getOption can throw if chart is mid-dispose */ }
  }
  await renderNavChart(_navChartData);
  if (preservedZoom && _navChartInstance) {
    // dispatchAction fires the datazoom listener inside renderNavChart,
    // which re-scales benchmark series to match the restored window.
    _navChartInstance.dispatchAction({
      type: 'dataZoom',
      start: preservedZoom.start,
      end: preservedZoom.end,
    });
  }
}

// Per-benchmark raw ratio arrays (bench_close / bench_close[0]), computed once.
// On zoom, we multiply by navValues[zoomStartIdx] to scale into NAV space.
let _benchRatios = {};  // code -> { ratioByLabel: {date: ratio}, labels }

async function renderNavChart(data) {
  const container = document.getElementById('pfNavChart');
  if (!container) return;
  if (_navChartInstance) { _navChartInstance.dispose(); _navChartInstance = null; }
  if (_navChartResizeObserver) { _navChartResizeObserver.disconnect(); _navChartResizeObserver = null; }
  await loadChartLib();
  _navChartData = data;

  if (!data.length) {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">스냅샷 데이터가 없습니다.</div>';
    return;
  }

  const navValues = data.map(d => {
    if (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) return d.nav / d.fx_usdkrw;
    return d.nav;
  });
  const labels = data.map(d => d.date);

  const last365 = data.slice(-365);
  const yoyPct = last365.length > 1
    ? ((navValues[navValues.length - 1] / navValues[navValues.length - last365.length]) - 1) * 100 : 0;
  const navColor = returnToColor(yoyPct);

  // Precompute benchmark ratio maps (close / first_close for each date)
  const benchCodes = _getSelectedBenchmarks();
  _benchRatios = {};
  for (const code of benchCodes) {
    const raw = _benchCache[code] || [];
    if (!raw.length) continue;
    const firstClose = raw[0].close;
    if (!firstClose) continue;
    const ratioByLabel = {};
    raw.forEach(d => { ratioByLabel[d.date] = d.close / firstClose; });
    _benchRatios[code] = ratioByLabel;
  }

  // Build benchmark series scaled to NAV at index 0
  function buildBenchSeries(startIdx) {
    const series = [];
    const navAtStart = navValues[startIdx];
    for (const code of benchCodes) {
      const ratioMap = _benchRatios[code];
      if (!ratioMap) continue;
      // Find the ratio at the start index (first overlapping date from startIdx onward)
      let baseRatio = null;
      for (let i = startIdx; i < labels.length; i++) {
        if (ratioMap[labels[i]] != null) { baseRatio = ratioMap[labels[i]]; break; }
      }
      if (!baseRatio) continue;
      // Scale: benchNAV = navAtStart * (ratio / baseRatio)
      const vals = labels.map(lbl => {
        const r = ratioMap[lbl];
        return r != null ? navAtStart * (r / baseRatio) : null;
      });
      series.push({
        name: _BENCH_LABELS[code] || code,
        type: 'line',
        data: vals.map(v => v === null ? '-' : v),
        smooth: 0.3,
        symbol: 'none',
        lineStyle: { color: _BENCH_COLORS[code], width: 1.5, type: 'dashed' },
        itemStyle: { color: _BENCH_COLORS[code] },
        connectNulls: true,
      });
    }
    return series;
  }

  const hasBench = Object.keys(_benchRatios).length > 0;
  const legendData = ['NAV', ...benchCodes.filter(c => _benchRatios[c]).map(c => _BENCH_LABELS[c] || c)];
  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#333';
  const yZero = document.getElementById('pfNavYZero')?.checked;

  // 모바일: 탭 전환 직후 container height 가 layout 확정 전이라 echarts
  // 가 0-크기로 init 되어 차트 안 보임. rAF 두 번으로 layout 확실히
  // 끝난 뒤 init 하고, 그래도 혹시 늦으면 ResizeObserver 가 추가 보완.
  await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));

  const ec = echarts.init(container);

  const initBenchSeries = buildBenchSeries(0);

  ec.setOption({
    legend: hasBench ? {
      data: legendData,
      top: 0, right: 0,
      textStyle: { color: textColor, fontSize: 11 },
      itemWidth: 18, itemHeight: 2,
    } : undefined,
    grid: { left: 55, right: 12, top: hasBench ? 28 : 10, bottom: 56 },
    dataZoom: [
      { type: 'slider', height: 22, bottom: 4, borderColor: gridColor, fillerColor: _hexToRgba(navColor, 0.12),
        handleStyle: { color: navColor }, textStyle: { color: textColor, fontSize: 10 },
        labelFormatter: (_, val) => labels[Math.round(val)] || '' },
      // inside zoom kept for click-drag panning, but wheel is disabled —
      // hovering over the NAV chart while scrolling the page was hijacking
      // the scroll and unexpectedly zooming the timeline.
      { type: 'inside', zoomOnMouseWheel: false, moveOnMouseWheel: false },
    ],
    xAxis: {
      type: 'category', data: labels,
      axisLine: { lineStyle: { color: gridColor } },
      axisLabel: { color: textColor, fontSize: 10 },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value',
      min: yZero ? 0 : 'dataMin',
      axisLine: { show: false },
      axisLabel: {
        color: textColor,
        fontSize: 10,
        formatter: v => Math.round(v).toLocaleString(),
      },
      splitLine: { lineStyle: { color: gridColor, width: 0.5 } },
    },
    tooltip: {
      trigger: 'axis',
      formatter(params) {
        let html = params[0] ? params[0].axisValueLabel : '';
        for (const p of params) {
          if (p.value == null || p.value === '-') continue;
          html += `<br/><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:4px;"></span>${p.seriesName}: ${Number(p.value).toFixed(2)}`;
        }
        return html;
      },
    },
    series: [
      {
        name: 'NAV',
        type: 'line',
        data: navValues.map(v => v === null ? '-' : v),
        smooth: 0.3,
        symbol: navValues.length > 30 ? 'none' : 'circle',
        symbolSize: navValues.length > 60 ? 0 : 4,
        lineStyle: { color: navColor, width: 2 },
        itemStyle: { color: navColor },
        areaStyle: !hasBench ? {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: _hexToRgba(navColor, 0.25) },
            { offset: 1, color: _hexToRgba(navColor, 0.0) },
          ]),
        } : undefined,
      },
      ...initBenchSeries,
    ],
  });

  // On dataZoom change: (a) re-scale benchmark series to the new window,
  // (b) refresh the CAGR card so it reflects the visible period.
  //
  // Previously this listener only ran when hasBench was true — but the CAGR
  // card should react to zoom regardless of whether benchmarks are on, so
  // the listener is now installed unconditionally and internally guards the
  // benchmark-rescale path.
  {
    let _zoomTimer = null;
    ec.on('datazoom', () => {
      clearTimeout(_zoomTimer);
      _zoomTimer = setTimeout(() => {
        const opt = ec.getOption();
        const dz = opt?.dataZoom?.[0];
        const startPct = dz?.start ?? 0;
        const endPct = dz?.end ?? 100;
        const last = Math.max(0, labels.length - 1);
        const startIdx = Math.max(0, Math.min(last, Math.round(startPct / 100 * last)));
        const endIdx = Math.max(0, Math.min(last, Math.round(endPct / 100 * last)));

        if (hasBench) {
          const newBench = buildBenchSeries(startIdx);
          // Update only benchmark series (index 1+)
          const seriesUpdate = [{ data: navValues.map(v => v === null ? '-' : v) }, ...newBench];
          ec.setOption({ series: seriesUpdate });
        }

        _updateNavCagrCard(data, startIdx, endIdx);
      }, 80);
    });
  }

  _navChartInstance = ec;

  // 폰에서 탭 전환 직후 container 높이가 늦게 확정되면 ECharts 가 0 으로
  // init 되어 차트가 보이지 않음. ResizeObserver 가 크기 확정 시점에
  // 발화해 자동 resize.
  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => {
      if (_navChartInstance) _navChartInstance.resize();
    });
    ro.observe(container);
    _navChartResizeObserver = ro;
  }
}

function onNavYZeroToggle() {
  if (_navChartInstance) {
    const yZero = document.getElementById('pfNavYZero')?.checked;
    _navChartInstance.setOption({ yAxis: { min: yZero ? 0 : 'dataMin' } });
  }
}

function onValueYZeroToggle() {
  if (_valueChartInstance) {
    const yZero = document.getElementById('pfValueYZero')?.checked;
    _valueChartInstance.setOption({ yAxis: { min: yZero ? 0 : 'dataMin' } });
  }
}

function _navZoomToDays(days) {
  if (!_navChartInstance || !_navChartData.length) return;
  const total = _navChartData.length;
  const startPct = Math.max(0, (1 - days / total) * 100);
  _navChartInstance.dispatchAction({ type: 'dataZoom', start: startPct, end: 100 });
}

let _valueChartInstance = null;

async function renderValueChart(data) {
  const container = document.getElementById('pfValueChart');
  if (!container) return;
  if (_valueChartInstance) { _valueChartInstance.dispose(); _valueChartInstance = null; }
  if (_valueChartResizeObserver) { _valueChartResizeObserver.disconnect(); _valueChartResizeObserver = null; }
  await loadChartLib();

  // 모바일 layout 확정 대기 — renderNavChart 와 동일 이유.
  await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));

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

  const valYZero = document.getElementById('pfValueYZero')?.checked;
  _valueChartInstance = createLineChart(container, {
    labels: data.map(d => d.date),
    values: fxValues.map(v => Math.round(v)),
    color: valColor,
    yMin: valYZero ? 0 : undefined,
    yFormatter: v => sym + (v / div).toFixed(pfCurrency === 'USD' ? 2 : 0) + unit,
    dataZoom: true,
  });

  // NAV 차트와 동일한 ResizeObserver — 폰 탭 전환 시 init 타이밍 보정.
  if (typeof ResizeObserver !== 'undefined' && _valueChartInstance) {
    const ro = new ResizeObserver(() => {
      if (_valueChartInstance && _valueChartInstance.resize) _valueChartInstance.resize();
    });
    ro.observe(container);
    _valueChartResizeObserver = ro;
  }

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
      // role='cagr' is the hook _updateValueCagrCard() latches onto when
      // the 평가금액 chart dataZoom moves.
      { label: 'CAGR', val: acctReturn !== null ? fmtPct(acctReturn) : '-', cls: returnClass(acctReturn), role: 'cagr' },
    ];
    statsEl.innerHTML = items.map(p => {
      const role = p.role ? ` data-role="${p.role}"` : '';
      return `<div class="pf-nav-ret-card"${role}><div class="pf-nav-ret-label">${p.label}</div><div class="pf-nav-ret-value ${p.cls || ''}">${p.val}</div></div>`;
    }).join('');
  }

  // On dataZoom change, refresh the CAGR card for the visible window. The
  // uPlot mobile path doesn't expose `on` — skip there (no dataZoom UI).
  if (_valueChartInstance && typeof _valueChartInstance.on === 'function') {
    let _zoomTimer = null;
    _valueChartInstance.on('datazoom', () => {
      clearTimeout(_zoomTimer);
      _zoomTimer = setTimeout(() => {
        const opt = _valueChartInstance.getOption();
        const dz = opt?.dataZoom?.[0];
        const startPct = dz?.start ?? 0;
        const endPct = dz?.end ?? 100;
        const last = Math.max(0, data.length - 1);
        const startIdx = Math.max(0, Math.min(last, Math.round(startPct / 100 * last)));
        const endIdx = Math.max(0, Math.min(last, Math.round(endPct / 100 * last)));
        _updateValueCagrCard(data, fxValues, startIdx, endIdx);
      }, 80);
    });
  }
}

// Update ONLY the CAGR card in #pfValueStats for the visible window
// [startIdx..endIdx]. Uses the closure-captured fxValues so the FX-adjusted
// math stays consistent with the snapshot renderValueChart computed at the
// top of the function.
function _updateValueCagrCard(data, fxValues, startIdx, endIdx) {
  const root = document.getElementById('pfValueStats');
  if (!root) return;
  const card = root.querySelector('[data-role="cagr"]');
  if (!card) return;
  const labelEl = card.querySelector('.pf-nav-ret-label');
  const valEl = card.querySelector('.pf-nav-ret-value');
  if (!labelEl || !valEl) return;

  const isFull = startIdx === 0 && endIdx === data.length - 1;
  labelEl.textContent = isFull ? 'CAGR' : 'CAGR (구간)';

  const first = fxValues[startIdx];
  const last = fxValues[endIdx];
  if (endIdx <= startIdx || first == null || last == null || !(first > 0)) {
    valEl.textContent = '-';
    valEl.className = 'pf-nav-ret-value';
    return;
  }
  const days = (new Date(data[endIdx].date) - new Date(data[startIdx].date)) / 86400000;
  const years = days / 365;
  if (!(years > 0)) {
    valEl.textContent = '-';
    valEl.className = 'pf-nav-ret-value';
    return;
  }
  const cagr = ((last - first) / first * 100) / years;
  valEl.textContent = fmtPct(cagr);
  valEl.className = 'pf-nav-ret-value ' + (returnClass(cagr) || '');
}

function renderNavReturns(data) {
  const el = document.getElementById('pfNavReturns');
  if (!el || !data.length) { if (el) el.innerHTML = ''; return; }

  const _nav = d => {
    if (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) return d.nav / d.fx_usdkrw;
    return d.nav;
  };

  const latest = data[data.length - 1];
  const latestNav = _nav(latest);
  const firstNav = _nav(data[0]);

  // Period returns helper
  const _periodPct = (days) => {
    if (data.length < 2) return null;
    const slice = data.slice(-days);
    if (!slice.length) return null;
    const base = _nav(slice[0]);
    return base > 0 ? ((latestNav / base) - 1) * 100 : null;
  };

  const pct7 = _periodPct(7);
  const pct30 = _periodPct(30);
  const pct90 = _periodPct(90);

  // 52-week range
  const last365 = data.slice(-365);
  const navs52 = last365.map(d => _nav(d));
  const min52 = Math.min(...navs52);
  const max52 = Math.max(...navs52);

  // YoY
  const oneYearAgo = last365.length >= 252 ? last365[0] : (last365.length > 0 ? last365[0] : null);
  const yoyPct = oneYearAgo ? ((latestNav / _nav(oneYearAgo)) - 1) * 100 : null;

  // CAGR
  const totalDays = data.length > 1 ? (new Date(latest.date) - new Date(data[0].date)) / 86400000 : 0;
  const totalYears = totalDays / 365;
  const annualizedPct = totalYears > 0
    ? ((latestNav - firstNav) / firstNav * 100) / totalYears : null;

  const items = [
    { label: '현재 NAV', val: latestNav.toFixed(2) },
    { label: '최근 7일', val: pct7 !== null ? fmtPct(pct7) : '-', cls: returnClass(pct7), days: 7 },
    { label: '최근 30일', val: pct30 !== null ? fmtPct(pct30) : '-', cls: returnClass(pct30), days: 30 },
    { label: '최근 90일', val: pct90 !== null ? fmtPct(pct90) : '-', cls: returnClass(pct90), days: 90 },
    { label: '52주 최저', val: min52.toFixed(2), days: 365 },
    { label: '52주 최고', val: max52.toFixed(2), days: 365 },
    { label: 'YoY', val: yoyPct !== null ? fmtPct(yoyPct) : '-', cls: returnClass(yoyPct), days: 365 },
    // role='cagr' lets _updateNavCagrCard() find this specific card when
    // the NAV chart's dataZoom changes, so the value reflects the visible
    // window instead of the full-history snapshot.
    { label: 'CAGR', val: annualizedPct !== null ? fmtPct(annualizedPct) : '-', cls: returnClass(annualizedPct), role: 'cagr' },
  ];
  el.innerHTML = items.map(p => {
    const zoomable = p.days ? ` js-pf-nav-zoom" data-zoom-days="${p.days}" style="cursor:pointer;` : '';
    const role = p.role ? ` data-role="${p.role}"` : '';
    return `<div class="pf-nav-ret-card${zoomable}"${role}><div class="pf-nav-ret-label">${p.label}</div><div class="pf-nav-ret-value ${p.cls || ''}">${p.val}</div></div>`;
  }).join('');
}

// Update ONLY the CAGR card in #pfNavReturns to reflect the visible window
// [startIdx..endIdx] on the NAV chart. Called on every debounced dataZoom
// event. When the full range is selected the label stays plain "CAGR";
// when zoomed in it becomes "CAGR (구간)" so the user knows the value is
// no longer full-history.
function _updateNavCagrCard(data, startIdx, endIdx) {
  const root = document.getElementById('pfNavReturns');
  if (!root) return;
  const card = root.querySelector('[data-role="cagr"]');
  if (!card) return;
  const labelEl = card.querySelector('.pf-nav-ret-label');
  const valEl = card.querySelector('.pf-nav-ret-value');
  if (!labelEl || !valEl) return;

  const isFull = startIdx === 0 && endIdx === data.length - 1;
  labelEl.textContent = isFull ? 'CAGR' : 'CAGR (구간)';

  if (endIdx <= startIdx || !data[startIdx] || !data[endIdx]) {
    valEl.textContent = '-';
    valEl.className = 'pf-nav-ret-value';
    return;
  }
  // Same FX-aware accessor renderNavReturns uses — keep the two in sync so
  // switching the display currency is reflected in the zoomed CAGR too.
  const _nav = d => (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) ? d.nav / d.fx_usdkrw : d.nav;
  const firstNav = _nav(data[startIdx]);
  const lastNav = _nav(data[endIdx]);
  const days = (new Date(data[endIdx].date) - new Date(data[startIdx].date)) / 86400000;
  const years = days / 365;
  if (!(years > 0) || !(firstNav > 0)) {
    valEl.textContent = '-';
    valEl.className = 'pf-nav-ret-value';
    return;
  }
  // Matches renderNavReturns' "simple annualized" formula (not compound
  // CAGR) — keeps the zoomed value numerically comparable to the initial
  // full-range value on the same card.
  const cagr = ((lastNav - firstNav) / firstNav * 100) / years;
  valEl.textContent = fmtPct(cagr);
  valEl.className = 'pf-nav-ret-value ' + (returnClass(cagr) || '');
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
    <td><button class="pf-row-btn delete js-pf-cf-delete" data-cf-id="${cf.id}">X</button></td>
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

// --- Delegated event handlers ---------------------------------------------
// All row-level actions used to live in inline `onclick="fn('${code}')"`,
// which interpolates user-controlled strings (stock codes/names) into a JS
// string context. escapeHtml is not safe there — `'` becomes `&#39;` which
// the HTML parser decodes back to `'` before the JS evaluates, allowing
// break-out. We replace those with CSS classes + data attributes and a
// single document-level delegated handler, which also removes the per-render
// listener churn and prevents accumulated `document.click` listeners from
// menu/picker code.
(function initPfDelegation() {
  const onReady = () => {
    document.addEventListener('click', (e) => {
      const t = e.target;
      const codeFromTr = (el) => {
        const host = el.closest('[data-code]');
        return host ? host.dataset.code : null;
      };
      let el;
      if ((el = t.closest('.js-pf-analyze'))) {
        const code = codeFromTr(el);
        if (code) { e.preventDefault(); pfGoAnalyze(code, e); }
      } else if ((el = t.closest('.js-pf-save'))) {
        const code = codeFromTr(el);
        if (code) savePortfolioEdit(code);
      } else if (t.closest('.js-pf-cancel')) {
        cancelPortfolioEdit();
      } else if ((el = t.closest('.js-pf-edit'))) {
        const code = codeFromTr(el);
        if (code) startPortfolioEdit(code);
      } else if ((el = t.closest('.js-pf-delete'))) {
        const code = codeFromTr(el);
        if (code) deletePortfolioItem(code);
      } else if ((el = t.closest('.js-pf-bench-picker'))) {
        const code = codeFromTr(el);
        if (code) pfShowBenchmarkPicker(code, el);
      } else if ((el = t.closest('.js-pf-bench-set'))) {
        const code = codeFromTr(el);
        if (code) pfSetBenchmark(code, el.dataset.bench || '');
      } else if ((el = t.closest('.js-pf-gold-gap'))) {
        const asset = el.dataset.gapAsset || _goldGapInfoForCode(codeFromTr(el)).asset;
        if (asset) _openGoldGapDashboard(asset);
      } else if ((el = t.closest('.js-pf-cf-delete'))) {
        const id = Number(el.dataset.cfId);
        if (!isNaN(id)) deleteCashflow(id);
      } else if ((el = t.closest('.js-pf-nav-zoom'))) {
        const days = Number(el.dataset.zoomDays);
        if (!isNaN(days)) _navZoomToDays(days);
      } else if ((el = t.closest('.js-pf-target-clear'))) {
        const code = codeFromTr(el);
        if (code) {
          e.preventDefault();
          e.stopPropagation();
          clearPortfolioTargetPrice(code);
        }
      }
    });
    document.addEventListener('change', (e) => {
      const t = e.target;
      let el;
      if ((el = t.closest('.js-pf-group'))) {
        const host = el.closest('[data-code]');
        if (host) pfChangeGroup(host.dataset.code, el.value);
      } else if ((el = t.closest('.js-pf-col-toggle'))) {
        pfToggleCol(el.dataset.colKey, el.checked);
      }
    });
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onReady);
  } else {
    onReady();
  }
})();
