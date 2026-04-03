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
let pfMonthEndStockValues = {}; // stock_code -> market_value at month end
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
    } catch (e) { console.warn(e); }
    // Restore benchmark names from localStorage cache for instant display
    try {
      const cached = JSON.parse(localStorage.getItem('pfBenchmarkNames') || '{}');
      for (const [k, v] of Object.entries(cached)) {
        pfBenchmarkQuotes[k] = { ...(pfBenchmarkQuotes[k] || {}), name: v };
      }
    } catch (e) { console.warn(e); }
    // Fetch benchmark quotes in background (don't block initial render)
    apiFetch('/api/portfolio/month-end-value').then(async r => {
      if (!r.ok) return;
      const snap = await r.json();
      pfMonthEndValue = snap.total_value ?? null;
      pfMonthEndStockValues = snap.stock_values || {};
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
        return `<button class="pf-filter-btn${active ? ' active' : ''}" onclick="pfToggleGroupFilter('${escapeHtml(g.group_name)}')">${escapeHtml(g.group_name)} <span class="pf-filter-cnt">${counts[g.group_name] || 0}</span></button>`;
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
  const isFiltered = pfGroupFilter !== null;
  let filteredMonthEndValue = pfMonthEndValue;
  if (isFiltered && Object.keys(pfMonthEndStockValues).length > 0) {
    filteredMonthEndValue = 0;
    rows.forEach(r => { filteredMonthEndValue += (pfMonthEndStockValues[r.stock_code] ?? 0); });
  }
  const monthlyReturnPct = filteredMonthEndValue && filteredMonthEndValue > 0
    ? ((totalMarketValue - filteredMonthEndValue) / filteredMonthEndValue * 100) : null;
  const monthlyPnl = filteredMonthEndValue != null && filteredMonthEndValue > 0
    ? totalMarketValue - filteredMonthEndValue : null;

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
      } catch (e) { console.warn(e); }
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
  } catch (e) { console.warn(e); }
}

let _navChartInstance = null;

function renderNavChart(data) {
  const canvas = document.getElementById('pfNavCanvas');
  if (!canvas) return;
  if (_navChartInstance) { _navChartInstance.destroy(); _navChartInstance = null; }

  if (!data.length) {
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary');
    ctx.font = '14px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('스냅샷 데이터가 없습니다.', canvas.width / 2, canvas.height / 2);
    return;
  }

  const labels = data.map(d => d.date.slice(5)); // MM-DD
  const navs = data.map(d => d.nav);
  const lineColor = getComputedStyle(document.documentElement).getPropertyValue('--primary').trim() || '#3b82f6';
  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#333';

  _navChartInstance = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: {
      labels,
      datasets: [{
        data: navs,
        borderColor: lineColor,
        borderWidth: 2,
        pointRadius: data.length > 30 ? 0 : 3,
        pointHoverRadius: 5,
        pointBackgroundColor: lineColor,
        tension: 0,
        fill: false,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => data[items[0].dataIndex]?.date || '',
            label: (item) => `NAV ${item.raw.toFixed(2)}`,
          },
        },
      },
      scales: {
        x: {
          ticks: { color: textColor, maxTicksLimit: 10, font: { size: 10 } },
          grid: { color: gridColor, lineWidth: 0.5 },
        },
        y: {
          ticks: { color: textColor, font: { size: 11 } },
          grid: { color: gridColor, lineWidth: 0.5 },
        },
      },
      interaction: { intersect: false, mode: 'index' },
    },
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
