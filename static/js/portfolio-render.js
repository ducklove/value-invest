// Portfolio holdings table rendering, formatting helpers, sparklines, benchmark labels.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
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
  const _curNavKrw = (_snapIsStale && latestSnap.total_units && grandTotalMarketValue > 0)
    ? grandTotalMarketValue / latestSnap.total_units
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
  // Filtered TODAY should use the same 22:00 snapshot baseline as the whole
  // portfolio. Falling back to quote "previous close" makes yesterday's KRX
  // daily move leak into the next pre-open session, even though the 22:00
  // snapshot has already absorbed it. Only use quote math when per-stock
  // snapshot data is unavailable for the filtered reference date.
  if (isFiltered && dailyNavPct === null && totalMarketValue > 0) {
    const prevMV = totalMarketValue - totalDailyPnl;
    if (prevMV > 0) {
      dailyNavPct = (totalDailyPnl / prevMV) * 100;
      totalDailyPnlDisplay = _fxConv(totalDailyPnl, null);
    }
  }
  // Table footer "등락률" is a quote-session aggregate of the visible rows.
  // It intentionally differs from the TODAY card, which compares against the
  // portfolio's 22:00 NAV snapshot.
  const tableDailyBaseValue = totalMarketValue - totalDailyPnl;
  const dailyReturnPct = tableDailyBaseValue > 0 ? (totalDailyPnl / tableDailyBaseValue * 100) : null;

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
  // Filtered views also use this 22:00 baseline when per-stock snapshots are
  // available, so pre-open domestic holdings do not carry over yesterday's
  // quote change as "today".
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
      <div class="pf-summary-side">
        <div class="pf-summary-label">NAV</div>
        <div class="pf-summary-side-value">${_l && curNav != null ? Number(curNav).toFixed(2) : '-'}</div>
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
  const _fp = pfFmtPortfolioValue;
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

  const canManualDrag = !pfSortKey && !pfGroupSort && currentUser && !pfEditingCode;

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
    const tagHtml = _renderPortfolioRowTags(pfGetTags(r));
    const dragHandle = canManualDrag
      ? '<button type="button" class="pf-row-drag-handle js-pf-row-drag" draggable="true" title="드래그하여 순서 변경" aria-label="드래그하여 순서 변경">&#x2630;</button>'
      : '';
    if (isEditing) {
      return `<tr data-code="${safeCode}">
        <td class="pf-stock-cell js-pf-analyze"><a href="#" class="pf-stock-link"><strong>${escapeHtml(r.stock_name)}</strong></a> <span class="pf-stock-code">${safeCode}</span>${curTag}${liveDotE}${tagHtml}</td>
        <td class="pf-col-group"><select class="pf-group-select js-pf-group">${groupOpts}</select></td>
        <td class="pf-col-num pf-col-changepct">${fmtChangePct(r.changePct, r.change)}</td>
        <td class="pf-col-num pf-col-curprice">${r.price !== null ? _fp(r.price) : '-'}</td>
        <td class="pf-col-num pf-col-benchmark">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
        <td class="pf-col-num pf-col-invested">${r.tradingValue !== null ? fmtKrw(r.tradingValue) : '-'}</td>
        <td class="pf-col-num pf-col-buyprice"><input class="pf-edit-input" id="pfEditPrice" value="${r.avgPrice}" type="number" step="1"></td>
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
    return `<tr data-code="${safeCode}">
      <td class="pf-stock-cell js-pf-analyze">${dragHandle}<a href="#" class="pf-stock-link"><strong>${escapeHtml(r.stock_name)}</strong></a> <span class="pf-stock-code">${safeCode}</span>${curTag}${liveDotE}${tagHtml}</td>
      <td class="pf-col-group"><select class="pf-group-select js-pf-group">${groupOpts}</select></td>
      <td class="pf-col-num pf-col-changepct">${fmtChangePct(r.changePct, r.change)}</td>
      <td class="pf-col-num pf-col-curprice">${r.price !== null ? _fp(r.price) : '-'}</td>
      <td class="pf-col-num pf-col-benchmark js-pf-bench-picker">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
      <td class="pf-col-num pf-col-invested">${r.tradingValue !== null ? fmtKrw(r.tradingValue) : '-'}</td>
      <td class="pf-col-num pf-col-buyprice">${_fp(r.avgPrice)}</td>
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

  // Drag-and-drop uses an explicit handle so normal stock-name clicks never
  // get swallowed by the browser's native row-drag gesture.
  if (canManualDrag) {
    tbody.querySelectorAll('.js-pf-row-drag').forEach(handle => {
      handle.addEventListener('dragstart', (e) => {
        const tr = handle.closest('tr[data-code]');
        if (!tr) return;
        tr.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', tr.dataset.code);
      });
      handle.addEventListener('dragend', () => {
        const tr = handle.closest('tr[data-code]');
        if (!tr) return;
        tr.classList.remove('dragging');
        tbody.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
      });
    });
    tbody.querySelectorAll('tr[data-code]').forEach(tr => {
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

function _drawSparklinePoints(canvasId, points, color, xMax) {
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

  const clean = (points || [])
    .map(p => ({ x: Number(p.x), y: Number(p.y) }))
    .filter(p => Number.isFinite(p.x) && Number.isFinite(p.y))
    .sort((a, b) => a.x - b.x);
  const ys = clean.map(p => p.y);
  const min = ys.length ? Math.min(...ys) : 0;
  const max = ys.length ? Math.max(...ys) : 0;
  const pad = 2;
  const minZ = Math.min(min, 0);
  const maxZ = Math.max(max, 0);
  const rangeZ = maxZ - minZ || 1;
  const axisMax = xMax || Math.max(clean[clean.length - 1]?.x || 1, 1);
  const yFor = (v) => pad + (1 - (v - minZ) / rangeZ) * (h - pad * 2);
  const xFor = (x) => (Math.max(0, Math.min(axisMax, x)) / axisMax) * w;
  const zeroY = yFor(0);

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

  if (clean.length > 1) {
    ctx.beginPath();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    clean.forEach((p, i) => {
      const x = xFor(p.x);
      const y = yFor(p.y);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }
}

function _sparkHourFromTs(ts) {
  const m = String(ts || '').match(/T(\d{2}):(\d{2})/);
  if (!m) return null;
  return Number(m[1]) + Number(m[2]) / 60;
}

function _formatLocalYmd(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function _parseLocalYmd(ymd) {
  const m = String(ymd || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
}

function _diffLocalDays(start, end) {
  return Math.round((end.getTime() - start.getTime()) / 86400000);
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

  // MTD sparkline: fixed previous-month-end -> current-month-end axis.
  // The first point is always previous month-end at 0%, so the shape is
  // stable even before the first daily snapshot of the month exists.
  if (pfMonthEndSnap?.total_value && pfMonthEndSnap?.total_value > 0) {
    const now = new Date();
    const prevMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0);
    const thisMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const thisMonthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0);
    const monthStartYmd = _formatLocalYmd(thisMonthStart);
    const monthEndYmd = _formatLocalYmd(thisMonthEnd);
    const axisDays = Math.max(1, _diffLocalDays(prevMonthEnd, thisMonthEnd));
    const monthPoints = [{ x: 0, y: 0 }];
    const monthData = pfNavHistory.filter(d => d.date >= monthStartYmd && d.date <= monthEndYmd);
    for (const d of monthData) {
      if (!d || !d.total_value) continue;
      const dt = _parseLocalYmd(d.date);
      if (!dt) continue;
      monthPoints.push({
        x: Math.max(0, Math.min(axisDays, _diffLocalDays(prevMonthEnd, dt))),
        y: ((d.total_value / pfMonthEndSnap.total_value) - 1) * 100,
      });
    }
    if (currentTotalValue) {
      monthPoints.push({
        x: Math.max(0, Math.min(axisDays, _diffLocalDays(prevMonthEnd, now))),
        y: ((currentTotalValue / pfMonthEndSnap.total_value) - 1) * 100,
      });
    }
    const lastPct = monthPoints.length ? monthPoints[monthPoints.length - 1].y : 0;
    _drawSparklinePoints('sparkMonthly', monthPoints, lastPct >= 0 ? '#dc2626' : '#2563eb', axisDays);
  } else {
    _drawSparklinePoints('sparkMonthly', [], '#dc2626', 31);
  }

  // 기준값 = 전일 22:00 결산값 (pfPrevDaySnapshot.total_value). sparkline
  // 은 전일 22:00 → 당일 22:00의 24시간 축으로 그린다. 첫 점은 항상
  // 전일 22:00 기준 0%로 고정한다.
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
    _drawSparklinePoints('sparkDaily', [], '#dc2626', 24);
  } else {
    const raw = [{ x: 0, y: 0 }];
    for (const d of pfIntradayData) {
      if (!d || !d.total_value) continue;
      const hour = _sparkHourFromTs(d.ts);
      if (hour === null) continue;
      const x = String(d.ts || '').endsWith('T00:00')
        ? 0
        : Math.max(0, Math.min(24, hour + 2));
      raw.push({ x, y: (d.total_value / _prevClose - 1) * 100 });
    }
    if (currentTotalValue) {
      const now = new Date();
      const currentHour = now.getHours() + now.getMinutes() / 60;
      raw.push({ x: Math.min(24, currentHour + 2), y: (currentTotalValue / _prevClose - 1) * 100 });
    }
    // Outlier 필터: 현재값 부호와 반대 부호인 점은 일시적 데이터 오류
    // (일부 종목 가격 fetch 실패로 total_value 잘못 저장) 로 간주해 제외.
    // 포트폴리오 전체가 30 분 사이 부호를 뒤집을 정도로 움직이는 건
    // 극히 드묾 — 반대 부호 점 하나가 있다면 데이터 문제일 가능성이
    // 훨씬 큼. 사용자가 '오늘 항상 + 였다' 고 확신하는 케이스와 일치.
    const lastRaw = raw.length ? raw[raw.length - 1].y : 0;
    const dayPcts = lastRaw >= 0
      ? raw.filter(p => p.y >= 0)
      : raw.filter(p => p.y <= 0);
    const lastPct = dayPcts.length ? dayPcts[dayPcts.length - 1].y : 0;
    _drawSparklinePoints('sparkDaily', dayPcts, lastPct >= 0 ? '#dc2626' : '#2563eb', 24);
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

function pfFmtPortfolioValue(value) {
  const converted = pfFx(value);
  return pfCurrency === 'USD'
    ? '$' + Number(converted).toLocaleString(undefined, { minimumFractionDigits: 3, maximumFractionDigits: 3 })
    : fmtNum(Math.round(converted));
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
