// Portfolio holdings table rendering, formatting helpers, sparklines, benchmark labels.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
const PF_AVG_PRICE_CURRENCIES = ['KRW', 'USD', 'EUR', 'JPY', 'CNY', 'HKD', 'GBP', 'AUD', 'CAD', 'CHF', 'TWD', 'VND'];

function pfAvgPriceCurrency(item) {
  const currency = String(item?.avg_price_currency || 'KRW').trim().toUpperCase();
  return PF_AVG_PRICE_CURRENCIES.includes(currency) ? currency : 'KRW';
}

function pfAvgPriceKrw(item) {
  const converted = Number(item?.avg_price_krw);
  if (Number.isFinite(converted)) return converted;
  const raw = Number(item?.avg_price || 0);
  return Number.isFinite(raw) ? raw : 0;
}

function pfCanEditAvgPriceCurrency(stockCode) {
  const code = String(stockCode || '').toUpperCase();
  if (!code || code.startsWith('CASH_')) return false;
  if (['KRX_GOLD', 'CRYPTO_BTC', 'CRYPTO_ETH', 'CRYPTO_USDT'].includes(code)) return false;
  return !/^[0-9][0-9A-Z]{5}$/.test(code);
}

function pfAvgPriceCurrencyOptions(selected) {
  const current = pfAvgPriceCurrency({ avg_price_currency: selected });
  return PF_AVG_PRICE_CURRENCIES.map(currency =>
    `<option value="${currency}"${currency === current ? ' selected' : ''}>${currency}</option>`
  ).join('');
}

function pfFmtNativeAvgPrice(value, currency) {
  const n = Number(value || 0);
  const digits = currency === 'KRW' ? 0 : (Math.abs(n) >= 100 ? 2 : 4);
  return Number.isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: digits }) : '-';
}

function pfFmtAvgPriceCell(row, formatter = pfFmtPortfolioValue) {
  const main = formatter(row.avgPriceKrw);
  if (row.avgPriceCurrency === 'KRW') return main;
  return `${main}<span class="pf-native-price">${escapeHtml(row.avgPriceCurrency)} ${escapeHtml(pfFmtNativeAvgPrice(row.avgPrice, row.avgPriceCurrency))}</span>`;
}

function renderPortfolio(options = {}) {
  const summaryOnly = !!(options && options.summaryOnly);
  _pfRenderColToggles();
  const tbody = document.getElementById('pfBody');
  const tfoot = document.getElementById('pfFoot');
  const summary = document.getElementById('pfSummary');
  const table = document.getElementById('pfTable');
  const empty = document.getElementById('pfEmpty');

  // Show/hide filter bar and update counts
  const filterBar = document.getElementById('pfFilterBar');
  if (filterBar && !summaryOnly) {
    filterBar.style.display = PfStore.items.length ? 'flex' : 'none';
    if (PfStore.items.length && PfStore.groups.length) {
      const counts = {};
      PfStore.groups.forEach(g => counts[g.group_name] = 0);
      PfStore.items.forEach(i => {
        const gn = pfGetGroup(i);
        if (counts[gn] !== undefined) counts[gn]++;
        else counts[gn] = 1;
      });
      // Build a fingerprint to avoid unnecessary DOM rebuilds (prevents click loss during rAF re-renders)
      const fingerprint = PfStore.groups.map(g => {
        const active = PfStore.filters.group === null || PfStore.filters.group.has(g.group_name);
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
        PfStore.groups.forEach(g => {
          const active = PfStore.filters.group === null || PfStore.filters.group.has(g.group_name);
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

  if (!PfStore.items.length) {
    if (!summaryOnly) {
      table.style.display = 'none';
      empty.style.display = 'block';
      empty.textContent = '포트폴리오가 비어 있습니다. 위 검색창에서 종목을 추가하세요.';
    }
    summary.innerHTML = '';
    return;
  }

  const allRows = PfStore.items.map(item => {
    const q = item.quote || {};
    const cur = item.currency || 'KRW';
    const price = quotePriceOrNull(q);
    const change = price !== null ? (q.change ?? 0) : 0;
    const changePct = price !== null ? (q.change_pct ?? null) : null;
    const qty = item.quantity;
    const avgPrice = Number(item.avg_price || 0);
    const avgPriceCurrency = pfAvgPriceCurrency(item);
    const avgPriceKrw = pfAvgPriceKrw(item);
    const invested = qty * avgPriceKrw;
    const marketValue = price !== null ? qty * price : null;
    const rawReturn = avgPriceKrw > 0 && price !== null ? ((price - avgPriceKrw) / avgPriceKrw * 100) : null;
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
    const tradingValue = (price !== null && q.trade_value !== undefined && q.trade_value !== null)
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
      ...item, cur, price, change, changePct, qty, avgPrice, avgPriceCurrency, avgPriceKrw,
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
  if (!summaryOnly) {
    const _externalCodesNeeded = new Set();
    const _portCodes = new Set(allRows.map(r => r.stock_code));
    for (const r of allRows) {
      // 우선주: 보통주 quote 가 포트폴리오에 없으면 외부 fetch 필요
      if ((r.target_price == null || _targetFormulaUses(r, '본주가격')) && _isPreferredStock(r.stock_code)) {
        const commonCode = r.stock_code.slice(0, -1) + '0';
        if (!_portCodes.has(commonCode)) _externalCodesNeeded.add(commonCode);
      }
      // 지주사: 자회사 quote 들
      const meta = _HOLDING_META[r.stock_code];
      if (meta && (r.target_price == null || _targetFormulaUses(r, '보유지분'))) {
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
  }

  // Check if all quotes are loaded
  const allQuotesLoaded = allRows.every(r => r.price !== null);

  // Total market value across ALL items (for weight calculation)
  let grandTotalMarketValue = 0;
  allRows.forEach(r => { if (r.marketValue !== null) grandTotalMarketValue += r.marketValue; });

  // Apply group + text filters. The text search is deliberately local and
  // instant: it filters the already-loaded holdings by name, code, group, tag.
  const groupRows = PfStore.filters.group === null ? allRows : allRows.filter(r => PfStore.filters.group.has(pfGetGroup(r)));
  const searchText = String(PfStore.filters.searchText || '').trim();
  const rows = searchText ? groupRows.filter(r => pfRowMatchesSearch(r, searchText)) : groupRows;
  const searchMeta = document.getElementById('pfSearchMeta');
  if (searchMeta && !summaryOnly) {
    searchMeta.textContent = searchText ? `검색 결과 ${rows.length.toLocaleString()} / ${groupRows.length.toLocaleString()}` : '';
  }

  if (!rows.length) {
    if (!summaryOnly) {
      table.style.display = 'none';
      empty.style.display = 'block';
      empty.textContent = searchText ? '검색 결과가 없습니다.' : '해당 분류의 종목이 없습니다.';
    }
    summary.innerHTML = '';
    return;
  }
  if (!summaryOnly) {
    table.style.display = 'table';
    empty.style.display = 'none';
  }

  let totalInvested = 0, totalMarketValue = 0, totalDailyPnl = 0, totalDividend = 0;
  rows.forEach(r => {
    totalInvested += r.invested;
    if (r.marketValue !== null) totalMarketValue += r.marketValue;
    totalDailyPnl += r.dailyPnl;
    if (r.dividendAmount !== null) totalDividend += r.dividendAmount;
  });

  // Sort rows: group sort (primary, if on) + column sort (secondary)
  if (!summaryOnly && (PfStore.sort.groupSort || PfStore.sort.key)) {
    const grpOrder = {};
    if (PfStore.sort.groupSort) PfStore.groups.forEach((g, i) => grpOrder[g.group_name] = i);
    rows.sort((a, b) => {
      // Primary: group sort
      if (PfStore.sort.groupSort) {
        const ga = grpOrder[pfGetGroup(a)] ?? 999;
        const gb = grpOrder[pfGetGroup(b)] ?? 999;
        if (ga !== gb) return ga - gb;
      }
      // Secondary: column sort. null/NaN/빈값은 방향 무관 항상 맨 아래.
      // 이전엔 `?? -Infinity` 로 치환해서 desc 에선 적절히 뒤로 갔지만
      // asc 에선 맨 앞에 몰려 "정렬 안된 것처럼" 보이는 버그가 있었다.
      if (PfStore.sort.key) {
        if (PfStore.sort.key === 'name') {
          const va = a.stock_name || '';
          const vb = b.stock_name || '';
          if (!va && vb) return 1;
          if (va && !vb) return -1;
          return PfStore.sort.asc ? va.localeCompare(vb) : vb.localeCompare(va);
        }
        if (PfStore.sort.key === 'createdAtSort') {
          const va = a.createdAtSort || '';
          const vb = b.createdAtSort || '';
          if (!va && vb) return 1;
          if (va && !vb) return -1;
          return PfStore.sort.asc ? va.localeCompare(vb) : vb.localeCompare(va);
        }
        const aVal = a[PfStore.sort.key];
        const bVal = b[PfStore.sort.key];
        const aMissing = aVal == null || Number.isNaN(aVal);
        const bMissing = bVal == null || Number.isNaN(bVal);
        if (aMissing && bMissing) return 0;
        if (aMissing) return 1;
        if (bMissing) return -1;
        return PfStore.sort.asc ? aVal - bVal : bVal - aVal;
      }
      return 0;
    });
  }
  if (!summaryOnly && PfStore.edit.code) {
    const idx = rows.findIndex(r => r.stock_code === PfStore.edit.code);
    if (idx > 0) {
      const [editing] = rows.splice(idx, 1);
      rows.unshift(editing);
    }
  }

  // Update sort arrows in header
  if (!summaryOnly) {
    document.querySelectorAll('.pf-sortable').forEach(th => {
      const key = th.dataset.sort;
      const existing = th.querySelector('.pf-sort-arrow');
      if (existing) existing.remove();
      const isActive = key === 'group' ? PfStore.sort.groupSort : PfStore.sort.key === key;
      if (isActive) {
        const arrow = document.createElement('span');
        arrow.className = 'pf-sort-arrow';
        arrow.textContent = key === 'group' ? ' \u25BC' : (PfStore.sort.asc ? ' \u25B2' : ' \u25BC');
        th.appendChild(arrow);
      }
    });
  }

  // FX helper: convert KRW value using a specific snapshot's FX rate, or current rate
  const _isUsd = PfStore.currency.unit === 'USD' && PfStore.currency.fxRate && PfStore.currency.fxRate > 0;
  const _fxConv = (krwVal, snap) => {
    if (!_isUsd) return krwVal;
    const rate = snap && snap.fx_usdkrw ? snap.fx_usdkrw : PfStore.currency.fxRate;
    return rate && rate > 0 ? krwVal / rate : krwVal;
  };
  const _currentFxVal = _fxConv(totalMarketValue, null); // today uses current rate
  const _currentFxInvested = _fxConv(totalInvested, null);

  const totalReturnPct = _currentFxInvested !== 0 ? ((_currentFxVal - _currentFxInvested) / Math.abs(_currentFxInvested) * 100) : 0;

  // NAV adjusted for currency mode: USD NAV = KRW NAV / FX
  const isFiltered = PfStore.filters.group !== null || Boolean(searchText);
  const _navAdj = (nav, fx) => {
    if (!_isUsd || !nav) return nav;
    const rate = fx && fx > 0 ? fx : PfStore.currency.fxRate;
    return rate && rate > 0 ? nav / rate : nav;
  };

  // FX-adjusted snap value helper
  const _snapToFxVal = (snap, field = 'total_value') => {
    if (!snap || !snap[field]) return null;
    if (_isUsd && snap.fx_usdkrw && snap.fx_usdkrw > 0) return snap[field] / snap.fx_usdkrw;
    return snap[field];
  };

  // Historical baseline for the current filter. For filtered cards, use the
  // exact per-stock 20:00 snapshot sum for the visible rows, not
  // `whole snapshot total × group ratio`. The latter mixes cash/other assets
  // into the selected group and makes TODAY drift away from the settlement
  // baseline.
  const _periodBaseValue = (snap) => {
    if (!snap) return null;
    if (!isFiltered) return _snapToFxVal(snap);
    const sv = snap.stock_values || {};
    if (!Object.keys(sv).length) return null;
    let filteredTotal = 0;
    for (const r of rows) {
      if (!Object.prototype.hasOwnProperty.call(sv, r.stock_code)) return null;
      filteredTotal += Number(sv[r.stock_code] || 0);
    }
    return filteredTotal > 0 ? _fxConv(filteredTotal, snap) : null;
  };

  const _periodCashflowValue = (snap) => {
    if (!snap || !isFiltered) return 0;
    const byStock = snap.today_cashflows_by_stock || {};
    let total = 0;
    for (const r of rows) total += Number(byStock[r.stock_code] || 0);
    return total ? _fxConv(total, null) : 0;
  };

  // Helper: compute return % and PnL for a period, group-aware
  const _periodReturn = (snap, navField) => {
    if (!snap) return { pct: null, pnl: null };
    const baseVal = _periodBaseValue(snap);
    if (isFiltered) {
      // Under a filter, if exact historical per-stock data is unavailable for
      // this reference date, return null so the card renders "-" instead
      // of falling through to whole-portfolio NAV (which would misleadingly
      // show the entire portfolio's return for the filtered view).
      if (baseVal === null) return { pct: null, pnl: null };
      const pnl = (_currentFxVal - _periodCashflowValue(snap)) - baseVal;
      const pct = baseVal > 0 ? (pnl / baseVal * 100) : null;
      return { pct, pnl };
    }
    // Whole portfolio: NAV-based % + total value PnL
    const nav = _navAdj(snap[navField || 'nav'], snap.fx_usdkrw);
    const pct = nav && curNav ? ((curNav / nav - 1) * 100) : null;
    const pnl = baseVal != null ? _currentFxVal - baseVal : null;
    return { pct, pnl };
  };

  // --- Compute current NAV ---
  const latestSnap = PfStore.navHistory.length ? PfStore.navHistory[PfStore.navHistory.length - 1] : null;
  let _pendingUnitsChange = 0;
  let _pendingCashflowWithoutUnits = 0;
  if (!isFiltered && Array.isArray(PfStore.snapshots.prevDay?.today_cashflows)) {
    for (const cf of PfStore.snapshots.prevDay.today_cashflows) {
      const units = Number(cf?.units_change);
      if (Number.isFinite(units)) _pendingUnitsChange += units;
      else _pendingCashflowWithoutUnits += Number(cf?.signed_amount || 0);
    }
  } else if (!isFiltered && PfStore.snapshots.prevDay?.today_net_cashflow) {
    _pendingCashflowWithoutUnits = Number(PfStore.snapshots.prevDay.today_net_cashflow || 0);
  }
  const _liveNavUnits = latestSnap && latestSnap.total_units
    ? Number(latestSnap.total_units) + _pendingUnitsChange
    : null;
  const _liveNavValueKrw = grandTotalMarketValue - _pendingCashflowWithoutUnits;
  const _curNavKrw = (_liveNavUnits && _liveNavUnits > 0 && _liveNavValueKrw > 0)
    ? _liveNavValueKrw / _liveNavUnits
    : (latestSnap ? latestSnap.nav : null);
  const curNav = _navAdj(_curNavKrw, PfStore.currency.fxRate);

  // --- Daily return ---
  const _dailyBaseValue = _periodBaseValue(PfStore.snapshots.prevDay);
  const _daily = _periodReturn(PfStore.snapshots.prevDay, 'nav');
  let dailyNavPct = _daily.pct;
  let totalDailyPnlDisplay = _daily.pnl ?? 0;
  // Subtract cashflow for daily (whole portfolio only)
  if (!isFiltered && PfStore.snapshots.prevDay && PfStore.snapshots.prevDay.today_net_cashflow) {
    totalDailyPnlDisplay -= _fxConv(PfStore.snapshots.prevDay.today_net_cashflow, null);
  }
  if (!isFiltered && _dailyBaseValue && _dailyBaseValue > 0) {
    // Keep the headline % and amount on the same 20:00 settlement basis.
    // After the 20:00 snapshot, latestSnap.nav equals the baseline NAV, so a
    // pure snapshot NAV comparison stays at 0.00% even while live ticks move
    // the amount. Use the cashflow-adjusted live PnL over the same base value.
    dailyNavPct = totalDailyPnlDisplay / _dailyBaseValue * 100;
  }
  // Table footer is quote-session math; TODAY stays on the 20:00 NAV snapshot.
  const tableDailyBaseValue = totalMarketValue - totalDailyPnl;
  const dailyReturnPct = tableDailyBaseValue > 0 ? (totalDailyPnl / tableDailyBaseValue * 100) : null;

  // --- Monthly return (MTD) ---
  const _mtd = _periodReturn(PfStore.snapshots.monthEnd, 'nav');
  const monthlyNavPct = _mtd.pct;
  const _mtdPnl = _mtd.pnl;
  const monthlyReturnPct = monthlyNavPct;

  // --- YTD return ---
  const yearStartSnap = PfStore.snapshots.yearStart || null;
  const _ytd = _periodReturn(yearStartSnap, 'nav');
  const ytdReturnPct = _ytd.pct;
  const _ytdPnl = _ytd.pnl;

  // Date labels for summary cards
  const _now = new Date();
  const _timeLabel = `${String(_now.getHours()).padStart(2,'0')}:${String(_now.getMinutes()).padStart(2,'0')}`;
  // Today compares against the previous 20:00 KST settlement snapshot.
  const _todayBaseDate = PfStore.snapshots.prevDay && PfStore.snapshots.prevDay.date;
  // Slice YYYY-MM-DD directly to avoid timezone-off-by-one browser parsing.
  const _todayLabel = _todayBaseDate
    ? `${_todayBaseDate.slice(5, 7)}/${_todayBaseDate.slice(8, 10)} 20시 정산 기준`
    : '기준 없음';
  const _mtdLabel = `${_now.getFullYear()}/${String(_now.getMonth()+1).padStart(2,'0')}`;
  const _ytdLabel = `${_now.getFullYear()}`;

  // Summary cards: Total, Today, MTD, YTD — show '-' until all quotes loaded
  const _l = allQuotesLoaded;
  const _loadingCount = allRows.filter(r => r.price === null).length;
  const _loadingSub = !_l ? `<span style="opacity:0.5">시세 로딩 중 (${allRows.length - _loadingCount}/${allRows.length})</span>` : '';
  // Format helpers — values passed here are already FX-converted
  const _fmtUsdVal = v => '$' + Number(v).toLocaleString(undefined, {minimumFractionDigits: 3, maximumFractionDigits: 3});
  const _fv = v => PfStore.currency.unit === 'USD' ? _fmtUsdVal(v) : fmtKrw(v);
  const _fsv = v => {
    if (PfStore.currency.unit === 'USD') {
      const abs = Math.round(Math.abs(v));
      const sign = v > 0 ? '+' : v < 0 ? '-' : '';
      return sign + '$' + abs.toLocaleString();
    }
    return fmtSignedKrw(v);
  };
  // Today's daily PnL in selected currency
  const _dailyPnlFx = PfStore.currency.unit === 'USD' ? pfFx(totalDailyPnl) : totalDailyPnl;

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
  _renderSummarySparklines(_l ? _liveNavValueKrw : null);
  if (summaryOnly) return;

  // Table body — apply FX conversion to price columns
  const _fp = pfFmtPortfolioValue;
  // 편집 중 input 의 사용자 입력(값/포커스/커서)을 re-render 전후로 보존.
  // tbody.innerHTML 재할당은 모든 <input> 을 재생성하므로, QuoteManager
  // WebSocket tick 이나 benchmark polling 이 돌 때마다 편집 중인 값이
  // DB 값으로 덮어써지는 문제가 있었다. 목표가 × 버튼으로 input 을
  // 비워도 즉시 '자동 계산 값' 으로 복원되던 증상이 대표적.
  // 따라서 DOM 교체 전에 현재 값을 snapshot 하고, 교체 후에 복원한다.
  const _editInputIds = ['pfEditPrice', 'pfEditPriceCurrency', 'pfEditTarget', 'pfEditQty', 'pfEditCreatedAt'];
  const _preservedEdit = PfStore.edit.code ? {} : null;
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

  const canManualDrag = PfStore.filters.group === null && !PfStore.sort.key && !PfStore.sort.groupSort && !searchText && currentUser && !PfStore.edit.code;

  tbody.innerHTML = rows.map((r, i) => {
    const weight = grandTotalMarketValue > 0 && r.marketValue !== null ? (r.marketValue / grandTotalMarketValue * 100) : 0;
    const isEditing = PfStore.edit.code === r.stock_code;
    const isCash = r.stock_code.startsWith('CASH_');
    const isSpecialFloat = ['KRX_GOLD', 'CRYPTO_BTC', 'CRYPTO_ETH', 'CRYPTO_USDT'].includes(r.stock_code) || isCash;
    const curTag = r.stock_code === 'KRX_GOLD' ? '<span class="pf-stock-code">원/g</span>' : r.cur !== 'KRW' ? `<span class="pf-stock-code">${r.cur}</span>` : '';
    const qtyStep = isSpecialFloat ? 'any' : '1';
    const qtyDecimals = r.stock_code === 'KRX_GOLD' ? 2 : isCash ? 2 : 8;
    const fmtQty = isSpecialFloat ? (v => v !== null && v !== undefined ? Number(v).toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: qtyDecimals}) : '-') : fmtNum;
    const groupOpts = PfStore.groups.map(g => `<option value="${escapeHtml(g.group_name)}"${g.group_name === pfGetGroup(r) ? ' selected' : ''}>${escapeHtml(g.group_name)}</option>`).join('');
    const targetInputValue = r.target_price_formula ?? (r.target_price ?? '');
    const targetTooltip = _targetPriceTooltip(r);
    const targetTitle = targetTooltip ? ` title="${escapeHtml(targetTooltip)}"` : '';
    const targetHelp = '숫자 또는 수식. 사용 가능: BPS, EPS, DPS, 보유지분, 본주가격, 매입가. 예: BPS*0.4+DPS*10. 기존 목표가를 비우면 -로 고정됩니다.';

    const liveDotE = QuoteManager.isLive(r.stock_code) ? '<span class="ws-live-dot" title="실시간"></span>' : '';
    const safeCode = escapeHtml(r.stock_code);
    const tagHtml = _renderPortfolioRowTags(pfGetTags(r));
    const groupHtml = _renderPortfolioRowGroup(r);
    const isSaving = PfStore.edit.savingCode === r.stock_code;
    const editAttrs = isSaving ? ' disabled' : '';
    const saveAttrs = isSaving ? ' disabled aria-busy="true"' : '';
    const rowClass = isSaving ? ' class="pf-row-saving" aria-busy="true"' : '';
    const canEditAvgPriceCurrency = pfCanEditAvgPriceCurrency(r.stock_code);
    const avgPriceCurrencyControl = canEditAvgPriceCurrency
      ? `<select class="pf-price-currency-select js-pf-edit-price-currency" id="pfEditPriceCurrency"${editAttrs}>${pfAvgPriceCurrencyOptions(r.avgPriceCurrency)}</select>`
      : `<span class="pf-price-currency-lock">${escapeHtml(r.avgPriceCurrency)}</span>`;
    const saveContent = isSaving
      ? '<span class="pf-save-spinner" aria-hidden="true"></span><span class="pf-save-label">저장중</span>'
      : '✓';
    const dragHandle = canManualDrag
      ? '<button type="button" class="pf-row-drag-handle js-pf-row-drag" draggable="true" title="드래그하여 순서 변경" aria-label="드래그하여 순서 변경">&#x2630;</button>'
      : '';
    const safeName = escapeHtml(r.stock_name);
    const stockIdentity = `<span class="pf-stock-main"><span class="pf-stock-line"><a href="#" class="pf-stock-link js-pf-open-insight" title="${safeName}"><strong>${safeName}</strong></a><span class="pf-stock-code">${safeCode}</span>${curTag}${liveDotE}</span>${tagHtml}</span>`;
    const stockEditIdentity = `<span class="pf-stock-main pf-stock-edit-main"><input class="pf-edit-input pf-stock-name-edit js-pf-edit-name" id="pfEditName" value="${safeName}" type="text" maxlength="80" autocomplete="off"${editAttrs}><span class="pf-stock-line"><span class="pf-stock-code">${safeCode}</span>${curTag}${liveDotE}</span>${tagHtml}</span>`;
    const stockCellClass = canManualDrag ? 'pf-stock-cell pf-stock-cell-with-drag js-pf-analyze' : 'pf-stock-cell js-pf-analyze';
    if (isEditing) {
      return `<tr data-code="${safeCode}"${rowClass}>
        <td class="pf-stock-cell pf-stock-cell-editing">${stockEditIdentity}</td>
        <td class="pf-col-group"><select class="pf-group-select js-pf-group"${editAttrs}>${groupOpts}</select></td>
        <td class="pf-col-num pf-col-changepct">${fmtChangePct(r.changePct, r.change)}</td>
        <td class="pf-col-num pf-col-curprice">${r.price !== null ? _fp(r.price) : '-'}</td>
        <td class="pf-col-num pf-col-benchmark js-pf-bench-picker" title="벤치마크 변경">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
        <td class="pf-col-num pf-col-invested">${r.tradingValue !== null ? fmtTradingValueKrw(r.tradingValue) : '-'}</td>
        <td class="pf-col-num pf-col-buyprice"><span class="pf-price-edit-wrap"><input class="pf-edit-input js-pf-edit-price" id="pfEditPrice" value="${r.avgPrice}" type="number" step="any"${editAttrs}>${avgPriceCurrencyControl}</span></td>
        <td class="pf-col-num pf-col-target"${targetTitle}><span class="pf-target-edit-wrap"><input class="pf-edit-input js-pf-edit-target" id="pfEditTarget" value="${escapeHtml(targetInputValue)}" type="text" inputmode="decimal" placeholder="자동 또는 BPS*0.4+DPS*10" title="${escapeHtml(targetHelp)}"${editAttrs}><button type="button" class="pf-target-clear js-pf-target-clear" title="목표가 표시 안 함 (- 로 고정)"${editAttrs}>×</button></span></td>
        <td class="pf-col-num pf-col-achiev"${targetTitle}>${r.achievementPct !== null ? fmtPct(r.achievementPct, false) : '-'}</td>
        <td class="pf-col-num pf-col-return"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
        <td class="pf-col-num pf-col-qty"><input class="pf-edit-input js-pf-edit-qty" id="pfEditQty" value="${r.qty}" type="number" step="${qtyStep}"${editAttrs}></td>
        <td class="pf-col-num pf-col-mktval">${r.marketValue !== null ? _fp(r.marketValue) : '-'}</td>
        <td class="pf-col-num pf-col-dividend">${r.dividendAmount !== null ? _fp(r.dividendAmount) : '-'}</td>
        <td class="pf-col-num pf-col-divyield">${r.dividendYield !== null ? fmtPct(r.dividendYield, false) : '-'}</td>
        <td class="pf-col-num pf-col-weight">${fmtPct(weight)}</td>
        <td class="pf-col-date"><input class="pf-edit-input js-pf-edit-created-at" id="pfEditCreatedAt" value="${r.createdAtSort || ''}" type="date"${editAttrs}></td>
        <td class="pf-col-act"><div class="pf-row-actions">
          <button type="button" class="pf-row-btn save js-pf-save" title="${isSaving ? '저장 중입니다' : '저장'}"${saveAttrs}>${saveContent}</button>
          <button type="button" class="pf-row-btn cancel js-pf-cancel" title="취소"${editAttrs}>✕</button>
        </div></td>
      </tr>`;
    }
    return `<tr data-code="${safeCode}">
      <td class="${stockCellClass}">${dragHandle}${stockIdentity}</td>
      <td class="pf-col-group">${groupHtml}</td>
      <td class="pf-col-num pf-col-changepct">${fmtChangePct(r.changePct, r.change)}</td>
      <td class="pf-col-num pf-col-curprice">${r.price !== null ? _fp(r.price) : '-'}</td>
      <td class="pf-col-num pf-col-benchmark" title="수정모드에서 변경">${fmtBenchmarkPct(r.benchmark_code)}<span class="pf-benchmark-name">${escapeHtml(benchmarkName(r.benchmark_code || ''))}</span></td>
      <td class="pf-col-num pf-col-invested">${r.tradingValue !== null ? fmtTradingValueKrw(r.tradingValue) : '-'}</td>
      <td class="pf-col-num pf-col-buyprice">${pfFmtAvgPriceCell(r, _fp)}</td>
      <td class="pf-col-num pf-col-target"${targetTitle}>${r.targetPrice !== null ? _fp(r.targetPrice) : '-'}</td>
      <td class="pf-col-num pf-col-achiev"${targetTitle}>${r.achievementPct !== null ? fmtPct(r.achievementPct, false) : '-'}</td>
      <td class="pf-col-num pf-col-return"><span class="pf-return ${returnClass(r.returnPct)}">${r.returnPct !== null ? fmtPct(r.returnPct) : '-'}</span></td>
      <td class="pf-col-num pf-col-qty">${fmtQty(r.qty)}</td>
      <td class="pf-col-num pf-col-mktval">${r.marketValue !== null ? _fp(r.marketValue) : '-'}</td>
      <td class="pf-col-num pf-col-dividend">${r.dividendAmount !== null ? _fp(r.dividendAmount) : '-'}</td>
      <td class="pf-col-num pf-col-divyield">${r.dividendYield !== null ? fmtPct(r.dividendYield, false) : '-'}</td>
      <td class="pf-col-num pf-col-weight">${fmtPct(weight)}</td>
      <td class="pf-col-date">${r.createdAtSort || '-'}</td>
      <td class="pf-col-act"><div class="pf-row-actions">
        <button type="button" class="pf-row-btn edit js-pf-edit" title="편집">✎</button>
        <button type="button" class="pf-row-btn delete js-pf-delete" title="삭제">✕</button>
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
        _pfClearPortfolioDragOver(tbody);
      });
    });
    tbody.querySelectorAll('tr[data-code]').forEach(tr => {
      tr.addEventListener('dragover', (e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        _pfClearPortfolioDragOver(tbody);
        if (!tr.classList.contains('dragging')) {
          tr.classList.add(_pfDropPositionForEvent(e, tr) === 'after' ? 'drag-over-after' : 'drag-over-before');
        }
      });
      tr.addEventListener('dragleave', () => tr.classList.remove('drag-over-before', 'drag-over-after'));
      tr.addEventListener('drop', (e) => {
        e.preventDefault();
        const dropPosition = _pfDropPositionForEvent(e, tr);
        tr.classList.remove('drag-over-before', 'drag-over-after');
        const fromCode = e.dataTransfer.getData('text/plain');
        const toCode = tr.dataset.code;
        if (fromCode && toCode && fromCode !== toCode) pfDropRow(fromCode, toCode, dropPosition);
      });
    });
  }
}

function returnClass(val) {
  if (val === null || val === undefined) return '';
  return val > 0 ? 'pf-return positive' : val < 0 ? 'pf-return negative' : '';
}

// _drawSparkline / _drawSparklinePoints — pure canvas-drawing primitives —
// live in static/js/portfolio-sparklines.js (loaded before this file). The
// data-preparation helpers below stay here, coupled to portfolio settlement state.
function _sparkLocalMinuteValue(ts) {
  const m = String(ts || '').match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d+(?:\.\d+)?))?/);
  if (!m) return null;
  const seconds = Number(m[6] || 0);
  return Date.UTC(
    Number(m[1]),
    Number(m[2]) - 1,
    Number(m[3]),
    Number(m[4]),
    Number(m[5]),
    Math.floor(seconds),
    Math.round((seconds % 1) * 1000),
  ) / 60000;
}

// TODAY sparkline 은 세션일 08:00~20:00(KST) 고정 축으로 그린다. 결산창(직전 20:00→
// 다음 20:00)의 야간 빈 구간을 잘라 장전·장중·장후 활성 시간대만 보여준다.
const SPARK_DAILY_START_HOUR = 8;
const SPARK_DAILY_END_HOUR = 20;
// 세션일 = intraday 최신 점의 날짜(주말·공휴일에도 장중 점이 몰리지 않음). 없으면 현재 KST.
function _sparkDailyAxis() {
  const ints = Array.isArray(PfStore.snapshots.intraday) ? PfStore.snapshots.intraday : [];
  let maxTs = null;
  for (const d of ints) {
    if (d && d.ts && (maxTs === null || d.ts > maxTs)) maxTs = d.ts;
  }
  const m = maxTs && /^(\d{4})-(\d{2})-(\d{2})T/.exec(maxTs);
  const ymd = m ? `${m[1]}-${m[2]}-${m[3]}` : _sparkNowKstIsoMinute().slice(0, 10);
  const pad = (n) => String(n).padStart(2, '0');
  return { start: `${ymd}T${pad(SPARK_DAILY_START_HOUR)}:00`, end: `${ymd}T${pad(SPARK_DAILY_END_HOUR)}:00` };
}

function _sparkNowKstIsoMinute() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Seoul',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(new Date()).reduce((acc, part) => {
    if (part.type !== 'literal') acc[part.type] = part.value;
    return acc;
  }, {});
  return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}`;
}

function _sparkAxisHoursFromTs(ts, axisStartTs, axisEndTs) {
  const start = _sparkLocalMinuteValue(axisStartTs);
  const end = _sparkLocalMinuteValue(axisEndTs);
  const value = _sparkLocalMinuteValue(ts);
  if (start === null || end === null || value === null || end <= start) return null;
  const hours = (value - start) / 60;
  const maxHours = (end - start) / 60;
  return Math.max(0, Math.min(maxHours, hours));
}

function _sparkTodayCashflowThroughTs(ts) {
  const target = _sparkLocalMinuteValue(ts);
  if (target === null) return 0;
  const cashflows = Array.isArray(PfStore.snapshots.prevDay?.today_cashflows)
    ? PfStore.snapshots.prevDay.today_cashflows
    : [];
  let total = 0;
  for (const cf of cashflows) {
    const cfTime = _sparkLocalMinuteValue(cf?.created_at);
    if (cfTime === null || cfTime > target) continue;
    if (cf.signed_amount !== undefined && cf.signed_amount !== null) {
      total += Number(cf.signed_amount || 0);
    } else if (cf.type === 'deposit') {
      total += Number(cf.amount || 0);
    } else if (cf.type === 'withdrawal') {
      total -= Number(cf.amount || 0);
    }
  }
  return total;
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
  if (PfStore.navHistory.length > 1) {
    const last365 = PfStore.navHistory.slice(-365);
    const returnPcts = last365.map(d => d.total_invested > 0 ? ((d.total_value - d.total_invested) / d.total_invested * 100) : 0);
    const lastReturn = returnPcts[returnPcts.length - 1] || 0;
    _drawSparkline('sparkTotalReturn', returnPcts, lastReturn >= 0 ? '#dc2626' : '#2563eb', 252, 'right');
  } else {
    _drawSparkline('sparkTotalReturn', [], '#dc2626', 252, 'right');
  }

  // MTD sparkline: fixed previous-month-end -> current-month-end axis.
  // The first point is always previous month-end at 0%, so the shape is
  // stable even before the first daily snapshot of the month exists.
  if (PfStore.snapshots.monthEnd?.total_value && PfStore.snapshots.monthEnd?.total_value > 0) {
    const now = new Date();
    const prevMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0);
    const thisMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const thisMonthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0);
    const monthStartYmd = _formatLocalYmd(thisMonthStart);
    const monthEndYmd = _formatLocalYmd(thisMonthEnd);
    const axisDays = Math.max(1, _diffLocalDays(prevMonthEnd, thisMonthEnd));
    const monthPoints = [{ x: 0, y: 0 }];
    const monthData = PfStore.navHistory.filter(d => d.date >= monthStartYmd && d.date <= monthEndYmd);
    for (const d of monthData) {
      if (!d || !d.total_value) continue;
      const dt = _parseLocalYmd(d.date);
      if (!dt) continue;
      monthPoints.push({
        x: Math.max(0, Math.min(axisDays, _diffLocalDays(prevMonthEnd, dt))),
        y: ((d.total_value / PfStore.snapshots.monthEnd.total_value) - 1) * 100,
      });
    }
    if (currentTotalValue) {
      monthPoints.push({
        x: Math.max(0, Math.min(axisDays, _diffLocalDays(prevMonthEnd, now))),
        y: ((currentTotalValue / PfStore.snapshots.monthEnd.total_value) - 1) * 100,
      });
    }
    const lastPct = monthPoints.length ? monthPoints[monthPoints.length - 1].y : 0;
    _drawSparklinePoints('sparkMonthly', monthPoints, lastPct >= 0 ? '#dc2626' : '#2563eb', axisDays);
  } else {
    _drawSparklinePoints('sparkMonthly', [], '#dc2626', 31);
  }

  // TODAY sparkline 은 세션일 08:00~20:00(KST) 고정 축. y 는 직전 20:00 결산(prevClose)
  // 대비 등락%. 축은 _sparkDailyAxis() 가 세션일 기준으로 만든다(now 까지 그려지고
  // 우측 빈 구간은 미래 시간).
  const _prevClose = (PfStore.snapshots.prevDay && PfStore.snapshots.prevDay.total_value > 0)
    ? PfStore.snapshots.prevDay.total_value
    : null;
  const _dailyAxis = _sparkDailyAxis();
  const axisStartTs = _dailyAxis.start;
  const axisEndTs = _dailyAxis.end;
  const _dailyAxisHours = SPARK_DAILY_END_HOUR - SPARK_DAILY_START_HOUR;
  if (!_prevClose) {
    _drawSparklinePoints('sparkDaily', [], '#dc2626', _dailyAxisHours);
  } else {
    const raw = [{ x: 0, y: 0 }];
    for (const d of PfStore.snapshots.intraday) {
      if (!d || !d.total_value) continue;
      const x = _sparkAxisHoursFromTs(d.ts, axisStartTs, axisEndTs);
      if (x === null) continue;
      const adjustedTotal = Number(d.total_value) - _sparkTodayCashflowThroughTs(d.ts);
      raw.push({ x, y: (adjustedTotal / _prevClose - 1) * 100 });
    }
    if (currentTotalValue) {
      const x = _sparkAxisHoursFromTs(_sparkNowKstIsoMinute(), axisStartTs, axisEndTs);
      if (x !== null) {
        raw.push({ x, y: (currentTotalValue / _prevClose - 1) * 100 });
      }
    }
    const lastPct = raw.length ? raw[raw.length - 1].y : 0;
    _drawSparklinePoints('sparkDaily', raw, lastPct >= 0 ? '#dc2626' : '#2563eb', _dailyAxisHours);
  }
}
function fmtNum(n) { return n !== null && n !== undefined ? Number(n).toLocaleString() : '-'; }
function fmtKrw(n, maxDecimals = null) {
  if (n === null || n === undefined) return '-';
  const a = Math.abs(n);
  const clampDigits = d => Number.isFinite(maxDecimals) ? Math.min(d, Math.max(0, maxDecimals)) : d;
  if (a >= 1e12) { const v = n / 1e12; const d = clampDigits(a >= 1e15 ? 0 : a >= 1e14 ? 1 : a >= 1e13 ? 2 : 3); return v.toFixed(d) + '조'; }
  if (a >= 1e8)  { const v = n / 1e8;  const d = clampDigits(a >= 1e11 ? 0 : a >= 1e10 ? 1 : a >= 1e9 ? 2 : 3);  return v.toFixed(d) + '억'; }
  return Number(Math.round(n)).toLocaleString();
}
function fmtTradingValueKrw(n) {
  return fmtKrw(n, 2);
}

function pfFmtPortfolioValue(value) {
  const converted = pfFx(value);
  return PfStore.currency.unit === 'USD'
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
  const bq = PfStore.benchmarkQuotes[benchmarkCode];
  // For stock benchmarks (e.g., common stock for preferred), check regular quote cache
  if (!bq && benchmarkCode.length === 6) {
    const item = PfStore.items.find(i => i.stock_code === benchmarkCode);
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
  const bq = PfStore.benchmarkQuotes[code];
  if (bq && bq.name && bq.name !== code) name = bq.name;
  else {
    const preset = _BENCHMARK_PRESETS.find(p => p.code === code);
    if (preset) name = preset.name;
    else {
      const item = PfStore.items.find(i => i.stock_code === code);
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
