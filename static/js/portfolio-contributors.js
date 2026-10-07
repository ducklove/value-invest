// TODAY / MTD / YTD share a hover preview and click-to-pin popover.
let _pfContributorData = {};
let _pfContributorPeriod = null;
let _pfContributorPinned = false;
let _pfContributorCloseTimer = null;
let _pfContributorPopover = null;
let _pfContributorRestoringFocus = false;

function _pfContributorNumber(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function _pfContributorIsCash(code) {
  return /^(CASH_|CMA_|FUTURES_)/.test(code);
}

function pfBuildSummaryContributors(rows, snap, latestSnap, period, allRows = rows) {
  if (PfStore.accountId) return { message: '계좌별 비교 기준이 없습니다. 전체 계좌 · 합산 보기에서 확인해 주세요.' };
  if (!snap?.date || !snap.stock_positions) return { message: '종목별 비교 기준이 없습니다.' };
  if (period !== 'today' && !snap.linked && latestSnap?.price_basis
      && (snap.price_basis || 'legacy_latest') !== latestSnap.price_basis) {
    return { message: '정산 기준이 변경되어 기간 비교를 준비 중입니다.' };
  }
  const usd = PfStore.currency.unit === 'USD';
  const currentFx = usd ? _pfContributorNumber(PfStore.currency.fxRate) : 1;
  const baselineFx = usd ? _pfContributorNumber(snap.fx_usdkrw) : 1;
  if (!(currentFx > 0 && baselineFx > 0)) return { message: '비교에 필요한 환율이 없습니다.' };
  const current = new Map(allRows.map(row => [row.stock_code, row]));
  const visible = new Set(rows.map(row => row.stock_code));
  const values = snap.stock_values || {};
  const positions = snap.stock_positions;
  const trades = snap.stock_trade_flows || {};
  const codes = new Set([...current.keys(), ...Object.keys(values), ...Object.keys(trades)]);
  const search = (PfStore.filters.searchText || '').trim().toLowerCase();
  const candidates = [];
  let excluded = 0;
  let foreignTrades = false;
  for (const code of codes) {
    if (_pfContributorIsCash(code)) continue;
    const row = current.get(code);
    if (row && !visible.has(code)) continue;
    const position = positions[code];
    const trade = trades[code];
    const name = row?.stock_name || trade?.stock_name || code;
    // Sold positions still contribute. Apply the same group/search scope to them.
    if (!row) {
      if (PfStore.filters.group !== null && !PfStore.filters.group.has(position?.group_name)) continue;
      if (search && !search.split(/[\s,]+/).every(token => `${name} ${code} ${position?.group_name || ''}`.toLowerCase().includes(token))) continue;
    }
    const hasBase = Object.prototype.hasOwnProperty.call(values, code);
    const baseValue = hasBase ? _pfContributorNumber(values[code]) : 0;
    const baseQuantity = hasBase ? _pfContributorNumber(position?.quantity) : 0;
    const quantity = row ? _pfContributorNumber(row.qty) : 0;
    const currentValue = row ? _pfContributorNumber(row.marketValue) : 0;
    const quantityChange = trade ? _pfContributorNumber(trade.quantity_change) : 0;
    if (quantity === 0 && baseQuantity === 0 && !trade) continue;
    if (baseValue === null || baseQuantity === null || quantity === null || currentValue === null
        || quantityChange === null || trade?.comparable === false
        || Math.abs(quantity - baseQuantity - quantityChange) > 1e-8) {
      excluded++;
      continue;
    }
    const tradeFx = !trade || trade.currency === 'KRW' ? 1
      : trade.currency === 'USD' ? (PfStore.currency.fxRate || trade.fx_rate) : trade.fx_rate;
    if (!(tradeFx > 0) || (trade && (_pfContributorNumber(trade.cash_change) === null
        || _pfContributorNumber(trade.buy_amount) === null))) {
      excluded++;
      continue;
    }
    // Stock value changes alone would rank purchases as gains and sales as losses.
    // Add net sale proceeds / subtract purchases and their fees instead.
    const base = baseValue / baselineFx;
    const amount = currentValue / currentFx - base + (trade?.cash_change || 0) * tradeFx / currentFx;
    const capital = Math.abs(base) + (trade?.buy_amount || 0) * tradeFx / currentFx;
    const pct = capital > 0 ? amount / capital * 100 : null;
    if (!Number.isFinite(amount)) { excluded++; continue; }
    if (trade && trade.currency !== 'KRW') foreignTrades = true;
    candidates.push({ code, name, amount, pct });
  }
  if (!candidates.length) return { message: '종목별 비교 기준이 없습니다.' };
  const tie = (a, b) => a.code.localeCompare(b.code);
  return {
    date: snap.date, pending: snap.settlement_pending, excluded, foreignTrades,
    positive: candidates.filter(row => row.amount > 0).sort((a, b) => b.amount - a.amount || tie(a, b)).slice(0, 3),
    negative: candidates.filter(row => row.amount < 0).sort((a, b) => a.amount - b.amount || tie(a, b)).slice(0, 3),
  };
}

function pfSummaryContributorFocus() {
  return document.activeElement?.closest('.js-pf-contributors')?.dataset.period || null;
}

function pfUpdateSummaryContributors(rows, latestSnap, ready, focusPeriod = null, allRows = rows) {
  const snapshots = { today: PfStore.snapshots.prevDay, mtd: PfStore.snapshots.monthEnd, ytd: PfStore.snapshots.yearStart };
  for (const [period, snap] of Object.entries(snapshots)) {
    _pfContributorData[period] = ready
      ? pfBuildSummaryContributors(rows, snap, latestSnap, period, allRows)
      : { message: '시세를 불러오는 중입니다.' };
    // Contributor availability controls only the tooltip, never card metrics.
    const button = document.querySelector(`.js-pf-contributors[data-period="${period}"]`);
    if (!button) continue;
    const available = !_pfContributorData[period].message;
    button.disabled = !available;
    button.querySelector('.pf-summary-info').hidden = !available;
    button.setAttribute('aria-label', `${period.toUpperCase()} 성과${available ? ' 기여 종목 보기' : ''}`);
    for (const [name, value] of Object.entries({ 'aria-haspopup': 'dialog', 'aria-controls': 'pfContributorPopover', 'aria-expanded': 'false' })) {
      if (available) button.setAttribute(name, value);
      else button.removeAttribute(name);
    }
  }
  if (focusPeriod) {
    _pfContributorRestoringFocus = true;
    document.querySelector(`.js-pf-contributors[data-period="${focusPeriod}"]`)?.focus({ preventScroll: true });
    _pfContributorRestoringFocus = false;
  }
  if (_pfContributorPeriod) _pfRenderContributorPopover();
}

function _pfContributorButton() {
  return document.querySelector(`.js-pf-contributors[data-period="${_pfContributorPeriod}"]`);
}

function _pfContributorMoney(amount) {
  const sign = amount > 0 ? '+' : amount < 0 ? '-' : '';
  return sign + (PfStore.currency.unit === 'USD' ? '$' : '')
    + Math.abs(amount).toLocaleString('ko-KR', { maximumFractionDigits: PfStore.currency.unit === 'USD' ? 2 : 0 })
    + (PfStore.currency.unit === 'USD' ? '' : '원');
}

function _pfRenderContributorPopover() {
  const button = _pfContributorButton();
  const data = _pfContributorData[_pfContributorPeriod];
  if (!button || !data || data.message) { pfCloseSummaryContributors(); return; }
  if (!_pfContributorPopover) {
    _pfContributorPopover = document.createElement('div');
    _pfContributorPopover.id = 'pfContributorPopover';
    _pfContributorPopover.className = 'pf-contributors-popover';
    _pfContributorPopover.setAttribute('role', 'dialog');
    _pfContributorPopover.setAttribute('aria-modal', 'false');
    _pfContributorPopover.setAttribute('aria-labelledby', 'pfContributorTitle');
    document.body.appendChild(_pfContributorPopover);
  }
  const section = (key, label) => `<section class="pf-contributors-section ${key}">
    <h4>${label} <span>TOP 3</span></h4>
    ${data[key].length ? `<ol>${data[key].map(row => `<li data-code="${escapeHtml(row.code)}">
      <span class="pf-contributor-stock"><strong>${escapeHtml(row.name)}</strong><small>${escapeHtml(row.code)}</small></span>
      <span class="pf-contributor-numbers ${key}"><strong>${_pfContributorMoney(row.amount)}</strong><small>${fmtPct(row.pct)}</small></span>
    </li>`).join('')}</ol>` : '<p class="pf-contributors-empty">해당 종목이 없습니다.</p>'}
  </section>`;
  const body = data.message ? `<p class="pf-contributors-empty">${escapeHtml(data.message)}</p>`
    : `<div class="pf-contributors-columns">${section('positive', '+ 상승 기여')}${section('negative', '− 하락 기여')}</div>
      <p class="pf-contributors-note">변동액순 · 변동률은 기준 평가액 + 매수금액 대비<br>매매 비용 포함 · 현금·배당 제외${data.foreignTrades ? '<br>외화 매매대금은 현재 환율로 환산' : ''}${data.excluded ? `<br>수량 변경·기준/시세 누락 ${data.excluded}종목 제외` : ''}</p>`;
  // Keep the close control stable when realtime quotes update the content.
  if (!_pfContributorPopover.firstChild) {
    _pfContributorPopover.innerHTML = `<div class="pf-contributors-head"><div><h3 id="pfContributorTitle"></h3><p></p></div>
      <button type="button" class="pf-contributors-close js-pf-contributors-close" aria-label="성과 기여 종목 닫기">×</button></div>
      <div class="pf-contributors-content"></div>`;
  }
  _pfContributorPopover.querySelector('h3').textContent = `${_pfContributorPeriod.toUpperCase()} 성과 기여 종목`;
  _pfContributorPopover.querySelector('.pf-contributors-head p').textContent = data.date
    ? `${data.date} 정산 대비 · 최신 평가${data.pending ? ' · 정산 미완료' : ''}` : '';
  _pfContributorPopover.querySelector('.pf-contributors-content').innerHTML = body;
  _pfContributorPopover.hidden = false;
  document.querySelectorAll('.js-pf-contributors').forEach(el => {
    if (!el.disabled) el.setAttribute('aria-expanded', String(el === button));
  });
  pfPositionSummaryContributors();
}

function pfPositionSummaryContributors() {
  if (!_pfContributorPeriod || !_pfContributorPopover || _pfContributorPopover.hidden) return;
  const button = _pfContributorButton();
  if (!button || !button.getClientRects().length) { pfCloseSummaryContributors(); return; }
  const rect = button.getBoundingClientRect();
  if (rect.bottom < 0 || rect.top > window.innerHeight) { pfCloseSummaryContributors(); return; }
  const panel = _pfContributorPopover;
  const width = Math.min(480, window.innerWidth - 24);
  const left = Math.max(12, Math.min(rect.left, window.innerWidth - width - 12));
  panel.style.width = `${width}px`;
  panel.style.left = `${left}px`;
  const below = window.innerHeight - rect.bottom - 20;
  const above = rect.top - 20;
  panel.style.maxHeight = `${Math.max(80, Math.max(below, above))}px`;
  const height = panel.getBoundingClientRect().height;
  const top = below >= height || below >= above ? rect.bottom + 8 : rect.top - height - 8;
  panel.style.top = `${Math.max(12, Math.min(top, window.innerHeight - height - 12))}px`;
}

function pfOpenSummaryContributors(button, pin = false) {
  clearTimeout(_pfContributorCloseTimer);
  const period = button?.dataset.period;
  if (!['today', 'mtd', 'ytd'].includes(period)) return;
  if (!_pfContributorData[period] || _pfContributorData[period].message) return;
  if (!pin && _pfContributorPinned) return;
  if (pin && _pfContributorPeriod === period && _pfContributorPinned) {
    pfCloseSummaryContributors();
    return;
  }
  _pfContributorPeriod = period;
  _pfContributorPinned = pin;
  _pfRenderContributorPopover();
}

function pfCloseSummaryContributors(restoreFocus = false) {
  clearTimeout(_pfContributorCloseTimer);
  const button = _pfContributorButton();
  _pfContributorPeriod = null;
  _pfContributorPinned = false;
  if (_pfContributorPopover) _pfContributorPopover.hidden = true;
  document.querySelectorAll('.js-pf-contributors').forEach(el => {
    if (!el.disabled) el.setAttribute('aria-expanded', 'false');
  });
  if (restoreFocus && button) {
    _pfContributorRestoringFocus = true;
    button.focus({ preventScroll: true });
    _pfContributorRestoringFocus = false;
  }
}

function pfSummaryContributorPreview(event, entering) {
  if (event.type.startsWith('pointer') && event.pointerType !== 'mouse') return;
  if (_pfContributorRestoringFocus) return;
  const button = event.target.closest?.('.js-pf-contributors');
  const panel = event.target.closest?.('#pfContributorPopover');
  if (!button && !panel) return;
  if (entering) {
    clearTimeout(_pfContributorCloseTimer);
    if (button) pfOpenSummaryContributors(button);
  } else if (!event.relatedTarget?.closest?.('.js-pf-contributors, #pfContributorPopover') && !_pfContributorPinned) {
    clearTimeout(_pfContributorCloseTimer);
    _pfContributorCloseTimer = setTimeout(() => {
      if (!_pfContributorPinned) pfCloseSummaryContributors();
    }, 180);
  }
}

if (typeof window !== 'undefined') {
  window.addEventListener('resize', pfPositionSummaryContributors);
  document.addEventListener('scroll', event => {
    if (!event.target.closest?.('#pfContributorPopover')) pfPositionSummaryContributors();
  }, true);
}
