// 배당 캘린더 — 성과 탭의 '배당 캘린더' 카드 (#pfRebalanceWrap 다음).
//
// GET /api/portfolio/dividend-calendar?months=12 (routes/dividend_calendar.py)
// 를 소비해 월별 합계와 표준 배당 내역 표(기준일·종목·지급일·주당액·수량·총액·세금·실 수령액)를 렌더링한다.
//
// - lazy: 성과 탭이 처음 보일 때 pfSwitchTab(portfolio-performance.js)이
//   pfLoadDividendCalendarPanel() 을 호출한다. 응답은 인메모리 메모
//   (_pfDivCalData)는 사용자·수량·날짜가 같을 때 5분 이내에서만 재사용한다.
// - 지급일만 현금 합계에 포함한다. 배당락일과 기준일은 권리일 안내다.
// - 실제 입금 근거가 있으면 지급일 칸에 증권사 배지를 붙인다. 일정 연결 여부와 관계없이 같은 컬럼을 쓴다.
// - 표의 총액·세금·실 수령액은 계산액이다. 실제 계좌 수령액은 증권사 배지의 툴팁으로 표시한다.
// - 백그라운드 로드 오류는 reportApiError silent + 패널 내 안내.
// 포맷터(fmtKrw/escapeHtml)는 portfolio-render.js / utils.js 공용 헬퍼를
// 재사용한다 — 여기서 중복 정의하지 않는다.

let _pfDivCalData = null; // 마지막 GET /api/portfolio/dividend-calendar 페이로드 (메모)
let _pfDivCalLoadSeq = 0;
let _pfDivCalCacheKey = '';
let _pfDivCalLoadedAt = 0;
const _pfDivCalOpenMonths = new Set(); // 펼쳐진 월 키 — 재렌더에도 유지

function _pfDivCalContentEl() { return document.getElementById('pfDivCalContent'); }

function _pfDivCalMsg(message) {
  const el = _pfDivCalContentEl();
  if (el) el.innerHTML = `<div class="pf-risk-empty">${escapeHtml(message)}</div>`;
}

// '2026-06' → '2026년 6월'
function _pfDivCalMonthLabel(month) {
  const m = String(month || '').match(/^(\d{4})-(\d{2})$/);
  return m ? `${m[1]}년 ${Number(m[2])}월` : String(month || '');
}

// 주당 배당 표기 — KRW 는 fmtKrw, 외화는 소수 유지 + 통화코드.
function _pfDivCalPerShare(ev) {
  if (ev.amount_status === 'unknown' || ev.amount_per_share === null || ev.amount_per_share === undefined) return '배당금 미확인';
  if ((ev.currency || 'KRW') === 'KRW') return `${fmtKrw(ev.amount_per_share)}원`;
  return `${Number(ev.amount_per_share).toLocaleString(undefined, { maximumFractionDigits: 4 })} ${escapeHtml(ev.currency)}`;
}

function _pfDivCalEstimateBadge(ev) {
  return ev.date_status === 'estimated' || ev.type === 'estimated'
    ? '<span class="pf-divcal-badge">예상</span>' : '';
}

// 원통화 금액 표기 — KRW 는 fmtKrw + 원, 외화는 소수 4자리까지 + 통화코드.
function _pfDivCalMoney(value, currency) {
  if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) return '';
  if ((currency || 'KRW') === 'KRW') return `${fmtKrw(Number(value))}원`;
  return `${Number(value).toLocaleString(undefined, { maximumFractionDigits: 4 })} ${currency}`;
}

// 실제 계좌 기록이 연결된 경우에만 지급일에 증권사 배지를 표시한다.
function _pfDivCalPaymentEvidence(ev) {
  return ev.nh_match && (ev.verification === 'nh_confirmed' || ev.verification === 'nh_partial' || ev.date_status === 'nh')
    ? ev.nh_match : null;
}

function _pfDivCalBrokerBadge(evidence) {
  if (!evidence) return '';
  const broker = evidence.broker_name || 'NH';
  const amount = _pfDivCalMoney(evidence.net_amount, evidence.currency || 'KRW') || '미확인';
  const domestic = Number(evidence.domestic_tax_krw) > 0
    ? ` · 원화 별도 세금 ${_pfDivCalMoney(evidence.domestic_tax_krw, 'KRW')}` : '';
  const tooltip = `${broker} 계좌 수령액: ${amount}${domestic}`;
  return `<span class="pf-divcal-badge broker" tabindex="0" title="${escapeHtml(tooltip)}" aria-label="${escapeHtml(tooltip)}">${escapeHtml(broker)}</span>`;
}

function _pfDivCalDay(ev, field) {
  const kinds = { pay_date: 'payment', ex_date: 'ex_date', record_date: 'record_date' };
  return ev[field] || (ev.date_kind === kinds[field] || ev.type === kinds[field] ? ev.date : null);
}

function _pfDivCalDateHtml(day, ev) {
  if (!day) return '<span class="pf-divcal-unknown">미확인</span>';
  return `${escapeHtml(day)}${ev.date_precision === 'approximate' && day === ev.date ? ' 전후' : ''}`;
}

function _pfDivCalHasNumber(value) {
  return value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value));
}

function _pfDivCalValue(value, currency) {
  return _pfDivCalHasNumber(value) ? escapeHtml(_pfDivCalMoney(value, currency))
    : '<span class="pf-divcal-unknown">미확인</span>';
}

function _pfDivCalEventHtml(ev, todayIso) {
  const evidence = _pfDivCalPaymentEvidence(ev);
  const nhOnly = ev.date_status === 'nh';
  const estimated = ev.date_status === 'estimated' || ev.type === 'estimated';
  const currency = ev.currency || 'KRW';
  const classes = ['pf-divcal-event'];
  if (estimated) classes.push('pf-divcal-est');
  if (evidence) classes.push('pf-divcal-paid');
  if (nhOnly) classes.push('pf-divcal-nh');
  if (ev.date >= todayIso) classes.push('pf-divcal-upcoming');

  const exDay = _pfDivCalDay(ev, 'ex_date');
  const basisDay = ev.dividend_basis_date;
  const basisTitle = ev.dividend_basis_rule === 'krx_record_t2'
    ? `공시 배당기준일 ${_pfDivCalDay(ev, 'record_date')}의 2거래일 전${ev.dividend_basis_approximate ? ' (거래일 근사)' : ''}` : '';
  const exBadge = exDay && exDay <= todayIso ? '<span class="pf-divcal-badge ex">배당락</span>' : '';
  const actualPay = evidence ? ev.paid_date || evidence.date : null;
  const payDay = actualPay || _pfDivCalDay(ev, 'pay_date');
  const hasShares = _pfDivCalHasNumber(ev.shares);
  const hasPerShare = ev.amount_status !== 'unknown' && _pfDivCalHasNumber(ev.amount_per_share);
  const quantity = hasShares ? `${Number(ev.shares).toLocaleString()}주` : '<span class="pf-divcal-unknown">미확인</span>';
  const sold = ev.held_now === false && !nhOnly ? ' <span class="pf-divcal-badge sold">매도</span>' : '';
  const taxTitle = _pfDivCalHasNumber(ev.calculated_tax_rate) ? `기본 세율 ${ev.calculated_tax_rate}%로 계산` : '';

  return `<tr class="${classes.join(' ')}">
    <td class="pf-divcal-record-date" title="${escapeHtml(basisTitle)}"><span class="pf-divcal-basis-day">${_pfDivCalDateHtml(basisDay, ev)}</span>${exBadge}</td>
    <th scope="row" class="pf-divcal-stock"><span class="pf-divcal-stock-name">${escapeHtml(ev.stock_name || ev.stock_code)}${sold}</span>${_pfDivCalEstimateBadge(ev)}</th>
    <td class="pf-divcal-payment-date"><span class="pf-divcal-pay-day">${_pfDivCalDateHtml(payDay, actualPay ? {} : ev)}</span> ${actualPay ? _pfDivCalBrokerBadge(evidence) : ''}</td>
    <td class="pf-divcal-per-share pf-divcal-number">${hasPerShare ? _pfDivCalPerShare(ev) : '<span class="pf-divcal-unknown">미확인</span>'}</td>
    <td class="pf-divcal-quantity pf-divcal-number">${quantity}</td>
    <td class="pf-divcal-gross pf-divcal-number">${_pfDivCalValue(ev.calculated_gross_amount, currency)}</td>
    <td class="pf-divcal-tax pf-divcal-number" title="${escapeHtml(taxTitle)}">${_pfDivCalValue(ev.calculated_tax_amount, currency)}</td>
    <td class="pf-divcal-net pf-divcal-number">${_pfDivCalValue(ev.calculated_net_amount, currency)}</td>
  </tr>`;
}

function _pfDivCalMonthHtml(monthRow, eventsByMonth, todayMonth, todayIso) {
  const month = monthRow.month;
  const events = eventsByMonth[month] || [];
  const isNow = month === todayMonth;
  const open = _pfDivCalOpenMonths.has(month);
  const empty = events.length === 0;
  const calculatedTotal = monthRow.calculated_total_krw ?? Math.max(0, (monthRow.total_krw || 0) - (monthRow.nh_only_krw || 0));
  const total = calculatedTotal > 0 ? `${fmtKrw(calculatedTotal)}원` : '-';
  const rowCls = `pf-divcal-month${isNow ? ' now' : ''}${empty ? ' empty' : ''}`;
  const head = `<div class="${rowCls}" data-month="${escapeHtml(month)}"${empty ? '' : ` onclick="pfDivCalToggleMonth('${escapeHtml(month)}')"`}>
    <span class="pf-divcal-caret">${empty ? '·' : (open ? '▾' : '▸')}</span>
    <span class="pf-divcal-month-label">${_pfDivCalMonthLabel(month)}${isNow ? ' <span class="pf-divcal-sub">(이번 달)</span>' : ''}</span>
    <span class="pf-divcal-month-total">${events.length ? `${events.length}건 · ` : ''}${total}<span class="pf-divcal-sub">지급 일정 기준 세전 계산 합계</span>${monthRow.unconverted_count ? ' + 환산 미확인' : ''}${monthRow.quantity_unknown_count ? ' + 수량 미상' : ''}</span>
  </div>`;
  if (empty) return head;
  const list = `<div class="pf-divcal-events" data-month-events="${escapeHtml(month)}" style="display:${open ? '' : 'none'};">
    <div class="pf-divcal-table-scroll" role="region" aria-label="${escapeHtml(_pfDivCalMonthLabel(month))} 배당 내역" tabindex="0">
      <table class="pf-divcal-table"><caption>${escapeHtml(_pfDivCalMonthLabel(month))} 배당 내역</caption>
        <thead><tr>${['배당기준일', '종목', '지급일', '주당 배당액', '수량', '배당총액', '세금', '실 수령액'].map((name, index) => `<th scope="col"${index >= 3 ? ' class="pf-divcal-number"' : ''}>${name}</th>`).join('')}</tr></thead>
        <tbody>${events.map((ev) => _pfDivCalEventHtml(ev, todayIso)).join('')}</tbody>
      </table>
    </div>
  </div>`;
  return head + list;
}

function _pfRenderDividendCalendar(data) {
  const el = _pfDivCalContentEl();
  if (!el) return;
  const events = (data && Array.isArray(data.events)) ? data.events : [];
  const monthly = (data && Array.isArray(data.monthly)) ? data.monthly : [];
  const coverage = Array.isArray(data?.coverage) ? data.coverage : [];
  if (!events.length && !coverage.length) {
    _pfDivCalMsg('보유 종목의 배당 정보가 수집되면 표시됩니다.');
    return;
  }
  const todayIso = data.as_of || new Date().toISOString().slice(0, 10);
  const todayMonth = todayIso.slice(0, 7);
  // 첫 렌더에서는 이번 달을 기본으로 펼쳐 보여준다(있을 때만).
  if (!_pfDivCalOpenMonths.size && monthly.some((m) => m.month === todayMonth && m.count > 0)) {
    _pfDivCalOpenMonths.add(todayMonth);
  }
  const eventsByMonth = {};
  for (const ev of events) {
    const key = String(ev.date || '').slice(0, 7);
    (eventsByMonth[key] = eventsByMonth[key] || []).push(ev);
  }
  const summary = data.summary || {};
  const totalLine = `기간 <strong>${escapeHtml(data.start_month || '')} ~ ${escapeHtml(data.end_month || '')}</strong>`
    + ` · 지급 일정 기준 세전 계산 합계 <strong>${fmtKrw(summary.calculated_total_krw ?? monthly.reduce((sum, row) => sum + (row.calculated_total_krw ?? Math.max(0, (row.total_krw || 0) - (row.nh_only_krw || 0))), 0))}원</strong>`
    + (summary.estimated_count ? ` · 예상 ${Number(summary.estimated_count)}건` : '')
    + (summary.not_held_count ? ` · 기준 시점 미보유 ${Number(summary.not_held_count)}건 제외` : '');
  const coverageHtml = coverage.length ? `<details class="pf-divcal-coverage"><summary>종목별 일정 확인 · 지급일 미확인 ${Number(summary.unknown_payment_count || 0)}종목${summary.stale_count ? ` · 갱신 미완료 ${Number(summary.stale_count)}종목` : ''}</summary>${coverage.map(c => `<div><strong>${escapeHtml(c.stock_name)}</strong> · ${escapeHtml(c.frequency_label)}${c.held === false ? ' · 매도' : ''} · ${c.has_payment_dates ? '지급일 수집' : '지급일 미확인'}${c.status !== 'fresh' ? ' · 갱신 필요' : ''}</div>`).join('')}</details>` : '';
  el.innerHTML = `<div class="pf-divcal-list">
    ${monthly.map((m) => _pfDivCalMonthHtml(m, eventsByMonth, todayMonth, todayIso)).join('')}
  </div>
  <div class="pf-chart-range">${totalLine}</div>
  <div class="pf-divcal-note">배당총액 = 주당 배당액 × 수량 · 세금 = 기본 세율로 계산 · 실 수령액 = 배당총액 − 세금. NH 배지에 마우스를 올리면 계좌 수령액을 볼 수 있습니다.</div>
  <details class="pf-divcal-help pf-divcal-note"><summary>표의 날짜·금액 기준</summary><dl>
    <dt>배당기준일</dt><dd>배당락일이 제공되면 그 날짜를 표시하고, 날짜가 지나면 배당락 배지를 붙입니다. 국내 기준일만 제공되면 공시 기준일의 2거래일 전을 표시합니다. 공시 기준일은 날짜의 툴팁에서 볼 수 있습니다.</dd>
    <dt>지급일</dt><dd>계좌 기록이 있으면 실제 수령일과 증권사 배지를 표시하고, 없으면 공시·예상 지급일을 표시합니다. 증권사 배지의 툴팁은 연결된 계좌의 수령액입니다.</dd>
    <dt>계산 금액</dt><dd>총액은 주당 배당액 × 수량, 세금은 국가별 기본 세율, 실 수령액은 총액에서 계산 세금을 뺀 금액입니다. 통화의 최소 단위로 총액은 반올림하고 세금은 절사합니다. 세금 칸의 툴팁에서 적용 세율을 볼 수 있습니다. 실제 계좌 수령액과 다를 수 있습니다.</dd>
    <dt>수량</dt><dd>지난 배당은 기준 시점(배당락 전 거래일 종가, 국내 기준일은 2거래일 전 종가)에 보유한 종목만 그때 수량으로 셉니다. 수량은 전 계좌 합산이며 미래·예상은 현재 수량입니다.</dd>
    <dt>월 합계</dt><dd>지급 일정의 세전 계산액을 합산합니다. 배당락일·기준일만 있는 일정과 계산할 수 없는 계좌 내역은 월 합계에서 제외됩니다. 월은 수집 일정의 대표 날짜 기준입니다.</dd>
    <dt>미확인</dt><dd>해당 날짜·금액 자료가 없다는 뜻입니다. 무배당이나 0원 확정을 뜻하지 않습니다. 주당액·수량이 없으면 금액을 계산하지 않습니다. 예상은 과거 패턴의 추정입니다.</dd>
  </dl></details>${coverageHtml}`;
}

// 월 행 클릭 — 이벤트 목록 펼침/접힘 (상태는 _pfDivCalOpenMonths 에 유지).
function pfDivCalToggleMonth(month) {
  const el = _pfDivCalContentEl();
  if (!el) return;
  const list = el.querySelector(`[data-month-events="${month}"]`);
  const head = el.querySelector(`.pf-divcal-month[data-month="${month}"]`);
  if (!list) return;
  const opening = list.style.display === 'none';
  list.style.display = opening ? '' : 'none';
  if (opening) _pfDivCalOpenMonths.add(month); else _pfDivCalOpenMonths.delete(month);
  const caret = head && head.querySelector('.pf-divcal-caret');
  if (caret) caret.textContent = opening ? '▾' : '▸';
}

// 성과 탭이 보일 때 호출(lazy). 메모가 있으면 그대로 그린다.
async function pfLoadDividendCalendarPanel({ force = false } = {}) {
  const el = _pfDivCalContentEl();
  if (!el) return;
  const key = JSON.stringify([typeof currentUser !== 'undefined' ? currentUser?.google_sub : null,
    new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' }),
    (PfStore.items || []).map(item => [item.stock_code, item.quantity])]);
  if (!force && _pfDivCalData && _pfDivCalCacheKey === key && Date.now() - _pfDivCalLoadedAt < 300000) {
    _pfRenderDividendCalendar(_pfDivCalData);
    return;
  }
  const seq = ++_pfDivCalLoadSeq;
  _pfDivCalMsg('배당 캘린더를 불러오는 중입니다...');
  try {
    const data = await apiFetchJson('/api/portfolio/dividend-calendar?months=12', {
      errorMessage: '배당 캘린더 요청 실패',
    });
    if (seq !== _pfDivCalLoadSeq) return;
    _pfDivCalData = data;
    _pfDivCalCacheKey = key;
    _pfDivCalLoadedAt = Date.now();
    _pfRenderDividendCalendar(data);
  } catch (e) {
    if (e.status === 401) {
      if (seq === _pfDivCalLoadSeq) _pfDivCalMsg('로그인 후 이용할 수 있습니다.');
      return;
    }
    // 백그라운드 로드 — 토스트 없이 콘솔 기록만 남기고 패널 안에 안내.
    reportApiError(e, '배당 캘린더', { silent: true });
    if (seq === _pfDivCalLoadSeq) {
      _pfDivCalMsg('배당 캘린더를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.');
    }
  }
}

if (typeof window !== 'undefined') {
  Object.assign(window, { pfLoadDividendCalendarPanel, pfDivCalToggleMonth });
}
