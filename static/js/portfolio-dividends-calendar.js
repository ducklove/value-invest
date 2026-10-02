// 배당 캘린더 — 성과 탭의 '배당 캘린더' 카드 (#pfRebalanceWrap 다음).
//
// GET /api/portfolio/dividend-calendar?months=12 (routes/dividend_calendar.py)
// 를 소비해 월별 합계와 표준 배당 내역 표(종목 + 날짜·배당락·주당액·수량·총액·세금·지급액)를 렌더링한다.
//
// - lazy: 성과 탭이 처음 보일 때 pfSwitchTab(portfolio-performance.js)이
//   pfLoadDividendCalendarPanel() 을 호출한다. 응답은 인메모리 메모
//   (_pfDivCalData)는 사용자·수량·날짜가 같을 때 5분 이내에서만 재사용한다.
// - 지급일만 현금 합계에 포함한다. 배당락일과 기준일은 권리일 안내다.
// - 실제 입금 근거가 있으면 지급일 칸에 증권사 배지를 붙인다. 일정 연결 여부와 관계없이 같은 컬럼을 쓴다.
// - 지난 배당은 기준 시점(배당락 전 거래일 종가·국내 기준일 2거래일 전 종가)에 보유한 종목만
//   그때 수량으로 온다. 행마다 보유 근거('수량 근거: 9/24 보유 기록', '첫 보유 기록으로 추정')와
//   지금은 없는 종목의 '매도' 태그를 보여 준다.
//   공시 / 수집 이력 / 날짜 전후 예상과 출처·갱신 시점을 함께 표시한다.
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

function _pfDivCalBadge(ev) {
  // 어느 일정에도 연결되지 않은 NH 배당 입금 — 실제 입금 행.
  if (ev.date_status === 'nh') return '<span class="pf-divcal-sub">입금 내역</span>';
  if (ev.date_status === 'observed') return '<span class="pf-divcal-badge observed">수집 이력</span>';
  return ev.confirmed
    ? '<span class="pf-divcal-badge confirmed" title="공시 자료에 있는 일정입니다. 배당금과 입금 여부는 별도로 표시합니다.">공시</span>'
    : '<span class="pf-divcal-badge">예상</span>';
}

// 원통화 금액 표기 — KRW 는 fmtKrw + 원, 외화는 소수 4자리까지 + 통화코드.
function _pfDivCalMoney(value, currency) {
  if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) return '';
  if ((currency || 'KRW') === 'KRW') return `${fmtKrw(Number(value))}원`;
  return `${Number(value).toLocaleString(undefined, { maximumFractionDigits: 4 })} ${currency}`;
}

// 외화 세금 정산(외화제세금환급) — 원화 순효과(예: '세금 정산 −55원'), 환율 미확인이면 외화 환급액.
function _pfDivCalAdjustments(m) {
  return (Array.isArray(m?.adjustments) ? m.adjustments : []).map((a) => {
    if (a.income_krw !== null && a.income_krw !== undefined && Number.isFinite(Number(a.income_krw))) {
      const v = Number(a.income_krw);
      return `세금 정산 ${v < 0 ? '−' : '+'}${fmtKrw(Math.abs(v))}원`;
    }
    const refund = _pfDivCalMoney(a.net_amount, a.currency);
    return `세금 정산${refund ? ` 환급 ${refund}` : ''}${a.domestic_tax_krw ? ` · 국내세 ${fmtKrw(Number(a.domestic_tax_krw))}원` : ''}`;
  });
}

// 입금 확인은 실제 거래 근거가 있는 경우에만 표시한다.
function _pfDivCalPaymentEvidence(ev) {
  return ev.nh_match && (ev.verification === 'nh_confirmed' || ev.verification === 'nh_partial' || ev.date_status === 'nh')
    ? ev.nh_match : null;
}

function _pfDivCalBrokerBadge(ev, evidence) {
  if (!evidence) return '';
  const broker = evidence.broker_name || 'NH';
  const partial = ev.verification === 'nh_partial';
  return `<span class="pf-divcal-badge broker${partial ? ' partial' : ''}" title="${escapeHtml(`${broker} 실제 계좌 입금 확인${partial ? ' · 다른 계좌 몫은 미확인' : ''}`)}">${escapeHtml(broker)}</span>`;
}

// '2026-09-24' → '9/24'
function _pfDivCalShortDay(value) {
  const m = String(value || '').match(/^\d{4}-(\d{2})-(\d{2})/);
  return m ? `${Number(m[1])}/${Number(m[2])}` : String(value || '');
}

// 기준 시점에 보유했지만 수량을 모르는 이유(quantity_unknown_reason).
const _PF_DIVCAL_QUANTITY_UNKNOWN = {
  changed_before_record: '수량 미상(수량 기록 전 매매)',
  not_recorded: '수량 기록 없음',
};

// 수량은 수량 칸에 한 번만 표시하고, 펼침 영역에는 보유 기록의 날짜·추정 근거를 적는다.
// 미래·예상(현재 보유)은 빈 문자열. 다른 날 수량 사용과 수량 미상 사유는 유지한다.
function _pfDivCalHolding(ev) {
  const hasShares = ev.shares !== null && ev.shares !== undefined && Number.isFinite(Number(ev.shares));
  const unknown = _PF_DIVCAL_QUANTITY_UNKNOWN[ev.quantity_unknown_reason] || '수량 미상';
  const quantityNote = ev.quantity_as_of ? ` (수량 ${_pfDivCalShortDay(ev.quantity_as_of)} 기록)` : '';
  const day = _pfDivCalShortDay(ev.holding_as_of);
  if (ev.holding_basis === 'snapshot') {
    return `수량 근거: ${day} 보유 기록${hasShares ? quantityNote : ` · ${unknown}`}`;
  }
  if (ev.holding_basis === 'earliest_snapshot') return `수량 근거: 첫 보유 기록(${day})으로 추정${hasShares ? quantityNote : ` · ${unknown}`}`;
  if (ev.holding_basis === 'current_fallback') return '수량 근거: 과거 보유 기록 없음 · 현재 수량 사용';
  return '';
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

function _pfDivCalTaxHtml(evidence, currency) {
  if (!evidence) return '<span class="pf-divcal-unknown">미확인</span>';
  const foreign = currency !== 'KRW';
  const localTax = _pfDivCalValue(evidence.tax_amount, currency);
  const domestic = foreign && _pfDivCalHasNumber(evidence.domestic_tax_krw)
    ? `<span class="pf-divcal-cell-note">국내 ${escapeHtml(_pfDivCalMoney(evidence.domestic_tax_krw, 'KRW'))}</span>` : '';
  const adjustments = _pfDivCalAdjustments(evidence);
  const detail = adjustments.length ? `<details class="pf-divcal-tax-adjustments"><summary>세금 정산 ${adjustments.length}건</summary>${adjustments.map(line => `<div>${escapeHtml(line)}</div>`).join('')}</details>` : '';
  return `${foreign ? '현지 ' : ''}${localTax}${domestic}${detail}`;
}

// 보유 근거 줄의 title: 배당 기준(규칙)과 실제로 쓴 정산일. reference_date는 '그 이하 마지막 정산'의 상한일 뿐이라
// 주말·휴장일이거나 배당락일 당일(미국·유럽)일 수 있다.
function _pfDivCalHoldingTitle(ev) {
  if (!ev.reference_date) return '';
  const rules = {
    krx_record_t2: '배당기준일 2거래일 전 종가 보유(T+2 결제)',
    ex_date_prev_day: '배당락 전 거래일 종가 보유',
    ex_date_same_day: '현지 배당락 전 거래일 종가 보유(그 장은 배당락일 KST 정산에 반영)',
    record_date: '해외 기준일로 근사',
    pay_date: '지급일 전날로 근사',
  };
  const rule = rules[ev.reference_rule] || '근사';
  const used = ev.holding_basis === 'earliest_snapshot'
    ? `기록 시작 전이라 첫 장 마감 정산(${ev.holding_as_of})의 보유로 추정합니다.`
    : `${ev.reference_date} 이하 마지막 장 마감 정산(${ev.holding_as_of || '-'})의 보유입니다.`;
  return `배당 기준: ${rule}. ${used}${ev.quantity_as_of ? ` 수량은 ${ev.quantity_as_of} 정산 기록입니다.` : ''}`;
}

function _pfDivCalCheckedDay(value) {
  const day = new Date(value);
  return Number.isNaN(day.getTime()) ? String(value || '').slice(0, 10)
    : day.toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' });
}

function _pfDivCalSource(ev) {
  const url = String(ev.source_url || '');
  const source = /^https:\/\//.test(url)
    ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(ev.source || '출처')}</a>`
    : escapeHtml(ev.source || '');
  const dates = [['배당락일', ev.ex_date], ['배당기준일', ev.record_date], ['지급일', ev.pay_date]]
    .filter(([, day]) => day && day !== ev.date).map(([label, day]) => `${label} ${escapeHtml(day)}`).join(' · ');
  const stale = ev.data_status === 'stale' ? ' · 갱신 실패, 이전 자료' : '';
  const basis = ev.basis_date ? ` · ${escapeHtml(ev.basis_date)} 이력 기준` : '';
  const fetched = ev.fetched_at ? ` · 자료 확인 ${escapeHtml(_pfDivCalCheckedDay(ev.fetched_at))}` : '';
  return `<details class="pf-divcal-source pf-divcal-sub"><summary>출처${fetched}${stale}</summary>${dates}${dates && source ? ' · ' : ''}${source}${basis}${ev.fx_source === 'stored' ? ' · 저장 환율' : ''}</details>`;
}

function _pfDivCalEventHtml(ev, todayIso) {
  const evidence = _pfDivCalPaymentEvidence(ev);
  const nhOnly = ev.date_status === 'nh';
  const estimated = ev.date_status === 'estimated' || ev.type === 'estimated';
  const partial = ev.verification === 'nh_partial';
  const currency = evidence?.currency || ev.currency || 'KRW';
  const classes = ['pf-divcal-event'];
  if (estimated) classes.push('pf-divcal-est');
  if (evidence) classes.push('pf-divcal-paid');
  if (nhOnly) classes.push('pf-divcal-nh');
  if (ev.date >= todayIso) classes.push('pf-divcal-upcoming');

  const recordDay = _pfDivCalDay(ev, 'record_date');
  const exDay = _pfDivCalDay(ev, 'ex_date');
  const scheduledPay = _pfDivCalDay(ev, 'pay_date');
  const actualPay = evidence ? ev.paid_date || evidence.date : null;
  const payDay = actualPay || scheduledPay;
  const paymentNotes = [];
  if (actualPay && scheduledPay && actualPay !== scheduledPay) paymentNotes.push(`공시 ${scheduledPay}`);
  if (partial) paymentNotes.push('일부 입금');
  else if (evidence) paymentNotes.push('입금 확인');
  else if (ev.verification === 'unconfirmed') paymentNotes.push('입금 미확인');
  else if (scheduledPay) paymentNotes.push(estimated ? '예상' : '지급 일정');
  const payNote = paymentNotes.length ? `<span class="pf-divcal-cell-note">${paymentNotes.map(escapeHtml).join(' · ')}</span>` : '';
  const exStatus = exDay ? `${exDay <= todayIso ? '배당락' : '예정'}<span class="pf-divcal-cell-note">${_pfDivCalDateHtml(exDay, ev)}</span>`
    : '<span class="pf-divcal-unknown">미확인</span>';

  const hasShares = _pfDivCalHasNumber(ev.shares);
  const hasPerShare = ev.amount_status !== 'unknown' && _pfDivCalHasNumber(ev.amount_per_share);
  const calculatedGross = hasShares && hasPerShare ? Number(ev.shares) * Number(ev.amount_per_share) : null;
  const actualGross = evidence?.gross_amount;
  const gross = _pfDivCalHasNumber(actualGross) ? actualGross : nhOnly ? ev.gross_amount : calculatedGross;
  const grossNote = _pfDivCalHasNumber(actualGross) ? (partial ? '계좌 확인분' : '실제 세전')
    : _pfDivCalHasNumber(gross) ? (estimated ? '예상 세전' : '주당액 × 수량') : '';
  const converted = !evidence && ev.currency !== 'KRW' && _pfDivCalHasNumber(ev.expected_amount_krw)
    ? `<span class="pf-divcal-cell-note">약 ${fmtKrw(ev.expected_amount_krw)}원</span>` : '';
  const holding = nhOnly ? '' : _pfDivCalHolding(ev);
  const holdingTitle = holding ? _pfDivCalHoldingTitle(ev) : '';
  const quantity = hasShares ? `${Number(ev.shares).toLocaleString()}주` : '<span class="pf-divcal-unknown">미확인</span>';
  const quantityDetail = holding ? `<details class="pf-divcal-quantity-source"><summary>근거</summary><span class="pf-divcal-holding" title="${escapeHtml(holdingTitle)}">${escapeHtml(holding)}</span></details>` : '';
  const sold = ev.held_now === false && !nhOnly ? ' <span class="pf-divcal-badge sold">매도</span>' : '';
  const code = ev.stock_name && ev.stock_name !== ev.stock_code ? `<span class="pf-divcal-cell-note">${escapeHtml(ev.stock_code || '')}</span>` : '';
  const receipt = ev.receiptable === false || ev.verification === 'nh_confirmed' ? ''
    : `<button type="button" class="pf-mini-btn pf-divcal-receipt js-pf-dividend-receipt" data-dividend-source="${escapeHtml(ev.source_key || `${ev.stock_code}:${ev.type}:${ev.date}`)}">수취 입력</button>`;
  const netNote = evidence && _pfDivCalHasNumber(evidence.net_amount)
    ? `<span class="pf-divcal-cell-note">${partial ? '계좌 확인분' : '실제 입금'}${currency !== 'KRW' && Number(evidence.domestic_tax_krw) > 0 ? ' · 국내세 별도' : ''}</span>` : '';

  return `<tr class="${classes.join(' ')}">
    <th scope="row" class="pf-divcal-stock"><span class="pf-divcal-stock-name">${escapeHtml(ev.stock_name || ev.stock_code)}${sold}</span>${code}${_pfDivCalBadge(ev)}${_pfDivCalSource(ev)}</th>
    <td class="pf-divcal-record-date">${_pfDivCalDateHtml(recordDay, ev)}</td>
    <td class="pf-divcal-payment-date"><span class="pf-divcal-pay-day">${_pfDivCalDateHtml(payDay, actualPay ? {} : ev)}</span> ${actualPay ? _pfDivCalBrokerBadge(ev, evidence) : ''}${payNote}</td>
    <td class="pf-divcal-ex-status">${exStatus}</td>
    <td class="pf-divcal-per-share pf-divcal-number">${hasPerShare ? _pfDivCalPerShare(ev) : '<span class="pf-divcal-unknown">미확인</span>'}</td>
    <td class="pf-divcal-quantity pf-divcal-number" title="${nhOnly ? '입금 내역에 배당 권리 수량이 없습니다.' : '배당 기준 시점의 전 계좌 합산 수량입니다.'}">${quantity}${quantityDetail}</td>
    <td class="pf-divcal-gross pf-divcal-number">${_pfDivCalValue(gross, currency)}${grossNote ? `<span class="pf-divcal-cell-note">${grossNote}</span>` : ''}${converted}</td>
    <td class="pf-divcal-tax pf-divcal-number">${_pfDivCalTaxHtml(evidence, currency)}</td>
    <td class="pf-divcal-net pf-divcal-number">${_pfDivCalValue(evidence?.net_amount, currency)}${netNote}${receipt}</td>
  </tr>`;
}

function _pfDivCalMonthHtml(monthRow, eventsByMonth, todayMonth, todayIso) {
  const month = monthRow.month;
  const events = eventsByMonth[month] || [];
  const isNow = month === todayMonth;
  const open = _pfDivCalOpenMonths.has(month);
  const empty = events.length === 0;
  const total = monthRow.total_krw > 0 ? `${fmtKrw(monthRow.total_krw)}원` : '-';
  const rowCls = `pf-divcal-month${isNow ? ' now' : ''}${empty ? ' empty' : ''}`;
  const head = `<div class="${rowCls}" data-month="${escapeHtml(month)}"${empty ? '' : ` onclick="pfDivCalToggleMonth('${escapeHtml(month)}')"`}>
    <span class="pf-divcal-caret">${empty ? '·' : (open ? '▾' : '▸')}</span>
    <span class="pf-divcal-month-label">${_pfDivCalMonthLabel(month)}${isNow ? ' <span class="pf-divcal-sub">(이번 달)</span>' : ''}</span>
    <span class="pf-divcal-month-total">${events.length ? `${events.length}건 · ` : ''}${total}<span class="pf-divcal-sub">지급 일정 기준 세전 계산 합계</span>${monthRow.unconverted_count ? ' + 환산 미확인' : ''}${monthRow.quantity_unknown_count ? ' + 수량 미상' : ''}${monthRow.announced_krw > 0 || monthRow.estimated_krw > 0 || monthRow.nh_only_krw > 0 ? `<span class="pf-divcal-sub">공시 ${fmtKrw(monthRow.announced_krw || 0)}원 · 예상 ${fmtKrw(monthRow.estimated_krw || 0)}원${monthRow.nh_only_krw > 0 ? ` · NH 입금 ${fmtKrw(monthRow.nh_only_krw)}원` : ''}</span>` : ''}</span>
  </div>`;
  if (empty) return head;
  const list = `<div class="pf-divcal-events" data-month-events="${escapeHtml(month)}" style="display:${open ? '' : 'none'};">
    <div class="pf-divcal-table-scroll" role="region" aria-label="${escapeHtml(_pfDivCalMonthLabel(month))} 배당 내역" tabindex="0">
      <table class="pf-divcal-table"><caption>${escapeHtml(_pfDivCalMonthLabel(month))} 배당 내역</caption>
        <thead><tr>${['종목', '배당기준일', '지급일', '배당락 여부', '주당 배당액', '수량', '배당총액', '세금', '지급액'].map((name, index) => `<th scope="col"${index >= 4 ? ' class="pf-divcal-number"' : ''}>${name}</th>`).join('')}</tr></thead>
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
    + ` · 지급 일정 기준 세전 계산 합계 <strong>${fmtKrw(summary.total_expected_krw || 0)}원</strong>`
    + ` · 공시 ${Number(summary.confirmed_count || 0)}건 / 예상 ${Number(summary.estimated_count || 0)}건 / 수집 이력 ${Number(summary.observed_count || 0)}건`
    + (summary.nh_count ? ` · NH 입금 연결 ${Number(summary.nh_count)}건${summary.nh_only_count ? `(일정 없는 입금 ${Number(summary.nh_only_count)}건)` : ''}` : '')
    + (summary.unconfirmed_count ? ` · NH 입금 미확인 ${Number(summary.unconfirmed_count)}건` : '')
    + (summary.not_held_count ? ` · 기준 시점 미보유 ${Number(summary.not_held_count)}건 제외` : '');
  const coverageHtml = coverage.length ? `<details class="pf-divcal-coverage"><summary>종목별 일정 확인 · 지급일 미확인 ${Number(summary.unknown_payment_count || 0)}종목${summary.stale_count ? ` · 갱신 미완료 ${Number(summary.stale_count)}종목` : ''}</summary>${coverage.map(c => `<div><strong>${escapeHtml(c.stock_name)}</strong> · ${escapeHtml(c.frequency_label)}${c.held === false ? ' · 매도' : ''} · ${c.has_payment_dates ? '지급일 수집' : '지급일 미확인'}${c.status !== 'fresh' ? ' · 갱신 필요' : ''}${c.fetched_at ? ` · 확인 ${escapeHtml(_pfDivCalCheckedDay(c.fetched_at))}` : ''}</div>`).join('')}</details>` : '';
  el.innerHTML = `<div class="pf-divcal-list">
    ${monthly.map((m) => _pfDivCalMonthHtml(m, eventsByMonth, todayMonth, todayIso)).join('')}
  </div>
  <div class="pf-chart-range">${totalLine}</div>
  <div class="pf-divcal-note">지급일의 증권사 배지는 실제 계좌 입금 확인을 뜻합니다. 세금·지급액은 계좌에서 확인된 값만 표시하며, 미확인은 0원이 아닙니다. 모바일에서는 표를 좌우로 이동할 수 있습니다.</div>
  <details class="pf-divcal-help pf-divcal-note"><summary>표의 날짜·금액 기준</summary><dl>
    <dt>배당기준일</dt><dd>회사가 이번 배당을 받을 주주를 정하는 날입니다. 매수 후 결제 기간이 있어 이날 매수한다고 배당을 받는 것은 아닙니다.</dd>
    <dt>지급일</dt><dd>입금이 확인되면 실제 계좌 입금일과 증권사 배지를 표시합니다. 입금 전에는 수집한 지급 일정이나 예상 날짜를 표시합니다. 일부 입금은 확인된 계좌 몫만 받은 상태입니다.</dd>
    <dt>배당락 여부</dt><dd>배당락 날짜가 지났으면 배당락, 앞으로면 예정으로 표시합니다. 배당락일부터 산 주식은 해당 회차 배당을 받을 권리가 없습니다. 날짜 자료가 없으면 미확인입니다.</dd>
    <dt>배당총액</dt><dd>계좌 세전 금액이 있으면 실제 금액을 표시하고, 없으면 주당 배당액 × 수량으로 계산합니다. 일부 입금 행의 실제 금액은 확인된 계좌 몫입니다.</dd>
    <dt>세금 / 지급액</dt><dd>세금은 실제 계좌 기록의 원천징수액, 지급액은 실제 계좌 입금액입니다. 해외 배당의 현지세와 원화 국내세는 통화를 구분합니다. 외화 지급액에서 원화 국내세가 별도로 차감될 수 있고, 후속 세금 정산은 세금 칸에서 펼쳐 봅니다.</dd>
    <dt>수량</dt><dd>지난 배당은 기준 시점(배당락 전 거래일 종가, 국내 기준일은 2거래일 전 종가)에 보유한 종목만 그때 수량으로 셉니다. 수량은 전 계좌 합산이며 미래·예상은 현재 수량입니다. 보유 기록 날짜와 추정 여부는 수량 칸의 근거에서 확인합니다.</dd>
    <dt>월 합계</dt><dd>기존 지급 일정의 세전 계산액과 일정에 연결되지 않은 실제 입금을 합산합니다. 배당락일·배당기준일만 있는 일정은 월 합계에서 제외됩니다. 표의 계좌 실제 금액·세후 지급액 합계와는 다를 수 있습니다. 월은 수집 일정의 대표 날짜 기준이며 입금 내역만 있는 행은 실제 입금월에 표시합니다.</dd>
    <dt>미확인</dt><dd>해당 날짜·금액 자료가 없다는 뜻입니다. 무배당이나 0원 확정을 뜻하지 않습니다. 공시는 일정의 출처, 예상은 과거 패턴의 추정으로 실제 입금 확인과 별개입니다.</dd>
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
