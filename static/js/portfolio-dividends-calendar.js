// 배당 캘린더 — 성과 탭의 '배당 캘린더' 카드 (#pfRebalanceWrap 다음).
//
// GET /api/portfolio/dividend-calendar?months=12 (routes/dividend_calendar.py)
// 를 소비해 월별 예상 배당 현금흐름(월 행 + 합계)과 펼침식 이벤트 목록
// (날짜 · 종목 · 확정/예상 배지 · 주당 배당 × 보유수량 = 금액)을 렌더링한다.
//
// - lazy: 성과 탭이 처음 보일 때 pfSwitchTab(portfolio-performance.js)이
//   pfLoadDividendCalendarPanel() 을 호출한다. 응답은 인메모리 메모
//   (_pfDivCalData)는 사용자·수량·날짜가 같을 때 5분 이내에서만 재사용한다.
// - 지급일만 현금 합계에 포함한다. 배당락일과 기준일은 권리일 안내다.
// - NH 배당 입금: 연결된 일정은 'NH 입금 확인'(실제 입금일·세후·세금 정산), 연결할 일정이
//   없는 입금은 'NH 입금' 행(실제 금액, 수취 입력 없음)으로 그린다(docs/dividend-calendar.md).
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
  if (ev.date_status === 'nh') return `<span class="pf-divcal-badge nh deposit" title="${escapeHtml(_pfDivCalNhTitle(ev))}">NH 입금</span>`;
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

// NH 입금 요약(문자열, 호출부에서 escape): 'NH 입금일 2026-09-15 · 세후 12.5 USD · 국내세 300원 · 세금 정산 −55원'
// (같은 날 여러 입금이면 '입금 2건 합계')
function _pfDivCalNhLine(ev) {
  const m = ev.nh_match || {};
  const paid = ev.paid_date || m.date;
  const parts = [];
  if (paid) parts.push(`NH 입금일 ${paid}`);
  // 같은 날 여러 입금(다른 NH 계좌·추가 분배)이 한 일정에 붙으면 금액은 합계다.
  if (Array.isArray(m.parts) && m.parts.length > 1) parts.push(`입금 ${m.parts.length}건 합계`);
  if (ev.date_status === 'nh') {
    const gross = _pfDivCalMoney(m.gross_amount, m.currency);
    if (gross) parts.push(`세전 ${gross}`);
    if (m.tax_amount) parts.push(`현지세 ${_pfDivCalMoney(m.tax_amount, m.currency)}`);
  }
  const net = _pfDivCalMoney(m.net_amount, m.currency);
  if (net) parts.push(`세후 ${net}`);
  if (m.currency && m.currency !== 'KRW' && m.domestic_tax_krw) parts.push(`국내세 ${fmtKrw(Number(m.domestic_tax_krw))}원`);
  return parts.concat(_pfDivCalAdjustments(m)).join(' · ');
}

function _pfDivCalNhTitle(ev) {
  const line = _pfDivCalNhLine(ev);
  return line ? `NH 입금 확인 · ${line}` : 'NH 배당 입금 확인';
}

// NH 배당 입금 대조: 일정·배당금 확인 여부와 구분해 입금 상태를 명시한다.
function _pfDivCalVerify(ev) {
  if (ev.date_status === 'nh') return '';
  if (ev.verification === 'nh_confirmed') {
    return ` <span class="pf-divcal-badge nh" title="${escapeHtml(_pfDivCalNhTitle(ev))}">NH 입금 확인</span>`;
  }
  // 같은 종목을 NH 밖 계좌에도 보유: NH 계좌 몫만 확인, 나머지 몫은 미확인(수취 입력 유지).
  if (ev.verification === 'nh_partial') {
    const line = _pfDivCalNhLine(ev);
    return ` <span class="pf-divcal-badge nh partial" title="${escapeHtml(`NH 계좌 몫의 배당 입금만 확인했습니다. 다른 계좌 몫은 미확인입니다.${line ? ` (${line})` : ''}`)}">NH 일부 입금 확인</span>`;
  }
  if (ev.verification === 'unconfirmed') {
    const title = ev.date_kind === 'payment' ? '지급일이 지났지만 NH 배당 입금을 찾지 못했습니다.'
      : `${ev.date_kind === 'record_date' ? '기준일' : '배당락일'} 이후 대기 기간이 지났지만 NH 배당 입금을 찾지 못했습니다.`;
    return ` <span class="pf-divcal-badge unconfirmed" title="${escapeHtml(title)} 다른 증권사 입금 여부는 알 수 없습니다.">NH 입금 미확인</span>`;
  }
  return '';
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

// 수량은 계산 줄에 한 번만 표시하고, 이 줄에는 보유 기록의 날짜·추정 근거를 적는다.
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

function _pfDivCalDateKind(ev) {
  const kind = ev.date_kind || (ev.type === 'estimated' ? (ev.pay_date ? 'payment' : ev.ex_date ? 'ex_date' : ev.record_date ? 'record_date' : '') : ev.type);
  return { payment: '지급일', ex_date: '배당락일', record_date: '배당기준일' }[kind] || '일정';
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
  const classes = ['pf-divcal-event'];
  const nhOnly = ev.date_status === 'nh';
  if (!ev.confirmed && !nhOnly) classes.push('pf-divcal-est');
  if (nhOnly) classes.push('pf-divcal-nh');
  if (ev.date >= todayIso) classes.push('pf-divcal-upcoming');
  const hasShares = ev.shares !== null && ev.shares !== undefined;
  const unknownAmount = ev.amount_status === 'unknown' || ev.amount_per_share === null || ev.amount_per_share === undefined;
  const nativeGross = nhOnly ? _pfDivCalMoney(ev.gross_amount, ev.currency) : '';
  const amount = (ev.expected_amount_krw === null || ev.expected_amount_krw === undefined)
    ? (nativeGross ? escapeHtml(nativeGross) : nhOnly ? '원화 환산 미확인' : unknownAmount ? '배당금 미확인' : !hasShares ? '수량 미상' : '원화 환산 미확인') : `${fmtKrw(ev.expected_amount_krw)}원`;
  const frequency = { monthly: '월배당', quarterly: '분기배당', semiannual: '반기배당', annual: '연배당', irregular: '비정기 배당' }[ev.frequency];
  const amountDetail = unknownAmount ? '주당 배당금 미확인' : `주당 ${_pfDivCalPerShare(ev)}`;
  const detail = nhOnly
    ? 'NH 배당 입금 · 실제 입금 금액'
    : `${frequency ? `${frequency} · ` : ''}${amountDetail}${unknownAmount ? ' · 보유 ' : ' × '}${hasShares ? `${Number(ev.shares).toLocaleString()}주` : '수량 미상'}`;
  const nhLine = ev.nh_match ? _pfDivCalNhLine(ev) : '';
  const paymentNote = !nhOnly && _pfDivCalDateKind(ev) !== '지급일' && !nhLine
    ? '<span class="pf-divcal-sub pf-divcal-payment-note">지급일 미확인 · 월 합계 제외</span>' : '';
  const holding = nhOnly ? '' : _pfDivCalHolding(ev);
  const holdingTitle = holding ? _pfDivCalHoldingTitle(ev) : '';
  // 지금은 보유하지 않지만 기준 시점에 보유한 종목(매도 뒤 지급·지난 권리일).
  const sold = ev.held_now === false && !nhOnly
    ? ' <span class="pf-divcal-badge sold" title="지금은 보유하지 않지만 배당 기준 시점에 보유한 종목입니다.">매도</span>' : '';
  const receipt = ev.receiptable === false || ev.verification === 'nh_confirmed' ? ''
    : `<button type="button" class="pf-mini-btn pf-divcal-receipt js-pf-dividend-receipt" data-dividend-source="${escapeHtml(ev.source_key || `${ev.stock_code}:${ev.type}:${ev.date}`)}">수취 입력</button>`;
  return `<div class="${classes.join(' ')}">
    <span class="pf-divcal-date">${escapeHtml(ev.date)}${ev.date_precision === 'approximate' ? ' 전후' : ''}<span class="pf-divcal-sub pf-divcal-date-kind">${nhOnly ? 'NH 입금일' : _pfDivCalDateKind(ev)}</span></span>
    <div class="pf-divcal-stock">
      <span class="pf-divcal-stock-name">${escapeHtml(ev.stock_name || ev.stock_code)} ${_pfDivCalBadge(ev)}${_pfDivCalVerify(ev)}${sold}</span>
      <span class="pf-divcal-sub">${detail}</span>
      ${holding ? `<span class="pf-divcal-sub pf-divcal-holding"${holdingTitle ? ` title="${escapeHtml(holdingTitle)}"` : ''}>${escapeHtml(holding)}</span>` : ''}
      ${nhLine ? `<span class="pf-divcal-sub pf-divcal-nh-line">${escapeHtml(nhLine)}</span>` : ''}
      ${paymentNote}
      ${nhOnly ? '' : _pfDivCalSource(ev)}
    </div>
    <span class="pf-divcal-amount"><span class="pf-divcal-sub pf-divcal-amount-label">${nhOnly ? '실제 입금액(세전)' : '배당액(세전)'}</span>${amount}${receipt}</span>
  </div>`;
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
    <span class="pf-divcal-month-total">${events.length ? `${events.length}건 · ` : ''}${total}${monthRow.unconverted_count ? ' + 환산 미확인' : ''}${monthRow.quantity_unknown_count ? ' + 수량 미상' : ''}${monthRow.announced_krw > 0 || monthRow.estimated_krw > 0 || monthRow.nh_only_krw > 0 ? `<span class="pf-divcal-sub">공시 ${fmtKrw(monthRow.announced_krw || 0)}원 · 예상 ${fmtKrw(monthRow.estimated_krw || 0)}원${monthRow.nh_only_krw > 0 ? ` · NH 입금 ${fmtKrw(monthRow.nh_only_krw)}원` : ''}</span>` : ''}</span>
  </div>`;
  if (empty) return head;
  const list = `<div class="pf-divcal-events" data-month-events="${escapeHtml(month)}" style="display:${open ? '' : 'none'};">
    ${events.map((ev) => _pfDivCalEventHtml(ev, todayIso)).join('')}
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
    + ` · 지급일 기준 세전 합계 <strong>${fmtKrw(summary.total_expected_krw || 0)}원</strong>`
    + ` · 공시 ${Number(summary.confirmed_count || 0)}건 / 예상 ${Number(summary.estimated_count || 0)}건 / 수집 이력 ${Number(summary.observed_count || 0)}건`
    + (summary.nh_count ? ` · NH 입금 연결 ${Number(summary.nh_count)}건${summary.nh_only_count ? `(일정 없는 입금 ${Number(summary.nh_only_count)}건)` : ''}` : '')
    + (summary.unconfirmed_count ? ` · NH 입금 미확인 ${Number(summary.unconfirmed_count)}건` : '')
    + (summary.not_held_count ? ` · 기준 시점 미보유 ${Number(summary.not_held_count)}건 제외` : '');
  const coverageHtml = coverage.length ? `<details class="pf-divcal-coverage"><summary>종목별 일정 확인 · 지급일 미확인 ${Number(summary.unknown_payment_count || 0)}종목${summary.stale_count ? ` · 갱신 미완료 ${Number(summary.stale_count)}종목` : ''}</summary>${coverage.map(c => `<div><strong>${escapeHtml(c.stock_name)}</strong> · ${escapeHtml(c.frequency_label)}${c.held === false ? ' · 매도' : ''} · ${c.has_payment_dates ? '지급일 수집' : '지급일 미확인'}${c.status !== 'fresh' ? ' · 갱신 필요' : ''}${c.fetched_at ? ` · 확인 ${escapeHtml(_pfDivCalCheckedDay(c.fetched_at))}` : ''}</div>`).join('')}</details>` : '';
  el.innerHTML = `<div class="pf-divcal-list">
    ${monthly.map((m) => _pfDivCalMonthHtml(m, eventsByMonth, todayMonth, todayIso)).join('')}
  </div>
  <div class="pf-chart-range">${totalLine}</div>
  <div class="pf-divcal-note">날짜 아래에 지급일·배당락일·배당기준일을 구분해 표시합니다. 월 합계는 지급일이 있는 배당의 세전 금액과 일정에 연결되지 않은 NH 입금으로 계산합니다. 배당락일·배당기준일만 있는 행은 월 합계에서 제외됩니다.</div>
  <details class="pf-divcal-help pf-divcal-note"><summary>날짜와 확인 상태는 무슨 뜻인가요?</summary>
    <dl>
      <dt>배당락일</dt><dd>이날부터 산 주식은 해당 회차 배당을 받을 권리가 없습니다. 돈이 들어오는 날과는 다릅니다.</dd>
      <dt>배당기준일</dt><dd>회사가 이번 배당을 받을 주주를 정하는 날입니다. 매수 후 결제 기간이 있어, 이날 매수한다고 배당을 받는 것은 아닙니다.</dd>
      <dt>지급일 / NH 입금일</dt><dd>지급일은 자료에 기재된 배당 지급 날짜입니다. NH 입금일은 연결된 NH 계좌에서 실제 입금이 확인된 날짜로, 지급일과 다를 수 있습니다.</dd>
      <dt>지급일 미확인</dt><dd>배당락일이나 배당기준일만 수집되어 언제 지급하는지 알 수 없습니다.</dd>
      <dt>배당금 미확인</dt><dd>주당 배당액을 확인할 수 없어 금액을 계산하지 않습니다. 무배당이나 0원 확정을 뜻하지 않습니다.</dd>
      <dt>NH 입금 미확인</dt><dd>일정에 대응하는 NH 배당 입금 기록을 찾지 못했습니다. 배당금·일정의 확정 여부와 별개이며, 다른 증권사 입금 여부는 알 수 없습니다.</dd>
      <dt>공시 / 수집 이력 / 예상</dt><dd>공시는 공시 자료의 일정, 수집 이력은 외부에서 가져온 배당 이력입니다. 예상(점선·날짜 전후)은 과거 패턴으로 추정한 일정과 금액으로 바뀔 수 있습니다. 공시 배지 자체가 배당금 확정이나 입금 완료를 뜻하지 않습니다.</dd>
      <dt>보유 수량과 금액</dt><dd>지난 배당은 기준 시점(배당락 전 거래일 종가, 국내 기준일은 2거래일 전 종가)에 보유한 종목만 그때 수량으로 셉니다. 수량 근거는 전 계좌 합산 보유 기록이며, 첫 기록으로 추정하거나 다른 날 수량을 사용한 경우 표시합니다. 미래·예상 금액은 현재 수량 기준입니다. 금액은 세전이며 실제 수취액과 다를 수 있습니다. 지금은 보유하지 않는 종목은 '매도'로 표시합니다.</dd>
    </dl>
  </details>${coverageHtml}`;
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
