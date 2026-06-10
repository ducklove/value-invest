// 배당 캘린더 — 성과 탭의 '배당 캘린더' 카드 (#pfRebalanceWrap 다음).
//
// GET /api/portfolio/dividend-calendar?months=12 (routes/dividend_calendar.py)
// 를 소비해 월별 예상 배당 현금흐름(월 행 + 합계)과 펼침식 이벤트 목록
// (날짜 · 종목 · 확정/예상 배지 · 주당 배당 × 보유수량 = 금액)을 렌더링한다.
//
// - lazy: 성과 탭이 처음 보일 때 pfSwitchTab(portfolio-performance.js)이
//   pfLoadDividendCalendarPanel() 을 호출한다. 응답은 인메모리 메모
//   (_pfDivCalData) — 서버도 사용자별 30분 캐시라 단순 메모로 충분.
// - 확정(ex_date, 배당기준일)은 기준일 안내일 뿐 현금 유입이 아니므로 월
//   합계에 포함되지 않는다(서버 규약). 예상 이벤트는 과거 배당 실적 기반
//   추정 — 점선/흐린 스타일로 확정과 시각적으로 구분한다.
// - 백그라운드 로드 오류는 reportApiError silent + 패널 내 안내.
// 포맷터(fmtKrw/escapeHtml)는 portfolio-render.js / utils.js 공용 헬퍼를
// 재사용한다 — 여기서 중복 정의하지 않는다.

let _pfDivCalData = null; // 마지막 GET /api/portfolio/dividend-calendar 페이로드 (메모)
let _pfDivCalLoadSeq = 0;
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
  if (ev.amount_per_share === null || ev.amount_per_share === undefined) return '-';
  if ((ev.currency || 'KRW') === 'KRW') return `${fmtKrw(ev.amount_per_share)}원`;
  return `${Number(ev.amount_per_share).toLocaleString(undefined, { maximumFractionDigits: 4 })} ${escapeHtml(ev.currency)}`;
}

function _pfDivCalBadge(ev) {
  return ev.confirmed
    ? '<span class="pf-divcal-badge confirmed">확정</span>'
    : '<span class="pf-divcal-badge">예상</span>';
}

function _pfDivCalEventHtml(ev, todayIso) {
  const classes = ['pf-divcal-event'];
  if (!ev.confirmed) classes.push('pf-divcal-est');
  if (ev.date >= todayIso) classes.push('pf-divcal-upcoming');
  const shares = Number(ev.shares || 0);
  const amount = (ev.expected_amount_krw === null || ev.expected_amount_krw === undefined)
    ? '-' : `${fmtKrw(ev.expected_amount_krw)}원`;
  return `<div class="${classes.join(' ')}">
    <span class="pf-divcal-date">${escapeHtml(ev.date)}</span>
    <span class="pf-divcal-stock">
      <span class="pf-divcal-stock-name">${escapeHtml(ev.stock_name || ev.stock_code)} ${_pfDivCalBadge(ev)}</span>
      <span class="pf-divcal-sub">${escapeHtml(ev.label || '')} · 주당 ${_pfDivCalPerShare(ev)} × ${shares.toLocaleString()}주</span>
    </span>
    <span class="pf-divcal-amount">${amount}</span>
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
    <span class="pf-divcal-month-total">${events.length ? `${events.length}건 · ` : ''}${total}</span>
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
  if (!events.length) {
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
    + ` · 예상 배당 합계 <strong>${fmtKrw(summary.total_expected_krw || 0)}원</strong>`
    + ` · 확정 ${Number(summary.confirmed_count || 0)}건 / 예상 ${Number(summary.estimated_count || 0)}건`;
  el.innerHTML = `<div class="pf-divcal-list">
    ${monthly.map((m) => _pfDivCalMonthHtml(m, eventsByMonth, todayMonth, todayIso)).join('')}
  </div>
  <div class="pf-chart-range">${totalLine}</div>
  <div class="pf-divcal-note">예상(점선) 이벤트는 최근 연간 배당 실적 기반 추정 일정입니다 — 국내 연 1회(4월), USD 분기, 기타 반기 가정. 배당기준일(확정)은 현금 유입일이 아니므로 월 합계에서 제외됩니다.</div>`;
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
  if (!force && _pfDivCalData) {
    _pfRenderDividendCalendar(_pfDivCalData);
    return;
  }
  const seq = ++_pfDivCalLoadSeq;
  _pfDivCalMsg('배당 캘린더를 불러오는 중입니다...');
  try {
    const resp = await apiFetch('/api/portfolio/dividend-calendar?months=12');
    if (resp.status === 401) {
      if (seq === _pfDivCalLoadSeq) _pfDivCalMsg('로그인 후 이용할 수 있습니다.');
      return;
    }
    if (!resp.ok) throw new Error(`배당 캘린더 요청 실패 (${resp.status})`);
    const data = await resp.json();
    _pfDivCalData = data;
    if (seq !== _pfDivCalLoadSeq) return;
    _pfRenderDividendCalendar(data);
  } catch (e) {
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
