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
  if (ev.amount_per_share === null || ev.amount_per_share === undefined) return '-';
  if ((ev.currency || 'KRW') === 'KRW') return `${fmtKrw(ev.amount_per_share)}원`;
  return `${Number(ev.amount_per_share).toLocaleString(undefined, { maximumFractionDigits: 4 })} ${escapeHtml(ev.currency)}`;
}

function _pfDivCalBadge(ev) {
  if (ev.date_status === 'observed') return '<span class="pf-divcal-badge observed">수집 이력</span>';
  return ev.confirmed
    ? '<span class="pf-divcal-badge confirmed">공시</span>'
    : '<span class="pf-divcal-badge">예상</span>';
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
  const dates = [['배당락', ev.ex_date], ['기준', ev.record_date], ['지급', ev.pay_date]]
    .filter(([, day]) => day && day !== ev.date).map(([label, day]) => `${label} ${escapeHtml(day)}`).join(' · ');
  const stale = ev.data_status === 'stale' ? ' · 갱신 실패, 이전 자료' : '';
  const basis = ev.basis_date ? ` · ${escapeHtml(ev.basis_date)} 이력 기준` : '';
  const fetched = ev.fetched_at ? ` · 확인 ${escapeHtml(_pfDivCalCheckedDay(ev.fetched_at))}` : '';
  return `<span class="pf-divcal-sub">${dates}${dates && source ? ' · ' : ''}${source}${fetched}${basis}${stale}${ev.fx_source === 'stored' ? ' · 저장 환율' : ''}</span>`;
}

function _pfDivCalEventHtml(ev, todayIso) {
  const classes = ['pf-divcal-event'];
  if (!ev.confirmed) classes.push('pf-divcal-est');
  if (ev.date >= todayIso) classes.push('pf-divcal-upcoming');
  const shares = Number(ev.shares || 0);
  const amount = (ev.expected_amount_krw === null || ev.expected_amount_krw === undefined)
    ? '-' : `${fmtKrw(ev.expected_amount_krw)}원`;
  return `<div class="${classes.join(' ')}">
    <span class="pf-divcal-date">${escapeHtml(ev.date)}${ev.date_precision === 'approximate' ? ' 전후' : ''}</span>
    <span class="pf-divcal-stock">
      <span class="pf-divcal-stock-name">${escapeHtml(ev.stock_name || ev.stock_code)} ${_pfDivCalBadge(ev)}</span>
      <span class="pf-divcal-sub">${escapeHtml(ev.label || '')} · 주당 ${_pfDivCalPerShare(ev)} × ${shares.toLocaleString()}주</span>
      ${_pfDivCalSource(ev)}
    </span>
    <span class="pf-divcal-amount">${amount}${ev.receiptable === false ? '' : `<button type="button" class="pf-mini-btn pf-divcal-receipt js-pf-dividend-receipt" data-dividend-source="${escapeHtml(ev.source_key || `${ev.stock_code}:${ev.type}:${ev.date}`)}">수취 입력</button>`}</span>
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
    <span class="pf-divcal-month-total">${events.length ? `${events.length}건 · ` : ''}${total}${monthRow.unconverted_count ? ' + 환산 미확인' : ''}${monthRow.announced_krw > 0 || monthRow.estimated_krw > 0 ? `<span class="pf-divcal-sub">공시 ${fmtKrw(monthRow.announced_krw || 0)}원 · 예상 ${fmtKrw(monthRow.estimated_krw || 0)}원</span>` : ''}</span>
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
    + ` · 공시 ${Number(summary.confirmed_count || 0)}건 / 예상 ${Number(summary.estimated_count || 0)}건 / 수집 이력 ${Number(summary.observed_count || 0)}건`;
  const coverageHtml = coverage.length ? `<details class="pf-divcal-coverage"><summary>종목별 일정 확인 · 지급일 미확인 ${Number(summary.unknown_payment_count || 0)}종목${summary.stale_count ? ` · 갱신 미완료 ${Number(summary.stale_count)}종목` : ''}</summary>${coverage.map(c => `<div><strong>${escapeHtml(c.stock_name)}</strong> · ${escapeHtml(c.frequency_label)} · ${c.has_payment_dates ? '지급일 수집' : '지급일 미확인'}${c.status !== 'fresh' ? ' · 갱신 필요' : ''}${c.fetched_at ? ` · 확인 ${escapeHtml(_pfDivCalCheckedDay(c.fetched_at))}` : ''}</div>`).join('')}</details>` : '';
  el.innerHTML = `<div class="pf-divcal-list">
    ${monthly.map((m) => _pfDivCalMonthHtml(m, eventsByMonth, todayMonth, todayIso)).join('')}
  </div>
  <div class="pf-chart-range">${totalLine}</div>
  <div class="pf-divcal-note">공시 지급일을 우선하며, 예상(점선)은 최근 지급 패턴을 반복한 날짜 전후의 추정입니다. 월배당도 지급이 없는 달이나 같은 달 복수 지급이 있을 수 있습니다. 배당락일·기준일은 월 합계에서 제외됩니다. 금액은 세전·현재 보유 수량 기준이며 실제 수취·권리 수량·증권사 입금일과 다를 수 있습니다. 예상 건은 공시 후 수취 입력에 연결됩니다.</div>${coverageHtml}`;
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
