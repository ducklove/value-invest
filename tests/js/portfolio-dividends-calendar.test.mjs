// jsdom behavior test for static/js/portfolio-dividends-calendar.js
// (성과 탭 '배당 캘린더' 카드).
//
// 실제 소스(utils → store → render → dividends-calendar)를 브라우저와 같은
// 순서로 올리고 apiFetch 만 모킹해 검증한다: 월 행 렌더 + 합계 포맷(fmtKrw),
// 월 펼침 표(표준 컬럼, 지급일 증권사 배지, 계산 금액·계좌 수령액 툴팁), 빈 상태,
// 메모(재호출 시 재요청 없음), silent 오류 처리.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

const SOURCES = [
  read("static", "app-config.js"),
  read("static", "js", "utils.js"),
  read("static", "js", "portfolio-store.js"),
  read("static", "js", "portfolio-render.js"), // fmtKrw 는 utils.js 로 승격됨
  read("static", "js", "portfolio-dividends-calendar.js"),
];

// index.html 의 pfDivCalWrap 마크업과 동일한 구조.
const PANEL_HTML = `
  <div class="pf-nav-chart-wrap" id="pfDivCalWrap">
    <div class="pf-nav-header">
      <h3>배당 캘린더</h3>
      <span class="pf-chart-note">월별 예상 배당 현금흐름 · 보유수량 × 주당 배당</span>
    </div>
    <div id="pfDivCalContent" class="pf-divcal-content">
      <div class="pf-risk-empty">성과 탭을 열면 배당 캘린더를 불러옵니다.</div>
    </div>
  </div>`;

const FULL_PAYLOAD = {
  as_of: "2026-06-10",
  start_month: "2026-04",
  end_month: "2027-04",
  events: [
    { date: "2026-04-15", stock_code: "005930", stock_name: "삼성전자",
      label: "연간 배당 (예상)", type: "estimated", date_kind: "payment", pay_date: "2026-04-15", amount_per_share: 1500,
      currency: "KRW", shares: 10, expected_amount_krw: 15000, confirmed: false,
      calculated_gross_amount: 15000, calculated_tax_amount: 2310, calculated_net_amount: 12690 },
    { date: "2026-06-15", stock_code: "AAPL", stock_name: "Apple",
      label: "분기 배당 (예상)", type: "estimated", date_kind: "payment", pay_date: "2026-06-15", amount_per_share: 0.25,
      currency: "USD", shares: 5, expected_amount_krw: 1750, confirmed: false,
      calculated_gross_amount: 1.25, calculated_tax_amount: 0.18, calculated_net_amount: 1.07 },
    { date: "2026-06-26", stock_code: "005930", stock_name: "삼성전자",
      label: "배당기준일 (확정)", type: "record_date", amount_per_share: 361,
      currency: "KRW", shares: 10, expected_amount_krw: 3610, confirmed: true,
      dividend_basis_date: "2026-06-24", dividend_basis_rule: "krx_record_t2",
      calculated_gross_amount: 3610, calculated_tax_amount: 555, calculated_net_amount: 3055 },
  ],
  monthly: [
    { month: "2026-04", total_krw: 15000, count: 1 },
    { month: "2026-05", total_krw: 0, count: 0 },
    { month: "2026-06", total_krw: 1750, count: 2 },
  ],
  summary: { event_count: 3, confirmed_count: 1, estimated_count: 2, total_expected_krw: 16750 },
};

const EMPTY_PAYLOAD = {
  as_of: "2026-06-10", start_month: "2026-04", end_month: "2027-04",
  events: [],
  monthly: [{ month: "2026-04", total_krw: 0, count: 0 }],
  summary: { event_count: 0, confirmed_count: 0, estimated_count: 0, total_expected_krw: 0 },
};

function loadPanel(payload = FULL_PAYLOAD, { fail = false } = {}) {
  const dom = new JSDOM(`<!doctype html><html><body>${PANEL_HTML}</body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  w.fetch = () => Promise.reject(new Error("no raw fetch in test"));
  for (const src of SOURCES) {
    const script = w.document.createElement("script");
    script.textContent = src;
    w.document.body.appendChild(script);
  }
  const calls = [];
  w.apiFetch = (path) => {
    calls.push(path);
    if (fail) return Promise.resolve({ ok: false, status: 500, json: async () => ({}) });
    return Promise.resolve({ ok: true, status: 200, json: async () => payload });
  };
  return { w, calls };
}

test("월 행 렌더 — 합계는 fmtKrw 포맷, 이번 달 강조 + 기본 펼침", async () => {
  const { w, calls } = loadPanel();
  await w.pfLoadDividendCalendarPanel();

  assert.equal(calls.length, 1);
  assert.equal(calls[0], "/api/portfolio/dividend-calendar?months=12");

  const content = w.document.getElementById("pfDivCalContent");
  const monthRows = [...content.querySelectorAll(".pf-divcal-month")];
  assert.equal(monthRows.length, 3);
  assert.deepEqual(monthRows.map((r) => r.dataset.month), ["2026-04", "2026-05", "2026-06"]);

  // 4월: 1건 · 15,000원 / 5월(빈 달): '-' + empty 클래스(클릭 불가).
  assert.match(monthRows[0].textContent, /2026년 4월/);
  assert.match(monthRows[0].textContent, /1건 · 15,000원/);
  assert.ok(monthRows[1].classList.contains("empty"));
  assert.match(monthRows[1].textContent, /-/);
  // 이번 달(as_of 기준 2026-06)은 now 강조 + 기본 펼침.
  assert.ok(monthRows[2].classList.contains("now"));
  assert.match(monthRows[2].textContent, /이번 달/);
  const juneList = content.querySelector('[data-month-events="2026-06"]');
  assert.notEqual(juneList.style.display, "none");
  // 4월 목록은 접힌 상태로 시작.
  const aprilList = content.querySelector('[data-month-events="2026-04"]');
  assert.equal(aprilList.style.display, "none");

  // 요약 라인: 기간 + 합계 + 확정/예상 건수.
  const range = content.querySelector(".pf-chart-range");
  assert.match(range.textContent, /2026-04 ~ 2027-04/);
  assert.match(range.textContent, /16,750원/);
  assert.match(range.textContent, /공시 1건 \/ 예상 2건/);
  // 추정 휴리스틱 + 기준일 제외 안내문.
  assert.match(content.querySelector(".pf-divcal-help").textContent, /월 합계에서 제외/);
  const headers = [...juneList.querySelectorAll('thead th')].map(cell => cell.textContent);
  assert.deepEqual(headers, ['배당기준일', '종목', '지급일', '주당 배당액', '수량', '배당총액', '세금', '실 수령액']);
});

test('계좌 수령액과 세금이 달라도 표는 계산액을 쓰고 실제 수령액은 증권사 툴팁에만 있다', async () => {
  const paid = { date: '2026-09-20', date_kind: 'payment', pay_date: '2026-09-20', record_date: '2026-08-31',
    ex_date: '2026-08-28', dividend_basis_date: '2026-08-28', stock_code: '005930', stock_name: '삼성전자', confirmed: true,
    amount_per_share: 370, shares: 100, currency: 'KRW', expected_amount_krw: 37000,
    calculated_gross_amount: 37000, calculated_tax_rate: 15.4, calculated_tax_amount: 5698, calculated_net_amount: 31302,
    verification: 'nh_confirmed', paid_date: '2026-09-21',
    nh_match: { date: '2026-09-21', broker_name: 'NH', currency: 'KRW', gross_amount: 37400, tax_amount: 5750, net_amount: 31650 } };
  const zeroTax = { ...paid, stock_name: '다른 계좌', nh_match: { ...paid.nh_match, broker_name: '다른증권사', tax_amount: 0, net_amount: 37400 } };
  const pending = { ...paid, stock_name: '입금 전', verification: 'unconfirmed', nh_match: null, paid_date: null };
  const { w } = loadPanel({ as_of: '2026-09-30', events: [paid, zeroTax, pending], monthly: [{ month: '2026-09', count: 3 }], summary: {} });
  await w.pfLoadDividendCalendarPanel();
  const [row, otherRow, pendingRow] = [...w.document.querySelectorAll('.pf-divcal-event')];
  for (const current of [row, otherRow, pendingRow]) {
    assert.equal(current.firstElementChild.className, 'pf-divcal-record-date');
    assert.equal(current.querySelector('.pf-divcal-gross').textContent, '37,000원');
    assert.equal(current.querySelector('.pf-divcal-tax').textContent, '5,698원');
    assert.match(current.querySelector('.pf-divcal-tax').title, /15.4%/);
    assert.match(current.querySelector('.pf-divcal-net').textContent, /^31,302원/);
  }
  assert.match(row.querySelector('.pf-divcal-payment-date').textContent, /2026-09-21.*NH/);
  assert.equal(row.querySelectorAll('.broker').length, 1);
  assert.equal(row.querySelector('.pf-divcal-stock .broker'), null);
  assert.equal(row.querySelector('.broker').title, 'NH 계좌 수령액: 31,650원');
  assert.equal(row.querySelector('.broker').getAttribute('aria-label'), row.querySelector('.broker').title);
  assert.doesNotMatch(row.textContent, /31,650|37,400|5,750/);
  assert.equal(otherRow.querySelector('.broker').textContent, '다른증권사');
  assert.match(otherRow.querySelector('.broker').title, /37,400원/);
  assert.equal(pendingRow.querySelector('.broker'), null);
  assert.ok(pendingRow.querySelector('.js-pf-dividend-receipt'));
  assert.equal(row.querySelector('.js-pf-dividend-receipt'), null);
});

test('일부 계좌 수령도 같은 NH 배지를 쓰고 전 계좌 수량으로 계산한다', async () => {
  const event = { date: '2026-09-10', ex_date: '2026-09-10', dividend_basis_date: '2026-09-10', date_kind: 'ex_date', stock_name: '해외 종목',
    stock_code: 'AAA', currency: 'USD', amount_per_share: 0.21, shares: 15, verification: 'nh_partial', paid_date: '2026-09-16',
    calculated_gross_amount: 3.15, calculated_tax_rate: 15, calculated_tax_amount: 0.47, calculated_net_amount: 2.68,
    nh_match: { date: '2026-09-16', currency: 'USD', gross_amount: 2, tax_amount: 0.3, domestic_tax_krw: 300, net_amount: 1.7 } };
  const unverified = { ...event, stock_name: '계좌 기록 없음', verification: 'nh_confirmed', nh_match: null, paid_date: null };
  const { w } = loadPanel({ as_of: '2026-09-01', events: [event, unverified], monthly: [{ month: '2026-09', count: 2 }], summary: {} });
  await w.pfLoadDividendCalendarPanel();
  const [row, unverifiedRow] = [...w.document.querySelectorAll('.pf-divcal-event')];
  assert.equal(row.querySelector('.broker').className, 'pf-divcal-badge broker');
  assert.match(row.querySelector('.broker').title, /계좌 수령액: 1.7 USD.*원화 별도 세금 300원/);
  assert.equal(row.querySelector('.pf-divcal-gross').textContent, '3.15 USD');
  assert.equal(row.querySelector('.pf-divcal-tax').textContent, '0.47 USD');
  assert.match(row.querySelector('.pf-divcal-net').textContent, /^2.68 USD/);
  assert.equal(row.querySelector('.pf-divcal-record-date').textContent, '2026-09-10');
  assert.equal(row.querySelector('.pf-divcal-record-date .ex'), null);
  assert.ok(row.querySelector('.js-pf-dividend-receipt'));
  assert.equal(unverifiedRow.querySelector('.broker'), null);
});

test("이벤트 행 — 확정/예상 배지, 주당 × 수량 = 금액, 예상·임박 구분 클래스", async () => {
  const { w } = loadPanel();
  await w.pfLoadDividendCalendarPanel();
  const content = w.document.getElementById("pfDivCalContent");

  const june = [...content.querySelectorAll('[data-month-events="2026-06"] .pf-divcal-event')];
  assert.equal(june.length, 2);

  // AAPL 예상 행: USD per-share + 점선/흐림 클래스 + (미래라) upcoming.
  const aapl = june.find((row) => /Apple/.test(row.textContent));
  assert.ok(aapl.classList.contains("pf-divcal-est"));
  assert.ok(aapl.classList.contains("pf-divcal-upcoming"));
  assert.equal(aapl.querySelector(".pf-divcal-badge").textContent, "예상");
  assert.equal(aapl.querySelector('.pf-divcal-per-share').textContent, '0.25 USD');
  assert.equal(aapl.querySelector('.pf-divcal-quantity').textContent, '5주');
  assert.match(aapl.querySelector('.pf-divcal-gross').textContent, /1.25 USD/);
  assert.equal(aapl.querySelector(".pf-divcal-gross").textContent, "1.25 USD");

  // 삼성전자 확정 행: confirmed 배지 + est 클래스 없음.
  const ssec = june.find((row) => /삼성전자/.test(row.textContent));
  assert.ok(!ssec.classList.contains("pf-divcal-est"));
  assert.equal(ssec.querySelector(".pf-divcal-badge").textContent, "공시");
  assert.ok(ssec.querySelector(".pf-divcal-badge").classList.contains("confirmed"));
  assert.equal(ssec.querySelector('.pf-divcal-per-share').textContent, '361원');
  assert.equal(ssec.querySelector('.pf-divcal-quantity').textContent, '10주');
  assert.match(ssec.querySelector(".pf-divcal-gross").textContent, /3,610원/);

  // 과거(2026-04-15 < as_of) 이벤트는 upcoming 클래스 없음.
  const april = content.querySelector('[data-month-events="2026-04"] .pf-divcal-event');
  assert.ok(!april.classList.contains("pf-divcal-upcoming"));
});

test("월 행 클릭으로 이벤트 목록을 펼치고 접는다", async () => {
  const { w } = loadPanel();
  await w.pfLoadDividendCalendarPanel();
  const content = w.document.getElementById("pfDivCalContent");
  const aprilList = () => content.querySelector('[data-month-events="2026-04"]');

  assert.equal(aprilList().style.display, "none");
  w.pfDivCalToggleMonth("2026-04");
  assert.notEqual(aprilList().style.display, "none");
  w.pfDivCalToggleMonth("2026-04");
  assert.equal(aprilList().style.display, "none");
});

test("이벤트 없음 — 빈 상태 문구", async () => {
  const { w } = loadPanel(EMPTY_PAYLOAD);
  await w.pfLoadDividendCalendarPanel();

  const content = w.document.getElementById("pfDivCalContent");
  assert.equal(content.querySelectorAll(".pf-divcal-month").length, 0);
  assert.match(content.textContent, /보유 종목의 배당 정보가 수집되면 표시됩니다\./);
});

test("재호출은 메모로 재요청 없이 그리고, force 는 다시 가져온다", async () => {
  const { w, calls } = loadPanel();
  await w.pfLoadDividendCalendarPanel();
  await w.pfLoadDividendCalendarPanel();
  assert.equal(calls.length, 1);
  await w.pfLoadDividendCalendarPanel({ force: true });
  assert.equal(calls.length, 2);
});

test("요청 실패 시 토스트 없이 패널 안 안내 문구만 보인다(silent)", async () => {
  const { w } = loadPanel(FULL_PAYLOAD, { fail: true });
  let toasts = 0;
  w.showToast = () => { toasts += 1; };
  await w.pfLoadDividendCalendarPanel();

  assert.match(w.document.getElementById("pfDivCalContent").textContent, /불러오지 못했습니다/);
  assert.equal(toasts, 0);
});

test('배당락 날짜는 첫 컬럼 배지로 합치고 근거·출처·입금 상태 문구를 제거한다', async () => {
  const payload = structuredClone(FULL_PAYLOAD);
  Object.assign(payload.events[1], { date_precision: 'approximate', receiptable: false, basis_date: '2025-06-14' });
  Object.assign(payload.events[2], { type: 'payment', date_kind: 'payment', pay_date: '2026-06-26', ex_date: '2026-06-01',
    dividend_basis_date: '2026-06-01', dividend_basis_rule: 'ex_date', record_date: '2026-06-02',
    source: '공식 배당 이력', source_url: 'https://example.com/dividends',
    fetched_at: '2026-06-01T23:00:00Z', data_status: 'stale', holding_basis: 'snapshot', holding_as_of: '2026-05-29',
    held_now: false, verification: 'unconfirmed', source_key: '005930:ex_date:2026-06-01' });
  payload.summary.unconfirmed_count = 1;
  const { w } = loadPanel(payload);
  await w.pfLoadDividendCalendarPanel();
  const rows = [...w.document.querySelectorAll('[data-month-events="2026-06"] .pf-divcal-event')];
  assert.match(rows[0].textContent, /2026-06-15 전후/);
  assert.equal(rows[0].querySelectorAll('button').length, 0);
  assert.equal(rows[1].querySelector('.pf-divcal-record-date').textContent, '2026-06-01배당락');
  assert.equal(rows[1].querySelector('.pf-divcal-record-date .ex').textContent, '배당락');
  assert.equal(rows[1].querySelector('.pf-divcal-ex-status'), null);
  assert.equal(rows[1].querySelector('.pf-divcal-badge.sold').textContent, '매도');
  assert.equal((rows[1].textContent.match(/10주/g) || []).length, 1);
  assert.equal(rows[1].querySelector('button').dataset.dividendSource, '005930:ex_date:2026-06-01');
  const content = w.document.getElementById('pfDivCalContent');
  assert.equal(content.querySelectorAll('.pf-divcal-source, .pf-divcal-quantity-source').length, 0);
  assert.doesNotMatch(content.textContent, /근거|출처|자료 확인|입금 확인|입금 미확인|일부 입금/);
});

test('일정 수집이 실패해도 누락 종목을 빈 배당으로 숨기지 않는다', async () => {
  const payload = { ...EMPTY_PAYLOAD, coverage: [{ stock_code: 'SCHP', stock_name: 'SCHP', frequency_label: '비정기·자료 부족', status: 'unavailable', has_payment_dates: false }],
    summary: { unknown_payment_count: 1, stale_count: 1 } };
  const { w } = loadPanel(payload);
  await w.pfLoadDividendCalendarPanel();
  const content = w.document.getElementById('pfDivCalContent');
  assert.match(content.textContent, /지급일 미확인 1종목/);
  assert.match(content.textContent, /SCHP.*갱신 필요/);
});

test('수량 변경 뒤 캘린더를 열면 기존 합계 캐시를 다시 사용하지 않는다', async () => {
  const { w, calls } = loadPanel();
  await w.pfLoadDividendCalendarPanel();
  w.PfStore.items = [{ stock_code: 'AGNC', quantity: 20 }];
  await w.pfLoadDividendCalendarPanel();
  assert.equal(calls.length, 2);
});

test('일정 없는 NH 내역은 계산 입력을 추정하지 않고 수령액만 툴팁에 표시한다', async () => {
  const deposit = { date: '2026-09-03', date_kind: 'payment', pay_date: '2026-09-03', date_status: 'nh',
    stock_code: '83188.HK', stock_name: '<img src=x>', currency: 'CNY', amount_per_share: null, shares: null,
    calculated_gross_amount: null, calculated_tax_amount: null, calculated_net_amount: null,
    gross_amount: 120, tax_amount: 12, net_amount: 108, verification: 'nh_confirmed', receiptable: false,
    nh_match: { date: '2026-09-03', currency: 'CNY', gross_amount: 120, tax_amount: 12, net_amount: 108 } };
  const { w } = loadPanel({ as_of: '2026-09-10', events: [deposit],
    monthly: [{ month: '2026-09', count: 1, total_krw: 24000, nh_only_krw: 24000, calculated_total_krw: 0 }],
    summary: { total_expected_krw: 24000, calculated_total_krw: 0, nh_only_count: 1 } });
  await w.pfLoadDividendCalendarPanel();
  const row = w.document.querySelector('.pf-divcal-event');
  for (const column of ['record-date', 'per-share', 'quantity', 'gross', 'tax', 'net']) {
    assert.equal(row.querySelector(`.pf-divcal-${column}`).textContent, '미확인');
  }
  assert.equal(row.querySelector('.pf-divcal-payment-date .broker').textContent, 'NH');
  assert.equal(row.querySelector('.broker').title, 'NH 계좌 수령액: 108 CNY');
  assert.equal(row.querySelector('img'), null);
  assert.match(row.querySelector('.pf-divcal-stock').textContent, /<img src=x>/);
  assert.equal(row.querySelector('.js-pf-dividend-receipt'), null);
  assert.doesNotMatch(w.document.getElementById('pfDivCalContent').textContent, /108 CNY|120 CNY|24,000/);
});

test('국내 D-2 기준일을 먼저 표시하고 삼성전자우 미확정 금액과 명시된 0원을 구분한다', async () => {
  const pending = { date: '2026-09-30', record_date: '2026-09-30', stock_code: '005935', stock_name: '삼성전자우',
    dividend_basis_date: '2026-09-28', dividend_basis_rule: 'krx_record_t2',
    date_kind: 'record_date', date_status: 'announced', confirmed: true, amount_per_share: null, amount_status: 'unknown',
    shares: 100, holding_basis: 'snapshot', holding_as_of: '2026-09-23', verification: 'unconfirmed', receiptable: false,
    calculated_gross_amount: null, calculated_tax_amount: null, calculated_net_amount: null };
  const zero = { ...pending, stock_name: '명시된 0원', date_kind: 'payment', pay_date: '2026-09-30',
    amount_status: 'reported', amount_per_share: 0, verification: null,
    calculated_gross_amount: 0, calculated_tax_amount: 0, calculated_net_amount: 0 };
  const { w } = loadPanel({ as_of: '2026-09-30', events: [pending, zero], monthly: [{ month: '2026-09', count: 2, total_krw: 0 }], summary: {} });
  await w.pfLoadDividendCalendarPanel();
  const [row, zeroRow] = [...w.document.querySelectorAll('.pf-divcal-event')];
  assert.equal(row.firstElementChild.textContent, '2026-09-28');
  assert.match(row.firstElementChild.title, /공시 배당기준일 2026-09-30의 2거래일 전/);
  assert.equal(row.querySelector('.pf-divcal-per-share').textContent, '미확인');
  assert.equal(row.querySelector('.pf-divcal-quantity').textContent, '100주');
  assert.equal((row.textContent.match(/100주/g) || []).length, 1);
  assert.doesNotMatch(row.textContent, /0원/);
  assert.equal(row.querySelector('.pf-divcal-payment-date').textContent.trim(), '미확인');
  assert.equal(row.querySelector('.broker'), null);
  for (const column of ['per-share', 'gross', 'tax', 'net']) assert.equal(zeroRow.querySelector(`.pf-divcal-${column}`).textContent, '0원');
  assert.match(w.document.querySelector('.pf-divcal-help').textContent, /무배당이나 0원 확정을 뜻하지 않습니다/);
});
