// jsdom behavior test for static/js/portfolio-dividends-calendar.js
// (성과 탭 '배당 캘린더' 카드).
//
// 실제 소스(utils → store → render → dividends-calendar)를 브라우저와 같은
// 순서로 올리고 apiFetch 만 모킹해 검증한다: 월 행 렌더 + 합계 포맷(fmtKrw),
// 월 펼침 표(표준 컬럼, 지급일 증권사 배지, 실제 세금·입금액), 빈 상태,
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
      currency: "KRW", shares: 10, expected_amount_krw: 15000, confirmed: false },
    { date: "2026-06-15", stock_code: "AAPL", stock_name: "Apple",
      label: "분기 배당 (예상)", type: "estimated", date_kind: "payment", pay_date: "2026-06-15", amount_per_share: 0.25,
      currency: "USD", shares: 5, expected_amount_krw: 1750, confirmed: false },
    { date: "2026-06-26", stock_code: "005930", stock_name: "삼성전자",
      label: "배당기준일 (확정)", type: "record_date", amount_per_share: 361,
      currency: "KRW", shares: 10, expected_amount_krw: 3610, confirmed: true },
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
  assert.deepEqual(headers, ['종목', '배당기준일', '지급일', '배당락 여부', '주당 배당액', '수량', '배당총액', '세금', '지급액']);
});

test('입금이 확인된 행은 실제 세전·세금·지급액을 사용하고 증권사 배지는 지급일에만 붙는다', async () => {
  const paid = { date: '2026-09-20', date_kind: 'payment', pay_date: '2026-09-20', record_date: '2026-08-31',
    ex_date: '2026-08-28', stock_code: '005930', stock_name: '삼성전자', confirmed: true,
    amount_per_share: 370, shares: 100, currency: 'KRW', expected_amount_krw: 37000,
    verification: 'nh_confirmed', paid_date: '2026-09-21',
    nh_match: { date: '2026-09-21', broker_name: 'NH', currency: 'KRW', gross_amount: 37400, tax_amount: 5750, net_amount: 31650 } };
  const zeroTax = { ...paid, stock_name: '면세 입금', nh_match: { ...paid.nh_match, broker_name: '다른증권사', tax_amount: 0, net_amount: 37400 } };
  const pending = { ...paid, stock_name: '입금 전', verification: null, nh_match: null, paid_date: null };
  const { w } = loadPanel({ as_of: '2026-09-30', events: [paid, zeroTax, pending], monthly: [{ month: '2026-09', count: 3 }], summary: {} });
  await w.pfLoadDividendCalendarPanel();
  const [row, zeroRow, pendingRow] = [...w.document.querySelectorAll('.pf-divcal-event')];
  assert.match(row.querySelector('.pf-divcal-payment-date').textContent, /2026-09-21.*NH.*공시 2026-09-20/);
  assert.equal(row.querySelectorAll('.broker').length, 1);
  assert.equal(row.querySelector('.pf-divcal-stock .broker'), null);
  assert.match(row.querySelector('.pf-divcal-gross').textContent, /37,400원.*실제 세전/);
  assert.equal(row.querySelector('.pf-divcal-tax').textContent, '5,750원');
  assert.match(row.querySelector('.pf-divcal-net').textContent, /31,650원.*실제 입금/);
  assert.equal(zeroRow.querySelector('.pf-divcal-payment-date .broker').textContent, '다른증권사');
  assert.equal(zeroRow.querySelector('.pf-divcal-tax').textContent, '0원');
  assert.equal(pendingRow.querySelector('.broker'), null);
  assert.equal(pendingRow.querySelector('.pf-divcal-tax').textContent, '미확인');
  assert.match(pendingRow.querySelector('.pf-divcal-net').textContent, /^미확인/);
});

test('외화 일부 입금은 확인분·현지세·국내세를 구분하고 배당락 예정일을 표시한다', async () => {
  const event = { date: '2026-09-10', ex_date: '2026-09-10', date_kind: 'ex_date', stock_name: '해외 종목',
    stock_code: 'AAA', currency: 'USD', amount_per_share: 0.21, shares: 15, verification: 'nh_partial', paid_date: '2026-09-16',
    nh_match: { date: '2026-09-16', currency: 'USD', gross_amount: 2, tax_amount: 0.3, domestic_tax_krw: 300, net_amount: 1.7 } };
  const unverified = { ...event, stock_name: '근거 없는 확인 상태', verification: 'nh_confirmed', nh_match: null, paid_date: null };
  const { w } = loadPanel({ as_of: '2026-09-01', events: [event, unverified], monthly: [{ month: '2026-09', count: 2 }], summary: {} });
  await w.pfLoadDividendCalendarPanel();
  const [row, unverifiedRow] = [...w.document.querySelectorAll('.pf-divcal-event')];
  assert.match(row.querySelector('.pf-divcal-payment-date').textContent, /일부 입금/);
  assert.match(row.querySelector('.pf-divcal-gross').textContent, /^2 USD.*계좌 확인분/);
  assert.match(row.querySelector('.pf-divcal-tax').textContent, /현지 0.3 USD.*국내 300원/);
  assert.match(row.querySelector('.pf-divcal-net').textContent, /^1.7 USD.*국내세 별도/);
  assert.match(row.querySelector('.pf-divcal-ex-status').textContent, /예정.*2026-09-10/);
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
  assert.match(aapl.querySelector(".pf-divcal-gross").textContent, /1,750원/);

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

test('공시 지급일·배당락일·출처와 이전 자료를 표시하고 예상 수취 입력을 숨긴다', async () => {
  const payload = structuredClone(FULL_PAYLOAD);
  Object.assign(payload.events[1], { date_precision: 'approximate', receiptable: false, basis_date: '2025-06-14' });
  Object.assign(payload.events[2], { type: 'payment', date_kind: 'payment', pay_date: '2026-06-26', ex_date: '2026-06-01',
    record_date: '2026-06-01', source: '공식 배당 이력', source_url: 'https://example.com/dividends',
    fetched_at: '2026-06-01T23:00:00Z', data_status: 'stale', source_key: '005930:ex_date:2026-06-01' });
  const { w } = loadPanel(payload);
  await w.pfLoadDividendCalendarPanel();
  const rows = [...w.document.querySelectorAll('[data-month-events="2026-06"] .pf-divcal-event')];
  assert.match(rows[0].textContent, /2026-06-15 전후/);
  assert.equal(rows[0].querySelectorAll('button').length, 0);
  assert.match(rows[1].textContent, /배당락일 2026-06-01/);
  assert.match(rows[1].textContent, /확인 2026-06-02.*갱신 실패/);
  assert.equal(rows[1].querySelector('a').href, 'https://example.com/dividends');
  assert.equal(rows[1].querySelector('button').dataset.dividendSource, '005930:ex_date:2026-06-01');
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

test('지난 지급일은 NH 입금 확인/미확인 태그를 달고, NH 확인 건은 수취 입력을 숨긴다', async () => {
  const payload = structuredClone(FULL_PAYLOAD);
  payload.events = [
    { date: '2026-05-10', stock_code: 'AGNC', stock_name: 'AGNC', label: '월 배당 · 지급일', type: 'payment', date_kind: 'payment',
      amount_per_share: 0.12, currency: 'USD', shares: 10, expected_amount_krw: 1680, confirmed: true,
      source_key: 'AGNC:ex_date:2026-04-30', verification: 'nh_confirmed', nh_match: { date: '2026-05-11', net_amount: 1.02, currency: 'USD' } },
    { date: '2026-05-20', stock_code: '005930', stock_name: '삼성전자', label: '분기 배당 · 지급일', type: 'payment', date_kind: 'payment',
      amount_per_share: 361, currency: 'KRW', shares: 10, expected_amount_krw: 3610, confirmed: true,
      source_key: '005930:ex_date:2026-03-31', verification: 'unconfirmed', nh_match: null },
    { date: '2026-05-25', stock_code: 'O', stock_name: '리얼티인컴', label: '월 배당 · 지급일', type: 'payment', date_kind: 'payment',
      amount_per_share: 0.26, currency: 'USD', shares: 30, expected_amount_krw: 10920, confirmed: true,
      source_key: 'O:ex_date:2026-04-30', verification: 'nh_partial', nh_match: { date: '2026-05-26', net_amount: 2.2, currency: 'USD' } },
  ];
  payload.monthly = [{ month: '2026-05', total_krw: 16210, count: 3 }];
  payload.summary = { ...payload.summary, unconfirmed_count: 1 };
  const { w } = loadPanel(payload);
  await w.pfLoadDividendCalendarPanel();
  w.pfDivCalToggleMonth('2026-05');
  const rows = [...w.document.querySelectorAll('[data-month-events="2026-05"] .pf-divcal-event')];
  const agnc = rows.find(row => /AGNC/.test(row.textContent));
  const ssec = rows.find(row => /삼성전자/.test(row.textContent));
  assert.equal(agnc.querySelector('.pf-divcal-payment-date .pf-divcal-badge.broker').textContent, 'NH');
  assert.match(agnc.querySelector('.pf-divcal-payment-date').textContent, /2026-05-11.*NH/);
  assert.match(agnc.querySelector('.pf-divcal-net').textContent, /1.02 USD/);
  assert.equal(agnc.querySelector('.js-pf-dividend-receipt'), null);
  assert.match(ssec.querySelector('.pf-divcal-payment-date').textContent, /입금 미확인/);
  assert.equal(ssec.querySelector('.pf-divcal-badge.broker'), null);
  assert.ok(ssec.querySelector('.js-pf-dividend-receipt'));
  // NH 밖 계좌에도 보유한 종목은 NH 몫만 확인 — 나머지 몫의 수취 입력은 그대로 둔다.
  const realty = rows.find(row => /리얼티인컴/.test(row.textContent));
  assert.equal(realty.querySelector('.pf-divcal-payment-date .pf-divcal-badge.broker.partial').textContent, 'NH');
  assert.ok(realty.querySelector('.js-pf-dividend-receipt'));
  assert.match(w.document.querySelector('.pf-chart-range').textContent, /NH 입금 미확인 1건/);
});

test('해외 배당락 행에 연결된 NH 입금은 실제 지급일·세후·세금 정산을 보여 주고, 일정 없는 입금은 NH 입금 행이다', async () => {
  const payload = structuredClone(FULL_PAYLOAD);
  payload.as_of = '2026-10-01';
  payload.events = [
    { date: '2026-09-01', stock_code: 'AAA.AX', stock_name: '호주 단기채', label: '월배당 · 배당락일 · 지급일 미확인', type: 'ex_date',
      date_kind: 'ex_date', date_status: 'observed', ex_date: '2026-09-01', amount_per_share: 0.2, currency: 'AUD', shares: 100,
      expected_amount_krw: 19000, confirmed: false, cashflow: false, source_key: 'AAA.AX:ex_date:2026-09-01',
      verification: 'nh_confirmed', paid_date: '2026-09-15',
      nh_match: { date: '2026-09-15', net_amount: 21, currency: 'AUD', domestic_tax_krw: 2100, adjustments: [] } },
    { date: '2026-09-08', stock_code: 'GOOGL', stock_name: '구글', label: '분기배당 · 배당락일 · 지급일 미확인', type: 'ex_date',
      date_kind: 'ex_date', date_status: 'observed', amount_per_share: 0.21, currency: 'USD', shares: 15, expected_amount_krw: 4410,
      confirmed: false, cashflow: false, source_key: 'GOOGL:ex_date:2026-09-08', verification: 'nh_partial', paid_date: '2026-09-16',
      nh_match: { date: '2026-09-16', net_amount: 1.7, currency: 'USD', adjustments: [{ date: '2026-09-25', income_krw: -55, currency: 'USD' }],
        parts: [{ id: 3, net_amount: 1.2 }, { id: 4, net_amount: 0.5 }] } },
    { date: '2026-09-02', stock_code: 'EUN2.DE', stock_name: '유로스탁 50', label: '반기배당 · 배당락일 · 지급일 미확인', type: 'ex_date',
      date_kind: 'ex_date', date_status: 'observed', amount_per_share: 0.3, currency: 'EUR', shares: 10, expected_amount_krw: 4900,
      confirmed: false, cashflow: false, source_key: 'EUN2.DE:ex_date:2026-09-02', verification: 'unconfirmed', nh_match: null },
    { date: '2026-09-20', stock_code: 'XYZ', stock_name: '<img src=x onerror=alert(1)>', label: 'NH 배당 입금', type: 'payment',
      date_kind: 'payment', date_status: 'nh', currency: 'USD', gross_amount: 10, shares: null, expected_amount_krw: 14000,
      confirmed: false, cashflow: true, receiptable: false, source_key: null, verification: 'nh_confirmed', paid_date: '2026-09-20',
      nh_match: { date: '2026-09-20', gross_amount: 10, tax_amount: 1.5, net_amount: 8.5, currency: 'USD', adjustments: [] } },
  ];
  payload.monthly = [{ month: '2026-09', total_krw: 14000, announced_krw: 0, estimated_krw: 0, nh_only_krw: 14000, nh_only_count: 1, nh_count: 3, count: 4 }];
  payload.summary = { ...payload.summary, nh_count: 3, nh_only_count: 1, unconfirmed_count: 1 };
  const { w } = loadPanel(payload);
  await w.pfLoadDividendCalendarPanel();
  w.pfDivCalToggleMonth('2026-09');
  const rows = [...w.document.querySelectorAll('[data-month-events="2026-09"] .pf-divcal-event')];
  const find = (text) => rows.find(row => row.textContent.includes(text));
  const aaa = find('호주 단기채');
  const badge = aaa.querySelector('.pf-divcal-payment-date .pf-divcal-badge.broker');
  assert.equal(badge.textContent, 'NH');
  assert.match(badge.title, /NH 실제 계좌 입금 확인/);
  assert.match(aaa.querySelector('.pf-divcal-payment-date').textContent, /2026-09-15.*NH/);
  assert.match(aaa.querySelector('.pf-divcal-net').textContent, /21 AUD/);
  assert.match(aaa.querySelector('.pf-divcal-ex-status').textContent, /배당락.*2026-09-01/);
  assert.equal(aaa.querySelector('.pf-divcal-stock .broker'), null);
  assert.equal(aaa.querySelector('.js-pf-dividend-receipt'), null);
  const googl = find('구글');
  assert.equal(googl.querySelector('.pf-divcal-payment-date .broker.partial').textContent, 'NH');
  assert.match(googl.querySelector('.pf-divcal-payment-date').textContent, /2026-09-16.*일부 입금/);
  assert.match(googl.querySelector('.pf-divcal-net').textContent, /1.7 USD.*계좌 확인분/);
  assert.match(googl.querySelector('.pf-divcal-tax').textContent, /세금 정산 −55원/);
  assert.ok(googl.querySelector('.js-pf-dividend-receipt'));
  const eun = find('유로스탁');
  assert.match(eun.querySelector('.pf-divcal-payment-date').textContent, /미확인.*입금 미확인/);
  assert.equal(eun.querySelector('.broker'), null);
  const deposit = rows.find(row => row.classList.contains('pf-divcal-nh'));
  assert.equal(deposit.querySelector('.pf-divcal-payment-date .broker').textContent, 'NH');
  assert.equal(deposit.querySelector('img'), null);
  assert.match(deposit.textContent, /<img src=x/);
  assert.equal(deposit.querySelector('.js-pf-dividend-receipt'), null);
  assert.match(deposit.querySelector('.pf-divcal-tax').textContent, /현지 1.5 USD/);
  assert.match(deposit.querySelector('.pf-divcal-net').textContent, /8.5 USD/);
  assert.match(deposit.querySelector('.pf-divcal-gross').textContent, /10 USD.*실제 세전/);
  assert.equal(deposit.querySelector('.pf-divcal-record-date').textContent, '미확인');
  assert.equal(deposit.querySelector('.pf-divcal-ex-status').textContent, '미확인');
  assert.ok(!deposit.classList.contains('pf-divcal-est'));
  const head = w.document.querySelector('.pf-divcal-month[data-month="2026-09"]');
  assert.match(head.textContent, /NH 입금 14,000원/);
  assert.match(w.document.querySelector('.pf-chart-range').textContent, /NH 입금 연결 3건\(일정 없는 입금 1건\)/);
});

test('지난 배당은 기준 시점 보유 근거와 매도 태그를 보여 주고, 미래·예상 행에는 붙이지 않는다', async () => {
  const payload = structuredClone(FULL_PAYLOAD);
  payload.as_of = '2026-10-01';
  payload.events = [
    { date: '2026-08-03', stock_code: 'AAA.AX', stock_name: '호주 단기채', label: '월배당 · 배당락일 · 지급일 미확인', type: 'ex_date',
      date_kind: 'ex_date', date_status: 'observed', amount_per_share: 0.2, currency: 'AUD', shares: 100, expected_amount_krw: 19000,
      confirmed: false, holding_basis: 'earliest_snapshot', holding_as_of: '2026-03-31', quantity_as_of: '2026-06-30',
      reference_date: '2026-08-02', reference_rule: 'ex_date_prev_day', held_now: true, source_key: 'AAA.AX:ex_date:2026-08-03' },
    { date: '2026-09-08', stock_code: 'GOOGL', stock_name: '구글', label: '분기배당 · 배당락일 · 지급일 미확인', type: 'ex_date',
      date_kind: 'ex_date', date_status: 'observed', amount_per_share: 0.21, currency: 'USD', shares: 10, expected_amount_krw: 2940,
      confirmed: false, holding_basis: 'snapshot', holding_as_of: '2026-09-08', reference_date: '2026-09-08',
      reference_rule: 'ex_date_same_day', held_now: true, source_key: 'GOOGL:ex_date:2026-09-08' },
    { date: '2026-09-15', stock_code: 'O', stock_name: '<b>리얼티</b>', label: '월배당 · 지급일', type: 'payment', date_kind: 'payment',
      date_status: 'announced', amount_per_share: 0.27, currency: 'USD', shares: 30, expected_amount_krw: 11340, confirmed: true,
      cashflow: true, holding_basis: 'snapshot', holding_as_of: '2026-09-01', reference_date: '2026-09-01',
      reference_rule: 'ex_date_same_day', held_now: false, source_key: 'O:ex_date:2026-09-01' },
    { date: '2026-09-20', stock_code: '005930', stock_name: '삼성전자', label: '분기배당 · 지급일', type: 'payment', date_kind: 'payment',
      date_status: 'announced', amount_per_share: 370, currency: 'KRW', shares: null, expected_amount_krw: null, confirmed: true,
      cashflow: true, holding_basis: 'snapshot', holding_as_of: '2026-06-26', reference_date: '2026-06-26',
      reference_rule: 'krx_record_t2', held_now: true, quantity_unknown_reason: 'changed_before_record',
      source_key: '005930:ex_date:2026-06-30' },
    { date: '2026-09-28', stock_code: '000660', stock_name: 'SK하이닉스', label: '배당락일 · 지급일 미확인', type: 'ex_date',
      date_kind: 'ex_date', date_status: 'observed', amount_per_share: 375, currency: 'KRW', shares: 7, expected_amount_krw: 2625,
      confirmed: false, holding_basis: 'snapshot', holding_as_of: '2026-09-23', reference_date: '2026-09-27',
      reference_rule: 'ex_date_prev_day', held_now: true, source_key: '000660:ex_date:2026-09-28' },
    { date: '2026-10-09', stock_code: 'AGNC', stock_name: 'AGNC', label: '월배당 · 지급일', type: 'payment', date_kind: 'payment',
      date_status: 'announced', amount_per_share: 0.12, currency: 'USD', shares: 10, expected_amount_krw: 1680, confirmed: true,
      cashflow: true, holding_basis: 'current', held_now: true, source_key: 'AGNC:ex_date:2026-09-30' },
  ];
  payload.monthly = [{ month: '2026-08', total_krw: 0, count: 1 },
    { month: '2026-09', total_krw: 11340, count: 4, quantity_unknown_count: 1, unconverted_count: 0 },
    { month: '2026-10', total_krw: 1680, count: 1 }];
  payload.coverage = [{ stock_code: 'O', stock_name: '리얼티인컴', held: false, frequency_label: '월배당', status: 'fresh', has_payment_dates: true }];
  payload.summary = { ...payload.summary, not_held_count: 3 };
  const { w } = loadPanel(payload);
  await w.pfLoadDividendCalendarPanel();
  w.pfDivCalToggleMonth('2026-08');
  w.pfDivCalToggleMonth('2026-09');
  const rows = [...w.document.querySelectorAll('.pf-divcal-event')];
  const find = (text) => rows.find(row => row.textContent.includes(text));
  const holding = (row) => row.querySelector('.pf-divcal-holding');

  const googl = find('구글');
  assert.equal(holding(googl).textContent, '수량 근거: 9/8 보유 기록');
  // 미국 배당락: 기준 시점이 배당락일 당일이므로 '배당락 전 거래일'이라는 날짜로 쓰지 않는다.
  assert.match(holding(googl).title, /현지 배당락 전 거래일 종가 보유\(그 장은 배당락일 KST 정산에 반영\)/);
  assert.match(holding(googl).title, /2026-09-08 이하 마지막 장 마감 정산\(2026-09-08\)/);
  // 국내 배당락 9/28(월): 상한일 9/27(일)이 아니라 실제로 쓴 정산(9/23, 추석 전)을 보여 준다.
  const hynix = find('SK하이닉스');
  assert.equal(holding(hynix).textContent, '수량 근거: 9/23 보유 기록');
  assert.equal((hynix.textContent.match(/7주/g) || []).length, 1);
  assert.match(holding(hynix).title, /배당락 전 거래일 종가 보유\. 2026-09-27 이하 마지막 장 마감 정산\(2026-09-23\)/);
  assert.equal(googl.querySelector('.pf-divcal-badge.sold'), null);
  assert.equal(holding(find('호주 단기채')).textContent, '수량 근거: 첫 보유 기록(3/31)으로 추정 (수량 6/30 기록)');
  const realty = rows.find(row => row.querySelector('.pf-divcal-badge.sold'));
  assert.equal(realty.querySelector('.pf-divcal-badge.sold').textContent, '매도');
  assert.equal(realty.querySelector('b'), null); // 종목명은 escape
  assert.match(realty.textContent, /<b>리얼티<\/b>/);
  assert.equal(holding(realty).textContent, '수량 근거: 9/1 보유 기록');
  const samsung = find('삼성전자');
  assert.equal(samsung.querySelector('.pf-divcal-per-share').textContent, '370원');
  assert.match(samsung.querySelector('.pf-divcal-quantity').textContent, /미확인/);
  assert.equal(samsung.querySelector('.pf-divcal-gross').textContent, '미확인');
  assert.equal(holding(samsung).textContent, '수량 근거: 6/26 보유 기록 · 수량 미상(수량 기록 전 매매)');
  assert.match(holding(samsung).title, /배당기준일 2거래일 전 종가 보유/);
  // 수량을 모르는 지급 행은 환율 문제가 아니라 '수량 미상'으로 월 합계에 표시한다.
  const sept = w.document.querySelector('.pf-divcal-month[data-month="2026-09"] .pf-divcal-month-total').textContent;
  assert.match(sept, /\+ 수량 미상/);
  assert.doesNotMatch(sept, /환산 미확인/);
  // 미래 일정(현재 보유)은 보유 근거 줄이 없다.
  const agnc = w.document.querySelector('[data-month-events="2026-10"] .pf-divcal-event');
  assert.equal(holding(agnc), null);

  const content = w.document.getElementById('pfDivCalContent');
  assert.match(content.querySelector('.pf-chart-range').textContent, /기준 시점 미보유 3건 제외/);
  assert.match(content.querySelector('.pf-divcal-help').textContent, /기준 시점\(배당락 전 거래일 종가, 국내 기준일은 2거래일 전 종가\)에 보유한 종목만/);
  assert.match(content.querySelector('.pf-divcal-coverage').textContent, /리얼티인컴 · 월배당 · 매도/);
});

test('삼성전자우 배당금 미확인과 NH 입금 미확인은 구분하고, 확정 0원은 보존한다', async () => {
  const pending = { date: '2026-09-30', record_date: '2026-09-30', stock_code: '005935', stock_name: '삼성전자우',
    date_kind: 'record_date', date_status: 'announced', confirmed: true, frequency: 'quarterly',
    amount_per_share: null, amount_status: 'unknown', shares: 100, expected_amount_krw: null,
    holding_basis: 'snapshot', holding_as_of: '2026-09-23', verification: 'unconfirmed',
    source: 'KIS·예탁원 배당 일정', fetched_at: '2026-10-02T00:00:00Z', receiptable: true };
  const zero = { ...pending, stock_name: '지급액 0원 확인', date_kind: 'payment', pay_date: '2026-09-30',
    amount_status: 'reported', amount_per_share: 0, expected_amount_krw: 0, verification: null };
  const payload = { as_of: '2026-09-30', events: [pending, zero],
    monthly: [{ month: '2026-09', count: 2, total_krw: 0 }], summary: {} };
  const { w } = loadPanel(payload);
  await w.pfLoadDividendCalendarPanel();
  const [row, zeroRow] = [...w.document.querySelectorAll('.pf-divcal-event')];
  assert.equal(row.querySelector('.pf-divcal-record-date').textContent, '2026-09-30');
  assert.equal(row.querySelector('.pf-divcal-per-share').textContent, '미확인');
  assert.match(row.querySelector('.pf-divcal-quantity').textContent, /100주/);
  assert.equal((row.textContent.match(/100주/g) || []).length, 1);
  assert.doesNotMatch(row.textContent, /0원/);
  assert.equal(row.querySelector('.pf-divcal-gross').textContent, '미확인');
  assert.match(row.querySelector('.pf-divcal-payment-date').textContent, /미확인.*입금 미확인/);
  assert.equal(row.querySelector('.pf-divcal-payment-date .broker'), null);
  assert.match(row.querySelector('.pf-divcal-source summary').textContent, /자료 확인 2026-10-02/);
  assert.equal(row.querySelector('.pf-divcal-source').open, false);
  assert.equal(zeroRow.querySelector('.pf-divcal-per-share').textContent, '0원');
  assert.match(zeroRow.querySelector('.pf-divcal-gross').textContent, /0원/);
  assert.match(zeroRow.querySelector('.pf-divcal-payment-date').textContent, /2026-09-30/);
  assert.match(w.document.querySelector('.pf-divcal-help').textContent, /무배당이나 0원 확정을 뜻하지 않습니다/);
});
