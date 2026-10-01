// jsdom behavior test for static/js/portfolio-dividends-calendar.js
// (성과 탭 '배당 캘린더' 카드).
//
// 실제 소스(utils → store → render → dividends-calendar)를 브라우저와 같은
// 순서로 올리고 apiFetch 만 모킹해 검증한다: 월 행 렌더 + 합계 포맷(fmtKrw),
// 월 펼침 이벤트 목록(확정/예상 배지, 주당 × 수량 = 금액), 빈 상태,
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
      label: "연간 배당 (예상)", type: "estimated", amount_per_share: 1500,
      currency: "KRW", shares: 10, expected_amount_krw: 15000, confirmed: false },
    { date: "2026-06-15", stock_code: "AAPL", stock_name: "Apple",
      label: "분기 배당 (예상)", type: "estimated", amount_per_share: 0.25,
      currency: "USD", shares: 5, expected_amount_krw: 1750, confirmed: false },
    { date: "2026-06-26", stock_code: "005930", stock_name: "삼성전자",
      label: "배당기준일 (확정)", type: "ex_date", amount_per_share: 361,
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
  assert.match(content.querySelector(".pf-divcal-note").textContent, /월 합계에서 제외/);
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
  assert.match(aapl.textContent, /주당 0\.25 USD × 5주/);
  assert.match(aapl.querySelector(".pf-divcal-amount").textContent, /1,750원/);

  // 삼성전자 확정 행: confirmed 배지 + est 클래스 없음.
  const ssec = june.find((row) => /배당기준일/.test(row.textContent));
  assert.ok(!ssec.classList.contains("pf-divcal-est"));
  assert.equal(ssec.querySelector(".pf-divcal-badge").textContent, "공시");
  assert.ok(ssec.querySelector(".pf-divcal-badge").classList.contains("confirmed"));
  assert.match(ssec.textContent, /주당 361원 × 10주/);
  assert.match(ssec.querySelector(".pf-divcal-amount").textContent, /3,610원/);

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
  assert.match(rows[1].textContent, /배당락 2026-06-01/);
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
  assert.equal(agnc.querySelector('.pf-divcal-badge.nh').textContent, 'NH 확인');
  assert.match(agnc.querySelector('.pf-divcal-badge.nh').title, /지급 2026-05-11 · 세후 1.02 USD/);
  assert.equal(agnc.querySelector('.js-pf-dividend-receipt'), null);
  assert.equal(ssec.querySelector('.pf-divcal-badge.unconfirmed').textContent, '미확인');
  assert.ok(ssec.querySelector('.js-pf-dividend-receipt'));
  // NH 밖 계좌에도 보유한 종목은 NH 몫만 확인 — 나머지 몫의 수취 입력은 그대로 둔다.
  const realty = rows.find(row => /리얼티인컴/.test(row.textContent));
  assert.equal(realty.querySelector('.pf-divcal-badge.nh.partial').textContent, 'NH 일부 확인');
  assert.ok(realty.querySelector('.js-pf-dividend-receipt'));
  assert.match(w.document.querySelector('.pf-chart-range').textContent, /지난 지급 미확인 1건/);
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
  const badge = aaa.querySelector('.pf-divcal-badge.nh');
  assert.equal(badge.textContent, 'NH 확인');
  assert.match(badge.title, /지급 2026-09-15 · 세후 21 AUD · 국내세 2,100원/);
  assert.match(aaa.querySelector('.pf-divcal-nh-line').textContent, /지급 2026-09-15 · 세후 21 AUD/);
  assert.match(aaa.querySelector('.pf-divcal-date').textContent, /2026-09-01/); // 배당락일은 그대로
  assert.equal(aaa.querySelector('.js-pf-dividend-receipt'), null);
  const googl = find('구글');
  assert.equal(googl.querySelector('.pf-divcal-badge.nh.partial').textContent, 'NH 일부 확인');
  assert.match(googl.querySelector('.pf-divcal-nh-line').textContent, /지급 2026-09-16 · 입금 2건 합계 · 세후 1.7 USD · 세금 정산 −55원/);
  assert.doesNotMatch(aaa.querySelector('.pf-divcal-nh-line').textContent, /건 합계/);
  assert.ok(googl.querySelector('.js-pf-dividend-receipt'));
  const eun = find('유로스탁');
  assert.equal(eun.querySelector('.pf-divcal-badge.unconfirmed').textContent, '미확인');
  assert.match(eun.querySelector('.pf-divcal-badge.unconfirmed').title, /배당락일 이후/);
  const deposit = rows.find(row => row.querySelector('.pf-divcal-badge.deposit'));
  assert.equal(deposit.querySelector('.pf-divcal-badge.deposit').textContent, 'NH 입금');
  assert.equal(deposit.querySelector('img'), null); // 종목명은 escape
  assert.match(deposit.textContent, /<img src=x/);
  assert.equal(deposit.querySelector('.js-pf-dividend-receipt'), null);
  assert.equal(deposit.querySelector('.pf-divcal-badge.nh:not(.deposit)'), null);
  assert.match(deposit.querySelector('.pf-divcal-nh-line').textContent, /세전 10 USD · 현지세 1.5 USD · 세후 8.5 USD/);
  assert.match(deposit.querySelector('.pf-divcal-amount').textContent, /14,000원/);
  assert.ok(!deposit.classList.contains('pf-divcal-est'));
  const head = w.document.querySelector('.pf-divcal-month[data-month="2026-09"]');
  assert.match(head.textContent, /NH 입금 14,000원/);
  assert.match(w.document.querySelector('.pf-chart-range').textContent, /NH 입금 연결 3건\(일정 없는 입금 1건\)/);
});
