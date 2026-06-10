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
  read("static", "js", "portfolio-render.js"), // fmtKrw 의 홈
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
  assert.match(range.textContent, /확정 1건 \/ 예상 2건/);
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
  assert.equal(ssec.querySelector(".pf-divcal-badge").textContent, "확정");
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
