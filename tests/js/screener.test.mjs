// jsdom behavior tests for screener.js (밸류 스크리너, Labs):
// spec 로드 → 커버리지 타일/히스토리 조건 칩/필터 카드 렌더, 필터 직렬화(빈 값 제외),
// 결과 테이블 포맷(조/억·퍼센트·증감·배지·분석 링크)과 연도별 실적 펼침,
// 열 헤더 정렬(종가 제외, 방향 토글), 조건 칩 즉시 실행, 히스토리 준비 중/불가 상태,
// 관심종목(별 토글·관심종목만 보기·비로그인), 페이저, 빈 상태·spec 실패, 초기화를
// admin.test.mjs 와 동일한 하니스(실제 스크립트 로드 + fetch 모킹)로 검증한다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

// screener.js 는 독립 뷰 — 의존은 utils.js(apiFetchJson/escapeHtml)뿐이다.
const SOURCES = [
  read("static", "js", "utils.js"),
  read("static", "js", "screener.js"),
];

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

// index.html 의 #screenerView 마크업 골격을 그대로 복제한다(id 계약).
function buildDom() {
  const dom = new JSDOM(`<!doctype html>
<html lang="ko"><body>
  <div id="screenerView" style="display:none;">
    <div class="screener-page">
      <div class="screener-coverage" id="screenerCoverage" aria-live="polite"></div>
      <section class="screener-panel">
        <div class="screener-conditions" id="screenerConditions"></div>
      </section>
      <section class="screener-panel">
        <div class="screener-filters" id="screenerFilters"></div>
      </section>
      <div class="screener-controls">
        <button class="bt-run" id="screenerRunBtn" type="button">스크린 실행</button>
        <button class="sc-btn-ghost" id="screenerResetBtn" type="button">초기화</button>
        <button class="screener-watch-toggle" id="screenerWatchOnlyBtn" type="button" aria-pressed="false" disabled>☆ 관심종목만 보기</button>
      </div>
      <div class="screener-status" id="screenerStatus" aria-live="polite"></div>
      <div class="screener-results" id="screenerResults">
        <div class="screener-empty">히스토리 조건을 고르거나 수치 조건을 넣고 '스크린 실행'을 누르세요.</div>
      </div>
      <div class="screener-pager" id="screenerPager"></div>
    </div>
  </div>
</body></html>`, {
    runScripts: "dangerously",
    url: "https://hub.example.com/screener",
  });
  const { window: w } = dom;
  w.requestAnimationFrame = (cb) => cb();
  // 페이저 클릭이 결과 상단 복귀에 쓰는 scrollIntoView 는 jsdom 미구현 — no-op.
  w.HTMLElement.prototype.scrollIntoView = () => {};
  for (const src of SOURCES) appendScript(w, src);
  return w;
}

async function withDom(fn) {
  const w = buildDom();
  try {
    return await fn(w);
  } finally {
    await new Promise((resolve) => setTimeout(resolve, 20));
    w.close();
  }
}

// 클릭 핸들러의 fire-and-forget 비동기 렌더를 기다린다.
async function waitFor(check, { timeout = 2000 } = {}) {
  const start = Date.now();
  for (;;) {
    if (check()) return;
    if (Date.now() - start > timeout) throw new Error("waitFor timeout");
    await new Promise((r) => setTimeout(r, 5));
  }
}

const HISTORY_READY = {
  status: "ready", built_at: "2026-11-14T06:29:13+09:00", refreshing: false, progress: null,
  covered: 2050, dividend_covered: 1136, latest_fiscal_year: 2025,
};

const SPEC_FIXTURE = {
  filters: {
    per: { label: "P/E", group: "밸류에이션", needs_history: false },
    dividend_yield: { label: "배당수익률", group: "밸류에이션", needs_history: false },
    market_cap: { label: "시가총액", group: "밸류에이션", needs_history: false },
    roe: { label: "ROE", group: "수익성·재무", needs_history: false },
    revenue_growth: { label: "매출 증가율", group: "성장 (전년 대비)", needs_history: true },
  },
  conditions: [
    { key: "turnaround", label: "영업이익 흑자 전환", group: "실적 추세", description: "직전 사업연도 영업손실 → 최근 사업연도 영업흑자 (턴어라운드)" },
    { key: "revenue_growth_3y", label: "매출 3년 연속 성장", group: "실적 추세", description: "최근 사업연도까지 3년 연속으로 매출이 전년보다 증가" },
    { key: "dividend_growth", label: "배당 증가", group: "배당", description: "최근 사업연도 주당배당금이 전년보다 증가 (전년에도 배당 지급)" },
  ],
  sorts: ["market_cap", "per", "roe"],
  default_sort: "market_cap",
  default_sort_dir: "desc",
  max_limit: 200,
  coverage: { universe: 2450, valued: 2300, fundamentals: 2100 },
  as_of: "2026-11-14",
  history: HISTORY_READY,
};

const ROW_SAMSUNG = {
  stock_code: "005930", corp_name: "삼성전자", market: "KOSPI", close_price: 71400,
  per: 12.34, pbr: 1.23, roe: 8.9, roe_change: 1.85, dividend_yield: 2.1,
  operating_margin: 15.25, debt_ratio: 40.1, market_cap: 4267543, // 억 → 426.75조
  dps: 1668, revenue_growth: 10.88, op_profit_growth: 33.23, dps_growth: 15.35,
  dps_growth_years: 2, revenue_growth_years: 3, profit_years: 5, turnaround_op: false, turnaround_net: false,
  history_fy: 2025,
  history: [
    { fy: 2024, revenue: 300870903000000, operating_profit: 32725961000000, net_income: 33621363000000, roe: 8.58, operating_margin: 10.88, debt_ratio: 28.68, dps: 1446 },
    { fy: 2025, revenue: 333605938000000, operating_profit: 43601051000000, net_income: 44260956000000, roe: 10.43, operating_margin: 13.07, debt_ratio: 30.78, dps: 1668 },
  ],
};
const ROW_SMALL = {
  stock_code: "035720", corp_name: "카카오", market: "KOSPI", close_price: 41250,
  per: null, pbr: 2.05, roe: -1.2, roe_change: null, dividend_yield: null,
  operating_margin: 3.4, debt_ratio: 55.9, market_cap: 850, // 억 단위 유지
  dps: null, revenue_growth: -4.2, op_profit_growth: null, dps_growth: null,
  dps_growth_years: 0, revenue_growth_years: 0, profit_years: 0, turnaround_op: true, turnaround_net: false,
  history_fy: 2025,
  history: [
    { fy: 2024, revenue: 800000000000, operating_profit: -30000000000, net_income: -50000000000, roe: -3.1, operating_margin: -3.75, debt_ratio: 60.2, dps: null },
    { fy: 2025, revenue: 766400000000, operating_profit: 26000000000, net_income: -19000000000, roe: -1.2, operating_margin: 3.4, debt_ratio: 55.9, dps: null },
  ],
};

function makeRows(count, offset = 0) {
  return Array.from({ length: count }, (_, i) => ({
    ...ROW_SAMSUNG,
    stock_code: String(100000 + offset + i),
    corp_name: `종목${offset + i}`,
  }));
}

// 실제 apiFetchJson 을 그대로 태우고 fetch 만 모킹한다. run 응답은 요청의
// offset 을 반영해 페이지네이션 시나리오를 지원한다. watchlist 는 로그인 사용자의
// 관심종목 코드(null 이면 비로그인), spec 은 값 또는 호출 횟수를 받는 함수.
function installFetch(w, { spec = SPEC_FIXTURE, run = null, failSpec = false, watchlist = [], failStar = false } = {}) {
  const calls = [];
  let specCalls = 0;
  w.fetch = async (url, init = {}) => {
    const raw = String(url);
    const path = raw.split("?")[0];
    const params = new w.URLSearchParams(raw.split("?")[1] || "");
    calls.push({ path, params, init });
    if (path === "/api/screener/spec") {
      if (failSpec) {
        return { ok: false, status: 500, statusText: "Internal Server Error", json: async () => ({ detail: "spec 서버 오류" }) };
      }
      specCalls += 1;
      const body = typeof spec === "function" ? spec(specCalls) : spec;
      return { ok: true, status: 200, json: async () => structuredClone(body) };
    }
    if (path === "/api/screener/watchlist") {
      const body = watchlist === null ? { authenticated: false, codes: [] } : { authenticated: true, codes: watchlist };
      return { ok: true, status: 200, json: async () => structuredClone(body) };
    }
    if (path.startsWith("/api/screener/watchlist/")) {
      if (failStar) {
        return { ok: false, status: 500, statusText: "Internal Server Error", json: async () => ({ detail: "저장 실패" }) };
      }
      const code = decodeURIComponent(path.split("/").pop());
      return { ok: true, status: 200, json: async () => ({ stock_code: code, starred: JSON.parse(init.body).starred }) };
    }
    if (path === "/api/screener/run") {
      const offset = Number(params.get("offset") || 0);
      const body = typeof run === "function" ? run({ offset, params, init }) : run;
      return { ok: true, status: 200, json: async () => structuredClone(body) };
    }
    return { ok: false, status: 404, statusText: "Not Found", json: async () => ({ detail: `missing ${path}` }) };
  };
  return calls;
}

const runCallsOf = (calls) => calls.filter((c) => c.path === "/api/screener/run");
const cellTexts = (row) => [...row.children].map((td) => td.textContent.replace(/\s+/g, " ").trim());

// (1) spec 렌더 — 커버리지 타일, 묶음별 필터 카드(min/max + 단위 placeholder), 조건 칩과 정의.
test("spec 렌더: 커버리지 타일·묶음별 필터 카드·정의가 보이는 히스토리 조건 칩이 그려진다", async () => {
  await withDom(async (w) => {
    installFetch(w);
    await w.loadScreener();

    const coverage = w.document.getElementById("screenerCoverage");
    const tiles = [...coverage.querySelectorAll(".screener-stat")].map((t) => [
      t.querySelector(".screener-stat-label").textContent, t.querySelector(".screener-stat-value").textContent,
    ]);
    assert.deepEqual(tiles, [["검색 대상", "2,450"], ["재무 데이터", "2,100"], ["연도별 실적", "2,050"], ["배당 이력", "1,136"]]);
    assert.match(coverage.textContent, /최근 사업연도 2025년까지/);
    assert.match(coverage.textContent, /시세 기준일 2026-11-14 · 연도별 실적·배당 수집일 2026-11-14/);

    const groups = [...w.document.querySelectorAll("#screenerFilters .screener-filter-group")];
    assert.deepEqual(groups.map((g) => g.querySelector(".screener-group-label").textContent), ["밸류에이션", "수익성·재무", "성장 (전년 대비)"]);
    const cards = [...w.document.querySelectorAll("#screenerFilters .screener-filter-card")];
    assert.deepEqual(cards.map((c) => c.dataset.filter), ["per", "dividend_yield", "market_cap", "roe", "revenue_growth"]);
    assert.equal(cards[0].querySelector(".screener-filter-label").textContent, "P/E");
    // 단위 placeholder — FILTER_UNITS 보강(per → '배', market_cap → '억원').
    assert.equal(cards[0].querySelector(".screener-filter-min").placeholder, "최소 배");
    assert.equal(cards[0].querySelector(".screener-filter-max").placeholder, "최대 배");
    assert.equal(cards[2].querySelector(".screener-filter-min").placeholder, "최소 억원");
    assert.equal(cards[4].querySelector(".screener-filter-min").disabled, false);

    const condGroups = [...w.document.querySelectorAll("#screenerConditions .screener-cond-group")];
    assert.deepEqual(condGroups.map((g) => g.querySelector(".screener-group-label").textContent), ["실적 추세", "배당"]);
    const chips = [...w.document.querySelectorAll("#screenerConditions .screener-cond")];
    assert.deepEqual(chips.map((c) => c.dataset.cond), ["turnaround", "revenue_growth_3y", "dividend_growth"]);
    // 조건의 정의가 칩 안에 그대로 보인다.
    assert.equal(chips[0].querySelector(".screener-cond-label").textContent, "영업이익 흑자 전환");
    assert.equal(chips[0].querySelector(".screener-cond-desc").textContent, "직전 사업연도 영업손실 → 최근 사업연도 영업흑자 (턴어라운드)");
    assert.ok(chips.every((c) => !c.disabled && c.getAttribute("aria-pressed") === "false"));
    assert.equal(w.document.querySelector("#screenerConditions .screener-notice"), null);
  });
});

// (2) 필터 직렬화 — 채운 입력만 {key: {min/max}} 로 POST body 에 실린다.
test("필터 직렬화: 빈 입력은 제외되고 min/max 가 숫자로 직렬화되며, Enter 로도 실행된다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, { run: { total: 1, offset: 0, rows: [ROW_SAMSUNG] } });
    await w.loadScreener();

    const input = (key, op) => w.document.querySelector(`#screenerFilters input[data-key="${key}"][data-op="${op}"]`);
    input("per", "max").value = "10";
    input("roe", "min").value = "5.5";
    input("roe", "max").value = "";       // 빈 값 — 제외
    input("dividend_yield", "min").value = ""; // 손대지 않은 카드 — 제외

    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => runCallsOf(calls).length === 1);

    const call = runCallsOf(calls)[0];
    assert.equal(call.init.method, "POST");
    assert.deepEqual(JSON.parse(call.init.body), { filters: { per: { max: 10 }, roe: { min: 5.5 } } });
    // 정렬 선택 상자는 없다 — 기본 정렬(시가총액 내림차순)로 시작한다.
    assert.equal(call.params.get("sort_by"), "market_cap");
    assert.equal(call.params.get("sort_dir"), "desc");
    assert.equal(call.params.get("limit"), "50");
    assert.equal(call.params.get("offset"), "0"); // 실행 버튼은 항상 1페이지부터

    input("roe", "min").dispatchEvent(new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));
    await waitFor(() => runCallsOf(calls).length === 2);
  });
});

// (3) 결과 테이블 — 포맷터(조/억·퍼센트·증감·null → '-')·배지·분석 딥링크.
test("결과 테이블: 조/억·퍼센트·부호 있는 증감률·null 은 '-', 추세 배지와 /analysis 링크", async () => {
  await withDom(async (w) => {
    installFetch(w, { run: { total: 2, offset: 0, rows: [ROW_SAMSUNG, ROW_SMALL] } });
    await w.loadScreener();
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));

    assert.equal(w.document.getElementById("screenerStatus").textContent, "총 2개 종목 일치");
    const heads = [...w.document.querySelectorAll("#screenerResults .screener-table > thead th")].map((th) => th.textContent.replace(/[▲▼]/g, "").trim());
    assert.deepEqual(heads, [
      "관심종목·상세", "코드", "종목명", "종가", "시가총액", "P/E", "P/B", "ROE", "영업이익률", "부채비율",
      "배당수익률", "주당배당금", "매출 증가율", "영업이익 증가율", "배당 증가율",
    ]);
    const rows = [...w.document.querySelectorAll("#screenerResults tbody tr.sc-row")];
    assert.equal(rows.length, 2);

    const first = cellTexts(rows[0]);
    assert.deepEqual(first.slice(1), [
      "005930", "삼성전자KOSPI배당↑ 2년매출↑ 3년5년 흑자", "71,400", "426.75조", "12.34", "1.23",
      "8.90%+1.9%p", "15.25%", "40.10%", "2.10%", "1,668", "+10.9%", "+33.2%", "+15.3%",
    ]);
    const link = rows[0].querySelector("a.screener-code-link");
    assert.equal(link.getAttribute("href"), "/analysis?code=005930");
    assert.deepEqual([...rows[0].querySelectorAll(".sc-badge")].map((b) => b.textContent), ["배당↑ 2년", "매출↑ 3년", "5년 흑자"]);
    // 증가=빨강(up), 감소=파랑(down) — 한국 시장 관행.
    assert.ok(rows[0].children[12].querySelector(".sc-up"));

    const second = cellTexts(rows[1]);
    assert.equal(second[5], "-");        // per null → '-'
    assert.equal(second[7], "-1.20%");   // 음수 ROE 도 2자리 고정, 변화량 없음
    assert.equal(second[4], "850억");    // 1조 미만은 억 표기
    assert.equal(second[10], "-");       // 배당 없음
    assert.equal(second[11], "-");
    assert.equal(second[12], "-4.2%");
    assert.ok(rows[1].children[12].querySelector(".sc-down"));
    assert.equal(second[13], "-");       // 전년 적자라 증가율 없음 → 대신 흑자전환 배지
    assert.deepEqual([...rows[1].querySelectorAll(".sc-badge")].map((b) => b.textContent), ["영업 흑자전환"]);
  });
});

// (4) 행 펼침 — 최근 사업연도 실적 표.
test("연도별 실적: ▸ 를 누르면 사업연도별 매출·이익·ROE·배당 표가 펼쳐지고 다시 누르면 접힌다", async () => {
  await withDom(async (w) => {
    installFetch(w, { run: { total: 2, offset: 0, rows: [ROW_SAMSUNG, ROW_SMALL] } });
    await w.loadScreener();
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));

    const details = [...w.document.querySelectorAll("#screenerResults tr.sc-detail-row")];
    assert.equal(details.length, 2);
    assert.ok(details.every((d) => d.hidden));

    const expand = w.document.querySelector('#screenerResults button[data-expand="0"]');
    expand.click();
    assert.equal(details[0].hidden, false);
    assert.equal(details[1].hidden, true);
    assert.equal(expand.getAttribute("aria-expanded"), "true");
    assert.match(details[0].textContent, /2025년까지/);
    const table = details[0].querySelector("table.sc-history");
    assert.deepEqual([...table.querySelectorAll(":scope > thead th")].map((th) => th.textContent), ["사업연도", "2024", "2025"]);
    const lines = [...table.querySelectorAll(":scope > tbody > tr")].map(cellTexts);
    assert.deepEqual(lines, [
      ["매출", "300.87조", "333.61조"],
      ["영업이익", "32.73조", "43.60조"],
      ["순이익", "33.62조", "44.26조"],
      ["영업이익률", "10.88%", "13.07%"],
      ["ROE", "8.58%", "10.43%"],
      ["부채비율", "28.68%", "30.78%"],
      ["주당배당금", "1,446원", "1,668원"],
    ]);

    // 적자는 음수 색으로, 없는 값은 '-'.
    w.document.querySelector('#screenerResults button[data-expand="1"]').click();
    const loss = details[1].querySelector("table.sc-history > tbody > tr:nth-child(2)");
    assert.deepEqual(cellTexts(loss), ["영업이익", "-300억", "260억"]);
    assert.ok(loss.children[1].querySelector(".sc-neg"));
    assert.deepEqual(cellTexts(details[1].querySelector("table.sc-history > tbody > tr:last-child")), ["주당배당금", "-", "-"]);

    expand.click();
    assert.equal(details[0].hidden, true);
    assert.equal(expand.getAttribute("aria-expanded"), "false");
  });
});

// (5) 헤더 정렬 — 종가만 빼고 모든 열이 정렬 버튼이다.
test("헤더 정렬: 종가를 뺀 모든 열이 정렬되고, 같은 열을 다시 누르면 방향이 바뀌며 1페이지로 돌아간다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, {
      run: ({ offset }) => ({ total: 123, offset, rows: makeRows(Math.min(50, 123 - offset), offset) }),
    });
    await w.loadScreener();
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));

    const sortable = () => [...w.document.querySelectorAll("#screenerResults .screener-table > thead th")]
      .map((th) => [th.textContent.replace(/[▲▼]/g, "").trim(), th.querySelector("button[data-sort]")?.dataset.sort || null]);
    assert.deepEqual(sortable().slice(1), [
      ["코드", "stock_code"], ["종목명", "corp_name"], ["종가", null], ["시가총액", "market_cap"], ["P/E", "per"],
      ["P/B", "pbr"], ["ROE", "roe"], ["영업이익률", "operating_margin"], ["부채비율", "debt_ratio"],
      ["배당수익률", "dividend_yield"], ["주당배당금", "dps"], ["매출 증가율", "revenue_growth"],
      ["영업이익 증가율", "op_profit_growth"], ["배당 증가율", "dps_growth"],
    ]);
    // 서버가 받는 정렬 키만 쓴다(spec.sorts 는 축약 fixture 라 실제 목록은 pytest 가 고정).
    const th = (key) => w.document.querySelector(`#screenerResults button[data-sort="${key}"]`).closest("th");
    assert.equal(th("market_cap").getAttribute("aria-sort"), "descending"); // 기본 정렬 표시
    assert.equal(th("per").getAttribute("aria-sort"), "none");

    // 2페이지로 간 뒤 P/E 헤더 → 내림차순, offset 0.
    w.document.querySelector('#screenerPager button[data-page="1"]').click();
    await waitFor(() => runCallsOf(calls).length === 2);
    await waitFor(() => /51-100/.test(w.document.getElementById("screenerPager").textContent));
    w.document.querySelector('#screenerResults button[data-sort="per"]').click();
    await waitFor(() => runCallsOf(calls).length === 3);
    let last = runCallsOf(calls).at(-1);
    assert.equal(last.params.get("sort_by"), "per");
    assert.equal(last.params.get("sort_dir"), "desc");
    assert.equal(last.params.get("offset"), "0");
    await waitFor(() => th("per").getAttribute("aria-sort") === "descending");
    assert.equal(th("market_cap").getAttribute("aria-sort"), "none");
    assert.equal(th("per").querySelector(".sc-sort-arrow").textContent, "▼");

    // 같은 열 다시 → 오름차순.
    w.document.querySelector('#screenerResults button[data-sort="per"]').click();
    await waitFor(() => runCallsOf(calls).length === 4);
    last = runCallsOf(calls).at(-1);
    assert.equal(last.params.get("sort_dir"), "asc");
    await waitFor(() => th("per").getAttribute("aria-sort") === "ascending");
    assert.equal(th("per").querySelector(".sc-sort-arrow").textContent, "▲");

    // 글자 열(종목명)은 오름차순부터.
    w.document.querySelector('#screenerResults button[data-sort="corp_name"]').click();
    await waitFor(() => runCallsOf(calls).length === 5);
    last = runCallsOf(calls).at(-1);
    assert.equal(last.params.get("sort_by"), "corp_name");
    assert.equal(last.params.get("sort_dir"), "asc");
  });
});

// (6) 히스토리 조건 칩 — 입력 없이 한 번 누르면 바로 실행.
test("히스토리 조건: 칩을 누르면 바로 실행되고, 조건·수치 필터가 함께 전송되며, 다 끄면 안내로 돌아간다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, {
      run: ({ init }) => ({ total: 1, offset: 0, rows: [ROW_SMALL], conditions: JSON.parse(init.body).conditions || [] }),
    });
    await w.loadScreener();
    const chip = (key) => w.document.querySelector(`#screenerConditions button[data-cond="${key}"]`);

    chip("turnaround").click();
    await waitFor(() => runCallsOf(calls).length === 1);
    assert.deepEqual(JSON.parse(runCallsOf(calls)[0].init.body), { filters: {}, conditions: ["turnaround"] });
    assert.equal(chip("turnaround").getAttribute("aria-pressed"), "true");
    assert.ok(chip("turnaround").classList.contains("is-active"));
    await waitFor(() => /총 1개 종목 일치 — 영업이익 흑자 전환/.test(w.document.getElementById("screenerStatus").textContent));

    w.document.querySelector('#screenerFilters input[data-key="roe"][data-op="min"]').value = "5";
    chip("dividend_growth").click();
    await waitFor(() => runCallsOf(calls).length === 2);
    assert.deepEqual(JSON.parse(runCallsOf(calls)[1].init.body), {
      filters: { roe: { min: 5 } }, conditions: ["turnaround", "dividend_growth"],
    });
    await waitFor(() => /영업이익 흑자 전환 · 배당 증가/.test(w.document.getElementById("screenerStatus").textContent));

    // 조건을 모두 끄고 수치 조건도 없으면 실행하지 않고 안내 문구로.
    w.document.querySelector('#screenerFilters input[data-key="roe"][data-op="min"]').value = "";
    chip("turnaround").click();
    await waitFor(() => runCallsOf(calls).length === 3);
    chip("dividend_growth").click();
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(runCallsOf(calls).length, 3);
    assert.match(w.document.getElementById("screenerResults").textContent, /히스토리 조건을 고르거나/);
    assert.equal(w.document.getElementById("screenerStatus").textContent, "");
  });
});

// (7) 히스토리 준비 중 → 준비 완료 / 불가.
test("히스토리 수집 중: 칩·히스토리 필터가 잠기고 진행률을 보여 주다가, 준비되면 자동으로 켜진다", async () => {
  await withDom(async (w) => {
    // 폴링 간격(8초)을 테스트에서는 즉시로 당긴다.
    const realSetTimeout = w.setTimeout.bind(w);
    w.setTimeout = (fn, ms, ...rest) => realSetTimeout(fn, Math.min(ms, 5), ...rest);
    const building = { ...SPEC_FIXTURE, history: { status: "building", built_at: null, refreshing: false, progress: 0.42 } };
    const calls = installFetch(w, { spec: (n) => (n >= 3 ? SPEC_FIXTURE : building) });
    await w.loadScreener();

    const chips = () => [...w.document.querySelectorAll("#screenerConditions .screener-cond")];
    assert.ok(chips().every((c) => c.disabled));
    assert.match(w.document.querySelector("#screenerConditions .screener-notice").textContent, /모으는 중입니다 \(42%\)/);
    const growthMin = () => w.document.querySelector('#screenerFilters input[data-key="revenue_growth"][data-op="min"]');
    assert.equal(growthMin().disabled, true);
    const perMax = w.document.querySelector('#screenerFilters input[data-key="per"][data-op="max"]');
    assert.equal(perMax.disabled, false);
    perMax.value = "9";
    // 준비 전 커버리지에는 히스토리 타일이 없다.
    assert.equal(w.document.querySelectorAll("#screenerCoverage .screener-stat").length, 2);

    await waitFor(() => chips().every((c) => !c.disabled));
    assert.equal(w.document.querySelector("#screenerConditions .screener-notice"), null);
    assert.equal(growthMin().disabled, false);
    assert.equal(w.document.querySelectorAll("#screenerCoverage .screener-stat").length, 4);
    // 다시 그려도 이미 넣은 값은 남는다.
    assert.equal(w.document.querySelector('#screenerFilters input[data-key="per"][data-op="max"]').value, "9");
    // 준비되면 더 묻지 않는다.
    const specCalls = calls.filter((c) => c.path === "/api/screener/spec").length;
    await new Promise((r) => setTimeout(r, 40));
    assert.equal(calls.filter((c) => c.path === "/api/screener/spec").length, specCalls);
  });

  await withDom(async (w) => {
    installFetch(w, { spec: { ...SPEC_FIXTURE, history: { status: "unavailable", built_at: null, refreshing: false, progress: null } } });
    await w.loadScreener();
    const notice = w.document.querySelector("#screenerConditions .screener-notice.is-warning");
    assert.match(notice.textContent, /히스토리 조건을 쓸 수 없습니다/);
    assert.ok([...w.document.querySelectorAll("#screenerConditions .screener-cond")].every((c) => c.disabled));
  });
});

// (8) 관심종목 — 별 버튼 한 번으로 담고 뺀다.
test("관심종목: 별을 누르면 서버에 저장되고 개수가 바뀌며, 실패하면 되돌린다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, { watchlist: ["035720"], run: { total: 2, offset: 0, rows: [ROW_SAMSUNG, ROW_SMALL] } });
    await w.loadScreener();
    const toggle = w.document.getElementById("screenerWatchOnlyBtn");
    assert.equal(toggle.disabled, false);
    assert.equal(toggle.querySelector(".screener-watch-count").textContent, "1");

    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));
    const star = (code) => w.document.querySelector(`#screenerResults button[data-star="${code}"]`);
    assert.equal(star("035720").getAttribute("aria-pressed"), "true");
    assert.equal(star("035720").textContent, "★");
    assert.equal(star("005930").getAttribute("aria-pressed"), "false");
    assert.equal(star("005930").textContent, "☆");

    star("005930").click();
    await waitFor(() => calls.some((c) => c.path === "/api/screener/watchlist/005930"));
    const put = calls.find((c) => c.path === "/api/screener/watchlist/005930");
    assert.equal(put.init.method, "PUT");
    assert.deepEqual(JSON.parse(put.init.body), { starred: true });
    await waitFor(() => toggle.querySelector(".screener-watch-count").textContent === "2");
    assert.equal(star("005930").getAttribute("aria-pressed"), "true");
    assert.equal(star("005930").getAttribute("aria-label"), "관심종목 해제");

    star("035720").click();
    await waitFor(() => toggle.querySelector(".screener-watch-count").textContent === "1");
    assert.deepEqual(JSON.parse(calls.filter((c) => c.path === "/api/screener/watchlist/035720").at(-1).init.body), { starred: false });
    assert.equal(star("035720").textContent, "☆");
  });

  await withDom(async (w) => {
    installFetch(w, { watchlist: [], failStar: true, run: { total: 1, offset: 0, rows: [ROW_SAMSUNG] } });
    await w.loadScreener();
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));
    const star = w.document.querySelector('#screenerResults button[data-star="005930"]');
    star.click();
    await waitFor(() => /저장 실패/.test(w.document.getElementById("screenerStatus").textContent));
    assert.equal(star.getAttribute("aria-pressed"), "false");
    assert.equal(star.textContent, "☆");
    assert.equal(star.disabled, false);
    assert.equal(w.document.querySelector("#screenerWatchOnlyBtn .screener-watch-count").textContent, "0");
  });
});

test("관심종목만 보기: 토글이 watchlist_only 로 바로 실행하고, 끄면 조건이 없을 때 안내로 돌아간다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, {
      watchlist: ["035720", "999999"],
      run: { total: 1, offset: 0, rows: [ROW_SMALL], watchlist_only: true, watchlist_missing: 1 },
    });
    await w.loadScreener();
    const toggle = w.document.getElementById("screenerWatchOnlyBtn");
    toggle.click();
    await waitFor(() => runCallsOf(calls).length === 1);
    assert.deepEqual(JSON.parse(runCallsOf(calls)[0].init.body), { filters: {}, watchlist_only: true });
    assert.equal(toggle.getAttribute("aria-pressed"), "true");
    await waitFor(() => /관심종목만 \(스크리너 대상 밖 1개 제외\)/.test(w.document.getElementById("screenerStatus").textContent));

    toggle.click();
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(runCallsOf(calls).length, 1);
    assert.equal(toggle.getAttribute("aria-pressed"), "false");
    assert.match(w.document.getElementById("screenerResults").textContent, /히스토리 조건을 고르거나/);
  });

  // 담은 종목이 없으면 담는 방법을 안내한다.
  await withDom(async (w) => {
    installFetch(w, { watchlist: [], run: { total: 0, offset: 0, rows: [], watchlist_only: true, watchlist_missing: 0 } });
    await w.loadScreener();
    w.document.getElementById("screenerWatchOnlyBtn").click();
    await waitFor(() => /관심종목이 아직 없습니다/.test(w.document.getElementById("screenerResults").textContent));
  });
});

test("비로그인: 관심종목만 보기는 잠기고, 별을 눌러도 저장 요청 없이 로그인 안내만 한다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, { watchlist: null, run: { total: 1, offset: 0, rows: [ROW_SAMSUNG] } });
    await w.loadScreener();
    const toggle = w.document.getElementById("screenerWatchOnlyBtn");
    assert.equal(toggle.disabled, true);
    assert.match(toggle.title, /로그인 후/);
    assert.equal(toggle.querySelector(".screener-watch-count"), null);

    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));
    w.document.querySelector('#screenerResults button[data-star="005930"]').click();
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(calls.filter((c) => c.path.startsWith("/api/screener/watchlist/")).length, 0);
    assert.match(w.document.getElementById("screenerStatus").textContent, /로그인 후 사용할 수 있습니다/);
  });
});

// (9) 페이저 — 오프셋 요청, 페이지 정보, 다음/이전 버튼 이동.
test("페이저: 총 123건이면 1-50/페이지 1/3 을 표시하고 › 클릭이 offset=50 으로 재조회한다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, {
      run: ({ offset }) => ({
        total: 123, offset,
        rows: makeRows(Math.min(50, 123 - offset), offset),
      }),
    });
    await w.loadScreener();
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerPager .screener-pager-info"));

    const pager = w.document.getElementById("screenerPager");
    assert.match(pager.querySelector(".screener-pager-info").textContent, /1-50 \/ 123 \(페이지 1\/3\)/);
    // 첫 페이지에는 이전(«/‹) 버튼이 없고 다음(›)과 마지막(») 만 있다.
    assert.deepEqual([...pager.querySelectorAll("button[data-page]")].map((b) => b.dataset.page), ["1", "2"]);

    pager.querySelector('button[data-page="1"]').click();
    await waitFor(() => /51-100/.test(w.document.getElementById("screenerPager").textContent));

    assert.equal(runCallsOf(calls).length, 2);
    assert.equal(runCallsOf(calls)[1].params.get("offset"), "50");
    const pager2 = w.document.getElementById("screenerPager");
    assert.match(pager2.querySelector(".screener-pager-info").textContent, /51-100 \/ 123 \(페이지 2\/3\)/);
    // 가운데 페이지는 양방향 버튼 모두: « ‹ › ».
    assert.deepEqual([...pager2.querySelectorAll("button[data-page]")].map((b) => b.dataset.page), ["0", "0", "2", "2"]);
    assert.match(w.document.querySelector("#screenerResults tbody tr.sc-row a.screener-code-link").textContent, /100050/); // offset 반영 행
  });
});

// (10) 빈 상태 + 실패 — 안내 문구와 인라인 오류.
test("빈 상태: 결과 0건이면 안내 문구·페이저 비움, spec·run 실패는 인라인 오류", async () => {
  await withDom(async (w) => {
    installFetch(w, { run: { total: 0, offset: 0, rows: [] } });
    await w.loadScreener();
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => /조건에 맞는 종목이 없습니다/.test(w.document.getElementById("screenerResults").textContent));

    assert.equal(w.document.getElementById("screenerStatus").textContent, "총 0개 종목 일치");
    assert.equal(w.document.getElementById("screenerPager").innerHTML, "");
  });

  // spec 5xx — loadScreener 가 필터 영역에 인라인 오류를 그린다.
  await withDom(async (w) => {
    installFetch(w, { failSpec: true });
    await w.loadScreener();
    const filters = w.document.getElementById("screenerFilters");
    assert.ok(filters.querySelector(".screener-empty.error"));
    assert.match(filters.textContent, /spec 서버 오류/);
  });

  // run 4xx(조건 없음 등) — 서버 메시지를 결과 영역에 그대로 보여 준다.
  await withDom(async (w) => {
    installFetch(w);
    await w.loadScreener();
    w.fetch = async () => ({ ok: false, status: 400, statusText: "Bad Request", json: async () => ({ detail: "필터·히스토리 조건을 하나 이상 고르세요." }) });
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults .screener-empty.error"));
    assert.match(w.document.getElementById("screenerResults").textContent, /하나 이상 고르세요/);
  });
});

// (11) 초기화 — 입력·조건·관심종목 토글·정렬·결과가 초기 화면으로 돌아간다.
test("초기화: 입력값·조건 칩·관심종목만 보기·정렬을 비우고 결과/상태/페이저를 초기 안내로 되돌린다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, {
      run: ({ offset }) => ({ total: 123, offset, rows: makeRows(Math.min(50, 123 - offset), offset) }),
    });
    await w.loadScreener();
    const perMax = w.document.querySelector('#screenerFilters input[data-key="per"][data-op="max"]');
    perMax.value = "10";
    const chip = w.document.querySelector('#screenerConditions button[data-cond="turnaround"]');
    chip.click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));
    w.document.querySelector('#screenerResults button[data-sort="roe"]').click();
    await waitFor(() => runCallsOf(calls).length === 2);
    w.document.getElementById("screenerWatchOnlyBtn").click();
    await waitFor(() => runCallsOf(calls).length === 3);

    w.document.getElementById("screenerResetBtn").click();
    assert.equal(perMax.value, "");
    assert.equal(chip.getAttribute("aria-pressed"), "false");
    assert.ok(!chip.classList.contains("is-active"));
    assert.equal(w.document.getElementById("screenerWatchOnlyBtn").getAttribute("aria-pressed"), "false");
    assert.match(w.document.getElementById("screenerResults").textContent, /히스토리 조건을 고르거나 수치 조건을 넣고 '스크린 실행'을 누르세요/);
    assert.equal(w.document.getElementById("screenerStatus").textContent, "");
    assert.equal(w.document.getElementById("screenerPager").innerHTML, "");

    // 초기화 뒤 다시 실행하면 조건 없이, 기본 정렬로 나간다.
    perMax.value = "8";
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => runCallsOf(calls).length === 4);
    const last = runCallsOf(calls).at(-1);
    assert.deepEqual(JSON.parse(last.init.body), { filters: { per: { max: 8 } } });
    assert.equal(last.params.get("sort_by"), "market_cap");
    assert.equal(last.params.get("sort_dir"), "desc");
  });
});
