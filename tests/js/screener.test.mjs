// jsdom behavior tests for screener.js (밸류 스크리너, Labs):
// spec 로드 → 커버리지/필터 카드/정렬 컨트롤 렌더, 필터 직렬화(빈 값 제외),
// 결과 테이블 포맷(조/억·퍼센트·분석 링크), 페이저(오프셋 요청·페이지 이동),
// 빈 상태·spec 실패, 초기화 버튼을 admin.test.mjs 와 동일한 하니스
// (실제 스크립트 로드 + fetch 모킹)로 검증한다.

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
      <div class="screener-filters" id="screenerFilters"></div>
      <div class="screener-controls">
        <button class="bt-run" id="screenerRunBtn" type="button">스크린 실행</button>
        <button class="sc-btn-ghost" id="screenerResetBtn" type="button">초기화</button>
        <select id="screenerSortBy"></select>
        <select id="screenerSortDir">
          <option value="desc">내림차순</option>
          <option value="asc">오름차순</option>
        </select>
      </div>
      <div class="screener-status" id="screenerStatus" aria-live="polite"></div>
      <div class="screener-results" id="screenerResults">
        <div class="screener-empty">조건을 설정하고 '스크린 실행'을 누르세요.</div>
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
  // 페이저 클릭이 상단 복귀에 쓰는 scrollIntoView 는 jsdom 미구현 — no-op.
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

const SPEC_FIXTURE = {
  filters: {
    per: { label: "P/E" },
    roe: { label: "ROE" },
    dividend_yield: { label: "배당수익률" },
    market_cap: { label: "시가총액" },
  },
  sorts: ["market_cap", "per", "roe"],
  default_sort: "market_cap",
  default_sort_dir: "desc",
  max_limit: 500,
  coverage: { universe: 2450, valued: 2300, fundamentals: 2100 },
};

const ROW_SAMSUNG = {
  stock_code: "005930", corp_name: "삼성전자", close_price: 71400,
  per: 12.34, pbr: 1.23, roe: 8.9, dividend_yield: 2.1,
  operating_margin: 15.25, debt_ratio: 40.1, market_cap: 4267543, // 억 → 426.75조
};
const ROW_SMALL = {
  stock_code: "035720", corp_name: "카카오", close_price: 41250,
  per: null, pbr: 2.05, roe: -1.2, dividend_yield: 0,
  operating_margin: 3.4, debt_ratio: 55.9, market_cap: 850, // 억 단위 유지
};

function makeRows(count, offset = 0) {
  return Array.from({ length: count }, (_, i) => ({
    ...ROW_SAMSUNG,
    stock_code: String(100000 + offset + i),
    corp_name: `종목${offset + i}`,
  }));
}

// 실제 apiFetchJson 을 그대로 태우고 fetch 만 모킹한다. run 응답은 요청의
// offset 을 반영해 페이지네이션 시나리오를 지원한다.
function installFetch(w, { spec = SPEC_FIXTURE, run = null, failSpec = false } = {}) {
  const calls = [];
  w.fetch = async (url, init = {}) => {
    const raw = String(url);
    const path = raw.split("?")[0];
    const params = new w.URLSearchParams(raw.split("?")[1] || "");
    calls.push({ path, params, init });
    if (path === "/api/screener/spec") {
      if (failSpec) {
        return { ok: false, status: 500, statusText: "Internal Server Error", json: async () => ({ detail: "spec 서버 오류" }) };
      }
      return { ok: true, status: 200, json: async () => structuredClone(spec) };
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

// (1) spec 렌더 — 커버리지 문구, 필터 카드(min/max + 단위 placeholder), 정렬 컨트롤.
test("spec 렌더: 커버리지·필터 카드·정렬 옵션이 spec 대로 그려지고 기본 정렬이 선택된다", async () => {
  await withDom(async (w) => {
    installFetch(w);
    await w.loadScreener();

    const coverage = w.document.getElementById("screenerCoverage");
    assert.match(coverage.textContent, /검색 대상: 2,450개 종목/);
    assert.match(coverage.textContent, /시세 데이터 보유 2,300개 \/ 재무 데이터 보유 2,100개/);

    const cards = [...w.document.querySelectorAll("#screenerFilters .screener-filter-card")];
    assert.equal(cards.length, 4);
    assert.deepEqual(cards.map((c) => c.dataset.filter), ["per", "roe", "dividend_yield", "market_cap"]);
    const perCard = cards[0];
    assert.equal(perCard.querySelector(".screener-filter-label").textContent, "P/E");
    // 단위 placeholder — FILTER_UNITS 보강(per → '배', market_cap → '억원').
    assert.equal(perCard.querySelector(".screener-filter-min").placeholder, "최소 배");
    assert.equal(perCard.querySelector(".screener-filter-max").placeholder, "최대 배");
    assert.equal(cards[3].querySelector(".screener-filter-min").placeholder, "최소 억원");

    const sortBy = w.document.getElementById("screenerSortBy");
    assert.deepEqual([...sortBy.options].map((o) => o.value), ["market_cap", "per", "roe"]);
    // 옵션 라벨은 필터 spec 의 label 을 재사용한다.
    assert.equal(sortBy.options[0].textContent, "시가총액");
    assert.equal(sortBy.value, "market_cap");
    assert.equal(w.document.getElementById("screenerSortDir").value, "desc");
  });
});

// (2) 필터 직렬화 — 채운 입력만 {key: {min/max}} 로 POST body 에 실린다.
test("필터 직렬화: 빈 입력은 제외되고 min/max 가 숫자로 직렬화되어 offset/정렬과 함께 전송된다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w, { run: { total: 1, offset: 0, rows: [ROW_SAMSUNG] } });
    await w.loadScreener();

    const input = (key, op) => w.document.querySelector(`#screenerFilters input[data-key="${key}"][data-op="${op}"]`);
    input("per", "max").value = "10";
    input("roe", "min").value = "5.5";
    input("roe", "max").value = "";       // 빈 값 — 제외
    input("dividend_yield", "min").value = ""; // 손대지 않은 카드 — 제외

    w.document.getElementById("screenerSortBy").value = "per";
    w.document.getElementById("screenerSortDir").value = "asc";
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => calls.some((c) => c.path === "/api/screener/run"));

    const call = calls.find((c) => c.path === "/api/screener/run");
    assert.equal(call.init.method, "POST");
    assert.deepEqual(JSON.parse(call.init.body), { filters: { per: { max: 10 }, roe: { min: 5.5 } } });
    assert.equal(call.params.get("sort_by"), "per");
    assert.equal(call.params.get("sort_dir"), "asc");
    assert.equal(call.params.get("limit"), "50");
    assert.equal(call.params.get("offset"), "0"); // 실행 버튼은 항상 1페이지부터
  });
});

// (3) 결과 테이블 — 포맷터(조/억·퍼센트·null → '-')와 분석 딥링크.
test("결과 테이블: 시총 조/억 표기·퍼센트 2자리·null 은 '-', 코드는 /analysis 링크가 된다", async () => {
  await withDom(async (w) => {
    installFetch(w, { run: { total: 2, offset: 0, rows: [ROW_SAMSUNG, ROW_SMALL] } });
    await w.loadScreener();
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));

    assert.equal(w.document.getElementById("screenerStatus").textContent, "총 2개 종목 일치");
    const rows = [...w.document.querySelectorAll("#screenerResults tbody tr")];
    assert.equal(rows.length, 2);

    const first = [...rows[0].children].map((td) => td.textContent.trim());
    // 코드, 종목명, 종가, P/E, P/B, ROE, 배당수익률, 영업이익률, 부채비율, 시총
    assert.deepEqual(first, [
      "005930", "삼성전자", "71,400", "12.34", "1.23",
      "8.90%", "2.10%", "15.25%", "40.10%", "426.75조",
    ]);
    const link = rows[0].querySelector("a.screener-code-link");
    assert.equal(link.getAttribute("href"), "/analysis?code=005930");

    const second = [...rows[1].children].map((td) => td.textContent.trim());
    assert.equal(second[3], "-");        // per null → '-'
    assert.equal(second[5], "-1.20%");   // 음수 ROE 도 2자리 고정
    assert.equal(second[9], "850억");    // 1조 미만은 억 표기
  });
});

// (4) 페이저 — 오프셋 요청, 페이지 정보, 다음/이전 버튼 이동.
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

    const runCalls = calls.filter((c) => c.path === "/api/screener/run");
    assert.equal(runCalls.length, 2);
    assert.equal(runCalls[1].params.get("offset"), "50");
    const pager2 = w.document.getElementById("screenerPager");
    assert.match(pager2.querySelector(".screener-pager-info").textContent, /51-100 \/ 123 \(페이지 2\/3\)/);
    // 가운데 페이지는 양방향 버튼 모두: « ‹ › ».
    assert.deepEqual([...pager2.querySelectorAll("button[data-page]")].map((b) => b.dataset.page), ["0", "0", "2", "2"]);
    assert.match(w.document.querySelector("#screenerResults tbody tr td").textContent, /100050/); // offset 반영 행
  });
});

// (5) 빈 상태 + spec 실패 — 안내 문구와 인라인 오류.
test("빈 상태: 결과 0건이면 안내 문구·페이저 비움, spec 실패는 필터 영역 인라인 오류", async () => {
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
});

// (6) 초기화 — 입력·결과·상태·페이저가 초기 화면으로 돌아간다.
test("초기화: 입력값을 비우고 결과/상태/페이저를 초기 안내로 되돌린다", async () => {
  await withDom(async (w) => {
    installFetch(w, {
      run: ({ offset }) => ({ total: 123, offset, rows: makeRows(Math.min(50, 123 - offset), offset) }),
    });
    await w.loadScreener();
    const perMax = w.document.querySelector('#screenerFilters input[data-key="per"][data-op="max"]');
    perMax.value = "10";
    w.document.getElementById("screenerRunBtn").click();
    await waitFor(() => w.document.querySelector("#screenerResults table"));

    w.document.getElementById("screenerResetBtn").click();
    assert.equal(perMax.value, "");
    assert.match(w.document.getElementById("screenerResults").textContent, /조건을 설정하고 '스크린 실행'을 누르세요/);
    assert.equal(w.document.getElementById("screenerStatus").textContent, "");
    assert.equal(w.document.getElementById("screenerPager").innerHTML, "");
  });
});
