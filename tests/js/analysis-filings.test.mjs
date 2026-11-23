// jsdom behavior tests for analysis-filings.js loadReports — 로컬 캐시 우선
// 렌더 → 네트워크 갱신 → 실패 폴백 분기를 검증한다. apiFetchJson 은 스파이로
// 바꿔치기하고(index.html 계약 순서로 로드한 classic script 전역이라 window
// 프로퍼티 덮어쓰기가 이긴다), 캐시는 실제 localStorage(loadReportCache/
// saveReportCache 경로)를 그대로 태운다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (p) => readFileSync(join(__dirname, "..", "..", "static", "js", p), "utf8");

const R1 = {
  date: "2025-06-20", title: "실적 개선 사이클 진입", firm: "증권A", firm_short: "증권A",
  analyst: "김애널", recommendation: "Buy", target_price: "90,000",
  pdf_url: "", source_url: "https://research.example/1",
};
const R2 = {
  date: "2025-05-10", title: "목표가 상향", firm: "증권B", firm_short: "증권B",
  analyst: "이애널", recommendation: "Hold", target_price: "85,000",
  pdf_url: "", source_url: "https://research.example/2",
};

// index.html 의 리포트 섹션 정적 마크업 사본(thead th 는 원본처럼 scope 없이 —
// 렌더 시점 scope="col" 보강을 검증하기 위함).
function buildWindow() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="coverageNote"></div>
    <div class="reports-section" id="reportsSection" style="display:none;">
      <span id="reportCount"></span>
      <div id="reportsLoading">리포트를 불러오는 중...</div>
      <table id="reportsTable" style="display:none;">
        <thead><tr><th>날짜</th><th>증권사</th><th>제목 / 요약</th><th>투자의견</th><th>목표가</th></tr></thead>
        <tbody id="reportsBody"></tbody>
      </table>
      <button id="reportsMore" style="display:none;"></button>
    </div>
  </body></html>`, { runScripts: "dangerously", url: "https://app.example.com/" });
  const w = dom.window;
  for (const src of [read("utils.js"), read("analysis-charts.js"), read("analysis-filings.js"), read("analysis-valuation.js"), read("analysis.js")]) {
    const s = w.document.createElement("script");
    s.textContent = src;
    w.document.body.appendChild(s);
  }
  // 목표가 오버레이는 차트 라이브러리에 닿으므로 스파이로 대체.
  const overlays = [];
  w._overlayTargetPrices = (reports) => { overlays.push(reports); };
  return { w, overlays };
}

function seedCache(w, stockCode, payload) {
  w.localStorage.setItem(`report_cache:${stockCode}`, JSON.stringify({ ...payload, savedAt: Date.now() }));
}

const rows = (w) => [...w.document.querySelectorAll("#reportsBody tr.report-row")];
const countText = (w) => w.document.getElementById("reportCount").textContent;

test("loadReports: 캐시 히트 시 즉시 렌더하고, 최신 리포트가 그대로면 전체 재조회를 생략한다", async () => {
  const { w, overlays } = buildWindow();
  seedCache(w, "005930", { latestReport: R1, reports: [R1, R2] });

  const calls = [];
  let resolveLatest;
  w.apiFetchJson = (path) => {
    calls.push(path);
    return new Promise((res) => { resolveLatest = res; });
  };

  const done = w.loadReports("005930");

  // 네트워크 응답 전(첫 await 이전)에 캐시가 이미 그려져 있어야 한다.
  assert.equal(w.document.getElementById("reportsTable").style.display, "table");
  assert.equal(w.document.getElementById("reportsLoading").style.display, "none");
  assert.equal(countText(w), "(최근 3년, 2건 · 캐시)");
  assert.equal(rows(w).length, 2);
  assert.equal(overlays.length, 1, "캐시 렌더 후 목표가 오버레이가 갱신돼야 함");
  assert.match(rows(w)[0].innerHTML, /실적 개선 사이클 진입/);
  assert.match(rows(w)[0].innerHTML, /badge-buy/);
  assert.match(rows(w)[1].innerHTML, /badge-hold/);

  // 네트워크 최신 1건 = 캐시와 동일 → 재렌더/전체 재조회 없이 종료.
  resolveLatest({ report: { ...R1 } });
  await done;
  assert.deepEqual([...calls], ["/api/reports/005930/latest"]);
  assert.equal(countText(w), "(최근 3년, 2건 · 캐시)");
  assert.equal(rows(w).length, 2);
});

test("loadReports: 캐시가 없으면 네트워크(최신 1건 → 전체)로 렌더하고 캐시에 저장한다", async () => {
  const { w, overlays } = buildWindow();
  const calls = [];
  w.apiFetchJson = async (path) => {
    calls.push(path);
    if (path.endsWith("/latest")) return { report: { ...R1 } };
    return { reports: [{ ...R1 }, { ...R2 }] };
  };

  await w.loadReports("005930");

  assert.deepEqual([...calls], ["/api/reports/005930/latest", "/api/reports/005930"]);
  assert.equal(w.document.getElementById("reportsTable").style.display, "table");
  assert.equal(w.document.getElementById("reportsLoading").style.display, "none");
  assert.equal(countText(w), "(최근 3년, 2건)");
  assert.equal(rows(w).length, 2);
  assert.match(rows(w)[0].innerHTML, /90,000원/);
  assert.ok(overlays.length >= 1, "전체 리포트 렌더 후 목표가 오버레이가 갱신돼야 함");
  assert.equal(overlays[overlays.length - 1].length, 2);

  // 다음 방문을 위한 로컬 캐시 저장.
  const saved = JSON.parse(w.localStorage.getItem("report_cache:005930"));
  assert.equal(saved.reports.length, 2);
  assert.equal(saved.latestReport.title, R1.title);
});

test("loadReports: 네트워크 실패 시 캐시된 최신 1건을 유지한다", async () => {
  const { w } = buildWindow();
  seedCache(w, "005930", { latestReport: R1 });
  w.apiFetchJson = async () => { throw new Error("네트워크 오류"); };

  await w.loadReports("005930");

  // 캐시 최신 1건이 지워지지 않고 남는다(에러로 빈 화면 금지).
  assert.equal(w.document.getElementById("reportsTable").style.display, "table");
  assert.equal(w.document.getElementById("reportsLoading").style.display, "none");
  assert.equal(countText(w), "(최신 1건만 표시)");
  assert.equal(rows(w).length, 1);
  assert.match(rows(w)[0].innerHTML, /실적 개선 사이클 진입/);
});

test("loadReports: 캐시도 없고 네트워크도 실패하면 에러 문구를 표시한다", async () => {
  const { w } = buildWindow();
  w.apiFetchJson = async () => { throw new Error("네트워크 오류"); };

  await w.loadReports("005930");

  const loading = w.document.getElementById("reportsLoading");
  assert.equal(loading.textContent, "리포트를 불러오지 못했습니다.");
  assert.equal(w.document.getElementById("reportsTable").style.display, "none");
  assert.equal(rows(w).length, 0);
});

test("renderReportsTable: 정적 thead th 에 scope=col 을 렌더 시점에 보강한다", async () => {
  const { w } = buildWindow();
  const ths = [...w.document.querySelectorAll("#reportsTable thead th")];
  assert.equal(ths.length, 5);
  assert.ok(ths.every((th) => !th.hasAttribute("scope")), "시작 상태는 index.html 원본처럼 scope 없음");

  w.apiFetchJson = async (path) => (path.endsWith("/latest") ? { report: { ...R1 } } : { reports: [{ ...R1 }] });
  await w.loadReports("005930");

  assert.ok(ths.every((th) => th.getAttribute("scope") === "col"), "렌더 후 모든 머리글이 scope=col");
});

// ── DART 공시 AI 리뷰: 블록 형식(blocks/v1) 본문 렌더 ──────────────────────
// index.html 의 #filingReviewSection 마크업 사본에 실제 utils/analysis-filings
// 스크립트를 태우고 renderFilingReview 결과 DOM 을 검사한다.
function buildReviewWindow() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div class="filing-review-section" id="filingReviewSection" style="display:none;">
      <span id="filingReviewMeta"></span>
      <button id="filingReviewGenerateBtn" type="button" style="display:none;"></button>
      <a id="filingReviewSource" href="#" style="display:none;">DART 원문</a>
      <div id="filingReviewStatus" role="status"></div>
      <div id="filingReviewCards"></div>
      <div class="filing-review-body" id="filingReviewBody"></div>
      <div id="filingReviewTrends"></div>
      <details id="filingReviewDetails" style="display:none;"><div id="filingReviewDetailsBody"></div></details>
    </div>
  </body></html>`, { runScripts: "dangerously", url: "https://app.example.com/" });
  const w = dom.window;
  for (const src of [read("vendor/marked.js"), read("vendor/purify.js"), read("utils.js"), read("analysis-filings.js")]) {
    const s = w.document.createElement("script");
    s.textContent = src;
    w.document.body.appendChild(s);
  }
  return w;
}

const BLOCKS = [
  { type: "card", label: "부채비율", value: "7.9%", tone: "good", detail: "개선" },
  { type: "trend", label: "부채비율", unit: "%", note: "", before: { label: "2025 말", value: 9.01 }, after: { label: "2026 3Q", value: 7.91 } },
  { type: "text", md: "자본총계 **66.65조 원**으로 확대" },
  { type: "section", title: "재무안정성·현금흐름" },
  { type: "table", columns: ["구분", "2025 말", "2026 3Q", "비고"], rows: [["부채총계", "2조 5,214억 원", "5조 2,729억 원", "증가"], ["부채비율", "9.01%", "**7.91%**", "우수"]], note: "단위 원" },
  { type: "section", title: "리스크" },
  { type: "bullets", items: ["커머스 매출 감소", "연대보증 의무"] },
  { type: "checklist", items: ["현금흐름표 확인", "매출 반등 여부"] },
  { type: "changes", items: ["부채비율 7.5% → 7.9%"] },
  { type: "limits", text: "손익계산서 본문 없음" },
];

function blockReview(overrides = {}) {
  return {
    stock_code: "402340",
    rcept_no: "r1",
    report_name: "분기보고서 (2026.09)",
    report_date: "2026-11-14",
    review_md: "## 재무안정성·현금흐름\n(파생 마크다운)",
    review: {
      format: "blocks/v1",
      blocks: BLOCKS,
      truncated: false,
      cards: [{ label: "부채비율", value: "7.9%", tone: "good", detail: "개선" }],
      metric_trends: [BLOCKS[1]],
      source_limits: "손익계산서 본문 없음",
      summary_md: "(파생 마크다운)",
      ...overrides,
    },
  };
}

test("renderFilingReview: 블록 리뷰는 파생 마크다운이 아니라 블록으로 본문을 그린다", () => {
  const w = buildReviewWindow();
  w.renderFilingReview(blockReview(), { cached: true });
  const body = w.document.getElementById("filingReviewBody");

  assert.doesNotMatch(body.textContent, /파생 마크다운/);
  const lead = body.querySelector("p.frb-lead");
  assert.ok(lead, "첫 section 앞 text 는 리드 문단");
  assert.equal(lead.querySelector("strong").textContent, "66.65조 원");
  assert.deepEqual([...body.querySelectorAll("h3.frb-section")].map((h) => h.textContent),
    ["재무안정성·현금흐름", "리스크", "다음 체크포인트", "이전 보고서 대비 변화"]);
  assert.equal(body.querySelectorAll(".frb-bullets li").length, 2);
  assert.equal(body.querySelectorAll(".frb-checklist li").length, 2);
  assert.equal(body.querySelectorAll(".frb-changes li").length, 1);
  // card/trend/limits 는 본문에 다시 그리지 않는다(카드 줄·지표 차트·상세 영역 담당).
  assert.doesNotMatch(body.textContent, /손익계산서 본문 없음/);
  assert.equal(w.document.querySelectorAll("#filingReviewCards .filing-review-card.good").length, 1);
  assert.match(w.document.getElementById("filingReviewTrends").innerHTML, /mt-item/);
  assert.equal(body.querySelector(".frb-truncated"), null);
});

test("renderFilingReview: 표는 첫 열을 행 머리글로, 수치 열은 우측·설명 열은 좌측 정렬 클래스로 그린다", () => {
  const w = buildReviewWindow();
  w.renderFilingReview(blockReview());
  const table = w.document.querySelector("#filingReviewBody .frb-table-wrap table.frb-table");
  assert.ok(table);
  const head = [...table.querySelectorAll("thead th")];
  assert.ok(head.every((th) => th.getAttribute("scope") === "col"));
  assert.deepEqual(head.map((th) => th.className), ["", "frb-num", "frb-num", "frb-text"]);
  const firstRow = table.querySelector("tbody tr");
  assert.equal(firstRow.querySelector("th").getAttribute("scope"), "row");
  assert.equal(firstRow.querySelector("th").textContent, "부채총계");
  assert.equal(table.querySelectorAll("tbody tr")[1].querySelector("td.frb-num strong").textContent, "7.91%");
  assert.equal(w.document.querySelector("#filingReviewBody .frb-note").textContent, "단위 원");
});

test("renderFilingReview: 블록 문자열의 HTML 은 이스케이프되고 **굵게** 만 살아남는다", () => {
  const w = buildReviewWindow();
  const blocks = [
    { type: "text", md: '<img src=x onerror="window.__xss=1">**<b>굵게</b>**' },
    { type: "section", title: "<script>t</script>" },
    { type: "table", columns: ["<i>a</i>", "b"], rows: [["<a href='javascript:1'>x</a>", "1"]] },
    { type: "bullets", items: ["<svg onload=alert(1)>"] },
  ];
  w.renderFilingReview(blockReview({ blocks }));
  const body = w.document.getElementById("filingReviewBody");
  assert.equal(body.querySelectorAll("img, script, svg, a, b, i").length, 0);
  assert.equal(body.querySelector("strong").textContent, "<b>굵게</b>");
  assert.match(body.querySelector("h3").textContent, /<script>t<\/script>/);
  assert.equal(w.__xss, undefined);
});

test("renderFilingReview: 잘린 블록 리뷰는 본문 끝에 잘림 안내를 붙인다", () => {
  const w = buildReviewWindow();
  w.renderFilingReview(blockReview({ truncated: true }));
  const notice = w.document.querySelector("#filingReviewBody .frb-truncated");
  assert.ok(notice);
  assert.match(notice.textContent, /뒷부분 일부가 빠졌을 수 있습니다/);
});

test("renderFilingReview: 블록 형식 이전 리뷰는 기존처럼 마크다운 본문을 렌더한다", () => {
  const w = buildReviewWindow();
  w.renderFilingReview({
    stock_code: "005930",
    report_name: "분기보고서 (2026.03)",
    review_md: "## 요약\n\n| 구분 | 값 |\n|---|---|\n| 매출 | 10조 |",
    review: { summary_md: "## 요약", cards: [{ label: "매출", value: "증가", tone: "good" }] },
  });
  const body = w.document.getElementById("filingReviewBody");
  assert.equal(body.querySelector("h2").textContent, "요약");
  assert.equal(body.querySelectorAll("table td").length, 2);
  assert.equal(body.querySelector(".frb-section"), null);
  assert.equal(w.document.querySelectorAll("#filingReviewCards .filing-review-card").length, 1);
});

test("renderFilingReview: 블록이 비어 있으면 마크다운 본문으로 물러선다", () => {
  const w = buildReviewWindow();
  w.renderFilingReview(blockReview({ blocks: [{ type: "card", label: "x", value: "y" }] }));
  assert.match(w.document.getElementById("filingReviewBody").textContent, /파생 마크다운/);
});
