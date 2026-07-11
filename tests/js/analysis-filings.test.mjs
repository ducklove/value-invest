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
  for (const src of [read("utils.js"), read("analysis-charts.js"), read("analysis-filings.js"), read("analysis.js")]) {
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
