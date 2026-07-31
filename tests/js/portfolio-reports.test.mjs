// jsdom behavior test for static/js/portfolio-reports.js (성과 탭 '기간 투자 보고서').
//
// 실제 소스(utils → store → render → reports)를 브라우저와 같은 순서로 올리고
// apiFetch 만 모킹해 검증한다: 연간 실적 목록의 펼침/접힘, 월 행 클릭 시
// 보고서 전환, 기간 종료 스냅샷 섹션의 토글과 구버전 보고서 폴백.

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
  read("static", "js", "portfolio-render.js"), // returnClass/fmtKrw 의 홈
  read("static", "js", "portfolio-reports.js"),
];

// index.html 의 pfPeriodReportWrap 마크업과 동일한 구조.
const PANEL_HTML = `
  <div class="pf-nav-chart-wrap pf-period-report-card" id="pfPeriodReportWrap">
    <div class="pf-nav-header pf-period-report-head">
      <div><h3>기간 투자 보고서</h3></div>
      <div class="pf-period-report-controls">
        <select id="pfPeriodReportType" onchange="pfPeriodReportTypeChanged(this.value)">
          <option value="monthly">월간</option>
          <option value="annual">연간</option>
        </select>
        <select id="pfPeriodReportKey" onchange="pfPeriodReportKeyChanged(this.value)"></select>
        <button class="pf-mini-btn" type="button" id="pfPeriodReportGenerateBtn" onclick="pfGeneratePeriodReport()">생성/갱신</button>
      </div>
    </div>
    <div class="pf-period-report-status" id="pfPeriodReportStatus"></div>
    <div id="pfPeriodReportContent" class="pf-period-report-content"></div>
  </div>
  <div class="pf-nav-chart-wrap pf-analysis-card pf-period-perf" id="pfPeriodPerformanceWrap">
    <div class="pf-nav-header"><h3>연간 실적</h3></div>
    <div id="pfPeriodPerformanceContent" class="pf-period-perf-content"></div>
  </div>`;

const PERIODS = {
  monthly: [
    { key: "2026-06", start_date: "2026-06-01", end_date: "2026-06-30", is_complete: true },
    { key: "2026-05", start_date: "2026-05-01", end_date: "2026-05-31", is_complete: true },
    { key: "2025-12", start_date: "2025-12-01", end_date: "2025-12-31", is_complete: true },
  ],
  annual: [
    { key: "2026", start_date: "2026-01-01", end_date: "2026-06-30", is_complete: false },
    { key: "2025", start_date: "2025-01-01", end_date: "2025-12-31", is_complete: true },
  ],
  defaults: { monthly: "2026-06", annual: "2026" },
  saved: [{ period_type: "monthly", period_key: "2026-06" }],
};

const PERFORMANCE = {
  anchor_date: "2026-07-01",
  years: [
    {
      type: "annual", key: "2026", start_date: "2026-01-01", end_date: "2026-06-30",
      is_complete: false, baseline_date: "2025-12-31", ending_date: "2026-06-30",
      baseline_mode: "previous_close", return_pct: 12.5,
      starting_value: 100000000, ending_value: 120000000, snapshot_count: 120,
      months: [
        {
          type: "monthly", key: "2026-06", start_date: "2026-06-01", end_date: "2026-06-30",
          is_complete: true, baseline_date: "2026-05-31", ending_date: "2026-06-30",
          baseline_mode: "previous_close", return_pct: 3.25,
          starting_value: 116000000, ending_value: 120000000, snapshot_count: 21,
        },
        {
          type: "monthly", key: "2026-05", start_date: "2026-05-01", end_date: "2026-05-31",
          is_complete: true, baseline_date: "2026-04-30", ending_date: "2026-05-31",
          baseline_mode: "previous_close", return_pct: -1.5,
          starting_value: 118000000, ending_value: 116000000, snapshot_count: 20,
        },
      ],
    },
    {
      type: "annual", key: "2025", start_date: "2025-01-01", end_date: "2025-12-31",
      is_complete: true, baseline_date: "2025-01-02", ending_date: "2025-12-31",
      baseline_mode: "first_snapshot_in_period", return_pct: -4.0,
      starting_value: 104000000, ending_value: 100000000, snapshot_count: 240,
      months: [
        {
          type: "monthly", key: "2025-12", start_date: "2025-12-01", end_date: "2025-12-31",
          is_complete: true, baseline_date: "2025-11-28", ending_date: "2025-12-31",
          baseline_mode: "previous_close", return_pct: 2.0,
          starting_value: 98000000, ending_value: 100000000, snapshot_count: 20,
        },
      ],
    },
  ],
};

const END_SNAPSHOT = {
  date: "2026-06-30",
  position_count: 2,
  total_market_value: 120000000,
  total_cost_basis: 100000000,
  total_gain_value: 20000000,
  total_gain_pct: 20.0,
  groups: [
    { group_name: "한국주식", market_value: 90000000, stock_count: 1, weight_pct: 75.0 },
    { group_name: "해외주식", market_value: 30000000, stock_count: 1, weight_pct: 25.0 },
  ],
  positions: [
    {
      stock_code: "005930", stock_name: "삼성전자", group_name: "한국주식",
      quantity: 1000, unit_price: 90000, avg_price_krw: 70000,
      market_value: 90000000, cost_basis: 70000000, gain_value: 20000000,
      gain_pct: 28.5714, weight_pct: 75.0,
    },
    {
      stock_code: "AAPL", stock_name: "Apple", group_name: "해외주식",
      quantity: 100, unit_price: 300000, avg_price_krw: 300000,
      market_value: 30000000, cost_basis: 30000000, gain_value: 0,
      gain_pct: 0, weight_pct: 25.0,
    },
  ],
};

function savedReport({ endSnapshot = END_SNAPSHOT } = {}) {
  const holdings = {
    snapshot_end_count: 2,
    changes: { counts: { added: 1, removed: 0, increased: 1, decreased: 0 }, top_increases: [], top_decreases: [] },
  };
  if (endSnapshot) holdings.end_snapshot = endSnapshot;
  return {
    period_type: "monthly",
    period_key: "2026-06",
    generated_at: "2026-07-01T09:00:00",
    source_hash: "abcdef1234567890",
    report: {
      schema_version: endSnapshot ? 3 : 2,
      period: { label: "월간 투자 보고서", key: "2026-06", start_date: "2026-06-01", end_date: "2026-06-30" },
      summary: { nav_return_pct: 3.25, ending_value: 120000000, starting_value: 116000000 },
      cashflows: {},
      composition_changes: { summary: {}, top_buys: [], top_sells: [] },
      risk: {},
      allocation: { groups: [], concentration: { end: {} } },
      holdings,
      data_quality: { warnings: [] },
      review_notes: [],
    },
  };
}

function loadPanel({ report = savedReport() } = {}) {
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
  const routes = {
    "/api/portfolio/period-reports/periods": PERIODS,
    "/api/portfolio/period-reports/performance": PERFORMANCE,
  };
  w.apiFetch = (path) => {
    calls.push(path);
    const payload = routes[path] ?? (path.startsWith("/api/portfolio/period-reports/monthly/") ? report : null);
    if (!payload) return Promise.resolve({ ok: false, status: 404, json: async () => ({}) });
    return Promise.resolve({ ok: true, status: 200, json: async () => payload });
  };
  return { w, calls };
}

const perfRows = (w, selector) => [...w.document.querySelectorAll(selector)];
const rowKey = (row) => row.querySelector(".pf-perf-key").textContent.trim();

test("연간 실적 목록 — 최근 연도만 펼친 채로 그린다", async () => {
  const { w, calls } = loadPanel();
  await w.pfLoadPeriodReportsPanel();

  assert.ok(calls.includes("/api/portfolio/period-reports/performance"));
  // 목록은 보고서 카드 밖의 독립 카드에 그려진다.
  const perfWrap = w.document.getElementById("pfPeriodPerformanceWrap");
  assert.equal(perfWrap.closest("#pfPeriodReportWrap"), null);
  assert.ok(perfWrap.classList.contains("pf-analysis-card"));

  const years = perfRows(w, ".pf-perf-year-row");
  assert.deepEqual(years.map((r) => rowKey(r)), ["2026진행 중", "2025"]);

  // 수익률은 공용 포맷터(fmtPct) 기반 부호 포함 2자리 + 색상 클래스.
  const values = years.map((r) => r.querySelector(".pf-perf-value"));
  assert.equal(values[0].textContent, "+12.50%");
  assert.ok(values[0].className.includes("positive"));
  assert.equal(values[1].textContent, "-4.00%");
  assert.ok(values[1].className.includes("negative"));

  // 첫 로드에서 최근 연도(2026)만 펼쳐져 있다.
  const blocks = perfRows(w, ".pf-perf-year");
  assert.equal(years[0].getAttribute("aria-expanded"), "true");
  assert.equal(blocks[0].querySelector(".pf-perf-months").hasAttribute("hidden"), false);
  assert.equal(years[1].getAttribute("aria-expanded"), "false");
  assert.equal(blocks[1].querySelector(".pf-perf-months").hasAttribute("hidden"), true);

  // 기준 스냅샷이 기간 내 첫 스냅샷이면 그 사실을 서브라벨에 남긴다.
  assert.match(years[1].querySelector(".pf-risk-sub").textContent, /기간 내 첫 스냅샷 기준/);
});

test("연도 행 클릭 — 그 해의 월간 실적이 펼쳐지고 다시 누르면 접힌다", async () => {
  const { w } = loadPanel();
  await w.pfLoadPeriodReportsPanel();

  // 2025 를 펼치면 그 해 월 행이 나온다.
  w.pfTogglePerformanceYear("2025");
  let blocks = perfRows(w, ".pf-perf-year");
  assert.equal(blocks[1].querySelector(".pf-perf-months").hasAttribute("hidden"), false);
  const months2025 = [...blocks[1].querySelectorAll(".pf-perf-month-row")].map(rowKey);
  assert.deepEqual(months2025, ["2025-12"]);

  const months2026 = [...blocks[0].querySelectorAll(".pf-perf-month-row")].map(rowKey);
  assert.deepEqual(months2026, ["2026-06", "2026-05"]);
  assert.equal(
    blocks[0].querySelectorAll(".pf-perf-month-row")[1].querySelector(".pf-perf-value").textContent,
    "-1.50%",
  );

  // 다시 누르면 접힌다.
  w.pfTogglePerformanceYear("2025");
  blocks = perfRows(w, ".pf-perf-year");
  assert.equal(blocks[1].querySelector(".pf-perf-months").hasAttribute("hidden"), true);
});

test("월 행 클릭 — 셀렉트를 그 달로 옮기고 해당 월 보고서를 띄운다", async () => {
  const { w, calls } = loadPanel();
  await w.pfLoadPeriodReportsPanel();

  // 저장된 보고서가 없는 달 — 셀렉트만 옮기고 안내 문구를 띄운다(요청 없음).
  let before = calls.length;
  await w.pfSelectPeriodFromPerformance("monthly", "2026-05");
  assert.equal(w.document.getElementById("pfPeriodReportKey").value, "2026-05");
  assert.equal(calls.length, before);
  assert.match(w.document.getElementById("pfPeriodReportStatus").textContent, /저장된 보고서 없음/);

  // 저장된 달로 돌아오면 그 달 보고서를 불러온다.
  before = calls.length;
  await w.pfSelectPeriodFromPerformance("monthly", "2026-06");
  assert.equal(w.document.getElementById("pfPeriodReportKey").value, "2026-06");
  assert.ok(calls.slice(before).some((p) => p.endsWith("/period-reports/monthly/2026-06")));
});

test("기간 종료 스냅샷 — 요약은 항상, 보유 목록은 토글로 펼친다", async () => {
  const { w } = loadPanel();
  await w.pfLoadPeriodReportsPanel();

  const content = w.document.getElementById("pfPeriodReportContent");
  assert.match(content.textContent, /기간 종료 스냅샷/);
  assert.match(content.textContent, /기준일 2026-06-30/);
  assert.match(content.textContent, /보유 2종목/);
  // 그룹 비중 칩.
  const groups = [...content.querySelectorAll(".pf-period-snapshot-group")].map((el) => el.textContent.trim());
  assert.deepEqual(groups, ["한국주식 75.00%", "해외주식 25.00%"]);

  // 기본은 접힘 — 표는 DOM 에 있지만 hidden.
  const body = w.document.getElementById("pfPeriodEndSnapshotBody");
  const toggle = w.document.getElementById("pfPeriodEndSnapshotToggle");
  assert.equal(body.hasAttribute("hidden"), true);
  assert.equal(toggle.getAttribute("aria-expanded"), "false");

  w.pfTogglePeriodEndSnapshot();
  assert.equal(body.hasAttribute("hidden"), false);
  assert.equal(toggle.getAttribute("aria-expanded"), "true");
  assert.equal(toggle.textContent, "접기");

  const rows = [...body.querySelectorAll("tbody tr")];
  assert.equal(rows.length, 2);
  assert.match(rows[0].textContent, /삼성전자/);
  assert.match(rows[0].textContent, /005930/);
  assert.match(rows[0].textContent, /75\.00%/);

  w.pfTogglePeriodEndSnapshot();
  assert.equal(body.hasAttribute("hidden"), true);
});

test("구버전(schema v2) 보고서 — 스냅샷 자리에 재생성 안내를 보여준다", async () => {
  const { w } = loadPanel({ report: savedReport({ endSnapshot: null }) });
  await w.pfLoadPeriodReportsPanel();

  const content = w.document.getElementById("pfPeriodReportContent");
  assert.match(content.textContent, /기간 종료 스냅샷/);
  assert.match(content.textContent, /생성\/갱신을 누르면 최신 형식으로/);
  assert.equal(w.document.getElementById("pfPeriodEndSnapshotBody"), null);
});
