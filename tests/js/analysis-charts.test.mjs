// jsdom behavior tests for analysis-charts.js — 목표가 차트의 인라인/모달
// 두 경로가 공용 옵션 빌더(buildTargetPriceChartOption)를 공유하는지, 그리고
// 차트 카드가 키보드(role=button + Enter/Space)로 모달을 열 수 있는지 검증.
// echarts/createLineChart 등 차트 라이브러리는 스텁으로 대체하고 setOption 에
// 전달된 옵션 객체를 캡처해 비교한다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const CHARTS_SRC = readFileSync(join(root, "static", "js", "analysis-charts.js"), "utf8");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function buildWindow({ style = "", theme = "" } = {}) {
  const dom = new JSDOM(`<!doctype html><html${theme ? ` data-theme="${theme}"` : ""}><head>${style ? `<style>${style}</style>` : ""}</head><body>
    <div id="weeklyChartsGrid" style="display:none"></div>
    <div id="chartModal">
      <h3 id="chartModalTitle"></h3>
      <button class="chart-modal-close"></button>
      <div id="chartModalCanvas"></div>
    </div>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  const setOptions = [];
  // echarts 스텁 — setOption 인자를 캡처. LinearGradient 는 areaStyle 생성용.
  w.echarts = {
    init: () => ({
      setOption: (opt) => setOptions.push(opt),
      on: () => {},
      dispose: () => {},
    }),
    graphic: {
      LinearGradient: class {
        constructor(x0, y0, x1, y1, stops) { this.stops = stops; }
      },
    },
  };
  w.loadEcharts = async () => {};
  w.loadChartLib = async () => {};
  // 원래 utils.js(charts)·analysis.js(_renderCoverage)·analysis-filings.js
  // (buildReportPdfUrl, allReports)가 제공하는 전역들.
  w.charts = {};
  w.CHART_COLORS = ["#0ea5e9", "#f97316"];
  w.PER_DISPLAY_MAX = 100;
  w.fmtKrw = (v) => `${Number(v).toLocaleString()}원`;
  w.describeChart = () => {};
  w.createLineChart = () => ({ dispose: () => {} });
  w.openManagedModal = () => {};
  w.closeManagedModal = () => {};
  w._renderCoverage = () => {};
  w.buildReportPdfUrl = (u) => u;
  w.allReports = [];
  appendScript(w, CHARTS_SRC);
  return { w, setOptions };
}

const PRICE_SERIES = [
  { date: "2024-01-05", value: 70000 },
  { date: "2024-01-12", value: 71000 },
  { date: "2024-02-02", value: 72000 },
  { date: "2024-02-09", value: 73000 },
];
const REPORTS = [
  { date: "2024-01-05", target_price: "90,000", recommendation: "매수", firm_short: "증권A", source_url: "https://a.example/1" },
  { date: "2024-02-02", target_price: "95000", recommendation: "Hold", firm_short: "증권B", source_url: "https://b.example/2" },
];

// _lastWeeklyIndicators 는 스크립트 전역 let — 같은 realm 의 classic script 로
// 값을 채운 뒤 오버레이를 실행한다(브라우저에서 다른 <script> 가 하는 것과 동일).
async function runOverlay(w) {
  appendScript(w, `
    _lastWeeklyIndicators = ${JSON.stringify({ "주가": PRICE_SERIES })};
    window.__overlayPromise = _overlayTargetPrices(${JSON.stringify(REPORTS)});
  `);
  await w.__overlayPromise;
}

test("목표가 인라인 카드와 모달이 공용 빌더의 동일한 핵심 옵션을 공유한다", async () => {
  const { w, setOptions } = buildWindow();
  await runOverlay(w);
  assert.equal(setOptions.length, 1, "인라인 목표가 차트가 그려져야 함");

  // 카드 클릭 → 모달 경로 실행
  const card = w.document.getElementById("targetPriceChartCard");
  assert.ok(card, "목표가 차트 카드가 생성되어야 함");
  card.click();
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(setOptions.length, 2, "모달 목표가 차트가 그려져야 함");

  const [inline, modal] = setOptions;
  // 핵심 옵션(축 데이터·시리즈 구성·데이터)이 두 경로에서 동일해야 한다.
  // jsdom realm 의 배열은 Node 리터럴과 프로토타입이 달라 deepStrictEqual 이
  // 실패하므로 Array.from 으로 Node realm 에 복사해 비교한다.
  const arr = (x) => Array.from(x);
  assert.deepEqual(arr(modal.xAxis.data), arr(inline.xAxis.data));
  assert.deepEqual(Array.from(modal.series, (s) => s.name), ["주가", "목표가", "리포트"]);
  assert.deepEqual(Array.from(inline.series, (s) => s.name), ["주가", "목표가", "리포트"]);
  assert.deepEqual(arr(modal.series[0].data), arr(inline.series[0].data)); // 주가
  assert.deepEqual(arr(inline.series[1].data), [90000, 90000, 95000, 95000]); // 목표가 스텝 라인
  assert.deepEqual(arr(modal.series[1].data), arr(inline.series[1].data));
  assert.equal(modal.series[2].data, inline.series[2].data); // 리포트 scatter — 같은 데이터 공유

  // 툴팁 문자열도 동일해야 한다.
  const params = [
    { seriesName: "주가", dataIndex: 1, color: "#3b82f6", value: 71000 },
    { seriesName: "목표가", color: "#f59e0b", value: 90000 },
    { seriesName: "리포트", data: { value: [1, 90000], firm: "증권A", buy: true } },
  ];
  assert.equal(inline.tooltip.formatter(params), modal.tooltip.formatter(params));
  assert.match(inline.tooltip.formatter(params), /주가: 71,000원/);

  // 경로별 의도된 차이: dataZoom 은 모달 전용, scatter 심볼 8→10.
  assert.equal(inline.dataZoom, undefined);
  assert.deepEqual(Array.from(modal.dataZoom, (z) => z.type), ["slider", "inside"]);
  assert.equal(inline.series[2].symbolSize, 8);
  assert.equal(modal.series[2].symbolSize, 10);
});

test("목표가 카드는 role=button 이고 Enter 키로 모달을 연다", async () => {
  const { w, setOptions } = buildWindow();
  await runOverlay(w);
  const card = w.document.getElementById("targetPriceChartCard");
  assert.equal(card.getAttribute("role"), "button");
  assert.equal(card.getAttribute("tabindex"), "0");
  assert.match(card.getAttribute("aria-label"), /증권사 목표가/);

  card.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(setOptions.length, 2, "Enter 로 모달 차트가 그려져야 함");
});

// --- 리포트 스캐터 Buy/Hold 의미색 (CSS 토큰) ---
// Buy 배지(badge-buy: 초록)와 목표가 스캐터의 Buy 색이 이원화돼 있던 것을
// --recomm-* 토큰(analysis.css)으로 통일했는지 검증. 토큰이 없으면(jsdom 에
// CSS 미로드) 라이트 팔레트 폴백을 쓴다.

// analysis.css 의 토큰 사본 — jsdom 은 <style> 의 커스텀 프로퍼티 캐스케이드를
// getComputedStyle 로 해석한다(admin-charts.test.mjs 와 동일 패턴).
const RECOMM_TOKEN_STYLE = `
  :root {
    --recomm-buy: #16a34a; --recomm-buy-strong: #15803d;
    --recomm-hold: #6b7280; --recomm-hold-strong: #4b5563;
  }
  [data-theme="dark"] {
    --recomm-buy: #4ade80; --recomm-buy-strong: #86efac;
    --recomm-hold: #9ca3af; --recomm-hold-strong: #d1d5db;
  }
`;

test("리포트 스캐터 Buy 는 배지와 같은 초록 의미색을 쓰고 빨강(#dc2626) 하드코딩이 없다", async () => {
  const { w, setOptions } = buildWindow();
  await runOverlay(w);
  const inline = setOptions[0];
  const scatter = inline.series[2];

  // 포인트 색: Buy=초록 / Hold=중립 회색 (토큰 미로드 시 폴백 값).
  assert.equal(scatter.itemStyle.color({ data: { buy: true } }), "#16a34a");
  assert.equal(scatter.itemStyle.color({ data: { buy: false } }), "#6b7280");
  assert.equal(scatter.itemStyle.borderColor({ data: { buy: true } }), "#15803d");
  assert.equal(scatter.itemStyle.borderColor({ data: { buy: false } }), "#4b5563");

  // 범례 아이콘도 대표 의미색(Buy 초록)으로 고정 — 팔레트색으로 빠지지 않는다.
  const legendReport = Array.from(inline.legend.data).find((d) => d.name === "리포트");
  assert.ok(legendReport, "범례에 리포트 항목이 있어야 함");
  assert.equal(legendReport.itemStyle.color, "#16a34a");

  // 툴팁의 Buy 라벨 색도 동일 토큰 — 상승색(#dc2626)과 혼동 금지.
  const html = inline.tooltip.formatter([
    { seriesName: "리포트", data: { value: [1, 90000], firm: "증권A", buy: true } },
  ]);
  assert.match(html, /#16a34a/);
  assert.ok(!html.includes("#dc2626"), "Buy 에 상승 빨강을 쓰지 않는다");
});

test("리포트 스캐터 의미색은 다크 테마 토큰(--recomm-*)을 따라간다", async () => {
  const { w, setOptions } = buildWindow({ style: RECOMM_TOKEN_STYLE, theme: "dark" });
  await runOverlay(w);
  const scatter = setOptions[0].series[2];
  assert.equal(scatter.itemStyle.color({ data: { buy: true } }), "#4ade80");
  assert.equal(scatter.itemStyle.color({ data: { buy: false } }), "#9ca3af");
  assert.equal(scatter.itemStyle.borderColor({ data: { buy: true } }), "#86efac");
});

test("차트 그리드 카드: Enter/Space 로 openChartModal, 그 외 키는 무시, 클릭도 유지", async () => {
  const { w } = buildWindow();
  const grid = w.document.getElementById("weeklyChartsGrid");
  await w.renderChartGrid(
    grid,
    ["주가"],
    { "주가": [{ year: 2023, value: 100 }, { year: 2024, value: 120 }] },
    "rgba(0,0,0,0.06)",
    "#666",
    "annual",
  );
  const card = grid.querySelector(".chart-card");
  assert.ok(card, "차트 카드가 렌더돼야 함");
  assert.equal(card.getAttribute("role"), "button");
  assert.equal(card.getAttribute("tabindex"), "0");
  assert.equal(card.getAttribute("aria-label"), "주가 차트 크게 보기");

  const calls = [];
  w.openChartModal = (title, opts) => calls.push({ title, opts });
  card.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
  card.dispatchEvent(new w.KeyboardEvent("keydown", { key: " ", bubbles: true }));
  card.dispatchEvent(new w.KeyboardEvent("keydown", { key: "a", bubbles: true }));
  assert.equal(calls.length, 2, "Enter/Space 만 모달을 연다");
  assert.equal(calls[0].title, "주가");

  card.click();
  assert.equal(calls.length, 3, "마우스 클릭 경로도 그대로 동작");
});
