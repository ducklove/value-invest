// Tests for describeChart() — 차트 대체 텍스트 접근성 헬퍼 (ST-05).
// describeChart 는 utils.js 에 정의된 함수로, jsdom 창에서 <script> 로
// utils.js 를 로드해 그 window 의 전역으로 노출한다(다른 utils.test.mjs 와
// 동일한 패턴). globalThis 를 직접 건드리지 않아 Node 버전 호환성 이슈가 없다.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const UTILS_SRC = readFileSync(
  join(__dirname, "..", "..", "static", "js", "utils.js"),
  "utf8",
);

// utils.js 를 실제 <script> 로 jsdom 창에 로드해 describeChart 가 그 창의
// 전역으로 붙게 한다. 브라우저와 동일한 환경.
function loadWindow() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const script = dom.window.document.createElement("script");
  script.textContent = UTILS_SRC;
  dom.window.document.body.appendChild(script);
  return dom.window;
}

test("describeChart: 컨테이너에 role=img 와 aria-label 을 설정한다", () => {
  const w = loadWindow();
  const container = w.document.createElement("div");
  // echarts canvas 가 있다고 가정하고 가짜 canvas 추가.
  const canvas = w.document.createElement("canvas");
  container.appendChild(canvas);
  w.describeChart(container, "포트폴리오 NAV 추이: 최근 30일 +5.2%");
  assert.equal(container.getAttribute("role"), "img");
  assert.equal(container.getAttribute("aria-label"), "포트폴리오 NAV 추이: 최근 30일 +5.2%");
  // canvas 는 보조기기가 무시하도록 aria-hidden.
  assert.equal(canvas.getAttribute("aria-hidden"), "true");
});

test("describeChart: rows 옵션으로 숨김 데이터 표를 추가한다 (sr-only)", () => {
  const w = loadWindow();
  const container = w.document.createElement("div");
  w.document.body.appendChild(container);
  w.describeChart(container, "연간 재무 지표", {
    caption: "최근 3년 주가/PER",
    headers: ["연도", "주가", "PER"],
    rows: [
      ["2024", "50000", "12.3"],
      ["2025", "55000", "11.8"],
    ],
  });
  const table = container.querySelector(".sr-only-chart-table");
  assert.ok(table, "숨김 표가 추가되어야 함");
  assert.equal(table.querySelector("caption").textContent, "최근 3년 주가/PER");
  const ths = [...table.querySelectorAll("th")].map((t) => t.textContent);
  assert.deepEqual(ths, ["연도", "주가", "PER"]);
  const cells = [...table.querySelectorAll("tbody td")].map((t) => t.textContent);
  assert.deepEqual(cells, ["2024", "50000", "12.3", "2025", "55000", "11.8"]);
});

test("describeChart: rows=null 이면 기존 숨김 표를 제거한다", () => {
  const w = loadWindow();
  const container = w.document.createElement("div");
  w.document.body.appendChild(container);
  w.describeChart(container, "라벨", {
    headers: ["x"],
    rows: [["1"]],
  });
  assert.ok(container.querySelector(".sr-only-chart-table"));
  w.describeChart(container, "라벨");  // rows 없음
  assert.equal(container.querySelector(".sr-only-chart-table"), null);
});

test("describeChart: 다시 호출하면 기존 숨김 표를 갱신(교체)한다", () => {
  const w = loadWindow();
  const container = w.document.createElement("div");
  w.document.body.appendChild(container);
  w.describeChart(container, "라벨", {
    headers: ["연도"],
    rows: [["2024"]],
  });
  w.describeChart(container, "라벨", {
    headers: ["연도"],
    rows: [["2025"], ["2026"]],
  });
  const rows = [...container.querySelectorAll(".sr-only-chart-table tbody td")].map((t) => t.textContent);
  assert.deepEqual(rows, ["2025", "2026"]);
});

test("describeChart: 컨테이너가 null 이면 no-op (에러 없음)", () => {
  const w = loadWindow();
  assert.doesNotThrow(() => w.describeChart(null, "라벨"));
});

test("describeChart: XSS 방지 — 셀 값은 textContent 로 주입(HTML 해석 안 됨)", () => {
  const w = loadWindow();
  const container = w.document.createElement("div");
  w.document.body.appendChild(container);
  w.describeChart(container, "라벨", {
    headers: ["x"],
    rows: [["<img src=x onerror=alert(1)>"]],
  });
  const table = container.querySelector(".sr-only-chart-table");
  // 스크립트 태그가 DOM 으로 해석되지 않고 텍스트로 들어가야 함.
  assert.equal(table.querySelector("img"), null);
  assert.equal(table.querySelector("tbody td").textContent, "<img src=x onerror=alert(1)>");
});
