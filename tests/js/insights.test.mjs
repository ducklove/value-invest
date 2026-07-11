// jsdom behavior test for static/js/insights.js — 인사이트 요약 지표 포맷.
//
// _fmtInsightMetric 은 공용 fmtPct 로 위임되었지만, 백테스트 관례(0 도 +0.00%,
// 값 없음은 칩 생략)는 그대로 유지되어야 한다. utils.js + insights.js 를 jsdom
// 에 올려 포맷 함수와 요약 칩 구성을 검증한다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (p) => readFileSync(join(__dirname, "..", "..", "static", "js", p), "utf8");
const UTILS = read("utils.js");
const INSIGHTS = read("insights.js");

function load() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  for (const src of [UTILS, INSIGHTS]) {
    const s = dom.window.document.createElement("script");
    s.textContent = src;
    dom.window.document.body.appendChild(s);
  }
  return dom.window;
}

test("_fmtInsightMetric: 부호 있는 2자리, 0 도 +0.00%, 값 없음은 null", () => {
  const w = load();
  assert.equal(w._fmtInsightMetric(12.3), "+12.30%");
  assert.equal(w._fmtInsightMetric(-4.5), "-4.50%");
  assert.equal(w._fmtInsightMetric(0), "+0.00%");   // 백테스트 관례: 0 도 +
  assert.equal(w._fmtInsightMetric(null), null);
  assert.equal(w._fmtInsightMetric(undefined), null);
  assert.equal(w._fmtInsightMetric("nope"), null);
});

test("_fmtInsightMetric: suffix 를 바꾸면 % 대신 붙는다", () => {
  const w = load();
  assert.equal(w._fmtInsightMetric(3, "%p"), "+3.00%p");
  assert.equal(w._fmtInsightMetric(-2, "%p"), "-2.00%p");
});

test("_summaryChips: 전략/기간 + 유효 지표만 칩으로, 값 escape", () => {
  const w = load();
  const html = w._summaryChips({
    strategy: "<b>소형가치</b>",
    period: "2021-2026",
    return_pct: 18.4,
    benchmark_return_pct: null,   // 값 없음 → 칩 생략
    cagr_pct: 0,                  // 0 도 +0.00% 칩으로 노출
    max_drawdown_pct: -12.5,
  });
  assert.match(html, /전략/);
  assert.match(html, /2021-2026/);
  assert.match(html, /\+18\.40%/);
  assert.match(html, /\+0\.00%/);          // CAGR 0
  assert.match(html, /-12\.50%/);          // MDD
  assert.ok(!/BM/.test(html), "값 없는 벤치마크 칩은 생략");
  assert.ok(!html.includes("<b>소형가치</b>"), "전략명 escape");
});
