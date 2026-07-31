// jsdom behavior tests for 컬러 모드 (portfolio-heat.js).
// 강도 등급, KRX 상/하한가 판정(호가단위 정렬), 목록 기준 상대 게이지,
// 토글 off 시 종전 표시 유지, WS tick 경로의 <tr> 속성 동기화를 고정한다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

const UTILS_SRC = read("static", "js", "utils.js");
const STORE_SRC = read("static", "js", "portfolio-store.js");
const HEAT_SRC = read("static", "js", "portfolio-heat.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function loadHeatDom() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <button id="pfHeatToggle" aria-pressed="false"></button>
    <span id="pfHeatSummary" hidden></span>
    <table><tbody id="pfBody"><tr data-code="005930"></tr></tbody></table>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  // utils.js 는 앱 부팅 side effect 가 많아 필요한 포맷터만 이식한다.
  const fmtPctSrc = UTILS_SRC.slice(UTILS_SRC.indexOf("function fmtPct("));
  appendScript(w, fmtPctSrc.slice(0, fmtPctSrc.indexOf("\n}") + 2));
  appendScript(w, STORE_SRC);
  appendScript(w, HEAT_SRC);
  // 컬러 모드 off 경로에서만 쓰이는 종전 포맷터.
  w.fmtChangePct = (pct) => (pct === null || pct === undefined ? "-" : `plain:${pct}`);
  return w;
}

const holding = (overrides = {}) => ({
  stock_code: "005930",
  changePct: 0,
  change: 0,
  price: null,
  quote: {},
  ...overrides,
});

test("등락률 강도는 1/2/4/7/15% 경계에서 한 단계씩 오른다", () => {
  const w = loadHeatDom();
  const levels = [0.99, 1, 1.9, 2, 3.9, 4, 6.9, 7, 14.9, 15, 29].map(w.pfHeatLevel);
  assert.deepEqual(levels, [0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5]);
  // 부호와 무관하게 절대값 기준.
  assert.equal(w.pfHeatLevel(-8), w.pfHeatLevel(8));
  assert.equal(w.pfHeatLevel(null), 0);
});

test("상/하한가는 ±30% 근사가 아니라 호가단위에 정렬된 정확한 가격에서만 잡힌다", () => {
  const w = loadHeatDom();
  // 기준가 10,000원 → 13,000×내림(호가 10원) = 13,000 / 7,000×올림 = 7,000
  assert.equal(w.pfKrxUpperLimit(10000), 13000);
  assert.equal(w.pfKrxLowerLimit(10000), 7000);
  // 기준가 3,333원 → 4,332.9 를 호가 5원으로 내림 = 4,330 (근사 30% 아님)
  assert.equal(w.pfKrxUpperLimit(3333), 4330);

  const atLimit = holding({ price: 13000, quote: { previous_close: 10000 } });
  const nearLimit = holding({ price: 12995, quote: { previous_close: 10000 } });
  assert.equal(w.pfHeatLimitState(atLimit), "up");
  assert.equal(w.pfHeatLimitState(nearLimit), null);
  assert.equal(w.pfHeatLimitState(holding({ price: 7000, quote: { previous_close: 10000 } })), "down");
});

test("국내 주식 코드가 아닌 자산은 가격제한폭 판정 대상이 아니다", () => {
  const w = loadHeatDom();
  for (const code of ["CASH_KRW", "KRX_GOLD", "CRYPTO_BTC", "AAPL"]) {
    const row = holding({ stock_code: code, price: 13000, quote: { previous_close: 10000 } });
    assert.equal(w.pfHeatLimitState(row), null, `${code} should have no KRX limit state`);
  }
});

test("게이지 폭은 현재 보이는 목록의 최대 등락률 기준 상대값이다", () => {
  const w = loadHeatDom();
  const rows = [holding({ changePct: 2 }), holding({ changePct: -8 })];
  assert.equal(w.pfHeatSetScale(rows), 8);
  assert.equal(w.pfHeatRowState(rows[0]).ratio, 0.25);
  assert.equal(w.pfHeatRowState(rows[1]).ratio, 1);

  // 필터가 좁혀져 최대치가 작아지면 같은 2% 가 꽉 찬 막대가 된다.
  w.pfHeatSetScale([rows[0]]);
  assert.equal(w.pfHeatRowState(rows[0]).ratio, 1);
});

test("전체 재렌더 없이 도착한 신고점 tick 도 게이지 분모를 넓힌다", () => {
  const w = loadHeatDom();
  w.pfHeatSetScale([holding({ changePct: 3 })]);
  const spike = w.pfHeatRowState(holding({ changePct: 12 }));
  assert.equal(spike.ratio, 1);
  // 분모가 12 로 올라갔으므로 기존 3% 행은 이제 1/4 길이.
  assert.equal(w.pfHeatRowState(holding({ changePct: 3 })).ratio, 0.25);
});

test("컬러 모드 셀은 방향·등급·상한가 상태를 마크업으로 노출한다", () => {
  const w = loadHeatDom();
  w.pfHeatSetScale([holding({ changePct: 29.91 })]);
  const html = w.pfChangeCellHtml(holding({
    changePct: 29.91,
    price: 13000,
    quote: { previous_close: 10000 },
  }));
  assert.match(html, /data-heat-dir="up"/);
  assert.match(html, /data-heat-level="5"/);
  assert.match(html, /data-heat-limit="up"/);
  assert.match(html, /상한/);
  assert.match(html, /--pf-heat-ratio:1\.000/);
  assert.match(html, /\+29\.91%/);
  // 상한 라벨은 등락률 옆에 덧붙는 게 아니라 같은 칸에서 교대로 뜬다(폭 증가 0).
  assert.match(html, /<span class="pf-heat-swap">/);
  assert.match(html, /pf-heat-swap-pct">\+29\.91%<\/span>/);
  assert.match(html, /pf-heat-swap-tag"[^>]*>상한가<\/span>/);
  assert.doesNotMatch(html, /pf-heat-limit-tag/);

  const calm = w.pfChangeCellHtml(holding({ changePct: 0.4 }));
  assert.match(calm, /data-heat-level="0"/);
  assert.doesNotMatch(calm, /data-heat-limit/);
  assert.doesNotMatch(calm, /pf-heat-icon/);
});

test("컬러 모드를 끄면 등락률 셀과 행 속성이 종전 표시로 돌아간다", () => {
  const w = loadHeatDom();
  const tr = w.document.querySelector("tr[data-code]");
  const row = holding({ changePct: 9.5 });

  assert.match(w.pfChangeCellHtml(row), /pf-heat-cell/);
  w.pfHeatApplyRow(tr, row);
  assert.equal(tr.getAttribute("data-heat-level"), "4");

  w.pfToggleHeatMode();
  assert.equal(w.document.body.classList.contains("pf-heat-mode"), false);
  assert.equal(w.document.getElementById("pfHeatToggle").getAttribute("aria-pressed"), "false");
  assert.equal(w.pfChangeCellHtml(row), "plain:9.5");
  assert.equal(w.pfHeatRowAttrs(row), "");
  w.pfHeatApplyRow(tr, row);
  assert.equal(tr.hasAttribute("data-heat-dir"), false);
  assert.equal(tr.hasAttribute("data-heat-level"), false);
});

test("등락 1% 미만 행에는 히트 속성을 붙이지 않아 평소 화면 그대로다", () => {
  const w = loadHeatDom();
  assert.equal(w.pfHeatRowAttrs(holding({ changePct: 0.6 })), "");
  assert.equal(w.pfHeatRowAttrs(holding({ changePct: null })), "");
  assert.match(w.pfHeatRowAttrs(holding({ changePct: -5 })), /data-heat-dir="down" data-heat-level="3"/);
});

test("요약 스트립은 상/하한가와 급등·급락 종목 수를 집계한다", () => {
  const w = loadHeatDom();
  const summary = w.document.getElementById("pfHeatSummary");
  w.pfHeatUpdateSummary([
    holding({ stock_code: "005930", changePct: 29.91, price: 13000, quote: { previous_close: 10000 } }),
    holding({ stock_code: "000660", changePct: -29.9, price: 7000, quote: { previous_close: 10000 } }),
    holding({ stock_code: "035420", changePct: 5.2 }),
    holding({ stock_code: "035720", changePct: 4.4 }),
    holding({ stock_code: "051910", changePct: -6.1 }),
    holding({ stock_code: "005380", changePct: 1.2 }),
  ]);
  assert.equal(summary.hidden, false);
  assert.match(summary.innerHTML, /상한가 1/);
  assert.match(summary.innerHTML, /하한가 1/);
  assert.match(summary.innerHTML, /급등 2/);
  assert.match(summary.innerHTML, /급락 1/);

  w.pfHeatUpdateSummary([holding({ changePct: 0.3 })]);
  assert.match(summary.textContent, /잔잔/);
});

test("상한가에 새로 닿은 순간에만 전면 이펙트가 한 번 뜬다", async () => {
  const w = loadHeatDom();
  const limitUp = holding({
    stock_code: "005930",
    stock_name: "삼성전자",
    changePct: 29.91,
    price: 13000,
    quote: { previous_close: 10000 },
  });
  const tick = () => new Promise((resolve) => setTimeout(resolve, 160));

  w.pfHeatUpdateSummary([limitUp]);
  await tick();
  const layers = w.document.querySelectorAll(".pf-fx");
  assert.equal(layers.length, 1);
  assert.equal(layers[0].getAttribute("data-fx"), "up");
  assert.equal(layers[0].querySelector(".pf-fx-names").textContent, "삼성전자");
  assert.ok(layers[0].querySelectorAll(".pf-fx-p").length > 10);

  // 상한가에 머무는 동안엔 렌더가 몇 번을 돌아도 다시 뿌리지 않는다.
  w.pfHeatUpdateSummary([limitUp]);
  w.pfHeatApplyRow(w.document.querySelector("tr[data-code]"), limitUp);
  await tick();
  assert.equal(w.document.querySelectorAll(".pf-fx").length, 1);

  // 풀렸다가 다시 닿으면 그때는 새 사건.
  w.pfHeatUpdateSummary([holding({ stock_code: "005930", changePct: 12 })]);
  w.pfHeatUpdateSummary([limitUp]);
  await tick();
  assert.equal(w.document.querySelectorAll(".pf-fx").length, 2);
});

test("전면 이펙트는 하한가 색을 따로 쓰고 종목명을 textContent 로만 넣는다", async () => {
  const w = loadHeatDom();
  const layer = w.pfHeatCelebrate("down", ["<img onerror=x>", "카카오", "네이버", "LG화학"]);
  assert.equal(layer.getAttribute("data-fx"), "down");
  assert.equal(layer.getAttribute("aria-hidden"), "true");
  assert.equal(layer.querySelector("img"), null);
  assert.match(layer.querySelector(".pf-fx-names").textContent, /외 1$/);
  assert.equal(layer.querySelector(".pf-fx-title").textContent, "🧊 하한가");
});
