// jsdom 행위 테스트: 종목 360° 수치 패널(static/js/analysis-360.js).
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { JSDOM } from "jsdom";
import { publicProjection } from "../../scripts/sync-ecosystem.mjs";

const read = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), "utf8");
const ECOSYSTEM = publicProjection(JSON.parse(read("config/ecosystem.json")));
const SOURCES = ["static/js/utils.js", "static/js/ecosystem-links.js", "static/js/analysis-360.js"].map(read);

function load({ response, rejects = false, active = "005935" } = {}) {
  const dom = new JSDOM(`<!doctype html><html data-theme="light"><body>
    <section class="stock360" id="stock360Panel" aria-labelledby="stock360Title" hidden></section>
  </body></html>`, { url: "https://app.example.com/analysis", runScripts: "dangerously" });
  const w = dom.window;
  w.APP_CONFIG = { ecosystem: ECOSYSTEM, integrations: {} };
  for (const src of SOURCES) {
    const script = w.document.createElement("script");
    script.textContent = src;
    w.document.body.appendChild(script);
  }
  w.eval(`activeStockCode = ${JSON.stringify(active)};`);  // utils.js 가 선언한 전역
  w.__calls = [];
  w.apiFetchJson = async (path) => {
    w.__calls.push(path);
    if (rejects) throw new Error("offline");
    return response;
  };
  return w;
}

const PREFERRED = {
  tool: "common_preferred_spread", kind: "preferred", label: "우선주 괴리", code: "005935", relatedCode: "005930",
  name: "삼성전자우", score: 92, scoreBasis: "history3y", scoreNote: "자기 3년 분포에서 현재 괴리율 이하인 비율",
  metrics: [
    { label: "괴리율", value: 19.59, unit: "%" },
    { label: "전일 대비", value: -0.81, unit: "%p" },
    { label: "3년 백분위", value: 92, unit: "pctile" },
    { label: "cps 매력도", value: 63.5, unit: "점" },
    { label: "보통주", value: 193500, unit: "원" },
    { label: "보통주 20일 평균 거래대금", value: 5e11, unit: "원" },
  ],
  asOf: "2026-11-06", distributionEnd: "2026-11-06", warnings: [], stale: false,
  url: "https://ducklove.github.io/common_preferred_spread/?code=005935",
};
const NPS = {
  tool: "nps-tracker", kind: "nps", label: "국민연금", code: "005935", name: null, score: null, scoreBasis: null,
  metrics: [
    { label: "공시 지분율", value: 5.1, unit: "%" },
    { label: "포트폴리오 비중(추정 수량 기반)", value: 0.002, unit: "%" },
    { label: "가격 기준일", value: "2026-11-06", unit: "text" },
  ],
  asOf: "2025-12-31", distributionEnd: null,
  warnings: ["2024-12-31·2025-12-31 공시의 지분율이 같아 변화를 계산할 수 없습니다"], stale: true,
  url: "https://ducklove.github.io/nps-tracker/?code=005935",
};
const SPAC = {
  tool: "spac-hunter", kind: "spac", label: "스팩 청산가치", code: "474930", name: "신한제13호스팩", score: 100, scoreBasis: "crossSection",
  metrics: [
    { label: "연환산 기대수익(거래비용 차감 전)", value: 8.3, unit: "%" },
    { label: "청산가 괴리", value: 1.86, unit: "%" },
    { label: "수령 예정일", value: "2027-03-11", unit: "text" },
  ],
  asOf: "2026-11-06", distributionEnd: null, warnings: [], stale: false,
  url: "https://ducklove.github.io/spac-hunter/?code=474930",
};

function payload(over = {}) {
  return {
    code: "005935",
    signals: [PREFERRED, NPS],
    liquidity: { value: 3e10, basis: "avg20d", asOf: "2026-11-06", low: false },
    failedTools: [],
    ...over,
  };
}

const panel = (w) => w.document.getElementById("stock360Panel");
const text = (el) => el.textContent.replace(/\s+/g, " ").trim();
const metrics = (card) => Object.fromEntries(
  [...card.querySelectorAll(".stock360-metrics > div")].map((row) => [row.querySelector("dt").textContent, row.querySelector("dd").textContent]),
);

test("도구별 수치와 기준일을 카드로 보여 준다", async () => {
  const w = load({ response: payload() });
  await w.loadStock360("005935");
  assert.equal(w.__calls[0], "/api/external/stock/005935/360");
  assert.equal(panel(w).hidden, false);
  const [card, nps] = panel(w).querySelectorAll(".stock360-card");
  assert.equal(card.querySelector(".stock360-label").textContent, "우선주 괴리");
  assert.equal(card.querySelector(".stock360-subject").textContent, "삼성전자우");
  assert.match(text(card.querySelector(".stock360-score")), /^92 ?자기 3년 분포$/);
  assert.deepEqual(metrics(card), {
    괴리율: "19.59%",
    "전일 대비": "-0.81%p",
    "3년 백분위": "92",
    "cps 매력도": "63.5점",
    보통주: "193,500원",
    "보통주 20일 평균 거래대금": "5000억원",
  });
  assert.equal(text(card.querySelector(".stock360-asof")), "기준일 2026-11-06 · 분포 2026-11-06까지");
  assert.equal(metrics(nps)["포트폴리오 비중(추정 수량 기반)"], "0.002%");
});

test("후보의 거래대금과 기준일을 함께 표시한다", async () => {
  const w = load({ response: payload() });
  await w.loadStock360("005935");
  assert.equal(text(panel(w).querySelector(".stock360-liquidity")), "20일 평균 300.0억원 · 2026-11-06");
});

test("거래대금을 모르면 미확인으로, 1억 미만이면 표시한다", async () => {
  const unknown = load({ response: payload({ liquidity: { value: null, basis: null, asOf: null, low: null } }) });
  await unknown.loadStock360("005935");
  assert.equal(text(panel(unknown).querySelector(".stock360-liquidity")), "거래대금 미확인");
  const low = load({ response: payload({ liquidity: { value: 5e7, basis: "session", asOf: "2026-11-06", low: true } }) });
  await low.loadStock360("005935");
  const cell = panel(low).querySelector(".stock360-liquidity");
  assert.ok(cell.classList.contains("is-low"));
  assert.match(text(cell), /당일 누적/);
});

test("오래된 데이터와 변화를 계산할 수 없는 값은 경고한다", async () => {
  const w = load({ response: payload() });
  await w.loadStock360("005935");
  const nps = panel(w).querySelectorAll(".stock360-card")[1];
  assert.ok(nps.classList.contains("is-stale"));
  assert.match(nps.querySelector(".stock360-warning").textContent, /변화를 계산할 수 없습니다/);
  assert.equal(nps.querySelector(".stock360-score"), null);
  assert.equal(nps.querySelector(".stock360-subject").textContent, "005935");  // 이름이 없으면 코드
  assert.match(text(nps.querySelector(".stock360-asof")), /^기준일 2025-12-31$/);
});

test("기준일이 없으면 확인 불가로 표시한다", async () => {
  const w = load({ response: payload({ signals: [{ ...PREFERRED, asOf: null, distributionEnd: null }] }) });
  await w.loadStock360("005935");
  assert.equal(text(panel(w).querySelector(".stock360-asof")), "기준일 확인 불가");
});

test("실제 0은 그대로, 작은 값은 자릿수를 늘려 표시한다", async () => {
  const signals = [{ ...PREFERRED, metrics: [
    { label: "자사주 비율", value: 0, unit: "%" },
    { label: "비중", value: 0.002, unit: "%" },
    { label: "빈 값", value: null, unit: "%" },
  ] }];
  const w = load({ response: payload({ signals }) });
  await w.loadStock360("005935");
  assert.deepEqual(metrics(panel(w).querySelector(".stock360-card")), { "자사주 비율": "0.00%", 비중: "0.002%", "빈 값": "-" });
});

test("연결된 쌍은 이 종목과의 관계를 표시한다", async () => {
  const w = load({ response: payload({ code: "005930" }), active: "005930" });
  await w.loadStock360("005930");
  assert.match(text(panel(w).querySelector(".stock360-subject")), /삼성전자우 연결 종목 005935 기준/);
});

test("해당 신호가 없는 종목은 패널을 숨긴다", async () => {
  for (const response of [payload({ signals: [] }), null, {}]) {
    const w = load({ response });
    await w.loadStock360("005935");
    assert.equal(panel(w).hidden, true);
    assert.equal(panel(w).innerHTML, "");
  }
});

test("네트워크가 실패해도 예외 없이 패널을 숨긴다", async () => {
  const w = load({ rejects: true });
  await w.loadStock360("005935");
  assert.equal(panel(w).hidden, true);
});

test("한 도구가 실패해도 나머지를 보여 주고 실패한 도구를 알린다", async () => {
  const w = load({ response: payload({ signals: [PREFERRED], failedTools: [{ tool: "holding_value", label: "지주사 할인" }] }) });
  await w.loadStock360("005935");
  assert.equal(panel(w).querySelectorAll(".stock360-card").length, 1);
  assert.match(text(panel(w).querySelector(".stock360-failed")), /응답 없는 도구: 지주사 할인/);
});

test("모든 도구가 실패하면 그 사실만 알린다", async () => {
  const w = load({ response: payload({ signals: [], failedTools: [{ tool: "holding_value", label: "지주사 할인" }] }) });
  await w.loadStock360("005935");
  assert.equal(panel(w).hidden, false);
  assert.equal(panel(w).querySelector(".stock360-card"), null);
  assert.match(text(panel(w)), /응답 없는 도구/);
});

test("종목을 바꾸면 이전 수치를 지우고 늦은 응답을 버린다", async () => {
  const w = load({ response: payload() });
  await w.loadStock360("005935");
  assert.equal(panel(w).hidden, false);

  let resolve;
  w.apiFetchJson = () => new Promise((done) => { resolve = done; });
  const pending = w.loadStock360("000670");
  assert.equal(panel(w).hidden, true);  // 응답 전에 이전 종목 수치가 사라진다
  w.eval('activeStockCode = "035720";');
  resolve(payload({ code: "000670" }));
  await pending;
  assert.equal(panel(w).hidden, true);  // 그 사이 다른 종목으로 바뀌었으므로 그리지 않는다
});

test("도구 링크는 레지스트리 딥링크를 쓰고 테마 전환 표식을 단다", async () => {
  const w = load({ response: payload() });
  await w.loadStock360("005935");
  const link = panel(w).querySelector(".stock360-card header a");
  assert.match(link.getAttribute("href"), /common_preferred_spread\/\?code=005935/);
  assert.equal(link.dataset.vcTool, "common_preferred_spread");
  assert.equal(link.dataset.vcCode, "005935");
  assert.equal(link.getAttribute("rel"), "noopener noreferrer");
});

test("스팩은 비용 차감 전임을 밝히고 수령 예정일을 그대로 보여 준다", async () => {
  const w = load({ response: payload({ code: "474930", signals: [SPAC] }), active: "474930" });
  await w.loadStock360("474930");
  const shown = metrics(panel(w).querySelector(".stock360-card"));
  assert.equal(shown["연환산 기대수익(거래비용 차감 전)"], "8.30%");
  assert.equal(shown["수령 예정일"], "2027-03-11");
});

test("서버 문자열은 이스케이프한다", async () => {
  const hostile = '<img src=x onerror="window.__xss=1">';
  const w = load({
    response: payload({
      signals: [{ ...PREFERRED, label: hostile, name: hostile, warnings: [hostile], asOf: hostile, metrics: [{ label: hostile, value: hostile, unit: "text" }] }],
      failedTools: [{ tool: "x", label: hostile }],
    }),
  });
  await w.loadStock360("005935");
  assert.equal(panel(w).querySelector("img"), null);
  assert.equal(w.__xss, undefined);
});

test("백분위를 수익 보장으로 표현하지 않는다", async () => {
  const w = load({ response: payload() });
  await w.loadStock360("005935");
  const page = text(panel(w));
  assert.match(page, /수익을 뜻하지 않습니다/);
  assert.match(page, /거래비용을 추정할 수 없어 비용을 뺀 수치는 표시하지 않습니다/);
  for (const banned of ["수익 보장", "보장합니다", "확정 수익", "매수 추천", "순수익", "세후"]) {
    assert.ok(!page.includes(banned), banned);
    assert.ok(!read("static/js/analysis-360.js").includes(banned), banned);
  }
});
