// jsdom behavior test for the "투자 대가의 전략" panel (masters.js):
// 카탈로그 렌더(카드/상세/비교표/시뮬 폼), 세션 내 1회 로드, 카드 선택 전환,
// 시뮬레이션 요청 본문과 결과 렌더(배분 막대·조정 내역·disclaimer)를 검증한다.

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
const MASTERS_SRC = read("static", "js", "masters.js");

const CATALOG_FIXTURE = {
  disclaimer: "투자 조언이 아닌 참고용 시뮬레이션입니다.",
  asset_groups: {
    equity: { label: "주식" },
    bond: { label: "채권" },
  },
  asset_classes: {
    equity_kr: { label: "국내 주식", group: "equity" },
    bond_mid: { label: "중기 국채", group: "bond" },
  },
  profile_options: {
    risk: [
      { id: "conservative", label: "안정형" },
      { id: "balanced", label: "중립형" },
      { id: "aggressive", label: "공격형" },
    ],
    horizon: [
      { id: "short", label: "3년 미만" },
      { id: "mid", label: "3~10년" },
      { id: "long", label: "10년 이상" },
    ],
  },
  strategies: [
    {
      id: "alpha",
      master: "대가 알파",
      title: "전략 알파",
      tagline: "알파 태그라인",
      summary: "알파 요약",
      principles: ["알파 원칙 1"],
      pros: ["알파 장점"],
      cons: ["알파 단점"],
      fit: { risk: ["balanced"], horizon: ["long"], description: "알파 적합" },
      risk_level: 4,
      effort_level: 2,
      base_allocation: [
        { asset: "equity_kr", weight: 70, note: "성장 축" },
        { asset: "bond_mid", weight: 30, note: "방어 축" },
      ],
      allocation_basis: "알파 배분 근거",
      rebalancing: { frequency: "연 1회", band_pct: 5, ideas: ["알파 리밸런싱"] },
      references: ["알파 책"],
    },
    {
      id: "beta",
      master: "대가 베타",
      title: "전략 베타",
      tagline: "베타 태그라인",
      summary: "베타 요약",
      principles: ["베타 원칙 1"],
      pros: ["베타 장점"],
      cons: ["베타 단점"],
      fit: { risk: ["conservative"], horizon: ["mid"], description: "베타 적합" },
      risk_level: 2,
      effort_level: 1,
      base_allocation: [{ asset: "bond_mid", weight: 100, note: "전량 방어" }],
      allocation_basis: "베타 배분 근거",
      rebalancing: { frequency: "반기 1회", band_pct: 5, ideas: ["베타 리밸런싱"] },
      references: ["베타 책"],
    },
  ],
};

const SIMULATE_FIXTURE = {
  disclaimer: CATALOG_FIXTURE.disclaimer,
  profile: { risk: "conservative", horizon: "short", asset_groups: ["equity", "bond"] },
  results: [
    {
      strategy_id: "beta",
      master: "대가 베타",
      title: "전략 베타",
      fit_score: 90,
      fit_reasons: ["성향이 잘 맞습니다."],
      allocation: [
        { asset: "bond_mid", label: "중기 국채", group: "bond", weight: 100, note: "" },
      ],
      adjustments: [],
      note: null,
      rebalancing: { frequency: "반기 1회", ideas: [] },
    },
    {
      strategy_id: "alpha",
      master: "대가 알파",
      title: "전략 알파",
      fit_score: 40,
      fit_reasons: ["기간이 짧습니다."],
      allocation: [
        { asset: "equity_kr", label: "국내 주식", group: "equity", weight: 45, note: "" },
        { asset: "bond_mid", label: "중기 국채", group: "bond", weight: 55, note: "" },
      ],
      adjustments: ["주식 비중 25%p 를 채권·현금성으로 옮겼습니다."],
      note: null,
      rebalancing: { frequency: "연 1회", ideas: [] },
    },
  ],
};

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function buildDom() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="mastersDisclaimer"></div>
    <div id="mastersCards"></div>
    <div id="mastersDetail"></div>
    <div id="mastersCompare"></div>
    <div id="mastersSimForm"></div>
    <div id="mastersSimResults"></div>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/masters",
  });
  const { window: w } = dom;
  w.requestAnimationFrame = (cb) => cb();
  w.matchMedia = () => ({ matches: false, addListener() {}, removeListener() {}, addEventListener() {}, removeEventListener() {} });
  appendScript(w, UTILS_SRC);
  appendScript(w, MASTERS_SRC);
  return w;
}

function installApiStub(w, { simulate = SIMULATE_FIXTURE } = {}) {
  const calls = [];
  w.apiFetchJson = async (path, options = {}) => {
    calls.push({ path, options });
    if (path === "/api/masters/strategies") return structuredClone(CATALOG_FIXTURE);
    if (path === "/api/masters/simulate") return structuredClone(simulate);
    throw new Error(`unexpected path: ${path}`);
  };
  return calls;
}

test("loadMasters 는 disclaimer·전략 카드·상세·비교표·시뮬 폼을 렌더한다", async () => {
  const w = buildDom();
  const calls = installApiStub(w);
  await w.loadMasters();

  assert.equal(calls.length, 1);
  assert.match(w.document.getElementById("mastersDisclaimer").textContent, /참고용 시뮬레이션/);

  const cards = w.document.querySelectorAll("#mastersCards button[data-strategy]");
  assert.equal(cards.length, 2);
  assert.match(cards[0].textContent, /대가 알파/);
  // 기본 선택(첫 전략) 카드에 active 표시가 붙는다.
  assert.ok(cards[0].classList.contains("active"));

  // 첫 전략이 기본 선택 — 상세에 원칙·배분 막대·리밸런싱이 보인다.
  const detail = w.document.getElementById("mastersDetail");
  assert.match(detail.textContent, /알파 원칙 1/);
  assert.match(detail.textContent, /알파 리밸런싱/);
  assert.equal(detail.querySelectorAll(".masters-bar-seg").length, 2);

  const compare = w.document.getElementById("mastersCompare");
  assert.equal(compare.querySelectorAll("thead th").length, 3); // 라벨 열 + 전략 2
  assert.match(compare.textContent, /베타 태그라인/);

  const form = w.document.getElementById("mastersSimForm");
  assert.ok(form.querySelector("#mastersSimRisk"));
  assert.ok(form.querySelector("#mastersSimHorizon"));
  assert.equal(form.querySelectorAll(".js-masters-group").length, 2);
});

test("force 없이 다시 부르면 카탈로그를 다시 받지 않는다", async () => {
  const w = buildDom();
  const calls = installApiStub(w);
  await w.loadMasters();
  await w.loadMasters();
  assert.equal(calls.length, 1);
});

test("카드를 클릭하면 상세가 해당 전략으로 바뀐다", async () => {
  const w = buildDom();
  installApiStub(w);
  await w.loadMasters();

  const betaCard = w.document.querySelector('#mastersCards button[data-strategy="beta"]');
  betaCard.click();

  const detail = w.document.getElementById("mastersDetail");
  assert.match(detail.textContent, /베타 원칙 1/);
  assert.ok(betaCard.classList.contains("active"));
});

test("시뮬레이션은 선택값을 profile 로 보내고 결과·조정 내역·disclaimer 를 렌더한다", async () => {
  const w = buildDom();
  const calls = installApiStub(w);
  await w.loadMasters();

  w.document.getElementById("mastersSimRisk").value = "conservative";
  w.document.getElementById("mastersSimHorizon").value = "short";
  await w._runMastersSimulation();

  const simCall = calls.find(c => c.path === "/api/masters/simulate");
  assert.ok(simCall, "simulate should be called");
  const body = JSON.parse(simCall.options.body);
  assert.deepEqual(body.profile, { risk: "conservative", horizon: "short", asset_groups: ["equity", "bond"] });

  const results = w.document.getElementById("mastersSimResults");
  assert.match(results.textContent, /참고용 시뮬레이션/);
  const simCards = results.querySelectorAll(".masters-sim-card");
  assert.equal(simCards.length, 2);
  // fit_score 상위가 먼저, 최상위에는 배지가 붙는다.
  assert.match(simCards[0].textContent, /대가 베타/);
  assert.ok(simCards[0].querySelector(".masters-chip.best"));
  assert.match(simCards[1].textContent, /주식 비중 25%p/);
});

test("선호 자산군을 모두 끄면 요청 없이 안내만 보여준다", async () => {
  const w = buildDom();
  const calls = installApiStub(w);
  await w.loadMasters();

  w.document.querySelectorAll("#mastersSimForm .js-masters-group").forEach(el => { el.checked = false; });
  await w._runMastersSimulation();

  assert.ok(!calls.some(c => c.path === "/api/masters/simulate"));
  assert.match(w.document.getElementById("mastersSimResults").textContent, /최소 1개/);
});
