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
  instruments: {
    equity_kr: [{ code: "069500", name: "KODEX 200", type: "ETF", note: "국내 대형주" }],
    bond_mid: [{ code: "148070", name: "KIWOOM 국고채10년", type: "ETF", note: "중기 국채" }],
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
  strategy: { id: "alpha", master: "대가 알파", title: "전략 알파" },
  fit_score: 40,
  fit_reasons: ["기간이 짧습니다."],
  adjustments: ["주식 비중 25%p 를 채권·현금성으로 옮겼습니다."],
  note: null,
  allocation: [
    { asset: "bond_mid", label: "중기 국채", group: "bond", weight: 55, note: "" },
    { asset: "equity_kr", label: "국내 주식", group: "equity", weight: 45, note: "" },
  ],
  portfolio: [
    {
      asset: "bond_mid", asset_label: "중기 국채", group: "bond", weight: 55,
      instrument: { code: "148070", name: "KIWOOM 국고채10년", type: "ETF", note: "중기 국채" },
      amount: 5500000, price: 100000, shares: 55, est_cost: 5500000,
    },
    {
      asset: "equity_kr", asset_label: "국내 주식", group: "equity", weight: 45,
      instrument: { code: "069500", name: "KODEX 200", type: "ETF", note: "국내 대형주" },
      amount: 4500000, price: 130000, shares: 34, est_cost: 4420000,
    },
  ],
  amount: { total: 10000000, invested: 9920000, residual_cash: 80000 },
  implementation_note: "개별 종목은 밸류 스크리너로.",
  rebalancing: { frequency: "연 1회", ideas: [] },
  quotes_incomplete: false,
};

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

const REVIEW_FIXTURE = {
  disclaimer: CATALOG_FIXTURE.disclaimer,
  strategy: { id: "alpha", master: "대가 알파", title: "전략 알파" },
  markdown: "## 총평\n- 집중도가 높습니다.",
  breakdown: {
    total_value: 10000000, holdings_count: 2, top3_weight: 100,
    asset_weights: [
      { asset: "equity_kr", label: "국내 주식", group: "equity", weight: 70 },
      { asset: "bond_mid", label: "중기 국채", group: "bond", weight: 30 },
    ],
    unpriced: [],
  },
  gap: [
    { asset: "equity_kr", label: "국내 주식", mine: 70, target: 70, diff: 0 },
    { asset: "bond_mid", label: "중기 국채", mine: 30, target: 30, diff: 0 },
  ],
  model: "test/model",
  truncated: false,
};

function buildDom() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="mastersDisclaimer"></div>
    <div id="mastersCards"></div>
    <div id="mastersDetail"></div>
    <div id="mastersCompare"></div>
    <div id="mastersSimForm"></div>
    <div id="mastersSimResults"></div>
    <div id="mastersReviewControls"></div>
    <div id="mastersReviewResult"></div>
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

function installApiStub(w, { simulate = SIMULATE_FIXTURE, review = REVIEW_FIXTURE } = {}) {
  const calls = [];
  w.apiFetchJson = async (path, options = {}) => {
    calls.push({ path, options });
    if (path === "/api/masters/strategies") return structuredClone(CATALOG_FIXTURE);
    if (path === "/api/masters/simulate") return structuredClone(simulate);
    if (path === "/api/masters/review") return structuredClone(review);
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
  assert.ok(form.querySelector("#mastersSimAmount"));
  assert.equal(form.querySelectorAll(".js-masters-group").length, 2);
  // 대가 선택 select — 기본값은 현재 선택된 카드(첫 전략)와 동기화된다.
  const strategySelect = form.querySelector("#mastersSimStrategy");
  assert.ok(strategySelect);
  assert.equal(strategySelect.querySelectorAll("option").length, 2);
  assert.equal(strategySelect.value, "alpha");
});

test("force 없이 다시 부르면 카탈로그를 다시 받지 않는다", async () => {
  const w = buildDom();
  const calls = installApiStub(w);
  await w.loadMasters();
  await w.loadMasters();
  assert.equal(calls.length, 1);
});

test("카드를 클릭하면 상세와 시뮬 폼의 대가 선택이 함께 바뀐다", async () => {
  const w = buildDom();
  installApiStub(w);
  await w.loadMasters();

  const betaCard = w.document.querySelector('#mastersCards button[data-strategy="beta"]');
  betaCard.click();

  const detail = w.document.getElementById("mastersDetail");
  assert.match(detail.textContent, /베타 원칙 1/);
  assert.ok(betaCard.classList.contains("active"));
  assert.equal(w.document.getElementById("mastersSimStrategy").value, "beta");
});

test("시뮬 폼에서 대가를 바꾸면 카드 선택·상세도 따라간다", async () => {
  const w = buildDom();
  installApiStub(w);
  await w.loadMasters();

  const select = w.document.getElementById("mastersSimStrategy");
  select.value = "beta";
  select.dispatchEvent(new w.Event("change", { bubbles: true }));

  assert.match(w.document.getElementById("mastersDetail").textContent, /베타 원칙 1/);
  const betaCard = w.document.querySelector('#mastersCards button[data-strategy="beta"]');
  assert.ok(betaCard.classList.contains("active"));
});

test("시뮬레이션은 대가·성향·금액을 보내고 상품 단위 포트폴리오를 렌더한다", async () => {
  const w = buildDom();
  const calls = installApiStub(w);
  await w.loadMasters();

  w.document.getElementById("mastersSimStrategy").value = "alpha";
  w.document.getElementById("mastersSimRisk").value = "conservative";
  w.document.getElementById("mastersSimHorizon").value = "short";
  w.document.getElementById("mastersSimAmount").value = "10000000";
  await w._runMastersSimulation();

  const simCall = calls.find(c => c.path === "/api/masters/simulate");
  assert.ok(simCall, "simulate should be called");
  const body = JSON.parse(simCall.options.body);
  assert.equal(body.strategy_id, "alpha");
  assert.equal(body.amount, 10000000);
  assert.deepEqual(body.profile, { risk: "conservative", horizon: "short", asset_groups: ["equity", "bond"] });

  const results = w.document.getElementById("mastersSimResults");
  assert.match(results.textContent, /참고용 시뮬레이션/);
  // 단일 대가 카드 하나 + 상품 테이블(코드·주수·잔여 현금).
  const simCards = results.querySelectorAll(".masters-sim-card");
  assert.equal(simCards.length, 1);
  assert.match(simCards[0].textContent, /대가 알파/);
  const table = simCards[0].querySelector(".masters-portfolio-table");
  assert.ok(table, "portfolio table should render");
  assert.equal(table.querySelectorAll("tbody tr").length, 2);
  assert.match(table.textContent, /KODEX 200/);
  assert.match(table.textContent, /069500/);
  assert.match(table.textContent, /34주/);
  assert.match(simCards[0].textContent, /잔여 현금 80,000원/);
  assert.match(simCards[0].textContent, /주식 비중 25%p/);
  assert.match(simCards[0].textContent, /밸류 스크리너/);
});

test("금액 없이 돌리면 amount 를 보내지 않는다", async () => {
  const w = buildDom();
  const calls = installApiStub(w);
  await w.loadMasters();

  await w._runMastersSimulation();

  const simCall = calls.find(c => c.path === "/api/masters/simulate");
  assert.ok(simCall);
  const body = JSON.parse(simCall.options.body);
  assert.equal(body.amount, undefined);
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

test("로그인 없으면 진단 섹션은 안내만 보여준다", async () => {
  const w = buildDom();
  installApiStub(w);
  await w.loadMasters();

  const controls = w.document.getElementById("mastersReviewControls");
  assert.match(controls.textContent, /로그인하면/);
  assert.equal(controls.querySelector("#mastersReviewRunBtn"), null);
});

test("로그인 상태에서 진단 실행 — strategy_id 전송, 갭 표와 마크다운 폴백 렌더", async () => {
  const w = buildDom();
  const calls = installApiStub(w);
  // currentUser 는 utils.js 의 전역 let 바인딩 — window 프로퍼티 대입은 가려지므로
  // 실제 auth.js 처럼 바인딩에 직접 대입한다.
  w.eval('currentUser = { google_sub: "u1", name: "tester" };');
  await w.loadMasters();

  const btn = w.document.getElementById("mastersReviewRunBtn");
  assert.ok(btn, "review button should render when logged in");
  assert.match(btn.textContent, /대가 알파/);

  await w._runMastersReview();

  const reviewCall = calls.find(c => c.path === "/api/masters/review");
  assert.ok(reviewCall, "review should be called");
  assert.equal(JSON.parse(reviewCall.options.body).strategy_id, "alpha");
  assert.ok(reviewCall.options.timeoutMs >= 60000, "LLM call needs a long timeout");

  const result = w.document.getElementById("mastersReviewResult");
  assert.match(result.textContent, /참고용 시뮬레이션/);
  assert.match(result.textContent, /상위3 집중도 100%/);
  const gapTable = result.querySelector(".masters-portfolio-table");
  assert.ok(gapTable, "gap table should render");
  assert.match(gapTable.textContent, /국내 주식/);
  // jsdom 에는 marked/DOMPurify 가 없으므로 이스케이프 폴백으로 렌더된다.
  const md = result.querySelector(".masters-review-md-plain");
  assert.ok(md, "markdown fallback should render without marked");
  assert.match(md.textContent, /총평/);
});

test("카드에서 대가를 바꾸면 진단 버튼 라벨도 따라간다", async () => {
  const w = buildDom();
  installApiStub(w);
  w.eval('currentUser = { google_sub: "u1" };');
  await w.loadMasters();

  w.document.querySelector('#mastersCards button[data-strategy="beta"]').click();

  assert.match(w.document.getElementById("mastersReviewRunBtn").textContent, /대가 베타/);
});
