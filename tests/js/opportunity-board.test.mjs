// jsdom 행위 테스트: 기회 보드(static/js/opportunity-board.js). 마크업은 실제 index.html 의 #opportunitiesView 를 쓴다.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { JSDOM } from "jsdom";
import { publicProjection } from "../../scripts/sync-ecosystem.mjs";

const read = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), "utf8");
const INDEX = read("static/index.html");
const VIEW = INDEX.slice(INDEX.indexOf('<div id="opportunitiesView"'), INDEX.indexOf('<div id="quantView"'));
const ECOSYSTEM = publicProjection(JSON.parse(read("config/ecosystem.json")));
const SOURCES = ["static/js/utils.js", "static/js/ecosystem-links.js", "static/js/opportunity-board.js"].map(read);

function load({ response, rejects = false } = {}) {
  const dom = new JSDOM(`<!doctype html><html data-theme="light"><body>${VIEW}</body></html>`, {
    url: "https://app.example.com/opportunities",
    runScripts: "dangerously",
  });
  const w = dom.window;
  w.APP_CONFIG = { ecosystem: ECOSYSTEM, integrations: {} };
  for (const src of SOURCES) {
    const script = w.document.createElement("script");
    script.textContent = src;
    w.document.body.appendChild(script);
  }
  w.__calls = [];
  w.__queue = [response];
  w.apiFetchJson = async (path, options) => {
    w.__calls.push({ path, options });
    if (rejects) throw new Error("offline");
    return w.__queue.length > 1 ? w.__queue.shift() : w.__queue[0];
  };
  return w;
}

function signal(over = {}) {
  return {
    tool: "common_preferred_spread", kind: "preferred", label: "우선주 괴리", code: "005935", relatedCode: "005930",
    name: "삼성전자우", pairName: "삼성전자", score: 92, scoreBasis: "history3y",
    scoreNote: "자기 3년 분포에서 현재 괴리율 이하인 비율", rankable: true,
    metrics: [
      { label: "괴리율", value: 19.59, unit: "%" },
      { label: "전일 대비", value: -0.81, unit: "%p" },
      { label: "3년 백분위", value: 92, unit: "pctile" },
    ],
    asOf: "2026-11-06", distributionEnd: "2026-11-06", warnings: [], stale: false,
    url: "https://ducklove.github.io/common_preferred_spread/?code=005935",
    ...over,
  };
}

function row(rank, over = {}, signalOver = {}) {
  const primary = signal(signalOver);
  return {
    rank, code: primary.code, name: primary.name, primary, others: [],
    liquidity: { value: 3e10, basis: "avg20d", asOf: "2026-11-06", low: false },
    ...over,
  };
}

const HOLDING = {
  tool: "holding_value", label: "지주사 할인", code: "000670", name: "영풍", pairName: "영풍→고려아연", score: 93,
  relatedCode: null, metrics: [{ label: "보유가치/시총", value: 917.34, unit: "%" }],
  url: "https://ducklove.github.io/holding_value/?code=000670",
};
const BUYBACK = {
  tool: "buybacks", label: "자사주", code: "115310", name: "인포바인", pairName: undefined, score: 99.9,
  scoreBasis: "crossSection", scoreNote: "상장사 1,622개 중 자사주 비율 이하 비율", relatedCode: null,
  metrics: [{ label: "자사주 비율", value: 54.1846, unit: "%" }], asOf: "2025-12-31", distributionEnd: null,
  url: "https://ducklove.github.io/buybacks/?stock=115310",
};

function tool(id, label, over = {}) {
  return { tool: id, label, status: "ok", asOf: "2026-11-06", distributionEnd: null, warning: null, count: 10, rankable: 10, ...over };
}

function board(over = {}) {
  return {
    generatedAt: "2026-11-09T19:00:00+09:00",
    rows: [row(1), row(2, {}, HOLDING), row(3, { liquidity: { value: null, basis: null, asOf: null, low: null } }, BUYBACK)],
    tools: [
      tool("common_preferred_spread", "우선주 괴리"),
      tool("holding_value", "지주사 할인"),
      tool("spac-hunter", "스팩 청산가치"),
      tool("buybacks", "자사주", { asOf: "2025-12-31" }),
      tool("nps-tracker", "국민연금", { asOf: "2025-12-31", warning: "지분율 변화 데이터가 없어 순위에 넣지 않고 문맥으로만 표시합니다" }),
    ],
    eligible: { common_preferred_spread: 2, holding_value: 1, buybacks: 326 },
    minScore: 80,
    perTool: 12,
    heldCodes: [],
    ...over,
  };
}

const rows = (w) => [...w.document.querySelectorAll(".opportunity-row")];
const text = (el) => el.textContent.replace(/\s+/g, " ").trim();

test("순위·종목·점수·수치·거래대금·기준일을 서버 순서대로 그린다", async () => {
  const w = load({ response: board() });
  await w.loadOpportunityBoard();
  assert.equal(w.__calls[0].path, "/api/external/opportunities");
  const [first, second] = rows(w);
  assert.equal(first.querySelector(".opportunity-rank").textContent, "1");
  assert.match(text(first.querySelector(".opportunity-stock")), /삼성전자우.*005935.*삼성전자/);
  assert.match(text(first.querySelector(".opportunity-score")), /^92 ?자기 3년 분포$/);
  assert.match(text(first.querySelector(".opportunity-metrics")), /괴리율 ?19\.59%/);
  assert.match(text(first.querySelector(".opportunity-liquidity")), /20일 평균 300\.0억원.*2026-11-06/);
  assert.match(text(first.querySelector(".opportunity-asof")), /2026-11-06.*분포 2026-11-06까지/);
  assert.equal(second.querySelector("[data-ob-stock]").dataset.obStock, "000670");
});

test("거래대금을 모르면 0이 아니라 미확인으로 표시한다", async () => {
  const w = load({ response: board() });
  await w.loadOpportunityBoard();
  const cell = rows(w)[2].querySelector(".opportunity-liquidity");
  assert.equal(text(cell), "거래대금 미확인");
  assert.doesNotMatch(text(cell), /\b0\b/);
});

test("거래대금이 1억 미만이면 표시한다", async () => {
  const response = board({ rows: [row(1, { liquidity: { value: 55349725, basis: "avg20d", asOf: "2026-11-06", low: true } })] });
  const w = load({ response });
  await w.loadOpportunityBoard();
  const cell = rows(w)[0].querySelector(".opportunity-liquidity");
  assert.ok(cell.classList.contains("is-low"));
  assert.match(text(cell), /1억 미만/);
});

test("오래된 데이터는 행과 도구 상태에 경고를 낸다", async () => {
  const warning = "일봉 분포가 2026-10-07까지입니다(21거래일 지연). 현재 값만 최신입니다";
  const response = board({
    rows: [row(1, {}, { ...HOLDING, stale: true, warnings: [warning], asOf: "2026-11-07", distributionEnd: "2026-10-07" })],
    tools: [tool("holding_value", "지주사 할인", { warning, distributionEnd: "2026-10-07" })],
  });
  const w = load({ response });
  await w.loadOpportunityBoard();
  const line = rows(w)[0];
  assert.ok(line.classList.contains("is-stale"));
  assert.equal(line.querySelector(".opportunity-warning").textContent, warning);
  assert.match(text(line.querySelector(".opportunity-asof")), /2026-11-07.*분포 2026-10-07까지/);
  const status = w.document.querySelector(".opportunity-tool");
  assert.ok(status.classList.contains("is-warning"));
  assert.match(text(status), /일봉 분포가 2026-10-07까지/);
});

test("한 도구가 실패해도 나머지 후보는 표시되고 실패를 알린다", async () => {
  const response = board({
    rows: [row(1)],
    tools: [
      tool("common_preferred_spread", "우선주 괴리"),
      tool("holding_value", "지주사 할인", { status: "failed", asOf: null, warning: "이번 조회에서 데이터를 받지 못했습니다", count: 0, rankable: 0 }),
    ],
    eligible: { common_preferred_spread: 1 },
  });
  const w = load({ response });
  await w.loadOpportunityBoard();
  assert.equal(rows(w).length, 1);
  const failed = w.document.querySelector(".opportunity-tool.is-failed");
  assert.match(text(failed), /지주사 할인.*받지 못했습니다/);
  assert.doesNotMatch(text(failed), /후보/);
});

test("모든 도구가 실패하면 빈 표 대신 이유를 보여 준다", async () => {
  const response = board({
    rows: [],
    tools: [tool("holding_value", "지주사 할인", { status: "failed", warning: "이번 조회에서 데이터를 받지 못했습니다" })],
    eligible: {},
  });
  const w = load({ response });
  await w.loadOpportunityBoard();
  assert.match(text(w.document.getElementById("opportunityBoard")), /도구 데이터를 받지 못했습니다/);
  assert.equal(w.document.querySelector("table"), null);
});

test("후보가 없으면 기준 점수를 알려 준다", async () => {
  const w = load({ response: board({ rows: [], eligible: {} }) });
  await w.loadOpportunityBoard();
  assert.match(text(w.document.getElementById("opportunityBoard")), /점수 80 이상인 후보가 없습니다/);
});

test("도구별 상한으로 잘린 후보 수를 드러낸다", async () => {
  const w = load({ response: board() });
  await w.loadOpportunityBoard();
  const statuses = [...w.document.querySelectorAll(".opportunity-tool")].map(text);
  assert.ok(statuses.some((value) => /자사주.*후보 326개 중 상위 12개/.test(value)));
  assert.ok(statuses.some((value) => /우선주 괴리.*후보 2개/.test(value)));
});

test("도구 필터는 대표 신호로 거르고 후보가 있는 도구만 보여 준다", async () => {
  const w = load({ response: board() });
  await w.loadOpportunityBoard();
  const filters = () => [...w.document.querySelectorAll("[data-ob-tool]")];
  assert.deepEqual(filters().map((el) => el.textContent), ["전체", "우선주 괴리", "지주사 할인", "자사주"]);
  assert.equal(filters()[0].getAttribute("aria-pressed"), "true");
  filters().find((el) => el.dataset.obTool === "holding_value").click();
  assert.deepEqual(rows(w).map((el) => el.querySelector("[data-ob-stock]").dataset.obStock), ["000670"]);
  assert.equal(filters().find((el) => el.dataset.obTool === "holding_value").getAttribute("aria-pressed"), "true");
  filters()[0].click();
  assert.equal(rows(w).length, 3);
});

test("거래대금 필터는 미확인 후보도 숨기고 숨긴 수를 알려 준다", async () => {
  const w = load({ response: board() });
  await w.loadOpportunityBoard();
  const select = w.document.getElementById("opportunityMinLiquidity");
  select.value = "100000000";
  select.dispatchEvent(new w.Event("change", { bubbles: true }));
  assert.equal(rows(w).length, 2);
  assert.match(text(w.document.querySelector(".opportunity-footnote")), /후보 1개를 숨겼습니다.*확인하지 못한 후보/);
  select.value = "10000000000";
  select.dispatchEvent(new w.Event("change", { bubbles: true }));
  assert.equal(rows(w).length, 2);
  select.value = "0";
  select.dispatchEvent(new w.Event("change", { bubbles: true }));
  assert.equal(rows(w).length, 3);
  assert.equal(w.document.querySelector(".opportunity-footnote"), null);
});

test("조건에 맞는 후보가 없으면 안내한다", async () => {
  const response = board({ rows: [row(1, { liquidity: { value: 5e7, basis: "avg20d", asOf: "2026-11-06", low: true } })] });
  const w = load({ response });
  await w.loadOpportunityBoard();
  const select = w.document.getElementById("opportunityMinLiquidity");
  select.value = "1000000000";
  select.dispatchEvent(new w.Event("change", { bubbles: true }));
  assert.match(text(w.document.getElementById("opportunityBoard")), /이 조건에 맞는 후보가 없습니다/);
});

test("보유 표시는 배지일 뿐 순서를 바꾸지 않는다", async () => {
  const w = load({ response: board({ heldCodes: ["115310"] }) });
  await w.loadOpportunityBoard();
  const list = rows(w);
  assert.equal(list[0].querySelector(".opportunity-held"), null);
  assert.equal(list[2].querySelector(".opportunity-held").textContent, "보유");
  assert.equal(list[2].querySelector(".opportunity-rank").textContent, "3");
});

test("종목 이름을 누르면 분석 화면으로 이동한다", async () => {
  const w = load({ response: board() });
  const moves = [];
  w.switchView = (view) => moves.push(["view", view]);
  w.analyzeStock = (code) => moves.push(["stock", code]);
  await w.loadOpportunityBoard();
  const link = rows(w)[0].querySelector("[data-ob-stock]");
  assert.equal(link.getAttribute("href"), "/analysis?code=005935");
  link.click();
  assert.deepEqual(moves, [["view", "analysis"], ["stock", "005935"]]);
});

test("도구 링크는 레지스트리 딥링크이고 새 탭으로 열린다", async () => {
  const w = load({ response: board() });
  await w.loadOpportunityBoard();
  const link = rows(w)[0].querySelector(".opportunity-tool-link");
  assert.match(link.getAttribute("href"), /common_preferred_spread\/\?code=005935/);
  assert.equal(link.getAttribute("target"), "_blank");
  assert.equal(link.dataset.vcTool, "common_preferred_spread");
});

test("같은 종목의 다른 도구 신호는 문맥으로 붙는다", async () => {
  const others = [
    { tool: "buybacks", label: "자사주", code: "005930", score: 50, metrics: [{ label: "자사주 비율", value: 1.55, unit: "%" }] },
    { tool: "nps-tracker", label: "국민연금", code: "005935", score: null, metrics: [{ label: "공시 지분율", value: 7.26, unit: "%" }] },
    { tool: "nps-tracker", label: "국민연금", code: "000000", score: null, metrics: [] },
    { tool: "buybacks", label: "자사주", code: "005935", score: 7, metrics: [{ label: "자사주 비율", value: 0, unit: "%" }] },
  ];
  const w = load({ response: board({ rows: [row(1, { others })] }) });
  await w.loadOpportunityBoard();
  const chips = [...rows(w)[0].querySelectorAll(".opportunity-context")];
  assert.deepEqual(chips.map(text), ["자사주 1.55%", "국민연금 7.26%"]);
  assert.equal(chips[0].getAttribute("title"), "005930 기준");
  assert.equal(chips[1].getAttribute("title"), null);
});

test("서버 문자열은 이스케이프한다", async () => {
  const hostile = '<img src=x onerror="window.__xss=1">';
  const w = load({
    response: board({
      rows: [row(1, { name: hostile }, { name: hostile, pairName: hostile, label: hostile, warnings: [hostile],
                                         metrics: [{ label: hostile, value: hostile, unit: "text" }] })],
      tools: [tool("common_preferred_spread", hostile, { warning: hostile })],
    }),
  });
  await w.loadOpportunityBoard();
  assert.equal(w.document.querySelector("#opportunitiesView img"), null);
  assert.equal(w.__xss, undefined);
});

test("안전하지 않은 도구 URL 은 링크로 만들지 않는다", async () => {
  const w = load({
    response: board({ rows: [row(1, {}, { tool: "unknown-tool", url: "javascript:alert(1)" })] }),
  });
  await w.loadOpportunityBoard();
  assert.equal(rows(w)[0].querySelector(".opportunity-tool-link").getAttribute("href"), "#");
});

test("응답이 없으면 안내하고, 한번 받은 후보는 다음 실패에서 지우지 않는다", async () => {
  const first = load({ response: null });
  await first.loadOpportunityBoard();
  assert.match(text(first.document.getElementById("opportunityBoard")), /불러오지 못했습니다/);

  const thrown = load({ rejects: true });
  await thrown.loadOpportunityBoard();
  assert.match(text(thrown.document.getElementById("opportunityBoard")), /불러오지 못했습니다/);

  const w = load({ response: board() });
  w.__queue = [board(), null];
  await w.loadOpportunityBoard();
  assert.equal(rows(w).length, 3);
  await w.loadOpportunityBoard();
  assert.equal(rows(w).length, 3);
});

test("늦게 도착한 이전 응답은 최신 화면을 덮지 않는다", async () => {
  const w = load({ response: board() });
  const resolvers = [];
  w.apiFetchJson = () => new Promise((resolve) => resolvers.push(resolve));
  const older = w.loadOpportunityBoard();
  const newer = w.loadOpportunityBoard();
  resolvers[1](board({ rows: [row(1)] }));
  await newer;
  resolvers[0](board());
  await older;
  assert.equal(rows(w).length, 1);
});

test("다시 열어도 이벤트는 한 번만 묶이고 사라진 도구 필터는 전체로 돌아간다", async () => {
  const w = load({ response: board() });
  w.__queue = [board(), board({ rows: [row(1)] })];
  const moves = [];
  w.switchView = (view) => moves.push(view);
  w.analyzeStock = () => {};
  await w.loadOpportunityBoard();
  [...w.document.querySelectorAll("[data-ob-tool]")].find((el) => el.dataset.obTool === "holding_value").click();
  await w.loadOpportunityBoard();
  assert.equal(rows(w).length, 1);
  assert.equal(w.document.querySelector('[data-ob-tool="all"]').getAttribute("aria-pressed"), "true");
  rows(w)[0].querySelector("[data-ob-stock]").click();
  assert.deepEqual(moves, ["analysis"]);
});

test("백분위를 수익 보장으로 표현하지 않고 비용 수치를 만들지 않는다", async () => {
  const w = load({ response: board() });
  await w.loadOpportunityBoard();
  const page = text(w.document.getElementById("opportunitiesView"));
  assert.match(page, /수익을 뜻하지 않습니다/);
  assert.match(page, /거래비용을 추정할 수 없어 비용을 뺀 수치는 표시하지 않습니다/);
  for (const banned of ["수익 보장", "보장합니다", "확정 수익", "기대수익률", "매수 추천", "순수익", "세후"]) {
    assert.ok(!page.includes(banned), banned);
    assert.ok(!read("static/js/opportunity-board.js").includes(banned), banned);
  }
});
