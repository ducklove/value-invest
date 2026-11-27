import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { JSDOM } from "jsdom";

const read = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), "utf8");
const SOURCES = ["static/js/utils.js", "static/js/analysis-timeline.js"].map(read);

function load(responder) {
  const dom = new JSDOM(`<!doctype html><html><body>
    <section id="stockTimelinePanel" hidden></section></body></html>`,
  { url: "https://app.example.com/analysis", runScripts: "dangerously" });
  const w = dom.window;
  w.APP_CONFIG = { integrations: {} };
  for (const src of SOURCES) {
    const s = w.document.createElement("script");
    s.textContent = src;
    w.document.body.appendChild(s);
  }
  w.eval('_lastAnalysisData = { corp_name: "삼성전자" };');
  w.__calls = [];
  w.apiFetchJson = (path) => { w.__calls.push(path); return responder(path); };
  return w;
}

const OK = {
  stock_code: "005930",
  items: [
    { type: "news", id: "1", date: "2026-11-28", time: "09:31", title: "HBM <공급>", source: "한경", url: "https://n/1" },
    { type: "disclosure", id: "2", date: "2026-11-27", time: "", title: "자기주식취득결정", source: "DART", url: "https://dart.fss.or.kr/x" },
    { type: "news", id: "3", date: "2026-11-27", time: "08:00", title: "악성", source: "", url: "javascript:alert(1)" },
  ],
  sources: { news: { status: "ok", count: 2 }, disclosure: { status: "ok", count: 1 } },
};

test("종목을 열면 뉴스·공시를 날짜순으로 보여 주고 필터로 거른다", async () => {
  const w = load(async () => OK);
  await w.loadStockTimeline("005930");
  const host = w.document.getElementById("stockTimelinePanel");
  assert.equal(host.hidden, false);
  assert.deepEqual(w.__calls, ["/api/stocks/005930/timeline?limit=20"]);
  assert.equal(host.querySelectorAll(".stl-row").length, 3);
  assert.ok(host.innerHTML.includes("HBM &lt;공급&gt;"), "제목 이스케이프");
  assert.equal(host.querySelectorAll('a[href^="javascript"]').length, 0, "https 가 아닌 링크는 걸지 않는다");
  assert.match(host.querySelector(".stl-more").getAttribute("href"), /query=%EC%82%BC%EC%84%B1/);

  host.querySelector('.js-stl-filter[data-filter="disclosure"]').click();
  assert.equal(host.querySelectorAll(".stl-row").length, 1);
  assert.match(host.textContent, /자기주식취득결정/);
});

test("조회 실패와 한도 보호를 '소식 없음'과 구분해 알린다", async () => {
  const w = load(async () => ({
    stock_code: "005930",
    items: [OK.items[0]],
    sources: { news: { status: "ok", count: 1, stale: true }, disclosure: { status: "quota", count: 0 } },
  }));
  await w.loadStockTimeline("005930");
  const note = w.document.querySelector(".stl-note").textContent;
  assert.match(note, /지난 목록/);
  assert.match(note, /한도 보호/);

  const w2 = load(async () => ({ stock_code: "005930", items: [],
    sources: { news: { status: "error" }, disclosure: { status: "error" } } }));
  await w2.loadStockTimeline("005930");
  assert.match(w2.document.getElementById("stockTimelinePanel").textContent, /불러올 수 없습니다/);

  const w3 = load(async () => ({ stock_code: "005930", items: [],
    sources: { news: { status: "empty" }, disclosure: { status: "empty" } } }));
  await w3.loadStockTimeline("005930");
  assert.match(w3.document.getElementById("stockTimelinePanel").textContent, /최근 뉴스와 공시가 없습니다/);
});

test("늦게 도착한 이전 종목 응답은 버리고, 미지원 종목은 카드를 숨긴다", async () => {
  let releaseFirst;
  const w = load((path) => path.includes("000660")
    ? Promise.resolve({ ...OK, stock_code: "000660", items: [{ ...OK.items[1], title: "새 종목 공시" }] })
    : new Promise((r) => { releaseFirst = () => r(OK); }));
  const first = w.loadStockTimeline("005930");
  await w.loadStockTimeline("000660");
  releaseFirst();
  await first;
  const host = w.document.getElementById("stockTimelinePanel");
  assert.match(host.textContent, /새 종목 공시/);
  assert.doesNotMatch(host.textContent, /HBM/);

  const w2 = load(async () => ({ stock_code: "AAPL", items: [],
    sources: { news: { status: "unsupported" }, disclosure: { status: "unsupported" } } }));
  await w2.loadStockTimeline("AAPL");
  assert.equal(w2.document.getElementById("stockTimelinePanel").hidden, true);
});

test("'전체'는 뉴스가 많은 날에도 최근 공시를 섞어 보여 준다", async () => {
  const news = Array.from({ length: 20 }, (_, i) => ({ type: "news", id: `n${i}`, date: "2026-11-28", time: `${String(i).padStart(2, "0")}:00`, title: `기사${i}`, source: "", url: "https://n/x" }));
  const disc = [{ type: "disclosure", id: "d1", date: "2026-11-20", time: "", title: "현금배당결정", source: "DART", url: "https://dart.fss.or.kr/x" }];
  const w = load(async () => ({ stock_code: "005930", items: [...news, ...disc],
    sources: { news: { status: "ok", count: 20 }, disclosure: { status: "ok", count: 1 } } }));
  await w.loadStockTimeline("005930");
  const host = w.document.getElementById("stockTimelinePanel");
  assert.equal(host.querySelectorAll(".stl-news").length, 10);
  assert.equal(host.querySelectorAll(".stl-disclosure").length, 1);
  assert.equal(host.querySelector(".stl-row:last-child .stl-type").textContent, "공시", "날짜순이라 오래된 공시가 뒤");
  host.querySelector('.js-stl-filter[data-filter="news"]').click();
  assert.equal(host.querySelectorAll(".stl-news").length, 20);
});
