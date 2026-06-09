// jsdom behavior test for the SPAC link action in static/js/portfolio-actions.js.
//
// 포트폴리오 종목의 연결 메뉴(투자 인사이트 팝업 포함)는 _portfolioLinkActions()
// 가 만든다. 국내 스팩(종목명에 "스팩")은 "분석 화면" 대신 "스팩 분석"을 받고
// spac-hunter(?code=) 로 연결돼야 한다. 실제 소스 4개를 브라우저와 같은 순서로
// 한 window 에 올려 라벨/링크 동작을 검증한다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

// index.html 의 로드 순서를 그대로 재현. portfolio-actions.js 는 insights.js 가
// 선언하는 _HOLDING_CODES 를 참조하므로 insights.js 까지 함께 올린다.
const SOURCES = [
  read("static", "app-config.js"),
  read("static", "js", "utils.js"),
  read("static", "js", "portfolio-store.js"),
  read("static", "js", "portfolio-actions.js"),
  read("static", "js", "portfolio-insights.js"),
];

function loadPortfolioActions(items) {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window } = dom;
  // jsdom 에는 fetch 가 없다. insights.js 의 holdings 동기화 IIFE 가 호출하므로
  // 거부 프로미스 스텁만 둔다(.catch 로 흡수됨).
  window.fetch = () => Promise.reject(new Error("no fetch in test"));
  for (const src of SOURCES) {
    const script = window.document.createElement("script");
    script.textContent = src;
    window.document.body.appendChild(script);
  }
  window.PfStore.items = items;
  return window;
}

test("스팩 종목은 '분석 화면' 대신 '스팩 분석' 액션을 받고 spac-hunter 로 연결된다", () => {
  const w = loadPortfolioActions([{ stock_code: "0131D0", stock_name: "교보15호스팩" }]);
  const actions = w._portfolioLinkActions("0131D0", { includeInsight: false });

  const first = actions[0];
  assert.equal(first.label, "스팩 분석");

  // openIntegration() 은 window.open(url, ...) 으로 새 탭을 연다. 외부 도구는
  // 현재 앱 테마를 ?theme= 로 받으므로(기본 light), code 앞에 theme 가 붙는다.
  let openedUrl = "";
  w.open = (url) => { openedUrl = url; };
  first.run();
  assert.equal(openedUrl, "https://ducklove.github.io/spac-hunter/?theme=light&code=0131D0");
});

test("일반 국내 종목은 '분석 화면' 액션을 유지한다", () => {
  const w = loadPortfolioActions([{ stock_code: "005930", stock_name: "삼성전자" }]);
  const actions = w._portfolioLinkActions("005930", { includeInsight: false });
  assert.equal(actions[0].label, "분석 화면");
});
