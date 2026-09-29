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
  // These registration panels and the holdings request are outside this fixture.
  window.pfInitInitialRegistration = () => {};
  window.pfInitHoldingRemoval = () => {};
  window.fetch = async () => ({ json: async () => [] });
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
  assert.equal(openedUrl, "https://ducklove.duckdns.org:3691/api/portfolio/open/spacHunter?code=0131D0&theme=light");
});

test("일반 국내 종목은 '분석 화면' 액션을 유지한다", () => {
  const w = loadPortfolioActions([{ stock_code: "005930", stock_name: "삼성전자" }]);
  const actions = w._portfolioLinkActions("005930", { includeInsight: false });
  assert.equal(actions[0].label, "분석 화면");
});

function insightCards(w, data) {
  const container = w.document.createElement('div');
  container.innerHTML = w._renderAssetInsight(data);
  return Object.fromEntries([...container.querySelectorAll('.pf-insight-card')].map(card => [
    card.querySelector('.pf-insight-card-label').textContent,
    card.querySelector('.pf-insight-card-value').textContent,
  ]));
}

test('스팩 인사이트는 일반 기업 지표 대신 청산 지표와 상장일을 표시한다', () => {
  const w = loadPortfolioActions([{ stock_code: '0209J0', stock_name: 'KB제34호스팩' }]);
  const cards = insightCards(w, {
    profile: { code: '0209J0', name: 'KB제34호스팩', isSpac: true },
    valuation: { applicable: true, per: 10, pbr: 1, roe: 10, treasuryShareRatioPct: 5 },
    spac: { applicable: true, currentLiquidationValue: 2001.54, liquidationDiscountPct: 5.97,
      annualizedReturnPct: 4.06, listingDate: '2026-09-22', asOf: '2026-09-29' },
  });
  assert.equal(cards['청산가'], '2,001.54');
  assert.equal(cards['청산가 괴리율'], '+5.97%');
  assert.equal(cards['연환산 기대수익률'], '+4.06%');
  assert.equal(cards['상장일'], '2026-09-22');
  for (const label of ['PBR', 'PER', 'ROE', '자사주 비율']) assert.ok(!(label in cards));
  assert.ok('현재가' in cards);
  w.close();
});

test('스팩 원본 데이터가 없어도 청산 항목을 유지하고 0%는 누락하지 않는다', () => {
  const w = loadPortfolioActions([{ stock_code: '0209J0', stock_name: 'KB제34호스팩' }]);
  const profile = { code: '0209J0', name: 'KB제34호스팩', isSpac: true };
  const cards = insightCards(w, { profile, valuation: { applicable: true } });
  for (const label of ['청산가', '청산가 괴리율', '연환산 기대수익률', '상장일']) assert.equal(cards[label], '-');
  assert.ok(!('PER' in cards));
  const zero = insightCards(w, { profile, spac: { liquidationDiscountPct: 0, annualizedReturnPct: 0 } });
  assert.equal(zero['청산가 괴리율'], '0.00%');
  assert.equal(zero['연환산 기대수익률'], '0.00%');
  w.close();
});

test('일반 종목 인사이트는 기존 기업 지표를 유지한다', () => {
  const w = loadPortfolioActions([{ stock_code: '005930', stock_name: '삼성전자' }]);
  const cards = insightCards(w, {
    profile: { code: '005930', name: '삼성전자' },
    valuation: { applicable: true, per: 10, pbr: 1, roe: 10, treasuryShareRatioPct: 5 },
  });
  for (const label of ['PBR', 'PER', 'ROE', '자사주 비율']) assert.ok(label in cards);
  assert.ok(!('청산가' in cards));
  w.close();
});
