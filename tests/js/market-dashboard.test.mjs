// jsdom behavior tests for static/js/market-dashboard.js (투자정보 dashboard).
//
// Loads utils.js (for escapeHtml) + market-dashboard.js into a jsdom window and
// exercises the pure grouping/formatting/render logic. The network-bound
// loadInvestingDashboard() is not exercised here.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (p) => readFileSync(join(__dirname, "..", "..", "static", "js", p), "utf8");
const UTILS = read("utils.js");
const DASH = read("market-dashboard.js");

function load() {
  const dom = new JSDOM(
    "<!doctype html><html><body>"
      + "<div class='md-grid' id='marketDashboard'>"
      + "<div class='md-main'><div id='mdIndMain'></div><div id='externalTools'></div><div id='marketNews'></div></div>"
      + "<aside class='md-rail'><div id='mdIndRail'></div><div id='marketMovers'></div><div id='marketSectors'></div></aside>"
      + "</div></body></html>",
    { runScripts: "dangerously", url: "https://app.example.com/" },
  );
  for (const src of [UTILS, DASH]) {
    const s = dom.window.document.createElement("script");
    s.textContent = src;
    dom.window.document.body.appendChild(s);
  }
  return dom.window;
}

const CATALOG = {
  KOSPI: { label: "KOSPI", category: "국내 지수" },
  SPX: { label: "S&P 500", category: "해외 지수" },
  CMDT_GC: { label: "금", category: "원자재" },
  USD_KRW: { label: "달러/원", category: "환율" },
  WEIRD: { label: "기타지표", category: "신규카테고리" },
};

test("_mdGroupByCategory orders known categories first, unknown last", () => {
  const w = load();
  const groups = w._mdGroupByCategory(CATALOG);
  const cats = groups.map((g) => g.category);
  assert.deepEqual([...cats], ["국내 지수", "해외 지수", "원자재", "환율", "신규카테고리"]);
  // each group lists its codes
  assert.deepEqual([...groups[0].codes], ["KOSPI"]);
});

test("_mdChange maps direction to class + sign", () => {
  const w = load();
  const up = w._mdChange({ direction: "up", change: "12.3", change_pct: "0.45%" });
  assert.equal(up.cls, "md-up");
  assert.equal(up.text, "+12.3 (+0.45%)");
  const down = w._mdChange({ direction: "down", change: "5", change_pct: "-1.1%" });
  assert.equal(down.cls, "md-down");
  assert.equal(down.text, "-5 (-1.1%)");
  const flat = w._mdChange({ direction: "flat", change: "", change_pct: "" });
  assert.equal(flat.cls, "md-flat");
  assert.equal(flat.text, "");
});

test("_mdChange exposes abs/pct for mobile collapsing", () => {
  const w = load();
  const c = w._mdChange({ direction: "up", change: "1,636.38", change_pct: "2.53%" });
  assert.equal(c.abs, "+1,636.38");
  assert.equal(c.pct, "(+2.53%)");
  assert.equal(c.text, "+1,636.38 (+2.53%)");
});

test("_mdRenderDashboard list rows split change into abs/pct spans", () => {
  const w = load();
  w._mdRenderDashboard(CATALOG, { SPX: { value: "5,300.0", change: "8.0", change_pct: "-0.15%", direction: "down" } });
  const rail = w.document.getElementById("mdIndRail");  // 해외 지수는 이제 레일
  const abs = rail.querySelector(".md-row .md-chg .md-chg-abs");
  const pct = rail.querySelector(".md-row .md-chg .md-chg-pct");
  assert.ok(abs && /-8\.0/.test(abs.textContent), "abs span present");
  assert.ok(pct && /-0\.15%/.test(pct.textContent), "pct span present");
});

test("list rows show '-' placeholder when 전일대비 is missing (값만 있고 변동 없음)", () => {
  const w = load();
  // CNBC UNCH 처럼 값은 있으나 change/change_pct 가 비어있는 경우.
  w._mdRenderDashboard(CATALOG, { USD_KRW: { value: "1,517.60", change: "", change_pct: "", direction: "" } });
  const rail = w.document.getElementById("mdIndRail");
  const usdRow = [...rail.querySelectorAll(".md-row")].find((r) => /달러\/원/.test(r.textContent));
  assert.ok(usdRow, "행은 그대로 유지(정렬 지킴)");
  assert.match(usdRow.querySelector(".md-row-val").textContent, /1,517\.60/);
  // 전일대비 영역은 빈칸이 아니라 '-' 로 채워진다.
  const chg = usdRow.querySelector(".md-chg");
  assert.ok(chg, "전일대비 영역 존재");
  assert.equal(chg.textContent.trim(), "-");
});

test("_mdRenderDashboard builds two-column layout: hero indices in main, others in rail", () => {
  const w = load();
  const dataMap = {
    KOSPI: { value: "2,650.12", change: "10.0", change_pct: "0.38%", direction: "up" },
    SPX: { value: "5,300.0", change: "8.0", change_pct: "-0.15%", direction: "down" },
    // CMDT_GC / USD_KRW / WEIRD have no data -> render "-"
  };
  w._mdRenderDashboard(CATALOG, dataMap);
  const root = w.document.getElementById("marketDashboard");
  const main = w.document.getElementById("mdIndMain");
  const rail = w.document.getElementById("mdIndRail");
  assert.ok(main && rail, "indicator slots present");

  // 국내 지수 → hero card in main, with value + up class.
  const hero = main.querySelector(".md-hero-card");
  assert.ok(hero && /2,650\.12/.test(hero.innerHTML));
  assert.ok(root.querySelector(".md-hero-card .md-chg.md-up"));
  // 해외 지수 (SPX) → compact row in the rail now, with down class.
  assert.match(rail.innerHTML, /md-chg md-down/);

  // hero count = # of 국내 지수 codes (1); rows = the other 4 codes.
  assert.equal(root.querySelectorAll(".md-hero-card").length, 1);
  assert.equal(root.querySelectorAll(".md-row").length, 4);

  // main carries only 국내 지수 (hero); the rail carries 해외 지수 + the rest.
  const mainTitles = [...main.querySelectorAll(".md-section-title")].map((el) => el.textContent);
  assert.deepEqual(mainTitles, ["국내 지수"]);
  const railTitles = [...rail.querySelectorAll(".md-section-title")].map((el) => el.textContent);
  assert.deepEqual(railTitles, ["해외 지수", "원자재", "환율", "신규카테고리"]);
});

test("야간선물 카테고리는 KOSPI 선물 iframe 섹션으로 상시 렌더된다", () => {
  const w = load();
  const catalog = {
    KOSPI: { label: "KOSPI", category: "국내 지수" },
    NIGHT_FUTURES: { label: "야간선물", category: "야간선물" },
  };
  const data = { NIGHT_FUTURES: { value: "350.5", change: "1.2", change_pct: "0.3%", direction: "up" } };
  w._mdRenderDashboard(catalog, data);
  const section = w.document.querySelector('[data-md-cat="야간선물"]');
  assert.ok(section, "야간선물 카테고리 섹션은 시간대와 무관하게 노출");
  assert.equal(section.querySelector(".md-section-title").textContent, "KOSPI 선물");
  const frame = section.querySelector("iframe.md-kospi-futures-frame");
  assert.ok(frame, "실시간 그래프 iframe 존재");
  assert.equal(
    frame.getAttribute("src"),
    "https://cantabile.tplinkdns.com:3358/?index=kospi-night-futures&theme=light&period=1D&headless=1",
  );
});

test("_mdRenderDashboard puts a per-card flow slot inside each 국내 지수 hero card", () => {
  const w = load();
  // two 국내 지수 codes so we can assert one slot per card
  const catalog = {
    KOSPI: { label: "KOSPI", category: "국내 지수" },
    KOSDAQ: { label: "KOSDAQ", category: "국내 지수" },
    SPX: { label: "S&P 500", category: "해외 지수" },
  };
  w._mdRenderDashboard(catalog, { KOSPI: { value: "2,650", direction: "up" } });
  const main = w.document.getElementById("mdIndMain");
  const cards = main.querySelectorAll(".md-hero-card");
  assert.equal(cards.length, 2);
  // every hero card carries its own flow slot, keyed by the index code
  const codes = [...cards].map((c) => c.querySelector(".md-card-flow")?.dataset.flowCode);
  assert.deepEqual([...codes].sort(), ["KOSDAQ", "KOSPI"]);
});

test("_cardFlowHtml renders one market's 개인/외국인/기관 with direction class + escaping", () => {
  const w = load();
  const html = w._cardFlowHtml({
    date: "26.05.29",
    individual: { value: "-14,054", direction: "down" },
    foreign: { value: "<b>x</b>", direction: "flat" },
    institution: { value: "23,688", direction: "up" },
  });
  const root = w.document.getElementById("mdIndMain");
  root.innerHTML = html;
  const rows = root.querySelectorAll(".cf-row");
  assert.equal(rows.length, 3);
  assert.match(root.innerHTML, /순매수/);
  assert.match(root.innerHTML, /26\.05\.29/);
  assert.ok(root.querySelector(".cf-val.md-up"));   // 기관 +
  assert.ok(root.querySelector(".cf-val.md-down"));  // 개인 -
  assert.ok(!root.innerHTML.includes("<b>x</b>"));   // hostile value escaped
});

test("_cardFlowHtml returns empty string when no flow", () => {
  const w = load();
  assert.equal(w._cardFlowHtml(null), "");
  assert.equal(w._cardFlowHtml(undefined), "");
});

test("hero cards embed a Naver mini trend chart for KOSPI/KOSDAQ only", () => {
  const w = load();
  const catalog = {
    KOSPI: { label: "KOSPI", category: "국내 지수" },
    KOSDAQ: { label: "KOSDAQ", category: "국내 지수" },
  };
  w._mdRenderDashboard(catalog, { KOSPI: { value: "2,650", direction: "up" } });
  const main = w.document.getElementById("mdIndMain");
  const charts = main.querySelectorAll(".md-hero-card img.md-hero-chart");
  assert.equal(charts.length, 2);
  assert.match(charts[0].getAttribute("src"), /KOSPI_end_up_tablet\.png\?\d+/);
  assert.match(charts[1].getAttribute("src"), /KOSDAQ_end_up_tablet\.png\?\d+/);
  // failed image hides itself rather than showing a broken icon
  assert.match(charts[0].getAttribute("onerror") || "", /display='none'/);
});

test("_miniChartHtml only emits for known index symbols", () => {
  const w = load();
  assert.match(w._miniChartHtml("KOSPI"), /md-hero-chart/);
  assert.equal(w._miniChartHtml("SPX"), "");
  assert.equal(w._miniChartHtml("AAPL"), "");
});

test("_mdRenderDashboard escapes catalog labels (no raw HTML injection)", () => {
  const w = load();
  const evil = { X: { label: "<img src=x onerror=alert(1)>", category: "국내 지수" } };
  w._mdRenderDashboard(evil, {});
  const html = w.document.getElementById("marketDashboard").innerHTML;
  assert.ok(!html.includes("<img src=x"));
  assert.match(html, /&lt;img src=x/);
});

test("_mvRenderShell renders 4 ranking tabs + market toggle with active state", () => {
  const w = load();
  const root = w.document.getElementById("marketMovers");
  w._mvRenderShell(root);
  assert.equal(root.querySelectorAll(".mv-tab").length, 4);
  assert.ok(root.querySelector(".mv-tab.active[data-kind='market_cap']"));
  assert.ok(root.querySelector(".mv-mkt.active[data-market='kospi']"));
  assert.ok(root.querySelector(".mv-body"));
});

test("_mvRenderRows renders ranking rows with direction class, metric, escaping", () => {
  const w = load();
  const root = w.document.getElementById("marketMovers");
  w._mvRenderShell(root);
  w._mvRenderRows(root, [
    { rank: "1", code: "005930", name: "삼성전자", price: "317,000", change_pct: "+5.84%", direction: "up", metric: "470조" },
    { rank: "2", code: "000660", name: "<b>x</b>", price: "100", change_pct: "-1.0%", direction: "down" },
  ]);
  const rows = root.querySelectorAll(".mv-row");
  assert.equal(rows.length, 2);
  assert.equal(rows[0].dataset.code, "005930");
  assert.match(rows[0].innerHTML, /삼성전자/);
  assert.match(rows[0].innerHTML, /mv-chg md-up/);
  assert.match(rows[0].innerHTML, /470조/); // metric shown for market_cap kind
  assert.match(rows[1].innerHTML, /mv-chg md-down/);
  // hostile name escaped
  assert.ok(!rows[1].innerHTML.includes("<b>x</b>"));
});

test("_secRenderRows renders sector rows with direction class + escaping", () => {
  const w = load();
  const root = w.document.getElementById("marketSectors");
  w._secRenderRows(root, [
    { name: "전자제품", change_pct: "+29.19%", direction: "up" },
    { name: "<i>철강</i>", change_pct: "-2.50%", direction: "down" },
  ]);
  const rows = root.querySelectorAll(".sec-row");
  assert.equal(rows.length, 2);
  assert.match(rows[0].innerHTML, /전자제품/);
  assert.match(rows[0].innerHTML, /sec-chg md-up/);
  assert.match(rows[1].innerHTML, /sec-chg md-down/);
  assert.ok(!rows[1].innerHTML.includes("<i>철강</i>"));
  assert.match(root.querySelector(".md-section-title").textContent, /업종별 등락/);
});

test("_secRenderRows shows empty state when no sectors", () => {
  const w = load();
  const root = w.document.getElementById("marketSectors");
  w._secRenderRows(root, []);
  assert.match(root.innerHTML, /표시할 업종이 없습니다/);
});

test("_newsRender renders external links with title/meta and escapes hostile fields", () => {
  const w = load();
  const root = w.document.getElementById("marketNews");
  w._newsRender(root, [
    { title: "코스피 급등", url: "https://finance.naver.com/news/x", source: "아이뉴스24", date: "2026-05-30 20:45", summary: "요약문" },
    { title: "<script>bad</script>", url: "javascript:alert(1)", source: "S", date: "", summary: "<b>x</b>" },
  ]);
  const items = root.querySelectorAll(".news-item");
  assert.equal(items.length, 2);
  // first opens externally in a new tab
  assert.equal(items[0].getAttribute("href"), "https://finance.naver.com/news/x");
  assert.equal(items[0].getAttribute("target"), "_blank");
  assert.match(items[0].getAttribute("rel"), /noopener/);
  assert.match(items[0].innerHTML, /코스피 급등/);
  assert.match(items[0].innerHTML, /아이뉴스24 · 2026-05-30 20:45/);
  // hostile title/summary escaped; non-http url neutralized to '#'
  assert.ok(!items[1].innerHTML.includes("<script>bad</script>"));
  assert.ok(!items[1].innerHTML.includes("<b>x</b>"));
  assert.equal(items[1].getAttribute("href"), "#");
});

test("_extRender builds 3 tool cards with deep-link, gap sign class, escaping", () => {
  const w = load();
  const root = w.document.getElementById("externalTools");
  w._extRender(root, {
    holding: {
      url: "https://ducklove.github.io/holding_value/",
      averageRatio: 215.6,
      top: [{ name: "영풍→고려아연", code: "000670", ratio: 781.87 },
            { name: "<b>x</b>", code: "036710", ratio: 512.18 }],
    },
    spread: {
      url: "https://ducklove.github.io/common_preferred_spread/",
      averageSpread: 48.28,
      top: [{ name: "두산퓨얼셀", code: "336260", spread: 88.8 }],
    },
    goldGap: {
      url: "https://ducklove.github.io/gold_gap/",
      assets: [{ key: "gold", label: "금", gap: -2.81, link: "https://ducklove.github.io/gold_gap/?asset=gold&gold_source=ny_futures" },
               { key: "usdt", label: "USDT", gap: 1.2, link: "https://ducklove.github.io/gold_gap/?asset=usdt" }],
    },
  });
  const cards = root.querySelectorAll(".ext-card");
  assert.equal(cards.length, 3);
  // holding row uses ?code= deep-link, with current theme appended.
  const holdingFirst = cards[0].querySelector(".ext-row");
  assert.match(holdingFirst.getAttribute("href"), /holding_value\/\?code=000670/);
  assert.match(holdingFirst.getAttribute("href"), /[?&]theme=(light|dark)/);
  assert.equal(holdingFirst.getAttribute("target"), "_blank");
  // ratio formatted as percent
  assert.match(cards[0].innerHTML, /781\.9%/);
  // spread card does NOT use code deep-link (param unsupported) → home url + theme
  const spreadFirst = cards[1].querySelector(".ext-row");
  assert.equal(spreadFirst.getAttribute("href"), "https://ducklove.github.io/common_preferred_spread/?theme=light");
  // gold gap: negative → md-down, positive → md-up; uses asset deep-link
  assert.ok(cards[2].querySelector(".ext-val.md-down"));
  assert.ok(cards[2].querySelector(".ext-val.md-up"));
  assert.match(cards[2].querySelector(".ext-row").getAttribute("href"), /asset=gold/);
  // hostile name escaped
  assert.ok(!root.innerHTML.includes("<b>x</b>"));
});

test("_extRender adds a SPAC card (after spread) showing 현재가 with ?code= deep-link", () => {
  const w = load();
  const root = w.document.getElementById("externalTools");
  w._extRender(root, {
    spread: {
      url: "https://ducklove.github.io/common_preferred_spread/",
      averageSpread: 48.28,
      top: [{ name: "두산퓨얼셀", code: "336260", spread: 88.8 }],
    },
    spac: {
      url: "https://ducklove.github.io/spac-hunter/",
      // 백엔드가 현재가 오름차순으로 정렬해 내려준다.
      top: [{ name: "신한제12호스팩", code: "474660", currentPrice: 1975 },
            { name: "<b>x</b>", code: "0131D0", currentPrice: 1990 }],
    },
  });
  const cards = root.querySelectorAll(".ext-card");
  assert.equal(cards.length, 2);
  // spac card comes right after the spread card.
  const spacCard = cards[1];
  assert.match(spacCard.innerHTML, /스팩 저가순/);
  assert.match(spacCard.innerHTML, /현재가 낮은 순/);
  // each row deep-links into spac-hunter via ?code=.
  const first = spacCard.querySelector(".ext-row");
  assert.match(first.getAttribute("href"), /spac-hunter\/\?code=474660/);
  assert.equal(first.getAttribute("target"), "_blank");
  // 현재가가 원 단위(천단위 콤마)로 표시된다.
  assert.match(spacCard.innerHTML, /1,975/);
  // hostile name escaped.
  assert.ok(!root.innerHTML.includes("<b>x</b>"));
});

test("_extRender adds 오늘의 추천 ETF card with realtime daily change and deep-links", () => {
  const w = load();
  const root = w.document.getElementById("externalTools");
  w._extRender(root, {
    etfPicks: {
      url: "https://ducklove.github.io/eiayn/",
      date: "2026-06-12",
      top: [
        { rank: 1, name: "TIGER MSCI Korea TR", code: "310970", score: 92, changePct: 1.23, link: "https://ducklove.github.io/eiayn/?code=310970" },
        { rank: 5, name: "KODEX 코리아밸류업", code: "495850", score: 86, changePct: -0.5 },
        { rank: 7, name: "<b>x</b>", code: "0000", score: 83 },  // 시세 실패 → changePct 없음, link 없음 → 도구 홈
      ],
    },
  });
  const cards = root.querySelectorAll(".ext-card");
  assert.equal(cards.length, 1);
  assert.match(cards[0].innerHTML, /오늘의 추천 ETF/);
  assert.match(cards[0].innerHTML, /AIYN TOP 100/);
  const rows = cards[0].querySelectorAll(".ext-row");
  assert.equal(rows.length, 3);
  // 항목 deep-link + 테마 전달, 값 = 일간 등락률(부호 + 방향 색상).
  assert.match(rows[0].getAttribute("href"), /eiayn\/\?code=310970/);
  assert.match(rows[0].getAttribute("href"), /[?&]theme=(light|dark)/);
  assert.equal(rows[0].querySelector(".ext-val").textContent, "+1.23%");
  assert.ok(rows[0].querySelector(".ext-val.md-up"));
  assert.equal(rows[1].querySelector(".ext-val").textContent, "-0.50%");
  assert.ok(rows[1].querySelector(".ext-val.md-down"));
  // 시세 없는 항목은 '-'(md-flat), link 없으면 도구 홈, 적대적 이름 escape.
  assert.match(rows[2].getAttribute("href"), /eiayn\//);
  assert.equal(rows[2].querySelector(".ext-val").textContent, "-");
  assert.ok(rows[2].querySelector(".ext-val.md-flat"));
  assert.ok(!cards[0].innerHTML.includes("<b>x</b>"));
});

test("_extRender renders empty when no data", () => {
  const w = load();
  const root = w.document.getElementById("externalTools");
  w._extRender(root, {});
  assert.equal(root.innerHTML, "");
});

test("_extPct formats percents (signed for gaps)", () => {
  const w = load();
  assert.equal(w._extPct(781.87), "781.9%");
  assert.equal(w._extPct(-2.81, true), "-2.81%");
  assert.equal(w._extPct(1.2, true), "+1.20%");
  assert.equal(w._extPct(null), "-");
});

test("_newsRender shows empty state when no news", () => {
  const w = load();
  const root = w.document.getElementById("marketNews");
  w._newsRender(root, []);
  assert.match(root.innerHTML, /표시할 뉴스가 없습니다/);
});

// --- 국채 (yield curve + 국가별 10년물) ---
const BOND_CATALOG = {
  US_SOFR: { label: "미국 SOFR", category: "국채", country: "US", maturity: 0 },
  US3M: { label: "미국3개월", category: "국채", country: "US", maturity: 0.25 },
  US10Y: { label: "미국10년물", category: "국채", country: "US", maturity: 10 },
  KOFR: { label: "KOFR", category: "국채", country: "KR", maturity: 0 },
  KR_CD91: { label: "한국 CD(91일)", category: "국채", country: "KR", maturity: 0.25 },
  KR10Y: { label: "한국10년물", category: "국채", country: "KR", maturity: 10 },
  JP2Y: { label: "일본2년물", category: "국채", country: "JP", maturity: 2 },
  JP10Y: { label: "일본10년물", category: "국채", country: "JP", maturity: 10 },
  CN10Y: { label: "중국10년물", category: "국채", country: "CN", maturity: 10 },
};
const BOND_DATA = {
  US_SOFR: { value: "4.30", direction: "down" },
  US3M: { value: "3.71", direction: "down" },
  US10Y: { value: "4.45", direction: "down" },
  KOFR: { value: "2.54", direction: "down" },
  KR_CD91: { value: "2.60", direction: "down" },
  KR10Y: { value: "4.12", direction: "down" },
  JP2Y: { value: "1.40", direction: "up" },
  JP10Y: { value: "2.57", direction: "down" },
  CN10Y: { value: "1.75", direction: "down" },
};

test("_mdBondCurve aligns KR/US/JP onto a shared maturity axis (overnight label, nulls where missing)", () => {
  const w = load();
  const curve = w._mdBondCurve(Object.keys(BOND_CATALOG), BOND_CATALOG, BOND_DATA);
  assert.deepEqual([...curve.labels], ["1D", "3M", "2Y", "10Y"]);
  assert.deepEqual([...curve.kr], [2.54, 2.60, null, 4.12]); // 한국: overnight=KOFR, 3M=CD91, 2Y 없음
  assert.deepEqual([...curve.us], [4.30, 3.71, null, 4.45]); // 미국: overnight=SOFR, 3M, 2Y 없음
  assert.deepEqual([...curve.jp], [null, null, 1.40, 2.57]); // 일본: 2Y/10Y만
});

test("_mdBondCountries lists 10Y by yield desc, incl KR/US/JP/CN", () => {
  const w = load();
  const cs = w._mdBondCountries(Object.keys(BOND_CATALOG), BOND_CATALOG, BOND_DATA);
  assert.deepEqual([...cs.map((c) => c.name)], ["미국", "한국", "일본", "중국"]);
  assert.equal(cs[0].value, 4.45);
});

test("_mdBondCountries maps newly added IT/CA/IN/BR to Korean names, yield desc", () => {
  const w = load();
  const cat = {
    US10Y: { category: "국채", country: "US", maturity: 10 },
    IT10Y: { category: "국채", country: "IT", maturity: 10 },
    CA10Y: { category: "국채", country: "CA", maturity: 10 },
    IN10Y: { category: "국채", country: "IN", maturity: 10 },
    BR10Y: { category: "국채", country: "BR", maturity: 10 },
  };
  const data = {
    US10Y: { value: "4.45" }, IT10Y: { value: "3.83" }, CA10Y: { value: "3.53" },
    IN10Y: { value: "6.91" }, BR10Y: { value: "14.76" },
  };
  const cs = w._mdBondCountries(Object.keys(cat), cat, data);
  // 금리 내림차순 + BOND_COUNTRY_NAMES 한글 매핑.
  assert.deepEqual([...cs.map((c) => c.name)], ["브라질", "인도", "미국", "이탈리아", "캐나다"]);
});

test("_bondMatLabel maps overnight(0) to 1D, sub-year to months, else years", () => {
  const w = load();
  assert.equal(w._bondMatLabel(0), "1D");
  assert.equal(w._bondMatLabel(0.25), "3M");
  assert.equal(w._bondMatLabel(0.5), "6M");
  assert.equal(w._bondMatLabel(10), "10Y");
});

test("_mdRenderDashboard renders 국채 chart containers + yield-curve table", () => {
  const w = load();
  w._mdRenderDashboard(BOND_CATALOG, BOND_DATA);
  const main = w.document.getElementById("mdIndMain");
  assert.ok(main.querySelector("#bondYieldCurve"), "yield curve container");
  assert.ok(main.querySelector("#bondCountryCompare"), "country compare container");
  const curveTbl = main.querySelector("#bondCurveTable .bond-tbl");
  assert.ok(curveTbl && /1D/.test(curveTbl.textContent), "curve table filled");
  assert.ok(/일본/.test(curveTbl.textContent), "curve table has 일본 column");
  // 국가별 10년물은 그래프만 두고 텍스트 표는 제거됨.
  assert.equal(main.querySelector("#bondCountryTable"), null, "country table removed");
});
