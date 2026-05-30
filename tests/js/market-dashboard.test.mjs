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
      + "<div class='md-main'><div id='mdIndMain'></div><div id='marketMovers'></div><div id='marketNews'></div></div>"
      + "<aside class='md-rail'><div id='mdIndRail'></div><div id='marketSectors'></div></aside>"
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
  // 해외 지수 (SPX) → compact row in main, with down class.
  assert.match(main.innerHTML, /md-chg md-down/);

  // hero count = # of 국내 지수 codes (1); rows = the other 4 codes.
  assert.equal(root.querySelectorAll(".md-hero-card").length, 1);
  assert.equal(root.querySelectorAll(".md-row").length, 4);

  // main column carries 국내 지수 then 해외 지수; rail carries the rest in order.
  const mainTitles = [...main.querySelectorAll(".md-section-title")].map((el) => el.textContent);
  assert.deepEqual(mainTitles, ["국내 지수", "해외 지수"]);
  const railTitles = [...rail.querySelectorAll(".md-section-title")].map((el) => el.textContent);
  assert.deepEqual(railTitles, ["원자재", "환율", "신규카테고리"]);
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

test("_newsRender shows empty state when no news", () => {
  const w = load();
  const root = w.document.getElementById("marketNews");
  w._newsRender(root, []);
  assert.match(root.innerHTML, /표시할 뉴스가 없습니다/);
});
