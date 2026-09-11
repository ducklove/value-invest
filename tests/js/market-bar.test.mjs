// jsdom tests for the sidebar market bar (_mbRenderBar) in
// portfolio-groups-market.js. mbCodes/mbCatalog are module-level `let`s, so we
// set them via same-realm eval rather than as window properties.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (p) => readFileSync(join(__dirname, "..", "..", "static", "js", p), "utf8");
const UTILS = read("utils.js");
const MARKET = read("portfolio-groups-market.js");

function load() {
  const dom = new JSDOM(
    "<!doctype html><html><body><div class='market-bar' id='marketBar'></div></body></html>",
    { runScripts: "dangerously", url: "https://app.example.com/" },
  );
  // Externals defined in other bundles that the market bar references.
  dom.window.currentUser = null;
  dom.window.apiFetch = () => Promise.resolve({ ok: false });
  dom.window.flashEl = () => {};
  for (const src of [UTILS, MARKET]) {
    const s = dom.window.document.createElement("script");
    s.textContent = src;
    dom.window.document.body.appendChild(s);
  }
  return dom.window;
}

function render(w, catalog, codes, dataMap) {
  w.__cat = JSON.stringify(catalog);
  w.__codes = JSON.stringify(codes);
  w.__data = JSON.stringify(dataMap);
  w.eval("mbCatalog = JSON.parse(__cat); mbCodes = JSON.parse(__codes); _mbRenderBar(JSON.parse(__data));");
}

test("market bar shows '-' when 전일대비 is missing (값만 있고 변동 없음)", () => {
  const w = load();
  render(
    w,
    { GB10Y: { label: "영국10년물", category: "채권" } },
    ["GB10Y"],
    { GB10Y: { value: "4.90", change: "", change_pct: "", direction: "" } },
  );
  const bar = w.document.getElementById("marketBar");
  assert.match(bar.querySelector(".mi-val").textContent, /4\.90/);
  const chg = bar.querySelector(".mi-chg");
  assert.equal(chg.textContent.trim(), "-");
  assert.ok(bar.querySelector(".mi-chg .mi-flat"), "uses mi-flat placeholder");
});

test("market bar renders signed 전일대비 when present", () => {
  const w = load();
  render(
    w,
    { US2Y: { label: "미국2년물", category: "채권" } },
    ["US2Y"],
    { US2Y: { value: "4.04", change: "0.02", change_pct: "0.39%", direction: "down" } },
  );
  const chg = w.document.getElementById("marketBar").querySelector(".mi-chg");
  assert.match(chg.innerHTML, /mi-down/);
  assert.match(chg.textContent, /-0\.02/);
  assert.match(chg.textContent, /-0\.39%/);
});

test("legacy Binance market-bar codes migrate to Hyperliquid without duplicates", () => {
  const w = load();
  const normalized = w._mbNormalizeCodes([
    "KOSPI",
    "BNB_EWY",
    "HL_EWY",
    "BNB_SAMSUNG",
    "BNB_SKHYNIX",
    "BNB_HYUNDAI",
    "",
    null,
  ]);
  assert.deepEqual(
    [...normalized],
    ["KOSPI", "HL_KR200", "HL_SAMSUNG", "HL_SKHYNIX", "HL_HYUNDAI"],
  );
});

test("local and server market-bar settings are normalized when loaded", async () => {
  const w = load();
  w.localStorage.setItem("market_bar_codes", JSON.stringify(["BNB_EWY", "USD_KRW"]));
  assert.deepEqual([...w._mbGetCodes()], ["HL_KR200", "USD_KRW"]);

  w.__settingsApi = async () => ({ codes: ["BNB_SKHYNIX", "HL_SKHYNIX", "BNB_HYUNDAI"] });
  w.eval("currentUser = { google_sub: 'user-1' }; apiFetchJson = window.__settingsApi;");
  await w._mbLoadCodes();
  const loaded = JSON.parse(w.eval("JSON.stringify(mbCodes)"));
  assert.deepEqual(loaded, ["HL_SKHYNIX", "HL_HYUNDAI"]);
  assert.equal(w.localStorage.getItem("market_bar_codes"), JSON.stringify(loaded));
});

test('오래된 지수는 지연 표시와 기준 시각을 보이고 회복하면 표시를 해제한다', () => {
  const w = load();
  const cat = { KOSPI: { label: 'KOSPI' } };
  const quote = { value: '7,033.92', _stale: true, as_of: '2026-09-10T15:30:00+09:00' };
  render(w, cat, ['KOSPI'], { KOSPI: quote });
  assert.equal(w.document.querySelector('.mi-stale').textContent, '지연');
  assert.match(w.document.querySelector('.mi-val').title, /갱신 지연.*시세 기준.*2026.*KST/);
  render(w, cat, ['KOSPI'], { KOSPI: { value: '6,864.30', as_of: '2026-09-11T10:11:00+09:00' } });
  assert.equal(w.document.querySelector('.mi-stale'), null);
  assert.equal(w.document.querySelector('.mi-val').textContent, '6,864.30');
  w.close();
});

test('정기 갱신 실패는 이전 숫자를 지연 상태로 보존하고 성공 응답으로 회복한다', async () => {
  const w = load();
  w.eval("mbCodes = ['KOSPI']; mbCatalog = { KOSPI: { label: 'KOSPI' } };");
  const responses = [{ KOSPI: { value: '7,033.92' } }, null, { KOSPI: { value: '6,864.30' } }];
  w.apiFetchJson = async (url, options) => {
    assert.equal(options.cache, 'no-store');
    return responses.shift();
  };
  await w.loadMarketSummary();
  await w.loadMarketSummary();
  assert.equal(w.document.querySelector('.mi-val').textContent, '7,033.92');
  assert.ok(w.document.querySelector('.mi-stale'));
  await w.loadMarketSummary();
  assert.equal(w.document.querySelector('.mi-val').textContent, '6,864.30');
  assert.equal(w.document.querySelector('.mi-stale'), null);
  w.close();
});

test('대체 공급원 시세는 출처를 표시하고 이전 값 지연과 구별한다', () => {
  const w = load();
  render(w, { KOSPI: { label: 'KOSPI' } }, ['KOSPI'], {
    KOSPI: { value: '6,864.30', _degraded: true, source: 'yahoo_fallback' },
  });
  assert.equal(w.document.querySelector('.mi-stale').textContent, '대체');
  assert.match(w.document.querySelector('.mi-val').title, /Yahoo 대체 시세/);
  w.close();
});
