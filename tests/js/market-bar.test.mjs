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
    ["KOSPI", "HL_EWY", "HL_SAMSUNG", "HL_SKHYNIX", "HL_HYUNDAI"],
  );
});

test("local and server market-bar settings are normalized when loaded", async () => {
  const w = load();
  w.localStorage.setItem("market_bar_codes", JSON.stringify(["BNB_EWY", "USD_KRW"]));
  assert.deepEqual([...w._mbGetCodes()], ["HL_EWY", "USD_KRW"]);

  w.__settingsApi = async () => ({ codes: ["BNB_SKHYNIX", "HL_SKHYNIX", "BNB_HYUNDAI"] });
  w.eval("currentUser = { google_sub: 'user-1' }; apiFetchJson = window.__settingsApi;");
  await w._mbLoadCodes();
  const loaded = JSON.parse(w.eval("JSON.stringify(mbCodes)"));
  assert.deepEqual(loaded, ["HL_SKHYNIX", "HL_HYUNDAI"]);
  assert.equal(w.localStorage.getItem("market_bar_codes"), JSON.stringify(loaded));
});
