// jsdom behavior tests for the long/short pair helpers in portfolio-data.js:
// net-invested aggregation (pfPairStats), pointer parsing (pfPairLongCode) and
// the pair chip renderer used by the holdings table.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

const STORE_SRC = read("static", "js", "portfolio-store.js");
const DATA_SRC = read("static", "js", "portfolio-data.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function loadPairDom() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  appendScript(w, STORE_SRC);
  appendScript(w, DATA_SRC);
  w.escapeHtml = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/"/g, "&quot;");
  w.quotePriceOrNull = (q) => {
    const price = Number(q && q.price);
    return Number.isFinite(price) ? price : null;
  };
  return w;
}

const LONG = {
  stock_code: "006800",
  stock_name: "미래에셋증권2우B",
  quantity: 100,
  avg_price: 10000,
  quote: { price: 12000 },
};
const SHORT = {
  stock_code: "MIRAE_FUT",
  stock_name: "미래에셋 선물매도",
  quantity: -100,
  avg_price: 11000,
  pair_long_code: "006800",
  quote: { price: 12000 },
};

test("pfPairLongCode parses the pointer and treats blank as absent", () => {
  const w = loadPairDom();
  assert.equal(w.pfPairLongCode(SHORT), "006800");
  assert.equal(w.pfPairLongCode(LONG), null);
  assert.equal(w.pfPairLongCode({ pair_long_code: "  " }), null);
  assert.equal(w.pfPairLongCode(null), null);
});

test("pfPairStats sums net invested across long and short legs", () => {
  const w = loadPairDom();
  const stats = w.pfPairStats(LONG, [SHORT]);
  // 순투자액 = 100×10,000 + (-100)×11,000 = -100,000 (숏 매도대금이 더 큼)
  assert.equal(stats.netInvested, -100000);
  // 순평가액 = 1,200,000 - 1,200,000 = 0
  assert.equal(stats.netMarketValue, 0);
  // 합산 손익 = 롱 +200,000, 숏 -100,000 → +100,000
  assert.equal(stats.totalPnl, 100000);
  assert.equal(stats.allPriced, true);
  assert.equal(stats.legs.length, 2);
  assert.equal(stats.legs[0].code, "006800");
  assert.equal(stats.legs[1].qty, -100);
});

test("pfPairStats hides market value totals when a quote is missing", () => {
  const w = loadPairDom();
  const stats = w.pfPairStats(LONG, [{ ...SHORT, quote: {} }]);
  assert.equal(stats.netInvested, -100000);
  assert.equal(stats.netMarketValue, null);
  assert.equal(stats.totalPnl, null);
  assert.equal(stats.allPriced, false);
});

test("pfPairStats prefers avg_price_krw over the native avg_price", () => {
  const w = loadPairDom();
  const usdLong = {
    stock_code: "AAPL",
    stock_name: "Apple",
    quantity: 10,
    avg_price: 100,
    avg_price_krw: 130000,
    quote: { price: 140000 },
  };
  const stats = w.pfPairStats(usdLong, []);
  assert.equal(stats.netInvested, 1300000);
  assert.equal(stats.totalPnl, 100000);
});

test("pair chip renders on both legs and nowhere else", () => {
  const w = loadPairDom();
  w.PfStore.items = [LONG, SHORT, { stock_code: "005930", stock_name: "삼성전자", quantity: 5 }];

  const shortChip = w._renderPortfolioRowPairChip(SHORT);
  assert.match(shortChip, /js-pf-open-pair-summary/);
  assert.match(shortChip, /data-long-code="006800"/);
  assert.match(shortChip, /미래에셋증권2우B/);

  const longChip = w._renderPortfolioRowPairChip(LONG);
  assert.match(longChip, /js-pf-open-pair-summary/);
  assert.match(longChip, /data-long-code="006800"/);
  assert.match(longChip, /롱숏/);

  assert.equal(w._renderPortfolioRowPairChip(w.PfStore.items[2]), "");
});

test("pfPairShortsForLong finds every short pointing at the long", () => {
  const w = loadPairDom();
  const secondShort = { ...SHORT, stock_code: "MIRAE_FUT2" };
  w.PfStore.items = [LONG, SHORT, secondShort];
  const shorts = w.pfPairShortsForLong("006800");
  assert.deepEqual(shorts.map(s => s.stock_code), ["MIRAE_FUT", "MIRAE_FUT2"]);
});
