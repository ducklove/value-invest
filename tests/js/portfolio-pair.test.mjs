// jsdom behavior tests for the long/short pair helpers in portfolio-data.js:
// net-invested aggregation (pfPairStats), pointer parsing (pfPairLongCode) and
// pair ordering and the existing daily-change cell's combined-performance action.

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
  w.CSS = {escape: value => String(value)};
  appendScript(w, STORE_SRC);
  appendScript(w, DATA_SRC);
  w.escapeHtml = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/"/g, "&quot;");
  w.quotePriceOrNull = (q) => {
    if (q?.price === null || q?.price === undefined) return null;
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

test("performance figures expose a tooltip trigger on every holding without adding badges", () => {
  const w = loadPairDom();
  w.PfStore.items = [LONG, SHORT, { stock_code: "005930", stock_name: "삼성전자", quantity: 5 }];

  for (const item of w.PfStore.items) {
    const html = w.pfPerformanceCellHtml(item, '<span>+1.23%</span>');
    assert.match(html, /js-pf-performance-tooltip/);
    assert.ok(html.includes(`data-code="${item.stock_code}"`));
    assert.match(html, /data-metric="changePct"/);
    assert.match(html, /tabindex="0"/);
    assert.doesNotMatch(html, /pf-stock-tag|pf-pair-chip/);
    assert.doesNotMatch(html, /<button/);
    const holder = w.document.createElement('div');
    holder.innerHTML = html;
    assert.equal(holder.textContent, '+1.23%');
  }
  w.PfStore.accountId = 'manual';
  assert.match(w.pfPerformanceCellHtml(SHORT, '+1%'), /js-pf-performance-tooltip/);
  w.close();
});

test("pfPairShortsForLong finds every short pointing at the long", () => {
  const w = loadPairDom();
  const secondShort = { ...SHORT, stock_code: "MIRAE_FUT2" };
  w.PfStore.items = [LONG, SHORT, secondShort];
  const shorts = w.pfPairShortsForLong("006800");
  assert.deepEqual(shorts.map(s => s.stock_code), ["MIRAE_FUT", "MIRAE_FUT2"]);
});

test('pair daily change combines signed PnL over the previous net valuation', () => {
  const w = loadPairDom();
  const stats = w.pfPairStats({...LONG, quote: {price: 12000, previous_close: 10000}},
    [{...SHORT, quantity: -50, quote: {price: 12000, previous_close: 11000}}]);
  assert.equal(stats.netPreviousValue, 450000);
  assert.equal(stats.dailyPnl, 150000);
  assert.ok(Math.abs(stats.dailyChangePct - 100 / 3) < 1e-8);
  w.close();
});

test('actual futures previous close takes priority over its underlying stock percentage', () => {
  const w = loadPairDom();
  assert.equal(w.pfPairPreviousClose({previous_close: 11000, change: 1000, change_pct: 30}, 12000), 11000);
  assert.equal(w.pfPairPreviousClose({change: 1000, change_pct: 30}, 12000), 11000);
  assert.equal(w.pfPairPreviousClose({change_pct: 20}, 12000), 10000);
  assert.equal(w.pfPairPreviousClose({change_pct: null}, 12000), null);
  assert.equal(w.pfPairPreviousClose({change_pct: -100}, 12000), null);
  assert.equal(w.pfPairPreviousClose({previous_close: 11000, _stale: true}, 12000), null);
  assert.equal(w.pfPairPreviousClose({previous_close: 11000}, NaN), null);
  w.close();
});

test('missing daily quotes and a zero net baseline never produce a misleading percentage', () => {
  const w = loadPairDom();
  const long = {...LONG, quote: {price: 12000, previous_close: 10000}};
  const short = {...SHORT, quote: {price: 11000, previous_close: 10000}};
  const zero = w.pfPairStats(long, [short]);
  assert.equal(zero.dailyPnl, 100000);
  assert.equal(zero.dailyChangePct, null);
  const missing = w.pfPairStats(long, [{...short, quote: {price: 11000}}]);
  assert.equal(missing.dailyPnl, null);
  assert.equal(missing.dailyChangePct, null);
  const noPrice = w.pfPairStats(long, [{...short, quote: {price: null, previous_close: 10000}}]);
  assert.equal(noPrice.dailyPnl, null);
  assert.equal(noPrice.totalPnl, null);
  const negative = w.pfPairStats(long, [{...short, quantity: -200}]);
  assert.equal(negative.dailyChangePct, 0);
  w.close();
});

test('shorts stay below their long and moving or dropping onto a pair preserves the whole block', () => {
  const w = loadPairDom();
  appendScript(w, read('static', 'js', 'portfolio-order.js'));
  const other = {stock_code: '005930'};
  const second = {...SHORT, stock_code: 'MIRAE_FUT2'};
  const items = [SHORT, other, LONG, second];
  const codes = list => Array.from(list, item => item.stock_code);
  assert.deepEqual(codes(w.pfKeepPairsTogether(items)), ['005930', '006800', 'MIRAE_FUT', 'MIRAE_FUT2']);
  assert.deepEqual(codes(items), ['MIRAE_FUT', '005930', '006800', 'MIRAE_FUT2']);
  assert.equal(w._pfNextOrderAfterDrop(items, 'MIRAE_FUT', '005930'), null);
  assert.equal(w._pfNextOrderAfterDrop(items, '006800', 'MIRAE_FUT'), null);
  assert.deepEqual(codes(w._pfNextOrderAfterDrop(items, '006800', '005930')), ['006800', 'MIRAE_FUT', 'MIRAE_FUT2', '005930']);
  assert.deepEqual(codes(w._pfNextOrderAfterDrop(items, '005930', 'MIRAE_FUT', 'after')), ['006800', 'MIRAE_FUT', 'MIRAE_FUT2', '005930']);
  assert.deepEqual(codes(w._pfNextOrderAfterDrop(items, '005930', 'MIRAE_FUT', 'before')), ['005930', '006800', 'MIRAE_FUT', 'MIRAE_FUT2']);
  w.PfStore.accountId = 'manual';
  assert.deepEqual(codes(w._pfNextOrderAfterDrop(items, 'MIRAE_FUT', '006800', 'after')), ['005930', '006800', 'MIRAE_FUT', 'MIRAE_FUT2']);
  w.close();
});

test('hover previews combined percentage and updates in place with live quotes', () => {
  const w = loadPairDom();
  w.PfStore.items = [{...LONG, quote: {price: 12000, previous_close: 10000}},
    {...SHORT, quantity: -50, quote: {price: 12000, previous_close: 10000}}];
  w.pfFmtPortfolioValue = v => String(v);
  w.returnClass = v => v > 0 ? 'positive' : v < 0 ? 'negative' : '';
  w.fmtPct = v => `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
  appendScript(w, read('static', 'js', 'portfolio-pair.js'));
  w.document.body.insertAdjacentHTML('beforeend', `<table><tbody id="pfBody"><tr data-code="006800"><td>${w.pfPerformanceCellHtml(LONG, '+20%')}</td></tr></tbody></table>`);
  const trigger = w.document.querySelector('.js-pf-performance-tooltip');
  trigger.getClientRects = () => [{left: 0, right: 100, top: 100, bottom: 120}];
  trigger.getBoundingClientRect = () => trigger.getClientRects()[0];
  w.pfPreviewPerformanceTooltip({target: trigger, relatedTarget: null}, true);
  const tooltip = w.document.getElementById('pfPerformanceTooltip');
  assert.equal(tooltip.getAttribute('role'), 'tooltip');
  assert.equal(tooltip.querySelector('.pf-tooltip-line strong').textContent, '+20.00%');
  assert.notEqual(w.document.activeElement, trigger);
  w.PfStore.items[1].quote.price = 13000;
  w.updatePortfolioRowQuote('MIRAE_FUT', false);
  assert.equal(w.document.getElementById('pfPerformanceTooltip'), tooltip);
  assert.equal(tooltip.querySelector('.pf-tooltip-line strong').textContent, '+10.00%');
  w.PfStore.items[1].pair_long_code = null;
  w.pfRefreshPerformanceTooltip();
  assert.equal(tooltip.querySelector('.pf-tooltip-title').textContent, LONG.stock_name);
  assert.doesNotMatch(tooltip.textContent, /합산/);
  w.pfPreviewPerformanceTooltip({target: trigger, relatedTarget: null}, false);
  assert.equal(w.document.getElementById('pfPerformanceTooltip'), null);
  assert.equal(trigger.hasAttribute('aria-describedby'), false);
  w.close();
});

test('daily change amounts use one-unit price movement and signed quantities; missing prices stay unknown', () => {
  const w = loadPairDom();
  const short = {...SHORT, quote: {price: 12000, previous_close: 11000}};
  assert.equal(w.pfHoldingDailyStats(short).change, 1000);
  assert.equal(w.pfHoldingDailyStats(short).dailyPnl, -100000);
  assert.equal(w.pfHoldingDailyStats({...short, quote: {price: 12000}}).dailyPnl, null);
  assert.equal(w.pfHoldingDailyStats({...short, quote: {price: null, previous_close: 11000}}).change, null);
  w.close();
});

test('connected rows reserve a marker column on every pair leg including multiple shorts', () => {
  const w = loadPairDom();
  const second = {...SHORT, stock_code: 'SECOND'};
  const items = [LONG, SHORT, second];
  assert.match(w.pfPairRowPresentation(LONG, items).attrs, /data-pair-position="first"/);
  assert.match(w.pfPairRowPresentation(SHORT, items).marker, /├/);
  assert.match(w.pfPairRowPresentation(second, items).marker, /└/);
  w.PfStore.accountId = 'manual';
  assert.equal(w.pfPairRowPresentation(SHORT, items).marker, '');
  w.close();
});
