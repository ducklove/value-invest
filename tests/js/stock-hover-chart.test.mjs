// jsdom tests for the stock hover candle tooltip (stock-hover-chart.js).
//
// The script is a document-level delegated handler, desktop-only via
// matchMedia('(hover: hover) and (pointer: fine)'). Tests stub matchMedia and
// apiFetchJson, and set STOCK_CANDLE_TIP_DELAY_MS = 0 so the show timer fires
// on the next tick.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SCRIPT = readFileSync(
  join(__dirname, "..", "..", "static", "js", "stock-hover-chart.js"),
  "utf8",
);

const FIXTURE = {
  code: "005930",
  days: 60,
  currency: "KRW",
  source: "kis",
  supported: true,
  candles: [
    { date: "2026-07-16", open: 1100, high: 1250, low: 1080, close: 1200 },
    { date: "2026-07-17", open: 1210, high: 1330, low: 1190, close: 1300 },
  ],
};

function load({ hoverCapable = true, fixture = FIXTURE } = {}) {
  const dom = new JSDOM(
    `<!doctype html><html><body>
      <table><tbody id="pfBody">
        <tr data-code="005930"><td class="pf-stock-cell">
          <span class="pf-stock-main"><a href="#" class="pf-stock-link"><strong>삼성전자</strong></a></span>
        </td></tr>
        <tr data-code="CASH_KRW"><td class="pf-stock-cell">
          <span class="pf-stock-main"><a href="#" class="pf-stock-link"><strong>원화</strong></a></span>
        </td></tr>
      </tbody></table>
      <span id="anySurface" data-candle-code="AAPL" data-candle-name="Apple">AAPL</span>
    </body></html>`,
    { runScripts: "dangerously", url: "https://app.example.com/" },
  );
  const w = dom.window;
  const calls = [];
  w.STOCK_CANDLE_TIP_DELAY_MS = 0;
  w.matchMedia = () => ({ matches: hoverCapable });
  w.escapeHtml = (s) => String(s ?? "")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  w.apiFetchJson = async (path) => {
    calls.push(path);
    return fixture;
  };
  const s = w.document.createElement("script");
  s.textContent = SCRIPT;
  w.document.body.appendChild(s);
  return { w, calls };
}

const tick = (ms = 20) => new Promise((r) => setTimeout(r, ms));

function hover(w, el) {
  el.dispatchEvent(new w.MouseEvent("mouseover", { bubbles: true }));
}

function unhover(w, el) {
  el.dispatchEvent(new w.MouseEvent("mouseout", { bubbles: true }));
}

test("hovering a portfolio stock cell fetches candles once and shows the tooltip", async () => {
  const { w, calls } = load();
  const strong = w.document.querySelector('tr[data-code="005930"] strong');
  hover(w, strong);
  await tick();

  assert.deepEqual(calls, ["/api/stocks/005930/daily-candles?days=60"]);
  const tip = w.document.getElementById("stockCandleTip");
  assert.ok(tip, "tooltip element created");
  assert.ok(tip.classList.contains("visible"));
  assert.match(tip.textContent, /삼성전자/);
  assert.match(tip.textContent, /005930/);
  assert.match(tip.textContent, /2026-07-16 ~ 2026-07-17 · 2거래일/);
  assert.match(tip.textContent, /\+8\.33%/);
  assert.ok(tip.querySelector("canvas.sct-canvas"));

  // mouseout hides; re-hover reuses the cache (no second fetch).
  unhover(w, strong.closest(".pf-stock-cell"));
  assert.equal(tip.classList.contains("visible"), false);
  hover(w, strong);
  await tick();
  assert.equal(calls.length, 1);
  assert.ok(tip.classList.contains("visible"));
});

test("special asset codes are skipped without any fetch", async () => {
  const { w, calls } = load();
  hover(w, w.document.querySelector('tr[data-code="CASH_KRW"] strong'));
  await tick();
  assert.equal(calls.length, 0);
  const tip = w.document.getElementById("stockCandleTip");
  assert.ok(!tip || !tip.classList.contains("visible"));
});

test("non-hover environments (touch) do nothing", async () => {
  const { w, calls } = load({ hoverCapable: false });
  hover(w, w.document.querySelector('tr[data-code="005930"] strong'));
  await tick();
  assert.equal(calls.length, 0);
});

test("data-candle-code opt-in surface works and close-only data falls back to a line", async () => {
  const fixture = {
    code: "AAPL",
    days: 60,
    currency: "USD",
    source: "yahoo",
    supported: true,
    candles: [
      { date: "2026-07-16", close: 10.0 },
      { date: "2026-07-17", close: 10.5 },
    ],
  };
  const { w, calls } = load({ fixture });
  hover(w, w.document.getElementById("anySurface"));
  await tick();

  assert.deepEqual(calls, ["/api/stocks/AAPL/daily-candles?days=60"]);
  const tip = w.document.getElementById("stockCandleTip");
  assert.ok(tip.classList.contains("visible"));
  assert.match(tip.textContent, /Apple/);
  assert.match(tip.textContent, /종가 라인/);
  assert.match(tip.textContent, /USD/);
});

test("unsupported or empty responses never show a tooltip", async () => {
  const fixture = { code: "XXX", days: 60, currency: null, source: null, supported: false, candles: [] };
  const { w } = load({ fixture });
  const el = w.document.getElementById("anySurface");
  el.dataset.candleCode = "XXX";
  hover(w, el);
  await tick();
  const tip = w.document.getElementById("stockCandleTip");
  assert.ok(!tip || !tip.classList.contains("visible"));
});
