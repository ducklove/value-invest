// jsdom behavior tests for the portfolio tag summary modal.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SOURCES = ["portfolio-trends.js", "portfolio-popup-trends.js", "portfolio-tag-summary.js"]
  .map(file => readFileSync(join(__dirname, "..", "..", "static", "js", file), "utf8"));

function loadTagSummary() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/portfolio",
  });
  const { window } = dom;
  window.PfStore = {
    currency: { unit: "KRW" },
    items: [
      {
        stock_code: "005930",
        stock_name: "삼성전자",
        quantity: 10,
        group_name: "반도체",
        tags: ["AI"],
        quote: { price: 70, change: -2, change_pct: -2.78 },
      },
      {
        stock_code: "000660",
        stock_name: "SK하이닉스",
        quantity: 5,
        group_name: "반도체",
        tags: ["AI"],
        quote: { price: 60, change: 1, change_pct: 1.69 },
      },
      {
        stock_code: "035420",
        stock_name: "NAVER",
        quantity: 3,
        group_name: "인터넷",
        tags: ["인터넷"],
        quote: { price: 100, change: 0, change_pct: 0 },
      },
    ],
  };
  window.escapeHtml = value => String(value ?? "").replace(/[&<>"']/g, ch => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[ch]));
  window.pfGetTags = item => item.tags || [];
  window.pfGetGroup = item => item.group_name || "기타";
  window.quotePriceOrNull = quote => quote?.price ?? null;
  window.fmtPct = (value, signed = true) => {
    if (value === null || value === undefined) return "-";
    return `${signed && value > 0 ? "+" : ""}${value.toFixed(2)}%`;
  };
  window.fmtSignedKrw = value => `${value > 0 ? "+" : ""}${Math.round(value).toLocaleString()}`;
  window.returnClass = () => "";
  window.pfFmtPortfolioValue = value => `${Math.round(value).toLocaleString()}원`;
  window.pfFx = value => value;
  window.pfFxDivisor = () => 100000000;
  window.pfFxUnit = () => "억";
  window.pfFxSymbol = () => "";
  window.apiFetch = async () => ({ ok: true, json: async () => [] });
  window.apiFetchJson = async () => [];
  window.ResizeObserver = class {
    observe() {}
    disconnect() {}
  };
  const closeCallbacks = new WeakMap();
  window.openManagedModal = (modal, options = {}) => {
    closeCallbacks.set(modal, options.onClose);
    modal.style.display = options.display || "flex";
    const target = options.initialFocus ? modal.querySelector(options.initialFocus) : modal.querySelector("button");
    target?.focus();
    return modal;
  };
  window.closeManagedModal = (modal, options = {}) => {
    if (options.remove) modal.remove();
    else modal.style.display = "none";
    closeCallbacks.get(modal)?.();
    closeCallbacks.delete(modal);
  };
  for (const source of SOURCES) {
    const script = window.document.createElement("script");
    script.textContent = source;
    window.document.body.appendChild(script);
  }
  window._waitForChartContainer = async () => true;
  return window;
}

test("tag modal shows value total under portfolio weight and renders composition/trend shells", () => {
  const w = loadTagSummary();

  w.pfOpenTagSummary("AI");

  const modal = w.document.getElementById("pfTagSummaryModal");
  assert.ok(modal);
  const cards = [...modal.querySelectorAll(".pf-tag-summary-card")];
  const weightCard = cards.at(-1);
  assert.equal(weightCard.querySelector(".pf-tag-summary-label").textContent, "전체 비중");
  assert.equal(weightCard.querySelector(".pf-tag-summary-sub").textContent, "1,000원");
  assert.equal(weightCard.textContent.includes("2종목"), false);
  assert.ok(modal.querySelector(".pf-tag-summary-composition"));
  assert.ok(modal.querySelector(".pf-tag-summary-treemap"));
  const treemapTiles = modal.querySelectorAll(".pf-tag-summary-treemap-tile");
  assert.equal(treemapTiles.length, 2);
  assert.match(treemapTiles[0].getAttribute("style"), /width:70\.0000%/);
  assert.equal(modal.querySelector(".pf-tag-summary-pie"), null);
  assert.equal(modal.querySelector(".pf-tag-summary-heatmap"), null);
  assert.ok(modal.querySelector(".pf-tag-summary-table"));
  assert.ok(modal.textContent.includes("평가금액 합 추세"));
  assert.ok(modal.textContent.includes("포트폴리오 비중 추세"));
});

test("group summary opens the same modal layout with group-scoped labels", () => {
  const w = loadTagSummary();

  w.pfOpenGroupSummary("반도체");

  const modal = w.document.getElementById("pfTagSummaryModal");
  assert.ok(modal);
  assert.equal(modal.querySelector("#pfTagSummaryTitle").textContent, "반도체");
  assert.equal(modal.querySelector(".pf-tag-summary-card .pf-tag-summary-label").textContent, "그룹 일간");
  assert.ok(modal.querySelector(".pf-tag-summary-treemap"));
  assert.equal(modal.querySelectorAll(".pf-tag-summary-treemap-tile").length, 2);
  assert.equal(modal.querySelector("thead tr th:last-child").textContent, "그룹 비중");
  assert.equal(modal.querySelector(".pf-tag-summary-pie"), null);
});

test("live portfolio weight is unavailable when a holding has no quote", () => {
  const w = loadTagSummary();
  w.PfStore.items[2].quote = {};

  w.pfOpenTagSummary("AI");

  const cards = w.document.querySelectorAll(".pf-tag-summary-card");
  assert.equal(cards[cards.length - 1].querySelector(".pf-tag-summary-value").textContent, "-");
  w.close();
});

test("popup trends preserve missing points and include negative holdings below zero", async () => {
  const w = loadTagSummary();
  const createdOptions = [];
  w.PortfolioTrendChart = {
    create(_container, option) {
      createdOptions.push(option);
      return {
        dispose() {},
        resize() {},
        on() {},
        setOption(next) { Object.assign(option.yAxis, next.yAxis); },
        getOption() { return { ...option, dataZoom: [{ start: 0, end: 100 }] }; },
      };
    },
  };
  w.document.body.innerHTML = w._pfRenderPopupTrendShell("평가금액", "전체 계좌 정산");
  w.PfStore.currency.unit = "USD";
  w.apiFetchJson = async () => [
    { date: "2026-01-01", market_value: 1000, weight_pct: 10, total_value: 10000, snapshot_total_value: 9000, fx_usdkrw: null },
    { date: "2026-01-02", market_value: 150000, weight_pct: 12.5, total_value: 1200000, snapshot_total_value: 1200000, fx_usdkrw: 1500 },
    { date: "2026-01-03", market_value: -140000, weight_pct: -10, total_value: 1400000, snapshot_total_value: 1400000, fx_usdkrw: 1400 },
    { date: "2026-01-04", market_value: 0, weight_pct: null, total_value: 0, snapshot_total_value: 0, fx_usdkrw: 1400 },
  ];
  await w._pfLoadPopupTrends(w.document.body.firstElementChild, "/fixture", {
    valueTitle: "평가금액", scopeLabel: "종목",
  });

  assert.equal(createdOptions.length, 2);
  assert.deepEqual(Array.from(createdOptions[0].series[0].data), [null, 100, -100, 0]);
  assert.deepEqual(Array.from(createdOptions[1].series[0].data), [null, 12.5, -10, null]);
  assert.match(w.document.body.textContent, /비중 산출 불가 2일/);
  assert.ok(createdOptions[0].yAxis.min <= -100);
  assert.ok(createdOptions[1].yAxis.min <= -10);
  assert.ok(createdOptions[1].yAxis.max >= 12.5);
  w.close();
});

test("group trend uses exact historical group names and includes post-exit snapshot days", () => {
  const w = loadTagSummary();
  const history = w._pfGroupPopupHistory([
    { date: "2026-01-01", group_name: "core", market_value: 120, total_value: 120, weight_pct: 100 },
    { date: "2026-01-02", group_name: "core", market_value: 900, total_value: 1000, weight_pct: 90 },
    { date: "2026-01-02", group_name: "Core", market_value: 100, total_value: 1000, weight_pct: 10 },
    { date: "2026-01-03", group_name: "core", market_value: 800, total_value: 800, weight_pct: 100 },
  ], "Core");
  assert.deepEqual(Array.from(history, row => [row.date, row.market_value, row.total_value]), [
    ["2026-01-02", 100, 1000],
    ["2026-01-03", 0, 800],
  ]);
  w.close();
});
