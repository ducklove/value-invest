// jsdom behavior tests for the portfolio tag summary modal.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TAG_SUMMARY_SRC = readFileSync(
  join(__dirname, "..", "..", "static", "js", "portfolio-tag-summary.js"),
  "utf8",
);

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
        tags: ["AI"],
        quote: { price: 70, change: -2, change_pct: -2.78 },
      },
      {
        stock_code: "000660",
        stock_name: "SK하이닉스",
        quantity: 5,
        tags: ["AI"],
        quote: { price: 60, change: 1, change_pct: 1.69 },
      },
      {
        stock_code: "035420",
        stock_name: "NAVER",
        quantity: 3,
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
  const script = window.document.createElement("script");
  script.textContent = TAG_SUMMARY_SRC;
  window.document.body.appendChild(script);
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
  assert.ok(modal.querySelector(".pf-tag-summary-pie"));
  assert.ok(modal.querySelector(".pf-tag-summary-table"));
  assert.ok(modal.textContent.includes("평가금액 합 추세"));
  assert.ok(modal.textContent.includes("포트폴리오 비중 추세"));
});

test("tag trend charts start their y-axis from zero", async () => {
  const w = loadTagSummary();
  const createdOptions = [];
  w.PortfolioTrendChart = {
    create(_container, option) {
      createdOptions.push(option);
      return {
        dispose() {},
        resize() {},
        on() {},
        setOption() {},
        getOption() { return option; },
      };
    },
  };
  w.document.body.innerHTML = `
    <div id="pfTagValueTrend" class="pf-tag-summary-chart"></div>
    <div id="pfTagWeightTrend" class="pf-tag-summary-chart"></div>
    <span id="pfTagValueRange"></span>
    <span id="pfTagWeightRange"></span>
  `;

  await w._pfRenderTagSummaryTrendCharts([
    { date: "2026-01-01", tag_value: 1000, weight_pct: 12.5 },
    { date: "2026-01-02", tag_value: 1200, weight_pct: 15.0 },
  ]);

  assert.equal(createdOptions.length, 2);
  assert.equal(createdOptions[0].yAxis.min, 0);
  assert.equal(createdOptions[1].yAxis.min, 0);
});
