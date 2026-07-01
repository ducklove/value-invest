// jsdom behavior tests for portfolio load failure visibility and sortable
// table-header accessibility. These cover the audit findings that were not
// protected by the old Python string-presence checks.

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
const RENDER_SRC = read("static", "js", "portfolio-render.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function loadPortfolioDataDom() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="pfLoadStatus" role="status" aria-live="polite" style="display:none;"></div>
    <table id="pfTable" style="display:table"><tbody id="pfBody"></tbody></table>
    <div id="pfEmpty" style="display:none;"></div>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  appendScript(w, STORE_SRC);
  appendScript(w, DATA_SRC);
  const reported = [];
  w.reportApiError = (error, context, options = {}) => {
    reported.push({ error, context, options });
  };
  w.renderPortfolio = () => {};
  w._updateQuoteSubscriptions = () => {};
  return { w, reported };
}

test("loadPortfolio shows an inline failure state when the portfolio request returns 5xx", async () => {
  const { w, reported } = loadPortfolioDataDom();
  w.apiFetch = async (path) => {
    assert.equal(path, "/api/portfolio");
    return { ok: false, status: 503, json: async () => ({}) };
  };

  await w.loadPortfolio();

  const status = w.document.getElementById("pfLoadStatus");
  const empty = w.document.getElementById("pfEmpty");
  const table = w.document.getElementById("pfTable");
  assert.equal(status.style.display, "block");
  assert.equal(status.dataset.state, "error");
  assert.match(status.textContent, /포트폴리오를 불러오지 못했습니다/);
  assert.equal(empty.style.display, "block");
  assert.equal(empty.textContent, status.textContent);
  assert.equal(table.style.display, "none");
  assert.equal(reported.length, 1);
  assert.equal(reported[0].context, "포트폴리오");
  assert.equal(reported[0].options.silent, true);
});

test("loadPortfolio keeps cached holdings visible but marks them stale on network failure", async () => {
  const { w } = loadPortfolioDataDom();
  w.PfStore.items = [{ stock_code: "005930", stock_name: "삼성전자" }];
  w.apiFetch = async () => {
    throw new Error("network down");
  };

  await w.loadPortfolio();

  const status = w.document.getElementById("pfLoadStatus");
  assert.equal(status.style.display, "block");
  assert.match(status.textContent, /네트워크 상태/);
  assert.equal(w.document.getElementById("pfTable").style.display, "table");
  assert.equal(w.document.getElementById("pfEmpty").style.display, "none");
});

function loadSortableDom() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <table><thead><tr>
      <th class="pf-sortable" data-sort="name">종목명</th>
      <th class="pf-sortable" data-sort="returnPct">수익률</th>
      <th class="pf-sortable" data-sort="group">그룹</th>
    </tr></thead></table>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  appendScript(w, STORE_SRC);
  appendScript(w, RENDER_SRC);
  return w;
}

test("sortable portfolio headers are keyboard-operable", () => {
  const w = loadSortableDom();
  const calls = [];
  w.pfSort = (key) => calls.push(key);

  w._pfInitSortableHeaders();
  const header = w.document.querySelector('[data-sort="name"]');
  assert.equal(header.getAttribute("tabindex"), "0");
  assert.equal(header.getAttribute("scope"), "col");
  assert.equal(header.getAttribute("aria-sort"), "none");

  header.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));
  header.dispatchEvent(new w.KeyboardEvent("keydown", { key: " ", bubbles: true, cancelable: true }));
  header.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
  assert.deepEqual(calls, ["name", "name"]);
});

test("sortable portfolio headers expose the active sort direction through aria-sort", () => {
  const w = loadSortableDom();
  w.PfStore.sort.key = "returnPct";
  w.PfStore.sort.asc = false;
  w._pfUpdateSortableHeaders();

  const returnHeader = w.document.querySelector('[data-sort="returnPct"]');
  const nameHeader = w.document.querySelector('[data-sort="name"]');
  assert.equal(returnHeader.getAttribute("aria-sort"), "descending");
  assert.equal(returnHeader.querySelector(".pf-sort-arrow").textContent.trim(), "▼");
  assert.equal(nameHeader.getAttribute("aria-sort"), "none");

  w.PfStore.sort.key = null;
  w.PfStore.sort.groupSort = true;
  w._pfUpdateSortableHeaders();
  assert.equal(w.document.querySelector('[data-sort="group"]').getAttribute("aria-sort"), "ascending");
  assert.equal(returnHeader.getAttribute("aria-sort"), "none");
});
