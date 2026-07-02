// jsdom behavior tests for the mobile "recent/starred chips on empty-focus"
// search panel (UX review P1③): on ≤900px viewports the sidebar (최근 검색/
// 관심 목록) is hidden with no replacement, so once a stock is out of view
// there was no way back to it on mobile. Focusing the empty search box now
// shows the same two lists as dropdown chips.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

const SEARCH_SRC = read("static", "js", "search.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function buildDom({ compactMobile, currentUser = null, recentListItems = [], starredItems = [] } = {}) {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div class="search-container">
      <input class="search-input" id="searchInput" autocomplete="off">
      <div class="dropdown" id="dropdown"></div>
    </div>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/analysis",
  });
  const { window: w } = dom;
  // Normally declared once in utils.js (loaded well before search.js in index.html).
  w.searchTimeout = null;
  w.selectedIdx = -1;
  w.isCompactMobileViewport = () => compactMobile;
  w.currentUser = currentUser;
  w.recentListItems = recentListItems;
  w.trackEvent = () => {};
  const analyzeCalls = [];
  w.analyzeStock = (code) => analyzeCalls.push(code);
  const switchViewCalls = [];
  w.switchView = (view) => switchViewCalls.push(view);
  w.apiFetch = async (path) => {
    assert.equal(path, "/api/cache/list?tab=starred");
    return { ok: true, json: async () => starredItems };
  };
  appendScript(w, SEARCH_SRC);
  return { w, analyzeCalls, switchViewCalls };
}

test("focusing the empty search box on mobile shows a 최근 검색 chip section", async () => {
  const { w } = buildDom({
    compactMobile: true,
    recentListItems: [{ stock_code: "005930", corp_name: "삼성전자" }],
  });
  w.searchInput.dispatchEvent(new w.Event("focus"));
  await new Promise((r) => setTimeout(r, 0));

  assert.equal(w.dropdown.classList.contains("show"), true);
  const labels = [...w.dropdown.querySelectorAll(".dropdown-section-label")].map(el => el.textContent);
  assert.deepEqual(labels, ["최근 검색"]); // no starred section for a guest
  const chip = w.dropdown.querySelector('.dropdown-item[data-stock="005930"]');
  assert.ok(chip, "recent chip should be rendered");
});

test("focusing the empty search box on desktop does not open the chip panel", async () => {
  const { w } = buildDom({
    compactMobile: false,
    recentListItems: [{ stock_code: "005930", corp_name: "삼성전자" }],
  });
  w.searchInput.dispatchEvent(new w.Event("focus"));
  await new Promise((r) => setTimeout(r, 0));

  assert.equal(w.dropdown.classList.contains("show"), false);
});

test("logged-in mobile users see both 최근 검색 and 관심 목록 sections, fetched once", async () => {
  let fetchCount = 0;
  const { w } = buildDom({
    compactMobile: true,
    currentUser: { email: "a@b.com" },
    recentListItems: [{ stock_code: "005930", corp_name: "삼성전자" }],
    starredItems: [{ stock_code: "000660", corp_name: "SK하이닉스" }],
  });
  const realFetch = w.apiFetch;
  w.apiFetch = async (...args) => { fetchCount += 1; return realFetch(...args); };

  w.searchInput.dispatchEvent(new w.Event("focus"));
  await new Promise((r) => setTimeout(r, 0));
  const labels = [...w.dropdown.querySelectorAll(".dropdown-section-label")].map(el => el.textContent);
  assert.deepEqual(labels, ["최근 검색", "관심 목록"]);
  assert.ok(w.dropdown.querySelector('.dropdown-item[data-stock="000660"]'), "starred chip should be rendered");

  // Re-focusing should reuse the cached starred list, not refetch.
  w.searchInput.dispatchEvent(new w.Event("focus"));
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(fetchCount, 1);
});

test("clicking a chip analyzes that stock, switches to analysis, and closes the dropdown", async () => {
  const { w, analyzeCalls, switchViewCalls } = buildDom({
    compactMobile: true,
    recentListItems: [{ stock_code: "005930", corp_name: "삼성전자" }],
  });
  w.searchInput.dispatchEvent(new w.Event("focus"));
  await new Promise((r) => setTimeout(r, 0));

  w.dropdown.querySelector('.dropdown-item[data-stock="005930"]').dispatchEvent(new w.Event("click", { bubbles: true }));

  assert.deepEqual(analyzeCalls, ["005930"]);
  assert.deepEqual(switchViewCalls, ["analysis"]);
  assert.equal(w.dropdown.classList.contains("show"), false);
  assert.equal(w.searchInput.value, "삼성전자");
});

test("clearing the search input back to empty on mobile re-opens the chip panel", async () => {
  const { w } = buildDom({
    compactMobile: true,
    recentListItems: [{ stock_code: "005930", corp_name: "삼성전자" }],
  });
  w.searchInput.value = "삼성";
  w.searchInput.dispatchEvent(new w.Event("input"));
  w.searchInput.value = "";
  w.searchInput.dispatchEvent(new w.Event("input"));
  await new Promise((r) => setTimeout(r, 0));

  assert.equal(w.dropdown.classList.contains("show"), true);
  assert.ok(w.dropdown.querySelector('.dropdown-item[data-stock="005930"]'));
});
