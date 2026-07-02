// jsdom behavior tests for the switchView ↔ browser-history sync (UX review P1②):
// before this, tab switches never touched history/URL, so the back button exited
// the app instead of returning to the previous tab. Covers switchView's pushState
// writes, the skipHistory escape hatch used by initial routing + popstate restore,
// and that the shared PF_VIEW_PATHS/PF_PATH_TO_VIEW tables round-trip correctly
// (the exact class of bug a two-copies-of-the-same-map design invites).

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
const SHELL_SRC = read("static", "js", "portfolio-shell.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

const VIEW_IDS = [
  "investingView", "analysisView", "portfolioView",
  "labsView", "insightsView", "screenerView",
];

function buildDom(startUrl) {
  const dom = new JSDOM(`<!doctype html><html><body>
    <nav>
      <button class="nav-btn" data-view="investing"></button>
      <button class="nav-btn" data-view="analysis"></button>
      <button class="nav-btn" data-view="portfolio"></button>
      <button class="nav-btn" data-view="labs"></button>
      <button class="mnav-btn" data-view="labs"></button>
    </nav>
    ${VIEW_IDS.map(id => `<div id="${id}" style="display:none;"></div>`).join("\n")}
  </body></html>`, {
    runScripts: "dangerously",
    url: startUrl,
  });
  const { window: w } = dom;
  // jsdom has no matchMedia implementation; portfolio-shell.js's simple-mode-viewport
  // detection calls it at load time (an IIFE), so stub it before the script runs.
  w.matchMedia = () => ({ matches: false, addListener() {}, removeListener() {}, addEventListener() {}, removeEventListener() {} });
  appendScript(w, STORE_SRC);
  appendScript(w, SHELL_SRC);
  // switchView calls these unconditionally for some views; stub to isolate the
  // history/DOM-visibility behavior under test from unrelated data loading.
  w.loadPortfolio = () => {};
  w._updateQuoteSubscriptions = () => {};
  return w;
}

test("switchView pushes a new history entry with the matching URL for each top-level view", () => {
  const w = buildDom("https://app.example.com/investing");
  assert.equal(w.location.pathname, "/investing");

  w.switchView("portfolio");
  assert.equal(w.location.pathname, "/portfolio");
  assert.equal(w.document.getElementById("portfolioView").style.display, "block");

  w.switchView("analysis");
  assert.equal(w.location.pathname, "/analysis");
});

test("switchView does not push a duplicate entry when already on the target view's path", () => {
  const w = buildDom("https://app.example.com/investing");
  const before = w.history.length;

  w.switchView("investing"); // already here — should be a no-op for history
  assert.equal(w.history.length, before);

  w.switchView("portfolio"); // different view — should push exactly one entry
  assert.equal(w.history.length, before + 1);
});

test("skipHistory leaves the URL untouched while still swapping the visible view (initial-load / popstate path)", () => {
  const w = buildDom("https://app.example.com/?code=005930");
  const before = w.history.length;

  w.switchView("analysis", { skipHistory: true });

  assert.equal(w.location.pathname, "/"); // URL/query preserved, not overwritten
  assert.equal(w.location.search, "?code=005930");
  assert.equal(w.history.length, before); // no new entry
  assert.equal(w.document.getElementById("analysisView").style.display, "block");
});

test("PF_VIEW_PATHS and PF_PATH_TO_VIEW round-trip for every top-level view", () => {
  const w = buildDom("https://app.example.com/investing");
  // Top-level const/let in a classic <script> stay in the page's shared lexical
  // scope but never become window properties, so a follow-up script that runs
  // in that same scope is the only way to expose them for the test to read.
  appendScript(w, "window.__pfViewPaths = PF_VIEW_PATHS; window.__pfPathToView = PF_PATH_TO_VIEW;");
  for (const [view, path] of Object.entries(w.__pfViewPaths)) {
    assert.equal(w.__pfPathToView[path], view, `PF_PATH_TO_VIEW['${path}'] should map back to '${view}'`);
  }
});

test("nps/insights/screener sub-pages highlight the 도구 (labs) nav button, not a nonexistent one", () => {
  const w = buildDom("https://app.example.com/investing");
  w.switchView("screener");
  const labsButtons = [...w.document.querySelectorAll('[data-view="labs"]')];
  assert.ok(labsButtons.length > 0);
  for (const btn of labsButtons) assert.equal(btn.classList.contains("active"), true);
});
