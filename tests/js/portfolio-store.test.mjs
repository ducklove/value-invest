// jsdom test for the portfolio state store (static/js/portfolio-store.js).
//
// First slice of the global-state -> store migration: benchmark quotes now live
// on PfStore.benchmarkQuotes instead of a bare `pfBenchmarkQuotes` global.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const STORE_SRC = readFileSync(
  join(__dirname, "..", "..", "static", "js", "portfolio-store.js"),
  "utf8",
);

function loadStore() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const script = dom.window.document.createElement("script");
  script.textContent = STORE_SRC;
  dom.window.document.body.appendChild(script);
  return dom.window;
}

test("PfStore is exposed with a benchmarkQuotes map", () => {
  const w = loadStore();
  assert.equal(typeof w.PfStore, "object");
  assert.ok(w.PfStore !== null);
  // Cross-realm object: compare shape via key count, not deepEqual({}).
  assert.equal(typeof w.PfStore.benchmarkQuotes, "object");
  assert.equal(Object.keys(w.PfStore.benchmarkQuotes).length, 0);
});

test("benchmarkQuotes is a single mutable store others can share", () => {
  const w = loadStore();
  // A consumer writes a benchmark quote...
  w.PfStore.benchmarkQuotes["IDX_KOSPI"] = { change_pct: 1.2, name: "코스피" };
  // ...and any later reader sees the same object (single source of truth).
  assert.equal(w.PfStore.benchmarkQuotes["IDX_KOSPI"].change_pct, 1.2);
  assert.equal(w.PfStore.benchmarkQuotes["IDX_KOSPI"].name, "코스피");
  // Fresh load starts empty (no shared module-level leakage between sessions).
  const w2 = loadStore();
  assert.equal(Object.keys(w2.PfStore.benchmarkQuotes).length, 0);
});
