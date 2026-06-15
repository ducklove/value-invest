// jsdom behavior tests for portfolio trend helpers.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TRENDS_SRC = readFileSync(
  join(__dirname, "..", "..", "static", "js", "portfolio-trends.js"),
  "utf8",
);

function loadTrends() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/portfolio",
  });
  const script = dom.window.document.createElement("script");
  script.textContent = TRENDS_SRC;
  dom.window.document.body.appendChild(script);
  return dom.window;
}

test("period return helpers use calendar days, not sample count", () => {
  const w = loadTrends();
  const data = [
    { date: "2026-01-01" },
    { date: "2026-01-02" },
    { date: "2026-01-03" },
    { date: "2026-01-04" },
    { date: "2026-01-05" },
    { date: "2026-01-06" },
    { date: "2026-01-07" },
    { date: "2026-01-08" },
  ];
  const values = [100, 101, 102, 103, 104, 105, 106, 108];

  // Seven calendar days back from Jan 8 is Jan 1. The old slice(-7) logic
  // would have used Jan 2 and reported a smaller return.
  assert.equal(w._findLookbackIndexByDays(data, 7), 0);
  assert.ok(Math.abs(w._periodPctByCalendarDays(data, values, 7) - 8) < 0.000001);
});

test("period return helper returns null when history does not reach the requested date", () => {
  const w = loadTrends();
  const data = [{ date: "2026-01-05" }, { date: "2026-01-08" }];
  assert.equal(w._periodPctByCalendarDays(data, [100, 105], 7), null);
});
