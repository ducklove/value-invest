// jsdom behavior tests for the quote snapshot merge logic in static/js/utils.js:
// quoteSourceRank / shouldAcceptQuoteSnapshot / mergeQuoteSnapshot /
// mergeQuoteSupplementalFields / quoteIsUsable. The Python structure test only
// asserts these names exist as strings — here we execute the real source and
// pin down the merge semantics (source rank ordering, _stale handling,
// price-null rejection, previous-close preservation, identical-tick no-op).
// Run with `npm test` (node --test).

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const UTILS_SRC = readFileSync(
  join(__dirname, "..", "..", "static", "js", "utils.js"),
  "utf8",
);

// Load utils.js as a real <script> so its top-level function declarations
// attach to the window, exactly like the browser.
function loadUtils() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window } = dom;
  const script = window.document.createElement("script");
  script.textContent = UTILS_SRC;
  window.document.body.appendChild(script);
  return window;
}

// The functions are pure — one shared window for the whole file.
const w = loadUtils();

// Merge results are created inside the jsdom realm, whose Object.prototype is
// not ours — deepEqual (strict) compares prototypes, so re-spread the flat
// snapshot objects into this realm before comparing.
const plain = (o) => ({ ...o });

test("quoteSourceRank — ws > rest/quote > unknown > history > stale/null", () => {
  assert.equal(w.quoteSourceRank({ source: "ws" }), 4);
  assert.equal(w.quoteSourceRank({ source: "kis-ws" }), 4);
  assert.equal(w.quoteSourceRank({ source: "rest" }), 3);
  assert.equal(w.quoteSourceRank({ source: "naver-quote" }), 3);
  assert.equal(w.quoteSourceRank({ source: "something-else" }), 2);
  assert.equal(w.quoteSourceRank({}), 2); // no source field at all
  assert.equal(w.quoteSourceRank({ source: "history" }), 1);
  assert.equal(w.quoteSourceRank({ source: "ws", _stale: true }), 0); // stale beats any source
  assert.equal(w.quoteSourceRank(null), 0);
});

test("quoteIsUsable — null/priceless/stale are unusable, price 0 is usable", () => {
  assert.equal(w.quoteIsUsable(null), false);
  assert.equal(w.quoteIsUsable({}), false);
  assert.equal(w.quoteIsUsable({ price: null }), false);
  assert.equal(w.quoteIsUsable({ price: undefined }), false);
  assert.equal(w.quoteIsUsable({ price: 100, _stale: true }), false);
  assert.equal(w.quoteIsUsable({ price: 100 }), true);
  // 0 is a real price value (e.g. suspended issue) — currently accepted.
  assert.equal(w.quoteIsUsable({ price: 0 }), true);
});

test("shouldAcceptQuoteSnapshot — incoming without a price is always rejected", () => {
  const current = { price: 100, source: "history" };
  assert.equal(w.shouldAcceptQuoteSnapshot(current, null), false);
  assert.equal(w.shouldAcceptQuoteSnapshot(current, {}), false);
  assert.equal(w.shouldAcceptQuoteSnapshot(current, { price: null, source: "ws" }), false);
  assert.equal(w.shouldAcceptQuoteSnapshot(current, { price: undefined, source: "ws" }), false);
  // price 0 passes the null/undefined check — documented current behavior.
  assert.equal(w.shouldAcceptQuoteSnapshot(null, { price: 0, source: "ws" }), true);
});

test("shouldAcceptQuoteSnapshot — _stale incoming loses to a usable current, but seeds an empty slot", () => {
  const usable = { price: 100, source: "rest" };
  assert.equal(w.shouldAcceptQuoteSnapshot(usable, { price: 99, _stale: true }), false);
  // No current at all → a stale-but-priced snapshot is better than nothing
  // (both rank 0, time tiebreak passes).
  assert.equal(w.shouldAcceptQuoteSnapshot(null, { price: 99, _stale: true }), true);
  // QUIRK: a priceless current *object* still gets the default source rank 2
  // (quoteSourceRank ignores price), so it beats the stale incoming (rank 0)
  // and the slot keeps showing nothing — documented current behavior.
  assert.equal(
    w.shouldAcceptQuoteSnapshot({ price: null }, { price: 99, _stale: true }),
    false,
  );
});

test("shouldAcceptQuoteSnapshot — snapshot date dominates source rank (YYYYMMDD parsing)", () => {
  // Newer date wins even from a weaker source...
  assert.equal(
    w.shouldAcceptQuoteSnapshot(
      { price: 100, date: "20260609", source: "ws" },
      { price: 101, date: "20260610", source: "history" },
    ),
    true,
  );
  // ...and an older date is rejected even from a stronger source.
  assert.equal(
    w.shouldAcceptQuoteSnapshot(
      { price: 100, date: "20260610", source: "history" },
      { price: 101, date: "20260609", source: "ws" },
    ),
    false,
  );
});

test("shouldAcceptQuoteSnapshot — same date: rank downgrade rejected, upgrade accepted", () => {
  const d = "20260610";
  assert.equal(
    w.shouldAcceptQuoteSnapshot(
      { price: 100, date: d, source: "ws" },
      { price: 101, date: d, source: "history" },
    ),
    false,
  );
  assert.equal(
    w.shouldAcceptQuoteSnapshot(
      { price: 100, date: d, source: "history" },
      { price: 101, date: d, source: "ws" },
    ),
    true,
  );
});

test("shouldAcceptQuoteSnapshot — equal rank falls back to ts (sec vs ms normalized)", () => {
  // current ts in epoch seconds, incoming in epoch ms — 1s older → rejected.
  assert.equal(
    w.shouldAcceptQuoteSnapshot(
      { price: 100, source: "rest", ts: 1_749_500_000 },
      { price: 101, source: "rest", ts: 1_749_499_999_000 },
    ),
    false,
  );
  // 1s newer (still in seconds) → accepted.
  assert.equal(
    w.shouldAcceptQuoteSnapshot(
      { price: 100, source: "rest", ts: 1_749_500_000 },
      { price: 101, source: "rest", ts: 1_749_500_001 },
    ),
    true,
  );
  // Equal ts, or no ts at all → accepted (same-rank ticks flow through).
  assert.equal(
    w.shouldAcceptQuoteSnapshot(
      { price: 100, source: "rest", ts: 1_749_500_000 },
      { price: 101, source: "rest", ts: 1_749_500_000 },
    ),
    true,
  );
  assert.equal(
    w.shouldAcceptQuoteSnapshot({ price: 100, source: "rest" }, { price: 101, source: "rest" }),
    true,
  );
});

test("mergeQuoteSnapshot — accepted incoming overrides, keeps current-only fields, clears _stale", () => {
  const current = {
    price: 100, previous_close: 95, trade_value: 1_000_000,
    date: "20260610", source: "history", _stale: true, name: "삼성전자",
  };
  const incoming = { price: 102, date: "20260610", source: "ws", ts: 1_749_500_000 };
  const merged = w.mergeQuoteSnapshot(current, incoming);
  assert.deepEqual(plain(merged), {
    price: 102, previous_close: 95, trade_value: 1_000_000,
    date: "20260610", source: "ws", ts: 1_749_500_000, name: "삼성전자",
  });
  assert.ok(!("_stale" in merged), "fresh incoming must clear the _stale flag");
  // Inputs are not mutated.
  assert.equal(current.price, 100);
  assert.equal(current._stale, true);
});

test("mergeQuoteSnapshot — stale incoming over empty state keeps the _stale flag", () => {
  const merged = w.mergeQuoteSnapshot(null, { price: 99, _stale: true, source: "rest" });
  assert.deepEqual(plain(merged), { price: 99, _stale: true, source: "rest" });
  assert.equal(w.quoteIsUsable(merged), false); // still flagged for retry
});

test("mergeQuoteSnapshot — rejected incoming still donates previous_close and derives change", () => {
  // ws current without previous_close; weaker history snapshot is rejected for
  // price but must fill the supplemental fields (previous-close preservation).
  const current = { price: 110, date: "20260610", source: "ws" };
  const incoming = {
    price: 100, previous_close: 100, trade_value: 5_000_000,
    date: "20260610", source: "history",
  };
  assert.equal(w.shouldAcceptQuoteSnapshot(current, incoming), false);
  const merged = w.mergeQuoteSnapshot(current, incoming);
  assert.deepEqual(plain(merged), {
    price: 110, date: "20260610", source: "ws",
    previous_close: 100, trade_value: 5_000_000,
    change: 10, change_pct: 10, // recomputed from current price vs donated close
  });
});

test("mergeQuoteSupplementalFields — copies incoming change fields only when close is unusable", () => {
  // previous_close stays missing → cannot derive, so incoming change/_pct are copied.
  const merged = w.mergeQuoteSupplementalFields(
    { price: 110, source: "ws" },
    { price: 100, change: -2, change_pct: -1.8, source: "history" },
  );
  assert.deepEqual(plain(merged), { price: 110, source: "ws", change: -2, change_pct: -1.8 });
  // Existing change values are never overwritten.
  const keep = w.mergeQuoteSupplementalFields(
    { price: 110, change: 5, change_pct: 4.76, source: "ws" },
    { price: 100, change: -2, change_pct: -1.8, source: "history" },
  );
  assert.equal(keep.change, 5);
  assert.equal(keep.change_pct, 4.76);
});

test("mergeQuoteSnapshot — rejected _stale incoming changes nothing", () => {
  const current = { price: 100, previous_close: 95, source: "rest" };
  const merged = w.mergeQuoteSnapshot(current, {
    price: 90, previous_close: 80, trade_value: 7, _stale: true, source: "rest",
  });
  assert.deepEqual(plain(merged), plain(current)); // stale snapshots may not even donate fields
});

test("mergeQuoteSnapshot — identical tick is a no-op (no display regression)", () => {
  const q = {
    price: 100, previous_close: 99, change: 1, change_pct: 1.01,
    date: "20260610", source: "ws", ts: 1_749_550_000,
  };
  const merged = w.mergeQuoteSnapshot(q, { ...q });
  assert.deepEqual(plain(merged), q);
  assert.equal(w.quoteSnapshotDisplayChanged(q, merged), false);
});

test("QUIRK: accepted incoming with an explicitly-undefined field clobbers the current value", () => {
  // mergeQuoteSnapshot uses a plain spread for the accepted path, so a fresher
  // tick that *carries* previous_close: undefined (the sidebar handler in
  // app-main.js always materializes the key from the raw tick) erases the
  // previous_close the current snapshot already had. Documents current
  // behavior — fix would require quoteValuePresent-style filtering.
  const current = { price: 100, previous_close: 95, source: "ws", ts: 1_749_500_000 };
  const carriesUndefined = {
    price: 101, previous_close: undefined, source: "ws", ts: 1_749_500_001,
  };
  const clobbered = w.mergeQuoteSnapshot(current, carriesUndefined);
  assert.equal(clobbered.previous_close, undefined);
  assert.ok("previous_close" in clobbered);
  // Whereas a tick that simply lacks the key preserves it.
  const lacksKey = { price: 101, source: "ws", ts: 1_749_500_001 };
  assert.equal(w.mergeQuoteSnapshot(current, lacksKey).previous_close, 95);
});
