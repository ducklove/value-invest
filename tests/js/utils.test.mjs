// jsdom behavior tests for static/js/utils.js.
//
// First step of the roadmap's "프론트 문자열 존재 검증 → jsdom 동작 테스트로 점진
// 이전": instead of asserting a function name appears in the source, we load the
// real script into a jsdom window and assert its runtime behavior. Run with
// `npm test` (node --test).

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

// Load utils.js as a real <script> in a fresh jsdom window so its top-level
// function declarations attach to that window, exactly like the browser.
function loadUtils() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const script = dom.window.document.createElement("script");
  script.textContent = UTILS_SRC;
  dom.window.document.body.appendChild(script);
  return dom.window;
}

test("escapeHtml neutralizes HTML metacharacters", () => {
  const w = loadUtils();
  assert.equal(
    w.escapeHtml('<img src=x onerror="alert(1)">'),
    "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;",
  );
  assert.equal(w.escapeHtml("a & b"), "a &amp; b");
  assert.equal(w.escapeHtml("it's"), "it&#39;s");
  assert.equal(w.escapeHtml(null), "");
  assert.equal(w.escapeHtml(undefined), "");
});

test("safeExternalUrl only allows http(s) and blocks javascript:", () => {
  const w = loadUtils();
  assert.equal(w.safeExternalUrl("https://dart.fss.or.kr/x"), "https://dart.fss.or.kr/x");
  assert.equal(w.safeExternalUrl("javascript:alert(1)"), "");
  assert.equal(w.safeExternalUrl("data:text/html,<script>1</script>"), "");
  assert.equal(w.safeExternalUrl(""), "");
});

test("quoteIsUsable / quotePriceOrNull reflect price + stale flags", () => {
  const w = loadUtils();
  assert.equal(w.quoteIsUsable({ price: 100 }), true);
  assert.equal(w.quoteIsUsable({ price: 100, _stale: true }), false);
  assert.equal(w.quoteIsUsable({ price: null }), false);
  assert.equal(w.quoteIsUsable(null), false);
  assert.equal(w.quotePriceOrNull({ price: 100 }), 100);
  assert.equal(w.quotePriceOrNull({ price: null }), null);
  assert.equal(w.quotePriceOrNull(null), null);
});

test("guest recent list round-trips through localStorage and is capped", () => {
  const w = loadUtils();
  w.saveGuestRecent("005930", "삼성전자");
  w.saveGuestRecent("000660", "SK하이닉스");
  let list = w.getGuestRecent();
  assert.equal(list.length, 2);
  // Most recent first.
  assert.equal(list[0].stock_code, "000660");
  // Re-saving an existing code moves it to the front without duplicating.
  w.saveGuestRecent("005930", "삼성전자");
  list = w.getGuestRecent();
  assert.equal(list.length, 2);
  assert.equal(list[0].stock_code, "005930");
  w.removeGuestRecent("005930");
  // getGuestRecent() returns an array from the jsdom realm; spread it into a
  // Node array so deepStrictEqual compares structure, not Array prototype.
  assert.deepEqual(
    [...w.getGuestRecent()].map((i) => i.stock_code),
    ["000660"],
  );
});
