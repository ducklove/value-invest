// jsdom behavior tests for static/js/analysis.js — focused on the external-tool
// deep-link cards (renderStockExternalLinks). Loads utils.js (escapeHtml) +
// analysis.js into a jsdom window. Network-bound loadStockExternalLinks() and
// the heavy analyze flow are not exercised here.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (p) => readFileSync(join(__dirname, "..", "..", "static", "js", p), "utf8");

function load() {
  const dom = new JSDOM(
    "<!doctype html><html><body><div id='coverageNote'></div></body></html>",
    { runScripts: "dangerously", url: "https://app.example.com/" },
  );
  for (const src of [read("utils.js"), read("analysis.js")]) {
    const s = dom.window.document.createElement("script");
    s.textContent = src;
    dom.window.document.body.appendChild(s);
  }
  return dom.window;
}

test("_externalValuationCards builds preferred + holding valuation-cards, escapes + neutralizes urls", () => {
  const w = load();
  const cards = w._externalValuationCards({
    preferred: {
      name: "<b>x</b>", preferredName: "삼성전자우",
      spread: 36.12, commonPrice: 317000, preferredPrice: 202500,
      url: "javascript:alert(1)",
    },
    holding: {
      name: "영풍", ratio: 781.87, holdingValue: 72253.4, marketCap: 9241.1,
      url: "https://ducklove.github.io/holding_value/?code=000670",
    },
  });
  assert.equal(cards.length, 2);
  // they share the .valuation-card grid styling (not a separate widget)
  const root = w.document.getElementById("coverageNote");
  root.innerHTML = cards.join("");
  const els = root.querySelectorAll("a.valuation-card.is-link");
  assert.equal(els.length, 2);
  assert.match(root.innerHTML, /36\.1%/);   // spread percent
  assert.match(root.innerHTML, /781\.9%/);  // ratio percent
  assert.ok(!root.innerHTML.includes("<b>x</b>"));  // hostile name escaped
  // javascript: url neutralized to '#', https preserved
  assert.equal(els[0].getAttribute("href"), "#");
  assert.equal(els[0].getAttribute("target"), "_blank");
  assert.equal(els[1].getAttribute("href"), "https://ducklove.github.io/holding_value/?code=000670");
});

test("_externalValuationCards returns only the matching card", () => {
  const w = load();
  const cards = w._externalValuationCards({ holding: { name: "영풍", ratio: 781.87, url: "https://x.test/" } });
  assert.equal(cards.length, 1);
  assert.match(cards[0], /지주사/);
});

test("_externalValuationCards returns [] when no links", () => {
  const w = load();
  assert.deepEqual([...w._externalValuationCards(null)], []);
  assert.deepEqual([...w._externalValuationCards({})], []);
});
