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
    "<!doctype html><html><body><div id='stockExternalLinks'></div></body></html>",
    { runScripts: "dangerously", url: "https://app.example.com/" },
  );
  for (const src of [read("utils.js"), read("analysis.js")]) {
    const s = dom.window.document.createElement("script");
    s.textContent = src;
    dom.window.document.body.appendChild(s);
  }
  return dom.window;
}

test("renderStockExternalLinks builds preferred + holding cards, escapes + neutralizes urls", () => {
  const w = load();
  const root = w.document.getElementById("stockExternalLinks");
  w.renderStockExternalLinks(root, {
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
  const cards = root.querySelectorAll(".sxl-card");
  assert.equal(cards.length, 2);
  assert.match(root.innerHTML, /36\.1%/);   // spread percent
  assert.match(root.innerHTML, /781\.9%/);  // ratio percent
  // hostile name escaped
  assert.ok(!root.innerHTML.includes("<b>x</b>"));
  // javascript: url neutralized to '#', https url preserved
  assert.equal(cards[0].getAttribute("href"), "#");
  assert.equal(cards[0].getAttribute("target"), "_blank");
  assert.equal(cards[1].getAttribute("href"), "https://ducklove.github.io/holding_value/?code=000670");
});

test("renderStockExternalLinks renders only the matching card", () => {
  const w = load();
  const root = w.document.getElementById("stockExternalLinks");
  w.renderStockExternalLinks(root, { holding: { name: "영풍", ratio: 781.87, url: "https://x.test/" } });
  const cards = root.querySelectorAll(".sxl-card");
  assert.equal(cards.length, 1);
  assert.match(cards[0].innerHTML, /지주사/);
});

test("renderStockExternalLinks renders empty string when no match", () => {
  const w = load();
  const root = w.document.getElementById("stockExternalLinks");
  w.renderStockExternalLinks(root, {});
  assert.equal(root.innerHTML, "");
});
