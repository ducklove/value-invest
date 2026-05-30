// jsdom test for the portfolio quote-tick row lookup.
//
// updatePortfolioRowQuote() now locates the row with
//   tbody.querySelector(`tr[data-code="${CSS.escape(code)}"]`)
// instead of scanning every tr[data-code] in JS. This verifies the selector
// resolves the correct row — including codes with dots (e.g. A200.AX) that
// require CSS.escape — and matches a brute-force scan.

import test from "node:test";
import assert from "node:assert/strict";
import { JSDOM } from "jsdom";

// jsdom doesn't implement the CSS interface; every target browser does. Provide
// the spec-compliant CSS.escape polyfill so the test exercises the same
// selector escaping the production code relies on.
function cssEscape(value) {
  const string = String(value);
  const length = string.length;
  const firstCodeUnit = string.charCodeAt(0);
  let result = "";
  let index = -1;
  while (++index < length) {
    const codeUnit = string.charCodeAt(index);
    if (codeUnit === 0x0000) { result += "�"; continue; }
    if (
      (codeUnit >= 0x0001 && codeUnit <= 0x001f) || codeUnit === 0x007f ||
      (index === 0 && codeUnit >= 0x0030 && codeUnit <= 0x0039) ||
      (index === 1 && codeUnit >= 0x0030 && codeUnit <= 0x0039 && firstCodeUnit === 0x002d)
    ) {
      result += "\\" + codeUnit.toString(16) + " ";
      continue;
    }
    if (index === 0 && length === 1 && codeUnit === 0x002d) { result += "\\" + string.charAt(index); continue; }
    if (
      codeUnit >= 0x0080 || codeUnit === 0x002d || codeUnit === 0x005f ||
      (codeUnit >= 0x0030 && codeUnit <= 0x0039) ||
      (codeUnit >= 0x0041 && codeUnit <= 0x005a) ||
      (codeUnit >= 0x0061 && codeUnit <= 0x007a)
    ) {
      result += string.charAt(index);
      continue;
    }
    result += "\\" + string.charAt(index);
  }
  return result;
}

function buildTable(codes) {
  const dom = new JSDOM("<!doctype html><html><body><table><tbody id='pfBody'></tbody></table></body></html>");
  const { window } = dom;
  window.CSS = window.CSS || { escape: cssEscape };
  const tbody = window.document.getElementById("pfBody");
  for (const code of codes) {
    const tr = window.document.createElement("tr");
    tr.setAttribute("data-code", code);
    tbody.appendChild(tr);
  }
  return window;
}

// Mirror of the production lookup so we test the exact mechanism.
function lookup(window, code) {
  const tbody = window.document.getElementById("pfBody");
  return tbody.querySelector(`tr[data-code="${window.CSS.escape(code)}"]`);
}

function scan(window, code) {
  const tbody = window.document.getElementById("pfBody");
  for (const t of tbody.querySelectorAll("tr[data-code]")) {
    if (t.dataset.code === code) return t;
  }
  return null;
}

test("row lookup resolves plain, dotted and alphanumeric codes", () => {
  const codes = ["005930", "A200.AX", "BRK.B", "0074K0", "CASH_USD"];
  const window = buildTable(codes);
  for (const code of codes) {
    const found = lookup(window, code);
    assert.ok(found, `lookup found ${code}`);
    assert.equal(found.dataset.code, code);
    // Selector-based lookup agrees with a brute-force scan.
    assert.equal(found, scan(window, code));
  }
});

test("row lookup returns null for an absent code without throwing", () => {
  const window = buildTable(["005930", "A200.AX"]);
  assert.equal(lookup(window, "999999"), null);
  // A code containing characters that are special in a selector must not throw.
  assert.equal(lookup(window, 'x"]><script>'), null);
});
