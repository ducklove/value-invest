// jsdom behaviour test for the /portfolio?focus=CODE deep link (portfolio-render.js
// pfFocusHolding): scroll the held row into view once, highlight it briefly, survive
// re-renders during the highlight, and give up quietly when the code is not held.
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const RENDER = readFileSync(new URL('../../static/js/portfolio-render.js', import.meta.url), 'utf8');
// Only the focus section — renderPortfolio itself needs the whole portfolio stack.
const FOCUS = RENDER.slice(RENDER.indexOf('// --- /portfolio?focus=CODE'), RENDER.indexOf('function returnClass('));

function load() {
  const dom = new JSDOM('<!doctype html><html><body><table><tbody id="pfBody"></tbody></table></body></html>', {
    runScripts: 'dangerously', url: 'https://app.example.com/portfolio?focus=005930',
  });
  const w = dom.window;
  w.PfStore = { items: [] };
  w.eval(FOCUS + '\nwindow.__applyFocus = _pfApplyFocus;');
  const scrolled = [];
  w.renderRows = (codes) => {
    const tbody = w.document.getElementById('pfBody');
    tbody.innerHTML = codes.map(code => `<tr data-code="${code}"><td>${code}</td></tr>`).join('');
    tbody.querySelectorAll('tr').forEach(tr => { tr.scrollIntoView = (opts) => scrolled.push([tr.dataset.code, opts.block]); });
    w.PfStore.items = codes.map(code => ({ stock_code: code }));
    w.__applyFocus();  // renderPortfolio calls _pfApplyFocus at the end of every full render
  };
  return { w, scrolled };
}

const focused = w => [...w.document.querySelectorAll('tr.pf-row-focus')].map(tr => tr.dataset.code);

test('focus waits for the rows, scrolls once, and re-applies the highlight across re-renders', () => {
  const { w, scrolled } = load();
  w.pfFocusHolding('005930');           // before the portfolio loaded: nothing to do yet
  assert.deepEqual(scrolled, []);
  w.renderRows(['000660', '005930']);
  assert.deepEqual(scrolled, [['005930', 'center']]);
  assert.deepEqual(focused(w), ['005930']);
  w.renderRows(['005930', '000660']);   // quote-driven re-render inside the highlight window
  assert.deepEqual(focused(w), ['005930']);
  assert.equal(scrolled.length, 1, 'scrolls only once');
  w.close();
});

test('focus gives up quietly when the code is not held, and a cleared highlight stays cleared', () => {
  const { w, scrolled } = load();
  w.renderRows(['000660']);
  w.pfFocusHolding('005930');
  assert.deepEqual(scrolled, []);
  w.renderRows(['000660', '005930']);   // a later render must not jump to a dropped focus
  assert.deepEqual(scrolled, []);
  assert.deepEqual(focused(w), []);

  w.pfFocusHolding('000660');
  assert.deepEqual(focused(w), ['000660']);
  w._pfClearFocus();  // what the highlight timer runs after PF_FOCUS_HIGHLIGHT_MS
  assert.deepEqual(focused(w), []);
  w.renderRows(['000660']);
  assert.deepEqual(focused(w), [], 'cleared focus stays cleared');
  w.close();
});
