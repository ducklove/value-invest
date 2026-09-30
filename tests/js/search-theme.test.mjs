// jsdom behaviour tests for the hub theme contract (UI-3): the pre-paint
// vc:theme-boot block inlined in index.html <head> and search.js's theme code
// share one rule — ?theme=light|dark (applied, never stored) > localStorage
// 'theme' > prefers-color-scheme — and follow OS changes while nothing is stored.
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
const INDEX = read('static/index.html');
const SEARCH = read('static/js/search.js');
const BOOT_BLOCK = INDEX.match(/<!-- vc:theme-boot --><script>\n([\s\S]*?)\n<\/script><!-- \/vc:theme-boot -->/);

function load({ url = 'https://app.example.com/analysis', storage = {}, dark = false, boot = true } = {}) {
  const dom = new JSDOM(`<!doctype html><html lang="ko" data-theme="light"><body>
    <input id="searchInput"><div id="dropdown"></div></body></html>`, { url, runScripts: 'dangerously' });
  const w = dom.window;
  const listeners = [];
  const mq = { matches: dark, addEventListener: (_t, fn) => listeners.push(fn), removeEventListener() {} };
  w.matchMedia = () => mq;
  for (const [key, value] of Object.entries(storage)) w.localStorage.setItem(key, value);
  if (boot) w.eval(BOOT_BLOCK[1]);
  const events = [];
  Object.assign(w, {
    trackEvent: (name, params) => events.push([name, { ...params }]),
    escapeHtml: s => String(s), isCompactMobileViewport: () => false, currentUser: null, recentListItems: [],
    syncNpsFrameTheme: () => events.push(['nps']),
    syncBondsFrameTheme: () => events.push(['bonds']),
    syncMarketDashboardFrameTheme: () => events.push(['dashboard']),
  });
  const s = w.document.createElement('script');
  s.textContent = SEARCH;
  w.document.body.appendChild(s);
  const osChange = (matches) => { mq.matches = matches; listeners.forEach(fn => fn({ matches })); };
  return { w, events, osChange };
}

const theme = w => w.document.documentElement.getAttribute('data-theme');

test('index.html inlines the canonical theme-boot block before any stylesheet', () => {
  assert.ok(BOOT_BLOCK, 'vc:theme-boot markers with an inlined <script>');
  const canonical = read('static/ecosystem/vc-theme-boot.js').replace(/\s+$/, '');
  assert.equal(BOOT_BLOCK[1], canonical, 'regenerate with: node scripts/sync-ecosystem.mjs --write');
  assert.ok(INDEX.indexOf('<!-- vc:theme-boot -->') < INDEX.indexOf('<link rel="stylesheet"'));
});

test('first paint follows ?theme > stored > OS, and ?theme is never stored', () => {
  let t = load({ url: 'https://app.example.com/analysis?code=005930&theme=dark', storage: { theme: 'light' } });
  assert.equal(theme(t.w), 'dark');
  assert.equal(t.w.localStorage.getItem('theme'), 'light');
  t.w.close();
  t = load({ storage: { theme: 'dark' } });
  assert.equal(theme(t.w), 'dark');
  t.w.close();
  t = load({ dark: true });
  assert.equal(theme(t.w), 'dark');
  assert.equal(t.w.localStorage.getItem('theme'), null);
  t.w.close();
  // search.js alone (cached HTML without the boot block) reaches the same answer.
  t = load({ url: 'https://app.example.com/?theme=dark', boot: false });
  assert.equal(theme(t.w), 'dark');
  t.w.close();
});

test('toggleTheme stores the choice, strips the one-shot ?theme and re-syncs embeds once', () => {
  const { w, events } = load({ url: 'https://app.example.com/analysis?code=005930&theme=dark#x' });
  events.length = 0;
  w.toggleTheme();
  assert.equal(theme(w), 'light');
  assert.equal(w.localStorage.getItem('theme'), 'light');
  assert.equal(w.location.pathname + w.location.search + w.location.hash, '/analysis?code=005930#x');
  assert.deepEqual(events, [['nps'], ['bonds'], ['dashboard'], ['theme_toggle', { theme: 'light' }]]);

  // A vc:themechange for a theme we already synced does not reload the embeds again.
  events.length = 0;
  w.document.dispatchEvent(new w.CustomEvent('vc:themechange', { detail: { theme: 'light' } }));
  assert.deepEqual(events, []);
  w.close();
});

test('OS colour-scheme changes are followed only while no theme is stored', () => {
  let t = load({ dark: false });
  t.events.length = 0;
  t.osChange(true);
  assert.equal(theme(t.w), 'dark');
  assert.deepEqual(t.events, [['nps'], ['bonds'], ['dashboard']]);
  t.w.close();

  t = load({ dark: false, storage: { theme: 'light' } });
  t.osChange(true);
  assert.equal(theme(t.w), 'light', 'an explicit choice wins over the OS');
  t.w.close();

  t = load({ url: 'https://app.example.com/?theme=light', dark: false });
  t.osChange(true);
  assert.equal(theme(t.w), 'light', '?theme pins this page view');
  t.w.close();
});
