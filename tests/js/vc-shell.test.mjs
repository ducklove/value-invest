// jsdom behaviour tests for the ecosystem shell assets in static/ecosystem/:
// vc-shell.js (<vc-shell> + window.VCShell) and vc-theme-boot.js (pre-paint snippet).
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const SHELL = readFileSync(new URL('../../static/ecosystem/vc-shell.js', import.meta.url), 'utf8');
const BOOT = readFileSync(new URL('../../static/ecosystem/vc-theme-boot.js', import.meta.url), 'utf8');
const CPS = 'https://ducklove.github.io/common_preferred_spread/';
const FALLBACK = '<vc-shell tool="common_preferred_spread"><a class="hub-link" href="https://ducklove.duckdns.org:3691">Value Compass ↗</a></vc-shell><main id="app">app</main>';
const tick = () => new Promise(resolve => setTimeout(resolve, 0));

function stubMatchMedia(w, dark) {
  const listeners = [];
  w.matchMedia = media => ({
    matches: dark, media,
    addEventListener: (_type, fn) => listeners.push(fn),
    removeEventListener() {},
  });
  return listeners;
}

function setup({ url = CPS, body = FALLBACK, storage = {}, dark = false, boot = false, shell = true, htmlTheme = null } = {}) {
  const htmlAttr = htmlTheme ? ` data-theme="${htmlTheme}"` : '';
  const dom = new JSDOM(`<!doctype html><html${htmlAttr}><head></head><body>${body}</body></html>`, { url, runScripts: 'dangerously' });
  const w = dom.window;
  const mqListeners = stubMatchMedia(w, dark);
  for (const [key, value] of Object.entries(storage)) w.localStorage.setItem(key, value);
  if (boot) w.eval(BOOT);
  if (shell) w.eval(SHELL);
  const el = w.document.querySelector('vc-shell');
  return { dom, w, el, mqListeners, root: w.document.documentElement };
}

const theme = w => w.document.documentElement.getAttribute('data-theme');

test('theme boot: ?theme > stored theme > prefers-color-scheme, and ?theme is never stored', () => {
  let s = setup({ dark: true, boot: true, shell: false });
  assert.equal(theme(s.w), 'dark');
  s.w.close();
  s = setup({ dark: true, storage: { theme: 'light' }, boot: true, shell: false });
  assert.equal(theme(s.w), 'light');
  s.w.close();
  s = setup({ url: CPS + '?theme=dark', dark: false, storage: { theme: 'light' }, boot: true, shell: false });
  assert.equal(theme(s.w), 'dark');
  assert.equal(s.w.localStorage.getItem('theme'), 'light');
  s.w.close();
  s = setup({ url: CPS + '?theme=purple', dark: true, boot: true, shell: false });
  assert.equal(theme(s.w), 'dark');
  s.w.close();
});

test('theme boot migrates legacy keys once, only when theme is absent, and keeps legacy keys', () => {
  let s = setup({ storage: { 'eiayn:theme:v1': '"dark"' }, boot: true, shell: false });
  assert.equal(theme(s.w), 'dark');
  assert.equal(s.w.localStorage.getItem('theme'), 'dark');
  assert.equal(s.w.localStorage.getItem('eiayn:theme:v1'), '"dark"');
  s.w.close();
  s = setup({ storage: { 'bondmate.theme': 'auto', 'spac-hunter-theme': 'dark' }, boot: true, shell: false });
  assert.equal(s.w.localStorage.getItem('theme'), 'dark');
  s.w.close();
  s = setup({ storage: { theme: 'light', 'nps-theme': 'dark' }, dark: true, boot: true, shell: false });
  assert.equal(theme(s.w), 'light');
  assert.equal(s.w.localStorage.getItem('theme'), 'light');
  s.w.close();
});

test('theme boot survives blocked storage and flags embed / headless pages', () => {
  const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: CPS + '?embed=1', runScripts: 'dangerously' });
  stubMatchMedia(dom.window, true);
  Object.defineProperty(dom.window, 'localStorage', { get() { throw new Error('SecurityError'); } });
  dom.window.eval(BOOT);
  assert.equal(theme(dom.window), 'dark');
  assert.ok(dom.window.document.documentElement.hasAttribute('data-embed'));
  dom.window.close();
  for (const [query, embedded] of [['?embed=0', false], ['?embed=false', false], ['?embed', true], ['?headless=1', true], ['', false]]) {
    const s = setup({ url: CPS + query, boot: true, shell: false });
    assert.equal(s.root.hasAttribute('data-embed'), embedded, query);
    s.w.close();
  }
});

test('shell stays hidden (no shadow render) when embedded, headless, disabled or framed', () => {
  for (const query of ['?embed=1', '?embed=bonds', '?headless=1', '?vc-shell=0']) {
    const s = setup({ url: CPS + query });
    assert.equal(s.el.hidden, true, query);
    assert.equal(s.el.shadowRoot, null, query);
    s.w.close();
  }
  const s = setup({ url: CPS + '?embed=0' });
  assert.equal(s.el.hidden, false);
  assert.ok(s.el.shadowRoot);
  s.w.close();

  const dom = new JSDOM('<!doctype html><html><body><iframe></iframe></body></html>', { url: CPS, runScripts: 'dangerously' });
  const frame = dom.window.document.querySelector('iframe').contentWindow;
  stubMatchMedia(frame, false);
  frame.document.body.innerHTML = FALLBACK;
  frame.eval(SHELL);
  const framed = frame.document.querySelector('vc-shell');
  assert.equal(framed.hidden, true);
  assert.equal(framed.shadowRoot, null);
  dom.window.close();
});

test('fallback hub link stays in the light DOM when the script never runs, and survives upgrade', () => {
  let s = setup({ shell: false });
  const anchor = s.el.querySelector('a.hub-link');
  assert.equal(anchor.getAttribute('href'), 'https://ducklove.duckdns.org:3691');
  assert.equal(anchor.textContent, 'Value Compass ↗');
  assert.equal(s.el.shadowRoot, null);
  assert.equal(s.w.VCShell, undefined);
  s.w.close();
  s = setup();
  assert.ok(s.el.shadowRoot.querySelector('nav.bar'));
  assert.ok(s.el.querySelector('a.hub-link'), 'light-DOM fallback is left untouched');
  s.w.close();
});

test('VCShell.getTheme follows precedence; storage events and setTheme re-apply and notify', async () => {
  const s = setup({ dark: true, htmlTheme: 'dark' });
  assert.equal(s.w.VCShell.getTheme(), 'dark');
  const seen = [];
  s.w.document.addEventListener('vc:themechange', e => seen.push(e.detail.theme));
  s.w.localStorage.setItem('theme', 'light');
  s.w.dispatchEvent(new s.w.StorageEvent('storage', { key: 'theme', newValue: 'light' }));
  assert.equal(theme(s.w), 'light');
  assert.deepEqual(seen, ['light']);
  await tick();
  assert.equal(s.el.getAttribute('data-theme'), 'light', 'host reflects page theme');
  s.w.VCShell.setTheme('dark');
  assert.equal(s.w.localStorage.getItem('theme'), 'dark');
  assert.equal(theme(s.w), 'dark');
  s.w.VCShell.setTheme('auto');
  assert.equal(s.w.localStorage.getItem('theme'), null);
  assert.equal(theme(s.w), 'dark', 'auto falls back to prefers-color-scheme');
  s.w.close();

  const u = setup({ url: CPS + '?theme=light&code=005935', dark: true, storage: { theme: 'dark' } });
  assert.equal(u.w.VCShell.getTheme(), 'light');
  u.w.VCShell.setTheme('dark');
  assert.equal(u.w.location.search, '?code=005935', 'setTheme drops the one-shot ?theme');
  assert.equal(theme(u.w), 'dark');
  u.w.close();
});

test('host reflects html data-theme changes made by the page itself', async () => {
  const s = setup({ htmlTheme: 'light' });
  assert.equal(s.el.getAttribute('data-theme'), 'light');
  s.root.setAttribute('data-theme', 'dark');
  await tick();
  assert.equal(s.el.getAttribute('data-theme'), 'dark');
  const brand = s.el.shadowRoot.querySelector('a.brand');
  assert.match(brand.getAttribute('href'), /theme=dark/);
  s.w.close();
});

test('linkTo attaches theme + from and only applies deep links whose accepts regex matches', () => {
  const s = setup({ htmlTheme: 'dark' });
  const { linkTo } = s.w.VCShell;
  assert.equal(linkTo('holding_value', { code: '005930' }),
    'https://ducklove.github.io/holding_value/?code=005930&theme=dark&from=common_preferred_spread');
  assert.equal(linkTo('holding_value', { code: 'SPY' }),
    'https://ducklove.github.io/holding_value/?theme=dark&from=common_preferred_spread');
  assert.equal(linkTo('eiayn', { code: 'spy' }),
    'https://ducklove.github.io/eiayn/?code=SPY&theme=dark&from=common_preferred_spread');
  assert.equal(linkTo('buybacks', { code: '005930' }),
    'https://ducklove.github.io/buybacks/?stock=005930&theme=dark&from=common_preferred_spread');
  assert.equal(linkTo('bond-mate', { view: 'fx' }),
    'https://ducklove.github.io/bond-mate/?tab=fx&theme=dark&from=common_preferred_spread');
  assert.equal(linkTo('bond-mate', { view: 'evil"><x' }),
    'https://ducklove.github.io/bond-mate/?theme=dark&from=common_preferred_spread');
  assert.equal(linkTo('all-about-gold', { view: 'gold-history' }),
    'https://ducklove.github.io/all-about-gold/?theme=dark&from=common_preferred_spread#gold-history');
  assert.equal(linkTo('gold_gap', { asset: 'bitcoin' }),
    'https://ducklove.github.io/gold_gap/?asset=bitcoin&theme=dark&from=common_preferred_spread');
  assert.equal(linkTo('hub:screener'), 'https://ducklove.duckdns.org:3691/screener?theme=dark&from=common_preferred_spread');
  assert.equal(linkTo('kis-proxy'), null, 'internal tools are not in the public registry');
  assert.equal(linkTo('nope'), null);
  s.w.close();
});

test('hubAnalysisUrl only accepts hub-compatible 6-character codes', () => {
  const s = setup();
  const { hubAnalysisUrl } = s.w.VCShell;
  assert.equal(hubAnalysisUrl('005930'),
    'https://ducklove.duckdns.org:3691/analysis?code=005930&theme=light&from=common_preferred_spread');
  assert.match(hubAnalysisUrl('0131d0'), /code=0131D0/);
  for (const bad of ['SPY', '1570.T', '<svg>', '', null, undefined, '0059301']) assert.equal(hubAnalysisUrl(bad), null, String(bad));
  s.w.close();
});

test('popover: grouped public tools, aria-current, keyboard navigation, Esc and outside click', async () => {
  const s = setup();
  const sr = s.el.shadowRoot;
  const button = sr.querySelector('button.current');
  const menu = sr.querySelector('[role="menu"]');
  assert.equal(button.textContent.includes('우선주 괴리율'), true);
  assert.equal(menu.hidden, true);
  const items = [...sr.querySelectorAll('[role="menuitem"]')];
  const ids = items.map(a => a.dataset.tool);
  assert.ok(ids.includes('hub:screener') && ids.includes('index-popup'));
  assert.ok(!ids.includes('kis-proxy') && !ids.includes('finance-pi'));
  assert.deepEqual(items.filter(a => a.hasAttribute('aria-current')).map(a => a.dataset.tool), ['common_preferred_spread']);
  assert.ok([...sr.querySelectorAll('.group')].some(g => g.getAttribute('aria-label') === '허브 도구'));
  assert.equal(sr.querySelectorAll('svg path').length >= items.length, true, 'every item has an inline SVG icon');

  const key = (target, k) => target.dispatchEvent(new s.w.KeyboardEvent('keydown', { key: k, bubbles: true, composed: true }));
  button.focus();
  key(button, 'ArrowDown');
  assert.equal(menu.hidden, false);
  assert.equal(button.getAttribute('aria-expanded'), 'true');
  assert.equal(sr.activeElement.dataset.tool, 'common_preferred_spread');
  const at = ids.indexOf('common_preferred_spread');
  key(sr.activeElement, 'ArrowDown');
  assert.equal(sr.activeElement.dataset.tool, ids[at + 1]);
  key(sr.activeElement, 'End');
  assert.equal(sr.activeElement.dataset.tool, ids[ids.length - 1]);
  key(sr.activeElement, 'ArrowDown');
  assert.equal(sr.activeElement.dataset.tool, ids[0], 'wraps around');
  key(sr.activeElement, 'Escape');
  assert.equal(menu.hidden, true);
  assert.equal(sr.activeElement, button, 'focus returns to the trigger');

  button.click();
  assert.equal(menu.hidden, false);
  s.w.document.getElementById('app').click();
  assert.equal(menu.hidden, true, 'outside click closes');
  s.w.close();
});

test('setStock drives the hub chip and stock-aware tool links', () => {
  const s = setup();
  const sr = () => s.el.shadowRoot;
  assert.equal(sr().querySelector('a[data-stock-link]'), null);
  s.w.VCShell.setStock('005935', '삼성전자우');
  const chip = sr().querySelector('a[data-stock-link]');
  assert.ok(chip.textContent.includes('허브에서 분석'));
  assert.ok(chip.textContent.includes('삼성전자우'));
  assert.equal(chip.getAttribute('href'),
    'https://ducklove.duckdns.org:3691/analysis?code=005935&theme=light&from=common_preferred_spread');
  assert.match(sr().querySelector('[data-tool="holding_value"]').getAttribute('href'), /\?code=005935&/);
  assert.doesNotMatch(sr().querySelector('a.brand').getAttribute('href'), /code=/);
  s.w.VCShell.setStock('SPY', 'SPDR S&P 500');
  assert.equal(sr().querySelector('a[data-stock-link]'), null, 'foreign tickers get no hub chip');
  assert.match(sr().querySelector('[data-tool="eiayn"]').getAttribute('href'), /code=SPY/);
  assert.doesNotMatch(sr().querySelector('[data-tool="holding_value"]').getAttribute('href'), /code=/);
  s.w.VCShell.setStock(null);
  assert.equal(s.el.hasAttribute('stock'), false);
  s.w.close();
});

test('theme toggle is opt-in and writes the shared theme key', () => {
  let s = setup();
  assert.equal(s.el.shadowRoot.querySelector('[data-theme-toggle]'), null);
  s.w.close();
  s = setup({ body: '<vc-shell tool="holding_value" theme-toggle></vc-shell>', htmlTheme: 'light' });
  const toggle = s.el.shadowRoot.querySelector('[data-theme-toggle]');
  assert.equal(toggle.getAttribute('aria-label'), '다크 모드로 전환');
  toggle.click();
  assert.equal(s.w.localStorage.getItem('theme'), 'dark');
  assert.equal(theme(s.w), 'dark');
  s.w.close();
});

test('double load is a no-op and the registry excludes internal infrastructure', () => {
  const s = setup();
  const first = s.w.VCShell;
  s.w.eval(SHELL);
  assert.equal(s.w.VCShell, first);
  assert.equal(first.version, '1.0.0');
  const text = JSON.stringify(first.tools);
  for (const needle of ['192.168.', ':3288', ':8400', 'finance-pi', 'kis-proxy', 'vendor']) assert.ok(!text.includes(needle), needle);
  assert.equal(SHELL.includes('fetch('), false, 'no network requests');
  s.w.close();
});

test('hub utils derive the portfolio handoff allowlist from APP_CONFIG.ecosystem (legacy fallback kept)', () => {
  const UTILS = readFileSync(new URL('../../static/js/utils.js', import.meta.url), 'utf8');
  const load = config => {
    const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'https://app.example.com/', runScripts: 'dangerously' });
    dom.window.APP_CONFIG = config;
    const script = dom.window.document.createElement('script');
    script.textContent = UTILS;
    dom.window.document.body.appendChild(script);
    return dom.window;
  };
  const integrations = {
    holdingValue: { baseUrl: 'https://ducklove.github.io/holding_value' },
    goldGap: { baseUrl: 'https://ducklove.github.io/gold_gap' },
  };
  let w = load({ integrations, ecosystem: { tools: [
    { id: 'gold_gap', integrationKey: 'goldGap', handoff: true },
    { id: 'holding_value', integrationKey: 'holdingValue', handoff: false },
  ] } });
  assert.deepEqual([...w.portfolioHandoffKeys()], ['goldGap']);
  assert.equal(w.portfolioIntegrationHref('https://ducklove.github.io/gold_gap/?theme=dark'), '/api/portfolio/open/goldGap?theme=dark');
  assert.equal(w.portfolioIntegrationHref('https://ducklove.github.io/holding_value/?code=005930'), 'https://ducklove.github.io/holding_value/?code=005930');
  w.close();
  w = load({ integrations });
  assert.deepEqual([...w.portfolioHandoffKeys()], ['holdingValue', 'preferredSpread', 'spacHunter', 'buybacks', 'eiayn']);
  assert.equal(w.portfolioIntegrationHref('https://ducklove.github.io/holding_value/?code=005930'), '/api/portfolio/open/holdingValue?code=005930');
  w.close();
});

test('links never leave http(s), even if a registry url were tampered with', () => {
  const tampered = SHELL.replace('"url":"https://ducklove.github.io/holding_value"', '"url":"javascript:alert(1)//"');
  assert.notEqual(tampered, SHELL);
  const dom = new JSDOM(`<!doctype html><html><body>${FALLBACK}</body></html>`, { url: CPS, runScripts: 'dangerously' });
  stubMatchMedia(dom.window, false);
  dom.window.eval(tampered);
  assert.equal(dom.window.VCShell.linkTo('holding_value', { code: '005930' }), null);
  const item = dom.window.document.querySelector('vc-shell').shadowRoot.querySelector('[data-tool="holding_value"]');
  assert.doesNotMatch(item.getAttribute('href'), /javascript:/i);
  dom.window.close();
});
