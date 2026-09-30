// jsdom behaviour tests for static/js/ecosystem-links.js — the hub side of the
// Value Compass ecosystem: registry helpers (APP_CONFIG.ecosystem), the labs
// '연결 대시보드' grid (UI-12) and the iframe message bridge (UI-14:
// vc:ready / vc:height / vc:theme + bond-mate's legacy height message).
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';
import { publicProjection } from '../../scripts/sync-ecosystem.mjs';

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
const REGISTRY = JSON.parse(read('config/ecosystem.json'));
const ECOSYSTEM = publicProjection(REGISTRY);
const UTILS = read('static/js/utils.js');
const SHELL = read('static/ecosystem/vc-shell.js');
const LINKS = read('static/js/ecosystem-links.js');

const LAB_HTML = `<section id="labEcosystem" hidden><div id="labEcoGrid"></div></section>
  <div id="bondsContent"></div><div id="npsContent"></div>`;

async function load({ url = 'https://app.example.com/labs', ecosystem = ECOSYSTEM, theme = 'light', shell = true, body = LAB_HTML } = {}) {
  const dom = new JSDOM(`<!doctype html><html data-theme="${theme}"><body>${body}</body></html>`, {
    url, runScripts: 'dangerously',
  });
  const w = dom.window;
  w.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  w.APP_CONFIG = ecosystem ? { ecosystem } : {};
  const sources = shell ? [SHELL, UTILS, LINKS] : [UTILS, LINKS];
  for (const src of sources) {
    const s = w.document.createElement('script');
    s.textContent = src;
    w.document.body.appendChild(s);
  }
  // The labs grid renders on DOMContentLoaded when the scripts run while the page is still parsing.
  if (w.document.readyState === 'loading') await new Promise(resolve => w.document.addEventListener('DOMContentLoaded', resolve));
  return w;
}

function frame(w, toolId, src) {
  const iframe = w.document.createElement('iframe');
  iframe.dataset.vcTool = toolId;
  iframe.setAttribute('src', src);
  w.document.body.appendChild(iframe);
  const posted = [];
  iframe.contentWindow.postMessage = (message, targetOrigin) => posted.push(JSON.parse(JSON.stringify({ message, targetOrigin })));
  return { iframe, posted };
}

function send(w, source, origin, data) {
  w.dispatchEvent(new w.MessageEvent('message', { data, origin, source }));
}

test('labs 연결 대시보드: every public sibling from the registry, via /go with theme, new tab', async () => {
  const w = await load();
  const section = w.document.getElementById('labEcosystem');
  const cards = [...w.document.querySelectorAll('#labEcoGrid a.lab-eco-card')];
  const expected = ECOSYSTEM.tools.filter(t => t.id !== 'value-invest' && t.deploy !== 'hub').map(t => t.id);
  assert.equal(section.hidden, false);
  assert.deepEqual(cards.map(a => a.dataset.vcTool), expected);
  assert.ok(expected.includes('holding_value') && expected.includes('index-popup') && expected.includes('bond-mate'));
  assert.ok(!cards.some(a => a.dataset.vcTool.startsWith('hub:')), 'hub-internal views are not sibling dashboards');
  const hv = cards.find(a => a.dataset.vcTool === 'holding_value');
  assert.equal(hv.getAttribute('href'), '/go/holding_value?theme=light');
  assert.equal(hv.getAttribute('target'), '_blank');
  assert.match(hv.getAttribute('rel'), /noopener/);
  assert.match(hv.textContent, /지주사 지분가치/);
  assert.match(hv.textContent, /종목 밸류에이션/, 'category label from the registry');
  assert.ok(hv.querySelector('.lab-eco-icon svg'), 'icon comes from the shared vc-shell sprite');
  assert.equal(hv.style.getPropertyValue('--lab-eco-accent').trim(), ECOSYSTEM.tools.find(t => t.id === 'holding_value').accent);

  // Theme changes re-stamp ?theme on every rendered link.
  w.document.documentElement.setAttribute('data-theme', 'dark');
  w.ecoRefreshLinks();
  assert.equal(hv.getAttribute('href'), '/go/holding_value?theme=dark');
  w.close();
});

test('labs 연결 대시보드 falls back to a monogram without vc-shell and hides without a registry', async () => {
  let w = await load({ shell: false });
  const card = w.document.querySelector('#labEcoGrid a[data-vc-tool="gold_gap"]');
  assert.equal(card.querySelector('.lab-eco-mono').textContent, '김');
  w.close();
  w = await load({ ecosystem: null });
  assert.equal(w.document.getElementById('labEcosystem').hidden, true);
  assert.equal(w.document.getElementById('labEcoGrid').innerHTML, '');
  assert.equal(w.ecoTool('bond-mate'), null);
  assert.equal(w.ecoStockPath('holding_value', '000670'), null, 'null = no registry, callers keep legacy rules');
  w.close();
});

test('registry deep links follow stockLink/viewLink templates and accepts regexes', async () => {
  const w = await load({ url: 'https://app.example.com/analysis?code=000670&from=holding_value' });
  assert.equal(w.ecoStockPath('holding_value', '000670'), '?code=000670');
  assert.equal(w.ecoStockPath('buybacks', '005930'), '?stock=005930');
  assert.equal(w.ecoStockPath('holding_value', 'bad code'), '', 'not accepted → tool home');
  assert.equal(w.ecoDeepPath('bond-mate', 'viewLink', 'fx'), '?tab=fx');
  assert.equal(w.ecoDeepPath('bond-mate', 'viewLink', 'nope'), '');
  assert.equal(
    w.ecoToolUrl('buybacks', { code: '005930' }),
    'https://ducklove.github.io/buybacks/?stock=005930&theme=light&from=value-invest',
  );
  assert.deepEqual(w.ecoStockTools('000670').map(t => t.id).sort(),
    ['buybacks', 'common_preferred_spread', 'eiayn', 'holding_value', 'nps-tracker', 'spac-hunter']);
  assert.equal(w.ecoStockPath('nps-tracker', '005930'), '?code=005930');
  // ?from is honoured only for a registry tool and only while the URL code matches.
  assert.equal(w.ecoArrivalTool('000670').id, 'holding_value');
  assert.equal(w.ecoArrivalTool('005930'), null);
  assert.equal(w.ecoJosaRo('지주사 지분가치'), '로');
  assert.equal(w.ecoJosaRo('스팩 헌터'), '로');
  assert.equal(w.ecoJosaRo('우선주 괴리율'), '로');
  assert.equal(w.ecoJosaRo('자사주 분석'), '으로');
  assert.equal(w.ecoJosaRo('ETF 평가 (EIAYN)'), '로');
  w.close();
  const hacked = await load({ url: 'https://app.example.com/analysis?code=000670&from=kis-proxy' });
  assert.equal(hacked.ecoArrivalTool('000670'), null, 'internal/unknown ids never produce a back link');
  hacked.close();
});

test('iframe bridge: vc:ready switches theme sync from src reload to postMessage', async () => {
  const w = await load({ url: 'https://app.example.com/bonds' });
  const { iframe, posted } = frame(w, 'bond-mate', 'https://ducklove.github.io/bond-mate/?embed=overview&theme=light');
  assert.equal(w.ecoPostFrameTheme(iframe), false, 'not ready yet → caller reloads src');
  assert.equal(posted.length, 0);

  send(w, iframe.contentWindow, 'https://ducklove.github.io', { source: 'vc', type: 'vc:ready' });
  assert.equal(iframe.dataset.vcReady, '1');
  assert.deepEqual(posted.at(-1), {
    message: { source: 'vc', type: 'vc:theme', theme: 'light' }, targetOrigin: 'https://ducklove.github.io',
  }, 'ready child immediately gets the current theme');

  w.document.documentElement.setAttribute('data-theme', 'dark');
  assert.equal(w.ecoPostFrameTheme(iframe), true);
  assert.deepEqual(posted.at(-1).message, { source: 'vc', type: 'vc:theme', theme: 'dark' });
  w.close();
});

test('iframe bridge: origin and source checks, vc:height and legacy bond-mate height', async () => {
  const w = await load({ url: 'https://app.example.com/bonds' });
  const bm = frame(w, 'bond-mate', 'https://ducklove.github.io/bond-mate/?embed=overview');
  const nps = frame(w, 'nps-tracker', 'https://ducklove.github.io/nps-tracker/?embed=true');
  const popup = frame(w, 'index-popup', 'https://ducklove.duckdns.org:3358/?index=ekospi&headless=1');

  // Wrong origin → ignored.
  send(w, bm.iframe.contentWindow, 'https://evil.example', { source: 'vc', type: 'vc:ready' });
  assert.equal(bm.iframe.dataset.vcReady, undefined);
  // Right origin but not one of our registered frames → ignored.
  send(w, w, 'https://ducklove.github.io', { source: 'vc', type: 'vc:height', height: 900 });
  assert.equal(bm.iframe.style.height, '');
  assert.equal(nps.iframe.style.height, '');
  // A github.io sibling cannot impersonate the self-hosted index-popup.
  send(w, popup.iframe.contentWindow, 'https://ducklove.github.io', { source: 'vc', type: 'vc:height', height: 400 });
  assert.equal(popup.iframe.style.height, '');

  send(w, nps.iframe.contentWindow, 'https://ducklove.github.io', { source: 'vc', type: 'vc:height', height: 1234.4 });
  assert.equal(nps.iframe.style.height, '1234px');
  assert.ok(nps.iframe.classList.contains('vc-auto-height'));
  send(w, popup.iframe.contentWindow, 'https://ducklove.duckdns.org:3358', { source: 'vc', type: 'vc:height', height: 50 });
  assert.equal(popup.iframe.style.height, '200px', 'clamped to a sane minimum');
  send(w, nps.iframe.contentWindow, 'https://ducklove.github.io', { source: 'vc', type: 'vc:height', height: 'NaN' });
  assert.equal(nps.iframe.style.height, '1234px');

  // bond-mate's legacy message keeps working, but only from the bond-mate frame.
  send(w, bm.iframe.contentWindow, 'https://ducklove.github.io', { source: 'bond-mate', type: 'height', height: 1500 });
  assert.equal(bm.iframe.style.height, '1500px');
  send(w, nps.iframe.contentWindow, 'https://ducklove.github.io', { source: 'bond-mate', type: 'height', height: 700 });
  assert.equal(nps.iframe.style.height, '1234px');
  w.close();
});

test('iframe bridge: vc:open-stock opens hub analysis for valid codes only', async () => {
  const w = await load({ url: 'https://app.example.com/nps' });
  const calls = [];
  w.switchView = view => calls.push(['view', view]);
  w.analyzeStock = code => calls.push(['analyze', code]);
  const nps = frame(w, 'nps-tracker', 'https://ducklove.github.io/nps-tracker/?embed=true');
  send(w, nps.iframe.contentWindow, 'https://ducklove.github.io', { source: 'vc', type: 'vc:open-stock', code: '<img>' });
  assert.deepEqual(calls, []);
  send(w, nps.iframe.contentWindow, 'https://ducklove.github.io', { source: 'vc', type: 'vc:open-stock', code: '005930' });
  assert.deepEqual(calls, [['view', 'analysis'], ['analyze', '005930']]);
  w.close();
});

test('without a registry the bridge trusts only the origin the frame actually loaded', async () => {
  const w = await load({ url: 'https://app.example.com/nps', ecosystem: null });
  const nps = frame(w, 'nps-tracker', 'https://mirror.example.org/nps-tracker/?embed=true');
  send(w, nps.iframe.contentWindow, 'https://ducklove.github.io', { source: 'vc', type: 'vc:height', height: 800 });
  assert.equal(nps.iframe.style.height, '');
  send(w, nps.iframe.contentWindow, 'https://mirror.example.org', { source: 'vc', type: 'vc:height', height: 800 });
  assert.equal(nps.iframe.style.height, '800px');
  w.close();
});
