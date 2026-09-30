// jsdom behaviour tests for the analysis view's ecosystem wiring:
//  - UI-4: a successful analyzeStock rewrites the URL to /analysis?code=CODE
//    (history.replaceState; ?theme dropped, ?from kept only for the same stock)
//  - UI-13: '← 도구로 돌아가기' chip for ?from=<registry id>, registry-driven
//    '연결 도구' chips, and valuation-card links through the registry stockLink.
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';
import { publicProjection } from '../../scripts/sync-ecosystem.mjs';

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
const ECOSYSTEM = publicProjection(JSON.parse(read('config/ecosystem.json')));
// /app-config.js serves both: integrations.<key>.baseUrl (handoff redirect targets) and the registry.
const INTEGRATIONS = Object.fromEntries(ECOSYSTEM.tools.filter(t => t.integrationKey)
  .map(t => [t.integrationKey, { baseUrl: t.url }]));
const SOURCES = ['utils.js', 'ecosystem-links.js', 'analysis-charts.js', 'analysis-filings.js', 'analysis-valuation.js', 'analysis.js']
  .map(name => read(`static/js/${name}`));

function load({ url = 'https://app.example.com/analysis', ecosystem = ECOSYSTEM, theme = 'light' } = {}) {
  const dom = new JSDOM(`<!doctype html><html data-theme="${theme}"><body>
    <div id="analysisEcoLinks" hidden></div>
    <div id="coverageNote"></div>
    <div id="loadingOverlay"><div id="loadingText"></div><div id="loadingDetail"></div>
      <div id="progressBar"></div><div id="progressSteps"></div><button id="cancelBtn"></button></div>
  </body></html>`, { url, runScripts: 'dangerously' });
  const w = dom.window;
  w.APP_CONFIG = ecosystem ? { ecosystem, integrations: INTEGRATIONS } : { integrations: INTEGRATIONS };
  for (const src of SOURCES) {
    const s = w.document.createElement('script');
    s.textContent = src;
    w.document.body.appendChild(s);
  }
  return w;
}

// analyzeStock with a cached (JSON) response; renderResult is a spy that only sets the
// globals the real one sets first (activeStockCode, _lastAnalysisData).
function stubAnalyze(w, data) {
  w.requireApiConfiguration = () => {};
  w.trackEvent = () => {};
  w.loadRecentList = () => {};
  w.saveGuestRecent = () => {};
  w.currentUser = null;
  w.renderResult = (d) => {
    w.__data = d;
    w.eval('activeStockCode = window.__data.stock_code; _lastAnalysisData = window.__data;');
  };
  w.apiFetch = async () => ({ ok: true, headers: { get: () => 'application/json' }, json: async () => data });
}

const here = w => w.location.pathname + w.location.search;
const chips = w => [...w.document.querySelectorAll('#analysisEcoLinks a.eco-chip')];

test('analyzeStock success rewrites the URL to /analysis?code=, drops ?theme, keeps ?from for the same stock', async () => {
  const w = load({ url: 'https://app.example.com/?code=000670&from=holding_value&theme=dark&utm=x' });
  stubAnalyze(w, { stock_code: '000670', corp_name: '영풍' });
  const before = w.history.length;
  await w.analyzeStock('000670');
  assert.equal(here(w), '/analysis?code=000670&from=holding_value&utm=x');
  assert.equal(w.history.length, before, 'replaceState, not a new history entry');
  assert.equal(w.history.state.pfView, 'analysis');

  // Another stock: ?from no longer describes where this page came from.
  stubAnalyze(w, { stock_code: '005930', corp_name: '삼성전자' });
  await w.analyzeStock('005930');
  assert.equal(here(w), '/analysis?code=005930&utm=x');
  w.close();
});

test('a failed analysis leaves the URL alone, and a stale result never rewrites another view', async () => {
  const w = load({ url: 'https://app.example.com/analysis?code=000670' });
  w.requireApiConfiguration = () => {};
  w.trackEvent = () => {};
  w.showToast = () => {};
  w.apiFetch = async () => ({ ok: false, headers: { get: () => 'application/json' }, json: async () => ({ detail: 'x' }) });
  await w.analyzeStock('999999');
  assert.equal(here(w), '/analysis?code=000670');

  w.history.pushState({}, '', '/portfolio');
  w.PfStore = { activeView: 'portfolio' };
  w.syncAnalysisUrl('005930');
  assert.equal(here(w), '/portfolio');
  w.close();
});

test('analysisUrlFor keeps unrelated params and the hash, strips one-shot and other-view params', () => {
  const w = load();
  assert.equal(w.analysisUrlFor('0131d0', '?code=0131D0&from=eiayn&theme=light&focus=A&view=fx&x=1', '#h'),
    '/analysis?code=0131D0&from=eiayn&x=1#h');
  assert.equal(w.analysisUrlFor('005930', '?from=eiayn', ''), '/analysis?code=005930');
  w.close();
});

test('?from=<registry tool> shows a back chip to that tool for the same code (theme-aware)', async () => {
  const w = load({ url: 'https://app.example.com/?code=005930&from=buybacks' });
  stubAnalyze(w, { stock_code: '005930', corp_name: '삼성전자' });
  await w.analyzeStock('005930');
  const box = w.document.getElementById('analysisEcoLinks');
  assert.equal(box.hidden, false);
  const back = w.document.querySelector('#analysisEcoLinks a.eco-chip-back');
  assert.equal(back.textContent, '← 자사주 분석으로 돌아가기');
  // buybacks is a handoff tool → goes through the hub's held-snapshot redirect with its stock param.
  assert.equal(back.getAttribute('href'), '/api/portfolio/open/buybacks?theme=light&stock=005930');
  assert.equal(back.hasAttribute('target'), false, '돌아가기 stays in this tab');
  assert.ok(!chips(w).filter(a => !a.classList.contains('eco-chip-back')).some(a => a.dataset.vcTool === 'buybacks'),
    'the back tool is not repeated as a generic chip');

  w.document.documentElement.setAttribute('data-theme', 'dark');
  w.ecoRefreshLinks();
  assert.equal(back.getAttribute('href'), '/api/portfolio/open/buybacks?theme=dark&stock=005930');
  w.close();

  const bad = load({ url: 'https://app.example.com/?code=005930&from=kis-proxy' });
  stubAnalyze(bad, { stock_code: '005930', corp_name: '삼성전자' });
  await bad.analyzeStock('005930');
  assert.equal(bad.document.querySelector('#analysisEcoLinks a.eco-chip-back'), null);
  bad.close();
});

test('연결 도구 chips: registry stockLink tools, relevance-gated by existing signals', async () => {
  const w = load();
  stubAnalyze(w, { stock_code: '005930', corp_name: '삼성전자' });
  await w.analyzeStock('005930');
  // Plain stock: holding/preferred/ETF tools need a signal (their valuation card) → only generic tools.
  // nps-tracker accepts ?code= too, but the hub has no per-stock NPS holding signal → no chip.
  assert.ok(ECOSYSTEM.tools.find(t => t.id === 'nps-tracker').stockLink, 'registry advertises the nps-tracker stockLink');
  assert.deepEqual(chips(w).map(a => a.dataset.vcTool), ['buybacks']);
  const bb = chips(w)[0];
  assert.equal(bb.getAttribute('target'), '_blank');
  assert.equal(bb.getAttribute('href'), '/api/portfolio/open/buybacks?theme=light&stock=005930');
  assert.equal(w.document.querySelector('#analysisEcoLinks .eco-chip-label').textContent, '연결 도구');

  // SPAC names light up spac-hunter.
  stubAnalyze(w, { stock_code: '123450', corp_name: '케이비제30호스팩' });
  await w.analyzeStock('123450');
  assert.deepEqual(chips(w).map(a => a.dataset.vcTool), ['spac-hunter', 'buybacks']);
  w.close();

  // Without a registry the row stays hidden (no second mechanism, no broken links).
  const bare = load({ ecosystem: null });
  stubAnalyze(bare, { stock_code: '005930', corp_name: '삼성전자' });
  await bare.analyzeStock('005930');
  assert.equal(bare.document.getElementById('analysisEcoLinks').hidden, true);
  bare.close();
});

test('holding/preferred valuation cards deep-link through the registry stockLink with the current theme', () => {
  const w = load({ theme: 'dark' });
  w.eval("activeStockCode = '000670';");
  const box = w.document.getElementById('coverageNote');
  box.innerHTML = w._externalValuationCards({
    holding: { name: '영풍', ratio: 781.87, url: 'https://ducklove.github.io/holding_value/?code=000670' },
    preferred: { name: '영풍', preferredName: '영풍우', spread: 12.3, url: 'https://ducklove.github.io/common_preferred_spread' },
  }).join('');
  const [pref, hold] = [...box.querySelectorAll('a.valuation-card')];
  assert.equal(pref.getAttribute('href'), '/api/portfolio/open/preferredSpread?code=000670&theme=dark');
  assert.equal(hold.getAttribute('href'), '/api/portfolio/open/holdingValue?code=000670&theme=dark');
  assert.equal(hold.dataset.vcTool, 'holding_value');
  w.document.documentElement.setAttribute('data-theme', 'light');
  w.ecoRefreshLinks();
  assert.equal(hold.getAttribute('href'), '/api/portfolio/open/holdingValue?code=000670&theme=light');
  w.close();
});
