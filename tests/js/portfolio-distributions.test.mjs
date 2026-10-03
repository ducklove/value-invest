import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const source = readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8');
const html = source.match(/<dialog id="pfDistributionDialog"[\s\S]*?<\/dialog>/)[0];
const distributionScript = readFileSync(new URL('../../static/js/portfolio-distributions.js', import.meta.url), 'utf8');
const distributionResult = { currency: 'KRW', amount: 846, cash_before: 10846, cash_after: 10000,
  available_before: 846, available_after: 0, units_change: 0, revision: 'a'.repeat(64) };

function setup(handler) {
  const dom = new JSDOM(html, { url: 'http://localhost/', runScripts: 'outside-only' });
  const w = dom.window;
  w.PfStore = { items: [{ stock_code: 'CASH_KRW', quantity: 10846 }], currency: { fxRate: 1400 } };
  w.currentUser = { google_sub: 'u1' };
  w.escapeHtml = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  w.refreshPortfolioAfterCashflowMutation = async () => {};
  w.loadPortfolio = async () => {};
  w.pfRefreshTodayState = async () => {};
  const calls = [];
  w.apiFetchJson = async (path, options = {}) => {
    calls.push({ path, options });
    const custom = handler?.(path, options);
    if (custom !== undefined) return custom;
    if (path.endsWith('/balances')) return [{ currency: 'KRW', net_amount: 846, distributed_amount: 0, available_amount: 846, count: 1 }];
    if (path.includes('?limit=')) return [];
    return distributionResult;
  };
  const dialog = w.document.getElementById('pfDistributionDialog');
  dialog.showModal = () => { dialog.open = true; };
  dialog.close = () => { dialog.open = false; };
  w.eval(distributionScript);
  const el = id => w.document.getElementById(`pfDistribution${id}`);
  return { dom, w, calls, el };
}

test('분배금은 미분배 누계로 채우고 좌수 유지 미리보기 후 같은 요청으로 출금한다', async () => {
  let attempts = 0;
  const s = setup((path, options) => {
    if (path === '/api/portfolio/distributions' && options.method === 'POST') {
      if (++attempts === 1) throw new Error('연결 끊김');
      return { ...distributionResult, replayed: true };
    }
  });
  try {
    await s.w.pfOpenDistribution();
    assert.equal(s.el('Amount', 'Distribution').value, '846');
    await s.w.pfPreviewDistribution();
    assert.match(s.el('Preview', 'Distribution').textContent, /기존 좌수 유지/);
    await s.w.pfSaveDistribution();
    assert.equal(s.el('Fields', 'Distribution').disabled, true);
    await s.w.pfOpenDistribution();
    await s.w.pfSaveDistribution();
    const writes = s.calls.filter(c => c.path === '/api/portfolio/distributions');
    assert.equal(writes[0].options.body, writes[1].options.body);
    assert.equal(s.w.sessionStorage.length, 0);
    assert.match(s.el('Status', 'Distribution').textContent, /저장했습니다/);
  } finally { s.dom.window.close(); }
});

