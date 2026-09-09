import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const source = readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8');
const html = ['pfDividendDialog', 'pfDistributionDialog'].map(id => source.match(new RegExp(`<dialog id="${id}"[\\s\\S]*?<\\/dialog>`))[0]).join('');
const receiptScript = readFileSync(new URL('../../static/js/portfolio-dividend-receipts.js', import.meta.url), 'utf8');
const distributionScript = readFileSync(new URL('../../static/js/portfolio-distributions.js', import.meta.url), 'utf8');
const result = { stock_code: '005930', stock_name: '삼성전자', currency: 'KRW', country: 'KR', received_date: '2026-01-15', applied_date: '2026-09-09',
  gross_amount: 1000, tax_rate: 15.4, tax_amount: 154, net_amount: 846, amount_krw: 846, cash_before: 10000, cash_after: 10846, revision: 'a'.repeat(64), source_key: null };
const distributionResult = { currency: 'KRW', amount: 846, cash_before: 10846, cash_after: 10000, available_before: 846, available_after: 0, units_change: 0, revision: 'a'.repeat(64) };
const events = [{ stock_code: '005930', stock_name: '삼성전자', date: '2026-01-15', type: 'estimated', amount_per_share: 100, currency: 'KRW', shares: 9, source_key: '005930:estimated:2026-01-15', received: false },
  { stock_code: '005930', stock_name: '삼성전자', date: '2026-03-31', type: 'ex_date', amount_per_share: null, currency: 'KRW', source_key: '005930:ex_date:2026-03-31', received: false }];

function setup(handler) {
  const dom = new JSDOM(html, { url: 'http://localhost/', runScripts: 'outside-only' });
  const w = dom.window;
  w.PfStore = { items: [{ stock_code: '005930', stock_name: '삼성전자', quantity: 10, currency: 'KRW' },
    { stock_code: 'AAPL', stock_name: 'Apple', quantity: 2, currency: 'USD' },
    { stock_code: '600519.SS', stock_name: '귀주모태', quantity: 3, currency: 'CNY' },
    { stock_code: 'CASH_KRW', quantity: 10846, currency: 'KRW' }], currency: { fxRate: 1400 }, edit: {} };
  w.currentUser = { google_sub: 'u1' };
  w.escapeHtml = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  w.loadPortfolio = async () => {};
  w.pfRefreshTodayState = async () => {};
  w.refreshPortfolioAfterCashflowMutation = async () => {};
  const calls = [];
  w.apiFetchJson = async (path, options = {}) => {
    calls.push({ path, options });
    const custom = handler?.(path, options);
    if (custom !== undefined) return custom;
    if (path.endsWith('/candidates')) return { events };
    if (path.endsWith('/balances')) return [{ currency: 'KRW', net_amount: 846, distributed_amount: 0, available_amount: 846, count: 1 }];
    if (path.includes('?limit=')) return [];
    return path.includes('/distributions') ? distributionResult : result;
  };
  for (const dialog of w.document.querySelectorAll('dialog')) {
    dialog.showModal = () => { dialog.open = true; };
    dialog.close = () => { dialog.open = false; };
  }
  w.eval(receiptScript);
  w.eval(distributionScript);
  const el = (id, prefix = 'Dividend') => w.document.getElementById(`pf${prefix}${id}`);
  const fill = (id, value, prefix = 'Dividend', type = 'input') => { el(id, prefix).value = value; el(id, prefix).dispatchEvent(new w.Event(type, { bubbles: true })); };
  const select = (id, value, prefix) => fill(id, value, prefix, 'change');
  const prepared = async () => {
    await w.pfOpenDividendReceipt(events[0].source_key);
    await w.pfPreviewDividendReceipt();
  };
  return { dom, w, calls, el, fill, select, prepared };
}

test('배당 스케줄은 주당 금액·현재 수량·세율을 자동 채우고 실제 저장 전에는 잔고를 쓰지 않는다', async () => {
  const s = setup();
  try {
    await s.w.pfOpenDividendReceipt(events[0].source_key);
    assert.equal(s.el('Code').value, '005930');
    assert.equal(s.el('Quantity').value, '10');
    assert.equal(s.el('PerShare').value, '100');
    assert.equal(s.el('TaxRate').value, '15.4');
    assert.match(s.el('Amounts').textContent, /세후 846 KRW/);
    assert.match(s.el('ScheduleNote').textContent, /예상/);
    assert.equal(s.calls.filter(c => c.options.method).length, 0);
    await s.w.pfPreviewDividendReceipt();
    assert.equal(s.el('Save').disabled, false);
    const payload = JSON.parse(s.calls.find(c => c.path.endsWith('/preview')).options.body);
    assert.equal(payload.source_key, events[0].source_key);
    s.fill('Quantity', '11');
    assert.equal(s.el('Save').disabled, true);
  } finally { s.dom.window.close(); }
});

test('배당기준일은 실제 수취일로 쓰지 않으며 미수집 주당 금액을 임의 생성하지 않는다', async () => {
  const s = setup();
  try {
    await s.w.pfOpenDividendReceipt(events[1].source_key);
    assert.equal(s.el('Date').value, '');
    assert.equal(s.el('PerShare').value, '');
    assert.match(s.el('ScheduleNote').textContent, /배당기준일/);
    s.select('Holding', 'AAPL');
    assert.equal(s.el('TaxRate').value, '15');
    assert.equal(s.el('Currency').value, 'USD');
    assert.equal(s.el('Fx').value, '1400');
    s.select('Holding', '600519.SS');
    assert.equal(s.el('TaxRate').value, '14.4');
    s.select('Country', 'OTHER');
    assert.equal(s.el('TaxRate').value, '0');
    s.select('Mode', 'total');
    s.fill('Gross', '100'); s.fill('TaxAmount', '10');
    assert.match(s.el('Amounts').textContent, /세후 90 CNY/);
    await s.w.pfLoadDividendReceipts();
  } finally { s.dom.window.close(); }
});

test('공시 지급일은 수취일에 채우고 배당락일은 권리일로 안내한다', async () => {
  const key = 'AAPL:ex_date:2026-01-02';
  const event = { stock_code: 'AAPL', stock_name: 'Apple', date: '2026-01-10', type: 'payment', date_kind: 'payment',
    confirmed: true, amount_per_share: 0.25, currency: 'USD', shares: 2, source_key: key, received: false };
  const s = setup(path => path.endsWith('/candidates') ? { events: [event] } : undefined);
  try {
    await s.w.pfOpenDividendReceipt(key);
    assert.equal(s.el('Date').value, '2026-01-10');
    assert.match(s.el('ScheduleNote').textContent, /공시 지급일/);
    event.type = 'ex_date'; event.date_kind = 'ex_date'; event.date = '2026-01-02';
    await s.w.pfOpenDividendReceipt(key);
    assert.equal(s.el('Date').value, '');
    assert.match(s.el('ScheduleNote').textContent, /배당락일/);
  } finally { await new Promise(resolve => setImmediate(resolve)); s.dom.window.close(); }
});

test('배당 수취 응답 유실은 다시 열어도 동일 요청으로 복구하고 수취 스케줄을 잠근다', async () => {
  let attempts = 0;
  const s = setup((path, options) => {
    if (path === '/api/portfolio/dividend-receipts' && options.method === 'POST') {
      if (++attempts === 1) throw new Error('연결 끊김');
      return { ...result, source_key: events[0].source_key, replayed: true };
    }
  });
  try {
    await s.prepared();
    await s.w.pfSaveDividendReceipt();
    assert.equal(s.el('Fields').disabled, true);
    s.el('Dialog').close();
    await s.w.pfOpenDividendReceipt();
    await s.w.pfSaveDividendReceipt();
    const writes = s.calls.filter(c => c.path === '/api/portfolio/dividend-receipts');
    assert.equal(writes[0].options.body, writes[1].options.body);
    assert.equal(s.w.sessionStorage.length, 0);
    assert.equal([...s.el('Schedule').options].find(o => o.value === events[0].source_key).disabled, true);
  } finally { s.dom.window.close(); }
});

test('저장 중 중복 클릭은 한 요청만 전송하고 저장 실패 409는 재확인을 요구한다', async () => {
  let reject;
  const s = setup((path, options) => path === '/api/portfolio/dividend-receipts' && options.method === 'POST' ? new Promise((_, r) => { reject = r; }) : undefined);
  try {
    await s.prepared();
    const first = s.w.pfSaveDividendReceipt();
    await s.w.pfSaveDividendReceipt();
    assert.equal(s.calls.filter(c => c.path === '/api/portfolio/dividend-receipts').length, 1);
    reject(Object.assign(new Error('이미 수취'), { status: 409 }));
    await first;
    assert.equal(s.el('Fields').disabled, false);
    assert.equal(s.el('Save').disabled, true);
    assert.match(s.el('Status').textContent, /다시 확인/);
  } finally { s.dom.window.close(); }
});

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

test('늦게 도착한 스케줄은 사용자가 입력 중인 종목과 금액을 덮어쓰지 않는다', async () => {
  let release;
  const s = setup(path => path.endsWith('/candidates') ? new Promise(resolve => { release = resolve; }) : undefined);
  try {
    const opening = s.w.pfOpenDividendReceipt(events[0].source_key);
    s.fill('Code', 'MANUAL');
    s.fill('Name', '직접 입력');
    release({ events });
    await opening;
    assert.equal(s.el('Code').value, 'MANUAL');
  } finally { s.dom.window.close(); }
});
