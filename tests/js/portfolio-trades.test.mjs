import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const html = readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8').match(/<dialog id="pfTradeDialog"[\s\S]*?<\/dialog>/)[0];
const script = readFileSync(new URL('../../static/js/portfolio-trades.js', import.meta.url), 'utf8');
const result = { stock_code: '005930', stock_name: '삼성전자', side: 'buy', currency: 'KRW', quantity: 2, price: 100,
  fees: 10, gross_amount: 200, quantity_before: 10, quantity_after: 12, cash_before: 1000, cash_after: 790,
  avg_price_after: 100.83333333, avg_price_currency: 'KRW', revision: 'a'.repeat(64), created_at: '2026-09-09T01:00:00Z' };

function setup(handler) {
  const dom = new JSDOM(html, { url: 'http://localhost/', runScripts: 'outside-only' });
  const w = dom.window;
  w.PfStore = { items: [
    { stock_code: '005930', stock_name: '삼성전자', quantity: 10, avg_price: 100, currency: 'KRW' },
    { stock_code: 'CASH_KRW', stock_name: '원화', quantity: 1000, currency: 'KRW' },
  ], edit: { code: null } };
  w.currentUser = { google_sub: 'u1' };
  w.escapeHtml = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  w.loadPortfolio = async () => {};
  w.pfRefreshTodayState = async () => {};
  w.pfIsForeignSearchQuery = () => false;
  w.pfMatchedSpecialAssets = () => [];
  w.pfForeignSearchItem = x => x;
  const calls = [];
  w.apiFetchJson = async (path, options = {}) => {
    calls.push({ path, options });
    if (handler) return handler(path, options);
    return path.includes('?limit=') ? [] : result;
  };
  const dialog = w.document.getElementById('pfTradeDialog');
  dialog.showModal = () => { dialog.open = true; };
  dialog.close = () => { dialog.open = false; };
  w.eval(script);
  const el = id => w.document.getElementById(`pfTrade${id}`);
  const fill = (id, value) => { el(id).value = value; el(id).dispatchEvent(new w.Event('input', { bubbles: true })); };
  const prepared = async () => {
    w.pfOpenTrade('005930');
    fill('Quantity', '2'); fill('Price', '100'); fill('Fees', '10');
    await w.pfPreviewTrade();
  };
  return { dom, w, calls, el, fill, prepared };
}

test('매매는 미리보기에서 현금·수량을 확인하고 입력 변경 시 저장을 다시 막는다', async () => {
  const s = setup();
  try {
    await s.prepared();
    assert.match(s.el('Preview').textContent, /1,000 → 790/);
    assert.match(s.el('Preview').textContent, /10 → 12/);
    assert.equal(s.el('Save').disabled, false);
    assert.equal(s.calls.filter(c => c.path === '/api/portfolio/trades').length, 0);
    s.fill('Quantity', '3');
    assert.equal(s.el('Save').disabled, true);
    assert.equal(s.el('Preview').textContent, '');
  } finally { s.dom.window.close(); }
});

test('저장 중 반복 클릭은 한 번만 전송하고 현금·종목 PUT을 따로 호출하지 않는다', async () => {
  let release;
  const s = setup((path) => path.endsWith('/preview') ? result : path.includes('?limit=') ? [] : new Promise(resolve => { release = resolve; }));
  try {
    await s.prepared();
    const first = s.w.pfSaveTrade();
    await s.w.pfSaveTrade();
    assert.equal(s.el('Fields').disabled, true);
    assert.equal(s.calls.filter(c => c.path === '/api/portfolio/trades').length, 1);
    release(result);
    await first;
    assert.equal(s.el('Save').disabled, true);
    assert.match(s.el('Status').textContent, /저장했습니다/);
    assert.equal(s.w.sessionStorage.length, 0);
  } finally { s.dom.window.close(); }
});

test('응답 유실은 재시도·모달 재진입에서도 같은 요청 번호와 내용을 유지한다', async () => {
  let writes = 0;
  const s = setup(path => {
    if (path.includes('?limit=')) return [];
    if (path.endsWith('/preview')) return result;
    if (++writes === 1) throw new Error('연결 끊김');
    return { ...result, replayed: true };
  });
  try {
    await s.prepared();
    await s.w.pfSaveTrade();
    assert.equal(s.el('Fields').disabled, true);
    assert.equal(s.el('Save').textContent, '저장 결과 확인');
    s.el('Dialog').close();
    s.w.pfOpenTrade('005930');
    await s.w.pfSaveTrade();
    const writes = s.calls.filter(c => c.path === '/api/portfolio/trades');
    assert.equal(writes[0].options.body, writes[1].options.body);
    assert.equal(s.w.sessionStorage.length, 0);
  } finally { s.dom.window.close(); }
});

test('미리보기 뒤 잔고가 바뀌면 입력을 다시 열고 재확인을 요구한다', async () => {
  const s = setup(path => {
    if (path.includes('?limit=')) return [];
    if (path.endsWith('/preview')) return result;
    throw Object.assign(new Error('잔고가 변경됐습니다.'), { status: 409 });
  });
  try {
    await s.prepared();
    await s.w.pfSaveTrade();
    assert.equal(s.el('Fields').disabled, false);
    assert.equal(s.el('Save').disabled, true);
    assert.match(s.el('Status').textContent, /다시 확인/);
    assert.equal(s.w.sessionStorage.length, 0);
  } finally { s.dom.window.close(); }
});

test('외화 추가 매수는 기존 매입가 통화가 다를 때만 체결 환율을 받는다', async () => {
  const s = setup();
  try {
    s.w.PfStore.items.push({ stock_code: 'AAPL', stock_name: 'Apple', quantity: 2, avg_price: 100000, avg_price_currency: 'KRW', currency: 'USD' });
    s.w.pfOpenTrade('AAPL');
    assert.equal(s.el('FxLabel').hidden, false);
    assert.equal(s.el('Fx').required, true);
    assert.match(s.el('FxText').textContent, /1 USD = \? KRW/);
    s.el('Side').value = 'sell';
    s.el('Side').dispatchEvent(new s.w.Event('change'));
    assert.equal(s.el('FxLabel').hidden, true);
    assert.equal(s.el('Fx').required, false);
    s.el('Dialog').close();
    s.w.pfOpenTrade('005930');
    assert.equal(s.el('FxLabel').hidden, true);
    await s.w.pfLoadTrades();
  } finally { s.dom.window.close(); }
});

test('국내 매도는 0.2% 거래세가 수수료 합계에 자동 반영되고 금액 수정과 면세도 가능하다', async () => {
  const s = setup();
  try {
    s.w.pfOpenTrade('005930');
    s.el('Side').value = 'sell';
    s.el('Side').dispatchEvent(new s.w.Event('change'));
    s.fill('Quantity', '2'); s.fill('Price', '80000'); s.fill('Fees', '100');
    assert.equal(s.el('TaxRate').value, '0.2');
    assert.match(s.el('Costs').textContent, /세금 320 = 합계 420 KRW/);
    s.fill('TaxAmount', '300');
    assert.match(s.el('Costs').textContent, /합계 400 KRW/);
    s.fill('TaxAmount', ''); s.fill('TaxRate', '0');
    assert.match(s.el('Costs').textContent, /세금 0 = 합계 100 KRW/);
    await s.w.pfLoadTrades();
  } finally { s.dom.window.close(); }
});

test('종목 검색은 새 종목을 선택하되 보유 수량을 미리 등록하지 않는다', async () => {
  const s = setup(path => path.startsWith('/api/search') ? [{ stock_code: '000660', corp_name: 'SK하이닉스' }] : []);
  try {
    s.w.pfOpenTrade();
    s.fill('Stock', '000660');
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.equal(s.el('Stock').value, 'SK하이닉스 (000660)');
    assert.equal(s.calls.filter(c => c.options.method === 'POST' || c.options.method === 'PUT').length, 0);
  } finally { s.dom.window.close(); }
});

test('내역·미리보기에 들어온 종목명과 메모는 HTML로 실행하지 않는다', async () => {
  const malicious = { ...result, stock_name: '<img src=x onerror=alert(1)>', memo: '<script>alert(1)</script>' };
  const s = setup(path => path.includes('?limit=') ? [malicious] : malicious);
  try {
    await s.prepared();
    await s.w.pfLoadTrades();
    assert.equal(s.el('Preview').querySelector('img'), null);
    assert.equal(s.el('History').querySelector('script'), null);
    assert.match(s.el('History').textContent, /<script>/);
  } finally { s.dom.window.close(); }
});
