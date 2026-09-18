import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const read = path => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');
const html = read('static/index.html');
const tick = () => new Promise(resolve => setTimeout(resolve, 0));

async function setup(handler) {
  const dom = new JSDOM(html, { url: 'http://localhost/', runScripts: 'outside-only' });
  const w = dom.window;
  await new Promise(resolve => w.document.addEventListener('DOMContentLoaded', resolve, { once: true }));
  w.PfStore = { items: [{ stock_code: 'CASH_KRW', stock_name: '원화', quantity: 1000000, currency: 'KRW' }], edit: {} };
  w.currentUser = { google_sub: 'registration-test' };
  w.escapeHtml = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  w.eval(read('static/js/portfolio-render.js'));
  w.renderPortfolio = () => {};
  w.loadPortfolio = async () => {};
  w.showToast = () => {};
  w.reportApiError = () => {};
  w.pfRefreshTodayState = async () => {};
  w.getIntegrationConfig = () => ({});
  const calls = [];
  w.apiFetchJson = async (path, options = {}) => {
    calls.push({ path, options });
    if (handler) return handler(path, options);
    if (options.method === 'PUT') return { ...JSON.parse(options.body), stock_code: path.split('/').pop() };
    return path.includes('resolve-name') ? { stock_code: '002380', stock_name: 'KCC' } : [];
  };
  w.document.querySelectorAll('dialog').forEach(dialog => {
    dialog.showModal = () => { dialog.open = true; };
    dialog.close = () => { dialog.open = false; dialog.dispatchEvent(new w.Event('close')); };
  });
  w.eval(read('static/js/portfolio-add-search.js'));
  w.eval(read('static/js/portfolio-actions.js'));
  w.eval(read('static/js/portfolio-trades.js'));
  w.loadFeatureScripts = async () => {};
  w.document.dispatchEvent(new w.Event('DOMContentLoaded'));
  const el = id => w.document.getElementById(id);
  return { dom, w, calls, el, mutations: () => calls.filter(c => ['PUT', 'POST', 'DELETE'].includes(c.options.method)) };
}

test('초기 등록 선택 후 X·취소·Escape는 보유 행이나 저장 요청을 만들지 않는다', async () => {
  const s = await setup();
  try {
    for (const close of ['pfRegisterClose', 'pfRegisterCancel', 'Escape']) {
      await s.w.pfAddFromSearch('AAPL', 'Apple', 'USD');
      assert.equal(s.el('pfRegisterDialog').open, true);
      s.el('pfRegisterQuantity').value = '5';
      if (close === 'Escape') {
        const event = new s.w.Event('cancel', { cancelable: true });
        if (s.el('pfRegisterDialog').dispatchEvent(event)) s.el('pfRegisterDialog').close();
      } else s.el(close).click();
      assert.equal(s.el('pfRegisterDialog').open, false);
      assert.equal(s.w.PfStore.items.length, 1);
      assert.equal(s.mutations().length, 0);
    }
  } finally { s.dom.window.close(); }
});

test('홍콩 위안화 종목을 등록할 때 거래 통화와 초기 매입가 통화를 CNY로 정한다', async () => {
  const s = await setup();
  try {
    assert.equal(s.w.pfInferTickerCurrency('08388.HK'), 'HKD');
    assert.equal(s.w.pfInferTickerCurrency('83188.HK'), 'CNY');
    await s.w.pfAddFromSearch('83199.HK', '위안화 채권 ETF', s.w.pfInferTickerCurrency('83199.HK'));
    s.el('pfRegisterQuantity').value = '3';
    s.el('pfRegisterPrice').value = '100';
    await s.w.pfSaveInitialRegistration();
    const body = JSON.parse(s.mutations()[0].options.body);
    assert.equal(body.currency, 'CNY');
    assert.equal(body.avg_price_currency, 'CNY');
  } finally { s.dom.window.close(); }
});

test('초기 등록 확정만 입력한 수량·단가로 저장하고 현금을 유지한다', async () => {
  const s = await setup();
  try {
    await s.w.pfAddFromSearch('KCC', 'KCC');
    s.el('pfRegisterQuantity').value = '3';
    s.el('pfRegisterPrice').value = '200000';
    await s.w.pfSaveInitialRegistration();
    const [saved] = s.mutations();
    assert.equal(s.mutations().length, 1);
    assert.equal(saved.path, '/api/portfolio/002380');
    assert.deepEqual(JSON.parse(saved.options.body), { stock_name: 'KCC', quantity: 3, avg_price: 200000, currency: 'KRW', avg_price_currency: 'KRW' });
    assert.equal(s.w.PfStore.items.find(i => i.stock_code === 'CASH_KRW').quantity, 1000000);
    assert.equal(s.w.PfStore.items.find(i => i.stock_code === '002380').quantity, 3);
    assert.equal(s.el('pfRegisterDialog').open, false);
  } finally { s.dom.window.close(); }
});

test('저장 전 수량 0을 거절하고 저장 중 중복 제출과 닫기를 막는다', async () => {
  let resolve;
  const s = await setup((path, options) => options.method === 'PUT' ? new Promise(r => { resolve = r; }) : []);
  try {
    await s.w.pfAddFromSearch('AAPL', 'Apple', 'USD');
    s.el('pfRegisterQuantity').value = '0';
    await s.w.pfSaveInitialRegistration();
    assert.equal(s.mutations().length, 0);
    s.el('pfRegisterQuantity').value = '2.5';
    s.el('pfRegisterPrice').value = '100';
    const pending = s.w.pfSaveInitialRegistration();
    await s.w.pfSaveInitialRegistration();
    s.el('pfRegisterClose').click();
    assert.equal(s.el('pfRegisterDialog').dispatchEvent(new s.w.Event('cancel', { cancelable: true })), false);
    assert.equal(s.el('pfRegisterDialog').open, true);
    assert.equal(s.mutations().length, 1);
    assert.equal(JSON.parse(s.mutations()[0].options.body).avg_price_currency, 'USD');
    resolve({ stock_code: 'AAPL', quantity: 2.5, avg_price: 100, currency: 'USD' });
    await pending;
  } finally { s.dom.window.close(); }
});

test('매수 선택은 미보유 종목을 매매 창에 전달하며 먼저 등록하지 않는다', async () => {
  const s = await setup();
  try {
    s.w.document.querySelector('input[name="pfAddMode"][value="buy"]').checked = true;
    await s.w.pfAddFromSearch('AAPL', 'Apple', 'USD');
    assert.equal(s.el('pfTradeDialog').open, true);
    assert.equal(s.el('pfTradeStock').value, 'Apple (AAPL)');
    assert.equal(s.el('pfTradeCurrency').value, 'USD');
    assert.equal(s.el('pfTradeSide').value, 'buy');
    s.el('pfTradeClose').click();
    await tick();
    assert.equal(s.mutations().length, 0);
    assert.equal(s.w.PfStore.items.length, 1);
  } finally { s.dom.window.close(); }
});

test('종목 확인 응답 전에 입력 패널을 닫으면 뒤늦게 등록 창을 열지 않는다', async () => {
  let resolve;
  const s = await setup(() => new Promise(r => { resolve = r; }));
  try {
    const pending = s.w.pfAddFromSearch('KCC', 'KCC');
    s.w.pfSetAddPanelOpen(false);
    resolve({ stock_code: '002380', stock_name: 'KCC' });
    await pending;
    assert.equal(s.el('pfRegisterDialog').open, false);
    assert.equal(s.mutations().length, 0);
  } finally { s.dom.window.close(); }
});

test('직접 입력한 국내 별칭으로 매수해도 국내 코드와 원화로 연결한다', async () => {
  const s = await setup();
  try {
    s.w.document.querySelector('input[name="pfAddMode"][value="buy"]').checked = true;
    await s.w.pfAddFromSearch('KCC', 'KCC', 'USD');
    assert.equal(s.el('pfTradeStock').value, 'KCC (002380)');
    assert.equal(s.el('pfTradeCurrency').value, 'KRW');
    assert.equal(s.mutations().length, 0);
    await tick();
  } finally { s.dom.window.close(); }
});

test('종목 확인 중 입력 목적을 바꾸면 이전 목적의 창을 열지 않는다', async () => {
  let resolve;
  const s = await setup(() => new Promise(r => { resolve = r; }));
  try {
    const pending = s.w.pfAddFromSearch('KCC', 'KCC');
    const radio = s.w.document.querySelector('input[name="pfAddMode"][value="buy"]');
    radio.checked = true;
    radio.dispatchEvent(new s.w.Event('change'));
    resolve({ stock_code: '002380', stock_name: 'KCC' });
    await pending;
    assert.equal(s.el('pfRegisterDialog').open, false);
    assert.equal(s.el('pfTradeDialog').open, false);
    assert.equal(s.mutations().length, 0);
  } finally { s.dom.window.close(); }
});

test('보유분 정리에서 매도는 전량을 채우고 등록 삭제는 현금을 유지한다', async () => {
  const s = await setup();
  try {
    s.w.PfStore.items.push({ stock_code: '005930', stock_name: '삼성전자', quantity: 7, currency: 'KRW' });
    s.w.deletePortfolioItem('005930');
    assert.equal(s.mutations().length, 0);
    s.el('pfRemoveClose').click();
    assert.equal(s.w.PfStore.items.length, 2);
    s.w.deletePortfolioItem('005930');
    s.el('pfRemoveSell').click();
    await tick();
    assert.equal(s.el('pfTradeSide').value, 'sell');
    assert.equal(s.el('pfTradeQuantity').value, '7');
    assert.equal(s.el('pfTradeTaxRate').value, '0.2');
    assert.equal(s.el('pfTradePrice').value, '');
    s.el('pfTradeClose').click();
    assert.equal(s.mutations().length, 0);
    s.w.deletePortfolioItem('005930');
    await s.w.pfDeleteHoldingRegistration();
    assert.equal(s.mutations().length, 1);
    assert.equal(s.mutations()[0].options.method, 'DELETE');
    assert.equal(s.w.PfStore.items.length, 1);
    assert.equal(s.w.PfStore.items[0].quantity, 1000000);
    s.w.deletePortfolioItem('CASH_KRW');
    assert.equal(s.el('pfRemoveSell').hidden, true);
  } finally { s.dom.window.close(); }
});
