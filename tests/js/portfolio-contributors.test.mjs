import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const source = name => readFileSync(new URL(`../../static/js/${name}.js`, import.meta.url), 'utf8');
function setup(t) {
  const dom = new JSDOM('<div id="pfSummary"></div><table id="pfTable"><tbody id="pfBody"></tbody><tfoot id="pfFoot"></tfoot></table><div id="pfEmpty"></div>', {
    runScripts: 'dangerously', url: 'https://app.example.com', pretendToBeVisual: true,
  });
  const w = dom.window;
  t.after(() => w.close());
  w.escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  w.fmtPct = n => n == null ? '-' : `${n > 0 ? '+' : ''}${n.toFixed(2)}%`;
  w.fmtKrw = n => Math.round(n).toLocaleString('ko-KR');
  w.fmtSignedKrw = n => `${n > 0 ? '+' : ''}${w.fmtKrw(n)}`;
  w.quotePriceOrNull = q => q?.price ?? null;
  w._pfRenderColToggles = () => {};
  w._renderSummarySparklines = () => {};
  for (const name of ['portfolio-store', 'portfolio-data', 'portfolio-render', 'portfolio-contributors', 'portfolio-events']) {
    const script = w.document.createElement('script');
    script.textContent = source(name);
    w.document.body.appendChild(script);
  }
  w.document.dispatchEvent(new w.Event('DOMContentLoaded'));
  // jsdom has no layout; the browser tests check actual popover bounds.
  w.HTMLElement.prototype.getClientRects = () => [{ left: 20, top: 30, bottom: 100 }];
  return w;
}
const row = (code, value, quantity = 10) => ({ stock_code: code, stock_name: code, qty: quantity, marketValue: value });
function baseline(rows) {
  return {
    date: '2026-09-30', total_value: rows.reduce((n, r) => n + r.marketValue, 0),
    stock_values: Object.fromEntries(rows.map(r => [r.stock_code, r.marketValue])),
    stock_positions: Object.fromEntries(rows.map(r => [r.stock_code, { quantity: r.qty, group_name: '국내' }])),
    stock_trade_flows: {},
  };
}
function seed(w) {
  w.PfStore.items = [
    { stock_code: '005930', stock_name: '삼성전자', quantity: 10, avg_price: 900, quote: { price: 1200 } },
    { stock_code: '000660', stock_name: 'SK하이닉스', quantity: 10, avg_price: 900, quote: { price: 800 } },
  ];
  const snap = { ...baseline([row('005930', 10000), row('000660', 10000)]), nav: 1000, total_units: 20 };
  w.PfStore.snapshots.prevDay = { ...snap, today_net_cashflow: 0 };
  w.PfStore.snapshots.monthEnd = { ...snap };
  w.PfStore.snapshots.yearStart = { ...snap, date: '2025-12-31' };
  w.PfStore.navHistory = [snap];
  w.renderPortfolio({ summaryOnly: true });
}
const button = (w, period) => w.document.querySelector(`[data-period="${period}"]`);
const panel = w => w.document.getElementById('pfContributorPopover');

test('상승·하락 각각 금액순 3개이며 높은 등락률의 작은 종목이 순위를 왜곡하지 않는다', t => {
  const w = setup(t);
  const before = [row('BIG', 100000), row('TINY', 100), ...['P1', 'P2', 'P3', 'N1', 'N2', 'N3', 'N4', 'FLAT'].map(code => row(code, 1000))];
  const changes = [1000, 100, 300, 400, 200, -100, -400, -300, -200, 0];
  const now = before.map((r, i) => ({ ...r, marketValue: r.marketValue + changes[i] }));
  now.push(row('CASH_KRW', 999999));
  const result = w.pfBuildSummaryContributors(now, baseline(before), null, 'today');
  assert.deepEqual(Array.from(result.positive, r => r.code), ['BIG', 'P2', 'P1']);
  assert.deepEqual(Array.from(result.negative, r => r.code), ['N2', 'N3', 'N4']);
  assert.equal(result.positive[0].pct, 1);
});

test('추가 매수는 매수대금과 비용을 차감하고 전량 매도의 실현손익도 포함한다', t => {
  const w = setup(t);
  const snap = baseline([row('BUY', 1000), row('SOLD', 1000)]);
  snap.stock_trade_flows = {
    BUY: { currency: 'KRW', quantity_change: 10, cash_change: -1100, buy_amount: 1100 },
    SOLD: { stock_name: '매도한 종목', currency: 'KRW', quantity_change: -10, cash_change: 1200, buy_amount: 0 },
    NEW: { currency: 'KRW', quantity_change: 10, cash_change: -1000, buy_amount: 1000 },
  };
  const result = w.pfBuildSummaryContributors([row('BUY', 2200, 20), row('NEW', 900)], snap, null, 'mtd');
  assert.deepEqual(Array.from(result.positive, r => [r.code, r.amount]), [['SOLD', 200], ['BUY', 100]]);
  assert.equal(result.positive[0].pct, 20);
  assert.equal(result.positive[1].pct, 100 / 2100 * 100);
  assert.equal(result.negative[0].amount, -100);
  assert.equal(result.negative[0].pct, -10);
});

test('체결 기록으로 설명되지 않는 수량 변경·가격 누락·구형 스냅샷은 순위에서 제외한다', t => {
  const w = setup(t);
  const snap = baseline([row('EDIT', 1000), row('MISSING', 1000), row('LEGACY', 1000), row('OK', 1000)]);
  snap.stock_positions.LEGACY.quantity = null;
  const result = w.pfBuildSummaryContributors([row('EDIT', 2000, 20), row('MISSING', null), row('LEGACY', 1500), row('OK', 1100)], snap, null, 'today');
  assert.equal(result.excluded, 3);
  assert.deepEqual(Array.from(result.positive, r => r.code), ['OK']);
  assert.ok(w.pfBuildSummaryContributors([], { date: snap.date, stock_values: {} }, null, 'today').message);
});

test('원화·달러 기준에서 기준일 환율과 현재 환율을 각각 적용한다', t => {
  const w = setup(t);
  const snap = { ...baseline([row('AAPL', 130000)]), fx_usdkrw: 1300 };
  const current = [row('AAPL', 154000)];
  assert.equal(w.pfBuildSummaryContributors(current, snap, null, 'ytd').positive[0].amount, 24000);
  w.PfStore.currency = { unit: 'USD', fxRate: 1400 };
  const result = w.pfBuildSummaryContributors(current, snap, null, 'ytd');
  assert.equal(result.positive[0].amount, 10);
  assert.equal(result.positive[0].pct, 10);
  snap.stock_trade_flows.AAPL = { currency: 'USD', quantity_change: 10, cash_change: -100, buy_amount: 100, fx_rate: 999 };
  const bought = w.pfBuildSummaryContributors([row('AAPL', 308000, 20)], snap, null, 'ytd');
  assert.equal(bought.positive[0].amount, 20);
  assert.equal(bought.positive[0].pct, 10);
});

test('그룹·검색 필터가 숨긴 현재 보유분은 매도 종목으로 취급하지 않는다', t => {
  const w = setup(t);
  const snap = baseline([row('SHOW', 1000), row('HIDDEN', 1000), row('SOLD', 1000)]);
  snap.stock_trade_flows.SOLD = { stock_name: '매도', currency: 'KRW', quantity_change: -10, cash_change: 1300, buy_amount: 0 };
  const all = [row('SHOW', 1200), row('HIDDEN', 1100)];
  w.PfStore.filters.group = new w.Set(['국내']);
  let result = w.pfBuildSummaryContributors([all[0]], snap, null, 'today', all);
  assert.deepEqual(Array.from(result.positive, r => r.code), ['SOLD', 'SHOW']);
  assert.equal(result.negative.length, 0);
  w.PfStore.filters.searchText = 'SHOW';
  result = w.pfBuildSummaryContributors([all[0]], snap, null, 'today', all);
  assert.deepEqual(Array.from(result.positive, r => r.code), ['SHOW']);
});

test('기간별 정산 기준 변경·계좌 보기에서 비교 불가 상태를 표시한다', t => {
  const w = setup(t);
  const snap = baseline([row('A', 1000)]);
  const current = [row('A', 1200)];
  const latest = { price_basis: 'regular_close_v1' };
  assert.ok(w.pfBuildSummaryContributors(current, snap, latest, 'mtd').message);
  assert.equal(w.pfBuildSummaryContributors(current, snap, latest, 'today').positive.length, 1);
  snap.linked = true;
  assert.equal(w.pfBuildSummaryContributors(current, snap, latest, 'mtd').positive.length, 1);
  w.PfStore.accountId = 'account-1';
  assert.ok(w.pfBuildSummaryContributors(current, snap, latest, 'today').message);
});

test('클릭 고정·기간 전환·Escape·바깥 클릭과 실시간 갱신 중 포커스 유지', t => {
  const w = setup(t);
  seed(w);
  button(w, 'today').click();
  assert.equal(panel(w).hidden, false);
  assert.match(panel(w).textContent, /삼성전자/);
  assert.match(panel(w).textContent, /\+2,000원/);
  assert.match(panel(w).textContent, /\+20.00%/);
  button(w, 'mtd').focus();
  button(w, 'mtd').click();
  assert.match(panel(w).textContent, /MTD 성과/);
  const originalButton = button(w, 'mtd');
  const originalValue = originalButton.querySelector('.pf-summary-value');
  w.PfStore.items[0].quote.price = 1400;
  w.renderPortfolio({ summaryOnly: true });
  assert.equal(button(w, 'mtd'), originalButton);
  assert.equal(button(w, 'mtd').querySelector('.pf-summary-value'), originalValue);
  assert.equal(w.document.activeElement, button(w, 'mtd'));
  assert.equal(button(w, 'mtd').getAttribute('aria-expanded'), 'true');
  assert.match(panel(w).textContent, /\+4,000원/);
  button(w, 'mtd').click();
  assert.equal(panel(w).hidden, true);
  button(w, 'ytd').click();
  w.document.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
  assert.equal(panel(w).hidden, true);
  button(w, 'ytd').click();
  w.document.body.click();
  assert.equal(panel(w).hidden, true);
});

test('hover와 키보드 포커스로 미리보기하며 터치 hover는 무시한다', async t => {
  const w = setup(t);
  seed(w);
  const enter = new w.Event('pointerover', { bubbles: true });
  Object.defineProperty(enter, 'pointerType', { value: 'touch' });
  button(w, 'today').dispatchEvent(enter);
  assert.equal(panel(w), null);
  const mouse = new w.Event('pointerover', { bubbles: true });
  Object.defineProperty(mouse, 'pointerType', { value: 'mouse' });
  button(w, 'today').dispatchEvent(mouse);
  assert.equal(panel(w).hidden, false);
  const leave = new w.Event('pointerout', { bubbles: true });
  Object.defineProperty(leave, 'pointerType', { value: 'mouse' });
  button(w, 'today').dispatchEvent(leave);
  await new Promise(resolve => setTimeout(resolve, 220));
  assert.equal(panel(w).hidden, true);
  button(w, 'today').focus();
  assert.equal(panel(w).hidden, false);
  const close = panel(w).querySelector('button');
  close.focus();
  close.click();
  assert.equal(panel(w).hidden, true);
  assert.equal(w.document.activeElement, button(w, 'today'));
});

test('종목명은 HTML로 실행되지 않으며 한쪽 기여가 없으면 빈 상태를 보인다', t => {
  const w = setup(t);
  seed(w);
  w.PfStore.items[0].stock_name = '<img src=x onerror=alert(1)>';
  w.PfStore.items[1].quote.price = 1000;
  w.renderPortfolio({ summaryOnly: true });
  button(w, 'today').click();
  assert.equal(panel(w).querySelector('img'), null);
  assert.match(panel(w).textContent, /<img/);
  assert.match(panel(w).querySelector('.negative').textContent, /해당 종목이 없습니다/);
  w.PfStore.items = [];
  w.renderPortfolio({ summaryOnly: true });
  assert.equal(panel(w).hidden, true);
});
