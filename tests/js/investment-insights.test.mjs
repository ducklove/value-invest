import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

function setup() {
  const dom = new JSDOM(readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8'), { runScripts: 'outside-only', url: 'http://localhost' });
  const win = dom.window;
  win.escapeHtml = value => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
  win.reportApiError = () => {};
  for (const file of ['portfolio-attribution.js', 'portfolio-theses.js']) win.eval(readFileSync(new URL(`../../static/js/${file}`, import.meta.url), 'utf8'));
  return dom;
}

test('수익 분해는 음수·미분류·누락 정보를 보여주며 입력 문자열을 이스케이프한다', () => {
  const dom = setup();
  const data = { available: true, baseline_date: '2026-06-01', ending_date: '2026-06-02', value_change: -200, investment_pnl: -200, reconciliation_error: 0,
    components: [{ key: 'price', label: '<img src=x>', amount: -100 }, { key: 'unclassified', label: '미분류', amount: -100 }],
    stocks: [{ stock_code: 'AAPL', stock_name: '<script>alert(1)</script>', total: -100, price: -100, fx: 0, combined: 0 }],
    coverage: { eligible_intervals: 1, examined_intervals: 2, issues: { '수량 변경': 1 } }, notes: ['<b>계산 한계</b>'] };
  dom.window.document.getElementById('pfAttributionContent').innerHTML = dom.window.pfAttributionHtml(data);
  const content = dom.window.document.getElementById('pfAttributionContent');
  assert.equal(content.querySelectorAll('img,script,b').length, 0);
  assert.equal(content.querySelector('.negative').style.width, '100%');
  assert.match(content.textContent, /-100원/);
  assert.match(content.textContent, /수량 변경 1구간/);
  assert.match(content.textContent, /미분류/);
  dom.window.close();
});

test('빠른 기간 변경에서 이전 응답이 최신 수익 분해를 덮어쓰지 않는다', async () => {
  const dom = setup();
  const resolves = [];
  dom.window.apiFetchJson = () => new Promise(resolve => resolves.push(resolve));
  const old = dom.window.pfLoadAttribution();
  const current = dom.window.pfLoadAttribution();
  resolves[1]({ available: false, message: '최신 기간' }); await current;
  resolves[0]({ available: false, message: '이전 기간' }); await old;
  assert.match(dom.window.document.getElementById('pfAttributionContent').textContent, /최신 기간/);
  dom.window.close();
});

test('투자 논거의 데이터 누락은 정상으로 표시하지 않고 위험한 근거 링크를 렌더하지 않는다', async () => {
  const dom = setup();
  dom.window.apiFetchJson = async () => ({ items: [{ id: 1, stock_code: '005930', thesis: '<img src=x>', invalidation: '이익률 하락', archived: false,
    check: { status: 'unknown', value: null, threshold: 10, operator: 'lt', evidence_url: 'javascript:alert(1)' } }] });
  await dom.window.pfLoadTheses();
  const content = dom.window.document.getElementById('pfThesisContent');
  assert.match(content.textContent, /자료 확인 필요/);
  assert.match(content.textContent, /자료 없음/);
  assert.equal(content.querySelectorAll('img,a[href^="javascript:"]').length, 0);
  assert.equal(content.querySelector('a').getAttribute('href'), '/analysis?code=005930');
  dom.window.close();
});
