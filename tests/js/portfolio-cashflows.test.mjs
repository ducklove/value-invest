import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const script = readFileSync(new URL('../../static/js/portfolio-cashflows.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8');

function setupForm(handler) {
  const dom = new JSDOM(html.match(/<dialog id="pfCfDialog"[\s\S]*?<\/dialog>/)[0], { runScripts: 'outside-only' });
  const w = dom.window;
  const calls = [];
  w.apiFetchJson = async (path, options) => { calls.push(JSON.parse(options.body)); return handler?.(); };
  w.showToast = () => {};
  w.reportApiError = () => {};
  w.loadPortfolio = async () => {};
  w.loadPerformanceData = async () => { calls.push('history'); };
  const el = id => w.document.getElementById(`pfCf${id}`);
  el('Dialog').showModal = () => { el('Dialog').open = true; };
  el('Dialog').close = () => { el('Dialog').open = false; };
  w.eval(script);
  w.pfOpenCashflow();
  return { dom, w, calls, el };
}

test('상단 입력 창은 심층 분석에 진입하지 않고 등록하며 중복 클릭을 막고 내역을 갱신한다', async () => {
  let finish;
  const s = setupForm(() => new Promise(resolve => { finish = resolve; }));
  assert.equal(s.el('Dialog').open, true);
  assert.equal(s.el('Date').value, new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Seoul' }).format(new Date()));
  s.el('Amount').value = '2000';
  s.el('Memo').value = ' 추가 자금 ';
  const pending = s.w.addCashflow();
  await s.w.addCashflow();
  assert.equal(s.calls.length, 1);
  assert.equal(s.el('Save').disabled, true);
  assert.equal(s.calls[0].memo, '추가 자금');
  finish();
  await pending;
  assert.equal(s.el('Dialog').open, false);
  assert.equal(s.el('Amount').value, '');
  assert.equal(s.calls.at(-1), 'history');
  s.dom.window.close();
});

test('입출금 실패 시 입력을 보존하고 창에서 오류를 보여준다', async () => {
  const s = setupForm(() => { throw new Error('원화 잔액이 부족합니다.'); });
  s.el('Type').value = 'withdrawal';
  s.el('Amount').value = '2000';
  await s.w.addCashflow();
  assert.equal(s.el('Dialog').open, true);
  assert.equal(s.el('Amount').value, '2000');
  assert.equal(s.el('Save').disabled, false);
  assert.match(s.el('Status').textContent, /잔액이 부족/);
  assert.equal(s.calls.length, 1);
  s.dom.window.close();
});

test('분배금은 입출금과 같은 목록에 실제 통화·원화 금액·좌수 유지로 표시한다', () => {
  const dom = new JSDOM('<table><tbody id="pfCfBody"></tbody></table>', { runScripts: 'outside-only' });
  const w = dom.window;
  w.escapeHtml = s => String(s);
  w.fmtNum = n => String(n);
  w.eval(script);
  w.renderCashflows([
    { id: -1, date: '2026-09-09', type: 'distribution', amount: 119000, currency: 'USD', native_amount: 85, units_change: 0 },
    { id: 2, date: '2026-09-08', type: 'deposit', amount: 1000, units_change: 1, applied_snapshot_date: '2026-09-08' },
  ], [{ date: '2026-09-08', total_units: 12 }]);
  const row = w.document.querySelector('tr');
  assert.match(row.textContent, /분배금 출금/);
  assert.match(row.textContent, /119000원/);
  assert.match(row.textContent, /85 USD/);
  assert.equal(row.children[4].textContent, '0.00');
  assert.match(row.textContent, /좌수 유지/);
  assert.equal(row.querySelector('.js-pf-cf-delete'), null);
  assert.equal(w.document.querySelectorAll('tr').length, 2);
  dom.window.close();
});

test('정산 취소는 원거래와 취소 거래를 보여주고 반복 삭제를 제공하지 않는다', () => {
  const dom = new JSDOM('<table><tbody id="pfCfBody"></tbody></table>', { runScripts: 'outside-only' });
  const w = dom.window;
  w.escapeHtml = s => String(s);
  w.fmtNum = n => String(n);
  w.eval(readFileSync(new URL('../../static/js/portfolio-cashflows.js', import.meta.url), 'utf8'));
  w.renderCashflows([
    { id: 1, date: '2026-08-31', type: 'deposit', amount: 2000, cancelled_at: '2026-09-07T12:00:00',
      applied_snapshot_date: '2026-08-31', units_change: 2 },
    { id: 2, date: '2026-09-07', type: 'withdrawal', amount: 2000, reversal_of_id: 1 },
    { id: 3, date: '2026-09-07', type: 'deposit', amount: 1000 },
  ], [{ date: '2026-08-31', total_units: 12 }]);
  const rows = w.document.querySelectorAll('tr');
  assert.match(rows[0].textContent, /취소됨/);
  assert.match(rows[1].textContent, /취소 거래/);
  assert.equal(w.document.querySelectorAll('.js-pf-cf-delete').length, 1);
  assert.equal(rows[1].children[5].textContent, '-');
  dom.window.close();
});
