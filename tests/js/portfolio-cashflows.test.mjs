import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

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
