import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {JSDOM} from 'jsdom';

function setup({memo = '장기 보유', fail = false} = {}) {
  const dom = new JSDOM(`<table><tbody id="pfBody"><tr data-code="005930">
    <td><button class="pf-memo-badge js-pf-memo-edit">📝</button></td>
    <td class="pf-col-memo"><button class="pf-memo-cell js-pf-memo-edit">${memo}</button></td>
  </tr></tbody></table><div id="outside"></div>`, {runScripts: 'outside-only', url: 'https://example.test'});
  const w = dom.window;
  const calls = [], toasts = [], errors = [];
  let renders = 0;
  w.PfStore = {items: [{stock_code: '005930', stock_name: '삼성전자', quantity: 3, avg_price: 80000, memo: memo || null}]};
  w.escapeHtml = (v) => String(v ?? '').replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
  w.showToast = (msg) => toasts.push(msg);
  w.reportApiError = (err) => errors.push(err);
  w.renderPortfolio = () => { renders += 1; };
  w.apiFetchJson = async (path, options) => {
    const body = JSON.parse(options.body);
    calls.push({path, method: options.method, body});
    if (fail) throw new Error('저장 실패');
    return {ok: true, memo: body.memo || null};
  };
  w.eval(readFileSync(new URL('../../static/js/portfolio-memo.js', import.meta.url), 'utf8'));
  const anchor = w.document.querySelector('.pf-memo-cell');
  const flush = () => new Promise((r) => setTimeout(r, 0));
  return {dom, w, calls, toasts, errors, anchor, flush, renders: () => renders};
}

function keydown(w, el, key, extra = {}) {
  el.dispatchEvent(new w.KeyboardEvent('keydown', {key, bubbles: true, cancelable: true, ...extra}));
}

test('메모 칸을 누르면 수정 모드 없이 팝오버가 열리고 Enter 로 메모만 저장한다', async () => {
  const {dom, w, calls, toasts, anchor, flush, renders} = setup();
  try {
    w.pfOpenMemoEditor('005930', anchor);
    const input = w.document.querySelector('.pf-memo-popover textarea');
    assert.ok(input, '팝오버가 body 에 열린다');
    assert.equal(input.value, '장기 보유');
    assert.equal(w.document.activeElement, input);
    input.value = '  배당 확인 후 추가매수  ';
    keydown(w, input, 'Enter');
    await flush();
    assert.deepEqual(calls, [{path: '/api/portfolio/005930/metadata', method: 'PUT', body: {memo: '배당 확인 후 추가매수'}}]);
    assert.equal(w.PfStore.items[0].memo, '배당 확인 후 추가매수');
    assert.equal(renders(), 1);
    assert.equal(w.document.querySelector('.pf-memo-popover'), null);
    assert.deepEqual(toasts, ['메모를 저장했습니다.']);
  } finally { dom.window.close(); }
});

test('Shift+Enter 는 저장하지 않고 Esc 는 저장 없이 닫는다', async () => {
  const {dom, w, calls, anchor, flush} = setup();
  try {
    w.pfOpenMemoEditor('005930', anchor);
    const input = w.document.querySelector('.pf-memo-popover textarea');
    input.value = '바뀐 메모';
    keydown(w, input, 'Enter', {shiftKey: true});
    keydown(w, input, 'Enter', {isComposing: true});
    await flush();
    assert.equal(calls.length, 0);
    keydown(w, input, 'Escape');
    assert.equal(w.document.querySelector('.pf-memo-popover'), null);
    assert.equal(w.PfStore.items[0].memo, '장기 보유');
  } finally { dom.window.close(); }
});

test('바깥을 누르면 바뀐 메모는 저장하고, 그대로면 요청 없이 닫는다', async () => {
  const {dom, w, calls, anchor, flush} = setup();
  try {
    w.pfOpenMemoEditor('005930', anchor);
    w.document.getElementById('outside').dispatchEvent(new w.Event('pointerdown', {bubbles: true}));
    assert.equal(calls.length, 0);
    assert.equal(w.document.querySelector('.pf-memo-popover'), null);

    w.pfOpenMemoEditor('005930', anchor);
    w.document.querySelector('.pf-memo-popover textarea').value = '';
    w.document.getElementById('outside').dispatchEvent(new w.Event('pointerdown', {bubbles: true}));
    await flush();
    assert.deepEqual(calls[0].body, {memo: ''});
    assert.equal(w.PfStore.items[0].memo, null);
  } finally { dom.window.close(); }
});

test('저장 실패 시 팝오버와 입력값을 유지해 다시 시도할 수 있다', async () => {
  const {dom, w, errors, anchor, flush} = setup({fail: true});
  try {
    w.pfOpenMemoEditor('005930', anchor);
    const input = w.document.querySelector('.pf-memo-popover textarea');
    input.value = '재시도할 메모';
    w.document.querySelector('.js-pf-memo-save').click();
    await flush();
    assert.equal(errors.length, 1);
    assert.ok(w.document.querySelector('.pf-memo-popover'));
    assert.equal(input.value, '재시도할 메모');
    assert.equal(input.disabled, false);
    assert.equal(w.PfStore.items[0].memo, '장기 보유');
  } finally { dom.window.close(); }
});

test('투자 인사이트 모달 메모는 포커스를 잃을 때 자동 저장한다', async () => {
  const {dom, w, calls, flush} = setup({memo: ''});
  try {
    const host = w.document.createElement('div');
    host.innerHTML = w.pfRenderInsightMemo('005930');
    w.document.body.appendChild(host);
    const input = host.querySelector('.js-pf-insight-memo');
    assert.equal(input.value, '');
    input.value = '실적 발표 2/5';
    input.dispatchEvent(new w.Event('change', {bubbles: true}));
    await flush();
    assert.deepEqual(calls[0], {path: '/api/portfolio/005930/metadata', method: 'PUT', body: {memo: '실적 발표 2/5'}});
    assert.equal(host.querySelector('.pf-insight-memo-status').textContent, '저장됨');
    assert.equal(w.pfRenderInsightMemo('NOPE'), '');
  } finally { dom.window.close(); }
});

test('보고 있는 계좌에 없는 종목(종목분석 합산 보유)은 계좌 헤더 없이 저장하고, 늦은 완료가 다른 팝오버를 닫지 않는다', async () => {
  const {dom, w, anchor, flush} = setup();
  try {
    const opts = [];
    let release;
    w.apiFetchJson = (path, options) => { opts.push(options); return new Promise((r) => { release = () => r({memo: JSON.parse(options.body).memo}); }); };
    const fallback = {stock_code: '000660', stock_name: 'SK하이닉스', memo: ''};
    w.pfOpenMemoEditor('000660', anchor, fallback);
    const a = w.document.querySelector('.pf-memo-popover');
    a.querySelector('textarea').value = 'A';
    a.querySelector('.js-pf-memo-save').click();
    await flush();
    assert.equal(opts[0].aggregate, true);
    w.pfCloseMemoEditor();
    w.pfOpenMemoEditor('005930', anchor);
    const b = w.document.querySelector('.pf-memo-popover');
    release();
    await flush();
    assert.ok(b.isConnected, 'A 의 늦은 완료가 B 를 닫지 않는다');
    assert.equal(fallback.memo, 'A');
  } finally { dom.window.close(); }
});

test('같은 종목 메모 저장은 순서대로 보내 늦은 이전 응답이 최신 값을 덮지 않는다', async () => {
  const {dom, w, flush} = setup();
  try {
    const sent = [];
    const pending = [];
    w.apiFetchJson = (path, options) => {
      const memo = JSON.parse(options.body).memo;
      sent.push(memo);
      return new Promise((r) => pending.push(() => r({memo})));
    };
    const first = w.pfSaveHoldingMemo('005930', 'older');
    const second = w.pfSaveHoldingMemo('005930', 'newer');
    await flush();
    assert.deepEqual(sent, ['older'], '두 번째 요청은 첫 응답 뒤에 보낸다');
    pending.shift()();
    await first; await flush();
    assert.deepEqual(sent, ['older', 'newer']);
    pending.shift()();
    await second;
    assert.equal(w.PfStore.items[0].memo, 'newer');
    assert.equal(w.pfMemoEditorOpenFor('005930'), false);
  } finally { dom.window.close(); }
});
