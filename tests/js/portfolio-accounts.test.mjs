import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {JSDOM} from 'jsdom';

function setup() {
  const dom = new JSDOM(readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8'), {url:'https://example.test/portfolio',runScripts:'outside-only'});
  const w = dom.window;
  w.PfStore = {accountId:'',items:[],edit:{},manualOrder:{}};
  w.escapeHtml = value => String(value).replaceAll('<','&lt;');
  w.loadPortfolio = async () => {};
  const sockets = [];
  w.WebSocket = class { constructor() { sockets.push(this); } close() {this.closed = true;} };
  w.QuoteManager = {onQuote: (...args) => { w.lastQuote = args; }};
  for (const dialog of w.document.querySelectorAll('dialog')) {
    dialog.showModal = () => {dialog.open = true;};
    dialog.close = () => {dialog.open = false; dialog.dispatchEvent(new w.Event('close'));};
  }
  w.eval(readFileSync(new URL('../../static/js/portfolio-accounts.js',import.meta.url),'utf8') + '\nwindow.PfAccounts = PfAccounts;');
  w.document.dispatchEvent(new w.Event('DOMContentLoaded'));
  return {dom,w,sockets, el: id => w.document.getElementById(id)};
}

test('합산의 다중 계좌 편집은 계좌 선택으로 보내고 수동 계좌 선택 시 허용한다', async () => {
  const s=setup();
  try {
    s.w.apiFetchJson = async () => [{account_id:'a',name:'일반'},{account_id:'b',name:'연금'}];
    await s.w.pfLoadAccounts();
    assert.equal(s.w.pfAccountNeedsSelection(),true);
    s.w.eval('PfStore.accountId="b"');
    assert.equal(Boolean(s.w.pfAccountNeedsSelection()),false);
    s.w.eval('PfAccounts.rows[1].broker="namuh"');
    assert.equal(Boolean(s.w.pfAccountNeedsSelection()),true);
  } finally {s.dom.window.close();}
});

test('나무 키 확인·미리보기는 연결을 생성하지 않고 키는 확인 후 입력창에서 지운다', async () => {
  const s=setup(), calls=[];
  try {
    s.w.apiFetchJson = async (path, options) => {
      calls.push({path,body:JSON.parse(options.body)});
      return path.endsWith('/credentials') ? {accounts:[{account_no:'12345678901',account_mask:'••••8901',environment:'live',selection:'opaque'}]} : {items:[]};
    };
    s.w.pfOpenNhConnection({account_id:'a',name:'NH'});
    s.el('pfNhKey').value='private-key'; s.el('pfNhSecret').value='private-secret';
    await s.w.pfNhWork('verify');
    assert.equal(s.el('pfNhKey').value,'');
    assert.equal(s.el('pfNhSecret').value,'');
    assert.match(s.el('pfNhChoices').textContent,/12345678901/);
    await s.w.pfNhWork('preview');
    s.el('pfNhDialog').close();
    assert.deepEqual(calls.map(row => row.path), ['/api/portfolio/namuh/credentials','/api/portfolio/accounts/a/namuh/preview']);
    assert.equal(s.w.localStorage.length,0);
  } finally {s.dom.window.close();}
});

test('NH 브라우저 연결을 공유하고 사용자 상태 초기화 시 소켓·재연결·계좌 정보를 지운다', async () => {
  const s=setup();
  try {
    s.w.apiFetchJson = async () => [{account_id:'a',name:'NH',broker:'namuh',connection:{}}];
    await s.w.pfLoadAccounts(); await s.w.pfLoadAccounts(true);
    assert.equal(s.sockets.length,1);
    s.sockets[0].onmessage({data:JSON.stringify({type:'quote',code:'005930',price:100})});
    assert.equal(s.w.lastQuote[0],'005930');
    s.w.pfResetAccounts();
    assert.equal(s.sockets[0].closed,true);
    assert.equal(s.w.eval('PfAccounts.rows.length'),0);
    assert.equal(s.w.eval('PfAccounts.retry'),null);
  } finally {s.dom.window.close();}
});

test('해외주식을 제외한 NH 미리보기에도 결제 후 원화·외화 예수금을 표시한다', async () => {
  const s=setup();
  try {
    s.w.apiFetchJson = async (path, options) => {
      if (path.endsWith('/credentials')) return {accounts:[{account_no:'12345678901',environment:'live',selection:'opaque'}]};
      assert.equal(JSON.parse(options.body).include_overseas,false);
      return {items:[{stock_code:'CASH_KRW',stock_name:'원화 현금',quantity:800,currency:'KRW'},
        {stock_code:'CASH_USD',stock_name:'USD 현금',quantity:80,currency:'USD'}]};
    };
    s.w.pfOpenNhConnection({account_id:'a',name:'NH'});
    await s.w.pfNhWork('verify');
    s.el('pfNhOverseas').checked=false;
    await s.w.pfNhWork('preview');
    assert.match(s.el('pfNhDialog').textContent,/비상장·상장폐지 종목은 제외/);
    assert.match(s.el('pfNhPreview').textContent,/원화 현금800KRW/);
    assert.match(s.el('pfNhPreview').textContent,/USD 현금80USD/);
    assert.match(s.el('pfNhPreview').textContent,/D\+2 예수금/);
    assert.match(s.el('pfNhPreview').textContent,/외화는 결제 후 예수금/);
    assert.equal(s.el('pfNhSave').disabled,false);
  } finally {s.dom.window.close();}
});

test('NH 미리보기는 CMA RP와 현금 잔액을 별도로 표시하고 평가 시점을 설명한다', async () => {
  const s=setup();
  try {
    s.w.apiFetchJson = async path => path.endsWith('/credentials')
      ? {accounts:[{account_no:'12345678901',account_mask:'••••8901',environment:'live',selection:'opaque'}]}
      : {items:[{stock_code:'CASH_KRW',stock_name:'원화 현금',quantity:80,currency:'KRW'},
          {stock_code:'CMA_RP_KRW',stock_name:'CMA 원화RP',quantity:3050,currency:'KRW'}]};
    s.w.pfOpenNhConnection({account_id:'a',name:'NH'});
    s.el('pfNhKey').value='private-key'; s.el('pfNhSecret').value='private-secret';
    await s.w.pfNhWork('verify');
    await s.w.pfNhWork('preview');
    const rows=s.el('pfNhPreview').querySelectorAll('tbody tr');
    assert.equal(rows.length,2);
    assert.match(rows[0].textContent,/원화 현금80KRW/);
    assert.match(rows[1].textContent,/CMA 원화RP3,050KRW/);
    assert.match(s.el('pfNhPreview').textContent,/현금과 구분/);
    assert.match(s.el('pfNhPreview').textContent,/조회 시점의 평가액/);
    assert.equal(s.el('pfNhSave').disabled,false);
  } finally {s.dom.window.close();}
});

test('로그아웃 후 늦게 도착한 이전 사용자의 계좌 응답으로 연결을 재개하지 않는다', async () => {
  const s=setup();
  try {
    let finish;
    s.w.apiFetchJson = () => new Promise(resolve => {finish=resolve;});
    const pending=s.w.pfLoadAccounts();
    s.w.pfResetAccounts();
    finish([{account_id:'old',name:'이전 사용자',broker:'namuh'}]);
    await pending;
    assert.equal(s.w.eval('PfAccounts.rows.length'),0);
    assert.equal(s.sockets.length,0);
  } finally {s.dom.window.close();}
});

test('계좌 종류를 바꾸면 미리보기를 무효화하고 금·선물에는 주식 필터를 적용하지 않는다', async () => {
  const s=setup();
  try {
    s.w.pfOpenNhConnection({account_id:'a',name:'금 계좌'});
    s.w.PfAccounts.previewed=true;
    s.el('pfNhSave').disabled=false;
    s.el('pfNhProduct').value='gold';
    s.el('pfNhProduct').dispatchEvent(new s.w.Event('change'));
    assert.equal(s.w.PfAccounts.previewed,false);
    assert.equal(s.el('pfNhSave').disabled,true);
    assert.equal(s.el('pfNhOverseas').closest('label').hidden,true);
    assert.match(s.el('pfNhProductHelp').textContent,/금현물 전용/);
    assert.doesNotMatch(s.el('pfNhProductHelp').textContent,/비상장/);
    s.el('pfNhChoices').innerHTML='<option value="test">계좌</option>';
    s.w.apiFetchJson=async (_path,options) => {
      assert.equal(JSON.parse(options.body).product,'gold');
      return {items:[]};
    };
    await s.w.pfNhWork('preview');
    assert.equal(s.el('pfNhSave').disabled,false);
  } finally {s.dom.window.close();}
});

test('선물 계약 수·방향과 계좌 평가액을 구분하고 미제공 값·계좌 범위·로그아웃을 보존한다', async () => {
  const s=setup();
  try {
    const snapshot={product:'gbfuture',equity:1490,pnl:90,currency:'KRW',as_of_date:'2026-09-18',
      positions:[{code:'ESU26',name:'<선물>',side:'매도',quantity:2,currency:'USD',average_price:null,current_price:null,pnl:null}]};
    s.w.apiFetchJson=async () => [{account_id:'a',name:'해외 선물',broker:'namuh',connection:{sync_error:'조회 실패'},broker_snapshot:snapshot},
      {account_id:'b',name:'주식 계좌'}];
    await s.w.pfLoadAccounts();
    assert.equal(s.el('pfDerivativeBalances').hidden,false);
    assert.match(s.el('pfDerivativeBalances').textContent,/1,490원/);
    assert.match(s.el('pfDerivativeBalances').textContent,/매도2USD미제공미제공미제공/);
    assert.match(s.el('pfDerivativeBalances').textContent,/이전 잔고 표시/);
    assert.equal(s.el('pfDerivativeBalances').querySelector('선물'),null);
    s.w.PfStore.accountId='b';
    s.w.pfRenderDerivativeBalances();
    assert.equal(s.el('pfDerivativeBalances').hidden,true);
    s.w.PfStore.accountId='a';
    s.w.pfRenderDerivativeBalances();
    s.w.pfResetAccounts();
    assert.equal(s.el('pfDerivativeBalances').textContent,'');
    assert.equal(s.el('pfDerivativeBalances').hidden,true);
  } finally {s.dom.window.close();}
});
