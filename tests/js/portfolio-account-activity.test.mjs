import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {JSDOM} from 'jsdom';

function setup() {
  const dom = new JSDOM('<!doctype html><body></body>', {url:'https://example.test/portfolio',runScripts:'outside-only'});
  const w = dom.window;
  w.escapeHtml = value => String(value).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
  w.HTMLDialogElement.prototype.showModal = function() {this.open=true;};
  w.HTMLDialogElement.prototype.close = function() {this.open=false;this.dispatchEvent(new w.Event('close'));};
  w.eval(readFileSync(new URL('../../static/js/portfolio-account-activity.js',import.meta.url),'utf8'));
  return {dom,w,el:id=>w.document.getElementById(id)};
}
const row = {id:1,date:'2026-09-21',description:'이체입금',net_amount:1000,currency:'KRW',kind:'transfer',reason:'',revision:1,baseline:false};
const result = items => ({items,totals:[],has_more:false,state:{last_import_at:'2026-09-21T10:00:00'}});

test('입출금 사유는 원거래 번호·수정 버전과 저장하고 다른 행 입력을 유지한다', async () => {
  const s=setup(), calls=[];
  try {
    s.w.apiFetchJson=async (path,options) => {calls.push({path,options});return options ? {ok:true} : result([{...row},{...row,id:2}]);};
    await s.w.pfOpenAccountActivity({account_id:'a',name:'NH',broker:'namuh'});
    const forms=s.el('pfActivityRows').querySelectorAll('form');
    forms[0].elements.reason.value='생활비 <확인>';
    forms[1].elements.reason.value='아직 저장하지 않은 투자금';
    await s.w.pfSaveActivity({preventDefault(){},target:forms[0]});
    assert.equal(calls[1].path,'/api/portfolio/accounts/a/activity/1');
    assert.deepEqual(JSON.parse(calls[1].options.body),{revision:1,kind:'transfer',reason:'생활비 <확인>'});
    assert.equal(forms[1].elements.reason.value,'아직 저장하지 않은 투자금');
    assert.match(s.el('pfActivityStatus').textContent,/저장했습니다/);
  } finally {s.dom.window.close();}
});

test('외화 환율 미확인과 충돌 오류를 표시하고 실패한 입력을 유지한다', async () => {
  const s=setup();
  try {
    s.w.apiFetchJson=async (_path,options) => {if(options)throw Error('거래내역이 변경됐습니다');return result([{...row,currency:'USD',kind:'interest',net_amount:12,income_amount:12,fx_rate:null}]);};
    await s.w.pfOpenAccountActivity({account_id:'a',name:'NH'});
    assert.match(s.el('pfActivityRows').textContent,/환율 확인 전/);
    const form=s.el('pfActivityRows').querySelector('form');
    form.elements.reason.value='이자 확인'; form.elements.fx_rate.value='1300';
    await s.w.pfSaveActivity({preventDefault(){},target:form});
    assert.equal(form.elements.reason.value,'이자 확인');
    assert.match(s.el('pfActivityStatus').textContent,/변경됐습니다/);
  } finally {s.dom.window.close();}
});

test('로그아웃 뒤 늦은 원장 응답은 화면과 사용자 상태를 복구하지 않는다', async () => {
  const s=setup();
  try {
    let resolve;
    s.w.apiFetchJson=()=>new Promise(r=>{resolve=r;});
    const task=s.w.pfOpenAccountActivity({account_id:'a',name:'NH'});
    s.w.pfResetActivity(); resolve(result([{...row,reason:'이전 사용자'}])); await task;
    assert.equal(s.el('pfActivityDialog'),null);
    assert.equal(s.w.document.body.textContent.includes('이전 사용자'),false);
  } finally {s.dom.window.close();}
});

test('서버 변경은 원장을 갱신하지만 편집 중인 입출금 사유는 보존한다', async () => {
  const s=setup(); let calls=0;
  try {
    s.w.apiFetchJson=async()=>{calls++;return result([{...row}]);};
    await s.w.pfOpenAccountActivity({account_id:'a',name:'NH'});
    s.w.pfActivityAccountsChanged([{account_id:'other'}]);
    assert.equal(calls,1);
    s.w.pfActivityAccountsChanged([{account_id:'a'}]);
    await new Promise(resolve=>setTimeout(resolve,0));
    assert.equal(calls,2);
    const input=s.el('pfActivityRows').querySelector('[name="reason"]');
    input.value='아직 입력 중'; input.dispatchEvent(new s.w.Event('input',{bubbles:true}));
    s.w.pfActivityAccountsChanged([{account_id:'a'}]);
    assert.equal(calls,2); assert.equal(input.value,'아직 입력 중');
    assert.match(s.el('pfActivityStatus').textContent,/편집 중인/);
  } finally {s.dom.window.close();}
});
