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

test('NH 배당은 확인 태그와 해외 세전·현지세·국내세·원화 세후를 보이고 자동 반영 범위를 안내한다', async () => {
  const s=setup();
  try {
    const dividend={id:3,date:'2026-09-01',booked_date:'2026-09-03',description:'외화배당금입금',net_amount:8.5,income_amount:8.5,currency:'USD',kind:'dividend',
      reason:'',revision:1,baseline:true,stock_name:'애플',gross_amount:10,tax_amount:1.5,domestic_tax_krw:0,fx_rate:1400.5,net_krw:11904,verification:'nh_confirmed'};
    const review={...dividend,id:4,kind:'review',fx_rate:null,net_krw:null,gross_amount:null,verification:'needs_review'};
    const refund={...dividend,id:5,description:'외화제세금환급 · 배당 세금 정산',net_amount:1.5,domestic_tax_krw:2156,adjustment:'tax_refund',gross_amount:null};
    const domestic={...row,id:6,description:'배당금',kind:'dividend',net_amount:8460,income_amount:8460,gross_amount:10000,tax_amount:1540,fee_amount:0,verification:'nh_confirmed'};
    s.w.apiFetchJson=async()=>({items:[dividend,review,refund,domestic],totals:[{kind:'dividend',currency:'USD',amount:8.5,amount_krw:11904,adjustment_krw:-55}],has_more:false,auto_import:'dividends',state:{last_import_at:'2026-10-01T10:00:00'}});
    await s.w.pfOpenAccountActivity({account_id:'a',name:'NH',broker:'namuh'});
    assert.match(s.el('pfActivityPollingHelp').textContent,/배당·분배금은 NH 거래내역에서 자동/);
    assert.match(s.el('pfActivityPollingHelp').textContent,/이자·입출금 등은 .*보류/);
    const forms=[...s.el('pfActivityRows').querySelectorAll('form')];
    assert.equal(forms[0].querySelector('.pf-activity-tag.nh').textContent,'NH 확인');
    assert.match(forms[0].textContent,/세전 10 USD · 현지세 1\.5 USD · 국내세 0원 · 원화 세후 11,904원/);
    assert.match(forms[0].textContent,/NH 처리일 2026-09-03/);
    assert.match(forms[0].textContent,/원화 과세표준으로 검산/);
    assert.equal(forms[1].querySelector('.pf-activity-tag.review').textContent,'확인 필요');
    assert.match(forms[1].textContent,/환율 확인 전/);
    assert.match(forms[2].textContent,/배당 세금 정산 · 환급 1\.5 USD · 국내세 2,156원/);
    assert.match(forms[3].textContent,/세전 10,000, 세금 1,540/);
    // 원통화·원화 합계는 같은 배당 행이고, 외화 세금 정산은 따로 보인다.
    assert.match(s.el('pfActivityTotals').textContent,/8\.5 USD \(원화 11,904원\) · 세금 정산 -55원/);
  } finally {s.dom.window.close();}
});
