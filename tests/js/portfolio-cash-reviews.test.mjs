import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {JSDOM} from 'jsdom';

function setup() {
  const dom = new JSDOM('<!doctype html><body></body>', {url:'https://example.test/portfolio',runScripts:'outside-only'});
  const w = dom.window;
  w.escapeHtml = value => String(value).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
  w.HTMLDialogElement.prototype.showModal = function() {this.open = true;};
  w.HTMLDialogElement.prototype.close = function() {this.open = false; this.dispatchEvent(new w.Event('close'));};
  w.eval(readFileSync(new URL('../../static/js/portfolio-cash-reviews.js', import.meta.url), 'utf8'));
  return {dom,w,el:id=>w.document.getElementById(id)};
}
const categories = [{id:'external_deposit',label:'비관리 계좌에서 입금',direction:1},{id:'external_withdrawal',label:'비관리 계좌로 출금',direction:-1},{id:'distribution',label:'배당·분배금 출금',direction:-1},{id:'other',label:'기타',direction:0}];
const row = {id:1,account_name:'NH <계좌>',from_at:'2026-10-01T10:00:00',to_at:'2026-10-01T10:01:00',currency:'KRW',amount:1000,unexplained:1000,category:'',reason:'',revision:1};
const result = items => ({items:items.map(item=>({...item})),total:items.length,has_more:false,categories});
const tick = () => new Promise(resolve=>setTimeout(resolve,0));

test('접속 시 미입력만 자동 표시하고 나중에 입력을 선택해도 알림을 유지한다', async () => {
  const s=setup();
  try {
    s.w.apiFetchJson=async()=>result([row]);
    await s.w.pfLoadCashReviews({auto:true});
    assert.equal(s.el('pfCashReviewDialog').open,true);
    assert.equal(s.el('pfCashReviewRows').querySelector('h3').textContent,'NH <계좌> · 입금 1,000 KRW');
    assert.equal(s.el('pfCashReviewRows').querySelector('select').querySelector('[value="external_withdrawal"]'),null);
    s.el('pfCashReviewClose').click();
    await s.w.pfLoadCashReviews({auto:true});
    assert.equal(s.el('pfCashReviewDialog').open,false);
    assert.equal(s.el('pfCashReviewNotice').hidden,false);
    s.el('pfCashReviewNotice').click(); await tick();
    assert.equal(s.el('pfCashReviewDialog').open,true);
  } finally {s.dom.window.close();}
});

test('다른 대화상자가 열려 있으면 자동 창은 겹치지 않고 알림을 표시한다', async () => {
  const s=setup();
  try {
    s.w.document.body.innerHTML='<dialog open></dialog>';
    s.w.apiFetchJson=async()=>result([row]);
    await s.w.pfLoadCashReviews({auto:true});
    assert.equal(s.el('pfCashReviewDialog'),null);
    assert.match(s.el('pfCashReviewNotice').textContent,/1건/);
  } finally {s.dom.window.close();}
});

test('사유 저장은 버전과 보내고 다른 항목의 초안과 서버 충돌 시 입력을 보존한다', async () => {
  const s=setup(), calls=[];
  try {
    s.w.apiFetchJson=async(path,options)=>{calls.push({path,options});return options?.method === 'PATCH'?{ok:true}:result([row,{...row,id:2}]);};
    await s.w.pfOpenCashReviews();
    const [first,second]=s.el('pfCashReviewRows').querySelectorAll('form');
    first.elements.category.value='external_deposit'; first.elements.reason.value='투자금 추가';
    second.elements.category.value='other'; second.elements.reason.value='아직 확인 중';
    second.elements.reason.dispatchEvent(new s.w.Event('input',{bubbles:true}));
    await s.w.pfSaveCashReview({preventDefault(){},target:first});
    assert.equal(calls[1].path,'/api/portfolio/cash-reviews/1');
    assert.deepEqual(JSON.parse(calls[1].options.body),{revision:1,category:'external_deposit',reason:'투자금 추가'});
    assert.equal(second.elements.reason.value,'아직 확인 중');
    s.w.pfCheckCashReviews(); await tick();
    assert.equal(second.elements.reason.value,'아직 확인 중');
    s.w.apiFetchJson=async()=>{throw Error('변경됐습니다. 새로 조회');};
    await s.w.pfSaveCashReview({preventDefault(){},target:second});
    assert.equal(second.elements.reason.value,'아직 확인 중');
    assert.match(s.el('pfCashReviewStatus').textContent,/변경됐습니다/);
  } finally {s.dom.window.close();}
});

test('기타는 상세 사유가 필수이고 조회로 초안을 버리려면 확인한다', async () => {
  const s=setup(); let calls=0;
  try {
    s.w.apiFetchJson=async()=>{calls++;return result([row]);};
    await s.w.pfOpenCashReviews();
    const form=s.el('pfCashReviewRows').querySelector('form');
    form.elements.category.value='other';
    form.elements.category.dispatchEvent(new s.w.Event('change',{bubbles:true}));
    assert.equal(form.elements.reason.required,true);
    assert.equal(form.reportValidity(),false);
    s.w.confirm=()=>false;
    await s.w.pfLoadCashReviews({render:true,replaceDraft:true});
    assert.equal(calls,1);
    s.w.confirm=()=>true;
    await s.w.pfLoadCashReviews({render:true,replaceDraft:true});
    assert.equal(calls,2);
    assert.equal(s.el('pfCashReviewRows').querySelector('select').value,'');
  } finally {s.dom.window.close();}
});

test('로그아웃 뒤 도착한 응답은 이전 사용자의 알림과 대화상자를 복구하지 않는다', async () => {
  const s=setup();
  try {
    let resolve;
    s.w.apiFetchJson=()=>new Promise(r=>{resolve=r;});
    const promise=s.w.pfLoadCashReviews({auto:true});
    s.w.pfResetCashReviews(); resolve(result([row])); await promise;
    assert.equal(s.el('pfCashReviewDialog'),null);
    assert.equal(s.el('pfCashReviewNotice'),null);
  } finally {s.dom.window.close();}
});

test('저장 직전 시작된 조회의 늦은 응답이 저장한 사유를 되돌리지 않는다', async () => {
  const s=setup(); let gets=0, resolveOld;
  try {
    s.w.apiFetchJson=async (_path,options)=>{
      if(options?.method === 'PATCH') return {ok:true};
      gets++;
      if(gets === 2) return new Promise(resolve=>{resolveOld=resolve;});
      return result([{...row,...(gets>2?{category:'external_deposit',reason:'최신 사유',revision:2}:{})}]);
    };
    await s.w.pfOpenCashReviews();
    const oldQuery=s.w.pfLoadCashReviews({render:true});
    const form=s.el('pfCashReviewRows').querySelector('form');
    form.elements.category.value='external_deposit'; form.elements.reason.value='최신 사유';
    form.elements.reason.dispatchEvent(new s.w.Event('input',{bubbles:true}));
    await s.w.pfSaveCashReview({preventDefault(){},target:form});
    resolveOld(result([row])); await oldQuery;
    assert.equal(s.el('pfCashReviewRows').querySelector('textarea').value,'최신 사유');
  } finally {s.dom.window.close();}
});

test('전체 이력에서 자동 확인과 저장된 사유를 조회하고 출금 사유를 수정할 수 있다', async () => {
  const s=setup(), calls=[];
  try {
    s.w.apiFetchJson=async path=>{calls.push(path);return result([{...row,amount:-500,unexplained:-500,category:'distribution',reason:'월 분배금'}, {...row,id:2,unexplained:0}]);};
    await s.w.pfOpenCashReviews();
    s.el('pfCashReviewMode').click(); await tick();
    assert.match(calls[1],/pending=false/);
    assert.match(s.el('pfCashReviewRows').textContent,/자동 확인/);
    const form=s.el('pfCashReviewRows').querySelector('form');
    assert.equal(form.elements.category.value,'distribution');
    assert.equal(form.elements.reason.value,'월 분배금');
    assert.equal(form.querySelector('button').textContent,'사유 수정');
  } finally {s.dom.window.close();}
});
