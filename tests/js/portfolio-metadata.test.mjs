import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {JSDOM} from 'jsdom';

function setup(locked = true) {
  const dom = new JSDOM(`<table><tbody id="pfBody"><tr data-code="005930">
    <td><input class="js-pf-edit-name" value="내 삼성전자"></td>
    <td><input class="js-pf-edit-qty" value="3" ${locked ? 'disabled data-balance-locked' : ''}></td>
    <td><input class="js-pf-edit-price" value="80000" ${locked ? 'disabled data-balance-locked' : ''}></td>
    <td><input class="js-pf-edit-target" value="120000"></td>
    <td><input class="js-pf-edit-memo" value="장기 보유"></td>
    <td><input class="js-pf-edit-created-at" value="2020-02-20"></td>
    <td><button class="js-pf-save"></button></td>
  </tr></tbody></table>`, {runScripts:'outside-only', url:'https://example.test'});
  const w = dom.window, calls = [];
  w.PfStore = {items:[{stock_code:'005930',stock_name:'삼성전자',quantity:3,avg_price:80000}], edit:{code:'005930'}};
  w.getIntegrationConfig = () => ({});
  w.pfInitInitialRegistration = () => {};
  w.pfInitHoldingRemoval = () => {};
  w.pfAccountNeedsSelection = () => locked;
  w.renderPortfolio = () => {};
  w.showToast = () => {};
  w.reportApiError = () => {};
  w.pfAvgPriceKrw = () => 80000;
  w.apiFetchJson = async (path, options) => {
    const body = JSON.parse(options.body);
    calls.push({path,body});
    return body;
  };
  w.eval(readFileSync(new URL('../../static/js/portfolio-actions.js',import.meta.url),'utf8'));
  return {dom,w,calls,row:w.document.querySelector('tr')};
}

test('연동 설정 저장은 수량과 매입가를 전송하지 않고 로컬 잔고도 변경하지 않는다', async () => {
  const {dom,w,calls,row} = setup();
  try {
    row.querySelector('.js-pf-edit-qty').value = '999';
    row.querySelector('.js-pf-edit-price').value = '1';
    await w.savePortfolioEdit('005930', undefined, row);
    assert.equal(calls[0].path,'/api/portfolio/005930/metadata');
    assert.deepEqual(calls[0].body, {stock_name:'내 삼성전자',target_price:120000,memo:'장기 보유',created_at:'2020-02-20'});
    assert.equal(w.PfStore.items[0].quantity,3);
    assert.equal(w.PfStore.items[0].avg_price,80000);
    assert.equal(w.PfStore.items[0].stock_name,'내 삼성전자');
  } finally {dom.window.close();}
});

test('설정 저장 실패 후에도 연동 수량과 매입가는 잠겨 있고 설정은 재시도할 수 있다', async () => {
  const {dom,w,row} = setup();
  try {
    w.apiFetchJson = async () => {throw new Error('저장 실패');};
    await w.savePortfolioEdit('005930', undefined, row);
    assert.equal(row.querySelector('.js-pf-edit-qty').disabled,true);
    assert.equal(row.querySelector('.js-pf-edit-price').disabled,true);
    assert.equal(row.querySelector('.js-pf-edit-name').disabled,false);
    assert.equal(row.querySelector('.js-pf-save').disabled,false);
    assert.equal(w.PfStore.edit.code,'005930');
  } finally {dom.window.close();}
});

test('메모만 수정하면 증권사 종목명을 사용자 지정 이름으로 고정하지 않는다', async () => {
  const {dom,w,calls,row} = setup();
  try {
    row.querySelector('.js-pf-edit-name').value = '삼성전자';
    await w.savePortfolioEdit('005930', undefined, row);
    assert.equal('stock_name' in calls[0].body,false);
    assert.equal(w.PfStore.items[0].stock_name,'삼성전자');
  } finally {dom.window.close();}
});

test('목표가 해제는 잔고와 종목명 없이 설정만 저장하고 수동 계좌의 잔고 편집은 유지한다', async () => {
  const {dom,w,calls,row} = setup(false);
  try {
    await w.clearPortfolioTargetPrice('005930');
    assert.equal(calls[0].path,'/api/portfolio/005930/metadata');
    assert.deepEqual(calls[0].body,{target_price:null,target_price_formula:null,target_price_disabled:true});
    row.querySelector('.js-pf-edit-qty').value = '4';
    await w.savePortfolioEdit('005930', undefined, row);
    assert.equal(calls[1].path,'/api/portfolio/005930');
    assert.equal(calls[1].body.quantity,4);
    assert.equal(w.PfStore.items[0].quantity,4);
  } finally {dom.window.close();}
});
