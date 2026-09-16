import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {JSDOM} from 'jsdom';

function setup(data, calls) {
  const dom=new JSDOM('<section id="quantView"><section id="quantScanner"></section></section>',{runScripts:'outside-only',url:'https://example.test/quant'});
  const w=dom.window;
  w.escapeHtml=v=>String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
  w.quantNumber=v=>Number.isFinite(v)?v.toFixed(2):'—';
  w.apiFetchJson=async(path,options)=>{calls.push({path,options});return data;};
  w.eval(readFileSync(new URL('../../static/js/quant-scanner.js',import.meta.url),'utf8'));
  w.scannerInit();
  return dom;
}
const tick=()=>new Promise(r=>setTimeout(r,20));

test('계좌 없는 경우 실제 카탈로그를 표시하지만 감시 시작은 차단한다',async()=>{
  const calls=[],dom=setup({accounts:[],progress:{total:292,source_contracts:1734},rows:[{contract:'KTEST0001',spot_code:'005930',name:'<img src=x>',expiry:'20261008',roll_on:'2026-10-06',error:'아직 관측하지 않음'}],excluded:[{spot_code:'000000',name:'<img src=x>',reason:'전환 가능한 다음 월물 없음'}],events:[]},calls);
  await tick();
  assert.equal(dom.window.document.getElementById('scannerStart').disabled,true);
  assert.match(dom.window.document.getElementById('scannerCoverage').textContent,/감시 대상 292계약 \/ 원본 목록 1734계약 · 관측 0계약 · 월물 선택 제외 1종목/);
  assert.match(dom.window.document.getElementById('scannerRows').textContent,/전환 2026-10-06/);
  assert.match(dom.window.document.getElementById('scannerRows').textContent,/예정 · 시세 응답 대조 전/);
  assert.equal(dom.window.document.getElementById('scannerExcluded').hidden,false);
  assert.match(dom.window.document.getElementById('scannerExcluded').textContent,/전환 가능한 다음 월물 없음/);
  assert.equal(dom.window.document.querySelector('img'),null);
  assert.equal(calls.length,1);
  dom.window.close();
});

test('잘못된 입력 중에도 정지는 별도 경로로 실행하며 계좌가 없어도 가능하다',async()=>{
  const calls=[],dom=setup({accounts:[],config:{enabled:true,account_id:'removed'},progress:{},rows:[],events:[]},calls);
  await tick();
  const f=dom.window.document.getElementById('scannerForm');
  f.elements.watch_bps.value='100';f.elements.signal_bps.value='0';
  await dom.window.scannerSave(false);
  assert.equal(calls.at(-1).path,'/api/quant/scanner/stop');
  assert.equal(calls.at(-1).options.method,'POST');
  dom.window.close();
});
