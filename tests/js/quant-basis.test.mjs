import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {JSDOM} from 'jsdom';

function setup(handler) {
  const dom = new JSDOM('<section id="quantBasis"></section>',{runScripts:'outside-only',url:'https://test.example/quant'});
  dom.window.escapeHtml = v => String(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
  dom.window.quantNumber = (v,n=2)=>Number.isFinite(v)?v.toFixed(n):'—';
  dom.window.apiFetchJson = handler;
  dom.window.eval(readFileSync(new URL('../../static/js/quant-basis.js',import.meta.url),'utf8'));
  dom.window.basisInit();
  return dom;
}

test('예시임을 밝히고 손익 가정을 보내며 재시도 키를 고정한다',async()=>{
  const calls=[];
  const dom=setup(async(path,options)=>{if(options){calls.push(JSON.parse(options.body));throw new Error('응답 유실');}return {runs:[]};});
  const f=dom.window.document.getElementById('basisForm');
  assert.match(dom.window.document.body.textContent,/실제 시세가 아닌 예시/);
  f.dispatchEvent(new dom.window.Event('submit',{cancelable:true,bubbles:true}));
  await new Promise(r=>setTimeout(r,20));
  f.dispatchEvent(new dom.window.Event('submit',{cancelable:true,bubbles:true}));
  await new Promise(r=>setTimeout(r,20));
  assert.equal(calls.length,2);
  assert.equal(calls[0].request_key,calls[1].request_key);
  assert.equal(calls[0].input.config.capital,100000000);
  assert.equal(calls[0].input.config.spot_fee_bps,1);
  assert.equal(calls[0].input.config.futures_fee_bps,0.6);
  assert.equal(calls[0].input.scenario.spot,70000);
  assert.equal(calls[0].input.config.borrow_confirmed,false);
  f.elements.kind.value='index_etf';f.elements.kind.dispatchEvent(new dom.window.Event('change'));
  assert.equal(f.elements.multiplier.value,'50000');
  assert.equal(f.elements.sell_tax_bps.value,'0');
  dom.window.close();
});

test('미청산 결과를 확정 수익으로 표시하지 않고 외부 텍스트를 이스케이프한다',async()=>{
  const dom=setup(async()=>({runs:[]}));
  dom.window.basisRender({id:'r',result:{mode:'scenario',config:{capital:100000000,contract:'<img src=x>',direction:'cash_carry'},scenarios:[{name:'기본',status:'unresolved',pnl:null,return_on_capital_pct:null,contracts:1,shares:10,max_drawdown_pct:-1,additional_capital:10000,plan:{blocked:null,edge_bps:10,net_per_contract:100},ledger:[],events:[],delta_residual_won:0,peak_capital_required:100010000}],limitations:[],engine_version:'cash-futures-1',input_hash:'a',result_hash:'b'}});
  const report=dom.window.document.getElementById('basisReport');
  assert.match(report.textContent,/청산 미확인/);
  assert.match(report.textContent,/—원/);
  assert.equal(report.querySelector('img'),null);
  await new Promise(r=>setTimeout(r,20));
  dom.window.close();
});
