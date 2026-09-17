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

test('가상 원장은 실제 체결과 구분하고 호가 평가 지연 및 청산 가격을 표시한다',async()=>{
  const calls=[],data={accounts:[],config:{enabled:true},progress:{state:'scanning'},rows:[],events:[],paper:{
    state:{enabled:true,started_at:1789600000,last_quote_at:1789600010,config:{capital:100000000,allocation_pct:10,max_positions:5,participation_pct:25,delay_seconds:1,timeout_seconds:10,exit_basis_bps:10,stop_loss_bps:100},total_costs:70,closed:1,wins:1,cancels:2},
    summary:{equity:100000010,realized_pnl:100,unrealized_pnl:-90,stale_positions:1,positions:[{row:{name:'<img src=x>',contract:'KTEST'},contracts:2,entry_spot:10000,entry_future:10300,mark:{at:1789600010,spot_bid:9990,future_ask:10310,net_pnl:-90}}]},
    events:[{type:'fill',action:'entry',at:1789600010,name:'검증',spot_price:10000,future_price:10300,contracts:2,shares:20,reason:'순우위',net_pnl:null}]
  }};
  const dom=setup(data,calls);await tick();
  const doc=dom.window.document;
  assert.match(doc.getElementById('paperStatus').textContent,/새 호가 확인 전/);
  assert.match(doc.getElementById('paperPositions').textContent,/9990.00 \/ 10310.00/);
  assert.match(doc.getElementById('paperLedger').textContent,/가상 진입/);
  assert.equal(doc.getElementById('paperStart').disabled,true);
  assert.equal(doc.querySelector('img'),null);
  dom.window.close();
});

test('가상 운용 시작은 실제 주문 API를 호출하지 않고 기존 원장 설정으로 재개한다',async()=>{
  const calls=[],data={accounts:[],config:{enabled:true},progress:{},rows:[],events:[],paper:null};
  const dom=setup(data,calls);await tick();
  await dom.window.paperAction(true);
  assert.equal(calls.at(-1).path,'/api/quant/paper/start');
  assert.equal(JSON.parse(calls.at(-1).options.body).capital,100000000);
  assert.match(dom.window.document.getElementById('paperMessage').textContent,/브라우저를 닫아도/);
  dom.window.close();
});
