import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const html = readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8');
const source = readFileSync(new URL('../../static/js/quant.js', import.meta.url), 'utf8');
const tick = () => new Promise(resolve => setTimeout(resolve, 20));

function setup(handler) {
  const dom = new JSDOM(html, { runScripts: 'outside-only', url: 'https://test.example/quant' });
  const w = dom.window;
  w.escapeHtml = value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;');
  w.apiFetchJson = handler;
  w.eval(source);
  return dom;
}

test('팩터 입력 검사는 후보 수와 제외 이유를 표시하고 결측을 0으로 바꾸지 않는다', () => {
  const dom = setup(async () => ({}));
  const html = dom.window.quantFactorInputs({status:'review_required',as_of:'2026-08-28',latest_price_date:'2026-09-15',securities:2581,eligible_securities:6,complete_securities:4,exclusions:{unknown_or_future_publication:5},note:'원공시 검증 전'});
  assert.match(html, /후보 4종목/);
  assert.match(html, /평가일 이후 공개: 5행/);
  assert.match(html, /기준일과 다름/);
  assert.match(dom.window.quantFactorInputs({status:'missing_fields'}), /필수 필드가 부족/);
  assert(!dom.window.quantFactorInputs({status:'missing_fields'}).includes('0종목'));
  dom.window.close();
});

test('자료 지연을 정상으로 표시하지 않고 서버 오류를 보여준다', async () => {
  const dom = setup(async path => {
    if (path.endsWith('capabilities')) return { pairs: [], error: '연결 실패', readiness: null };
    if (path.endsWith('observations')) return { watches: [], observations: [] };
    return { runs: [] };
  });
  await dom.window.loadQuant();
  assert.match(dom.window.document.getElementById('quantHealth').textContent, /연결 실패/);
  assert(dom.window.document.getElementById('quantHealth').classList.contains('quant-warning'));
  dom.window.close();
});

test('서버 응답 유실 후 같은 설정 재시도는 같은 요청 키를 유지한다', async () => {
  const posted = [];
  const dom = setup(async (path, options) => {
    if (options?.method === 'POST') { posted.push(JSON.parse(options.body)); throw new Error('응답 유실'); }
    if (path.endsWith('capabilities')) return { pairs: [{common:'005930',preferred:'005935',name:'삼성전자우',catalog_snapshot_id:'a'.repeat(64)}] };
    if (path.endsWith('observations')) return { watches: [], observations: [] };
    return { runs: [] };
  });
  const w = dom.window;
  await w.loadQuant();
  w.document.getElementById('quantPair').value = '005930:005935';
  const form = w.document.getElementById('quantForm');
  form.dispatchEvent(new w.Event('submit', { bubbles: true, cancelable: true }));
  await tick();
  form.dispatchEvent(new w.Event('submit', { bubbles: true, cancelable: true }));
  await tick();
  assert.equal(posted.length, 2);
  assert.equal(posted[0].request_key, posted[1].request_key);
  assert.equal(posted[0].config.participation, 0.01);
  assert.equal(posted[0].config.catalog_snapshot_id, 'a'.repeat(64));
  assert.equal(w.document.getElementById('quantSubmit').disabled, false);
  w.close();
});

test('늦게 도착한 과거 상세 응답이 새로 선택한 실험을 덮지 않는다', async () => {
  let oldResolve;
  const dom = setup(path => path.endsWith('/old') ? new Promise(r => { oldResolve = r; }) :
    Promise.resolve({status:'failed',error:'새 실험의 자료 부족',result:null}));
  const old = dom.window.quantOpen('old');
  await dom.window.quantOpen('new');
  oldResolve({status:'failed',error:'이전 실험',result:null});
  await old;
  assert.match(dom.window.document.getElementById('quantReport').textContent, /새 실험/);
  dom.window.close();
});

test('사용자에게 표시하는 서버 오류의 HTML을 실행하지 않는다', async () => {
  const dom = setup(async () => ({status:'failed',error:'<img src=x onerror="alert(1)">',result:null}));
  await dom.window.quantOpen('test');
  assert.equal(dom.window.document.querySelector('#quantReport img'), null);
  assert.match(dom.window.document.getElementById('quantReport').textContent, /<img/);
  dom.window.close();
});

test('ETF 선택은 별도 전략과 비용 가정을 보내며 기간 검증을 OOS로 표시하지 않는다', async () => {
  let posted;
  const dom = setup(async (path, options) => {
    if (options?.method === 'POST') { posted = JSON.parse(options.body); throw new Error('검증용 중단'); }
    if (path.endsWith('capabilities')) return { pairs: [], etf_pairs: [{common:'069500',preferred:'102110',name:'KODEX / TIGER'}] };
    if (path.endsWith('observations')) return { watches: [], observations: [] };
    return { runs: [] };
  });
  const w = dom.window;
  await w.loadQuant();
  const select = w.document.getElementById('quantPair');
  select.value = '069500:102110';
  select.dispatchEvent(new w.Event('change'));
  w.document.getElementById('quantForm').dispatchEvent(new w.Event('submit', {bubbles:true,cancelable:true}));
  await tick();
  assert.equal(posted.config.strategy, 'etf_switch');
  assert.equal(posted.config.sell_tax_bps, 0);
  assert.match(w.document.getElementById('quantPairHint').textContent, /iNAV는 미반영/);
  assert.match(w.quantValidation({validation:{status:'insufficient_data'}}), /189개/);
  assert.match(w.quantValidation({}), /이전 버전/);
  w.close();
});

test('전진 평가 대기는 수익을 만들지 않고 중지 기록은 다시 시작할 수 없다', () => {
  const dom = setup(async () => ({}));
  const row = {id:'sample', config:{strategy:'etf_switch'}, forward:{status:'active',start_date:'2026-09-17',error:'<script>실패</script>'}};
  const pending = dom.window.quantForward(row, true);
  assert.match(pending, /수익률은 아직 없습니다/);
  assert.match(pending, /stop-forward/);
  assert(!pending.includes('<script>'));
  row.forward.status = 'stopped';
  const stopped = dom.window.quantForward(row, true);
  assert(!stopped.includes('data-quant-action="forward"'));
  assert(!stopped.includes('stop-forward'));
  assert.match(stopped, /자료가 쌓이기 전에 중지/);
  assert(!stopped.includes('기다립니다'));
  dom.window.close();
});

test('전진 평가 시작과 중지 요청은 정확한 경로와 상태를 전달한다', async () => {
  const posted = [];
  const dom = setup(async (path, options) => {
    if (options?.method === 'POST') { posted.push({path,body:JSON.parse(options.body)}); return {}; }
    if (path.endsWith('capabilities')) return {pairs:[]};
    if (path.endsWith('observations')) return {watches:[],observations:[]};
    return {runs:[]};
  });
  await dom.window.loadQuant();
  const report = dom.window.document.getElementById('quantReport');
  for (const action of ['forward','stop-forward']) {
    report.innerHTML = `<button data-quant-action="${action}" data-id="sample">평가</button>`;
    report.querySelector('button').click();
    await tick();
  }
  assert.deepEqual(posted, [{path:'/api/quant/runs/sample/forward',body:{enabled:true}}, {path:'/api/quant/runs/sample/forward',body:{enabled:false}}]);
  dom.window.close();
});
