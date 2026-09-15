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
    if (path.endsWith('capabilities')) return { pairs: [{common:'005930',preferred:'005935',name:'삼성전자우'}] };
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
