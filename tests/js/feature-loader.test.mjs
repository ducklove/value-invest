import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const source = readFileSync(new URL('../../static/js/feature-loader.js', import.meta.url), 'utf8');
function setup() {
  const dom = new JSDOM(`<script data-feature="detail" data-src="/js/first.js?v=test"></script>
    <script data-feature="detail" data-src="/js/second.js?v=test"></script>`,
  { runScripts: 'dangerously', url: 'https://app.example/' });
  const script = dom.window.document.createElement('script');
  script.textContent = source;
  dom.window.document.body.append(script);
  return dom;
}
const tick = () => new Promise(resolve => setImmediate(resolve));

test('요청 전에는 내려받지 않고 동시 진입도 의존 순서대로 한 번만 실행한다', async () => {
  const dom = setup();
  try {
    const w = dom.window;
    assert.equal(w.document.querySelectorAll('script[src]').length, 0);
    const first = w.loadFeatureScripts('detail');
    const second = w.loadFeatureScripts('detail');
    assert.equal(w.document.querySelectorAll('script[src]').length, 1);
    assert.equal(w.featureScriptsReady('detail'), false);
    assert.equal(w.document.querySelector('script[src]').src, 'https://app.example/js/first.js?v=test');
    w.document.querySelector('script[src]').onload();
    await tick();
    assert.equal(w.document.querySelectorAll('script[src]').length, 2);
    w.document.querySelectorAll('script[src]')[1].onload();
    await Promise.all([first, second]);
    await w.loadFeatureScripts('detail');
    assert.equal(w.featureScriptsReady('detail'), true);
    assert.equal(w.document.querySelectorAll('script[src]').length, 2);
  } finally { dom.window.close(); }
});

test('중간 파일 실패 후에는 성공한 앞 파일을 재실행하지 않고 이어서 재시도한다', async () => {
  const dom = setup();
  try {
    const w = dom.window;
    const failed = w.loadFeatureScripts('detail');
    w.document.querySelector('script[src]').onload();
    await tick();
    const rejection = assert.rejects(failed, /다시 눌러/);
    w.document.querySelectorAll('script[src]')[1].onerror();
    await rejection;
    assert.equal(w.featureScriptsReady('detail'), false);
    const retry = w.loadFeatureScripts('detail');
    await tick();
    assert.equal(w.document.querySelectorAll('script[src]').length, 2);
    w.document.querySelectorAll('script[src]')[1].onload();
    await retry;
    assert.equal(w.featureScriptsReady('detail'), true);
  } finally { dom.window.close(); }
});
