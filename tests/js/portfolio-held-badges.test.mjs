import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const source = readFileSync(new URL('../../static/js/portfolio-held-badges.js', import.meta.url), 'utf8');
const tick = () => new Promise(resolve => setTimeout(resolve, 0));
function setup(fetch, fragment = '') {
  const dom = new JSDOM(`<body><div id="list">
    <span data-portfolio-code="005930.KS" data-portfolio-price="75000">보통주</span>
    <span data-portfolio-code="005935.KS" data-portfolio-price="60000">우선주</span>
    <span data-portfolio-code="0131D0" data-portfolio-price="2000">스팩</span>
    <span data-portfolio-code="">평균</span></div></body>`, {
    url: 'https://ducklove.github.io/common_preferred_spread/?code=005935&theme=dark' + fragment, runScripts: 'dangerously',
  });
  dom.window.fetch = fetch;
  const script = dom.window.document.createElement('script');
  script.src = 'https://hub.example/js/portfolio-held-badges.js?v=1';
  Object.defineProperty(dom.window.document, 'currentScript', { value: script });
  dom.window.eval(source);
  return dom;
}
const badgeCodes = dom => [...dom.window.document.querySelectorAll('.portfolio-held-badge')]
  .map(badge => badge.parentElement.dataset.portfolioCode);

test('credentialed minimal API matches exact codes, including KRX letter codes; rerenders stay marked', async t => {
  const calls = [];
  const dom = setup(async (url, options) => {
    calls.push({ url, options });
    return { ok: true, json: async () => ({ codes: ['005930', '0131D0'] }) };
  });
  t.after(() => {
    dom.window.dispatchEvent(new dom.window.Event('pagehide'));
    dom.window.close();
  });
  await tick();
  assert.deepEqual(badgeCodes(dom), ['005930.KS', '0131D0']);
  assert.equal(calls[0].url, 'https://hub.example/api/portfolio/held-codes');
  assert.equal(calls[0].options.credentials, 'include');
  assert.equal(calls[0].options.cache, 'no-store');
  dom.window.document.getElementById('list').innerHTML = '<strong data-portfolio-code="0131D0" data-portfolio-price="2000">다른 목록</strong>';
  await tick();
  assert.deepEqual(badgeCodes(dom), ['0131D0']);
  dom.window.document.querySelector('strong').dataset.portfolioCode = '005935';
  await tick();
  assert.deepEqual(badgeCodes(dom), []);
});

test('logout, account change and network failure clear previous badges without persisting positions', async t => {
  let codes = ['005935'];
  let fails = false;
  const dom = setup(async () => {
    if (fails) throw new Error('offline');
    return { ok: true, json: async () => ({ codes }) };
  });
  t.after(() => {
    dom.window.dispatchEvent(new dom.window.Event('pagehide'));
    dom.window.close();
  });
  await tick();
  assert.deepEqual(badgeCodes(dom), ['005935.KS']);
  codes = [];
  dom.window.dispatchEvent(new dom.window.Event('focus'));
  await tick();
  assert.deepEqual(badgeCodes(dom), []);
  codes = ['0131D0'];
  dom.window.dispatchEvent(new dom.window.Event('focus'));
  await tick();
  assert.deepEqual(badgeCodes(dom), ['0131D0']);
  fails = true;
  dom.window.dispatchEvent(new dom.window.Event('focus'));
  await tick();
  assert.deepEqual(badgeCodes(dom), []);
  assert.equal(dom.window.localStorage.length, 0);
});

test('an old in-flight response cannot restore another users badges', async t => {
  let resolveOld;
  let calls = 0;
  const dom = setup(() => {
    if (++calls === 1) return new Promise(resolve => { resolveOld = resolve; });
    return Promise.resolve({ ok: true, json: async () => ({ codes: [] }) });
  });
  t.after(() => {
    dom.window.dispatchEvent(new dom.window.Event('pagehide'));
    dom.window.close();
  });
  dom.window.dispatchEvent(new dom.window.Event('focus'));
  await tick();
  resolveOld({ ok: true, json: async () => ({ codes: ['005930'] }) });
  await tick();
  assert.deepEqual(badgeCodes(dom), []);
});


test('fragment handoff works with blocked cross-site cookies and is immediately removed from the URL', async t => {
  let requests = 0;
  const dom = setup(async () => { requests++; throw new Error('third-party cookies blocked'); }, '#vc-held=005935%2C0131D0');
  t.after(() => {
    dom.window.dispatchEvent(new dom.window.Event('pagehide'));
    dom.window.close();
  });
  await tick();
  assert.deepEqual(badgeCodes(dom), ['005935.KS', '0131D0']);
  assert.equal(dom.window.location.hash, '');
  assert.equal(dom.window.location.search, '?code=005935&theme=dark');
  dom.window.dispatchEvent(new dom.window.Event('focus'));
  await tick();
  assert.deepEqual(badgeCodes(dom), ['005935.KS', '0131D0']);
  assert.equal(requests, 0);
  assert.equal(dom.window.localStorage.length, 0);
  assert.equal(dom.window.sessionStorage.length, 0);
});

test('an explicit empty snapshot never falls back to another session', async t => {
  const dom = setup(async () => { throw new Error('should not fetch'); }, '#vc-held=');
  t.after(() => {
    dom.window.dispatchEvent(new dom.window.Event('pagehide'));
    dom.window.close();
  });
  await tick();
  assert.deepEqual(badgeCodes(dom), []);
  assert.equal(dom.window.location.hash, '');
});


test('snapshot quantities produce formatted tooltips and valuation follows live displayed prices', async t => {
  const dom = setup(async () => { throw new Error('must not fetch'); }, '#vc-held=005930:1234.5,0131D0:50');
  t.after(() => { dom.window.dispatchEvent(new dom.window.Event('pagehide')); dom.window.close(); });
  await tick();
  const label = dom.window.document.querySelector('[data-portfolio-code="005930.KS"]');
  const badge = label.querySelector('.portfolio-held-badge');
  assert.equal(badge.title, '보유수량: 1,234.5주\n평가액: 92,587,500원\n화면 현재가 기준 · 수량은 링크를 연 시점 기준');
  assert.ok(badge.getAttribute('aria-label').includes('92,587,500원'));
  assert.equal(dom.window.location.hash, '');
  label.dataset.portfolioPrice = '80000';
  await tick();
  assert.ok(badge.title.includes('98,760,000원'));
  label.removeAttribute('data-portfolio-price');
  await tick();
  assert.ok(badge.title.includes('평가액: 확인 불가'));
  assert.equal(label.querySelectorAll('.portfolio-held-badge').length, 1);
});

test('API quantities update on refresh, while legacy links and invalid data never fabricate zero valuations', async t => {
  let quantity = 2;
  const dom = setup(async () => ({ok: true, json: async () => ({codes: ['005930'], quantities: {'005930': quantity}})}));
  t.after(() => { dom.window.dispatchEvent(new dom.window.Event('pagehide')); dom.window.close(); });
  await tick();
  assert.ok(dom.window.document.querySelector('.portfolio-held-badge').title.includes('평가액: 150,000원'));
  quantity = 3;
  dom.window.dispatchEvent(new dom.window.Event('focus'));
  await tick();
  assert.ok(dom.window.document.querySelector('.portfolio-held-badge').title.includes('평가액: 225,000원'));
  for (const fragment of ['#vc-held=005930', '#vc-held=005930:-1', '#vc-held=005930:Infinity']) {
    const legacy = setup(async () => { throw new Error('must not fetch'); }, fragment);
    await tick();
    assert.ok(legacy.window.document.querySelector('.portfolio-held-badge').title.includes('보유수량: 확인 불가\n평가액: 확인 불가'));
    legacy.window.dispatchEvent(new legacy.window.Event('pagehide'));
    legacy.window.close();
  }
});

test('ETF aliases sum quantities, preserve markets, and update native currency valuation', async t => {
  const dom = setup(() => { throw new Error('snapshot must not fetch'); },
    '#vc-held=SPY:2.5,SPY.US:1,SCHP.K:4,1570.T:3');
  t.after(() => dom.window.close());
  const list = dom.window.document.getElementById('list');
  list.innerHTML = `<span data-portfolio-code="SPY" data-portfolio-aliases="SPY,SPY.US" data-portfolio-price="500.25" data-portfolio-currency="USD"></span>
    <span data-portfolio-code="SCHP" data-portfolio-aliases="SCHP.K" data-portfolio-price="20" data-portfolio-currency="USD"></span>
    <span data-portfolio-code="1570.T" data-portfolio-price="69540" data-portfolio-currency="JPY"></span>
    <span data-portfolio-code="1570.HK" data-portfolio-price="10" data-portfolio-currency="HKD"></span>`;
  await tick();
  const labels = list.children;
  assert.match(labels[0].textContent, /보유/);
  assert.match(labels[0].firstElementChild.title, /보유수량: 3.5주\n평가액: 1,750.88 USD/);
  assert.match(labels[1].firstElementChild.title, /보유수량: 4주\n평가액: 80 USD/);
  assert.match(labels[2].firstElementChild.title, /평가액: 208,620 JPY/);
  assert.equal(labels[3].children.length, 0);
  labels[0].dataset.portfolioPrice = '600';
  await tick();
  assert.match(labels[0].firstElementChild.title, /평가액: 2,100 USD/);
  labels[0].dataset.portfolioCurrency = 'UNKNOWN';
  await tick();
  assert.match(labels[0].firstElementChild.title, /평가액: 확인 불가/);
  assert.equal(dom.window.location.hash, '');
});
