import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const read = name => readFileSync(new URL(`../../static/js/${name}`, import.meta.url), 'utf8');
const app = read('app-main.js');
const routes = read('portfolio-shell.js').match(/const PF_PATH_TO_VIEW = \{[\s\S]*?\n\};/)[0];

for (const [label, path, width, loggedIn, expected] of [
  ['데스크톱 기본 주소', '/', 1200, true, 'investing'],
  ['비로그인 모바일 기본 주소', '/', 390, false, 'investing'],
  ['로그인 모바일 기본 주소', '/', 390, true, 'portfolio'],
  ['투자정보 직접 진입', '/investing', 1200, true, 'investing'],
  ['포트폴리오 직접 진입', '/portfolio', 1200, true, 'portfolio'],
  ['종목 바로가기', '/?code=005930', 1200, true, 'analysis'],
]) {
  test(`${label}에서 초기 화면 로딩을 시작한다`, async () => {
    const dom = new JSDOM('', { url: `https://example.test${path}`, runScripts: 'outside-only' });
    const w = dom.window;
    const visits = [];
    const polls = [];
    let finish;
    const ready = new Promise(resolve => { finish = resolve; });
    Object.assign(w, {
      innerWidth: width,
      currentUser: loggedIn ? { id: 1 } : null,
      PfStore: { items: [], activeView: 'investing' },
      recentListItems: [], activeStockCode: null,
      QuoteManager: { connect() {}, updateSubscriptions() {} },
      initAuth: async () => {},
      switchView: (view, options) => {
        visits.push(view);
        assert.equal(options.skipHistory, true);
      },
      analyzeStock() {}, loadRecentList: async () => {},
      _mbLoadCatalog: async () => {}, _mbLoadCodes: async () => {},
      loadMarketSummary() {}, loadMarketTape() {}, loadDailyMarketBrief() {},
      _pollBenchmarkQuotes() {}, syncAuthState() {},
      schedulePoll: (name, fn, ms, options = {}) => {
        polls.push({ name, ms, options });
        return { cancel() {} };
      },
      trackEvent: () => finish(),
    });
    try {
      w.eval(routes + '\n' + app);
      await ready;
      assert.deepEqual(visits, [expected]);
      assert.equal(w.location.pathname + w.location.search, path);
      // 주기 갱신은 전부 가시성 인지 폴링(schedulePoll)으로 — 이름당 하나씩.
      assert.deepEqual(polls.map(p => [p.name, p.ms]), [
        ['mb.summary', 60_000], ['mb.tape', 45_000], ['pf.benchmarks', 60_000],
        ['pf.todayState', 300_000], ['wiki.stats', 300_000],
      ]);
      // 벤치마크 시세는 로그인 사용자 전용 API — 비로그인이면 틱을 건너뛴다.
      const benchmarks = polls.find(p => p.name === 'pf.benchmarks');
      assert.equal(benchmarks.options.when(), loggedIn);
    } finally {
      w.close();
    }
  });
}

// --- 딥링크: 최초 진입 ?focus, 뒤로/앞으로가기의 ?code / ?focus / ?view 복원 ---
function bootApp(path, extra = {}) {
  const dom = new JSDOM('', { url: `https://example.test${path}`, runScripts: 'outside-only' });
  const w = dom.window;
  const calls = [];
  let finish;
  const ready = new Promise(resolve => { finish = resolve; });
  Object.assign(w, {
    innerWidth: 1200, currentUser: { id: 1 },
    PfStore: { items: [], activeView: 'investing' },
    recentListItems: [], activeStockCode: null,
    QuoteManager: { connect() {}, updateSubscriptions() {} },
    initAuth: async () => {},
    switchView: (view) => calls.push(['view', view]),
    analyzeStock: (code) => calls.push(['analyze', code]),
    pfFocusHolding: (code) => calls.push(['focus', code]),
    loadBondsView: (opts) => calls.push(['bonds', opts && opts.view]),
    loadRecentList: async () => {},
    _mbLoadCatalog: async () => {}, _mbLoadCodes: async () => {},
    loadMarketSummary() {}, loadMarketTape() {}, loadDailyMarketBrief() {},
    _pollBenchmarkQuotes() {}, syncAuthState() {},
    trackEvent: () => finish(),
    ...extra,
  });
  w.eval(routes + '\n' + app);
  return { w, calls, ready };
}

test('/portfolio?focus=CODE 로 들어오면 포트폴리오를 열고 그 보유 행을 강조한다', async () => {
  const { w, calls, ready } = bootApp('/portfolio?focus=005930');
  try {
    await ready;
    assert.deepEqual(calls, [['view', 'portfolio'], ['focus', '005930']]);
  } finally { w.close(); }
});

test('popstate: /analysis?code 는 다른 종목일 때만 다시 분석하고, ?focus·?view 도 복원한다', async () => {
  const { w, calls, ready } = bootApp('/investing');
  try {
    await ready;
    calls.length = 0;
    const pop = (url) => { w.history.replaceState(null, '', url); w.dispatchEvent(new w.PopStateEvent('popstate')); };

    pop('/analysis?code=000670');
    assert.deepEqual(calls.splice(0), [['view', 'analysis'], ['analyze', '000670']]);

    w.eval("activeStockCode = '000670'");
    pop('/analysis?code=000670&from=holding_value');
    assert.deepEqual(calls.splice(0), [['view', 'analysis']], 'same stock → no re-analysis');

    pop('/?code=005930');
    assert.deepEqual(calls.splice(0), [['view', 'analysis'], ['analyze', '005930']]);

    pop('/portfolio?focus=035720');
    assert.deepEqual(calls.splice(0), [['view', 'portfolio'], ['focus', '035720']]);

    pop('/bonds?view=fx');
    assert.deepEqual(calls.splice(0), [['view', 'bonds'], ['bonds', 'fx']]);

    pop('/labs');
    assert.deepEqual(calls.splice(0), [['view', 'labs']]);
  } finally { w.close(); }
});

test('모바일 /portfolio?focus= 진입은 맨 위 고정(holdPageScrollTop)을 건너뛰어 강조 행 스크롤을 지킨다', async () => {
  for (const [path, expectHold] of [['/portfolio?focus=005930', false], ['/portfolio', true]]) {
    let held = 0;
    const { w, ready } = bootApp(path, {
      innerWidth: 390,
      isCompactMobileViewport: () => true,
      holdPageScrollTop: () => { held += 1; },
    });
    try {
      await ready;
      assert.equal(held > 0, expectHold, path);
    } finally { w.close(); }
  }
});
