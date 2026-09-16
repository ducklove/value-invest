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
      trackEvent: () => finish(),
    });
    try {
      w.eval(routes + '\n' + app);
      await ready;
      assert.deepEqual(visits, [expected]);
      assert.equal(w.location.pathname + w.location.search, path);
    } finally {
      w.close();
    }
  });
}
