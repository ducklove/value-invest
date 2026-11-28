import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const htmlSource = readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8');
const utilsSource = readFileSync(new URL('../../static/js/utils.js', import.meta.url), 'utf8');
const ghostSource = readFileSync(new URL('../../static/js/portfolio-ghost.js', import.meta.url), 'utf8');

function setup() {
  const dom = new JSDOM(htmlSource, { runScripts: 'outside-only', url: 'http://localhost/portfolio' });
  const win = dom.window;
  win.safeStorageGet = () => null;
  win.safeStorageSet = () => {};
  win.safeStorageRemove = () => {};
  win.PfStore = { currency: { unit: 'KRW' } };
  win.eval(utilsSource);
  win.PortfolioTrendChart = {
    created: [],
    create(container, option) {
      const inst = { container, option, disposed: false, resize() {}, on() {}, dispose() { this.disposed = true; } };
      this.created.push(inst);
      container.innerHTML = '<canvas class="mock-ghost-canvas"></canvas>';
      return inst;
    },
  };
  win.eval(ghostSource);
  return dom;
}

function samplePayload(overrides = {}) {
  return {
    available: true,
    as_of: '2026-11-28',
    requested_since: null,
    effective_since: '2026-01-01',
    baseline_date: '2025-12-30',
    baseline_mode: 'year_start',
    baseline_note: null,
    ending_date: '2026-11-27',
    ghost: {
      available: true,
      scope: 'complete',
      reason: null,
      baseline_value: 200000000,
      baseline_cash_krw: 20000000,
      net_cashflow_krw: 10000000,
      actual_end_value: 238400000,
      ghost_end_value: 233600000,
      actual_return_pct: 14.2,
      ghost_return_pct: 11.8,
      effect_krw: 4800000,
      effect_pct_p: 2.4,
      coverage: {
        total_holdings: 4,
        calculable_holdings: 4,
        uncalculated_holdings: 0,
        cash_weight_pct: 10.0,
        calculable_weight_pct: 100.0,
        uncalculated_weight_pct: 0.0,
      },
      holdings: [],
      uncalculated: [],
      series: [
        { date: '2025-12-30', actual_value: 200000000, ghost_value: 200000000, net_cashflow_cum: 0, diff_krw: 0, fx_usdkrw: 1400 },
        { date: '2026-11-27', actual_value: 238400000, ghost_value: 233600000, net_cashflow_cum: 10000000, diff_krw: 4800000, fx_usdkrw: 1410 },
      ],
      assumptions: ['기준일 보유 수량 고정 가정'],
    },
    post_sale: {
      since: '2026-01-01',
      summary: {
        total_count: 3,
        calculable_any_count: 2,
        uncalculated_sale_count: 0,
        matured_5d_count: 2,
        calculable_5d_count: 2,
        matured_20d_count: 2,
        calculable_20d_count: 1,
        uncalculated_20d_count: 1,
        pending_20d_count: 1,
        avoided_drop_20d_count: 1,
        rose_after_20d_count: 0,
        flat_20d_count: 0,
      },
      items: [
        {
          trade_date: '2026-05-12',
          matched_bar_date: '2026-05-12',
          stock_code: '005930',
          stock_name: '삼성전자',
          currency: 'KRW',
          source: 'nh_realized_pnl',
          source_label: 'NH 실현손익',
          sale_type: 'closed',
          sale_type_label: '전량',
          price_basis: 'execution',
          date_is_recorded_at: false,
          date_note: null,
          quantity: 100,
          sell_price: 80000,
          sell_amount: 8000000,
          status: 'ok',
          reason: null,
          day5: { status: 'ok', date: '2026-05-19', price: 77000, change_pct: -3.75, diff_amount: -300000 },
          day20: { status: 'ok', date: '2026-06-10', price: 74000, change_pct: -7.5, diff_amount: -600000, outcome: 'avoided_drop', outcome_label: '하락 회피' },
          current: { status: 'ok', date: '2026-11-27', price: 72000, change_pct: -10.0, diff_amount: -800000 },
        },
        {
          trade_date: '2026-06-20',
          matched_bar_date: '2026-06-20',
          stock_code: '000660',
          stock_name: 'SK하이닉스',
          currency: 'KRW',
          source: 'manual_trade',
          source_label: '수동 매매',
          sale_type: 'partial',
          sale_type_label: '일부',
          price_basis: 'execution',
          date_is_recorded_at: true,
          date_note: '기록일 기준(체결일과 다를 수 있음)',
          quantity: 10,
          sell_price: 200000,
          sell_amount: 2000000,
          status: 'ok',
          reason: null,
          day5: { status: 'ok', date: '2026-06-27', price: 210000, change_pct: 5.0, diff_amount: 100000 },
          day20: { status: 'uncalculated', reason: '배당락 구간' },
          current: { status: 'uncalculated', reason: '배당락 구간' },
        },
        {
          trade_date: '2026-11-24',
          matched_bar_date: '2026-11-24',
          stock_code: '005380',
          stock_name: '현대차',
          currency: 'KRW',
          source: 'snapshot_disappearance',
          source_label: '스냅샷 소멸',
          sale_type: 'closed',
          sale_type_label: '전량',
          price_basis: 'close_estimate',
          date_is_recorded_at: false,
          date_note: null,
          quantity: 5,
          sell_price: 250000,
          sell_amount: 1250000,
          status: 'pending',
          reason: '20거래일 미도래 (3/20일)',
          day5: { status: 'pending', reason: '5거래일 미도래 (3/5일)' },
          day20: { status: 'pending', reason: '20거래일 미도래 (3/20일)' },
          current: { status: 'ok', date: '2026-11-27', price: 255000, change_pct: 2.0, diff_amount: 25000 },
        },
      ],
      notes: ['세전 주가 기준 비교입니다.'],
    },
    ...overrides,
  };
}

test('유령 포트폴리오 요약·계산 범위·두 선 차트·NH 일반 계좌 문구·기록일 주석과 종목 링크를 렌더링한다', async () => {
  const dom = setup();
  try {
    dom.window.apiFetchJson = async () => samplePayload();
    await dom.window.pfLoadGhostPanel();

    const content = dom.window.document.getElementById('pfGhostContent');
    const text = content.textContent;
    assert.match(text, /올해 매매 효과 \(계산 범위\(기준일 비중 100\.0%\) 기준\)/);
    assert.match(text, /\+2\.40%p \(\+480만원\)/);
    assert.match(text, /현금 비례 가정/);
    assert.match(text, /NH 일반 계좌 원장 기준\(ISA·연금 등 제외\)/);
    assert.match(text, /기록일 기준\(체결일과 다를 수 있음\)/);
    assert.match(text, /전체 매도 3건 · 계산 가능 2건 · 20거래일 만기 2건\(계산 가능 1건 · 미계산 1건\) · 미도래 1건/);

    assert.equal(dom.window.PortfolioTrendChart.created.length, 1);
    const chartOpt = dom.window.PortfolioTrendChart.created[0].option;
    assert.deepEqual(Array.from(chartOpt.series, s => s.name), ['실제 평가액', '방치 평가액']);
    assert.equal(chartOpt.series[1].lineStyle.type, 'dashed');

    const links = [...content.querySelectorAll('a.vc-stock-link')].map(a => a.getAttribute('href'));
    assert.deepEqual(links, ['/analysis?code=005930', '/analysis?code=000660', '/analysis?code=005380']);

    assert.match(text, /NH 실현손익/);
    assert.match(text, /수동 매매/);
    assert.match(text, /스냅샷 소멸/);
    assert.match(text, /종가 기준/);
    assert.match(text, /하락 회피/);
    assert.match(text, /배당락 구간/);
    assert.match(text, /20거래일 미도래 \(3\/20일\)/);
  } finally {
    dom.window.close();
  }
});

test('계산 범위 한정 비교 시 기준일 비중을 헤드라인에 붙이고 50% 미만이면 헤드라인 수치를 내지 않는다', async () => {
  const dom = setup();
  try {
    const base = samplePayload();
    const partial = samplePayload({
      ghost: {
        ...base.ghost,
        scope: 'partial',
        coverage: { ...base.ghost.coverage, uncalculated_holdings: 1, calculable_weight_pct: 85.0, uncalculated_weight_pct: 15.0 },
        uncalculated: [
          { stock_code: 'CRYPTO_BTC', stock_name: '비트코인', quantity: 0.2, baseline_value: 30000000, baseline_weight_pct: 15.0, reason: '특수자산 과거 종가 미제공' },
        ],
      },
    });
    dom.window.apiFetchJson = async () => partial;
    await dom.window.pfLoadGhostPanel({ force: true });

    const content = dom.window.document.getElementById('pfGhostContent');
    assert.match(content.textContent, /올해 매매 효과 \(계산 범위\(기준일 비중 85\.0%\) 기준\)/);
    assert.match(content.textContent, /실제 평가액 \(계산 범위\)/);
    assert.match(content.textContent, /비트코인/);
    assert.match(content.textContent, /15\.00%/);
    assert.match(content.textContent, /특수자산 과거 종가 미제공/);

    // 계산 가능 비중 < 50% (미계산)일 때는 가짜 수치를 내지 않고 사유를 표시한다.
    const lowWeight = samplePayload({
      ghost: {
        ...partial.ghost,
        scope: 'uncalculated',
        reason: '계산 가능 비중 부족 (기준일 계산 가능 비중 40.0% < 50%)',
        actual_end_value: null,
        ghost_end_value: null,
        effect_krw: null,
        effect_pct_p: null,
      },
    });
    dom.window.apiFetchJson = async () => lowWeight;
    await dom.window.pfLoadGhostPanel({ force: true });
    assert.match(content.textContent, /올해 매매 효과: 미계산/);
    assert.match(content.textContent, /계산 가능 비중 부족/);
    assert.equal(content.textContent.includes('+480만원'), false);
    assert.equal(content.querySelector('#pfGhostChart'), null);
  } finally {
    dom.window.close();
  }
});

test('기간 버튼 전환 시 since 쿼리를 전달하고 이전 지연 응답·XSS를 방어한다', async () => {
  const dom = setup();
  try {
    const urls = [];
    const resolvers = [];
    dom.window.apiFetchJson = url => {
      urls.push(url);
      return new Promise(resolve => resolvers.push(resolve));
    };

    const p1 = dom.window.pfLoadGhostPanel({ force: true });
    dom.window.pfGhostSetWindow('3m');
    assert.equal(urls[0], '/api/portfolio/ghost');
    assert.match(urls[1], /^\/api\/portfolio\/ghost\?since=\d{4}-\d{2}-\d{2}$/);

    const xss = '<img src=x onerror=alert(1)>';
    resolvers[1](samplePayload({ requested_since: '2026-08-28', baseline_note: xss }));
    await new Promise(r => setTimeout(r, 0));
    resolvers[0](samplePayload({ baseline_note: '오래된 응답' }));
    await p1;

    const content = dom.window.document.getElementById('pfGhostContent');
    assert.equal(content.textContent.includes('오래된 응답'), false);
    assert.equal(content.querySelectorAll('img').length, 0);
  } finally {
    dom.window.close();
  }
});
