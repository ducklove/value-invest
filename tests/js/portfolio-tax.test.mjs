import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { JSDOM } from 'jsdom';

function setup() {
  const dom = new JSDOM(readFileSync(new URL('../../static/index.html', import.meta.url), 'utf8'), { runScripts: 'outside-only', url: 'http://localhost' });
  const win = dom.window;
  win.escapeHtml = value => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
  win.eval(readFileSync(new URL('../../static/js/portfolio-tax.js', import.meta.url), 'utf8'));
  return dom;
}

function payload(overrides = {}) {
  return {
    available: true, as_of: '2026-11-08', year: 2026, fields_verified: false,
    current: {
      year: 2026, tax_status: 'estimated', rules_stale: false,
      tax: { deduction_krw: 2500000, deduction_used_krw: 2500000, deduction_remaining_krw: 0, taxable_krw: 7279000, rate_pct: 22, estimated_tax_krw: 1601380 },
      overseas: { status: 'ok', realized_krw: 9779000, rows: 3, excluded_rows: 0, review_rows: 0, pending_rows: 0, total_gap_krw: 0, by_currency: [{ currency: 'USD', realized_native: 6985 }] },
      domestic: { status: 'ok', realized_krw: 70000, rows: 3 },
      stocks: [{ market: 'overseas', stock_code: 'AAPL', stock_name: '애플', currency: 'USD', realized_krw: 4191600, realized_native: 2994 },
        { market: 'domestic', stock_code: '005930', stock_name: '삼성전자', currency: 'KRW', realized_krw: 100000, realized_native: 100000 }],
    },
    years: [
      { year: 2026, tax_status: 'estimated', tax: { deduction_used_krw: 2500000, estimated_tax_krw: 1601380 }, overseas: { status: 'ok', realized_krw: 9779000, rows: 3 }, domestic: { status: 'ok', realized_krw: 70000, rows: 3 } },
      { year: 2025, tax_status: 'unavailable', tax: null, overseas: { status: 'pending', realized_krw: 0, rows: 0 }, domestic: { status: 'pending', realized_krw: 0, rows: 0 } },
    ],
    loss_harvest: { available: true, current_tax_krw: 1601380, max_saving_krw: 184800, loss_to_zero_tax_krw: 7279000, unpriced_count: 1, as_of: '2026-11-06',
      candidates: [{ stock_code: 'AAPL', stock_name: '애플', account_name: 'NH 위탁', unrealized_krw: -840000, tax_saving_krw: 184800 }] },
    financial_income: { year: 2026, threshold_krw: 20000000, dividends_krw: 13000000, interest_krw: 300000, received_krw: 13300000, upcoming_krw: 2900000, unscheduled_krw: 0,
      projected_krw: 16200000, ratio_pct: 81, status: 'near', review_rows: 1, unverified_rows: 0, interest_complete: true, interest_from: '2026-10-11', rules_stale: false,
      upcoming: { available: true, other_accounts_krw: 2900000, unscheduled: 0 } },
    coverage: { nh_accounts: ['NH 위탁'], excluded_accounts: [{ name: '수동 계좌', reason: 'not_nh' }, { name: 'ISA', reason: 'account_type' }], errors: [], last_success_at: '2026-11-08T20:00:00.000000' },
    notes: ['NH 손익은 NH 산식으로 계산한 값입니다.'],
    ...overrides,
  };
}

function render(dom, data) {
  const content = dom.window.document.getElementById('pfTaxContent');
  content.innerHTML = dom.window.pfTaxHtml(data);
  return content;
}

test('미검증 상태에서는 세액을 어림값으로만 보여 주고 신고용이 아님과 산식 차이를 밝힌다', () => {
  const dom = setup();
  const content = render(dom, payload());
  const text = content.textContent;
  assert.match(text, /신고용이 아닌 미검증 추정/);
  assert.match(text, /선입선출·취득 시 환율 기준은 다를 수 있습니다/);
  assert.match(text, /약 160만원/);
  assert.match(text, /미검증 추정/);
  assert.equal(text.includes('1,601,380'), false, '미검증 세액의 정확한 숫자를 표시하지 않는다');
  assert.equal(text.includes('9,779,000'), false, '미검증 해외 손익의 정확한 숫자를 표시하지 않는다');
  assert.equal(text.includes('확정 세액'), false);
  assert.match(content.querySelector('[role="note"]').textContent, /확정값이 아닙니다/);
  dom.window.close();
});

test('필드를 검증한 뒤에도 추정 표시는 유지하고 금액만 정확히 보여 준다', () => {
  const dom = setup();
  const text = render(dom, payload({ fields_verified: true })).textContent;
  assert.match(text, /1,601,380원/);
  assert.match(text, /신고용이 아닌 추정/);
  assert.match(text, /추정 세액/);
  assert.equal(text.includes('미검증'), false);
  assert.match(text, /취득 시 환율이 반영되지만/);
  dom.window.close();
});

test('NH 외 계좌와 다른 유형 계좌를 미포함으로 표시한다', () => {
  const dom = setup();
  const coverage = render(dom, payload()).querySelector('.pf-tax-coverage').textContent;
  assert.match(coverage, /포함: NH 주식 계좌 1개 \(NH 위탁\)/);
  assert.match(coverage, /미포함: NH 외 계좌 1개 \(수동 계좌\)/);
  assert.match(coverage, /일반 외 유형 계좌 ISA/);
  const none = render(dom, payload({ coverage: { nh_accounts: ['NH 위탁'], excluded_accounts: [], errors: [] } })).querySelector('.pf-tax-coverage').textContent;
  assert.match(none, /NH 외 계좌의 손익·배당은 계산하지 않습니다/);
  dom.window.close();
});

test('금융소득 미터는 받은 금액과 예정을 기준 대비로 그리고 상태를 글자로도 표시한다', () => {
  const dom = setup();
  const content = render(dom, payload());
  const income = content.querySelector('.pf-tax-income');
  assert.equal(income.classList.contains('near'), true);
  assert.equal(income.querySelector('.pf-tax-status').textContent, '근접');
  assert.equal(income.querySelector('.received').style.width, '66.5%');
  assert.equal(income.querySelector('.upcoming').style.width, '14.5%');
  assert.equal(income.querySelector('.pf-tax-meter-limit').style.left, '100%');
  assert.match(income.querySelector('.pf-tax-meter').getAttribute('aria-label'), /81%, 근접/);
  assert.match(income.textContent, /13,000,000원/);
  assert.match(income.textContent, /약 1,620만원/);
  assert.match(income.textContent, /확인 필요로 표시된 NH 배당 1건/);
  assert.match(income.textContent, /NH 외 계좌 몫의 예정 배당 약 290만원/);
  dom.window.close();
});

test('2,000만원을 넘으면 기준 눈금이 막대 안으로 들어와 초과를 보여 준다', () => {
  const dom = setup();
  const income = render(dom, payload({ financial_income: { ...payload().financial_income, received_krw: 22000000, dividends_krw: 22000000, upcoming_krw: 3000000, projected_krw: 25000000, ratio_pct: 125, status: 'over' } })).querySelector('.pf-tax-income');
  assert.equal(income.classList.contains('over'), true);
  assert.equal(income.querySelector('.pf-tax-status').textContent, '초과 예상');
  assert.equal(income.querySelector('.received').style.width, '88%');
  assert.equal(income.querySelector('.upcoming').style.width, '12%');
  assert.equal(income.querySelector('.pf-tax-meter-limit').style.left, '80%');
  dom.window.close();
});

test('이자 수집 범위와 일정 실패를 숨기지 않는다', () => {
  const dom = setup();
  const base = payload().financial_income;
  const text = render(dom, payload({ financial_income: { ...base, interest_complete: false, upcoming: { available: false, unscheduled: 2, unscheduled_krw: 500000 } } })).textContent;
  assert.match(text, /이자는 2026-10-11 이후 확인된 금액만 포함/);
  assert.match(text, /배당 일정을 불러오지 못해/);
  assert.match(text, /지급일을 모르는 일정 2건\(약 50만원\)/);
  dom.window.close();
});

test('수집 전과 실패 상태에서는 세액을 0원으로 보이지 않는다', () => {
  const dom = setup();
  const pending = payload({ current: { year: 2026, tax: null, tax_status: 'unavailable', overseas: { status: 'pending', realized_krw: 0, rows: 0 }, domestic: { status: 'pending', realized_krw: 0, rows: 0 } }, loss_harvest: { available: false } });
  const content = render(dom, pending);
  const tiles = content.querySelector('.pf-tax-tiles');
  assert.equal(tiles.textContent.includes('0원'), false);
  assert.match(tiles.textContent, /NH 조회 전/);
  assert.match(content.textContent, /아직 불러오지 못했습니다/);
  assert.equal([...content.querySelectorAll('summary')].some(item => item.textContent.includes('손실 종목')), false);
  const failed = payload({
    current: { ...pending.current, overseas: { status: 'failed', realized_krw: 0, rows: 0 } }, loss_harvest: { available: false },
    coverage: { nh_accounts: ['NH 위탁'], excluded_accounts: [], errors: [{ account_name: 'NH 위탁', year: 2026, market: 'overseas', message: 'NH 조회 실패', last_success_at: null }] },
  });
  const failedText = render(dom, failed).textContent;
  assert.match(failedText, /NH 조회 실패/);
  assert.match(failedText, /NH 위탁 2026년 해외 조회 보류: NH 조회 실패/);
  dom.window.close();
});

test('NH 주식 계좌가 없거나 명세 밖 시장을 보유하면 그대로 알린다', () => {
  const dom = setup();
  const base = payload();
  const none = render(dom, payload({ current: { year: 2026, tax: null, tax_status: 'unavailable', overseas: { status: 'no_account', realized_krw: 0, rows: 0 }, domestic: { status: 'no_account', realized_krw: 0, rows: 0 } },
    years: [{ year: 2026, tax: null, tax_status: 'unavailable', overseas: { status: 'no_account', realized_krw: 0, rows: 0 }, domestic: { status: 'no_account', realized_krw: 0, rows: 0 } }], loss_harvest: { available: false } }));
  assert.match(none.querySelector('.pf-tax-tiles').textContent, /NH 주식 계좌 없음/);
  assert.equal(none.textContent.includes('아직 불러오지 못했습니다'), false);
  assert.deepEqual([...none.querySelector('.pf-tax-table tbody tr').children].map(cell => cell.textContent), ['2026년', '—', '—', '—', '—']);
  const failed = render(dom, payload({ years: [{ ...base.years[0], domestic: { status: 'failed', realized_krw: 0, rows: 0 } }] }));
  assert.equal(failed.querySelector('.pf-tax-table tbody tr').children[1].textContent, '—', '조회에 실패한 시장을 0원으로 보이지 않는다');
  const unlisted = render(dom, payload({ unlisted_currencies: ['AUD', 'EUR'] }));
  assert.match(unlisted.textContent, /AUD·EUR 보유 종목의 매매는 NH 손익 명세의 국가 목록에 없어 누락될 수 있습니다/);
  dom.window.close();
});

test('액션 보드의 세금 항목은 간편 모드를 풀고 심층 분석 카드로 이동한다', async () => {
  const dom = new JSDOM(`<!doctype html><body class="pf-mobile-simple"><a href="#pfTaxWrap" data-pf-action-open="1" id="open">열기</a><section id="pfTaxWrap"></section></body>`, { runScripts: 'outside-only', url: 'http://localhost' });
  const win = dom.window;
  const calls = [];
  Object.assign(win, {
    escapeHtml: value => String(value ?? ''),
    safeStorageGet: () => null, safeStorageSet: () => {}, safeStorageRemove: () => {},
    PfStore: {}, pfSwitchTab: async tab => { calls.push(`tab:${tab}`); },
    pfToggleSimpleMode: () => { calls.push('simple'); },
  });
  win.Element.prototype.scrollIntoView = function scroll() { calls.push(`scroll:${this.id}`); };
  win.eval(readFileSync(new URL('../../static/js/portfolio-action-board.js', import.meta.url), 'utf8'));
  await new Promise(resolve => setTimeout(resolve, 0)); // DOMContentLoaded에서 클릭 위임을 연결한다.
  win.document.getElementById('open').click();
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.deepEqual(calls, ['simple', 'tab:performance', 'scroll:pfTaxWrap']);
  dom.window.close();
});

test('일부 누락·검산 표시와 세법 미확인 연도를 알린다', () => {
  const dom = setup();
  const base = payload().current;
  const text = render(dom, payload({ current: { ...base, tax_status: 'incomplete', rules_stale: true, overseas: { ...base.overseas, status: 'partial', excluded_rows: 2, review_rows: 1, pending_rows: 4, total_gap_krw: -30000 } } })).textContent;
  assert.match(text, /일부 누락 · 미검증 추정/);
  assert.match(text, /일부 계좌만 반영/);
  assert.match(text, /원화 손익을 확인하지 못한 2건/);
  assert.match(text, /검산이 맞지 않거나 거래통화 응답이 없는 1건/);
  assert.match(text, /종목별 상세 4건/);
  assert.match(text, /NH 기간 합계가 약 3만원 차이/);
  assert.match(text, /2026년 세법은 아직 확인하지 않아/);
  dom.window.close();
});

test('연도별 표와 손실 매도 표는 입력 없이 계산 결과를 보여 준다', () => {
  const dom = setup();
  const content = render(dom, payload());
  const years = content.querySelector('.pf-tax-table');
  const rows = [...years.querySelectorAll('tbody tr')].map(row => [...row.children].map(cell => cell.textContent));
  assert.deepEqual(rows[0], ['2026년', '70,000원', '약 978만원', '약 250만원', '약 160만원']);
  assert.deepEqual(rows[1], ['2025년', '—', '—', '—', '—']);
  assert.equal(content.querySelectorAll('input,select,textarea,form,button').length, 0, '입력 요소가 없다');
  const harvest = [...content.querySelectorAll('details')].find(item => item.textContent.includes('손실 종목을 매도'));
  assert.match(harvest.textContent, /애플/);
  assert.match(harvest.textContent, /약 -84만원/);
  assert.match(harvest.textContent, /약 18만원/);
  assert.match(harvest.textContent, /2026-11-06 정산 기준/);
  assert.match(harvest.textContent, /평가 기준이 없는 1종목 제외/);
  const none = render(dom, payload({ loss_harvest: { available: true, current_tax_krw: 0, max_saving_krw: 0, candidates: [] } }));
  assert.match(none.textContent, /줄어드는 세액이 없습니다/);
  assert.match(none.textContent, /평가손실 종목이 없습니다/);
  dom.window.close();
});

test('서버 문자열을 이스케이프한다', () => {
  const dom = setup();
  const xss = '<img src=x onerror=alert(1)>';
  const base = payload();
  const content = render(dom, payload({
    notes: [xss],
    current: { ...base.current, stocks: [{ market: 'overseas', stock_code: 'X', stock_name: xss, currency: '<b>', realized_krw: 1, realized_native: 1 }] },
    loss_harvest: { ...base.loss_harvest, candidates: [{ stock_code: 'X', stock_name: xss, account_name: xss, unrealized_krw: -1, tax_saving_krw: 0 }] },
    coverage: { nh_accounts: [xss], excluded_accounts: [{ name: xss, reason: 'not_nh' }], errors: [{ account_name: xss, year: 2026, market: 'overseas', message: xss }] },
  }));
  assert.equal(content.querySelectorAll('img,b:not(.pf-tax-meter-limit)').length, 0);
  dom.window.close();
});

test('NH 연결이 없으면 안내만 보여 주고 금액을 만들지 않는다', () => {
  const dom = setup();
  const content = render(dom, { available: false, message: 'NH 실계좌를 연결하면 입력 없이 자동으로 계산합니다.', coverage: { nh_accounts: [], excluded_accounts: [], errors: [] } });
  assert.match(content.textContent, /NH 실계좌를 연결하면/);
  assert.match(content.textContent, /미포함/);
  assert.equal(content.querySelector('.pf-tax-tiles'), null);
  dom.window.close();
});

test('늦게 온 이전 응답이 최신 결과를 덮어쓰지 않고 오류를 구분한다', async () => {
  const dom = setup();
  const resolves = [];
  dom.window.apiFetchJson = () => new Promise(resolve => resolves.push(resolve));
  const old = dom.window.pfLoadTaxPanel();
  const current = dom.window.pfLoadTaxPanel();
  resolves[1]({ available: false, message: '최신 응답' }); await current;
  resolves[0]({ available: false, message: '이전 응답' }); await old;
  const content = dom.window.document.getElementById('pfTaxContent');
  assert.match(content.textContent, /최신 응답/);
  dom.window.apiFetchJson = async () => { const error = new Error('x'); error.status = 401; throw error; };
  await dom.window.pfLoadTaxPanel();
  assert.match(content.textContent, /로그인 후 이용/);
  dom.window.apiFetchJson = async () => { throw new Error('x'); };
  await dom.window.pfLoadTaxPanel();
  assert.match(content.textContent, /세금 추정을 불러오지 못했습니다/);
  dom.window.close();
});

test('만원 어림 표기는 서버 규칙과 같다', () => {
  const dom = setup();
  assert.equal(dom.window.pfTaxApprox(1650000), '약 165만원');
  assert.equal(dom.window.pfTaxApprox(-4400000), '약 -440만원');
  assert.equal(dom.window.pfTaxApprox(8349), '약 8,300원');
  assert.equal(dom.window.pfTaxApprox(0), '0원');
  assert.equal(dom.window.pfTaxApprox(null), '—');
  assert.equal(dom.window.pfTaxExact(1601380), '1,601,380원');
  dom.window.close();
});
