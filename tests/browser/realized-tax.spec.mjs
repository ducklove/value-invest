import { test, expect } from '@playwright/test';

async function login(page) {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page).toHaveURL(/\/portfolio$/);
}

// 공개 명세 형태의 합성 요약. 실제 계좌·금액이 아니다.
const SUMMARY = {
  available: true, as_of: '2026-11-08', year: 2026, fields_verified: false, unlisted_currencies: ['AUD'],
  current: {
    year: 2026, tax_status: 'estimated', rules_stale: false,
    tax: { deduction_krw: 2500000, deduction_used_krw: 2500000, deduction_remaining_krw: 0, taxable_krw: 7279000, rate_pct: 22, estimated_tax_krw: 1601380 },
    overseas: { status: 'ok', realized_krw: 9779000, rows: 3, excluded_rows: 0, review_rows: 0, pending_rows: 0, total_gap_krw: 0, by_currency: [{ currency: 'USD', realized_native: 6985 }] },
    domestic: { status: 'ok', realized_krw: 70000, rows: 3 },
    stocks: [{ market: 'overseas', stock_code: 'AAA', stock_name: '합성 해외주식', currency: 'USD', realized_krw: 4191600, realized_native: 2994 }],
  },
  years: [{ year: 2026, tax_status: 'estimated', tax: { deduction_used_krw: 2500000, estimated_tax_krw: 1601380 }, overseas: { status: 'ok', realized_krw: 9779000, rows: 3 }, domestic: { status: 'ok', realized_krw: 70000, rows: 3 } }],
  loss_harvest: { available: true, current_tax_krw: 1601380, max_saving_krw: 184800, loss_to_zero_tax_krw: 7279000, unpriced_count: 0, as_of: '2026-11-06',
    candidates: [{ stock_code: 'BBB', stock_name: '합성 손실주식', account_name: 'NH 위탁', unrealized_krw: -840000, tax_saving_krw: 184800 }] },
  financial_income: { year: 2026, threshold_krw: 20000000, dividends_krw: 13000000, interest_krw: 300000, received_krw: 13300000, upcoming_krw: 2900000, unscheduled_krw: 0,
    projected_krw: 16200000, ratio_pct: 81, status: 'near', review_rows: 0, unverified_rows: 0, interest_complete: true, interest_from: null, rules_stale: false, upcoming: { available: true, other_accounts_krw: 0, unscheduled: 0 } },
  coverage: { nh_accounts: ['NH 위탁'], excluded_accounts: [{ name: '수동 계좌', reason: 'not_nh' }], errors: [], last_success_at: '2026-11-08T20:00:00.000000' },
  notes: ['신고용이 아닌 추정입니다.'],
};

test('세금 카드는 심층 분석에서 지연 로드되고 NH 연결 전에는 금액을 만들지 않는다', async ({ page }) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await login(page);
  expect(await page.locator('script[src*="portfolio-tax.js"]').count()).toBe(0);
  await page.locator('.pf-tab[data-tab="performance"]').click();
  await expect(page.locator('script[src*="portfolio-tax.js"]')).toHaveCount(1);
  // 실제 라우트·임시 DB: 연결 계좌가 없는 사용자.
  await expect(page.locator('#pfTaxContent')).toContainText('NH 실계좌를 연결하면 입력 없이 자동으로 계산합니다');
  await expect(page.locator('#pfTaxContent')).toContainText('NH 외 계좌');
  await expect(page.locator('#pfTaxContent .pf-tax-tiles')).toHaveCount(0);
  await expect(page.locator('#pfTaxWrap input, #pfTaxWrap form')).toHaveCount(0);
  expect(errors).toEqual([]);
});

test('세금 카드는 미검증 세액을 어림값으로 보여 주고 좁은 화면에서도 넘치지 않는다', async ({ page }) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await login(page);
  // 나중에 등록한 라우트가 먼저 적용된다.
  await page.route('**/api/portfolio/realized-tax', route => route.fulfill({ json: SUMMARY }));
  await page.locator('.pf-tab[data-tab="performance"]').click();
  const content = page.locator('#pfTaxContent');
  await expect(content).toContainText('약 160만원');
  await expect(content).toContainText('신고용이 아닌 미검증 추정');
  await expect(content).toContainText('선입선출·취득 시 환율 기준');
  await expect(content).toContainText('미포함: NH 외 계좌 1개 (수동 계좌)');
  await expect(content).toContainText('AUD 보유 종목의 매매는');
  await expect(content).not.toContainText('1,601,380');
  // 점프 내비는 보고 있는 묶음만 펼친다. 묶음을 먼저 누른 뒤 카드로 간다.
  await page.locator('.pf-deep-nav-section[href="#pfDeepIncome"]').click();
  await page.locator('.pf-deep-anchor-link[href="#pfTaxWrap"]').click();
  await expect(page.locator('#pfTaxTitle')).toBeInViewport();

  await page.setViewportSize({ width: 390, height: 844 });
  await page.reload();
  const toggle = page.locator('#pfSimpleToggle');
  if (await toggle.isVisible() && await page.evaluate(() => document.body.classList.contains('pf-mobile-simple'))) await toggle.click();
  await page.locator('.pf-tab[data-tab="performance"]').click();
  await expect(content).toContainText('약 160만원');
  const overflow = await page.locator('#pfTaxWrap').evaluate(card => {
    const box = card.getBoundingClientRect();
    const escaped = [...card.querySelectorAll('*')].filter(node => !node.closest('.pf-insight-table-scroll') && node.getBoundingClientRect().right > box.right + 1);
    const table = card.querySelector('.pf-insight-table-scroll');
    return { escaped: escaped.length, cardFits: box.right <= window.innerWidth + 1, tableScrolls: table.scrollWidth > table.clientWidth + 1 };
  });
  expect(overflow).toEqual({ escaped: 0, cardFits: true, tableScrolls: false });
  expect(errors).toEqual([]);
});
