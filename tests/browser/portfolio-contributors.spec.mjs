import { test, expect } from '@playwright/test';

const holdings = [
  ['005930', '삼성전자', 120000], ['000660', 'SK하이닉스', 105000], ['035420', 'NAVER', 101000],
  ['035720', '카카오', 95000], ['005380', '현대자동차', 90000], ['000270', '기아', 80000],
].map(([code, name, price]) => ({ stock_code: code, stock_name: name, quantity: 10, avg_price: 100000, currency: 'KRW', group_name: '국내', quote: { price, change: price - 100000, change_pct: price / 1000 - 100 } }));
const snapshot = {
  date: '2026-09-30', total_value: 6000000, total_units: 6000, nav: 1000,
  stock_values: Object.fromEntries(holdings.map(r => [r.stock_code, 1000000])),
  stock_positions: Object.fromEntries(holdings.map(r => [r.stock_code, { quantity: 10, group_name: '국내' }])),
  stock_trade_flows: {}, today_net_cashflow: 0,
};

async function openPortfolio(page) {
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (url.hostname !== '127.0.0.1') return route.abort();
    const path = url.pathname;
    const mock = path === '/api/portfolio' ? holdings
      : path === '/api/portfolio/prev-day-snapshot' || path === '/api/portfolio/month-end-value' ? snapshot
      : path === '/api/portfolio/year-start-value' ? { ...snapshot, date: '2025-12-31' }
      : path === '/api/portfolio/nav-history' ? [snapshot] : null;
    return mock ? route.fulfill({ json: mock }) : route.continue();
  });
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page.locator('#pfSummary [data-period="today"]')).toBeVisible();
  await page.waitForFunction(() => Boolean(PfStore.snapshots.yearStart?.stock_positions));
}

test('성과 카드의 hover·클릭 고정·키보드·실시간 갱신과 화면 경계를 검증한다', async ({ page }, testInfo) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await openPortfolio(page);
  const today = page.getByRole('button', { name: 'TODAY 성과 기여 종목 보기' });
  const mtd = page.getByRole('button', { name: 'MTD 성과 기여 종목 보기' });
  const ytd = page.getByRole('button', { name: 'YTD 성과 기여 종목 보기' });
  const panel = page.locator('#pfContributorPopover');
  await today.hover();
  await expect(panel).toBeVisible();
  await expect(panel.locator('.positive li')).toHaveCount(3);
  await expect(panel.locator('.negative li')).toHaveCount(3);
  await expect(panel.locator('.positive li').first()).toContainText('삼성전자');
  await expect(panel.locator('.positive li').first()).toContainText('+200,000원');
  await expect(panel.locator('.positive li').first()).toContainText('+20.00%');
  await panel.hover();
  await expect(panel).toBeVisible();
  await page.mouse.move(5, 5);
  await expect(panel).not.toBeVisible();
  await today.click();
  await page.mouse.move(5, 5);
  await expect(panel).toBeVisible();
  await mtd.click();
  await expect(panel).toContainText('MTD 성과 기여 종목');
  await page.evaluate(() => {
    PfStore.items.find(r => r.stock_code === '005930').quote.price = 130000;
    renderPortfolio({ summaryOnly: true });
  });
  await expect(mtd).toBeFocused();
  await expect(mtd).toHaveAttribute('aria-expanded', 'true');
  await expect(panel.locator('.positive li').first()).toContainText('+300,000원');
  await mtd.click();
  await expect(panel).not.toBeVisible();
  await ytd.focus();
  await page.keyboard.press('Enter');
  await expect(panel).toContainText('YTD 성과 기여 종목');
  await page.getByRole('button', { name: '성과 기여 종목 닫기' }).focus();
  await page.keyboard.press('Escape');
  await expect(panel).not.toBeVisible();
  await expect(ytd).toBeFocused();
  await page.keyboard.press('Space');
  await expect(panel).toBeVisible();
  const box = await panel.boundingBox();
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(1440);
  expect(box.y + box.height).toBeLessThanOrEqual(1000);
  await page.screenshot({ path: testInfo.outputPath('contributors-desktop.png') });
  await page.locator('#pfSearchInput').click();
  await expect(panel).not.toBeVisible();
  expect(errors).toEqual([]);
});

test('모바일에서는 탭으로 열고 스크롤·닫기를 사용할 수 있다', async ({ browser }, testInfo) => {
  const context = await browser.newContext({ viewport: { width: 390, height: 700 }, isMobile: true, hasTouch: true });
  const page = await context.newPage();
  await openPortfolio(page);
  const card = page.getByRole('button', { name: 'YTD 성과 기여 종목 보기' });
  const panel = page.locator('#pfContributorPopover');
  await card.tap();
  await expect(panel).toBeVisible();
  const box = await panel.boundingBox();
  expect(box.x).toBeGreaterThanOrEqual(12);
  expect(box.x + box.width).toBeLessThanOrEqual(378);
  expect(box.y + box.height).toBeLessThanOrEqual(700);
  await expect(panel.locator('.negative li')).toHaveCount(3);
  await page.screenshot({ path: testInfo.outputPath('contributors-mobile.png') });
  await page.getByRole('button', { name: '성과 기여 종목 닫기' }).tap();
  await expect(panel).not.toBeVisible();
  await card.tap();
  await expect(panel).toBeVisible();
  await card.tap();
  await expect(panel).not.toBeVisible();
  await context.close();
});
