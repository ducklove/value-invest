import { test, expect } from '@playwright/test';

test('배당 캘린더는 지급·권리일과 출처를 구분하고 모바일 수취 입력을 연결한다', async ({ page }, testInfo) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  const payment = { stock_code: 'AGNC', stock_name: 'AGNC', date: '2026-09-10', type: 'payment', date_kind: 'payment',
    label: '월배당 · 지급일', pay_date: '2026-09-10', ex_date: '2026-08-31', record_date: '2026-08-31',
    confirmed: true, date_status: 'announced', currency: 'USD', amount_per_share: 0.12, shares: 10, expected_amount_krw: 1680,
    source: 'AGNC 공시', source_url: 'https://investors.agnc.com/stock-information/dividend-history',
    fetched_at: '2026-09-10T00:00:00Z', source_key: 'AGNC:ex_date:2026-08-31', receiptable: true };
  const estimated = { ...payment, stock_code: 'SCHP', stock_name: 'SCHP', date: '2026-09-20', pay_date: '2026-09-20',
    label: '월배당 · 지급일 (예상)', source: 'Schwab 공시', source_url: 'https://www.schwabassetmanagement.com/products/schp',
    type: 'estimated', date_precision: 'approximate', confirmed: false, receiptable: false, source_key: null };
  await page.route('**/api/portfolio/dividend-calendar?*', route => route.fulfill({ json: {
    as_of: '2026-09-10', start_month: '2026-09', end_month: '2026-09', events: [payment, estimated],
    monthly: [{ month: '2026-09', count: 2, total_krw: 3360, announced_krw: 1680, estimated_krw: 1680 }],
    summary: { total_expected_krw: 3360, confirmed_count: 1, estimated_count: 1 },
  } }));
  await page.route('**/api/portfolio/dividend-receipts/candidates', route => route.fulfill({ json: { events: [payment] } }));
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page.locator('#pfBody tr[data-code="005930"]')).toBeVisible();
  await page.locator('.pf-tab[data-tab="performance"]').click();
  const calendar = page.locator('#pfDivCalWrap');
  await expect(calendar).toContainText('배당락 2026-08-31');
  await expect(calendar).toContainText('2026-09-20 전후');
  await expect(calendar.getByRole('button', { name: '수취 입력' })).toHaveCount(1);
  await expect(calendar.locator('a').filter({ hasText: 'AGNC 공시' })).toHaveAttribute('href', payment.source_url);
  await calendar.screenshot({ path: testInfo.outputPath('calendar-desktop.png') });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.locator('#pfSimpleToggle')).toBeVisible();
  if (await page.locator('#pfSimpleToggle').getAttribute('aria-pressed') === 'true') await page.locator('#pfSimpleToggle').click();
  await page.locator('.pf-tab[data-tab="performance"]').click();
  await expect(calendar).toBeVisible();
  expect(await calendar.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(false);
  await calendar.screenshot({ path: testInfo.outputPath('calendar-mobile.png') });
  await calendar.getByRole('button', { name: '수취 입력' }).click();
  await expect(page.locator('#pfDividendSchedule')).toHaveValue(payment.source_key);
  await expect(page.locator('#pfDividendPerShare')).toHaveValue('0.12');
  await expect(page.locator('#pfDividendDate')).toHaveValue('2026-09-10');
  await expect(page.locator('#pfDividendScheduleNote')).toContainText('공시 지급일');
});
