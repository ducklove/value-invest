import { test, expect } from '@playwright/test';

test('배당 수취 입력과 저장 API를 제거하고 기존 내역 조회와 분배 화면을 유지한다', async ({ page }, testInfo) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page.locator('#pfBody tr[data-code="005930"]')).toBeVisible();
  await expect(page.getByRole('button', { name: '배당금 수취', exact: true })).toHaveCount(0);
  await expect(page.locator('#pfDividendDialog, .js-pf-dividend-receipt')).toHaveCount(0);
  await expect(page.locator('script[data-feature="dividend-receipts"]')).toHaveCount(0);
  const before = await page.evaluate(async () => (await fetch('/api/portfolio')).json());
  const status = await page.evaluate(async () => {
    const endpoints = [
      ['/api/portfolio/dividend-receipts', 'POST'],
      ['/api/portfolio/dividend-receipts/preview', 'POST'],
      ['/api/portfolio/dividend-receipts/candidates', 'GET'],
      ['/api/portfolio/dividend-receipts', 'GET'],
      ['/api/portfolio/dividend-receipts/totals', 'GET'],
    ];
    return Promise.all(endpoints.map(async ([url, method]) => (await fetch(url, {
      method, ...(method === 'POST' ? { headers: { 'Content-Type': 'application/json' }, body: '{}' } : {}),
    })).status));
  });
  expect(status.slice(0, 3).every(code => code >= 400 && code < 500)).toBe(true);
  expect(status.slice(3)).toEqual([200, 200]);
  expect(await page.evaluate(async () => (await fetch('/api/portfolio')).json())).toEqual(before);
  await page.getByRole('button', { name: '분배금 출금', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: '분배금 출금', exact: true });
  await expect(dialog).toBeVisible();
  await expect(page.locator('#pfDistributionBalance')).toContainText('미분배 0');
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await dialog.evaluate(el => el.scrollWidth <= el.clientWidth)).toBe(true);
  await dialog.screenshot({ path: testInfo.outputPath('distribution-mobile.png') });
});
