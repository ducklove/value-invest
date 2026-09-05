import { test, expect } from '@playwright/test';

test('로그인·보유 수량 저장·새로고침·소켓 재접속이 이어진다', async ({ page }) => {
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    return url.hostname === '127.0.0.1' ? route.continue() : route.abort();
  });
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page).toHaveURL(/\/portfolio$/);
  // 시장 지표 응답이 지연되는 동안에도 포트폴리오를 먼저 보여야 한다.
  await expect(page.locator('#portfolioView')).toBeVisible({ timeout: 1500 });
  const row = page.locator('#pfTable tr[data-code="005930"]');
  await expect(row).toContainText('삼성전자');
  await row.locator('.js-pf-edit').click();
  await page.locator('#pfEditQty').fill('12');
  const saved = page.waitForResponse(r => r.request().method() === 'PUT' && r.url().endsWith('/api/portfolio/005930'));
  await page.locator('.js-pf-save').click();
  expect((await saved).status()).toBe(200);
  await page.reload();
  await expect(row.locator('.pf-col-qty')).toHaveText('12');
  await page.waitForFunction(() => QuoteManager.connected);
  const newSocket = page.waitForEvent('websocket');
  await page.evaluate(() => QuoteManager.ws.close());
  await newSocket;
  await page.waitForFunction(() => QuoteManager.connected);
  await expect(row.locator('.pf-col-qty')).toHaveText('12');
});
