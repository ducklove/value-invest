import { test, expect } from '@playwright/test';

test('사이드바 지수는 1분마다 갱신되고 실패와 회복 상태를 구별한다', async ({ page }) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.route('**/api/market-indicators', route => route.fulfill({ json: { KOSPI: { label: 'KOSPI', category: '국내 지수' } } }));
  await page.route('**/api/settings/market-bar', route => route.fulfill({ json: { codes: ['KOSPI'] } }));
  let value = '7,033.92';
  let failed = false;
  await page.route('**/api/market-summary?*', route => failed
    ? route.fulfill({ status: 503, json: { detail: '일시 오류' } })
    : route.fulfill({ json: { KOSPI: { value, change: '169.62', change_pct: '2.41%', direction: 'down', as_of: '2026-09-11T10:11:14+09:00' } } }));
  await page.clock.install();
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  const quote = page.locator('#marketBar .mi-val').first();
  await expect(quote).toHaveText('7,033.92');
  value = '6,864.30';
  await page.clock.fastForward(61_000);
  await expect(quote).toHaveText(value);
  await expect(quote).toHaveAttribute('title', /시세 기준.*KST/);
  failed = true;
  await page.clock.fastForward(61_000);
  await expect(page.locator('#marketBar .mi-stale')).toHaveText('지연');
  await expect(quote).toHaveText(value);
  failed = false;
  value = '6,865.00';
  await page.clock.fastForward(61_000);
  await expect(quote).toHaveText(value);
  await expect(page.locator('#marketBar .mi-stale')).toHaveCount(0);
});
