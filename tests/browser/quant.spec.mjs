import { test, expect } from '@playwright/test';

test('퀀트 연구 로그인·저장·새로고침·취소와 모바일 화면', async ({ page }) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.route('**/api/quant/capabilities', route => route.fulfill({ json: {
    pairs: [{ common: '005930', preferred: '005935', name: '삼성전자' }],
    readiness: { status: 'ready', checks: { latest_price_date: '2026-09-14' } }, live_enabled: false,
  } }));
  expect((await page.request.get('/api/quant/runs')).status()).toBe(401);
  await page.goto('/login?return_to=/quant');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page).toHaveURL(/\/quant$/);
  await expect(page.locator('#quantView')).toBeVisible();
  await page.locator('#quantPair').selectOption('005930:005935');
  await page.locator('#quantForm [name=start]').fill('2024-01-01');
  await page.locator('#quantForm [name=end]').fill('2026-09-14');
  const response = page.waitForResponse(r => r.url().endsWith('/api/quant/runs') && r.request().method() === 'POST');
  await page.locator('#quantSubmit').click();
  expect((await response).status()).toBe(202);
  await expect(page.locator('#quantReport')).toContainText('대기');
  await page.reload();
  await page.locator('#quantRuns button').first().click();
  await expect(page.locator('#quantReport')).toContainText('대기');
  await page.getByRole('button', { name: '연구 취소', exact: true }).click();
  await expect(page.locator('#quantReport')).toContainText('취소');
  const saved = await (await page.request.get('/api/quant/runs')).json();
  const row = saved.runs[0];
  const result = {
    config: row.config, config_hash: '검증용 설정', engine_version: 'preferred-switch-1',
    snapshot: { snapshot_id: '검증용 입력' },
    scenarios: ['switch', 'common', 'preferred', 'mixed'].map((mode, i) => ({
      mode, return_pct: i + 1, max_drawdown_pct: -i, cost: 10000, trade_count: 2,
      nav: [{ date: '2024-01-02', nav: 10000000 }, { date: '2026-09-14', nav: 10000000 * (1 + (i + 1) / 100) }], trades: [],
    })),
    stress: { return_pct: 0.5 }, latest_signal: { date: '2026-09-14', discount: 0.2, z: 1.2, target: 'common', reason: '보통주 유지' },
    limitations: ['화면 검증용 가상 데이터입니다.'],
  };
  await page.route(`**/api/quant/runs/${row.id}`, route => route.fulfill({ json: { ...row, status: 'succeeded', result } }));
  await page.locator('#quantRuns button').first().click();
  await expect(page.locator('#quantReport svg')).toBeVisible();
  await expect(page.locator('#quantReport')).toContainText('비용 2배');
  await page.screenshot({ path: 'test-results/quant-desktop.png', fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.locator('#quantSubmit')).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: 'test-results/quant-mobile.png', fullPage: true });
});
