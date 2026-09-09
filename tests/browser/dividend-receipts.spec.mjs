import { test, expect } from '@playwright/test';

test('배당 스케줄 수취와 분배금 출금이 현금·누계에 반영되고 모바일에서 유지된다', async ({ page }, testInfo) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' });
  const key = `005930:estimated:${today}`;
  await page.route('**/api/portfolio/dividend-receipts/candidates', route => route.fulfill({ json: {
    as_of: today, events: [{ stock_code: '005930', stock_name: '삼성전자', date: today, type: 'estimated', currency: 'KRW',
      amount_per_share: 100, shares: 10, source_key: key, received: false }],
  } }));
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page.locator('#pfBody tr[data-code="005930"]')).toBeVisible();
  const before = await page.evaluate(async () => (await fetch('/api/portfolio')).json());
  const cashBefore = before.find(i => i.stock_code === 'CASH_KRW').quantity;
  const quantity = before.find(i => i.stock_code === '005930').quantity;
  await page.getByRole('button', { name: '배당금 수취', exact: true }).click();
  await page.locator('#pfDividendSchedule').selectOption(key);
  await expect(page.locator('#pfDividendPerShare')).toHaveValue('100');
  await expect(page.locator('#pfDividendTaxRate')).toHaveValue('15.4');
  await page.locator('#pfDividendQuantity').fill('10');
  await page.getByRole('button', { name: '수취 내용 확인', exact: true }).click();
  await expect(page.locator('#pfDividendPreview')).toContainText('+846 KRW');
  await page.getByRole('dialog', { name: '배당금 수취', exact: true }).screenshot({ path: testInfo.outputPath('dividend-desktop.png') });
  await page.getByRole('button', { name: '배당금 수취 저장', exact: true }).click();
  await expect(page.locator('#pfDividendStatus')).toContainText('저장했습니다');
  await expect(page.locator('#pfDividendTotals')).toContainText('미분배 846');
  const received = await page.evaluate(async () => (await fetch('/api/portfolio')).json());
  expect(received.find(i => i.stock_code === 'CASH_KRW').quantity).toBe(cashBefore + 846);
  expect(received.find(i => i.stock_code === '005930').quantity).toBe(quantity);

  await page.getByRole('button', { name: '누적 배당금 분배하기' }).click();
  await expect(page.locator('#pfDistributionAmount')).toHaveValue('846');
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole('button', { name: '분배 내용 확인', exact: true }).click();
  await expect(page.locator('#pfDistributionPreview')).toContainText('기존 좌수 유지');
  const dialog = page.getByRole('dialog', { name: '분배금 출금', exact: true });
  expect(await dialog.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(false);
  await dialog.screenshot({ path: testInfo.outputPath('distribution-mobile.png') });
  await page.getByRole('button', { name: '분배금 출금 저장', exact: true }).click();
  await expect(page.locator('#pfDistributionStatus')).toContainText('저장했습니다');
  await page.getByRole('button', { name: '분배금 출금 닫기' }).click();
  await page.reload();
  const distributed = await page.evaluate(async () => (await fetch('/api/portfolio')).json());
  expect(distributed.find(i => i.stock_code === 'CASH_KRW').quantity).toBe(cashBefore);
  expect(distributed.find(i => i.stock_code === '005930').quantity).toBe(quantity);
  const balances = await page.evaluate(async () => (await fetch('/api/portfolio/distributions/balances')).json());
  expect(balances[0].available_amount).toBe(0);
  expect(balances[0].distributed_amount).toBe(846);
  expect(await page.locator('.pf-tab-bar').evaluate(el => el.scrollWidth > el.clientWidth)).toBe(false);
});
