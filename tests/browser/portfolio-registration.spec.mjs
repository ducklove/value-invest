import { test, expect } from '@playwright/test';

async function holdings(page) {
  return page.evaluate(async () => (await fetch('/api/portfolio')).json());
}

async function openAdd(page, code, mode = 'holding') {
  await page.locator('#pfAddToggle').click();
  await page.locator(`input[name="pfAddMode"][value="${mode}"]`).check();
  await page.locator('#pfAddInput').fill(code);
  await page.locator('#pfAddBtn').click();
}

test('달러 초기 잔고에서 새 종목을 매수·매도하면 달러만 변하고 원화는 유지된다', async ({ page }) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page.locator('#pfBody tr[data-code="005930"]')).toBeVisible();
  const krwBefore = (await holdings(page)).find(i => i.stock_code === 'CASH_KRW').quantity;
  await openAdd(page, 'CASH_USD');
  await page.locator('#pfRegisterQuantity').fill('1000');
  await page.locator('#pfRegisterSave').click();
  await expect(page.locator('#pfRegisterDialog')).not.toBeVisible();
  await openAdd(page, 'AAPL', 'buy');
  await expect(page.locator('#pfTradeCurrency')).toHaveValue('USD');
  await page.locator('#pfTradeQuantity').fill('2');
  await page.locator('#pfTradePrice').fill('100');
  await page.locator('#pfTradeFees').fill('1');
  await page.locator('#pfTradePreviewButton').click();
  await expect(page.locator('#pfTradeSave')).toBeEnabled();
  await page.locator('#pfTradeSave').click();
  await expect(page.locator('#pfTradeStatus')).toContainText('저장했습니다');
  await page.locator('#pfTradeClose').click();
  const bought = await holdings(page);
  expect(bought.find(i => i.stock_code === 'AAPL').quantity).toBe(2);
  expect(bought.find(i => i.stock_code === 'CASH_USD').quantity).toBe(799);
  expect(bought.find(i => i.stock_code === 'CASH_KRW').quantity).toBe(krwBefore);
  await page.locator('#pfBody tr[data-code="AAPL"] .js-pf-delete').click();
  await page.locator('#pfRemoveSell').click();
  await expect(page.locator('#pfTradeCurrency')).toHaveValue('USD');
  await expect(page.locator('#pfTradeQuantity')).toHaveValue('2');
  await page.locator('#pfTradePrice').fill('110');
  await page.locator('#pfTradeFees').fill('1');
  await page.locator('#pfTradePreviewButton').click();
  await expect(page.locator('#pfTradeSave')).toBeEnabled();
  await page.locator('#pfTradeSave').click();
  await expect(page.locator('#pfTradeStatus')).toContainText('저장했습니다');
  const sold = await holdings(page);
  expect(sold.some(i => i.stock_code === 'AAPL')).toBe(false);
  expect(sold.find(i => i.stock_code === 'CASH_USD').quantity).toBe(1018);
  expect(sold.find(i => i.stock_code === 'CASH_KRW').quantity).toBe(krwBefore);
});

test('초기 등록 취소·확정·등록 삭제와 신규 종목 매수·전량 매도의 현금 반영을 구분한다', async ({ page }, testInfo) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page.locator('#pfBody tr[data-code="005930"]')).toBeVisible();
  const before = await holdings(page);
  const cashBefore = before.find(i => i.stock_code === 'CASH_KRW').quantity;

  for (const close of ['pfRegisterClose', 'pfRegisterCancel', 'Escape']) {
    await openAdd(page, '000660');
    await expect(page.locator('#pfRegisterDialog')).toBeVisible();
    await page.locator('#pfRegisterQuantity').fill('3');
    if (close === 'Escape') await page.keyboard.press('Escape');
    else await page.locator(`#${close}`).click();
    await expect(page.locator('#pfRegisterDialog')).not.toBeVisible();
    expect(await holdings(page)).toEqual(before);
  }
  await page.reload();
  await expect(page.locator('#pfBody tr[data-code="005930"]')).toBeVisible();
  await openAdd(page, '000660');
  await page.locator('#pfRegisterQuantity').fill('3');
  await page.locator('#pfRegisterPrice').fill('70');
  await page.locator('#pfRegisterDialog').screenshot({ path: testInfo.outputPath('initial-registration-desktop.png') });
  await page.locator('#pfRegisterSave').click();
  await expect(page.locator('#pfRegisterDialog')).not.toBeVisible();
  await page.reload();
  await expect(page.locator('#pfBody tr[data-code="000660"]')).toBeVisible();
  const registered = await holdings(page);
  expect(registered.find(i => i.stock_code === '000660').quantity).toBe(3);
  expect(registered.find(i => i.stock_code === 'CASH_KRW').quantity).toBe(cashBefore);
  await page.locator('#pfBody tr[data-code="000660"] .js-pf-delete').click();
  await page.locator('#pfRemoveClose').click();
  expect(await holdings(page)).toEqual(registered);
  await page.locator('#pfBody tr[data-code="000660"] .js-pf-delete').click();
  await page.locator('#pfRemoveDelete').click();
  await expect(page.locator('#pfBody tr[data-code="000660"]')).toHaveCount(0);
  expect((await holdings(page)).find(i => i.stock_code === 'CASH_KRW').quantity).toBe(cashBefore);

  await openAdd(page, '000660', 'buy');
  await expect(page.locator('#pfTradeStock')).toHaveValue('000660 (000660)');
  expect((await holdings(page)).some(i => i.stock_code === '000660')).toBe(false);
  await page.locator('#pfTradeQuantity').fill('2');
  await page.locator('#pfTradePrice').fill('100');
  await page.locator('#pfTradeFees').fill('10');
  await page.locator('#pfTradePreviewButton').click();
  await expect(page.locator('#pfTradeSave')).toBeEnabled();
  await page.locator('#pfTradeSave').click();
  await expect(page.locator('#pfTradeStatus')).toContainText('저장했습니다');
  await page.locator('#pfTradeClose').click();
  await page.reload();
  await expect(page.locator('#pfBody tr[data-code="000660"]')).toBeVisible();
  const bought = await holdings(page);
  expect(bought.find(i => i.stock_code === '000660').quantity).toBe(2);
  expect(bought.find(i => i.stock_code === 'CASH_KRW').quantity).toBe(cashBefore - 210);

  await page.locator('#pfBody tr[data-code="000660"] .js-pf-delete').click();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.locator('#pfRemoveDialog')).toBeVisible();
  expect(await page.locator('#pfRemoveDialog').evaluate(el => el.scrollWidth > el.clientWidth)).toBe(false);
  await page.locator('#pfRemoveDialog').screenshot({ path: testInfo.outputPath('holding-removal-mobile.png') });
  await page.locator('#pfRemoveSell').click();
  await expect(page.locator('#pfTradeSide')).toHaveValue('sell');
  await expect(page.locator('#pfTradeQuantity')).toHaveValue('2');
  await page.locator('#pfTradePrice').fill('120');
  await page.locator('#pfTradeFees').fill('1');
  await page.locator('#pfTradeTaxRate').fill('0');
  await page.locator('#pfTradePreviewButton').click();
  await expect(page.locator('#pfTradeSave')).toBeEnabled();
  await page.locator('#pfTradeClose').click();
  expect(await holdings(page)).toEqual(bought);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.locator('#pfBody tr[data-code="000660"] .js-pf-delete').click();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.locator('#pfRemoveSell').click();
  await page.locator('#pfTradePrice').fill('120');
  await page.locator('#pfTradeFees').fill('1');
  await page.locator('#pfTradeTaxRate').fill('0');
  await page.locator('#pfTradePreviewButton').click();
  await expect(page.locator('#pfTradeSave')).toBeEnabled();
  await page.locator('#pfTradeSave').click();
  await expect(page.locator('#pfTradeStatus')).toContainText('저장했습니다');
  await page.locator('#pfTradeClose').click();
  await page.reload();
  await expect(page.locator('#pfBody tr[data-code="005930"]')).toBeVisible();
  const sold = await holdings(page);
  expect(sold.some(i => i.stock_code === '000660')).toBe(false);
  expect(sold.find(i => i.stock_code === 'CASH_KRW').quantity).toBe(cashBefore + 29);
});
