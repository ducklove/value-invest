import {test, expect} from '@playwright/test';

const headers = {Origin: 'http://127.0.0.1:18765'};
const codes = page => page.locator('#pfBody tr').evaluateAll(rows => rows.map(row => row.dataset.code));
const row = (page, code) => page.locator(`#pfBody tr[data-code="${code}"]`);

async function prepare(page) {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.goto('/login');
  expect((await page.request.post('/api/auth/register', {headers, data: {
    email: `pair-${Date.now()}@example.com`, name: '롱숏 검증', password: 'browser-test-password',
  }})).ok()).toBe(true);
  for (const [code, name, quantity] of [['006800', '미래에셋 롱', 100], ['005930', '헤지 숏', -50], ['000660', '다른 종목', 10]]) {
    expect((await page.request.put(`/api/portfolio/${code}`, {headers,
      data: {stock_name: name, quantity, avg_price: 70000}})).ok()).toBe(true);
  }
  expect((await page.request.put('/api/portfolio/order', {headers,
    data: {stock_codes: ['005930', '000660', '006800']}})).ok()).toBe(true);
  await page.goto('/portfolio');
  await expect(page.locator('#pfBody tr')).toHaveCount(3);
  await expect.poll(() => page.evaluate(() => PfStore.loading)).toBe(false);
  await row(page, '005930').locator('.js-pf-edit').click();
  const paired = page.waitForResponse(r => r.request().method() === 'PUT' && r.url().endsWith('/005930/pair'));
  await row(page, '005930').locator('.js-pf-pair').selectOption('006800');
  expect((await paired).ok()).toBe(true);
  await expect.poll(() => codes(page)).toEqual(['006800', '005930', '000660']);
  await row(page, '005930').locator('.js-pf-cancel').click();
  await expect.poll(() => codes(page)).toEqual(['000660', '006800', '005930']);
}

test('롱숏 등록 후 정렬·검색·이동·새로고침에도 숏은 롱 바로 아래에 고정된다', async ({page}) => {
  await prepare(page);
  await expect(row(page, '005930').locator('.js-pf-row-drag')).toHaveCount(0);
  await expect(row(page, '006800').locator('.js-pf-row-drag')).toHaveCount(1);
  await expect(page.locator('.pf-pair-chip')).toHaveCount(0);
  // 롱만 움직여도 숏을 포함한 순서가 저장된다.
  const saved = page.waitForResponse(r => r.request().method() === 'PUT' && r.url().endsWith('/portfolio/order'));
  await row(page, '006800').locator('.js-pf-row-drag').dragTo(row(page, '000660').locator('.js-pf-row-drag'), {targetPosition: {x: 12, y: 2}});
  expect((await saved).request().postDataJSON().stock_codes).toEqual(['006800', '005930', '000660']);
  await page.reload();
  await expect.poll(() => codes(page)).toEqual(['006800', '005930', '000660']);
  for (let i = 0; i < 3; i++) {
    await page.locator('#pfTable th[data-sort="name"]').click();
    const order = await codes(page);
    expect(order.indexOf('005930')).toBe(order.indexOf('006800') + 1);
  }
  await page.evaluate(() => pfSetPortfolioSearchText('헤지 숏'));
  await expect.poll(() => codes(page)).toEqual(['006800', '005930']);
  await page.evaluate(() => pfSetPortfolioSearchText(''));
  await row(page, '005930').locator('.js-pf-edit').click();
  const unpaired = page.waitForResponse(r => r.request().method() === 'PUT' && r.url().endsWith('/005930/pair'));
  await row(page, '005930').locator('.js-pf-pair').selectOption('');
  expect((await unpaired).ok()).toBe(true);
  await row(page, '005930').locator('.js-pf-cancel').click();
  await expect(row(page, '005930').locator('.js-pf-row-drag')).toHaveCount(1);
  await expect(page.locator('.js-pf-open-pair-summary')).toHaveCount(0);
});

test('기존 등락률 클릭으로 합산 성과를 확인하고 모바일·키보드·실시간 갱신에서도 유지한다', async ({page}, testInfo) => {
  await prepare(page);
  const button = row(page, '006800').locator('.pf-col-changepct button');
  await expect(button).toHaveText(/1\.35%/);
  await button.focus();
  await page.keyboard.press('Enter');
  const popup = page.getByRole('dialog', {name: '롱·숏 합산 등락률', exact: true});
  await expect(popup.locator('.pf-pair-value')).toHaveText('+1.35%');
  await expect(popup.locator('.pf-pair-title')).toHaveText('미래에셋 롱 + 헤지 숏');
  await expect(popup.locator('.pf-pair-pnl')).toContainText('+50,000');
  await page.screenshot({path: testInfo.outputPath('pair-desktop.png'), fullPage: true});
  await page.evaluate(() => {
    PfStore.items.find(i => i.stock_code === '005930').quote = {price: 76000, previous_close: 74000, change: 2000, change_pct: 2.70};
    updatePortfolioRowQuote('005930', false);
  });
  await expect(popup.locator('.pf-pair-value')).toHaveText('0.00%');
  await page.keyboard.press('Escape');
  await expect(popup).toHaveCount(0);
  await expect(button).toBeFocused();
  await page.evaluate(() => {
    const item = PfStore.items.find(i => i.stock_code === '006800');
    item.quote = {...item.quote, price: 76000, change: 2000, change_pct: 2.70};
    updatePortfolioRowQuote('006800', false);
  });
  await expect(button).toBeFocused();
  await page.setViewportSize({width: 390, height: 844});
  await row(page, '005930').locator('.pf-col-changepct button').click();
  await expect(popup).toBeVisible();
  const bounds = await popup.boundingBox();
  expect(bounds.x).toBeGreaterThanOrEqual(0);
  expect(bounds.x + bounds.width).toBeLessThanOrEqual(390);
  await page.screenshot({path: testInfo.outputPath('pair-mobile.png'), fullPage: true});
  await page.locator('#pfAccountSelect').selectOption({index: 1});
  await expect(popup).toHaveCount(0);
  await expect(page.locator('.js-pf-open-pair-summary')).toHaveCount(0);
  await expect(row(page, '005930').locator('.js-pf-row-drag')).toHaveCount(1);
});
