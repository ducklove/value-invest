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
  await expect.poll(() => page.evaluate(() => PfStore.loading)).toBe(false);
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

test('성과 숫자의 툴팁으로 합산 성과를 확인하고 모바일·키보드·실시간 갱신에서도 유지한다', async ({page}, testInfo) => {
  await prepare(page);
  const trigger = row(page, '006800').locator('.pf-col-changepct .js-pf-performance-tooltip');
  await expect(trigger).toHaveText(/1\.35%/);
  await trigger.hover();
  const popup = page.getByRole('tooltip');
  await expect(popup.locator('.pf-tooltip-line').first()).toHaveText('합산 등락률+0.71%');
  await expect(popup.locator('.pf-tooltip-note')).toHaveText('롱 투자금 대비');
  await expect(popup.locator('.pf-tooltip-title')).toHaveText('미래에셋 롱 + 헤지 숏');
  await expect(popup).toContainText('당일손익+50,000원');
  await expect(trigger).not.toBeFocused();
  await expect(page.locator('#pfPairSummary')).toHaveCount(0);
  await page.screenshot({path: testInfo.outputPath('pair-desktop.png'), fullPage: true});
  await trigger.focus();
  await page.evaluate(() => {
    PfStore.items.find(i => i.stock_code === '005930').quote = {price: 76000, previous_close: 74000, change: 2000, change_pct: 2.70};
    updatePortfolioRowQuote('005930', false);
  });
  await expect(popup.locator('.pf-tooltip-line').first()).toHaveText('합산 등락률0.00%');
  await page.keyboard.press('Escape');
  await expect(popup).toHaveCount(0);
  await expect(trigger).toBeFocused();
  await page.evaluate(() => {
    const item = PfStore.items.find(i => i.stock_code === '006800');
    item.quote = {...item.quote, price: 76000, change: 2000, change_pct: 2.70};
    updatePortfolioRowQuote('006800', false);
  });
  await expect(trigger).toBeFocused();
  await page.setViewportSize({width: 390, height: 844});
  await row(page, '005930').locator('.pf-col-changepct .js-pf-performance-tooltip').click();
  await expect(popup).toBeVisible();
  const bounds = await popup.boundingBox();
  expect(bounds.x).toBeGreaterThanOrEqual(0);
  expect(bounds.x + bounds.width).toBeLessThanOrEqual(390);
  await page.screenshot({path: testInfo.outputPath('pair-mobile.png'), fullPage: true});
  await page.mouse.move(0, 0);
  await page.locator('#pfAccountSelect').selectOption({index: 1});
  await expect(popup).toHaveCount(0);
  await expect(page.locator('.js-pf-open-pair-summary')).toHaveCount(0);
  await expect(row(page, '005930').locator('.js-pf-row-drag')).toHaveCount(1);
});

test('일반 종목의 등락률·수익률도 창 대신 툴팁으로 보여준다', async ({page}) => {
  await prepare(page);
  const ordinary = row(page, '000660');
  await ordinary.locator('.pf-col-changepct .js-pf-performance-tooltip').hover();
  await expect(page.getByRole('tooltip')).toContainText('등락률+1.35%');
  await expect(page.getByRole('tooltip')).toContainText('등락액+1,000원');
  await expect(page.getByRole('tooltip')).toContainText('당일손익+10,000원');
  await ordinary.locator('.pf-col-return .js-pf-performance-tooltip').hover();
  await expect(page.getByRole('tooltip')).toContainText('수익률+7.14%');
  await expect(page.getByRole('tooltip')).toContainText('평가손익+50,000원');
  await page.mouse.move(0, 0);
  await expect(page.getByRole('tooltip')).toHaveCount(0);
});

test('등락액·당일손익은 지정한 열 위치에 기본 숨김이며 숏 손익·실시간 갱신·합계를 반영한다', async ({page}, testInfo) => {
  await prepare(page);
  for (const key of ['change', 'daypnl']) {
    await expect(page.locator(`#pfTable th.pf-col-${key}`)).toBeHidden();
    const toggle = page.locator(`.js-pf-col-toggle[data-col-key="${key}"]`);
    await expect(toggle).not.toBeChecked();
    await toggle.check();
    await expect(page.locator(`#pfTable th.pf-col-${key}`)).toBeVisible();
  }
  await expect(page.locator('#pfTable th.pf-col-changepct + th')).toHaveClass(/pf-col-change/);
  await expect(page.locator('#pfTable th.pf-col-mktval + th')).toHaveClass(/pf-col-daypnl/);
  await expect(row(page, '006800').locator('.pf-col-change')).toHaveText('+1,000');
  await expect(row(page, '006800').locator('.pf-col-daypnl')).toHaveText('+100,000');
  await expect(row(page, '005930').locator('.pf-col-change')).toHaveText('+1,000');
  await expect(row(page, '005930').locator('.pf-col-daypnl')).toHaveText('-50,000');
  await expect(page.locator('#pfFoot .pf-col-daypnl')).toHaveText('+60,000');
  await page.evaluate(() => {
    PfStore.items.find(i => i.stock_code === '005930').quote = {price: 76000, previous_close: 74000, change: 2000, change_pct: 2.70};
    updatePortfolioRowQuote('005930', false);
    renderPortfolio({summaryOnly: true});
  });
  await expect(row(page, '005930').locator('.pf-col-change')).toHaveText('+2,000');
  await expect(row(page, '005930').locator('.pf-col-daypnl')).toHaveText('-100,000');
  await expect(page.locator('#pfFoot .pf-col-daypnl')).toHaveText('+10,000');
  await page.reload();
  for (const key of ['change', 'daypnl']) await expect(page.locator(`.js-pf-col-toggle[data-col-key="${key}"]`)).toBeChecked();
  await page.setViewportSize({width: 390, height: 844});
  await expect(page.locator('body')).toHaveClass(/pf-mobile-simple/);
  await expect(page.locator('#pfTable th.pf-col-change')).toBeVisible();
  await expect(page.locator('#pfTable th.pf-col-daypnl')).toBeVisible();
  expect((await row(page, '006800').locator('.pf-stock-cell').boundingBox()).width).toBeGreaterThan(100);
  await page.locator('.pf-table-wrap').evaluate(el => { el.scrollLeft = el.scrollWidth; });
  const stock = await row(page, '006800').locator('.pf-stock-cell').boundingBox();
  expect(stock.x).toBeGreaterThanOrEqual(0);
  expect(stock.x + stock.width).toBeLessThan(390);
  await expect(row(page, '006800').locator('.pf-col-daypnl')).toHaveText('+100,000');
  await page.screenshot({path: testInfo.outputPath('daily-columns-mobile.png'), fullPage: true});
});

test('롱·숏 종목명과 연결 표시의 위치가 기본·정렬·컴팩트·모바일에서도 맞는다', async ({page}, testInfo) => {
  await prepare(page);
  async function expectAligned() {
    await expect.poll(() => page.evaluate(() => {
      const left = code => document.querySelector(`#pfBody tr[data-code="${code}"] .pf-stock-link`).getBoundingClientRect().left;
      return Math.abs(left('006800') - left('005930'));
    })).toBeLessThan(1);
    await expect(row(page, '006800')).toHaveAttribute('data-pair-position', 'first');
    await expect(row(page, '005930')).toHaveAttribute('data-pair-position', 'last');
    await expect(row(page, '005930').locator('.pf-pair-connector')).toBeVisible();
  }
  await expectAligned();
  await page.locator('#pfTable th[data-sort="name"]').click();
  await expectAligned();
  await page.locator('#pfCompactToggle').check();
  await expectAligned();
  await page.screenshot({path: testInfo.outputPath('pair-compact.png'), fullPage: true});
  await page.locator('#pfCompactToggle').uncheck();
  await page.setViewportSize({width: 390, height: 844});
  await expectAligned();
  await page.screenshot({path: testInfo.outputPath('pair-mobile-alignment.png'), fullPage: true});
});
