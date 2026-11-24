import { expect, test } from '@playwright/test';

async function expectTrendPoint(chart, edge, text) {
  const canvas = chart.locator('canvas');
  await expect(canvas).toBeVisible();
  const { width } = await canvas.boundingBox();
  await canvas.hover({ position: { x: edge === 'first' ? 74 : width - 28, y: 50 } });
  await expect(chart.locator('.pf-lite-tooltip')).toContainText(text);
  await expect(chart.locator('.pf-lite-tooltip')).toBeVisible();
}

test('종목·그룹·태그 팝업은 날짜별 전체 평가액으로 포트폴리오 비중을 표시한다', async ({ page }) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1'
    ? route.continue() : route.abort());
  await page.route('**/api/portfolio/asset-insight/005930?*', route => route.fulfill({
    json: { profile: { code: '005930', name: '삼성전자' } },
  }));
  await page.route('**/api/asset-quote/CASH_USD', route => route.fulfill({ json: { price: 1600 } }));
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page).toHaveURL(/\/portfolio$/);

  const row = page.locator('#pfBody tr[data-code="005930"]');
  const insight = page.locator('#pfAssetInsightModal');
  const slice = page.locator('#pfTagSummaryModal');
  const charts = modal => modal.locator('[data-trend-chart]');
  const tag = '추세 검증';
  await expect(row).toBeVisible();
  const tags = await page.evaluate(() => PfStore.items.find(item => item.stock_code === '005930').tags);
  try {
    await row.locator('.js-pf-open-insight').click();
    await expect(insight.getByText('평가금액 추세', { exact: true })).toBeVisible();
    await expectTrendPoint(charts(insight).nth(0), 'first', '700,000원');
    await expectTrendPoint(charts(insight).nth(1), 'first', '70%');
    await expect(charts(insight).nth(1).locator('.pf-lite-tooltip')).toContainText('전체 평가액 1,000,000원');
    await expectTrendPoint(charts(insight).nth(1), 'last', '50%');
    await expect(charts(insight).nth(1).locator('.pf-lite-tooltip')).toContainText('전체 평가액 1,500,000원');

    await insight.locator('#pfInsightTagInput').fill(tag);
    await insight.getByRole('button', { name: '추가', exact: true }).click();
    await expect(insight.locator('.pf-insight-tag-list')).toContainText(tag);
    await expect(insight.locator('canvas')).toHaveCount(2);
    await page.keyboard.press('Escape');
    await expect(insight).toBeHidden();
    await expect(insight.locator('canvas')).toHaveCount(0);

    await page.locator('.pf-currency-btn[data-currency="USD"]').click();
    await row.locator('.js-pf-open-group-summary').click();
    await expectTrendPoint(charts(slice).nth(0), 'first', '500 USD');
    await expectTrendPoint(charts(slice).nth(0), 'last', '500 USD');
    await expectTrendPoint(charts(slice).nth(1), 'first', '70%');
    await expectTrendPoint(charts(slice).nth(1), 'last', '50%');
    await charts(slice).nth(1).getByRole('slider', { name: '차트 기간 시작' }).press('End');
    await expect(slice.locator('[data-trend-range]').nth(1)).toHaveText(/^(\d{4}-\d{2}-\d{2}) ~ \1 · 1일$/);
    await page.keyboard.press('Escape');
    await expect(slice).toHaveCount(0);

    await row.locator('.js-pf-open-tag-summary').filter({ hasText: tag }).click();
    await expectTrendPoint(charts(slice).nth(0), 'first', '500 USD');
    await expectTrendPoint(charts(slice).nth(1), 'first', '70%');
    await expectTrendPoint(charts(slice).nth(1), 'last', '50%');
    await page.keyboard.press('Escape');

    await page.setViewportSize({ width: 390, height: 844 });
    await row.locator('.js-pf-open-insight').click();
    await expect(insight.locator('canvas')).toHaveCount(2);
    const [valueChart, weightChart] = await charts(insight).evaluateAll(elements =>
      elements.map(element => ({ top: element.offsetTop, width: element.offsetWidth })));
    expect(weightChart.top).toBeGreaterThan(valueChart.top);
    expect(weightChart.width).toBe(valueChart.width);
    await expect(insight.getByRole('slider')).toHaveCount(0);
  } finally {
    await page.evaluate(tags => apiFetchJson('/api/portfolio/005930/tags', {
      method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ tags }),
    }), tags);
  }
});
