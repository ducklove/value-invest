import { test, expect } from '@playwright/test';

test('경제 캘린더 국가별 알림 조건을 저장·복원·삭제하고 모바일에서도 표시한다', async ({ page }, testInfo) => {
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    return url.hostname === '127.0.0.1' ? route.continue() : route.abort();
  });
  const tomorrow = new Date(Date.now() + 86400000).toISOString().slice(0, 10);
  await page.route('**/api/market/economic-calendar?*', route => route.fulfill({ json: { events: [
    { index_id: 'kr-low', date: tomorrow, datetime: `${tomorrow} 09:00:00`, time: '09:00', country: 'kr', country_name: '한국', flag: '🇰🇷', importance: 'low', importance_label: '하', event: '소비자심리지수' },
    { index_id: 'us-mid', date: tomorrow, datetime: `${tomorrow} 21:00:00`, time: '21:00', country: 'us', country_name: '미국', flag: '🇺🇸', importance: 'mid', importance_label: '중', event: '주택시장지수' },
    { index_id: 'jp-high', date: tomorrow, datetime: `${tomorrow} 09:30:00`, time: '09:30', country: 'jp', country_name: '일본', flag: '🇯🇵', importance: 'high', importance_label: '상', event: '정책금리' },
  ] } }));
  await page.goto('/login?return_to=/investing');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await page.locator('#econCalAlertsToggle').click();
  await expect(page.locator('#econCalRuleAdd')).toBeVisible();
  for (let i = 0; i < 3; i++) await page.locator('#econCalRuleAdd').click();
  await page.getByLabel('3번째 알림 국가').selectOption('jp');
  await page.getByLabel('2번째 알림 중요도').selectOption('mid');
  await page.getByLabel('3번째 알림 중요도').selectOption('high');
  await page.locator('#econCalRuleSave').click();
  await expect(page.locator('#econCalRuleStatus')).toHaveText('저장했습니다.');
  await expect(page.locator('.ec-rule-badge')).toHaveCount(3);
  await page.reload();
  await page.locator('#econCalAlertsToggle').click();
  const rules = page.locator('.ec-rule-row');
  await expect(rules).toHaveCount(3);
  const restored = await rules.evaluateAll(rows => rows.map(row => ({
    country: row.querySelector('[data-field="country"]').value,
    min_importance: row.querySelector('[data-field="min_importance"]').value,
  })));
  expect(restored).toEqual(expect.arrayContaining([
    { country: 'kr', min_importance: 'all' }, { country: 'us', min_importance: 'mid' }, { country: 'jp', min_importance: 'high' },
  ]));
  await page.locator('#econCalAlertSettings').scrollIntoViewIfNeeded();
  await page.locator('#econCalSection').screenshot({ path: testInfo.outputPath('calendar-alert-rules-desktop.png') });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.locator('#econCalAlertSettings').scrollIntoViewIfNeeded();
  expect(await page.locator('#econCalAlertSettings').evaluate(el => el.scrollWidth <= el.clientWidth)).toBe(true);
  await page.screenshot({ path: testInfo.outputPath('calendar-alert-rules-mobile.png') });
  await page.getByRole('button', { name: '일본 알림 조건 삭제' }).click();
  await page.locator('#econCalRuleSave').click();
  await expect(page.locator('#econCalRuleStatus')).toHaveText('저장했습니다.');
  await expect(page.locator('.ec-rule-badge')).toHaveCount(2);
  const individual = page.locator('.ec-bell:has(input[data-eid="jp-high"])');
  await expect(individual).toBeVisible();
  await individual.click();
  await expect(page.locator('.ec-bell-cb[data-eid="jp-high"]')).toBeChecked();
});
