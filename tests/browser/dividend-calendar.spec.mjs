import { test, expect } from '@playwright/test';

test('배당 캘린더는 지급·권리일과 출처를 구분하고 모바일 수취 입력을 연결한다', async ({ page }, testInfo) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  const payment = { stock_code: 'AGNC', stock_name: 'AGNC', date: '2026-09-10', type: 'payment', date_kind: 'payment',
    label: '월배당 · 지급일', pay_date: '2026-09-10', ex_date: '2026-08-31', record_date: '2026-08-31',
    confirmed: true, date_status: 'announced', currency: 'USD', amount_per_share: 0.12, shares: 10, expected_amount_krw: 1680,
    source: 'AGNC 공시', source_url: 'https://investors.agnc.com/stock-information/dividend-history',
    fetched_at: '2026-09-10T00:00:00Z', source_key: 'AGNC:ex_date:2026-08-31', receiptable: true };
  const estimated = { ...payment, stock_code: 'SCHP', stock_name: 'SCHP', date: '2026-09-20', pay_date: '2026-09-20',
    label: '월배당 · 지급일 (예상)', source: 'Schwab 공시', source_url: 'https://www.schwabassetmanagement.com/products/schp',
    type: 'estimated', date_precision: 'approximate', confirmed: false, receiptable: false, source_key: null };
  const pending = { ...payment, stock_code: '005935', stock_name: '삼성전자우', date: '2026-09-30',
    type: 'record_date', date_kind: 'record_date', record_date: '2026-09-30', ex_date: null, pay_date: null,
    frequency: 'quarterly', amount_status: 'unknown', amount_per_share: null, expected_amount_krw: null,
    currency: 'KRW', shares: 100, holding_basis: 'snapshot', holding_as_of: '2026-09-23',
    verification: null, receiptable: false, source: 'KIS·예탁원 배당 일정' };
  const matched = { ...payment, stock_code: 'AAA.AX', stock_name: '호주단기채', currency: 'AUD', shares: 100,
    source: 'Yahoo 배당락 이력', source_url: 'https://finance.yahoo.com/', confirmed: false, date_status: 'observed',
    date: '2026-09-01', type: 'ex_date', date_kind: 'ex_date', ex_date: '2026-09-01', record_date: null, pay_date: null,
    amount_per_share: 0.2, verification: 'nh_confirmed', paid_date: '2026-09-15', receiptable: false,
    nh_match: { broker_name: 'NH', date: '2026-09-15', gross_amount: 20, tax_amount: 0, domestic_tax_krw: 2100, net_amount: 20, currency: 'AUD' } };
  const deposit = { ...matched, stock_code: '83188.HK', stock_name: '83188.HK', date: '2026-09-03', paid_date: '2026-09-03',
    date_kind: 'payment', type: 'payment', date_status: 'nh', ex_date: null, record_date: null, pay_date: '2026-09-03',
    currency: 'CNY', amount_per_share: null, shares: null, source: 'NH 거래내역', source_url: null,
    nh_match: { broker_name: 'NH', date: '2026-09-03', gross_amount: 120, tax_amount: 12, net_amount: 108, currency: 'CNY' } };
  await page.route('**/api/portfolio/dividend-calendar?*', route => route.fulfill({ json: {
    as_of: '2026-09-10', start_month: '2026-09', end_month: '2026-09', events: [payment, estimated, pending, matched, deposit],
    monthly: [{ month: '2026-09', count: 5, total_krw: 3360, announced_krw: 1680, estimated_krw: 1680 }],
    summary: { total_expected_krw: 3360, confirmed_count: 1, estimated_count: 1 },
  } }));
  await page.route('**/api/portfolio/dividend-receipts/candidates', route => route.fulfill({ json: { events: [payment] } }));
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page.locator('#pfBody tr[data-code="005930"]')).toBeVisible();
  await page.locator('.pf-tab[data-tab="performance"]').click();
  const calendar = page.locator('#pfDivCalWrap');
  await expect(calendar).toContainText('배당락일 2026-08-31');
  const pendingRow = calendar.locator('.pf-divcal-event').filter({ hasText: '삼성전자우' });
  await expect(pendingRow.locator('.pf-divcal-record-date')).toHaveText('2026-09-30');
  await expect(pendingRow.locator('.pf-divcal-per-share')).toHaveText('미확인');
  await expect(pendingRow.locator('.pf-divcal-quantity')).toContainText('100주');
  await expect(pendingRow).not.toContainText('0원');
  await expect(calendar).toContainText('2026-09-20 전후');
  await expect(calendar.getByRole('button', { name: '수취 입력' })).toHaveCount(1);
  await expect(calendar.locator('a').filter({ hasText: 'AGNC 공시' })).toHaveAttribute('href', payment.source_url);
  await calendar.locator('.pf-divcal-source').first().locator('summary').click();
  await expect(calendar.getByRole('link', { name: 'AGNC 공시' })).toBeVisible();
  await expect(calendar.locator('.pf-divcal-table thead th')).toHaveText(['종목', '배당기준일', '지급일', '배당락 여부', '주당 배당액', '수량', '배당총액', '세금', '지급액']);
  for (const stock of ['호주단기채', '83188.HK']) {
    const row = calendar.locator('.pf-divcal-event').filter({ hasText: stock });
    await expect(row.locator('.pf-divcal-payment-date .broker')).toHaveText('NH');
    await expect(row.locator('.pf-divcal-stock .broker')).toHaveCount(0);
  }
  await expect(calendar.locator('.pf-divcal-event').filter({ hasText: '83188.HK' }).locator('.pf-divcal-net')).toContainText('108 CNY');
  await calendar.screenshot({ path: testInfo.outputPath('calendar-desktop.png') });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.locator('#pfSimpleToggle')).toBeVisible();
  await expect(page.locator('#pfSimpleToggle')).toHaveAttribute('aria-pressed', 'true');
  await page.locator('#pfSimpleToggle').click();
  await expect(page.locator('#pfSimpleToggle')).toHaveAttribute('aria-pressed', 'false');
  await page.locator('.pf-tab[data-tab="performance"]').click();
  await expect(calendar).toBeVisible();
  expect(await calendar.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(false);
  const scroll = calendar.locator('.pf-divcal-table-scroll');
  expect(await scroll.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(true);
  await scroll.evaluate(async el => {
    el.scrollLeft = el.scrollWidth;
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  });
  await expect(calendar.locator('.pf-divcal-event').filter({ hasText: '83188.HK' }).locator('.pf-divcal-net')).toBeVisible();
  await calendar.screenshot({ path: testInfo.outputPath('calendar-mobile.png') });
  await calendar.getByRole('button', { name: '수취 입력' }).click();
  await expect(page.locator('#pfDividendSchedule')).toHaveValue(payment.source_key);
  await expect(page.locator('#pfDividendPerShare')).toHaveValue('0.12');
  await expect(page.locator('#pfDividendDate')).toHaveValue('2026-09-10');
  await expect(page.locator('#pfDividendScheduleNote')).toContainText('공시 지급일');
});
