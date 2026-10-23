import { test, expect } from '@playwright/test';

for (const width of [1440,390]) {
test(`게스트 지수·환율은 사이드바·투자정보에 반영되고 차트를 유지한다 (${width}px)`, async ({page},testInfo) => {
  await page.setViewportSize({width,height:1000});
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  const catalog = {KOSPI:{label:'KOSPI',category:'국내 지수'},KOSDAQ:{label:'KOSDAQ',category:'국내 지수'},
    USD_KRW:{label:'달러/원',category:'환율'},JPY_KRW:{label:'엔/원',category:'환율'}};
  await page.addInitScript(codes => localStorage.setItem('market_bar_codes',JSON.stringify(codes)),Object.keys(catalog));
  await page.route('**/api/market-indicators', route => route.fulfill({json:catalog}));
  await page.route('**/api/settings/market-bar', route => route.fulfill({json:{codes:Object.keys(catalog)}}));
  const old = {value:'2,800.00',as_of:new Date(Date.now()-10000).toISOString()};
  await page.route('**/api/market-summary?*', route => route.fulfill({json:{KOSPI:old,KOSDAQ:{...old,value:'840.00'}}}));
  let socket;
  await page.routeWebSocket('**/ws/quotes', ws => {
    socket = ws;
    ws.send(JSON.stringify({type:'ws_status',shared:true,active:true,guest:true,stream_state:'connecting'}));
    ws.onMessage(raw => {if (JSON.parse(raw).action === 'ping') ws.send(JSON.stringify({type:'pong'}));});
  });
  await page.goto('/investing');
  await expect(page.locator('.md-index-quote[data-index-code="KOSPI"]')).toBeVisible();
  await page.waitForFunction(() => QuoteManager.sharedMode);
  await page.evaluate(() => {window.indexFrameBefore = document.querySelector('iframe[data-md-frame-index="ekospi"]');});
  const tick = {value:'2,900.00',change:'200.00',change_pct:'7.41%',direction:'up',as_of:new Date().toISOString(),update_mode:'polling',refresh_seconds:5};
  const push = data => socket.send(JSON.stringify({type:'market_indicators',data}));
  push({KOSPI:tick,KOSDAQ:{...tick,value:'850.00'}});
  await expect(page.locator('#marketBar .mi-val').first()).toHaveText('2,900.00');
  await expect(page.locator('[data-index-code="KOSPI"] .md-index-value')).toHaveText('2,900.00');
  await expect(page.locator('[data-index-code="KOSPI"] small')).toHaveText('5초 갱신');
  await expect(page.locator('[data-index-code="KOSDAQ"] .md-index-value')).toHaveText('850.00');
  await expect(page.locator('#marketBar .mi-val[data-idx="1"]')).toHaveText('850.00');
  // FX arrives independently of indices and keeps the provider's actual time.
  const fxTick = {...tick,value:'1,391.50',change:'2.50',change_pct:'0.18%',as_of:new Date(Date.now()-300000).toISOString()};
  await page.evaluate(() => {
    window.cashTicks = {};
    const original = QuoteManager.onQuote;
    QuoteManager.onQuote = (code,q) => {window.cashTicks[code] = q; original(code,q);};
  });
  push({USD_KRW:fxTick,JPY_KRW:{...fxTick,value:'848.16',change:'2.00'}});
  await expect(page.locator('.md-row[data-indicator-code="USD_KRW"] .md-row-val')).toHaveText('1,391.50');
  await expect(page.locator('.md-row[data-indicator-code="JPY_KRW"] .md-row-val')).toHaveText('848.16');
  await expect(page.locator('#marketBar .mi-val[data-idx="3"]')).toHaveText('848.16');
  await expect(page.locator('.md-row[data-indicator-code="USD_KRW"]')).toHaveAttribute('title',/5초 갱신.*시세 기준.*KST/);
  expect(await page.evaluate(() => PfStore.currency.fxRate)).toBe(1391.5);
  const yen = await page.evaluate(() => window.cashTicks.CASH_JPY);
  expect(yen.price).toBe(8.4816);
  expect(yen.change).toBe(.02);
  expect(yen.as_of).toBe(fxTick.as_of);
  push({USD_KRW:{...fxTick,_stale:true}});
  await expect(page.locator('.md-row[data-indicator-code="USD_KRW"]')).toHaveClass(/is-stale/);
  push({USD_KRW:fxTick});
  await expect(page.locator('.md-row[data-indicator-code="USD_KRW"]')).not.toHaveClass(/is-stale/);
  await page.evaluate(() => loadMarketSummary());
  await expect(page.locator('#marketBar .mi-val').first()).toHaveText('2,900.00');
  await expect(page.locator('.md-row[data-indicator-code="USD_KRW"] .md-row-val')).toHaveText('1,391.50');
  expect(await page.evaluate(() => window.indexFrameBefore === document.querySelector('iframe[data-md-frame-index="ekospi"]'))).toBe(true);
  await page.locator('[data-index-code="KOSPI"]').screenshot({path:testInfo.outputPath('index-refresh.png')});
  push({KOSPI:{...tick,_stale:true}});
  await expect(page.locator('[data-index-code="KOSPI"] small')).toHaveText('갱신 지연');
  push({KOSPI:{...tick,value:'2,901.00'}});
  await expect(page.locator('[data-index-code="KOSPI"] small')).toHaveText('5초 갱신');
});
}

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
