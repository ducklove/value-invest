import {test,expect} from '@playwright/test';

for (const [provider,name,number] of [['kiwoom','키움증권','1234567801'],['ls','LS증권','12345678901']]) {
  test(`${name} 계좌 확인·미리보기 취소·연결·현금 합산·연결 해제`, async ({page},testInfo)=>{
    await page.routeWebSocket('**/ws/broker-accounts',()=>{});
    await page.routeWebSocket('**/ws/quotes',()=>{});
    await page.route('**/*',route=>new URL(route.request().url()).hostname==='127.0.0.1'?route.continue():route.abort());
    const headers={Origin:'http://127.0.0.1:18765'};
    await page.request.post('/api/auth/register',{data:{email:`${provider}-${Date.now()}@example.com`,name:'증권사 검증',password:'browser-test-password'},headers});
    const account=await (await page.request.post('/api/portfolio/accounts',{data:{name},headers})).json();
    await page.goto('/portfolio');
    await page.locator('#pfAccountsOpen').click();
    const card=page.locator(`[data-account="${account.account_id}"]`);
    const openPreview=async()=>{
      await card.locator('[data-account-action="connect"]').click();
      await page.locator('#pfBrokerProvider').selectOption(provider);
      await expect(page.locator('#pfKisAccount')).not.toBeVisible();
      await page.locator('#pfNhKey').fill(`${provider}-${testInfo.testId}`);
      await page.locator('#pfNhSecret').fill('browser-test-secret');
      await page.locator('#pfNhVerify').click();
      await expect(page.locator('#pfNhChoices')).toContainText(number);
      await expect(page.locator('#pfNhKey')).toHaveValue('');
      await page.locator('#pfNhPreviewButton').click();
      await expect(page.locator('#pfNhSave')).toBeEnabled();
      await expect(page.locator('#pfNhPreview')).toContainText('800.5');
      await expect(page.locator('#pfNhPreview')).toContainText('9,000');
    };
    await openPreview();
    await page.locator('#pfNhClose').click();
    let saved=(await (await page.request.get('/api/portfolio/accounts')).json()).find(row=>row.account_id===account.account_id);
    expect(saved.broker).toBeNull(); expect(saved.holdings_count).toBe(0);
    await openPreview();
    await page.setViewportSize({width:390,height:844});
    expect(await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth)).toBe(false);
    await page.locator('#pfNhDialog').screenshot({path:testInfo.outputPath(`${provider}-mobile.png`)});
    await page.locator('#pfNhSave').click();
    await expect(page.locator('#pfNhDialog')).not.toBeVisible();
    await expect(card).toContainText(`${name} ${number}`);
    const before=await (await page.request.get(`/api/portfolio?account_id=${account.account_id}`)).json();
    expect(before.find(row=>row.stock_code==='CASH_USD').quantity).toBe(800.5);
    expect(before.find(row=>row.stock_code==='CASH_KRW').quantity).toBe(9000);
    expect(before.find(row=>row.stock_code==='900180')).toBeUndefined();
    if(provider==='ls') expect(before.find(row=>row.stock_code==='CMA_RP_KRW').quantity).toBe(1020);
    await card.locator('[data-account-action="sync"]').click();
    await expect(page.locator('#pfAccountsStatus')).toContainText('반영했습니다');
    const synced=await (await page.request.get(`/api/portfolio?account_id=${account.account_id}`)).json();
    const balances=rows=>rows.map(({stock_code,quantity,avg_price,currency})=>({stock_code,quantity,avg_price,currency}));
    expect(balances(synced)).toEqual(balances(before));
    page.once('dialog',dialog=>dialog.accept());
    await card.locator('[data-account-action="disconnect"]').click();
    await expect(card.locator('[data-account-action="connect"]')).toBeVisible();
    const after=await (await page.request.get(`/api/portfolio?account_id=${account.account_id}`)).json();
    expect(after).toEqual(synced);
  });
}
