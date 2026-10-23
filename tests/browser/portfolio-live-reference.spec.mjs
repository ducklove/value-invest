import {test,expect} from '@playwright/test';

for (const width of [1440,390]) {
test(`구글 현재가만 먼저 와도 기준가 보완 뒤 일간 변동률이 채워지고 다음 체결에 유지된다 (${width}px)`,async ({page},testInfo) => {
  await page.setViewportSize({width,height:1000});
  await page.route('**/*',route => new URL(route.request().url()).hostname==='127.0.0.1' ? route.continue() : route.abort());
  const at=new Date().toISOString();
  const live={code:'GOOGL',source:'toss_ws',date:at.slice(0,10),as_of:at,price:324.19*1322.4,
    original_price:324.19,original_currency:'USD',currency:'KRW',fx_rate:1322.4,
    reference_session:at.slice(0,10),previous_close:null,change:null,change_pct:null};
  await page.route(/\/api\/portfolio(?:\?.*)?$/,route => route.fulfill({json:[{
    stock_code:'GOOGL',stock_name:'구글',quantity:1,avg_price:258.96,avg_price_currency:'USD',avg_price_krw:342449,
    currency:'USD',group_name:'해외',quote:live,created_at:at,updated_at:at,
  }]}));
  await page.route('**/api/asset-quotes',route => route.fulfill({json:{GOOGL:live}}));
  let socket;
  await page.routeWebSocket('**/ws/quotes',ws => {
    socket=ws;
    ws.send(JSON.stringify({type:'ws_status',shared:true,active:true,stream_state:'connected'}));
    ws.onMessage(raw => {
      const message=JSON.parse(raw);
      if(message.action==='ping') ws.send(JSON.stringify({type:'pong'}));
      if(message.action==='subscribe') ws.send(JSON.stringify({type:'subscriptions',shared:true,ws:['GOOGL'],rest:[]}));
    });
  });
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button',{name:'이메일로 로그인'}).click();
  const row=page.locator('#pfTable tr[data-code="GOOGL"]');
  await expect(row).toBeVisible();
  await page.waitForFunction(() => QuoteManager.sharedMode && QuoteManager.wsCodes.has('GOOGL'));
  await expect(row.locator('.pf-col-curprice')).toHaveText('428,709');
  await expect(row.locator('.pf-col-changepct')).toHaveText('-');
  const hydrated={...live,previous_close:324.22*1322.4,change:(324.19-324.22)*1322.4,
    change_pct:(324.19/324.22-1)*100,original_previous_close:324.22,reference_date:'2026-10-22',reference_source:'yahoo_daily'};
  socket.send(JSON.stringify({type:'quote',...hydrated}));
  await expect(row.locator('.pf-col-changepct')).toContainText('-0.01%');
  await expect(row.locator('.pf-col-changepct [title]')).toHaveAttribute('title',/2026-10-22 정규장 종가 기준 · USD/);
  // A new FX rate scales price and base together, without becoming stock return.
  socket.send(JSON.stringify({type:'quote',...hydrated,as_of:new Date().toISOString(),original_price:325,
    fx_rate:1400,price:325*1400,previous_close:324.22*1400,change:(325-324.22)*1400,change_pct:(325/324.22-1)*100}));
  await expect(row.locator('.pf-col-changepct')).toContainText('+0.24%');
  await expect(row.locator('.pf-col-curprice')).toHaveText('455,000');
  await row.screenshot({path:testInfo.outputPath('googl-daily-change.png')});
});
}
