import {test,expect} from '@playwright/test';

test('접속 시 예수금 사유 입력, 임시 입력 보존, 나중에 입력 및 모바일 이력 조회', async ({page},testInfo) => {
  await page.routeWebSocket('**/ws/quotes',()=>{});
  await page.route('**/*',route=>new URL(route.request().url()).hostname === '127.0.0.1'?route.continue():route.abort());
  const headers={Origin:'http://127.0.0.1:18765'};
  await page.request.post('/api/auth/register',{data:{email:`cash-review-${Date.now()}@example.com`,name:'예수금 검증',password:'browser-test-password'},headers});
  const {categories}=await (await page.request.get('/api/portfolio/cash-reviews')).json();
  const items=[{id:1,account_name:'연동 계좌',from_at:'2026-10-17T09:00:00',to_at:'2026-10-17T09:01:00',currency:'KRW',amount:100000,unexplained:100000,revision:1,category:'',reason:''},
    {id:2,account_name:'해외 계좌',from_at:'2026-10-17T09:00:00',to_at:'2026-10-17T09:01:00',currency:'USD',amount:-25.5,unexplained:-25.5,revision:1,category:'',reason:'',positions_changed:true}];
  await page.route('**/api/portfolio/cash-reviews**',async route=>{
    const request=route.request();
    if(request.method()==='PATCH') {
      const item=items.find(row=>String(row.id)===new URL(request.url()).pathname.split('/').at(-1));
      Object.assign(item,request.postDataJSON()); item.revision++;
      await route.fulfill({json:{ok:true}}); return;
    }
    const pending=new URL(request.url()).searchParams.get('pending') !== 'false';
    const rows=items.filter(row=>!pending || !row.category);
    await route.fulfill({json:{items:rows,total:rows.length,has_more:false,categories}});
  });
  await page.goto('/portfolio');
  const dialog=page.locator('#pfCashReviewDialog');
  await expect(dialog).toBeVisible();
  await expect(dialog.locator('form')).toHaveCount(2);
  const first=dialog.locator('form').nth(0), second=dialog.locator('form').nth(1);
  await second.locator('textarea').fill('분배금 인출 확인 중');
  await first.locator('select').selectOption('external_deposit');
  await first.locator('textarea').fill('투자금 추가');
  await first.getByRole('button',{name:'사유 저장',exact:true}).click();
  await expect(page.locator('#pfCashReviewStatus')).toContainText('저장했습니다');
  await expect(second.locator('textarea')).toHaveValue('분배금 인출 확인 중');
  await expect(page.locator('#pfCashReviewNotice')).toContainText('1건');
  await second.locator('select').selectOption('distribution');
  await second.getByRole('button',{name:'사유 저장',exact:true}).click();
  await expect(page.locator('#pfCashReviewNotice')).toBeHidden();
  await page.locator('#pfCashReviewMode').click();
  await expect(page.locator('#pfCashReviewStatus')).toContainText('전체 2건');
  await page.setViewportSize({width:390,height:844});
  await expect(second.locator('textarea')).toHaveValue('분배금 인출 확인 중');
  expect(await dialog.evaluate(el=>el.scrollWidth > el.clientWidth+1)).toBe(false);
  await dialog.screenshot({path:testInfo.outputPath('cash-reviews-mobile.png')});
  await page.locator('#pfCashReviewClose').click();
  await expect(dialog).toBeHidden();
  await page.locator('#pfAccountsOpen').click();
  await page.locator('#pfCashReviewsOpen').click();
  await expect(dialog).toContainText('사유를 입력할 예수금 변동이 없습니다');
  await page.locator('#pfCashReviewMode').click();
  await expect(dialog).toContainText('투자금 추가');
});

test('소액 자동처리는 접속 시 입력 창을 열지 않고 전체 이력에서 선택적으로 확인한다', async ({page},testInfo) => {
  await page.routeWebSocket('**/ws/quotes',()=>{});
  await page.route('**/*',route=>new URL(route.request().url()).hostname === '127.0.0.1'?route.continue():route.abort());
  const headers={Origin:'http://127.0.0.1:18765'};
  await page.request.post('/api/auth/register',{data:{email:`cash-minor-${Date.now()}@example.com`,name:'소액 검증',password:'browser-test-password'},headers});
  const {categories}=await (await page.request.get('/api/portfolio/cash-reviews')).json();
  const item={id:1,account_name:'예수금 계좌',from_at:'2026-10-22T09:00:00',to_at:'2026-10-22T09:01:00',currency:'KRW',
    amount:315,unexplained:315,revision:1,category:'',reason:'',auto_resolution:'minor',minor_limit:1000,review_required:false};
  let checked=false;
  await page.route('**/api/portfolio/cash-reviews**',async route=>{
    const pending=new URL(route.request().url()).searchParams.get('pending') !== 'false';
    if(pending) checked=true;
    const rows=pending?[]:[item];
    await route.fulfill({json:{items:rows,total:rows.length,has_more:false,categories}});
  });
  await page.goto('/portfolio');
  await expect.poll(()=>checked).toBe(true);
  await expect(page.locator('#pfCashReviewDialog')).toHaveCount(0);
  await expect(page.locator('#pfCashReviewNotice')).toHaveCount(0);
  await page.locator('#pfAccountsOpen').click();
  await page.locator('#pfCashReviewsOpen').click();
  await expect(page.locator('#pfCashReviewDialog')).toContainText('사유를 입력할 예수금 변동이 없습니다');
  await page.locator('#pfCashReviewMode').click();
  await expect(page.locator('#pfCashReviewRows')).toContainText('소액 자동처리 315 KRW');
  await expect(page.locator('#pfCashReviewRows select')).toBeHidden();
  await page.setViewportSize({width:390,height:844});
  const dialog=page.locator('#pfCashReviewDialog');
  expect(await dialog.evaluate(el=>el.scrollWidth > el.clientWidth+1)).toBe(false);
  await dialog.screenshot({path:testInfo.outputPath('minor-cash-review-mobile.png')});
  await page.getByText('직접 사유 기록 (선택)',{exact:true}).click();
  await expect(page.locator('#pfCashReviewRows select')).toBeVisible();
});
