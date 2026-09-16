import {test,expect} from '@playwright/test';

test('순회 감시가 첫 화면이고 전체 검색·설정·정지가 동작한다',async({page})=>{
  await page.route('**/*',r=>new URL(r.request().url()).hostname==='127.0.0.1'?r.continue():r.abort());
  await page.route('**/api/quant/capabilities',r=>r.fulfill({json:{pairs:[]}}));
  let data={accounts:[{account_id:'a1',name:'검증용 나무',environment:'mock'}],config:null,
    progress:{total:1734,cursor:100,state:'scanning'},runtime:{watched:['KA0A6C000'],requested:2,approved:2},
    rows:[{contract:'KA0A6C000',contract_name:'검증 선물 12월',spot_code:'005930',name:'삼성전자',observed_at:1789500000,net_bps:45,gross_bps:90,spot:{ask:70000},future:{bid:70630}},
          {contract:'KA0B6C000',spot_code:'000660',name:'SK하이닉스',error:'아직 관측하지 않음'}],events:[]};
  await page.route('**/api/quant/scanner',r=>{
    if(r.request().method()==='PUT')data.config=r.request().postDataJSON();
    return r.fulfill({json:data});
  });
  await page.route('**/api/quant/scanner/stop',r=>{data.config.enabled=false;return r.fulfill({json:data});});
  expect((await page.request.get('/api/quant/scanner')).status()).toBe(401);
  expect((await page.request.post('/api/quant/scanner/stop')).status()).toBe(401);
  await page.goto('/login?return_to=/quant');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button',{name:'이메일로 로그인'}).click();
  await expect(page.locator('#quantScanner')).toBeVisible();
  await expect(page.locator('#basisForm')).not.toBeVisible();
  await expect(page.locator('#scannerCoverage')).toContainText('전체 1734계약');
  await page.locator('#scannerSearch').fill('005930');
  await expect(page.locator('#scannerRows')).toContainText('삼성전자');
  await expect(page.locator('#scannerRows')).not.toContainText('SK하이닉스');
  await page.getByText('계좌·순회·감시 기준',{exact:true}).click();
  await page.locator('#scannerForm [name=account_id]').selectOption('a1');
  await page.locator('#scannerStart').click();
  await expect(page.locator('#scannerMessage')).toContainText('설정을 저장');
  expect(data.config.interval_minutes).toBe(60);
  expect(data.config.future_fee_bps).toBe(0.6);
  await page.locator('#scannerStop').click();
  await expect(page.locator('#scannerMessage')).toContainText('중지를 요청');
  await page.setViewportSize({width:390,height:844});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  await page.screenshot({path:'test-results/scanner-mobile.png',fullPage:true});
});
