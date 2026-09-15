import {test,expect} from '@playwright/test';
import {readFileSync} from 'node:fs';

test('현선물 가정·저장 기록·파일 재생·모바일 보고서',async({page})=>{
  await page.route('**/*',r=>new URL(r.request().url()).hostname==='127.0.0.1'?r.continue():r.abort());
  const fixture=JSON.parse(readFileSync(new URL('../fixtures/basis-report.json',import.meta.url),'utf8'));
  let saved=null;
  await page.route('**/api/quant/capabilities',r=>r.fulfill({json:{pairs:[],readiness:null}}));
  await page.route('**/api/quant/basis/runs',r=>{
    if(r.request().method()==='POST'){
      const input=r.request().postDataJSON().input;
      saved={id:'basis-browser',created_at:1789500000,input,result:fixture};
      return r.fulfill({status:201,json:saved});
    }
    return r.fulfill({json:{runs:saved?[{id:saved.id,created_at:saved.created_at,contract:'SYNTHETIC-SSF',mode:'scenario'}]:[]}});
  });
  await page.route('**/api/quant/basis/runs/basis-browser',r=>r.fulfill({json:saved}));
  expect((await page.request.get('/api/quant/basis/runs')).status()).toBe(401);
  await page.goto('/login?return_to=/quant');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button',{name:'이메일로 로그인'}).click();
  await expect(page.locator('#basisForm')).toBeVisible();
  await page.locator('#basisSubmit').click();
  await expect(page.locator('#basisReport')).toContainText('가정별 손익 · 과거 실적 아님');
  expect(saved.input.config.capital).toBe(100000000);
  await expect(page.locator('#basisReport')).toContainText('목표 이익 청산');
  await page.reload();
  await page.getByText('저장한 현선물 연구',{exact:true}).click();
  await page.locator('#basisRuns button').first().click();
  await expect(page.locator('#basisReport')).toContainText('만기 수렴 가정');
  const download=page.waitForEvent('download');
  await page.getByRole('button',{name:'호가 재생 JSON 저장'}).click();
  expect((await download).suggestedFilename()).toContain('quotes');
  await page.getByText('동시 호가 JSON 재생',{exact:true}).click();
  await page.locator('#basisFile').setInputFiles({name:'quotes.json',mimeType:'application/json',buffer:Buffer.from(JSON.stringify(fixture.replay_input))});
  await expect(page.locator('#basisReplay')).toBeEnabled();
  await page.locator('#basisReplay').click();
  await expect(page.locator('#basisMessage')).toContainText('저장했습니다');
  expect(saved.input.mode).toBe('replay');
  await page.screenshot({path:'test-results/basis-desktop.png',fullPage:true});
  await page.setViewportSize({width:390,height:844});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  await page.screenshot({path:'test-results/basis-mobile.png',fullPage:true});
});
