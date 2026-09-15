import { test, expect } from '@playwright/test';

for (const sample of [
  { strategy: 'preferred_switch', pair: '005930:005935' },
  { strategy: 'etf_switch', pair: '069500:102110' },
]) {
test(`퀀트 ${sample.strategy} 로그인·저장·새로고침·취소와 모바일 화면`, async ({ page }) => {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.route('**/api/quant/capabilities', route => route.fulfill({ json: {
    pairs: [{ common: '005930', preferred: '005935', name: '삼성전자' }],
    etf_pairs: [{ common: '069500', preferred: '102110', name: 'KODEX 200 / TIGER 200' }],
    readiness: { status: 'ready', checks: { latest_price_date: '2026-09-14' } }, live_enabled: false,
    factor_inputs: {status:'review_required',as_of:'2026-08-28',latest_price_date:'2026-09-14',securities:2581,eligible_securities:6,complete_securities:4,exclusions:{historical_or_unknown_provenance:11567},note:'검증용 입력 감사'},
  } }));
  expect((await page.request.get('/api/quant/runs')).status()).toBe(401);
  expect((await page.request.post('/api/quant/runs/unknown/forward', {data:{enabled:true}})).status()).toBe(401);
  await page.goto('/login?return_to=/quant');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', { name: '이메일로 로그인' }).click();
  await expect(page).toHaveURL(/\/quant$/);
  await expect(page.locator('#quantView')).toBeVisible();
  await page.locator('#quantFactorInputs summary').click();
  await expect(page.locator('#quantFactorInputs')).toContainText('후보 4종목');
  await page.locator('#quantPair').selectOption(sample.pair);
  await page.locator('#quantForm [name=start]').fill('2024-01-01');
  await page.locator('#quantForm [name=end]').fill('2026-09-14');
  const response = page.waitForResponse(r => r.url().endsWith('/api/quant/runs') && r.request().method() === 'POST');
  await page.locator('#quantSubmit').click();
  expect((await response).status()).toBe(202);
  await expect(page.locator('#quantReport')).toContainText('대기');
  await page.reload();
  await page.locator('#quantRuns button').first().click();
  await expect(page.locator('#quantReport')).toContainText('대기');
  await page.getByRole('button', { name: '연구 취소', exact: true }).click();
  await expect(page.locator('#quantReport')).toContainText('취소');
  const saved = await (await page.request.get('/api/quant/runs')).json();
  const row = saved.runs[0];
  expect((await page.request.post(`/api/quant/runs/${row.id}/forward`, {data:{enabled:true},headers:{Origin:'http://127.0.0.1:18765'}})).status()).toBe(400);
  expect(row.config.strategy).toBe(sample.strategy);
  if (sample.strategy === 'etf_switch') expect(row.config.sell_tax_bps).toBe(0);
  const result = {
    config: row.config, config_hash: '검증용 설정', engine_version: sample.strategy === 'etf_switch' ? 'etf-switch-2' : 'preferred-switch-2',
    snapshot: { snapshot_id: '검증용 입력' },
    scenarios: ['switch', 'common', 'preferred', 'mixed'].map((mode, i) => ({
      mode, return_pct: i + 1, max_drawdown_pct: -i, cost: 10000, trade_count: 2,
      nav: [{ date: '2024-01-02', nav: 10000000 }, { date: '2026-09-14', nav: 10000000 * (1 + (i + 1) / 100) }], trades: [],
    })),
    stress: { return_pct: 0.5, excess_return_pct: -0.2 }, latest_signal: { date: '2026-09-14', discount: 0.2, relative_deviation_bps: 2, round_trip_cost_bps: 60, z: 1.2, target: 'common', reason: '유지' },
    validation: {status:'available', positive_excess_periods:1, note:'사후 분할 진단 · 미관측 검증 아님', periods:[
      {start:'2024-01-02',end:'2024-10-01',return_pct:1,benchmark_return_pct:2,excess_return_pct:-1,max_drawdown_pct:-3},
      {start:'2024-10-02',end:'2025-09-01',return_pct:2,benchmark_return_pct:1,excess_return_pct:1,max_drawdown_pct:-2},
      {start:'2025-09-02',end:'2026-09-14',return_pct:1,benchmark_return_pct:2,excess_return_pct:-1,max_drawdown_pct:-4},
    ]},
    limitations: ['화면 검증용 가상 데이터입니다.'],
    liquidity_stress: {note:'신호 고정 · 비용 2배 · 참여율 축소',scenarios:[{participation_multiplier:0.1,return_pct:0.1,excess_return_pct:-1,max_drawdown_pct:-3,ending_cash:300000}]},
  };
  let forward = null;
  await page.route(`**/api/quant/runs/${row.id}`, route => route.fulfill({ json: { ...row, status: 'succeeded', result, forward } }));
  await page.route(`**/api/quant/runs/${row.id}/forward`, route => {
    forward = {status:route.request().postDataJSON().enabled ? 'active' : 'stopped',start_date:'2026-09-17',payload:null};
    return route.fulfill({json:{forward}});
  });
  await page.locator('#quantRuns button').first().click();
  await expect(page.locator('#quantReport svg')).toBeVisible();
  await expect(page.locator('#quantReport')).toContainText('비용 2배');
  await expect(page.locator('#quantReport')).toContainText('기간 분할 진단');
  await expect(page.locator('#quantReport')).toContainText('비용·유동성 스트레스');
  await page.getByRole('button', {name:'이 설정으로 전진 평가 시작',exact:true}).click();
  await expect(page.locator('#quantReport')).toContainText('수익률은 아직 없습니다');
  await page.getByRole('button', {name:'전진 평가 중지',exact:true}).click();
  await expect(page.getByRole('button', {name:'이 설정으로 전진 평가 시작',exact:true})).toHaveCount(0);
  if (sample.strategy === 'etf_switch') {
    await expect(page.locator('#quantReport')).toContainText('ETF A 보유');
    await expect(page.locator('#quantReport')).not.toContainText('최근 할인율');
  }
  await page.screenshot({ path: `test-results/quant-${sample.strategy}-desktop.png`, fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.locator('#quantSubmit')).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: `test-results/quant-${sample.strategy}-mobile.png`, fullPage: true });
});
}
