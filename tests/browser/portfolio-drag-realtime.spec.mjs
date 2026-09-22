import { test, expect } from '@playwright/test';

async function prepare(page) {
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.goto('/login');
  const headers = {Origin: 'http://127.0.0.1:18765'};
  const signup = await page.request.post('/api/auth/register', {headers, data: {
    email: `drag-${Date.now()}@example.com`, name: '순서 검증', password: 'browser-test-password',
  }});
  expect(signup.ok()).toBe(true);
  for (const code of ['000660', '005930', 'CASH_KRW']) {
    expect((await page.request.put(`/api/portfolio/${code}`, {headers,
      data: {stock_name: code, quantity: 10, avg_price: 1000}})).ok()).toBe(true);
  }
  await page.goto('/portfolio');
  await expect(page.locator('#pfBody tr')).toHaveCount(3);
  await expect.poll(() => page.evaluate(() => PfStore.loading)).toBe(false);
}

async function startDrag(page) {
  const source = page.locator('#pfBody tr').last();
  const target = page.locator('#pfBody tr').first();
  const code = await source.getAttribute('data-code');
  await source.locator('.js-pf-row-drag').scrollIntoViewIfNeeded();
  const from = await source.locator('.js-pf-row-drag').boundingBox();
  const to = await target.locator('.js-pf-row-drag').boundingBox();
  await page.mouse.move(from.x + from.width / 2, from.y + from.height / 2);
  await page.mouse.down();
  await page.mouse.move(to.x + to.width / 2, to.y + 2, {steps: 8});
  await expect(source).toHaveClass(/dragging/);
  return {code, to};
}

test('시세 갱신 중 긴 드래그도 행을 유지하고 합산 순서를 저장한다', async ({page}) => {
  await prepare(page);
  await page.locator('#pfCompactToggle').check();
  const {code, to} = await startDrag(page);
  const survived = await page.evaluate(async () => {
    const row = document.querySelector('#pfBody .dragging');
    // 실제 시세 수신 후 실행되는 지연 렌더: 기존 450ms 포인터 보호보다 길게 끈다.
    _schedulePortfolioDeferredRender(600);
    await new Promise(resolve => setTimeout(resolve, 1300));
    return row.isConnected;
  });
  expect(survived).toBe(true);
  const saved = page.waitForResponse(r => r.request().method() === 'PUT' && r.url().endsWith('/portfolio/order'));
  await page.mouse.move(to.x + to.width / 2, to.y + 2);
  await page.mouse.move(to.x + to.width / 2, to.y + 2);
  await page.mouse.up();
  expect((await saved).ok()).toBe(true);
  await expect(page.locator('#pfBody tr').first()).toHaveAttribute('data-code', code);
  await page.reload();
  await expect(page.locator('#pfBody tr').first()).toHaveAttribute('data-code', code);
});

test('계좌 조회·요약 갱신 중 드래그 취소 후 밀린 화면을 반영하고 다시 이동한다', async ({page}) => {
  await prepare(page);
  const before = await page.locator('#pfBody tr').evaluateAll(rows => rows.map(row => row.dataset.code));
  await startDrag(page);
  expect(await page.evaluate(async () => {
    const row = document.querySelector('#pfBody .dragging');
    await loadPortfolio({force: true});
    renderPortfolio({summaryOnly: true});
    return row.isConnected;
  })).toBe(true);
  await page.keyboard.press('Escape');
  await page.mouse.up();
  await expect.poll(() => page.evaluate(() => PfStore.manualOrder.draggingCode)).toBe(null);
  await expect.poll(() => page.evaluate(() => PfStore.manualOrder.renderPending)).toBe(false);
  expect(await page.locator('#pfBody tr').evaluateAll(rows => rows.map(row => row.dataset.code))).toEqual(before);
  const {code, to} = await startDrag(page);
  const saved = page.waitForResponse(r => r.request().method() === 'PUT' && r.url().endsWith('/portfolio/order'));
  await page.mouse.move(to.x + to.width / 2, to.y + 2);
  await page.mouse.move(to.x + to.width / 2, to.y + 2);
  await page.mouse.up();
  expect((await saved).ok()).toBe(true);
  await expect(page.locator('#pfBody tr').first()).toHaveAttribute('data-code', code);
});

test('앞선 순서 저장 응답이 다음 드래그 도중 도착해도 연속 이동을 저장한다', async ({page}) => {
  await prepare(page);
  let release, received;
  const firstResponseReady = new Promise(resolve => {received = resolve;});
  const holdResponse = new Promise(resolve => {release = resolve;});
  let saves = 0;
  await page.route('**/api/portfolio/order', async route => {
    if (++saves !== 1) return route.continue();
    const response = await route.fetch();
    received();
    await holdResponse;
    await route.fulfill({response});
  });
  await page.locator('#pfBody tr').last().locator('.js-pf-row-drag').dragTo(
    page.locator('#pfBody tr').first().locator('.js-pf-row-drag'), {targetPosition: {x: 12, y: 2}});
  await firstResponseReady;
  const {code, to} = await startDrag(page);
  const row = await page.locator('#pfBody .dragging').elementHandle();
  release();
  await expect.poll(() => page.evaluate(() => PfStore.manualOrder.saveInFlight)).toBe(false);
  expect(await row.evaluate(el => el.isConnected)).toBe(true);
  const saved = page.waitForResponse(r => r.request().method() === 'PUT' && r.url().endsWith('/portfolio/order'));
  await page.mouse.move(to.x + to.width / 2, to.y + 2);
  await page.mouse.move(to.x + to.width / 2, to.y + 2);
  await page.mouse.up();
  expect((await saved).ok()).toBe(true);
  await expect(page.locator('#pfBody tr').first()).toHaveAttribute('data-code', code);
  await page.reload();
  await expect(page.locator('#pfBody tr').first()).toHaveAttribute('data-code', code);
});
