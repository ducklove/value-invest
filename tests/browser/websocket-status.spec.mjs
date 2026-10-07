import { test, expect } from '@playwright/test';

test('시세 서버 접속과 실제 수신을 구분하고 단절 즉시 배지와 NH 안내를 갱신한다', async ({ page }) => {
  let kisSocket;
  let nhSocket;
  await page.routeWebSocket('**/ws/quotes', socket => {
    kisSocket = socket;
    socket.onMessage(raw => {
      if (JSON.parse(raw).action === 'ping') socket.send(JSON.stringify({type: 'pong'}));
    });
  });
  await page.routeWebSocket('**/ws/broker-accounts', socket => { nhSocket = socket; });
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', {name: '이메일로 로그인'}).click();
  await expect(page).toHaveURL(/\/portfolio$/);
  const row = page.locator('#pfTable tr[data-code="005930"]');
  await expect(row).toBeVisible();
  await page.waitForFunction(() => QuoteManager.connected);
  await page.evaluate(() => { QuoteManager.setManualControlAllowed(true); QuoteManager.requestActive(); });
  const sendKis = message => kisSocket.send(JSON.stringify(message));
  sendKis({type: 'ws_status', active: true, stream_state: 'connecting', slots_connected: 0});
  sendKis({type: 'subscriptions', ws: ['005930'], rest: []});
  await expect(page.locator('#pfWsStatus')).toContainText('시세 서버 연결 중');
  await expect(row.locator('.ws-live-dot')).toHaveCount(0);
  sendKis({type: 'stream_status', stream_state: 'connected', slots_connected: 1});
  await expect(page.locator('#pfWsStatus')).toContainText('체결 대기');
  const now = new Date();
  sendKis({type: 'quote', code: '005930', price: 70001, previous_close: 69000, source: 'ws',
    as_of: now.toISOString(), date: now.toISOString().slice(0, 10), ts: now.getTime() / 1000, market: 'UN', currency: 'KRW'});
  await expect(page.locator('#pfWsStatus')).toContainText('실시간 시세 수신');
  await expect(row.locator('.ws-live-dot')).toHaveCount(1);
  sendKis({type: 'stream_status', stream_state: 'reconnecting', slots_connected: 0});
  await expect(page.locator('#pfWsStatus')).toContainText('시세 재연결 중');
  await expect(row.locator('.ws-live-dot')).toHaveCount(0);
  await page.evaluate(() => {
    PfAccounts.rows = [{account_id: 'status-test', broker: 'namuh'}];
    pfConnectNamuhQuotes();
  });
  await expect.poll(() => Boolean(nhSocket)).toBe(true);
  const sendNh = state => nhSocket.send(JSON.stringify({type: 'namuh_status', ...state}));
  sendNh({state: 'degraded', domestic: {reason: 'subscription_rejected', rejected: 1}});
  await expect(page.locator('#pfNhQuoteState')).toContainText('구독 제한');
  await expect(page.locator('#pfNhQuoteState')).not.toContainText('연결 불안정');
  sendNh({state: 'live'});
  await expect(page.locator('#pfNhQuoteState')).toContainText('실시간');
  nhSocket.close();
  await expect(page.locator('#pfNhQuoteState')).toContainText('전달 재연결');
  await expect(page.locator('#pfNhQuoteState')).not.toContainText('실시간');
});
