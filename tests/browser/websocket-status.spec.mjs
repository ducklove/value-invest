import { test, expect } from '@playwright/test';

for (const width of [1440, 390]) {
test(`공통 실시간 시세는 자동 연결하고 토스 수신·조회 보완을 표시한다 (${width}px)`, async ({ page }, testInfo) => {
  await page.setViewportSize({width, height:1000});
  let quoteSocket;
  let accountSocket;
  await page.routeWebSocket('**/ws/quotes', socket => {
    quoteSocket = socket;
    socket.send(JSON.stringify({type:'ws_status', shared:true, active:true, stream_state:'connecting'}));
    socket.onMessage(raw => {
      const message = JSON.parse(raw);
      if (message.action === 'ping') socket.send(JSON.stringify({type:'pong', stream_state:'connected'}));
      if (message.action === 'subscribe') {
        const codes = Object.values(message.requested).flat();
        socket.send(JSON.stringify({type:'subscriptions', shared:true, ws:codes, rest:[]}));
        socket.send(JSON.stringify({type:'stream_status', shared:true, stream_state:'connected', slots_connected:1,
          requested:codes.length, subscribed:codes.length, receiving:0, fallback:0,
          sources:[{provider:'toss', requested:codes.length, subscribed:codes.length, state:'subscribed'}]}));
      }
    });
  });
  await page.routeWebSocket('**/ws/broker-accounts', socket => {accountSocket = socket;});
  await page.route('**/*', route => new URL(route.request().url()).hostname === '127.0.0.1' ? route.continue() : route.abort());
  await page.goto('/login?return_to=/portfolio');
  await page.locator('#loginEmail').fill('browser@example.com');
  await page.locator('#loginPassword').fill('browser-test-password');
  await page.getByRole('button', {name:'이메일로 로그인'}).click();
  await expect(page).toHaveURL(/\/portfolio$/);
  const row = page.locator('#pfTable tr[data-code="005930"]');
  await expect(row).toBeVisible();
  await expect(page.locator('#pfWsToggle')).toBeHidden();
  await expect(page.locator('#pfWsStatus')).toContainText('실시간');
  await expect(page.locator('#pfWsDetail')).toContainText('토스');
  await expect(row.locator('.ws-live-dot')).toHaveCount(0);
  const at = new Date().toISOString();
  quoteSocket.send(JSON.stringify({type:'quote',code:'005930',price:70001,previous_close:69000,source:'toss_ws',
    as_of:at,date:at.slice(0,10),market:'UN',currency:'KRW'}));
  await expect(row.locator('.ws-live-dot')).toHaveCount(1);
  await page.locator('.pf-realtime-status').screenshot({path:testInfo.outputPath('shared-connections.png')});
  quoteSocket.send(JSON.stringify({type:'subscriptions',shared:true,ws:[],rest:['005930']}));
  quoteSocket.send(JSON.stringify({type:'stream_status',stream_state:'connecting',subscribed:0,receiving:0,fallback:1,
    requested:1,slots_connected:0,disconnected_codes:['005930']}));
  await expect(row.locator('.ws-live-dot')).toHaveCount(0);
  await expect(page.locator('#pfWsStatus')).toContainText('조회 시세 보완');
  await page.evaluate(() => {PfAccounts.rows = [{account_id:'status-test',broker:'namuh'}]; pfConnectNamuhQuotes();});
  await expect.poll(() => !!accountSocket).toBe(true);
  accountSocket.send(JSON.stringify({type:'namuh_status',state:'degraded',domestic:{response_code:'WSS10015'}}));
  await expect(page.locator('#pfNhQuoteState')).toHaveText('');
});
}

for (const width of [1440, 390]) {
test(`KIS·NH 연결 상태를 분리하고 모바일에서도 표시한다 (${width}px)`, async ({ page }, testInfo) => {
  await page.setViewportSize({width, height: 1000});
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
  await expect(page.locator('#pfWsStatus')).toBeVisible();
  await expect(page.locator('#pfWsStatus')).toHaveText('KIS 미연결');
  await expect(page.locator('#pfWsToggle')).toBeHidden();
  await page.waitForFunction(() => QuoteManager.connected);
  await page.evaluate(() => { QuoteManager.setManualControlAllowed(true); QuoteManager.requestActive(); });
  const sendKis = message => kisSocket.send(JSON.stringify(message));
  sendKis({type: 'ws_status', active: true, stream_state: 'connecting', slots_connected: 0});
  sendKis({type: 'subscriptions', ws: ['005930'], rest: []});
  await expect(page.locator('#pfWsStatus')).toContainText('KIS 연결 중');
  await expect(row.locator('.ws-live-dot')).toHaveCount(0);
  sendKis({type: 'stream_status', stream_state: 'connected', slots_connected: 1});
  await expect(page.locator('#pfWsStatus')).toContainText('체결 대기');
  const now = new Date();
  sendKis({type: 'quote', code: '005930', price: 70001, previous_close: 69000, source: 'ws',
    as_of: now.toISOString(), date: now.toISOString().slice(0, 10), ts: now.getTime() / 1000, market: 'UN', currency: 'KRW'});
  await expect(page.locator('#pfWsStatus')).toHaveText('KIS 연결됨 · 시세 수신 중');
  await expect(page.locator('#pfWsDetail')).toContainText('최근 체결');
  await page.locator('.pf-realtime-status').screenshot({path: testInfo.outputPath('connections.png')});
  await expect(row.locator('.ws-live-dot')).toHaveCount(1);
  sendKis({type: 'stream_status', stream_state: 'reconnecting', slots_connected: 0});
  await expect(page.locator('#pfWsStatus')).toContainText('KIS 재연결 중');
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
  sendNh({state: 'degraded', domestic: {reason: 'subscription_rejected', response_code: 'WSS10015'}});
  await expect(page.locator('#pfNhQuoteState')).toContainText('세션 한도 초과');
  sendNh({state: 'subscribed', foreign: {state: 'waiting', reason: 'scanner_reserved'}});
  await expect(page.locator('#pfNhQuoteState')).toContainText('해외는 조회 시세 사용');
  await expect(page.locator('#pfNhQuoteState')).not.toContainText('불안정');
  sendNh({state: 'live'});
  await expect(page.locator('#pfNhQuoteState')).toContainText('실시간');
  await expect(page.locator('#pfWsStatus')).toHaveText('KIS 재연결 중');
  nhSocket.close();
  await expect(page.locator('#pfNhQuoteState')).toContainText('전달 재연결');
  await expect(page.locator('#pfNhQuoteState')).not.toContainText('실시간');
});
}
