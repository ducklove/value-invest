// jsdom behavior tests for static/js/quote-manager.js — the WebSocket quote
// lifecycle plus its REST polling fallback. Real sources (utils → store →
// quote-manager) load in browser order; WebSocket is a recording mock, apiFetch
// is mocked, and window timers are replaced with a deterministic fake clock so
// reconnect/retry/poll schedules run without real waiting.
// Run with `npm test` (node --test).

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

const SOURCES = [
  read("static", "js", "utils.js"), // quoteIsUsable / recentListItems / apiFetch home
  read("static", "js", "portfolio-store.js"), // PfStore (window-exposed)
  read("static", "js", "quote-manager.js"),
];
// `const QuoteManager` is a global *lexical* binding — visible to sibling
// scripts but not a window property, so bridge it out for the test.
const BRIDGE = "window.QuoteManager = QuoteManager;";

// Deterministic replacement for window.setTimeout/setInterval. quote-manager
// looks the timer functions up on the global at call time, so swapping the
// window properties is enough. tick() yields a macrotask after each fired
// callback so async fetch chains settle between timer fires.
function installFakeTimers(w) {
  const timers = new Map();
  let nextId = 1;
  let now = 0;
  w.setTimeout = (fn, ms = 0, ...args) => {
    const id = nextId++;
    timers.set(id, { fn, args, at: now + ms, every: null });
    return id;
  };
  w.setInterval = (fn, ms = 0, ...args) => {
    const id = nextId++;
    timers.set(id, { fn, args, at: now + ms, every: ms });
    return id;
  };
  w.clearTimeout = (id) => timers.delete(id);
  w.clearInterval = (id) => timers.delete(id);
  async function tick(ms) {
    const end = now + ms;
    for (;;) {
      let dueId = null;
      let dueAt = Infinity;
      for (const [id, t] of timers) {
        if (t.at <= end && t.at < dueAt) { dueAt = t.at; dueId = id; }
      }
      if (dueId === null) break;
      const t = timers.get(dueId);
      now = t.at;
      if (t.every !== null) t.at = now + Math.max(t.every, 1);
      else timers.delete(dueId);
      t.fn(...t.args);
      await new Promise((r) => setImmediate(r));
    }
    now = end;
  }
  return { tick, pending: () => timers.size };
}

const flush = async () => {
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setImmediate(r));
};

// quotes: code -> snapshot returned by the mocked /api/asset-quotes.
function createHarness({ wsThrows = false, quotes = {} } = {}) {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "http://app.example.com/", // http → quote-manager must pick ws://
  });
  const { window: w } = dom;
  const clock = installFakeTimers(w);

  const wsAttempts = { count: 0 };
  class MockWebSocket {
    constructor(url) {
      wsAttempts.count += 1;
      if (wsThrows) throw new Error("ws unavailable");
      this.url = url;
      this.sent = [];
      this.closed = false;
      // Mock에는 실제 핸드셰이크가 없으므로 open 상태가 필요한 테스트는
      // ws.onopen() 호출과 함께 readyState = 1 을 직접 세팅한다.
      this.readyState = 0; // CONNECTING
      MockWebSocket.instances.push(this);
    }
    send(data) { this.sent.push(JSON.parse(data)); }
    // Like the browser, close() does NOT fire onclose synchronously; tests
    // deliver the close event explicitly via instance.onclose(...).
    close() { this.closed = true; this.readyState = 3; }
  }
  MockWebSocket.instances = [];
  w.WebSocket = MockWebSocket;

  for (const src of [...SOURCES, BRIDGE]) {
    const script = w.document.createElement("script");
    script.textContent = src;
    w.document.body.appendChild(script);
  }

  const fetchCalls = [];
  w.apiFetch = (path, init = {}) => {
    const body = init.body ? JSON.parse(init.body) : null;
    fetchCalls.push({ path, method: init.method, body });
    return Promise.resolve({
      ok: true,
      json: async () => {
        const out = {};
        for (const code of body?.codes || []) out[code] = quotes[code] ?? null;
        return out;
      },
    });
  };

  const qm = w.QuoteManager;
  const ticks = [];
  qm.onQuote = (code, q) => ticks.push({ code, q });
  return { w, qm, clock, MockWebSocket, wsAttempts, fetchCalls, ticks };
}

test("connect: ws:// URL, passive open, general poll interval armed", () => {
  const { qm, clock, MockWebSocket } = createHarness();
  qm.connect();
  assert.equal(MockWebSocket.instances.length, 1);
  const ws = MockWebSocket.instances[0];
  assert.equal(ws.url, "ws://app.example.com/ws/quotes");
  assert.equal(qm.connected, false);
  assert.equal(clock.pending(), 1); // the 60s general poll interval

  ws.onopen();
  assert.equal(qm.connected, true);
  assert.deepEqual(ws.sent, []);

  // connect() while a socket exists is a no-op.
  qm.connect();
  assert.equal(MockWebSocket.instances.length, 1);
  qm.disconnect();
});

test("requestActive: admin control sends the only takeover and persists tab intent", () => {
  const { w, qm, MockWebSocket } = createHarness();
  qm.setManualControlAllowed(true);
  qm.requestActive();
  assert.equal(MockWebSocket.instances.length, 1);
  assert.equal(qm.desiredActive, true);
  assert.equal(w.sessionStorage.getItem("quote_manager_manual_ws_enabled"), "1");

  const ws = MockWebSocket.instances[0];
  ws.onopen();
  assert.deepEqual(ws.sent, [{ action: "takeover" }]);
  qm.connect();
  assert.equal(MockWebSocket.instances.length, 1);
  assert.deepEqual(ws.sent, [{ action: "takeover" }]);
  qm.disconnect();
});

test("ws_status active → subscribe message with the requested map; isLive per ws code", async () => {
  const quotes = {
    "005930": { price: 70000, previous_close: 69000, source: "rest", date: "20260610" },
    AAPL: { price: 201.5, previous_close: 200, source: "rest", date: "20260610" },
  };
  const { qm, clock, MockWebSocket, fetchCalls, ticks } = createHarness({ quotes });
  qm.setManualControlAllowed(true);
  qm.requestActive();
  const ws = MockWebSocket.instances[0];
  ws.onopen();

  // Subscriptions stored before the slot is granted must not be sent yet.
  qm.updateSubscriptions({ portfolio: ["005930", "AAPL"] });
  assert.equal(ws.sent.length, 1); // still only the takeover

  ws.onmessage({ data: JSON.stringify({ type: "ws_status", active: true }) });
  assert.equal(qm.wsActive, true);
  assert.equal(ws.sent.length, 2);
  assert.equal(ws.sent[1].action, "subscribe");
  assert.deepEqual(ws.sent[1].requested, { portfolio: ["005930", "AAPL"] });

  // Server splits codes: ws slot for 005930, REST overflow for AAPL.
  ws.onmessage({ data: JSON.stringify({ type: "subscriptions", ws: ["005930"], rest: ["AAPL"] }) });
  assert.equal(qm.isLive("005930"), true);
  assert.equal(qm.isLive("AAPL"), false);

  await flush();
  // One initial bulk fetch (fresh:false), priority-sorted: alpha code before
  // digit-leading KRX code. The immediate overflow poll is deduped because the
  // codes are still inflight from the initial fetch.
  assert.equal(fetchCalls.length, 1);
  assert.equal(fetchCalls[0].path, "/api/asset-quotes");
  assert.equal(fetchCalls[0].method, "POST");
  assert.deepEqual(fetchCalls[0].body, { codes: ["AAPL", "005930"], fresh: false });
  // REST results were dispatched through onQuote with the code spliced in.
  assert.deepEqual(ticks.map((t) => t.code), ["AAPL", "005930"]);
  assert.equal(ticks[0].q.price, 201.5);
  assert.equal(ticks[0].q.code, "AAPL");

  // 30s later the overflow poll re-fetches the REST-only codes (fresh:true).
  await clock.tick(30_000);
  assert.equal(fetchCalls.length, 2);
  assert.deepEqual(fetchCalls[1].body, { codes: ["AAPL"], fresh: true });
  qm.disconnect();
});

test("ws_status occupied (not granted) → passive client does not steal the slot", () => {
  const { qm, MockWebSocket } = createHarness();
  qm.connect();
  const ws = MockWebSocket.instances[0];
  ws.onopen();
  ws.onmessage({ data: JSON.stringify({ type: "ws_status", active: false, occupied: true }) });
  assert.equal(qm.wsActive, false);
  assert.deepEqual(ws.sent, []);
  qm.disconnect();
});

test("ws_status occupied after explicit request → client does not loop takeover", () => {
  const { qm, MockWebSocket } = createHarness();
  qm.setManualControlAllowed(true);
  qm.requestActive();
  const ws = MockWebSocket.instances[0];
  ws.onopen();
  ws.onmessage({ data: JSON.stringify({ type: "ws_status", active: false, occupied: true }) });
  assert.equal(qm.wsActive, false);
  assert.equal(qm.desiredActive, true);
  assert.deepEqual(ws.sent, [{ action: "takeover" }]);
  qm.disconnect();
});

test("quote message → onQuote with the parsed tick; null price still dispatches but is not 'fresh'", () => {
  const { qm, MockWebSocket, ticks } = createHarness();
  qm.connect();
  const ws = MockWebSocket.instances[0];
  ws.onopen();

  ws.onmessage({ data: JSON.stringify({ type: "quote", code: "005930", price: 70100, change_pct: 1.2 }) });
  assert.equal(ticks.length, 1);
  assert.equal(ticks[0].code, "005930");
  assert.equal(ticks[0].q.price, 70100);
  assert.equal(ticks[0].q.change_pct, 1.2);
  assert.equal(typeof qm.lastWsQuoteAt["005930"], "number");

  // Priceless tick: handler still invoked (UI may want metadata), but the code
  // is NOT marked ws-fresh, so stale polling can pick it up.
  ws.onmessage({ data: JSON.stringify({ type: "quote", code: "AAPL", price: null }) });
  assert.equal(ticks.length, 2);
  assert.equal(qm.lastWsQuoteAt.AAPL, undefined);

  // Malformed frames are swallowed (console.warn), not thrown.
  assert.doesNotThrow(() => ws.onmessage({ data: "not-json{{" }));
  assert.equal(ticks.length, 2);
  qm.disconnect();
});

test("ws_taken_over → wsActive false + banner that auto-removes after 5s", async () => {
  const { w, qm, clock, MockWebSocket } = createHarness();
  qm.setManualControlAllowed(true);
  qm.requestActive();
  const ws = MockWebSocket.instances[0];
  ws.onopen();
  ws.onmessage({ data: JSON.stringify({ type: "ws_status", active: true }) });
  assert.equal(qm.wsActive, true);

  ws.onmessage({ data: JSON.stringify({ type: "ws_taken_over" }) });
  assert.equal(qm.wsActive, false);
  assert.equal(qm.desiredActive, false);
  assert.equal(w.sessionStorage.getItem("quote_manager_manual_ws_enabled"), null);
  const banner = [...w.document.body.querySelectorAll("div")]
    .find((el) => /다른 세션이 실시간 시세 연결을 가져갔습니다/.test(el.textContent));
  assert.ok(banner, "takeover banner must be shown");
  await clock.tick(5_000);
  assert.equal(banner.isConnected, false);
  qm.disconnect();
});

test("unexpected close → reconnect after 5s; close code 4001 (replaced) → no reconnect", async () => {
  const { qm, clock, MockWebSocket } = createHarness();
  qm.connect();
  MockWebSocket.instances[0].onopen();
  MockWebSocket.instances[0].onclose({ code: 1006 });
  assert.equal(qm.connected, false);
  assert.equal(qm.ws, null);

  await clock.tick(5_000);
  assert.equal(MockWebSocket.instances.length, 2, "must reconnect after the backoff");
  MockWebSocket.instances[1].onopen();
  assert.deepEqual(MockWebSocket.instances[1].sent, []);

  // Server-initiated replacement (4001) must NOT reconnect.
  MockWebSocket.instances[1].onclose({ code: 4001 });
  await clock.tick(30_000);
  assert.equal(MockWebSocket.instances.length, 2);
  qm.disconnect();
});

test("unexpected close with manual active intent → reconnect claims the slot again", async () => {
  const { qm, clock, MockWebSocket } = createHarness();
  qm.setManualControlAllowed(true);
  qm.requestActive();
  MockWebSocket.instances[0].onopen();
  assert.deepEqual(MockWebSocket.instances[0].sent, [{ action: "takeover" }]);

  MockWebSocket.instances[0].onclose({ code: 1006 });
  await clock.tick(5_000);
  assert.equal(MockWebSocket.instances.length, 2);
  MockWebSocket.instances[1].onopen();
  assert.deepEqual(MockWebSocket.instances[1].sent, [{ action: "takeover" }]);
  qm.disconnect();
});

test("WebSocket constructor unavailable → retries every 5s until disconnect", async () => {
  const { qm, clock, wsAttempts } = createHarness({ wsThrows: true });
  qm.connect();
  assert.equal(wsAttempts.count, 1);
  assert.equal(clock.pending(), 1); // only the reconnect timer (no poll interval)

  await clock.tick(5_000);
  assert.equal(wsAttempts.count, 2);

  qm.disconnect();
  await clock.tick(60_000);
  assert.equal(wsAttempts.count, 2, "disconnect must stop the retry loop");
  assert.equal(clock.pending(), 0);
});

test("polling fallback: without the active WS slot, the 60s poll fetches all subscribed codes", async () => {
  const quotes = {
    "005930": { price: 70000, source: "rest", date: "20260610" },
    GOOG: { price: 180.2, source: "rest", date: "20260610" },
  };
  const { qm, clock, MockWebSocket, fetchCalls, ticks } = createHarness({ quotes });
  qm.connect();
  MockWebSocket.instances[0].onopen();
  // Another session owns the WS slot — this one never becomes active.
  qm.updateSubscriptions({ portfolio: ["005930", "GOOG"], market: ["GOOG"] });
  assert.equal(fetchCalls.length, 0);

  await clock.tick(60_000);
  assert.equal(fetchCalls.length, 1);
  // Deduped across subscription groups, fresh, priority-sorted.
  assert.deepEqual(fetchCalls[0].body, { codes: ["GOOG", "005930"], fresh: true });
  assert.deepEqual(ticks.map((t) => t.code), ["GOOG", "005930"]);
  qm.disconnect();
});

test("_pollAll with the active slot: only overflow + ws codes stale for 55s; priority codes first", async () => {
  const { qm, fetchCalls } = createHarness();
  qm.wsActive = true;
  qm.wsCodes = new Set(["005930", "AAPL"]);
  qm.overflowCodes = ["EUN2"];
  qm.lastWsQuoteAt["005930"] = Date.now(); // fresh ws tick → excluded
  qm.lastWsQuoteAt.AAPL = Date.now() - 56_000; // stale ws code → polled

  await qm._pollAll();
  assert.equal(fetchCalls.length, 1);
  // EUN2 is in QUOTE_MANAGER_PRIORITY_CODES → sorted ahead of AAPL.
  assert.deepEqual(fetchCalls[0].body, { codes: ["EUN2", "AAPL"], fresh: true });
  qm.disconnect();
});

test("initial fetch retries codes whose PfStore quote stays unusable, then stops once usable", async () => {
  const { w, qm, clock, fetchCalls } = createHarness({ quotes: {} }); // server returns nothing
  w.PfStore.items = [{ stock_code: "005930", quote: null }];

  await qm._fetchInitialQuotes(["005930"]);
  // Pass 1: cached bulk (fresh:false). Pass 2: targeted refresh (fresh:true).
  assert.equal(fetchCalls.length, 2);
  assert.deepEqual(fetchCalls[0].body, { codes: ["005930"], fresh: false });
  assert.deepEqual(fetchCalls[1].body, { codes: ["005930"], fresh: true });
  assert.equal(clock.pending(), 1, "a 5s retry must be armed while the quote is missing");

  await clock.tick(5_000);
  assert.equal(fetchCalls.length, 3); // still missing → retried

  w.PfStore.items[0].quote = { price: 70000 }; // now usable
  await clock.tick(5_000);
  assert.equal(fetchCalls.length, 3, "retry loop must stop once nothing is missing");
  assert.equal(clock.pending(), 0);
});

test("inflight codes are not re-requested while a batch is pending", async () => {
  const { w, qm } = createHarness();
  const calls = [];
  const deferred = [];
  w.apiFetch = (path, init) => {
    calls.push(JSON.parse(init.body));
    return new Promise((resolve) => {
      deferred.push(() => resolve({ ok: true, json: async () => ({}) }));
    });
  };

  const first = qm._fetchQuotes(["005930"], { scheduleRetry: false });
  await qm._fetchQuotes(["005930"], { scheduleRetry: false }); // fully deduped → resolves
  assert.equal(calls.length, 1);

  deferred[0]();
  await first; // batch settles → inflight set cleared
  const second = qm._fetchQuotes(["005930"], { scheduleRetry: false });
  assert.equal(calls.length, 2, "after the batch settles the code may be fetched again");
  deferred[1]();
  await second;
});

test("batching: 32 codes split 30 + 2, priority code leads the first batch", async () => {
  const { qm, fetchCalls } = createHarness();
  const codes = Array.from({ length: 31 }, (_, i) => `SYM${String(i).padStart(2, "0")}`);
  codes.push("A200"); // priority code appended last on purpose
  await qm._fetchQuotes(codes, { scheduleRetry: false });
  assert.equal(fetchCalls.length, 2);
  assert.equal(fetchCalls[0].body.codes.length, 30);
  assert.equal(fetchCalls[1].body.codes.length, 2);
  assert.equal(fetchCalls[0].body.codes[0], "A200");
});

// --- 포그라운드 복귀 검증 (verifyConnection) --------------------------------
// 백그라운드에서 OS가 소켓을 끊어도 close 이벤트가 오지 않는 half-open 상태가
// 흔하다. 복귀 시 verifyConnection 이 실제 생사를 판별해 UI 표시를 맞춘다.

test("verifyConnection: readyState CLOSED(close 미전달) → 즉시 재접속하며 슬롯 재청구", () => {
  const { qm, MockWebSocket } = createHarness();
  qm.setManualControlAllowed(true);
  qm.requestActive();
  const ws = MockWebSocket.instances[0];
  ws.readyState = 1;
  ws.onopen();
  ws.onmessage({ data: JSON.stringify({ type: "ws_status", active: true }) });
  assert.equal(qm.wsActive, true);

  // 백그라운드에서 소켓이 죽었지만 close 이벤트는 전달되지 않았다.
  ws.readyState = 3;
  qm.verifyConnection();
  assert.equal(qm.wsActive, false, "죽은 연결은 즉시 비활성으로 반영");
  assert.equal(ws.onclose, null, "죽은 소켓의 뒤늦은 close 가 새 연결을 건드리면 안 됨");
  assert.equal(MockWebSocket.instances.length, 2, "백오프 없이 즉시 재접속");

  const ws2 = MockWebSocket.instances[1];
  ws2.readyState = 1;
  ws2.onopen();
  assert.deepEqual(ws2.sent, [{ action: "takeover" }], "수동 활성 의도는 유지되어 재청구");
  qm.disconnect();
});

test("verifyConnection: OPEN 소켓은 ping 으로 검증 — pong 수신 시 유지, 중복 ping 없음", async () => {
  const { qm, clock, MockWebSocket } = createHarness();
  qm.connect();
  const ws = MockWebSocket.instances[0];
  ws.readyState = 1;
  ws.onopen();

  qm.verifyConnection();
  assert.deepEqual(ws.sent, [{ action: "ping" }]);
  qm.verifyConnection(); // 검증 진행 중 재호출은 ping 을 다시 보내지 않는다
  assert.equal(ws.sent.length, 1);

  ws.onmessage({ data: JSON.stringify({ type: "pong" }) });
  await clock.tick(4_000); // = QUOTE_MANAGER_PING_TIMEOUT_MS
  assert.equal(MockWebSocket.instances.length, 1, "pong 을 받았으면 재접속하지 않는다");
  assert.equal(qm.connected, true);
  qm.disconnect();
});

test("verifyConnection: OPEN 이지만 pong 없음(half-open) → 타임아웃 후 강제 재접속", async () => {
  const { qm, clock, MockWebSocket } = createHarness();
  qm.setManualControlAllowed(true);
  qm.requestActive();
  const ws = MockWebSocket.instances[0];
  ws.readyState = 1;
  ws.onopen();
  ws.onmessage({ data: JSON.stringify({ type: "ws_status", active: true }) });
  assert.equal(qm.wsActive, true);

  qm.verifyConnection();
  await clock.tick(4_000); // = QUOTE_MANAGER_PING_TIMEOUT_MS
  assert.equal(MockWebSocket.instances.length, 2, "pong 미수신 → 강제 재접속");
  assert.equal(ws.closed, true);
  assert.equal(qm.wsActive, false);

  const ws2 = MockWebSocket.instances[1];
  ws2.readyState = 1;
  ws2.onopen();
  assert.deepEqual(ws2.sent, [{ action: "takeover" }]);
  qm.disconnect();
});

test("verifyConnection: 재접속 대기 중엔 백오프 생략, 연결 이력 없거나 끊은 상태는 no-op", async () => {
  const { qm, clock, MockWebSocket } = createHarness();
  qm.verifyConnection(); // 초기화 전 — 연결을 만들지 않는다
  assert.equal(MockWebSocket.instances.length, 0);

  qm.connect();
  const ws = MockWebSocket.instances[0];
  ws.readyState = 1;
  ws.onopen();
  ws.onclose({ code: 1006 }); // 백그라운드에서 닫힘 → 5초 백오프 대기
  assert.equal(MockWebSocket.instances.length, 1);

  qm.verifyConnection();
  assert.equal(MockWebSocket.instances.length, 2, "포그라운드 복귀 즉시 재접속");
  await clock.tick(10_000);
  assert.equal(MockWebSocket.instances.length, 2, "기존 백오프 타이머는 해제되어 중복 접속 없음");

  qm.disconnect();
  qm.verifyConnection(); // 명시적 disconnect 후에도 되살리지 않는다
  assert.equal(MockWebSocket.instances.length, 2);
});

test("disconnect clears every timer and resets state", () => {
  const { w, qm, clock, MockWebSocket } = createHarness();
  qm.setManualControlAllowed(true);
  qm.requestActive();
  const ws = MockWebSocket.instances[0];
  ws.onopen();
  ws.onmessage({ data: JSON.stringify({ type: "ws_status", active: true }) });
  ws.onmessage({ data: JSON.stringify({ type: "subscriptions", ws: ["005930"], rest: ["AAPL"] }) });
  assert.ok(clock.pending() >= 2); // general poll + overflow poll

  qm.disconnect();
  assert.equal(clock.pending(), 0);
  assert.equal(ws.closed, true);
  assert.equal(qm.ws, null);
  assert.equal(qm.connected, false);
  assert.equal(qm.wsActive, false);
  assert.equal(qm.desiredActive, false);
  assert.equal(w.sessionStorage.getItem("quote_manager_manual_ws_enabled"), null);
  assert.equal(qm.wsCodes.size, 0);
  assert.equal(qm.overflowCodes.length, 0); // realm-safe empty check
});

test("disconnect() detaches onclose so the deferred close event cannot resurrect the connection", async () => {
  // 회귀 가드: 과거에는 disconnect()가 onclose를 떼지 않아, 브라우저가
  // 비동기로 전달하는 close 이벤트(code 1000/1005 != 4001)가 5초 뒤
  // 아무도 원하지 않은 재접속을 일으켰다.
  const { qm, clock, MockWebSocket } = createHarness();
  qm.connect();
  const ws = MockWebSocket.instances[0];
  ws.onopen();

  qm.disconnect();
  assert.equal(clock.pending(), 0);
  assert.equal(ws.onclose, null, "disconnect() must detach onclose before closing");

  if (typeof ws.onclose === "function") ws.onclose({ code: 1000 });
  assert.equal(clock.pending(), 0, "no reconnect timer after explicit disconnect");
  await clock.tick(5_000);
  assert.equal(MockWebSocket.instances.length, 1, "no reconnection after explicit disconnect");
});
