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
      MockWebSocket.instances.push(this);
    }
    send(data) { this.sent.push(JSON.parse(data)); }
    // Like the browser, close() does NOT fire onclose synchronously; tests
    // deliver the close event explicitly via instance.onclose(...).
    close() { this.closed = true; }
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
