// jsdom behavior tests for static/js/utils.js.
//
// First step of the roadmap's "프론트 문자열 존재 검증 → jsdom 동작 테스트로 점진
// 이전": instead of asserting a function name appears in the source, we load the
// real script into a jsdom window and assert its runtime behavior. Run with
// `npm test` (node --test).

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const UTILS_SRC = readFileSync(
  join(__dirname, "..", "..", "static", "js", "utils.js"),
  "utf8",
);

// Load utils.js as a real <script> in a fresh jsdom window so its top-level
// function declarations attach to that window, exactly like the browser.
function loadUtils() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const script = dom.window.document.createElement("script");
  script.textContent = UTILS_SRC;
  dom.window.document.body.appendChild(script);
  return dom.window;
}

test('토스 체결은 보완된 기준가로 등락률을 갱신하고 기준가가 없으면 이전 등락을 표시하지 않는다', () => {
  const w = loadUtils();
  try {
    const at = new Date().toISOString();
    const next = w.mergeQuoteSnapshot({price:70000, previous_close:69000, change:1000, change_pct:1.45},
      {price:71000, source:'toss_ws', as_of:at});
    assert.equal(next.change, 2000);
    assert.equal(next.change_pct, 2000 / 69000 * 100);
    const unknown = w.mergeQuoteSnapshot({price:70000, change:1000, change_pct:1.45},
      {price:71000, source:'toss_ws', as_of:at});
    assert.equal(unknown.change, undefined);
    assert.equal(unknown.change_pct, undefined);
  } finally {w.close();}
});

test("escapeHtml neutralizes HTML metacharacters", () => {
  const w = loadUtils();
  assert.equal(
    w.escapeHtml('<img src=x onerror="alert(1)">'),
    "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;",
  );
  assert.equal(w.escapeHtml("a & b"), "a &amp; b");
  assert.equal(w.escapeHtml("it's"), "it&#39;s");
  assert.equal(w.escapeHtml(null), "");
  assert.equal(w.escapeHtml(undefined), "");
});

test('변경 요청에 출처 검증용 헤더를 보내며 사용자 헤더도 보존한다', async () => {
  const w = loadUtils();
  try {
    const requests = [];
    w.fetch = async (url, options) => { requests.push(options); return { ok: true }; };
    await w.apiFetch('/api/portfolio/005930', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: '{}' });
    assert.equal(requests[0].headers['X-Requested-With'], 'fetch');
    assert.equal(requests[0].headers['Content-Type'], 'application/json');
    await w.apiFetch('/api/portfolio');
    assert.equal(requests[1].headers?.['X-Requested-With'], undefined);
  } finally { w.close(); }
});

test("safeExternalUrl only allows http(s) and blocks javascript:", () => {
  const w = loadUtils();
  assert.equal(w.safeExternalUrl("https://dart.fss.or.kr/x"), "https://dart.fss.or.kr/x");
  assert.equal(w.safeExternalUrl("javascript:alert(1)"), "");
  assert.equal(w.safeExternalUrl("data:text/html,<script>1</script>"), "");
  assert.equal(w.safeExternalUrl(""), "");
});

test("quoteIsUsable / quotePriceOrNull reflect price + stale flags", () => {
  const w = loadUtils();
  assert.equal(w.quoteIsUsable({ price: 100 }), true);
  assert.equal(w.quoteIsUsable({ price: 100, _stale: true }), false);
  assert.equal(w.quoteIsUsable({ price: null }), false);
  assert.equal(w.quoteIsUsable(null), false);
  assert.equal(w.quotePriceOrNull({ price: 100 }), 100);
  assert.equal(w.quotePriceOrNull({ price: null }), null);
  assert.equal(w.quotePriceOrNull(null), null);
});

test("guest recent list round-trips through localStorage and is capped", () => {
  const w = loadUtils();
  w.saveGuestRecent("005930", "삼성전자");
  w.saveGuestRecent("000660", "SK하이닉스");
  let list = w.getGuestRecent();
  assert.equal(list.length, 2);
  // Most recent first.
  assert.equal(list[0].stock_code, "000660");
  // Re-saving an existing code moves it to the front without duplicating.
  w.saveGuestRecent("005930", "삼성전자");
  list = w.getGuestRecent();
  assert.equal(list.length, 2);
  assert.equal(list[0].stock_code, "005930");
  w.removeGuestRecent("005930");
  // getGuestRecent() returns an array from the jsdom realm; spread it into a
  // Node array so deepStrictEqual compares structure, not Array prototype.
  assert.deepEqual(
    [...w.getGuestRecent()].map((i) => i.stock_code),
    ["000660"],
  );
});

test('portfolio links use a first-party handoff for all five dashboards without changing data URLs', () => {
  const w = loadUtils();
  w.eval(`APP_INTEGRATIONS.holdingValue = {baseUrl: 'https://ducklove.github.io/holding_value'};
    APP_INTEGRATIONS.preferredSpread = {baseUrl: 'https://ducklove.github.io/common_preferred_spread'};
    APP_INTEGRATIONS.spacHunter = {baseUrl: 'https://ducklove.github.io/spac-hunter'};
    APP_INTEGRATIONS.buybacks = {baseUrl: 'https://ducklove.github.io/buybacks'};
    APP_INTEGRATIONS.eiayn = {baseUrl: 'https://ducklove.github.io/eiayn'};`);
  for (const [key, path] of [['holdingValue', 'holding_value'], ['preferredSpread', 'common_preferred_spread'], ['spacHunter', 'spac-hunter'], ['buybacks', 'buybacks'], ['eiayn', 'eiayn']]) {
    assert.equal(w.portfolioIntegrationHref(`https://ducklove.github.io/${path}/?theme=dark&code=005935`),
      `/api/portfolio/open/${key}?code=005935&theme=dark`);
    const data = `https://ducklove.github.io/${path}/config.json`;
    assert.equal(w.portfolioIntegrationHref(data), data);
    const lookalike = `https://other.example/${path}/`;
    assert.equal(w.portfolioIntegrationHref(lookalike), lookalike);
  }
  w.close();
});

// ── R12-F7: storage guards ────────────────────────────────────────────
// Safari private mode / blocked site data make the storage *getter* itself
// throw. The helpers must swallow that and fall back instead of aborting the
// calling script.
function blockStorage(w) {
  for (const name of ["localStorage", "sessionStorage"]) {
    Object.defineProperty(w, name, {
      configurable: true,
      get() { throw new w.DOMException("The operation is insecure.", "SecurityError"); },
    });
  }
}

test("safeStorage helpers round-trip and fall back when storage throws", () => {
  const w = loadUtils();
  try {
    assert.equal(w.safeStorageGet("missing", "fallback"), "fallback");
    assert.equal(w.safeStorageSet("k", 1), true);
    assert.equal(w.safeStorageGet("k"), "1");
    assert.equal(w.safeStorageSet("s", "x", "session"), true);
    assert.equal(w.sessionStorage.getItem("s"), "x");
    assert.equal(w.safeStorageRemove("s", "session"), true);
    assert.equal(w.sessionStorage.getItem("s"), null);

    blockStorage(w);
    assert.equal(w.safeStorageGet("k", "fallback"), "fallback");
    assert.equal(w.safeStorageSet("k", "v"), false);
    assert.equal(w.safeStorageRemove("k", "session"), false);
    // Guest recent list keeps working (empty) instead of throwing.
    assert.doesNotThrow(() => w.saveGuestRecent("005930", "삼성전자"));
    assert.doesNotThrow(() => w.removeGuestRecent("005930"));
    assert.equal(w.getGuestRecent().length, 0);
  } finally { w.close(); }
});

// ── D-07: cssToken / isDarkTheme ──────────────────────────────────────
test("cssToken reads trimmed custom properties, falls back when empty or on error", () => {
  const w = loadUtils();
  try {
    const style = w.document.createElement("style");
    style.textContent = ":root { --probe: #123456 ; } [data-theme=\"dark\"] { --probe: #abcdef; }";
    w.document.head.appendChild(style);
    assert.equal(w.cssToken("--probe", "#000"), "#123456");
    assert.equal(w.cssToken("--undefined-token", "#000"), "#000");
    assert.equal(w.cssToken("--undefined-token"), "");
    w.document.documentElement.setAttribute("data-theme", "dark");
    assert.equal(w.cssToken("--probe", "#000"), "#abcdef");
    w.getComputedStyle = () => { throw new Error("no layout"); };
    assert.equal(w.cssToken("--probe", "#fallback"), "#fallback");
  } finally { w.close(); }
});

test("isDarkTheme follows the data-theme attribute the hub CSS keys on", () => {
  const w = loadUtils();
  try {
    const root = w.document.documentElement;
    assert.equal(w.isDarkTheme(), false);
    root.setAttribute("data-theme", "dark");
    assert.equal(w.isDarkTheme(), true);
    root.setAttribute("data-theme", "light");
    assert.equal(w.isDarkTheme(), false);
  } finally { w.close(); }
});

// ── F1-F3: visibility-aware polling ───────────────────────────────────
function installPollClock(w) {
  const timers = new Map();
  let nextId = 1;
  let now = 1_000_000;
  w.Date.now = () => now;
  w.setInterval = (fn, ms) => { const id = nextId++; timers.set(id, { fn, at: now + ms, every: ms }); return id; };
  w.clearInterval = (id) => timers.delete(id);
  async function tick(ms) {
    const end = now + ms;
    for (;;) {
      let dueId = null;
      for (const [id, t] of timers) if (t.at <= end && (dueId === null || t.at < timers.get(dueId).at)) dueId = id;
      if (dueId === null) break;
      const t = timers.get(dueId);
      now = t.at;
      t.at = now + t.every;
      t.fn();
      await new Promise((r) => setImmediate(r));
    }
    now = end;
  }
  return { tick, pending: () => timers.size, advance: (ms) => { now += ms; } };
}

function setVisibility(w, state) {
  Object.defineProperty(w.document, "visibilityState", { configurable: true, get: () => state });
  w.document.dispatchEvent(new w.Event("visibilitychange"));
}

test("schedulePoll ticks on its interval and re-scheduling a name never duplicates timers", async () => {
  const w = loadUtils();
  try {
    const clock = installPollClock(w);
    let runs = 0;
    w.schedulePoll("probe", () => { runs += 1; }, 1000);
    w.schedulePoll("probe", () => { runs += 1; }, 1000);
    assert.equal(clock.pending(), 1, "same name replaces the previous timer");
    await clock.tick(3000);
    assert.equal(runs, 3);
    assert.equal(w.cancelPoll("probe"), true);
    assert.equal(clock.pending(), 0);
    await clock.tick(3000);
    assert.equal(runs, 3);
  } finally { w.close(); }
});

test("schedulePoll pauses while hidden and refreshes once on return only when stale", async () => {
  const w = loadUtils();
  try {
    const clock = installPollClock(w);
    let runs = 0;
    const handle = w.schedulePoll("probe", () => { runs += 1; }, 60_000);

    setVisibility(w, "hidden");
    assert.equal(clock.pending(), 0, "hidden tab: interval is cleared");
    await clock.tick(10 * 60_000);
    assert.equal(runs, 0, "no polling while hidden");

    setVisibility(w, "visible");
    assert.equal(runs, 1, "stale on return → exactly one immediate refresh");
    assert.equal(clock.pending(), 1, "interval re-armed once");
    setVisibility(w, "visible");
    assert.equal(runs, 1, "a second visible event within the window does not refetch");
    assert.equal(clock.pending(), 1, "no duplicate timer");

    // Quick hide/show inside the freshness window: resume without refetching.
    setVisibility(w, "hidden");
    clock.advance(10_000);
    setVisibility(w, "visible");
    assert.equal(runs, 1);
    await clock.tick(60_000);
    assert.equal(runs, 2, "regular cadence resumes");

    handle.cancel();
    assert.equal(clock.pending(), 0);
  } finally { w.close(); }
});

test("schedulePoll honours when(), refreshOnVisible:false and skips overlapping runs", async () => {
  const w = loadUtils();
  try {
    const clock = installPollClock(w);
    let allowed = false;
    let gated = 0;
    w.schedulePoll("gated", () => { gated += 1; }, 1000, { when: () => allowed });
    await clock.tick(2000);
    assert.equal(gated, 0);
    allowed = true;
    await clock.tick(1000);
    assert.equal(gated, 1);

    let quiet = 0;
    w.schedulePoll("quiet", () => { quiet += 1; }, 1000, { refreshOnVisible: false });
    setVisibility(w, "hidden");
    clock.advance(5000);
    setVisibility(w, "visible");
    assert.equal(quiet, 0, "refreshOnVisible:false leaves the immediate refresh to the caller");

    let release;
    let slow = 0;
    w.schedulePoll("slow", () => { slow += 1; return new Promise((r) => { release = r; }); }, 1000);
    await clock.tick(3000);
    assert.equal(slow, 1, "a still-running poll is not fired again");
    release();
    await new Promise((r) => setImmediate(r));
    await clock.tick(1000);
    assert.equal(slow, 2);
  } finally {
    for (const name of ["gated", "quiet", "slow"]) w.cancelPoll(name);
    w.close();
  }
});

test("schedulePoll re-fires a run that has hung for more than 3 intervals", async () => {
  const w = loadUtils();
  try {
    const clock = installPollClock(w);
    const releases = [];
    let runs = 0;
    w.schedulePoll("hung", () => { runs += 1; return new Promise((r) => { releases.push(r); }); }, 1000);
    await clock.tick(3000);
    assert.equal(runs, 1, "within 3 intervals the pending run blocks new ones");
    await clock.tick(1000);
    assert.equal(runs, 2, "after 3 intervals the hung run is treated as stuck");
    // The stale run resolving late must not clear the flag of the newer run.
    releases[0]();
    await new Promise((r) => setImmediate(r));
    await clock.tick(1000);
    assert.equal(runs, 2, "newer run is still in flight");
    releases[1]();
    await new Promise((r) => setImmediate(r));
    await clock.tick(1000);
    assert.equal(runs, 3);
  } finally {
    w.cancelPoll("hung");
    w.close();
  }
});
