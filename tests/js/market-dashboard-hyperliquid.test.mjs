import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (name) => readFileSync(join(__dirname, "..", "..", "static", "js", name), "utf8");
const UTILS = read("utils.js");
const DASHBOARD = read("market-dashboard.js");

const CATALOG = {
  USD_KRW: { label: "달러/원", category: "환율" },
  HL_KR200: { label: "KR200", category: "하이퍼리퀴드", symbol: "xyz:KR200", quote_currency: "KRW" },
  HL_SAMSUNG: { label: "삼성전자", category: "하이퍼리퀴드", symbol: "xyz:SMSN" },
  HL_SKHYNIX: { label: "SK하이닉스", category: "하이퍼리퀴드", symbol: "xyz:SKHX" },
  HL_HYUNDAI: { label: "현대차", category: "하이퍼리퀴드", symbol: "xyz:HYUNDAI" },
  HL_GOLD: { label: "GOLD", category: "하이퍼리퀴드", symbol: "xyz:GOLD" },
};

const SUMMARY = {
  USD_KRW: { value: "1,300.00", change: "", change_pct: "", direction: "" },
  HL_KR200: { value: "1,200.00", change: "10.00", change_pct: "0.84%", direction: "up" },
  HL_SAMSUNG: { value: "80.00", change: "1.00", change_pct: "1.27%", direction: "up" },
  HL_SKHYNIX: { value: "95.00", change: "5.00", change_pct: "5.56%", direction: "up" },
  HL_HYUNDAI: { value: "70.00", change: "1.00", change_pct: "1.45%", direction: "up" },
  HL_GOLD: { value: "4,100.00", change: "20.00", change_pct: "0.49%", direction: "up" },
};

class MockWebSocket {
  static instances = [];

  constructor(url) {
    this.url = url;
    this.readyState = 0;
    this.sent = [];
    MockWebSocket.instances.push(this);
  }

  send(payload) {
    this.sent.push(JSON.parse(payload));
  }

  open() {
    this.readyState = 1;
    this.onopen?.();
  }

  message(payload) {
    this.onmessage?.({ data: JSON.stringify(payload) });
  }

  close() {
    this.readyState = 3;
    this.onclose?.();
  }
}

function load() {
  MockWebSocket.instances = [];
  const dom = new JSDOM(
    "<!doctype html><html><body>"
      + "<div id='marketDashboard'><div id='mdIndMain'></div><div id='mdIndRail'></div></div>"
      + "</body></html>",
    { runScripts: "dangerously", url: "https://app.example.com/", pretendToBeVisual: true },
  );
  dom.window.WebSocket = MockWebSocket;
  for (const source of [UTILS, DASHBOARD]) {
    const script = dom.window.document.createElement("script");
    script.textContent = source;
    dom.window.document.body.appendChild(script);
  }
  dom.window.apiFetchJson = async (url) => {
    if (url === "/api/market-indicators") return CATALOG;
    if (url.startsWith("/api/market-summary")) return structuredClone(SUMMARY);
    if (url.startsWith("/api/market/live")) return {};
    if (url === "/api/market/investor-flows") return { flows: {} };
    throw new Error(`unexpected URL: ${url}`);
  };
  return dom.window;
}

function findRow(window, label) {
  return [...window.document.querySelectorAll("#mdHyperliquidSection .md-row")]
    .find((row) => row.querySelector(".md-row-label")?.textContent === label);
}

test("Hyperliquid section opens one socket and subscribes to the five XYZ asset contexts", async () => {
  const window = load();
  try {
    await window.loadInvestingDashboard();
    assert.equal(MockWebSocket.instances.length, 1);
    const socket = MockWebSocket.instances[0];
    assert.equal(socket.url, "wss://api.hyperliquid.xyz/ws");
    assert.equal(window.document.querySelector("#mdHyperliquidSection .md-section-title").textContent, "하이퍼리퀴드");
    assert.doesNotMatch(window.document.getElementById("marketDashboard").textContent, /바이낸스/);

    socket.open();
    assert.deepEqual(
      socket.sent.map((message) => message.subscription?.coin),
      ["xyz:KR200", "xyz:SMSN", "xyz:SKHX", "xyz:HYUNDAI", "xyz:GOLD"],
    );
    assert.ok(socket.sent.every((message) => (
      message.method === "subscribe" && message.subscription.type === "activeAssetCtx"
    )));
    assert.equal(window.document.getElementById("hlLiveStatus").textContent, "실시간");
  } finally {
    window._hlStopStream();
    window.close();
  }
});

test("activeAssetCtx updates only the matching row and preserves the USD toggle", async () => {
  const window = load();
  try {
    await window.loadInvestingDashboard();
    const socket = MockWebSocket.instances[0];
    socket.open();
    window.document.querySelector('[data-hl-ccy="USD"]').click();

    socket.message({
      channel: "activeAssetCtx",
      data: {
        coin: "xyz:SKHX",
        ctx: { midPx: "100", markPx: "99", prevDayPx: "80" },
      },
    });

    const row = findRow(window, "SK하이닉스");
    assert.equal(row.querySelector(".md-row-val").textContent, "100.00");
    assert.equal(row.querySelector(".md-chg-abs").textContent.trim(), "+20.00");
    assert.equal(row.querySelector(".md-chg-pct").textContent, "(+25.00%)");
    assert.ok(window.document.querySelector('[data-hl-ccy="USD"]').classList.contains("active"));

    socket.message({ channel: "activeAssetCtx", data: { coin: "xyz:UNKNOWN", ctx: { midPx: "1" } } });
    socket.onmessage({ data: "not json" });
    assert.equal(findRow(window, "SK하이닉스").querySelector(".md-row-val").textContent, "100.00");
  } finally {
    window._hlStopStream();
    window.close();
  }
});

test("KR200 keeps its native value in KRW mode and only converts for USD mode", async () => {
  const window = load();
  try {
    await window.loadInvestingDashboard();
    assert.equal(findRow(window, "KR200").querySelector(".md-row-val").textContent, "1,200.00");
    assert.equal(findRow(window, "GOLD").querySelector(".md-row-val").textContent, "5,330,000");

    window.document.querySelector('[data-hl-ccy="USD"]').click();
    assert.equal(findRow(window, "KR200").querySelector(".md-row-val").textContent, "0.92");
    assert.equal(findRow(window, "GOLD").querySelector(".md-row-val").textContent, "4,100.00");
  } finally {
    window._hlStopStream();
    window.close();
  }
});

test("unexpected close schedules an exponential reconnect and re-subscribes once", async () => {
  const window = load();
  let scheduled = null;
  try {
    await window.loadInvestingDashboard();
    const first = MockWebSocket.instances[0];
    first.open();
    window.Math.random = () => 0;
    window.setTimeout = (callback, delay) => {
      scheduled = { callback, delay };
      return 99;
    };
    window.clearTimeout = () => {};

    first.close();
    assert.equal(window.document.getElementById("hlLiveStatus").textContent, "재연결 중");
    assert.equal(scheduled.delay, 1000);
    scheduled.callback();
    assert.equal(MockWebSocket.instances.length, 2);

    const second = MockWebSocket.instances[1];
    second.open();
    assert.equal(second.sent.filter((message) => message.method === "subscribe").length, 5);
    window._hlStartStream();
    assert.equal(MockWebSocket.instances.length, 2, "open socket is reused");
  } finally {
    window._hlStopStream();
    window.close();
  }
});
