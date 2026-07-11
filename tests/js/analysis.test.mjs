// jsdom behavior tests for the analysis view scripts — focused on the
// external-tool deep-link cards (analysis.js), the DART filing review
// rendering helpers (analysis-filings.js), and the analyzeStock SSE stream
// parsing (progress/result 이벤트 버퍼링). Loads utils.js (escapeHtml) plus
// the analysis split files in index.html order. Network-bound
// loadStockExternalLinks() is not exercised here.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (p) => readFileSync(join(__dirname, "..", "..", "static", "js", p), "utf8");

function load() {
  const dom = new JSDOM(
    "<!doctype html><html><body><div id='coverageNote'></div></body></html>",
    { runScripts: "dangerously", url: "https://app.example.com/" },
  );
  for (const src of [read("utils.js"), read("analysis-charts.js"), read("analysis-filings.js"), read("analysis.js")]) {
    const s = dom.window.document.createElement("script");
    s.textContent = src;
    dom.window.document.body.appendChild(s);
  }
  return dom.window;
}

test("_externalValuationCards builds preferred + holding valuation-cards, escapes + neutralizes urls", () => {
  const w = load();
  const cards = w._externalValuationCards({
    preferred: {
      name: "<b>x</b>", preferredName: "삼성전자우",
      spread: 36.12, commonPrice: 317000, preferredPrice: 202500,
      url: "javascript:alert(1)",
    },
    holding: {
      name: "영풍", ratio: 781.87, holdingValue: 72253.4, marketCap: 9241.1,
      url: "https://ducklove.github.io/holding_value/?code=000670",
    },
  });
  assert.equal(cards.length, 2);
  // they share the .valuation-card grid styling (not a separate widget)
  const root = w.document.getElementById("coverageNote");
  root.innerHTML = cards.join("");
  const els = root.querySelectorAll("a.valuation-card.is-link");
  assert.equal(els.length, 2);
  assert.match(root.innerHTML, /36\.1%/);   // spread percent
  assert.match(root.innerHTML, /781\.9%/);  // ratio percent
  assert.ok(!root.innerHTML.includes("<b>x</b>"));  // hostile name escaped
  // javascript: url neutralized to '#', https preserved
  assert.equal(els[0].getAttribute("href"), "#");
  assert.equal(els[0].getAttribute("target"), "_blank");
  assert.equal(els[1].getAttribute("href"), "https://ducklove.github.io/holding_value/?code=000670");
});

test("_externalValuationCards returns only the matching card", () => {
  const w = load();
  const cards = w._externalValuationCards({ holding: { name: "영풍", ratio: 781.87, url: "https://x.test/" } });
  assert.equal(cards.length, 1);
  assert.match(cards[0], /지주사/);
});

test("_externalValuationCards returns [] when no links", () => {
  const w = load();
  assert.deepEqual([...w._externalValuationCards(null)], []);
  assert.deepEqual([...w._externalValuationCards({})], []);
});

test("_stripBlockBars removes unicode block-bar glyphs but keeps text", () => {
  const w = load();
  const out = w._stripBlockBars("영업이익률: ████ 13.1% → ████████ 42.8% (증가)");
  assert.ok(!/[▀-▟]/.test(out));
  assert.match(out, /13\.1%/);
  assert.match(out, /42\.8%/);
});

test("_renderMetricTrends draws before/after bars normalized to the larger value", () => {
  const w = load();
  const html = w._renderMetricTrends([
    { label: "영업이익률", unit: "%", note: "약 3.3배 증가",
      before: { label: "2025 연간", value: 13.1 },
      after: { label: "2026 1분기", value: 42.8 } },
    { label: "<b>x</b>", unit: "조",
      before: { label: "전", value: 333.6 },
      after: { label: "후", value: 133.9 } },
  ]);
  const root = w.document.getElementById("coverageNote");
  root.innerHTML = html;
  const items = root.querySelectorAll(".mt-item");
  assert.equal(items.length, 2);
  assert.match(root.innerHTML, /핵심 지표 변화/);
  assert.match(root.innerHTML, /약 3\.3배 증가/);
  // after(42.8) is the larger of the pair → its fill is 100%
  const fills = items[0].querySelectorAll(".mt-fill");
  assert.equal(fills[1].style.width, "100%");
  // before(13.1)/42.8 ≈ 30.6%
  assert.match(fills[0].style.width, /^30\./);
  // unit appended to value, hostile label escaped
  assert.match(items[0].innerHTML, /42\.8%/);
  assert.ok(!root.innerHTML.includes("<b>x</b>"));
});

test("_renderMetricTrends returns empty string when no trends", () => {
  const w = load();
  assert.equal(w._renderMetricTrends([]), "");
  assert.equal(w._renderMetricTrends(null), "");
});

// --- analyzeStock SSE 스트림 파싱 ---
// apiFetch 를 가짜 SSE 응답으로 바꿔치기해 실제 파서(줄 버퍼링, event/data
// 시퀀스 처리)를 브라우저 없이 구동한다. 무거운 renderResult 는 스파이로 대체.
function loadForAnalyze(chunks) {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="loadingOverlay">
      <div id="loadingText"></div>
      <div id="loadingDetail"></div>
      <div id="progressBar"></div>
      <div id="progressSteps"></div>
      <button id="cancelBtn"></button>
    </div>
  </body></html>`, { runScripts: "dangerously", url: "https://app.example.com/" });
  const w = dom.window;
  w.TextDecoder = TextDecoder; // jsdom window 에는 없음 — Node 전역 주입
  for (const src of [read("utils.js"), read("analysis-charts.js"), read("analysis-filings.js"), read("analysis.js")]) {
    const s = w.document.createElement("script");
    s.textContent = src;
    w.document.body.appendChild(s);
  }
  // 함수 선언은 window 프로퍼티가 되므로 로드 후 덮어쓰면 호출 시 스파이가 이긴다.
  w.requireApiConfiguration = () => {};
  w.trackEvent = () => {};
  w.loadRecentList = () => {};
  w.saveGuestRecent = () => {};
  w.currentUser = null; // auth.js 미로드
  const rendered = [];
  w.renderResult = (data) => rendered.push(data);
  const encoder = new TextEncoder();
  w.apiFetch = async () => ({
    ok: true,
    headers: { get: () => "text/event-stream" },
    body: {
      getReader() {
        let i = 0;
        return {
          read: async () => (i < chunks.length
            ? { done: false, value: encoder.encode(chunks[i++]) }
            : { done: true, value: undefined }),
          cancel: () => {},
        };
      },
    },
  });
  return { w, rendered };
}

test("analyzeStock: SSE 청크가 줄/JSON 경계와 무관하게 파싱돼 진행 표시와 최종 결과를 만든다", async () => {
  // result 의 JSON 을 청크 중간에서 잘라 버퍼링 로직까지 검증한다.
  const { w, rendered } = loadForAnalyze([
    'event: progress\ndata: {"step":"start","message":"분석 시작"}\n\n',
    'event: progress\ndata: {"step":"financial_start","message":"재무제표 수집"}\n\nevent: result\ndata: {"stock_code":"005930","corp_',
    'name":"삼성전자"}\n\n',
  ]);
  await w.analyzeStock("005930");

  assert.equal(rendered.length, 1, "result 이벤트가 renderResult 로 전달돼야 함");
  // jsdom realm 에서 JSON.parse 된 객체는 프로토타입이 달라 스프레드로 복사해 비교.
  assert.deepEqual({ ...rendered[0] }, { stock_code: "005930", corp_name: "삼성전자" });

  // 진행 스텝: start 는 완료(✓) 처리되고, financial_start 후 '분석 완료!' 가 붙는다.
  const steps = [...w.document.getElementById("progressSteps").children].map((el) => el.textContent);
  assert.deepEqual(steps, ["✓ 분석 시작", "✓ 재무제표 수집", "분석 완료!"]);
  assert.equal(w.document.getElementById("loadingText").textContent, "재무제표 수집");
  assert.equal(w.document.getElementById("progressBar").getAttribute("aria-valuenow"), "100");

  // 스트림 종료 후 오버레이는 닫힌다.
  const overlay = w.document.getElementById("loadingOverlay");
  assert.equal(overlay.classList.contains("show"), false);
  assert.equal(overlay.getAttribute("aria-busy"), "false");
});
