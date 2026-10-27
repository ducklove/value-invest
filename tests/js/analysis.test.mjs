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
  for (const src of [read("utils.js"), read("analysis-charts.js"), read("analysis-filings.js"), read("analysis-valuation.js"), read("analysis.js")]) {
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

// --- valuation 카드 지표 소스 ---
// 연간/주간 차트의 연간 EPS 와 별도로 검증된 TTM EPS 를 쓴다.
test("valuation 카드는 검증된 TTM EPS·최신 BPS와 주간 TTM 배당을 쓴다", () => {
  const w = load();
  const indicators = {
    // 오염된 연간 캐시 재현(현대차 2025 행): close 538,000 / pbr 0.67 →
    // 역산 BPS 802,985. 연간 2026 DPS 는 분기배당 부분합 2,500.
    "주가 (원)": [{ year: 2025, value: 538000 }],
    "PBR": [{ year: 2025, value: 0.67 }],
    "EPS (원)": [{ year: 2025, value: 36088 }],
    "주당배당금 (원)": [{ year: 2025, value: 10000 }, { year: 2026, value: 2500 }],
    "ROE (%)": [{ year: 2025, value: 9.99 }],
  };
  const weekly = {
    "주가": [{ date: "2026-08-18", value: 421000 }, { date: "2026-08-24", value: 398600 }],
    "PBR": [{ date: "2026-08-18", value: 0.96 }, { date: "2026-08-24", value: 0.91 }],
    "EPS (원)": [{ date: "2026-08-24", value: 35331 }],
    "주당배당금 (원)": [{ date: "2026-08-24", value: 10000 }],
    "ROE (%)": [{ date: "2026-08-24", value: 8.12 }],
  };
  const m = w.getCurrentValuationMetrics(indicators, weekly, { price: 399000 }, {
    earnings_basis: 'ttm', eps_ttm: 40000, equity_basis: 'latest', bps: 440000,
  });

  assert.equal(m.pbr, 399000 / 440000);
  // 배당수익률 = TTM 10,000/399,000 = 2.51% (연간 최신 양수 2,500 이면 0.63%)
  assert.ok(Math.abs(m.dividendYield - 2.506) < 0.01, `dy=${m.dividendYield}`);
  assert.equal(m.per, 399000 / 40000);
  assert.equal(m.roe, 8.12);
});

test("valuation 카드: TTM 자료가 없으면 연간 EPS·BPS로 조용히 폴백하지 않는다", () => {
  const w = load();
  const indicators = {
    "주가 (원)": [{ year: 2025, value: 100000 }],
    "PBR": [{ year: 2025, value: 1.0 }],
    "EPS (원)": [{ year: 2025, value: 10000 }],
    "주당배당금 (원)": [{ year: 2024, value: 3000 }, { year: 2025, value: 0 }],
    "ROE (%)": [{ year: 2025, value: 10 }],
  };
  const m = w.getCurrentValuationMetrics(indicators, {}, { price: 120000 });

  assert.equal(m.pbr, null);
  assert.equal(m.per, null);
  assert.ok(Math.abs(m.dividendYield - 2.5) < 0.001); // 연간 최신 양수 DPS 3,000
  assert.equal(m.roe, 10);
});

test("valuation 카드: 주간 TTM 배당 0 은 실측 0.00% 로 표시된다", () => {
  const w = load();
  const weekly = {
    "주가": [{ date: "2026-08-24", value: 50000 }],
    "PBR": [{ date: "2026-08-24", value: 2.0 }],
    "주당배당금 (원)": [{ date: "2026-08-24", value: 0 }],
  };
  const m = w.getCurrentValuationMetrics({}, weekly, { price: 50000 }, {
    earnings_basis: 'ttm', eps_ttm: -100, equity_basis: 'latest', bps: 25000,
  });

  assert.equal(m.dividendYield, 0);
  assert.ok(Math.abs(m.pbr - 2.0) < 0.001);
  assert.equal(m.per, null);
});

test("forward 카드는 각 지표 뒤에 표시하고 현재가 변동을 반영한다", () => {
  const w = load();
  w.eval(`_currentValuationBasis = {
    earnings_basis: 'ttm', eps_ttm: 1000, equity_basis: 'latest', bps: 10000,
    period_end: '2026-06-30', statement_basis: 'CFS'
  }; _forwardValuation = {
    period: '2026E', eps: 2000, bps: 12500, source: '테스트증권',
    source_url: 'https://stock.naver.com/research/company/1', as_of: '2026-10-12'
  };`);
  const root = w.document.getElementById('coverageNote');
  root.innerHTML = w.renderCurrentValuationSummary({}, {}, { price: 20000 });
  const cards = [...root.querySelectorAll('.valuation-card')];
  assert.deepEqual(cards.slice(0, 4).map(c => c.querySelector('.valuation-label').textContent),
    ['PER', 'Forward PER', 'PBR', 'Forward PBR']);
  assert.deepEqual(cards.slice(0, 4).map(c => c.querySelector('.valuation-value').textContent),
    ['20.00', '10.00', '2.00', '1.60']);
  assert.match(cards[0].textContent, /최근 4분기.*26\.06/);
  assert.match(cards[2].textContent, /분기말.*26\.06/);
  assert.match(cards[0].title, /실적 2026-06-30/);
  assert.match(cards[2].title, /분기말 2026-06-30/);
  assert.match(cards[1].textContent, /2026E.*테스트증권/);
  assert.match(cards[1].title, /리포트 게시일 2026-10-12/);
  assert.equal(cards[1].querySelector('a').getAttribute('href'), 'https://stock.naver.com/research/company/1');
  root.innerHTML = w.renderCurrentValuationSummary({}, {}, { price: 24000 });
  assert.equal(root.querySelectorAll('.valuation-value')[1].textContent, '12.00');
  w.eval('_forwardValuation = null');
  root.innerHTML = w.renderCurrentValuationSummary({}, {}, { price: 24000 });
  assert.ok(!root.textContent.includes('Forward'));
});

test("일부 예상치만 제공되면 해당 forward 카드만 표시하고 외부 텍스트를 이스케이프한다", () => {
  const w = load();
  w.eval(`_forwardValuation = {
    period: '2026E', eps: -10, bps: null, source: '<img src=x onerror=alert(1)>',
    source_url: 'javascript:alert(1)', report_title: '\" onmouseover=\"alert(1)'
  }; _currentValuationBasis = {};`);
  const root = w.document.getElementById('coverageNote');
  root.innerHTML = w.renderCurrentValuationSummary({}, {}, { price: 20000 });
  assert.equal(root.querySelectorAll('.valuation-card').length, 8);
  assert.equal(root.querySelectorAll('.valuation-value')[1].textContent, 'N/A');
  assert.equal(root.querySelectorAll('img, [onmouseover], a').length, 0);
  assert.ok(!root.textContent.includes('Forward PBR'));
});

test("늦게 도착한 이전 종목의 현재·예상 재무자료는 다음 종목 카드에 섞이지 않는다", async () => {
  const w = load();
  w.activeStockCode = '005930';
  let resolve;
  w.apiFetchJson = () => new Promise(r => { resolve = r; });
  const pending = w.loadCurrentValuation('005930');
  w.activeStockCode = '000660';
  resolve({ current: { eps_ttm: 10000 } });
  await pending;
  assert.equal(w.eval('_currentValuationBasis'), null);
  const forward = w.loadForwardValuation('000660');
  w.activeStockCode = '005380';
  resolve({ forward: { eps: 20000 } });
  await forward;
  assert.equal(w.eval('_forwardValuation'), null);
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

// --- 숫자 포맷 로케일(ko-KR) 고정 ---
// toLocaleString 로케일 혼재(ko-KR vs 미지정)를 'ko-KR' 명시로 통일 — 호스트
// 로케일과 무관하게 1,234,567 형태의 그룹핑이 나오는지 표시 결과를 고정한다.
function loadForQuote() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="coverageNote"></div>
    <div id="quoteSummary" style="display:none">
      <span id="quotePrice"></span><span id="quoteChange"></span><span id="quoteDate"></span>
    </div>
  </body></html>`, { runScripts: "dangerously", url: "https://app.example.com/" });
  const w = dom.window;
  for (const src of [read("utils.js"), read("analysis-charts.js"), read("analysis-filings.js"), read("analysis-valuation.js"), read("analysis.js")]) {
    const s = w.document.createElement("script");
    s.textContent = src;
    w.document.body.appendChild(s);
  }
  // renderQuoteSnapshot 이 참조하는 다른 파일 소유 전역들.
  w.QuoteManager = { isLive: () => false };
  w.activeStockCode = "005930";
  w.activeIndicators = {};
  w._renderCoverage = () => {};
  return w;
}

test("숫자 포맷은 ko-KR 로케일로 고정된다 (formatMetricNumber / renderQuoteSnapshot)", () => {
  const w = loadForQuote();

  assert.equal(w.formatMetricNumber(1234.5), "1,234.50");
  assert.equal(w.formatMetricNumber(12.3456, "%"), "12.35%");
  assert.equal(w.formatMetricNumber(null), "N/A");

  w.renderQuoteSnapshot({ price: 1234567, change: 1234, change_pct: 1.23, date: "2026-07-10" }, {});
  assert.equal(w.document.getElementById("quotePrice").textContent, "1,234,567원");
  assert.equal(w.document.getElementById("quoteChange").textContent, "+1,234원 (+1.23%)");
  assert.equal(w.document.getElementById("quoteDate").textContent, "2026-07-10 기준");
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
  for (const src of [read("utils.js"), read("analysis-charts.js"), read("analysis-filings.js"), read("analysis-valuation.js"), read("analysis.js")]) {
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
