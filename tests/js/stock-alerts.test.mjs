// jsdom behavior test for static/js/stock-alerts.js.
//
// 종목 분석 화면 개별 종목 알림 모달의 순수 로직(로그인 게이트, 4유형 폼 렌더,
// feed 토글 상태, 가격 알림 목록)을 검증한다. fetch 가 필요한 비동기 로딩/저장은
// 백엔드 통합 테스트(tests/test_notifications.py)가 담당한다.

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
  read("static", "app-config.js"),
  read("static", "js", "utils.js"),
  read("static", "js", "stock-alerts.js"),
];

const MODAL_HTML = `
  <div class="company-info"><h2 id="companyName">삼성전자 (005930)</h2></div>
  <div class="pf-modal-overlay" id="stockAlertModal" style="display:none;">
    <div class="pf-modal-body">
      <span id="stockAlertStockName"></span>
      <div class="pf-alert-form" id="stockAlertForm"></div>
      <div class="pf-alert-list" id="stockAlertList"></div>
    </div>
  </div>`;

function loadStockAlerts({ user = null, code = null, alerts = [] } = {}) {
  const dom = new JSDOM(`<!doctype html><html><body>${MODAL_HTML}</body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window } = dom;
  window.alert = () => {};
  window.fetch = () => Promise.reject(new Error("no fetch in test"));
  for (const src of SOURCES) {
    const s = window.document.createElement("script");
    s.textContent = src;
    window.document.body.appendChild(s);
  }
  // utils.js 의 전역 currentUser/activeStockCode 에 값 주입(let 전역 할당).
  const setup = window.document.createElement("script");
  setup.textContent = `currentUser = ${JSON.stringify(user)}; activeStockCode = ${JSON.stringify(code)};`;
  window.document.body.appendChild(setup);
  window.StockAlerts.alerts = alerts;
  return window;
}

test("비로그인: openStockAlerts 는 모달을 열지 않는다", () => {
  const w = loadStockAlerts({ user: null, code: "005930" });
  w.openStockAlerts();
  assert.equal(w.document.getElementById("stockAlertModal").style.display, "none");
});

test("로그인: openStockAlerts 가 모달을 열고 4유형 폼을 렌더한다", () => {
  const w = loadStockAlerts({ user: { google_sub: "u1" }, code: "005930" });
  w.openStockAlerts();
  assert.equal(w.document.getElementById("stockAlertModal").style.display, "flex");
  const html = w.document.getElementById("stockAlertForm").innerHTML;
  assert.ok(html.includes('id="saPriceVal"'), "가격 도달 입력");
  assert.ok(html.includes('id="saDailyVal"'), "일간 등락률 입력");
  assert.match(html, /신규 공시/);
  assert.match(html, /신규 리포트/);
});

test("feed 토글: 켜진 공시는 끄기(false), 안 켜진 리포트는 켜기(true)", () => {
  const w = loadStockAlerts({
    user: { google_sub: "u1" }, code: "005930",
    alerts: [{ id: 10, alert_type: "disclosure_new", stock_code: "005930", enabled: 1, armed: 1 }],
  });
  w.StockAlerts.code = "005930";
  w.saRender();
  const html = w.document.getElementById("stockAlertForm").innerHTML;
  assert.match(html, /saToggleFeed\('disclosure_new', false\)/);  // 켜짐 → 끄기
  assert.match(html, /saToggleFeed\('report_new', true\)/);       // 꺼짐 → 켜기
});

test("일간 등락률 규칙이 있으면 값과 '변경/해제'를 보인다", () => {
  const w = loadStockAlerts({
    user: { google_sub: "u1" }, code: "005930",
    alerts: [{ id: 7, alert_type: "stock_daily_abs", stock_code: "005930", threshold: 5, enabled: 1, armed: 1 }],
  });
  w.StockAlerts.code = "005930";
  w.saRender();
  const html = w.document.getElementById("stockAlertForm").innerHTML;
  assert.match(html, /value="5"/);
  assert.match(html, /변경/);
  assert.match(html, /saDelete\(7\)/);
});

test("가격 알림 목록: 여러 건(이상/이하)을 렌더한다", () => {
  const w = loadStockAlerts({
    user: { google_sub: "u1" }, code: "005930",
    alerts: [
      { id: 1, alert_type: "price_above", stock_code: "005930", threshold: 80000, enabled: 1, armed: 1 },
      { id: 2, alert_type: "price_below", stock_code: "005930", threshold: 60000, enabled: 1, armed: 0 },
    ],
  });
  w.StockAlerts.code = "005930";
  w.saRender();
  const html = w.document.getElementById("stockAlertList").innerHTML;
  assert.match(html, /현재가 80,000 이상/);
  assert.match(html, /현재가 60,000 이하/);
  assert.match(html, /saDelete\(1\)/);
});
