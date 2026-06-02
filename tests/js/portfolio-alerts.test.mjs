// jsdom behavior test for static/js/portfolio-alerts.js.
//
// 조건 알림 모달의 순수 렌더링/매핑 로직(폼 생성, 채널 상태, 규칙 목록 라벨,
// alert_type 매핑)을 실제 소스를 브라우저와 같은 순서로 올려 검증한다.
// fetch 가 필요한 비동기 로딩은 여기서 다루지 않는다(백엔드 통합 테스트가 담당).

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
  read("static", "js", "portfolio-store.js"),
  read("static", "js", "portfolio-alerts.js"),
];

const MODAL_HTML = `
  <div class="pf-modal-overlay" id="pfAlertsModal" style="display:none;">
    <div class="pf-modal-body">
      <div class="pf-alert-channel-status" id="pfAlertChannelStatus"></div>
      <div class="pf-alert-channel-actions" id="pfAlertChannelActions"></div>
      <div class="pf-alert-form" id="pfAlertForm"></div>
      <div class="pf-alert-list" id="pfAlertList"></div>
    </div>
  </div>`;

function loadAlerts(items = []) {
  const dom = new JSDOM(`<!doctype html><html><body>${MODAL_HTML}</body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window } = dom;
  // pfOpenAlerts 의 비동기 로딩이 호출할 수 있으므로 거부 스텁만 둔다.
  window.fetch = () => Promise.reject(new Error("no fetch in test"));
  for (const src of SOURCES) {
    const script = window.document.createElement("script");
    script.textContent = src;
    window.document.body.appendChild(script);
  }
  window.PfStore.items = items;
  return window;
}

test("alert_type 매핑: 카테고리+방향 -> 서버 타입", () => {
  const w = loadAlerts();
  assert.equal(w.pfAlertsBuildType("price", "above"), "price_above");
  assert.equal(w.pfAlertsBuildType("price", "below"), "price_below");
  assert.equal(w.pfAlertsBuildType("nav", "above"), "nav_above");
  assert.equal(w.pfAlertsBuildType("daily", "below"), "daily_change_below");
});

test("지정가 폼은 보유 종목 옵션과 현재가 힌트를 렌더한다", () => {
  const w = loadAlerts([{ stock_code: "005930", stock_name: "삼성전자", quote: { price: 72000 } }]);
  w.pfAlertsSetCategory("price");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.match(html, /삼성전자/);
  assert.match(html, /현재가/);
  assert.ok(html.includes('id="pfAlertStock"'), "종목 선택 셀렉트가 있어야 함");
  assert.ok(html.includes('id="pfAlertThreshold"'), "지정가 입력이 있어야 함");
});

test("총평가액 폼은 종목 선택을 숨긴다", () => {
  const w = loadAlerts([{ stock_code: "005930", stock_name: "삼성전자", quote: { price: 72000 } }]);
  w.pfAlertsSetCategory("nav");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(!html.includes('id="pfAlertStock"'), "총평가액 모드엔 종목 선택이 없어야 함");
  assert.ok(html.includes('id="pfAlertThreshold"'));
});

test("봇 미설정 시 안내 문구를 보여준다", () => {
  const w = loadAlerts();
  w.PfAlerts.channel = { bot_configured: false, telegram: { connected: false } };
  w.pfAlertsRenderChannel();
  const html = w.document.getElementById("pfAlertChannelStatus").innerHTML;
  assert.match(html, /TELEGRAM_BOT_TOKEN/);
});

test("연결됨 상태는 테스트/해제 버튼을 노출한다", () => {
  const w = loadAlerts();
  w.PfAlerts.channel = { bot_configured: true, telegram: { connected: true, enabled: true, username: "mybot" } };
  w.pfAlertsRenderChannel();
  const actions = w.document.getElementById("pfAlertChannelActions").innerHTML;
  assert.match(actions, /테스트 전송/);
  assert.match(actions, /연결 해제/);
});

test("규칙 목록은 종목명과 방향, 무장 상태를 라벨로 보여준다", () => {
  const w = loadAlerts([{ stock_code: "005930", stock_name: "삼성전자", quote: { price: 72000 } }]);
  w.PfAlerts.alerts = [
    { id: 1, alert_type: "price_above", threshold: 72000, stock_code: "005930", enabled: 1, armed: 1, note: "목표" },
  ];
  w.pfAlertsRenderList();
  const html = w.document.getElementById("pfAlertList").innerHTML;
  assert.match(html, /삼성전자/);
  assert.match(html, /이상/);
  assert.match(html, /대기/);
  assert.match(html, /목표/);
});

test("pfOpenAlerts 는 모달을 표시한다", () => {
  const w = loadAlerts();
  w.pfOpenAlerts();
  assert.equal(w.document.getElementById("pfAlertsModal").style.display, "flex");
});
