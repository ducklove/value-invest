// jsdom behavior test for static/js/portfolio-alerts.js.
//
// 조건 알림 모달의 순수 렌더링/매핑 로직(폼 생성, 채널 행 렌더, 규칙 목록 라벨,
// 카테고리→{alert_type,scope} 매핑)을 실제 소스를 브라우저와 같은 순서로 올려
// 검증한다. fetch 가 필요한 비동기 로딩은 백엔드 통합 테스트가 담당한다.

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
      <button class="pf-alert-help-toggle" onclick="pfAlertsToggleHelp()" aria-expanded="false">연결 방법 ▾</button>
      <div class="pf-alert-help" id="pfAlertHelp" hidden></div>
      <div class="pf-alert-channels" id="pfAlertChannels"></div>
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
  window.fetch = () => Promise.reject(new Error("no fetch in test"));
  for (const src of SOURCES) {
    const script = window.document.createElement("script");
    script.textContent = src;
    window.document.body.appendChild(script);
  }
  window.PfStore.items = items;
  return window;
}

const SAMSUNG = { stock_code: "005930", stock_name: "삼성전자", quote: { price: 72000 } };

test("카테고리 -> {alert_type, scope} 매핑", () => {
  const w = loadAlerts();
  // {...} 로 현재 realm 객체로 복사해 cross-realm prototype 차이 회피.
  assert.deepEqual({ ...w.pfAlertsBuildType("price", "above") }, { alert_type: "price_above" });
  assert.deepEqual({ ...w.pfAlertsBuildType("target", "above") }, { alert_type: "target_reached" });
  assert.deepEqual({ ...w.pfAlertsBuildType("stockDaily", "below") }, { alert_type: "daily_change_below", scope: "stock" });
  assert.deepEqual({ ...w.pfAlertsBuildType("nav", "above") }, { alert_type: "nav_above" });
  assert.deepEqual({ ...w.pfAlertsBuildType("daily", "below") }, { alert_type: "daily_change_below", scope: "portfolio" });
});

test("지정가 폼: 종목 + 방향 + 지정가 입력", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("price");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.match(html, /삼성전자/);
  assert.ok(html.includes('id="pfAlertStock"'));
  assert.ok(html.includes('id="pfAlertThreshold"'));
});

test("목표가 달성 폼: 종목만, 임계값 입력 없음", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("target");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(html.includes('id="pfAlertStock"'));
  assert.ok(!html.includes('id="pfAlertThreshold"'), "목표가 달성엔 임계값 입력이 없어야 함");
  assert.match(html, /목표가/);
});

test("종목 일간 등락률 폼: 종목 + 방향 + % 입력", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("stockDaily");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(html.includes('id="pfAlertStock"'));
  assert.ok(html.includes('id="pfAlertThreshold"'));
});

test("총평가액 폼: 종목 선택 없음", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("nav");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(!html.includes('id="pfAlertStock"'));
  assert.ok(html.includes('id="pfAlertThreshold"'));
});

test("채널 행: 텔레그램 연결됨 + 카카오 미설정", () => {
  const w = loadAlerts();
  w.PfAlerts.channels = {
    telegram: { configured: true, connected: true, enabled: true, username: "mybot" },
    kakao: { configured: false, connected: false },
  };
  w.pfAlertsRenderChannels();
  const html = w.document.getElementById("pfAlertChannels").innerHTML;
  assert.match(html, /텔레그램/);
  assert.match(html, /카카오톡/);
  assert.match(html, /테스트/);          // 텔레그램 연결됨 -> 테스트 버튼
  assert.match(html, /서버 미설정/);      // 카카오 미설정 배지
});

test("카카오 미연결(설정됨) 행은 '카카오 로그인' 버튼을 보인다", () => {
  const w = loadAlerts();
  w.PfAlerts.channels = {
    telegram: { configured: false, connected: false },
    kakao: { configured: true, connected: false },
  };
  w.pfAlertsRenderChannels();
  const html = w.document.getElementById("pfAlertChannels").innerHTML;
  assert.match(html, /카카오 로그인/);
});

test("규칙 목록: 목표가 달성/종목 일간 라벨", () => {
  const w = loadAlerts([SAMSUNG]);
  w.PfAlerts.alerts = [
    { id: 1, alert_type: "target_reached", scope: "stock", threshold: 0, stock_code: "005930", enabled: 1, armed: 1, note: "" },
    { id: 2, alert_type: "daily_change_below", scope: "stock", threshold: -5, stock_code: "005930", enabled: 1, armed: 1, note: "" },
    { id: 3, alert_type: "daily_change_above", scope: "portfolio", threshold: 3, enabled: 1, armed: 0, note: "" },
  ];
  w.pfAlertsRenderList();
  const html = w.document.getElementById("pfAlertList").innerHTML;
  assert.match(html, /삼성전자 목표가 달성 시/);
  assert.match(html, /삼성전자 일간 등락률/);
  assert.match(html, /포트폴리오 일간 등락률/);
  assert.match(html, /발송됨/); // armed=0 인 규칙
});

test("pfOpenAlerts 는 모달을 표시한다", () => {
  const w = loadAlerts();
  w.pfOpenAlerts();
  assert.equal(w.document.getElementById("pfAlertsModal").style.display, "flex");
});

test("연결 방법 안내는 토글로 열고 닫힌다", () => {
  const w = loadAlerts();
  const help = w.document.getElementById("pfAlertHelp");
  const btn = w.document.querySelector(".pf-alert-help-toggle");
  assert.ok(help.hasAttribute("hidden"), "초기엔 숨김");
  w.pfAlertsToggleHelp();
  assert.ok(!help.hasAttribute("hidden"), "토글 후 표시");
  assert.equal(btn.getAttribute("aria-expanded"), "true");
  w.pfAlertsToggleHelp();
  assert.ok(help.hasAttribute("hidden"), "다시 토글하면 숨김");
});
