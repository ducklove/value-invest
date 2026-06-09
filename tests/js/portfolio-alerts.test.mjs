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

test("카테고리 -> {alert_type} 매핑", () => {
  const w = loadAlerts();
  // {...} 로 현재 realm 객체로 복사해 cross-realm prototype 차이 회피.
  assert.deepEqual({ ...w.pfAlertsBuildType("price", "above") }, { alert_type: "price_above" });
  assert.deepEqual({ ...w.pfAlertsBuildType("target", "above") }, { alert_type: "target_reached" });
  assert.deepEqual({ ...w.pfAlertsBuildType("limit", "above") }, { alert_type: "limit_reached" });
  assert.deepEqual({ ...w.pfAlertsBuildType("dailyAbs", "above") }, { alert_type: "daily_change_abs" });
  assert.deepEqual({ ...w.pfAlertsBuildType("nav", "above") }, { alert_type: "nav_above" });
  assert.deepEqual({ ...w.pfAlertsBuildType("daily", "below") }, { alert_type: "daily_change_below" });
});

test("지정가 폼: 종목 + 방향 + 지정가 입력", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("price");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.match(html, /삼성전자/);
  assert.ok(html.includes('id="pfAlertStock"'));
  assert.ok(html.includes('id="pfAlertThreshold"'));
});

test("폼에 중요 알림 체크박스가 있다", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("price");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(html.includes('id="pfAlertImportant"'), "중요 체크박스 존재");
});

test("목표가 도달(전체) 폼: 종목 선택·임계값 모두 없음", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("target");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(!html.includes('id="pfAlertStock"'), "전체 대상이라 종목 선택 없음");
  assert.ok(!html.includes('id="pfAlertThreshold"'), "임계값 입력 없음");
  assert.match(html, /목표가/);
});

test("종목 일간 등락률(전체) 폼: 종목 없이 ±% 입력만", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("dailyAbs");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(!html.includes('id="pfAlertStock"'), "전체 대상이라 종목 선택 없음");
  assert.ok(html.includes('id="pfAlertThreshold"'));
});

test("상하한가 도달(전체) 폼: 종목 선택·임계값 모두 없음", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("limit");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(!html.includes('id="pfAlertStock"'), "전체 대상이라 종목 선택 없음");
  assert.ok(!html.includes('id="pfAlertThreshold"'), "임계값 입력 없음");
  assert.match(html, /상한가/);
});

test("총평가액 폼: 종목 선택 없음", () => {
  const w = loadAlerts([SAMSUNG]);
  w.pfAlertsSetCategory("nav");
  const html = w.document.getElementById("pfAlertForm").innerHTML;
  assert.ok(!html.includes('id="pfAlertStock"'));
  assert.ok(html.includes('id="pfAlertThreshold"'));
});

test("채널: 텔레그램 연결됨 행 + 카카오 등록 폼", () => {
  const w = loadAlerts();
  w.PfAlerts.channels = {
    telegram: { connected: true, enabled: true, username: "mybot" },
    kakao: { connected: false, redirect_uri: "https://x.test/api/notifications/kakao/callback" },
  };
  w.pfAlertsRenderChannels();
  const html = w.document.getElementById("pfAlertChannels").innerHTML;
  assert.match(html, /텔레그램/);
  assert.match(html, /테스트/);                 // 텔레그램 연결됨 -> 테스트 버튼
  assert.ok(html.includes('id="pfKkKey"'));     // 카카오 미연결 -> REST 키 입력
  assert.match(html, /kakao\/callback/);        // Redirect URI 안내
});

test("텔레그램 미연결 행은 봇 토큰 등록 폼을 보인다", () => {
  const w = loadAlerts();
  w.PfAlerts.channels = {
    telegram: { connected: false },
    kakao: { connected: false, redirect_uri: "https://x.test/cb" },
  };
  w.pfAlertsRenderChannels();
  const html = w.document.getElementById("pfAlertChannels").innerHTML;
  assert.ok(html.includes('id="pfTgToken"'), "봇 토큰 입력");
  assert.ok(html.includes('id="pfTgChat"'), "chat_id 입력");
});

test("규칙 목록: 블랭킷/포트폴리오 라벨", () => {
  const w = loadAlerts([SAMSUNG]);
  w.PfAlerts.alerts = [
    { id: 1, alert_type: "target_reached", scope: "all_stocks", threshold: 0, enabled: 1, armed: 1, note: "" },
    { id: 2, alert_type: "daily_change_abs", scope: "all_stocks", threshold: 5, enabled: 1, armed: 1, note: "" },
    { id: 3, alert_type: "daily_change_above", scope: "portfolio", threshold: 3, enabled: 1, armed: 0, note: "" },
    { id: 4, alert_type: "limit_reached", scope: "all_stocks", threshold: 0, enabled: 1, armed: 1, note: "" },
  ];
  w.pfAlertsRenderList();
  const html = w.document.getElementById("pfAlertList").innerHTML;
  assert.match(html, /보유 전 종목 — 목표가 도달 시/);
  assert.match(html, /보유 전 종목 — 일간 등락률 ±5% 이상/);
  assert.match(html, /포트폴리오 일간 등락률/);
  assert.match(html, /보유 전 종목 — 상한가·하한가 도달 시/);
});

test("규칙 목록: 중요 규칙은 배지·강조 카드·별 토글을 보인다", () => {
  const w = loadAlerts([SAMSUNG]);
  w.PfAlerts.alerts = [
    { id: 1, alert_type: "price_above", scope: "stock", stock_code: "005930", threshold: 72000, enabled: 1, armed: 1, note: "", important: 1 },
    { id: 2, alert_type: "nav_above", scope: "portfolio", threshold: 1000, enabled: 1, armed: 1, note: "", important: 0 },
  ];
  w.pfAlertsRenderList();
  const html = w.document.getElementById("pfAlertList").innerHTML;
  assert.match(html, /🚨 중요/);                                // 중요 배지
  assert.match(html, /class="pf-alert-rule\s+important"/);      // 카드 강조 클래스
  assert.match(html, /pfAlertsToggleImportant\(1, false\)/);    // 중요 → 해제 토글
  assert.match(html, /pfAlertsToggleImportant\(2, true\)/);     // 일반 → 중요 토글
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
