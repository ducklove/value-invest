// jsdom tests for static/js/economic-calendar.js — 현재 시각 표시선(ec-now-line).
//
// Loads utils.js (escapeHtml) + economic-calendar.js into a jsdom window with a
// fixed clock, then exercises _ecRenderBody()'s "now line" placement. Network
// (loadEconomicCalendar) and 구독 토글은 여기서 다루지 않는다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (p) => readFileSync(join(__dirname, "..", "..", "static", "js", p), "utf8");
const UTILS = read("utils.js");
const EC = read("economic-calendar.js");

// new Date()/Date.now() 를 2026-06-09 hh:mm 으로 고정해 로드한다(인자 있는
// new Date(y,m,d,...) 는 정상 동작 유지 — 그룹핑/요일 계산이 깨지지 않게).
function load(hour, minute) {
  const dom = new JSDOM(
    "<!doctype html><body><div id='econCalContent'></div><div id='econCalBody'></div></body>",
    { runScripts: "dangerously", url: "https://app.example.com/" },
  );
  const { window } = dom;
  const RealDate = window.Date;
  const fixed = new RealDate(2026, 5, 9, hour, minute).getTime();
  class MockDate extends RealDate {
    constructor(...a) { if (a.length === 0) super(fixed); else super(...a); }
    static now() { return fixed; }
  }
  window.Date = MockDate;
  for (const src of [UTILS, EC]) {
    const s = window.document.createElement("script");
    s.textContent = src;
    window.document.body.appendChild(s);
  }
  return window;
}

const TODAY = "2026-06-09";
const TOMORROW = "2026-06-10";
const mkEvents = () => [
  { date: TODAY, time: "09:00", country_name: "미국", event: "A", importance: "high" },
  { date: TODAY, time: "14:00", country_name: "미국", event: "B", importance: "high" },
  { date: TODAY, time: "20:00", country_name: "미국", event: "C", importance: "high" },
  { date: TOMORROW, time: "10:00", country_name: "미국", event: "D", importance: "high" },
];

// 오늘 그룹의 ec-row / ec-now-line 순서를 ["09:00","NOW",...] 로 추출.
function todaySeq(window) {
  const groups = window.document.getElementById("econCalBody").querySelectorAll(".ec-daygroup");
  return [...groups[0].children]
    .filter((c) => c.classList.contains("ec-row") || c.classList.contains("ec-now-line"))
    .map((c) => (c.classList.contains("ec-now-line") ? "NOW" : c.querySelector(".ec-time").textContent));
}

test("현재 시각선은 지난 일정과 다가올 일정 사이에 들어간다", () => {
  const w = load(12, 0);  // 12:00 — 09:00 과거, 14:00 미래
  w._ecRenderBody({ events: mkEvents() });
  assert.deepEqual(todaySeq(w), ["09:00", "NOW", "14:00", "20:00"]);
});

test("모든 일정이 미래면 현재 시각선이 맨 위에 온다", () => {
  const w = load(6, 0);  // 06:00
  w._ecRenderBody({ events: mkEvents() });
  assert.deepEqual(todaySeq(w), ["NOW", "09:00", "14:00", "20:00"]);
});

test("모든 일정이 과거면 현재 시각선이 맨 아래에 온다", () => {
  const w = load(23, 0);  // 23:00
  w._ecRenderBody({ events: mkEvents() });
  assert.deepEqual(todaySeq(w), ["09:00", "14:00", "20:00", "NOW"]);
});

test("오늘이 아닌 날짜 그룹에는 현재 시각선이 없다", () => {
  const w = load(12, 0);
  w._ecRenderBody({ events: mkEvents() });
  const groups = w.document.getElementById("econCalBody").querySelectorAll(".ec-daygroup");
  assert.equal(groups[0].querySelectorAll(".ec-now-line").length, 1, "오늘 그룹엔 하나");
  assert.equal(groups[1].querySelectorAll(".ec-now-line").length, 0, "내일 그룹엔 없음");
});

test("표시선 라벨은 현재 시각(HH:MM)을 보여준다", () => {
  const w = load(9, 5);
  w._ecRenderBody({ events: mkEvents() });
  const line = w.document.getElementById("econCalBody").querySelector(".ec-now-line span");
  assert.equal(line.textContent, "09:05");
});

test("⚙ 설정 버튼이 필터 패널의 hidden 속성을 토글한다", () => {
  const w = load(12, 0);
  w._ecRenderShell();
  const panel = w.document.getElementById("econCalSettings");
  const toggle = w.document.getElementById("econCalSettingsToggle");
  assert.ok(panel.hasAttribute("hidden"), "초기엔 숨김 상태");
  toggle.click();
  assert.ok(!panel.hasAttribute("hidden"), "클릭하면 펼쳐짐");
  toggle.click();
  assert.ok(panel.hasAttribute("hidden"), "다시 클릭하면 숨겨짐");
});

test("결과 알림 체크박스는 미래 미발표 추적 이벤트에만 렌더된다", () => {
  const w = load(12, 0);
  w._ecRenderShell();
  w._ecRenderBody({
    events: [
      { date: TODAY, datetime: `${TODAY} 14:00:00`, time: "14:00", event: "미래", index_id: "future-1" },
      { date: TODAY, datetime: `${TODAY} 10:00:00`, time: "10:00", event: "과거", index_id: "past-1" },
      { date: TODAY, datetime: `${TODAY} 15:00:00`, time: "15:00", event: "ID 없음" },
      { date: TODAY, datetime: `${TODAY} 16:00:00`, time: "16:00", event: "발표됨", index_id: "actual-1", actual: "3.1" },
    ],
  });

  const checks = [...w.document.querySelectorAll(".ec-bell-cb")];
  assert.equal(checks.length, 1);
  assert.equal(checks[0].dataset.eid, "future-1");
});

test("구독한 일정의 결과가 나오면 완료 마커와 행 강조를 렌더한다", () => {
  const w = load(12, 0);
  w.eval("_ecSubs = new Set(['done-1'])");
  w._ecRenderShell();
  w._ecRenderBody({
    events: [
      { date: TODAY, datetime: `${TODAY} 11:00:00`, time: "11:00", event: "발표됨", index_id: "done-1", actual: "2.0" },
    ],
  });

  const row = w.document.querySelector(".ec-row");
  assert.ok(row.classList.contains("ec-row-alerted"));
  assert.equal(row.querySelector(".ec-bell-done").textContent, "🔔");
  assert.equal(row.querySelector(".ec-bell-cb"), null);
});

test("결과 알림 토글·완료 마커는 hover title 외에 aria-label 로도 읽힌다", () => {
  const w = load(12, 0);
  w.eval("_ecSubs = new Set(['done-1'])");
  w._ecRenderShell();
  w._ecRenderBody({
    events: [
      { date: TODAY, datetime: `${TODAY} 14:00:00`, time: "14:00", event: "미래", index_id: "future-1" },
      { date: TODAY, datetime: `${TODAY} 11:00:00`, time: "11:00", event: "발표됨", index_id: "done-1", actual: "2.0" },
    ],
  });
  // 체크박스는 이름 없는 컨트롤이 되지 않도록 aria-label 을 갖는다.
  const cb = w.document.querySelector(".ec-bell-cb");
  assert.equal(cb.getAttribute("aria-label"), "결과 발표 시 알림 받기");
  // 완료 마커(🔔)는 title 전용이 아니라 role=img + aria-label 로도 전달된다.
  const done = w.document.querySelector(".ec-bell-done");
  assert.equal(done.getAttribute("role"), "img");
  assert.equal(done.getAttribute("aria-label"), "구독한 일정의 결과가 발표됨");
});

test("결과 알림 토글은 로그인과 채널 게이트를 실제 동작으로 처리한다", async () => {
  const w = load(12, 0);
  w._ecRenderShell();
  w._ecRenderBody({
    events: [
      { date: TODAY, datetime: `${TODAY} 14:00:00`, time: "14:00", event: "미래", index_id: "future-1" },
    ],
  });
  const cb = w.document.querySelector(".ec-bell-cb");

  const confirms = [];
  // 네이티브 confirm() → utils.js 의 confirmModal(Promise<boolean>) 로 교체됨.
  w.confirmModal = (message) => { confirms.push(message); return Promise.resolve(false); };
  cb.checked = true;
  await w._ecToggleSubscription(cb);
  assert.equal(cb.checked, false);
  assert.match(confirms[0], /로그인 후 이용/);

  w.eval("currentUser = { email: 'user@example.com' }");
  let posted = null;
  w.apiFetch = async (url, init) => {
    posted = { url, init };
    return { ok: false, status: 409 };
  };
  cb.checked = true;
  await w._ecToggleSubscription(cb);
  assert.equal(cb.checked, false);
  assert.match(confirms.at(-1), /텔레그램 또는 카카오톡 연결/);
  assert.equal(posted.url, "/api/notifications/calendar");
  assert.equal(JSON.parse(posted.init.body).event_id, "future-1");
});

test("confirmModal 은 확인=true / 취소=false 를 Promise 로 돌려준다", async () => {
  const w = load(12, 0);
  const okPromise = w.confirmModal("진행할까요?");
  const okBtn = w.document.querySelector(".confirm-modal .confirm-modal-ok");
  assert.ok(okBtn, "확인 버튼 렌더");
  assert.match(w.document.querySelector(".confirm-modal .confirm-modal-msg").textContent, /진행할까요/);
  okBtn.click();
  assert.equal(await okPromise, true);
  // 확인 후 오버레이는 DOM 에서 제거된다.
  assert.equal(w.document.querySelector(".confirm-modal"), null);

  const cancelPromise = w.confirmModal("진행할까요?");
  w.document.querySelector(".confirm-modal .confirm-modal-cancel").click();
  assert.equal(await cancelPromise, false);
});
