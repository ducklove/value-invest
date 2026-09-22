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

const COUNTRIES = [
  { code: 'kr', name: '한국', flag: '🇰🇷' },
  { code: 'us', name: '미국', flag: '🇺🇸' },
  { code: 'jp', name: '일본', flag: '🇯🇵' },
];
const RULES = [
  { country: 'kr', min_importance: 'all', starts_at: `${TODAY}T12:00:00+09:00` },
  { country: 'us', min_importance: 'mid', starts_at: `${TODAY}T12:00:00+09:00` },
  { country: 'jp', min_importance: 'high', starts_at: `${TODAY}T12:00:00+09:00` },
];

async function rulePanel(rules = []) {
  const w = load(12, 0);
  w.eval("currentUser = { google_sub: 'u1' }");
  w._ecResetUserState();
  w._ecRenderShell();
  w.apiFetchJson = async () => ({ rules, countries: COUNTRIES });
  await w._ecLoadRules();
  return w;
}

test('국가별 전체·중 이상·상을 추가하고 표시 필터와 독립적으로 저장한다', async () => {
  const w = await rulePanel();
  for (let i = 0; i < 3; i++) w.document.getElementById('econCalRuleAdd').click();
  const levels = w.document.querySelectorAll('[data-field="min_importance"]');
  ['all', 'mid', 'high'].forEach((value, i) => {
    levels[i].value = value;
    levels[i].dispatchEvent(new w.Event('change'));
  });
  const before = ['high', 'mid', 'low'].map(level => w._ecLevelParam(level));
  w.apiFetchJson = async (url, options) => {
    assert.equal(url, '/api/notifications/calendar/rules');
    assert.equal(options.method, 'PUT');
    assert.deepEqual(JSON.parse(options.body).rules, RULES.map(({ starts_at, ...rule }) => rule));
    return { rules: RULES, countries: COUNTRIES };
  };
  await w._ecSaveRules();
  assert.equal(w.document.getElementById('econCalRuleStatus').textContent, '저장했습니다.');
  assert.deepEqual(['high', 'mid', 'low'].map(level => w._ecLevelParam(level)), before);
  assert.equal(w.document.getElementById('econCalRuleFields').disabled, false);
});

test('저장된 조건을 복원하며 같은 국가를 중복 선택할 수 없다', async () => {
  const w = await rulePanel(RULES);
  assert.deepEqual([...w.document.querySelectorAll('[data-field="min_importance"]')].map(el => el.value), ['all', 'mid', 'high']);
  const first = w.document.querySelector('[data-field="country"]');
  assert.equal(first.querySelector('[value="us"]').disabled, true);
  assert.equal(w.document.getElementById('econCalRuleAdd').disabled, true);
  w.document.querySelector('[data-remove="1"]').click();
  assert.equal(w.document.getElementById('econCalRuleAdd').disabled, false);
  assert.equal(w.document.querySelector('[data-field="country"] [value="us"]').disabled, false);
});

test('조건 알림 대상은 표시하고 조건 밖 일정에는 개별 체크박스를 유지한다', async () => {
  const w = await rulePanel(RULES);
  const events = [
    ['kr', 'low'], ['us', 'mid'], ['us', 'low'], ['jp', 'high'], ['jp', 'mid'],
  ].map(([country, importance], i) => ({
    date: TOMORROW, datetime: `${TOMORROW} 14:00:00`, time: '14:00',
    country, importance, index_id: String(i), event: `${country}-${importance}`,
  }));
  w._ecRenderBody({ events });
  assert.equal(w.document.querySelectorAll('.ec-rule-badge').length, 3);
  assert.equal(w.document.querySelectorAll('.ec-bell-cb').length, 2);
  assert.deepEqual([...w.document.querySelectorAll('.ec-bell-cb')].map(cb => cb.dataset.eid), ['2', '4']);
  assert.equal(w._ecRuleMatches({ ...events[0], datetime: `${TODAY} 11:00:00` }), false);
});

test('조건 저장 후 ID가 없는 일정도 화면에 남아 있다', async () => {
  const w = await rulePanel(RULES);
  w._ecRenderBody({ events: [{ date: TOMORROW, time: '09:00', event: 'ID 없는 일정' }] });
  await w._ecSaveRules();
  assert.match(w.document.getElementById('econCalBody').textContent, /ID 없는 일정/);
});

test('조건 전체 삭제를 빈 목록으로 저장한다', async () => {
  const w = await rulePanel(RULES);
  while (w.document.querySelector('[data-remove]')) w.document.querySelector('[data-remove]').click();
  w.apiFetchJson = async (url, options) => {
    assert.deepEqual(JSON.parse(options.body), { rules: [] });
    return { rules: [], countries: COUNTRIES };
  };
  await w._ecSaveRules();
  assert.equal(w.document.getElementById('econCalRuleStatus').textContent, '저장했습니다.');
});

test('저장 실패는 편집 내용과 기존 적용 조건을 유지하며 채널 연결을 안내한다', async () => {
  const w = await rulePanel(RULES);
  let prompted = false;
  w._ecPromptChannel = async () => { prompted = true; };
  w.document.querySelector('[data-remove="0"]').click();
  w.apiFetchJson = async () => { throw Object.assign(new Error('채널 연결 필요'), { status: 409 }); };
  await w._ecSaveRules();
  assert.equal(w.document.querySelectorAll('[data-rule-index]').length, 2);
  assert.equal(w.eval('_ecRules.length'), 3);
  assert.equal(w.document.getElementById('econCalRuleFields').disabled, false);
  assert.equal(prompted, true);
  assert.match(w.document.getElementById('econCalRuleStatus').textContent, /채널 연결/);
});

test('로그인 전과 조회 실패 시 저장 가능한 빈 폼을 표시하지 않는다', async () => {
  const w = await rulePanel();
  w.apiFetchJson = async () => { throw new Error('network'); };
  await w._ecLoadRules();
  assert.equal(w.document.getElementById('econCalRuleSave'), null);
  assert.match(w.document.getElementById('econCalAlertSettings').textContent, /다시 시도/);
  w.eval('currentUser = null');
  await w._ecLoadRules();
  assert.match(w.document.getElementById('econCalAlertSettings').textContent, /로그인 후/);
});

test('계정을 바꾸면 이전 사용자의 개별·조건 알림 상태를 비운다', async () => {
  const w = await rulePanel(RULES);
  w.apiFetchJson = async () => ({ event_ids: ['u2-event'], rules: [], automatic_event_ids: [] });
  w.eval("currentUser = { google_sub: 'u2' }");
  await w._ecLoadSubs();
  assert.equal(w.eval('_ecRules.length'), 0);
  assert.deepEqual([...w.eval('_ecSubs')], ['u2-event']);
});
