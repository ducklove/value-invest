// jsdom behavior tests for pfSyncSimpleTableHeight — 모바일 간편 모드 표 박스 높이.
//
// 이 박스가 화면 하단(고정 탭바 바로 위)보다 길면 마지막 행이 탭바에 가리는 동시에
// 페이지 자체에 스크롤이 생겨, 맨 위 요약 카드(집계 숫자)가 밀려 올라간다. 고정
// 상수(calc(100dvh - 182px))로는 터치영역·safe-area 로 달라지는 크롬 높이를 못 따라가
// 실제로 60px 가까이 어긋났던 자리라, 실측 로직을 행위로 고정해 둔다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

const STORE_SRC = read("static", "js", "portfolio-store.js");
const SHELL_SRC = read("static", "js", "portfolio-shell.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

// jsdom 은 레이아웃이 없어 모든 rect 가 0 이다 — 화면 기하를 직접 심어준다.
function stubRect(el, { top = 0, height = 0, width = 375 } = {}) {
  el.getBoundingClientRect = () => ({
    top, height, width, left: 0, right: width, bottom: top + height, x: 0, y: top,
  });
}

function buildWindow({ innerHeight = 812, tabbarHeight = 53 } = {}) {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="portfolioView">
      <div class="pf-summary" id="pfSummary"></div>
      <div id="pfHoldingsTab"><div class="pf-table-wrap"></div></div>
    </div>
    <nav class="mobile-tabbar"></nav>
  </body></html>`, { runScripts: "dangerously", url: "https://app.example.com/portfolio" });
  const { window: w } = dom;
  w.matchMedia = () => ({ matches: false, addListener() {}, removeListener() {}, addEventListener() {}, removeEventListener() {} });
  appendScript(w, STORE_SRC);
  appendScript(w, SHELL_SRC);
  Object.defineProperty(w, "innerHeight", { value: innerHeight, configurable: true });
  const wrap = w.document.querySelector("#pfHoldingsTab .pf-table-wrap");
  const tabbar = w.document.querySelector(".mobile-tabbar");
  stubRect(tabbar, { top: innerHeight - tabbarHeight, height: tabbarHeight });
  return { w, wrap, tabbar };
}

const maxOf = (wrap) => wrap.style.getPropertyValue("--pf-simple-table-max");

test("표 박스 높이를 하단 탭바 바로 위까지로 실측해 넣는다", () => {
  const { w, wrap } = buildWindow({ innerHeight: 812, tabbarHeight: 53 });
  w.PfStore.prefs.simpleMode = true;
  stubRect(wrap, { top: 190, height: 400 }); // 요약·탭바 아래에서 시작

  w.pfSyncSimpleTableHeight();

  // 812 - 190(위 크롬) - 53(하단 탭바) - 4(여백) = 565
  assert.equal(maxOf(wrap), "565px");
});

test("하단 탭바가 두꺼워지면(safe-area) 그만큼 박스를 줄인다", () => {
  const { w, wrap } = buildWindow({ innerHeight: 812, tabbarHeight: 87 }); // 53 + iOS 34
  w.PfStore.prefs.simpleMode = true;
  stubRect(wrap, { top: 190, height: 400 });

  w.pfSyncSimpleTableHeight();

  assert.equal(maxOf(wrap), "531px");
});

test("간편 모드가 아니면 실측값을 걷어내 CSS 폴백으로 되돌린다", () => {
  const { w, wrap } = buildWindow();
  w.PfStore.prefs.simpleMode = true;
  stubRect(wrap, { top: 190, height: 400 });
  w.pfSyncSimpleTableHeight();
  assert.notEqual(maxOf(wrap), "");

  w.PfStore.prefs.simpleMode = false;
  w.pfSyncSimpleTableHeight();

  assert.equal(maxOf(wrap), "");
});

test("보유종목 탭이 숨어 있으면(0×0) 잘못 재지 않는다", () => {
  const { w, wrap } = buildWindow();
  w.PfStore.prefs.simpleMode = true;
  stubRect(wrap, { top: 0, height: 0, width: 0 }); // display:none

  w.pfSyncSimpleTableHeight();

  assert.equal(maxOf(wrap), "");
});

test("화면이 아주 낮아도 표가 접히지 않도록 최소 높이를 지킨다", () => {
  const { w, wrap } = buildWindow({ innerHeight: 300, tabbarHeight: 53 });
  w.PfStore.prefs.simpleMode = true;
  stubRect(wrap, { top: 190, height: 40 });

  w.pfSyncSimpleTableHeight();

  assert.equal(maxOf(wrap), "120px"); // 300-190-53-4 = 53 → 하한 적용
});
