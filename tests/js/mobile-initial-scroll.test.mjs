// jsdom behavior tests for holdPageScrollTop — 모바일 첫 진입 시 화면이 이미 조금
// 내려간 상태로 열려 포트폴리오 요약(집계 숫자)이 가려지던 문제의 재발 방지.
//
// 핵심은 "한 번 더 되돌린다"가 아니라 "데이터가 채워진 뒤의 뒤늦은 복원까지 되돌린다"
// 이다 — 진입 직후에는 표가 비어 문서가 짧아 스크롤 자체가 불가능하고, 모바일
// 브라우저의 위치 복원(탭 되살리기·bfcache)은 그 뒤에 들어온다. 동시에 사용자가
// 스스로 내린 화면을 도로 끌어올리면 안 되므로 첫 입력에서 즉시 손을 떼야 한다.

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

// window.scrollY / scrollTo / setTimeout 을 갈아끼워 스크롤 위치와 타이머를
// 결정적으로 조작한다. jsdom 은 실제 스크롤이 없고, 스크립트는 호출 시점에
// 전역에서 setTimeout 을 찾으므로 이 교체가 그대로 먹힌다.
function instrument(w) {
  let y = 0;
  const scrolls = [];
  const timers = [];
  Object.defineProperty(w, "scrollY", { get: () => y, configurable: true });
  w.scrollTo = (_x, top) => { scrolls.push(top); y = top; };
  w.setTimeout = (fn, delay) => {
    timers.push({ fn, delay, id: timers.length + 1, cancelled: false });
    return timers.length;
  };
  w.clearTimeout = (id) => {
    const timer = timers.find(t => t.id === id);
    if (timer) timer.cancelled = true;
  };
  return {
    scrolls,
    setScrollY: (value) => { y = value; },
    getScrollY: () => y,
    // 예약된 타이머를 지연 순서대로 실행 — 실행 중 취소된 것은 건너뛴다.
    runTimers: () => {
      [...timers].sort((a, b) => a.delay - b.delay).forEach(timer => {
        if (!timer.cancelled) timer.fn();
      });
    },
    pendingCount: () => timers.filter(t => !t.cancelled).length,
  };
}

function buildWindow() {
  const dom = new JSDOM(`<!doctype html><html><body></body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/portfolio",
  });
  const { window: w } = dom;
  // portfolio-shell.js 는 로드 시점 IIFE 에서 matchMedia 를 부른다(jsdom 미구현).
  w.matchMedia = () => ({ matches: false, addListener() {}, removeListener() {}, addEventListener() {}, removeEventListener() {} });
  appendScript(w, STORE_SRC);
  appendScript(w, SHELL_SRC);
  return w;
}

test("데이터가 채워진 뒤 브라우저가 이전 위치를 되살려도 최상단으로 되돌린다", () => {
  const w = buildWindow();
  const io = instrument(w);

  w.holdPageScrollTop();
  assert.equal(io.scrolls.length, 0, "이미 최상단이면 즉시 호출에서는 건드리지 않는다");

  io.setScrollY(140); // 표가 채워진 뒤 뒤늦게 복원된 스크롤
  io.runTimers();

  assert.ok(io.scrolls.length > 0, "예약된 보정이 최상단으로 되돌려야 한다");
  assert.equal(io.getScrollY(), 0);
});

test("사용자가 먼저 스크롤하면 더 이상 끌어올리지 않는다", () => {
  const w = buildWindow();
  const io = instrument(w);

  w.holdPageScrollTop();
  w.dispatchEvent(new w.Event("touchstart"));
  assert.equal(io.pendingCount(), 0, "첫 입력에서 예약된 보정이 모두 해제돼야 한다");

  io.setScrollY(300); // 사용자가 의도적으로 내린 위치
  io.runTimers();

  assert.equal(io.getScrollY(), 300);
  assert.equal(io.scrolls.length, 0);
});

test("이미 최상단이면 스크롤 API 를 호출하지 않는다", () => {
  const w = buildWindow();
  const io = instrument(w);

  w.holdPageScrollTop();
  io.runTimers();

  assert.equal(io.scrolls.length, 0);
});

test("holdPageScrollTop 은 보정을 끝낸 뒤 리스너를 남기지 않는다", () => {
  const w = buildWindow();
  const io = instrument(w);
  const added = [];
  const removed = [];
  const origAdd = w.addEventListener.bind(w);
  const origRemove = w.removeEventListener.bind(w);
  w.addEventListener = (type, fn, opts) => { added.push(type); return origAdd(type, fn, opts); };
  w.removeEventListener = (type, fn, opts) => { removed.push(type); return origRemove(type, fn, opts); };

  w.holdPageScrollTop();
  io.runTimers(); // 마지막 타이머가 release() 를 호출

  assert.deepEqual([...removed].sort(), [...added].sort());
});
