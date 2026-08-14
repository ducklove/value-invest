// jsdom behavior tests for the holdings-table column show/hide toggles
// (PF_COL_DEFS + localStorage 'pf_col_vis' + the injected <style> tag).
// The Python structure tests only pin the strings; these pin the behavior:
// new columns default correctly, stored choices win, and toggling actually
// writes a display:none rule for that column's class.

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
const UTILS_SRC = read("static", "js", "utils.js");
const SHELL_SRC = read("static", "js", "portfolio-shell.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function loadColumnDom(stored) {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div class="pf-col-toggle-wrap" id="pfColToggles"></div>
    <table class="pf-table"><thead><tr>
      <th class="pf-col-date">등록일자</th>
      <th class="pf-col-memo">메모</th>
      <th class="pf-col-act"></th>
    </tr></thead><tbody></tbody></table>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  if (stored !== undefined) w.localStorage.setItem("pf_col_vis", JSON.stringify(stored));
  appendScript(w, STORE_SRC);
  appendScript(w, UTILS_SRC);
  appendScript(w, SHELL_SRC);
  return w;
}

const injectedCss = (w) =>
  [...w.document.head.querySelectorAll("style")].map(el => el.textContent).join("\n");

test("메모 컬럼은 토글 가능한 컬럼 목록에 있고 기본은 숨김", () => {
  const w = loadColumnDom();
  // PF_COL_DEFS 는 classic script 의 top-level const 라 window 프로퍼티가
  // 아니다 — 전역 렉시컬 환경에서 꺼내온다.
  const defs = w.eval("PF_COL_DEFS");
  const memo = defs.find(c => c.key === "memo");

  assert.ok(memo, "PF_COL_DEFS 에 memo 컬럼이 없다");
  assert.equal(memo.cls, "pf-col-memo");
  assert.equal(memo.label, "메모");
  // 데이터 컬럼 중 마지막 — 토글 줄 순서가 표의 열 순서와 같아야 한다.
  assert.equal(defs[defs.length - 1].key, "memo");

  assert.equal(w._pfGetColVisibility().memo, false);
});

test("저장된 선택이 없던 신규 컬럼은 defaultVisible 로 떨어지고 기존 선택은 유지된다", () => {
  // 'pf_col_vis' 를 이미 갖고 있던 사용자 — memo 키는 아직 없다.
  const w = loadColumnDom({ group: false, date: true });
  const vis = w._pfGetColVisibility();

  assert.equal(vis.group, false, "저장된 숨김 선택이 유지돼야 한다");
  assert.equal(vis.date, true, "저장된 표시 선택이 defaultVisible:false 를 이겨야 한다");
  assert.equal(vis.memo, false, "저장된 적 없는 새 컬럼은 defaultVisible 을 따른다");
});

test("_pfRenderColToggles 는 기본 숨김 컬럼을 display:none 규칙으로 감춘다", () => {
  const w = loadColumnDom();
  w._pfRenderColToggles();

  assert.match(injectedCss(w), /\.pf-col-memo \{ display: none !important; \}/);

  const cb = w.document.querySelector('.js-pf-col-toggle[data-col-key="memo"]');
  assert.ok(cb, "메모 체크박스가 렌더되지 않았다");
  assert.equal(cb.checked, false);
});

test("pfToggleCol('memo', true) 는 숨김 규칙을 걷고 선택을 localStorage 에 남긴다", () => {
  const w = loadColumnDom();
  w._pfRenderColToggles();

  w.pfToggleCol("memo", true);
  assert.doesNotMatch(injectedCss(w), /\.pf-col-memo \{ display: none/);
  assert.equal(JSON.parse(w.localStorage.getItem("pf_col_vis")).memo, true);
  // 다른 컬럼의 기본 숨김은 그대로 — 토글은 한 컬럼만 건드린다.
  assert.match(injectedCss(w), /\.pf-col-date \{ display: none !important; \}/);

  w.pfToggleCol("memo", false);
  assert.match(injectedCss(w), /\.pf-col-memo \{ display: none !important; \}/);
  assert.equal(JSON.parse(w.localStorage.getItem("pf_col_vis")).memo, false);
});
