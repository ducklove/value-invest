// jsdom behavior tests for the analysis search combobox ARIA contract —
// index.html 이 role="combobox"/listbox 정적 속성을 선언하고, search.js 가
// 드롭다운 열림/닫힘(aria-expanded)과 활성 옵션(aria-activedescendant,
// aria-selected)을 기존 키보드 내비게이션(ArrowDown/Up, Enter, Escape)과
// 함께 동기화하는지 검증한다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

const SEARCH_SRC = read("static", "js", "search.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function buildDom({ results = [] } = {}) {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div class="search-container">
      <input class="search-input" id="searchInput" autocomplete="off"
             role="combobox" aria-expanded="false" aria-controls="dropdown"
             aria-autocomplete="list" aria-haspopup="listbox" aria-label="종목 검색">
      <div class="dropdown" id="dropdown" role="listbox" aria-label="종목 검색 결과"></div>
    </div>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/analysis",
  });
  const { window: w } = dom;
  // utils.js/auth.js 등이 평소 제공하는 전역들(칩 테스트와 동일 패턴).
  w.searchTimeout = null;
  w.selectedIdx = -1;
  w.isCompactMobileViewport = () => false;
  w.currentUser = null;
  w.recentListItems = [];
  w.trackEvent = () => {};
  w.switchView = () => {};
  w.requireApiConfiguration = () => {};
  w.escapeHtml = (s) => String(s);
  const analyzeCalls = [];
  w.analyzeStock = (code) => analyzeCalls.push(code);
  w.apiFetchJson = async () => results;
  appendScript(w, SEARCH_SRC);
  return { w, analyzeCalls };
}

const RESULTS = [
  { stock_code: "005930", corp_name: "삼성전자" },
  { stock_code: "000660", corp_name: "SK하이닉스" },
];

test("index.html 이 combobox/listbox 정적 계약을 선언한다", () => {
  const html = read("static", "index.html");
  assert.ok(html.includes('id="searchInput"'));
  assert.ok(html.includes('role="combobox"'));
  assert.ok(html.includes('aria-expanded="false"'));
  assert.ok(html.includes('aria-controls="dropdown"'));
  assert.ok(html.includes('aria-autocomplete="list"'));
  assert.match(html, /<div class="dropdown" id="dropdown" role="listbox"/);
});

test("검색 결과가 열리면 aria-expanded=true 가 되고 항목은 role=option", async () => {
  const { w } = buildDom({ results: RESULTS });
  assert.equal(w.searchInput.getAttribute("aria-expanded"), "false");

  await w.doSearch("반도체");
  assert.equal(w.dropdown.classList.contains("show"), true);
  assert.equal(w.searchInput.getAttribute("aria-expanded"), "true");
  const options = [...w.dropdown.querySelectorAll('[role="option"]')];
  assert.equal(options.length, 2);
  assert.ok(options.every((o) => o.getAttribute("aria-selected") === "false"));
});

test("ArrowDown 은 활성 옵션을 aria-activedescendant/aria-selected 로 반영한다", async () => {
  const { w } = buildDom({ results: RESULTS });
  await w.doSearch("반도체");

  w.searchInput.dispatchEvent(new w.KeyboardEvent("keydown", { key: "ArrowDown" }));
  const active = w.dropdown.querySelector(".dropdown-item.active");
  assert.ok(active, "활성 옵션이 표시돼야 함");
  assert.equal(active.dataset.stock, "005930");
  assert.equal(active.getAttribute("aria-selected"), "true");
  assert.ok(active.id, "활성 옵션에는 id 가 부여돼야 함");
  assert.equal(w.searchInput.getAttribute("aria-activedescendant"), active.id);

  // 두 번째 항목으로 이동하면 activedescendant 도 따라간다.
  w.searchInput.dispatchEvent(new w.KeyboardEvent("keydown", { key: "ArrowDown" }));
  const second = w.dropdown.querySelector('.dropdown-item[data-stock="000660"]');
  assert.equal(second.getAttribute("aria-selected"), "true");
  assert.equal(w.searchInput.getAttribute("aria-activedescendant"), second.id);
});

test("Escape 는 드롭다운을 닫고 aria-expanded/aria-activedescendant 를 정리한다", async () => {
  const { w } = buildDom({ results: RESULTS });
  await w.doSearch("반도체");
  w.searchInput.dispatchEvent(new w.KeyboardEvent("keydown", { key: "ArrowDown" }));
  assert.ok(w.searchInput.getAttribute("aria-activedescendant"));

  w.searchInput.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Escape" }));
  assert.equal(w.dropdown.classList.contains("show"), false);
  assert.equal(w.searchInput.getAttribute("aria-expanded"), "false");
  assert.equal(w.searchInput.getAttribute("aria-activedescendant"), null);
});

test("바깥 클릭도 aria-expanded=false 로 동기화된다", async () => {
  const { w } = buildDom({ results: RESULTS });
  await w.doSearch("반도체");
  assert.equal(w.searchInput.getAttribute("aria-expanded"), "true");

  w.document.body.dispatchEvent(new w.MouseEvent("click", { bubbles: true }));
  assert.equal(w.dropdown.classList.contains("show"), false);
  assert.equal(w.searchInput.getAttribute("aria-expanded"), "false");
});

test("옵션 클릭은 종목 분석을 실행하고 combobox 를 닫는다", async () => {
  const { w, analyzeCalls } = buildDom({ results: RESULTS });
  await w.doSearch("반도체");

  w.dropdown.querySelector('[data-stock="005930"]').click();
  assert.deepEqual(analyzeCalls, ["005930"]);
  assert.equal(w.searchInput.getAttribute("aria-expanded"), "false");
  assert.equal(w.searchInput.value, "삼성전자");
});
