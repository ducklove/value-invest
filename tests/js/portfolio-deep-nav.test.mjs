// jsdom behavior test for the 심층 분석 점프 내비 (static/js/portfolio-performance.js pfInitDeepNav).
//
// index.html 과 같은 구조(묶음 캡슐 + 카드 링크)를 놓고, 카드의 화면 위치만 바꿔 가며
// 보고 있는 묶음·카드 표시가 따라오는지 본다. jsdom 은 레이아웃이 없어 위치는 직접 준다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const source = readFileSync(join(__dirname, "..", "..", "static", "js", "portfolio-performance.js"), "utf8");

const HTML = `<!doctype html><html><body><div id="portfolioView">
  <button class="pf-tab" data-tab="holdings"></button><button class="pf-tab" data-tab="performance"></button>
  <div id="pfHoldingsTab"></div>
  <div id="pfPerformanceTab">
    <nav class="pf-deep-anchors">
      <div class="pf-deep-nav-group" data-section="secA">
        <a class="pf-deep-nav-section" href="#secA">성과</a>
        <a class="pf-deep-anchor-link" href="#cardA1">NAV</a>
        <a class="pf-deep-anchor-link" href="#cardA2">리스크</a>
      </div>
      <div class="pf-deep-nav-group" data-section="secB">
        <a class="pf-deep-nav-section" href="#secB">구성·위험</a>
        <a class="pf-deep-anchor-link" href="#cardB1">X-Ray</a>
      </div>
    </nav>
    <section id="secA"><div id="cardA1"></div><div id="cardA2"></div></section>
    <section id="secB"><div id="cardB1"></div></section>
  </div>
</div></body></html>`;

function setup() {
  const dom = new JSDOM(HTML, { runScripts: "dangerously", pretendToBeVisual: true, url: "https://app.example.com/portfolio" });
  const { window: w } = dom;
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
  w.loadPerformanceData = () => {};
  w._loadAiModels = () => {};
  const tops = { secA: 60, cardA1: 80, cardA2: 600, secB: 1500, cardB1: 1540 };
  const rect = (top) => ({ top, bottom: top + 40, left: 0, right: 0, width: 0, height: 40 });
  Object.keys(tops).forEach((id) => { w.document.getElementById(id).getBoundingClientRect = () => rect(tops[id]); });
  w.document.querySelector(".pf-deep-anchors").getBoundingClientRect = () => rect(0); // 바 아래쪽 = 40
  const frame = () => new Promise((resolve) => w.requestAnimationFrame(() => resolve()));
  const scrollTo = async (offset) => {
    Object.keys(tops).forEach((id) => { tops[id] -= offset; });
    w.document.dispatchEvent(new w.Event("scroll"));
    await frame();
  };
  const active = () => ({
    group: w.document.querySelector(".pf-deep-nav-group.is-active")?.dataset.section,
    link: w.document.querySelector(".pf-deep-anchor-link.is-active")?.getAttribute("href"),
    count: w.document.querySelectorAll(".pf-deep-anchor-link.is-active").length,
  });
  return { w, tops, scrollTo, frame, active };
}

test("탭을 열면 첫 묶음과 첫 카드가 현재로 표시되고, 스크롤하면 따라온다", async () => {
  const { w, scrollTo, active } = setup();
  const nav = w.document.querySelector(".pf-deep-anchors");
  assert.ok(!nav.classList.contains("is-spy")); // 스크립트가 돌기 전에는 전부 펼친 상태다.

  w.pfSwitchTab("performance");
  assert.ok(nav.classList.contains("is-spy"));
  assert.deepEqual(active(), { group: "secA", link: "#cardA1", count: 1 });

  await scrollTo(560); // 리스크 카드가 바 바로 아래(40 ≤ 64)로 올라온다.
  assert.deepEqual(active(), { group: "secA", link: "#cardA2", count: 1 });
  assert.equal(w.document.querySelector('[href="#cardA2"]').getAttribute("aria-current"), "true");
  assert.equal(w.document.querySelector('[href="#cardA1"]').getAttribute("aria-current"), null);

  await scrollTo(940); // 다음 묶음의 첫 카드.
  assert.deepEqual(active(), { group: "secB", link: "#cardB1", count: 1 });

  await scrollTo(-1500);
  assert.deepEqual(active(), { group: "secA", link: "#cardA1", count: 1 });
});

test("페이지 끝이라 기준선까지 못 올라와도 누른 묶음이 펼쳐지고, 지나가면 위치로 돌아온다", async () => {
  const { w, scrollTo, frame, active } = setup();
  w.pfSwitchTab("performance");

  // 묶음을 눌렀지만 조금만 내려간다: 대상은 화면 안(1500-900=600)인데 기준선(64) 아래다.
  w.document.querySelector('.pf-deep-nav-section[href="#secB"]').dispatchEvent(new w.MouseEvent("click", { bubbles: true }));
  await scrollTo(900);
  assert.deepEqual(active(), { group: "secB", link: "#cardB1", count: 1 });

  // 대상이 화면 밖으로 나가면 고정이 풀리고, 다시 들어와도 위치만 본다.
  await scrollTo(-900);
  assert.deepEqual(active(), { group: "secA", link: "#cardA1", count: 1 });
  await scrollTo(900);
  await frame();
  assert.deepEqual(active(), { group: "secA", link: "#cardA2", count: 1 });
});

test("다른 탭에서는 표시를 건드리지 않는다", async () => {
  const { w, scrollTo, active } = setup();
  w.pfSwitchTab("performance");
  w.pfSwitchTab("holdings");
  await scrollTo(1500);
  assert.deepEqual(active(), { group: "secA", link: "#cardA1", count: 1 });
});
