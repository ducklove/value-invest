// jsdom behavior test for the NPS iframe reload policy (UX review P3): reloading
// the iframe on every tab revisit reset its scroll position every time. Now it
// loads once per session and only reloads on an explicit refresh (force) or a
// theme change.

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

function buildDom() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="npsContent" class="nps-embed" role="status" aria-live="polite"></div>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/nps",
  });
  const { window: w } = dom;
  w.matchMedia = () => ({ matches: false, addListener() {}, removeListener() {}, addEventListener() {}, removeEventListener() {} });
  appendScript(w, STORE_SRC);
  appendScript(w, SHELL_SRC);
  return w;
}

test("첫 진입은 iframe을 만든다", () => {
  const w = buildDom();
  w.loadNpsView();
  const frame = w.document.querySelector("#npsContent iframe.nps-frame");
  assert.ok(frame, "iframe should be created on first load");
});

test("force 없이 다시 열면 기존 iframe을 재사용한다 (스크롤 보존)", () => {
  const w = buildDom();
  w.loadNpsView();
  const first = w.document.querySelector("#npsContent iframe.nps-frame");
  const firstSrc = first.src;

  w.loadNpsView(); // 탭 재방문 시뮬레이션 — 인자 없음

  const second = w.document.querySelector("#npsContent iframe.nps-frame");
  assert.equal(second, first, "the same iframe element should be reused, not replaced");
  assert.equal(second.src, firstSrc, "src should not change without force");
});

test("force: true 로 열면 iframe을 새로 받아온다", () => {
  const w = buildDom();
  w.loadNpsView();
  const first = w.document.querySelector("#npsContent iframe.nps-frame");

  w.loadNpsView({ force: true });

  const second = w.document.querySelector("#npsContent iframe.nps-frame");
  assert.notEqual(second, first, "force should replace the iframe element");
});

test("npsContent가 없으면 조용히 아무 것도 하지 않는다", () => {
  const w = buildDom();
  w.document.getElementById("npsContent").remove();
  assert.doesNotThrow(() => w.loadNpsView());
});

test("테마 전환: vc:ready 를 보낸 nps-tracker 에는 postMessage, 아니면 기존처럼 src 를 갱신한다", () => {
  const w = buildDom();
  w.APP_CONFIG = { ecosystem: { version: 1, hub: "https://ducklove.duckdns.org:3691", categories: [], tools: [
    { id: "nps-tracker", url: "https://ducklove.github.io/nps-tracker", deploy: "github-pages", themeParam: true },
  ] } };
  appendScript(w, read("static", "js", "utils.js"));
  appendScript(w, read("static", "js", "ecosystem-links.js"));
  w.loadNpsView();
  const frame = w.document.querySelector("#npsContent iframe.nps-frame");
  assert.equal(frame.dataset.vcTool, "nps-tracker");

  // Not ready → reload with the new theme (legacy behaviour).
  w.document.documentElement.setAttribute("data-theme", "dark");
  w.syncNpsFrameTheme();
  assert.match(frame.src, /theme=dark/);

  const posted = [];
  frame.contentWindow.postMessage = (message) => posted.push(JSON.parse(JSON.stringify(message)));
  w.dispatchEvent(new w.MessageEvent("message", {
    data: { source: "vc", type: "vc:ready" }, origin: "https://ducklove.github.io", source: frame.contentWindow,
  }));
  const src = frame.src;
  w.document.documentElement.setAttribute("data-theme", "light");
  w.syncNpsFrameTheme();
  assert.equal(frame.src, src, "ready child keeps its document (and scroll position)");
  assert.deepEqual(posted.at(-1), { source: "vc", type: "vc:theme", theme: "light" });
});
