// jsdom behavior tests for the unified fetch error layer in static/js/utils.js:
// apiFetch 의 기본 타임아웃(AbortController) / 스트리밍 제외, 그리고
// reportApiError 의 토스트 vs silent 경로를 실제 스크립트로 검증한다.
// Run with `npm test` (node --test).

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const UTILS_SRC = readFileSync(
  join(__dirname, "..", "..", "static", "js", "utils.js"),
  "utf8",
);

// Load utils.js as a real <script> in a fresh jsdom window so its top-level
// function declarations attach to that window, exactly like the browser.
function loadUtils() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window } = dom;
  // showToast uses requestAnimationFrame; jsdom only provides it with
  // pretendToBeVisual, so give it a synchronous stand-in.
  window.requestAnimationFrame = (cb) => cb();
  const script = window.document.createElement("script");
  script.textContent = UTILS_SRC;
  window.document.body.appendChild(script);
  return window;
}

// fetch stub that records its init and never settles on its own — it only
// rejects with an AbortError when the wired signal fires, like real fetch.
function installRecordingFetch(w) {
  const calls = [];
  w.fetch = (url, init = {}) => {
    const call = { url, init };
    calls.push(call);
    return new Promise((resolve, reject) => {
      call.resolve = resolve;
      if (init.signal) {
        init.signal.addEventListener("abort", () => {
          reject(init.signal.reason || Object.assign(new Error("The operation was aborted."), { name: "AbortError" }));
        });
      }
    });
  };
  return calls;
}

function toastTexts(w) {
  return [...w.document.querySelectorAll("#toastContainer div")].map(
    (el) => el.textContent,
  );
}

test("apiFetch aborts with TimeoutError after the timeout", async () => {
  const w = loadUtils();
  const calls = installRecordingFetch(w);
  await assert.rejects(
    w.apiFetch("/api/portfolio", { timeoutMs: 20 }),
    (err) => err.name === "TimeoutError",
  );
  assert.equal(calls.length, 1);
  assert.ok(calls[0].init.signal, "a timeout signal must be attached");
  assert.equal(calls[0].init.signal.aborted, true);
  assert.equal(calls[0].init.signal.reason.name, "TimeoutError");
});

test("apiFetch resolves normally before the timeout and clears the timer", async () => {
  const w = loadUtils();
  const calls = installRecordingFetch(w);
  const promise = w.apiFetch("/api/portfolio", { timeoutMs: 5000 });
  calls[0].resolve({ ok: true });
  const resp = await promise;
  assert.equal(resp.ok, true);
  assert.equal(calls[0].init.signal.aborted, false);
});

test("apiFetch skips the timeout for stream: true (SSE) requests", async () => {
  const w = loadUtils();
  const calls = installRecordingFetch(w);
  // 기본 타임아웃보다 훨씬 짧게 기다려볼 수 있도록 즉시 검증한다.
  void w.apiFetch("/api/portfolio/ai-analysis", { method: "POST", stream: true });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].init.signal, undefined, "stream requests must not get a timeout signal");
  // stream/timeoutMs 는 fetch init 으로 새어 나가면 안 된다.
  assert.ok(!("stream" in calls[0].init));
  assert.ok(!("timeoutMs" in calls[0].init));
});

test("apiFetch leaves a caller-provided signal alone (no timeout layered on top)", async () => {
  const w = loadUtils();
  const calls = installRecordingFetch(w);
  const controller = new w.AbortController();
  void w.apiFetch("/api/analyze/005930", { signal: controller.signal });
  assert.equal(calls[0].init.signal, controller.signal);
});

test("apiFetch timeoutMs: 0 disables the timeout", async () => {
  const w = loadUtils();
  const calls = installRecordingFetch(w);
  void w.apiFetch("/api/portfolio/bulk", { timeoutMs: 0 });
  assert.equal(calls[0].init.signal, undefined);
});

test("reportApiError shows a Korean '<context> 실패: ...' toast for user actions", () => {
  const w = loadUtils();
  w.reportApiError(new Error("재고가 없습니다"), "저장");
  assert.deepEqual(toastTexts(w), ["저장 실패: 재고가 없습니다"]);
});

test("reportApiError maps timeout aborts to a friendly Korean message", () => {
  const w = loadUtils();
  const abortErr = Object.assign(new Error("The operation was aborted."), { name: "AbortError" });
  w.reportApiError(abortErr, "삭제");
  assert.deepEqual(toastTexts(w), ["삭제 실패: 요청 시간이 초과되었습니다."]);
});

test("reportApiError with silent: true logs but never toasts", () => {
  const w = loadUtils();
  const warnings = [];
  w.console.warn = (...args) => warnings.push(args);
  w.reportApiError(new Error("boom"), "벤치마크 시세", { silent: true });
  assert.equal(w.document.getElementById("toastContainer"), null);
  assert.equal(warnings.length, 1);
  assert.match(String(warnings[0][0]), /벤치마크 시세 실패/);
});
