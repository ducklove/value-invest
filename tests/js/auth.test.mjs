// jsdom behavior tests for the auth-state resilience in static/js/auth.js:
// loadCurrentUser/loadAuthConfig 가 일시적 네트워크 장애(fetch reject, 5xx)를
// "비로그인/미설정"으로 오판하지 않고 직전 상태를 유지하는지 검증한다.
// 실제 비인증 판정(200 + user:null, 4xx)은 여전히 로그아웃으로 처리해야 한다.
// Run with `npm test` (node --test).

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const STATIC_JS = join(__dirname, "..", "..", "static", "js");
const UTILS_SRC = readFileSync(join(STATIC_JS, "utils.js"), "utf8");
const AUTH_SRC = readFileSync(join(STATIC_JS, "auth.js"), "utf8");

// utils.js(전역 상태 선언) → auth.js 순서로 실제 <script>처럼 로드한다.
function loadAuth() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window } = dom;
  window.requestAnimationFrame = (cb) => cb();
  for (const src of [UTILS_SRC, AUTH_SRC]) {
    const script = window.document.createElement("script");
    script.textContent = src;
    window.document.body.appendChild(script);
  }
  return window;
}

function jsonResponse(body, { ok = true, status = 200 } = {}) {
  return { ok, status, json: async () => body };
}

const USER = { email: "user@example.com", name: "사용자" };

async function loginVia(w, user = USER) {
  w.apiFetch = async () => jsonResponse({ user });
  const loaded = await w.loadCurrentUser();
  assert.deepEqual(loaded, user, "precondition: 로그인 상태로 시작해야 한다");
}

test("loadCurrentUser keeps the logged-in user when the request fails (network)", async () => {
  const w = loadAuth();
  await loginVia(w);
  w.apiFetch = async () => { throw new TypeError("Failed to fetch"); };
  assert.deepEqual(await w.loadCurrentUser(), USER);
});

test("loadCurrentUser keeps the logged-in user on a 5xx server error", async () => {
  const w = loadAuth();
  await loginVia(w);
  w.apiFetch = async () => jsonResponse({}, { ok: false, status: 502 });
  assert.deepEqual(await w.loadCurrentUser(), USER);
});

test("loadCurrentUser logs out on an explicit 200 + user:null verdict", async () => {
  const w = loadAuth();
  await loginVia(w);
  w.apiFetch = async () => jsonResponse({ user: null });
  assert.equal(await w.loadCurrentUser(), null);
});

test("loadCurrentUser logs out on an explicit 4xx verdict", async () => {
  const w = loadAuth();
  await loginVia(w);
  w.apiFetch = async () => jsonResponse({}, { ok: false, status: 401 });
  assert.equal(await w.loadCurrentUser(), null);
});

test("loadCurrentUser stays logged out when failures happen before any login", async () => {
  const w = loadAuth();
  w.apiFetch = async () => { throw new TypeError("Failed to fetch"); };
  assert.equal(await w.loadCurrentUser(), null);
});

test("loadAuthConfig keeps a previously loaded config on network failure", async () => {
  const w = loadAuth();
  w.apiFetch = async () => jsonResponse({ enabled: true, googleClientId: "cid" });
  assert.deepEqual(await w.loadAuthConfig(), { enabled: true, googleClientId: "cid" });
  w.apiFetch = async () => { throw new TypeError("Failed to fetch"); };
  assert.deepEqual(await w.loadAuthConfig(), { enabled: true, googleClientId: "cid" });
});

test("loadAuthConfig falls back to disabled when it never loaded", async () => {
  const w = loadAuth();
  w.apiFetch = async () => { throw new TypeError("Failed to fetch"); };
  const config = await w.loadAuthConfig();
  // fallback 객체는 jsdom realm에서 생성되어 prototype이 달라지므로 필드로 비교.
  assert.equal(config.enabled, false);
  assert.equal(config.googleClientId, "");
});
