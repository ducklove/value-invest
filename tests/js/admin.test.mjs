// jsdom behavior tests for the admin console (admin.js + admin-charts.js +
// admin-observability.js + admin-linked-projects.js):
// 부트 부분 실패 허용(패널별 인라인 오류 + 나머지 정상), 전체 실패 문구,
// triggerJob 오류 보고(reportApiError 토스트), 테마 키 'theme' 합류(구키
// 마이그레이션), 관리형 다이얼로그(adminConfirm/adminPromptDate), 죽은 코드
// (_renderDeployCard/_renderServerCard) 부재를 실제 스크립트로 검증한다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

// admin.html 의 스크립트 계약 순서 그대로 로드한다.
const SOURCES = [
  read("static", "js", "utils.js"),
  read("static", "js", "admin.js"),
  read("static", "js", "admin-charts.js"),
  read("static", "js", "admin-observability.js"),
  read("static", "js", "admin-linked-projects.js"),
];

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function buildDom({ prepare } = {}) {
  const dom = new JSDOM(`<!doctype html>
<html lang="ko" data-theme="light"><body>
  <div id="adminContent"></div>
</body></html>`, {
    runScripts: "dangerously",
    url: "https://admin.example.com/admin.html",
  });
  const { window: w } = dom;
  w.requestAnimationFrame = (cb) => cb();
  if (prepare) prepare(w); // 스크립트 로드 전 상태 주입(localStorage 등)
  for (const src of SOURCES) appendScript(w, src);
  return w;
}

// _startLiveUpdates 의 5초 인터벌 등 남은 타이머가 테스트 러너를 붙잡지
// 않도록 각 테스트가 끝나면 window.close() 로 정리한다. 닫기 전에 한 박자
// 기다려서 loadAdminView 가 fire-and-forget 으로 띄운 배당 목록 로드 등
// 진행 중인 마이크로태스크 체인이 window 해체 후 document 를 만지지 않게 한다.
async function withDom(fn, opts) {
  const w = buildDom(opts);
  try {
    return await fn(w);
  } finally {
    await new Promise((resolve) => setTimeout(resolve, 20));
    w.close();
  }
}

function fixtures() {
  return {
    "/api/admin/deploy-status": {
      build: { short_sha: "abc1234", sha: "abc1234def5678", subject: "테스트 커밋", committed_at: "2026-07-11T09:00:00+09:00" },
      service_started: "2026-07-11T09:10:00+09:00",
      actions_runner: { active: true, name: "runner-1" },
    },
    "/api/admin/batch-status": [{
      name: "market_data", label: "시장 데이터 수집", schedule: "매일 08:00",
      status: "success", last_start: "2026-07-11 08:00:00", next_run: "2026-07-12 08:00:00",
      staleness: { level: "ok", note: "2026-07-11" }, slo: { level: "ok", note: "" },
    }],
    "/api/admin/server-stats": {
      uptime: "3 days", cpu_temp: 51.2, cpu_pct: 17, load_avg: "0.42 0.35 0.31",
      memory: { MemTotal: 8000000, MemAvailable: 4000000 },
      disk: { total: 100000000000, used: 42000000000 },
    },
    "/api/admin/db-stats": { db_size_bytes: 1048576, tables: { user_portfolio: 12, system_events: 340 } },
    "/api/admin/users": [{
      google_sub: "sub-1", name: "홍길동", email: "hong@example.com", is_admin: false,
      portfolio_count: 3, picture: "", email_verified: true,
      last_login_at: "2026-07-10T12:00:00", created_at: "2026-01-01T00:00:00",
    }],
    "/api/admin/event-summary": { by_source: {}, latest: {}, data_quality: null },
    "/api/admin/events": [],
    "/api/admin/http-metrics": { hours: 24, endpoints: [] },
    "/api/admin/timeseries": { hours: 24, events: [], http: [] },
    "/api/admin/linked-project-configs": [],
    "/api/admin/ai-config": { openrouter: { configured: false }, features: [], usage: { by_feature: [], days: 30 } },
    "/api/admin/preferred-dividends": [],
    "/api/admin/foreign-dividends": [],
  };
}

// 실제 apiFetchJson(fallback 분기 포함)을 그대로 태우기 위해 fetch 만 모킹한다.
function installFetch(w, { fail = [], reject = false } = {}) {
  const calls = [];
  const data = fixtures();
  w.fetch = async (url, init = {}) => {
    const path = String(url).split("?")[0];
    calls.push({ path, init });
    if (reject) throw new TypeError("Failed to fetch");
    if (fail.includes(path)) {
      return { ok: false, status: 500, statusText: "Internal Server Error", json: async () => ({ detail: "서버 오류" }) };
    }
    if (path in data) {
      return { ok: true, status: 200, json: async () => structuredClone(data[path]) };
    }
    if (path.startsWith("/api/admin/trigger/")) {
      return { ok: true, status: 200, json: async () => ({ ok: true, message: "실행 시작" }) };
    }
    return { ok: false, status: 404, statusText: "Not Found", json: async () => ({ detail: `missing ${path}` }) };
  };
  return calls;
}

function toastTexts(w) {
  return [...w.document.querySelectorAll("#toastContainer div")].map(el => el.textContent);
}

// (a) 부트 부분 실패 — 실패한 패널만 인라인 오류, 나머지는 정상 렌더.
test("부트: batch-status 실패 시 배치 패널만 오류 문구, 나머지 패널은 정상", async () => {
  await withDom(async (w) => {
    installFetch(w, { fail: ["/api/admin/batch-status"] });
    await w.loadAdminView();

    const container = w.document.getElementById("adminContent");
    assert.match(container.textContent, /배치 상태를 불러오지 못했습니다/);
    assert.doesNotMatch(container.textContent, /어드민 데이터를 불러오지 못했습니다/);
    // 다른 패널은 정상: 사용자 목록·DB·이벤트 섹션이 그대로 렌더된다.
    assert.match(w.document.getElementById("adminUsersBody").textContent, /홍길동/);
    assert.ok(w.document.getElementById("dbStatsSection"));
    assert.ok(w.document.getElementById("eventsSection"));
    assert.match(container.textContent, /abc1234/); // 배포 KPI 도 정상
  });
});

test("부트: server/db/users 동시 실패도 각 패널 인라인 오류 + KPI 는 '-' 처리", async () => {
  await withDom(async (w) => {
    installFetch(w, { fail: ["/api/admin/server-stats", "/api/admin/db-stats", "/api/admin/users"] });
    await w.loadAdminView();

    const text = w.document.getElementById("adminContent").textContent;
    assert.match(text, /사용자 목록을 불러오지 못했습니다/);
    assert.match(text, /DB 상태를 불러오지 못했습니다/);
    assert.match(text, /서버 상태 조회 실패/);
    assert.match(text, /사용자 조회 실패/);
    assert.match(text, /DB 상태 조회 실패/);
    // 서버 차트는 0% 가짜 샘플 대신 빈 상태를 보여준다.
    assert.match(text, /서버 샘플 대기 중/);
    // 배치 패널은 정상.
    assert.match(text, /시장 데이터 수집/);
    assert.doesNotMatch(text, /어드민 데이터를 불러오지 못했습니다/);
  });
});

// (b) 전체 실패(네트워크 단절) — 기존 전체 대체 문구 유지.
test("부트: 전체 실패(네트워크)면 페이지 전체 오류 문구로 대체된다", async () => {
  await withDom(async (w) => {
    installFetch(w, { reject: true });
    await w.loadAdminView();
    assert.match(
      w.document.getElementById("adminContent").textContent,
      /어드민 데이터를 불러오지 못했습니다/,
    );
  });
});

// (c) triggerJob 실패 — reportApiError 경유 토스트.
test("triggerJob: 실패는 reportApiError 토스트('실행 요청 실패: ...')로 보고한다", async () => {
  await withDom(async (w) => {
    installFetch(w, { fail: ["/api/admin/trigger/market_data"] });
    await w.triggerJob("market_data");
    assert.deepEqual(toastTexts(w), ["실행 요청 실패: 서버 오류"]);
  });
});

test("triggerJobWithDate: adminPromptDate 값으로 date 를 보내고, 형식 오류는 요청 없이 경고한다", async () => {
  await withDom(async (w) => {
    const calls = installFetch(w);
    w.adminPromptDate = async () => "2026-07-01";
    await w.triggerJobWithDate("market_data");
    const call = calls.find(c => c.path === "/api/admin/trigger/market_data");
    assert.ok(call, "trigger should be called");
    assert.equal(JSON.parse(call.init.body).date, "2026-07-01");
    assert.deepEqual(toastTexts(w), ["실행 시작 (2026-07-01)"]);

    // 잘못된 형식이면 요청 자체를 보내지 않는다.
    w.adminPromptDate = async () => "07/01/2026";
    await w.triggerJobWithDate("market_data");
    assert.equal(calls.filter(c => c.path === "/api/admin/trigger/market_data").length, 1);
    assert.ok(toastTexts(w).includes("올바른 날짜 형식이 아닙니다."));
  });
});

// (d) 테마 — 생태계 공통 'theme' 키 + 구키 마이그레이션.
test("테마: 구키(valueInvestAdminTheme)는 'theme' 으로 이전되고 토글은 'theme' 에 저장한다", async () => {
  await withDom(async (w) => {
    assert.equal(w.document.documentElement.getAttribute("data-theme"), "dark");
    assert.equal(w.localStorage.getItem("theme"), "dark");
    assert.equal(w.localStorage.getItem("valueInvestAdminTheme"), null);

    w.toggleAdminTheme();
    assert.equal(w.document.documentElement.getAttribute("data-theme"), "light");
    assert.equal(w.localStorage.getItem("theme"), "light");
    assert.equal(w.localStorage.getItem("valueInvestAdminTheme"), null);
  }, { prepare: (w) => w.localStorage.setItem("valueInvestAdminTheme", "dark") });
});

test("테마: 'theme' 키가 있으면 구키보다 우선하고 구키는 제거된다", async () => {
  await withDom(async (w) => {
    assert.equal(w.document.documentElement.getAttribute("data-theme"), "dark");
    assert.equal(w.localStorage.getItem("theme"), "dark");
    assert.equal(w.localStorage.getItem("valueInvestAdminTheme"), null);
  }, {
    prepare: (w) => {
      w.localStorage.setItem("theme", "dark");
      w.localStorage.setItem("valueInvestAdminTheme", "light");
    },
  });
});

// (e) adminConfirm / adminPromptDate — 관리형 모달 동작.
test("adminConfirm: 확인 → true, 취소 → false, 모달 DOM 은 닫히며 제거된다", async () => {
  await withDom(async (w) => {
    const p1 = w.adminConfirm("삭제할까요?\n되돌릴 수 없습니다.");
    const overlay = w.document.querySelector(".admin-dialog-overlay");
    assert.ok(overlay, "overlay should be created");
    assert.equal(overlay.style.display, "flex"); // openManagedModal 이 켠다
    assert.match(overlay.textContent, /삭제할까요\?/);
    overlay.querySelector("[data-admin-dialog-confirm]").click();
    assert.equal(await p1, true);
    assert.equal(w.document.querySelector(".admin-dialog-overlay"), null);

    const p2 = w.adminConfirm("정말요?");
    w.document.querySelector("[data-admin-dialog-cancel]").click();
    assert.equal(await p2, false);
    assert.equal(w.document.querySelector(".admin-dialog-overlay"), null);

    // Escape 는 utils 의 관리형 모달 스택이 처리 — 취소로 닫힌다.
    const p3 = w.adminConfirm("Escape?");
    w.document.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
    assert.equal(await p3, false);
    assert.equal(w.document.querySelector(".admin-dialog-overlay"), null);
  });
});

test("adminPromptDate: 네이티브 date 입력 + 오늘 기본값, 확인 → 값, 취소 → null", async () => {
  await withDom(async (w) => {
    const p1 = w.adminPromptDate("실행할 날짜를 입력하세요 (YYYY-MM-DD)");
    const input = w.document.querySelector(".admin-dialog-overlay input");
    assert.ok(input, "date input should render");
    // 자유 텍스트가 아닌 <input type="date"> — 값 형식(YYYY-MM-DD)은
    // 브라우저 date picker 가 보장한다. 기본값은 오늘 날짜.
    assert.equal(input.type, "date");
    assert.match(input.value, /^\d{4}-\d{2}-\d{2}$/);
    input.value = "2026-07-01";
    w.document.querySelector("[data-admin-dialog-confirm]").click();
    assert.equal(await p1, "2026-07-01");

    // defaultValue 를 주면 오늘 대신 그 날짜가 초기값이 된다.
    const p2 = w.adminPromptDate("날짜?", "2026-01-15");
    const input2 = w.document.querySelector(".admin-dialog-overlay input");
    assert.equal(input2.value, "2026-01-15");
    w.document.querySelector("[data-admin-dialog-cancel]").click();
    assert.equal(await p2, null);
    assert.equal(w.document.querySelector(".admin-dialog-overlay"), null);
  });
});

// (f) 죽은 코드 부재 — 소스 문자열 단언은 구조 테스트
// (tests/test_frontend_structure_admin_labs.py)가 맡고, 여기서는 런타임
// 전역에 다시 붙지 않았는지 확인한다.
test("죽은 코드: _renderDeployCard/_renderServerCard 는 정의되지 않는다", async () => {
  await withDom(async (w) => {
    assert.equal(typeof w._renderDeployCard, "undefined");
    assert.equal(typeof w._renderServerCard, "undefined");
    // 대체 화면(운영 콘솔 KPI)은 존재한다.
    assert.equal(typeof w._renderOperationsOverview, "function");
  });
});
