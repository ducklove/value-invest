// jsdom behavior tests for admin-charts.js (관리자 콘솔 SVG 시계열 차트):
// 서버 샘플 → polyline 좌표 스케일, 빈 상태(부트 실패 포함), 시리즈 색의
// CSS 토큰(--accent-blue/--color-success/--color-warning/--color-danger)
// 소싱과 폴백, 이벤트 누적 막대·HTTP 오류 겹침 막대, 샘플 버퍼 한도(120),
// 테마 토글(toggleAdminTheme) 시 세 차트 재렌더로 색 갱신을 검증한다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

// admin.html 의 스크립트 계약 순서 그대로 로드한다(admin.test.mjs 와 동일).
const SOURCES = [
  read("static", "js", "utils.js"),
  read("static", "js", "admin.js"),
  read("static", "js", "admin-charts.js"),
  read("static", "js", "admin-observability.js"),
  read("static", "js", "admin-linked-projects.js"),
];

// admin.html <style> 의 차트 관련 토큰(라이트/다크) 사본 — jsdom 은
// getComputedStyle 로 커스텀 프로퍼티 캐스케이드를 해석한다.
const ADMIN_TOKEN_STYLE = `
  :root {
    --text-secondary: #64748b;
    --border: #dbe3ee;
    --primary: #2563eb;
    --accent-blue: #2563eb;
    --color-success: #059669;
    --color-warning: #d97706;
    --color-danger: #dc2626;
  }
  [data-theme="dark"] {
    --text-secondary: #94a3b8;
    --border: #243244;
    --primary: #60a5fa;
    --accent-blue: #60a5fa;
    --color-success: #34d399;
    --color-warning: #f59e0b;
    --color-danger: #f87171;
  }
`;

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function buildDom({ style = ADMIN_TOKEN_STYLE } = {}) {
  const dom = new JSDOM(`<!doctype html>
<html lang="ko" data-theme="light"><head><style>${style}</style></head><body>
  <div id="adminContent"></div>
</body></html>`, {
    runScripts: "dangerously",
    url: "https://admin.example.com/admin.html",
  });
  const { window: w } = dom;
  w.requestAnimationFrame = (cb) => cb();
  for (const src of SOURCES) appendScript(w, src);
  return w;
}

// _startLiveUpdates 의 5초 인터벌 등 남은 타이머가 러너를 붙잡지 않도록
// 테스트 종료 시 window.close() 로 정리한다(admin.test.mjs 와 동일 패턴).
async function withDom(fn, opts) {
  const w = buildDom(opts);
  try {
    return await fn(w);
  } finally {
    await new Promise((resolve) => setTimeout(resolve, 20));
    w.close();
  }
}

// 렌더 함수가 돌려준 HTML 문자열을 실제 DOM 으로 붙여 SVG 를 쿼리한다.
function mount(w, html) {
  const host = w.document.getElementById("adminContent");
  host.innerHTML = html;
  return host;
}

const TIMELINE_FIXTURE = {
  hours: 24,
  events: [
    { bucket: "2026-07-11T09:00:00", info: 2, warning: 1, error: 1, total: 4 },
    { bucket: "2026-07-11T10:00:00", info: 0, warning: 0, error: 2, total: 2 },
  ],
  http: [
    { bucket: "2026-07-11T09:00:00", count: 4, errors: 1, max_ms: 900 },
    { bucket: "2026-07-11T10:00:00", count: 2, errors: 0, max_ms: 120 },
  ],
};

// (a) 서버 샘플 시리즈 → polyline 3개(CPU/메모리/디스크) + 좌표 스케일.
test("서버 차트: 샘플 시리즈로 polyline 3개를 0~100% 스케일 좌표로 그린다", async () => {
  await withDom(async (w) => {
    const host = mount(w, w._renderServerChartSvg([
      { cpu: 0, memory: 50, disk: 100 },
      { cpu: 100, memory: 50, disk: 100 },
    ]));
    const lines = [...host.querySelectorAll("polyline")];
    assert.equal(lines.length, 3);
    // x: 44→540(전폭), y: 0%→176 / 50%→98 / 100%→20 (뷰박스 스케일 계약).
    assert.equal(lines[0].getAttribute("points"), "44.0,176.0 540.0,20.0"); // CPU 0→100
    assert.equal(lines[1].getAttribute("points"), "44.0,98.0 540.0,98.0"); // Memory 50
    assert.equal(lines[2].getAttribute("points"), "44.0,20.0 540.0,20.0"); // Disk 100
    // 범례는 마지막 샘플 값을 보여준다.
    assert.match(host.textContent, /CPU 100%/);
    assert.match(host.textContent, /Memory 50%/);
    assert.match(host.textContent, /Disk 100%/);
  });
});

// (b) server=null 부트(서버 상태 조회 실패) — 0% 가짜 샘플 대신 빈 상태.
test("빈 상태: server=null·빈 timeline 이면 세 차트 모두 빈 상태 문구를 그린다", async () => {
  await withDom(async (w) => {
    assert.equal(w._renderServerChartSvg([]).includes("<svg"), false);
    const host = mount(w, w._renderTimelineSection(null, { hours: 24, events: [], http: [] }));
    assert.match(host.querySelector("#adminServerChart").textContent, /서버 샘플 대기 중/);
    assert.match(host.querySelector("#adminEventChart").textContent, /이벤트 기록 없음/);
    assert.match(host.querySelector("#adminHttpChart").textContent, /느린 요청\/5xx 기록 없음/);
    assert.equal(host.querySelector("svg"), null);
  });
});

// (c) 시리즈 색은 하드코딩이 아니라 문서의 CSS 토큰에서 읽는다.
test("색 토큰: 시리즈 색은 --accent-blue/--color-success/--color-warning/--color-danger 에서 읽는다", async () => {
  const sentinel = `
    :root {
      --accent-blue: #101112;
      --color-success: #131415;
      --color-warning: #161718;
      --color-danger: #191a1b;
    }
  `;
  await withDom(async (w) => {
    const host = mount(w, w._renderServerChartSvg([{ cpu: 10, memory: 20, disk: 30 }]));
    const strokes = [...host.querySelectorAll("polyline")].map((el) => el.getAttribute("stroke"));
    assert.deepEqual(strokes, ["#101112", "#131415", "#161718"]);

    mount(w, w._renderEventChartSvg(TIMELINE_FIXTURE.events));
    const eventFills = [...host.querySelectorAll("rect")].map((el) => el.getAttribute("fill"));
    assert.ok(eventFills.includes("#131415"), "info → --color-success");
    assert.ok(eventFills.includes("#161718"), "warning → --color-warning");
    assert.ok(eventFills.includes("#191a1b"), "error → --color-danger");

    mount(w, w._renderHttpChartSvg(TIMELINE_FIXTURE.http));
    const httpFills = [...host.querySelectorAll("rect")].map((el) => el.getAttribute("fill"));
    assert.ok(httpFills.includes("#101112"), "전체 건수 → --accent-blue");
    assert.ok(httpFills.includes("#191a1b"), "오류 → --color-danger");
  }, { style: sentinel });
});

test("색 토큰: 토큰이 정의되지 않은 문서에서는 라이트 기본값으로 폴백한다", async () => {
  await withDom(async (w) => {
    const host = mount(w, w._renderServerChartSvg([{ cpu: 10, memory: 20, disk: 30 }]));
    const strokes = [...host.querySelectorAll("polyline")].map((el) => el.getAttribute("stroke"));
    assert.deepEqual(strokes, ["#2563eb", "#059669", "#d97706"]);
  }, { style: "" });
});

// (d) 이벤트 차트 — info/warning/error 누적 막대와 높이 스케일.
test("이벤트 차트: info/warning/error 를 한 막대에 누적하고 0건 구간은 rect 를 생략한다", async () => {
  await withDom(async (w) => {
    const host = mount(w, w._renderEventChartSvg(TIMELINE_FIXTURE.events));
    const rects = [...host.querySelectorAll("rect")];
    // 첫 막대(총 4건): info 2/warning 1/error 1 → 3개, 둘째 막대: error 2 만 1개.
    assert.equal(rects.length, 4);
    const byFill = (fill) => rects.filter((r) => r.getAttribute("fill") === fill);
    // max=4건 기준 높이: 2건=78px, 1건=39px — 누적이라 y 가 위로 쌓인다.
    const [info] = byFill("#059669");
    assert.equal(info.getAttribute("height"), "78.0");
    assert.equal(info.getAttribute("y"), "98.0");
    const [warning] = byFill("#d97706");
    assert.equal(warning.getAttribute("height"), "39.0");
    assert.equal(warning.getAttribute("y"), "59.0");
    const errors = byFill("#dc2626");
    assert.deepEqual(errors.map((r) => r.getAttribute("height")), ["39.0", "78.0"]);
    assert.match(host.textContent, /최대 4건\/h/);
  });
});

// (e) HTTP 차트 — 전체 건수 막대 위 오류 막대 겹침, 오류 0이면 생략.
test("HTTP 차트: 전체 막대 위에 오류 막대를 겹치고 오류 0 구간은 생략한다", async () => {
  await withDom(async (w) => {
    const host = mount(w, w._renderHttpChartSvg(TIMELINE_FIXTURE.http));
    const totals = [...host.querySelectorAll('rect[fill="#2563eb"]')];
    const errors = [...host.querySelectorAll('rect[fill="#dc2626"]')];
    assert.equal(totals.length, 2);
    assert.equal(errors.length, 1); // 둘째 구간은 errors=0 → 오류 rect 없음
    assert.deepEqual(totals.map((r) => r.getAttribute("height")), ["156.0", "78.0"]);
    assert.equal(errors[0].getAttribute("height"), "39.0");
    assert.match(host.textContent, /max 900ms/);
  });
});

// (f) 라이브 샘플 버퍼는 120개 한도로 오래된 샘플을 버린다.
test("서버 차트: 라이브 샘플은 120개 한도로 유지된다", async () => {
  await withDom(async (w) => {
    mount(w, '<div id="adminServerChart"></div>');
    const stats = {
      cpu_pct: 40,
      memory: { MemTotal: 100, MemAvailable: 60 },
      disk: { total: 100, used: 30 },
    };
    w._seedServerSeries(stats);
    for (let i = 0; i < 130; i += 1) w._pushServerSample(stats);
    w._renderServerTimeline();
    const points = w.document.querySelector("#adminServerChart polyline").getAttribute("points");
    assert.equal(points.split(" ").length, 120);
  });
});

// (g) 테마 토글 → 세 차트가 다크 토큰 색으로 다시 그려진다.
test("테마 토글: toggleAdminTheme 이 세 차트를 새 토큰 색으로 재렌더한다", async () => {
  await withDom(async (w) => {
    const stats = { cpu_pct: 25, memory: { MemTotal: 100, MemAvailable: 50 }, disk: { total: 100, used: 40 } };
    // loadAdminView 부트와 동일하게 서버 시리즈를 시딩해 두어야 토글 재렌더
    // (_renderAdminCharts → _renderServerTimeline)가 빈 상태로 떨어지지 않는다.
    w._seedServerSeries(stats);
    const host = mount(w, w._renderTimelineSection(stats, TIMELINE_FIXTURE));
    const cpuStroke = () => host.querySelector("#adminServerChart polyline").getAttribute("stroke");
    const eventFills = () => [...host.querySelectorAll("#adminEventChart rect")].map((r) => r.getAttribute("fill"));
    const httpFills = () => [...host.querySelectorAll("#adminHttpChart rect")].map((r) => r.getAttribute("fill"));
    assert.equal(cpuStroke(), "#2563eb"); // 라이트 --accent-blue

    w.toggleAdminTheme();
    assert.equal(w.document.documentElement.getAttribute("data-theme"), "dark");
    assert.equal(cpuStroke(), "#60a5fa"); // 다크 --accent-blue
    assert.ok(eventFills().includes("#34d399"), "info → 다크 --color-success");
    assert.ok(eventFills().includes("#f87171"), "error → 다크 --color-danger");
    assert.ok(httpFills().includes("#60a5fa"), "전체 건수 → 다크 --accent-blue");
    assert.ok(httpFills().includes("#f87171"), "HTTP 오류 → 다크 --color-danger");

    // 다시 토글하면 라이트 토큰 색으로 복귀한다.
    w.toggleAdminTheme();
    assert.equal(cpuStroke(), "#2563eb");
    assert.ok(eventFills().includes("#059669"));
  });
});
