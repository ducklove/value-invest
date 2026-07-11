// jsdom behavior tests for admin-observability.js 패널 렌더러:
// 배치 섹션(실행 상태·신선도 배지·SLO), 사용자 테이블 렌더 + 검색/역할 필터,
// 서브시스템 이벤트 요약(이상 징후 칩 포함), HTTP 메트릭 테이블(임계 색상),
// 이벤트 피드(이상 징후 레벨 증폭), 5초 라이브 샘플 push 갱신을
// admin.test.mjs 와 동일한 하니스(스크립트 계약 순서 로드 + fetch 모킹)로 검증한다.

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

function buildDom() {
  const dom = new JSDOM(`<!doctype html>
<html lang="ko" data-theme="light"><body>
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
async function withDom(fn) {
  const w = buildDom();
  try {
    return await fn(w);
  } finally {
    await new Promise((resolve) => setTimeout(resolve, 20));
    w.close();
  }
}

// 렌더 함수가 돌려준 HTML 문자열을 실제 DOM 으로 붙여 쿼리한다.
function mount(w, html) {
  const host = w.document.getElementById("adminContent");
  host.innerHTML = html;
  return host;
}

const USERS_FIXTURE = [
  {
    google_sub: "sub-1", name: "홍길동", email: "hong@example.com", is_admin: false,
    portfolio_count: 3, picture: "", email_verified: true,
    last_login_at: "2026-07-10T12:00:00", created_at: "2026-01-01T00:00:00",
  },
  {
    google_sub: "sub-2", name: "김관리", email: "admin@example.com", is_admin: true,
    portfolio_count: 0, picture: "", email_verified: true,
    last_login_at: "2026-07-11T08:00:00", created_at: "2026-02-01T00:00:00",
  },
  {
    google_sub: "sub-3", name: "Lee Dev", email: "lee@example.com", is_admin: false,
    portfolio_count: 1, picture: "", email_verified: false,
    last_login_at: null, created_at: "2026-03-05T00:00:00",
  },
];

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
    "/api/admin/users": USERS_FIXTURE,
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
// 반환된 data 객체를 테스트에서 직접 변형하면 다음 fetch 응답에 반영된다.
function installFetch(w) {
  const data = fixtures();
  w.fetch = async (url) => {
    const path = String(url).split("?")[0];
    if (path in data) {
      return { ok: true, status: 200, json: async () => structuredClone(data[path]) };
    }
    return { ok: false, status: 404, statusText: "Not Found", json: async () => ({ detail: `missing ${path}` }) };
  };
  return data;
}

// (a) 배치 섹션 — 실행 상태 아이콘, 최신 데이터(신선도) 배지, SLO 라벨.
test("배치 섹션: 신선도 ok/stale/missing 배지와 SLO 정상/주의/위반, running 은 실행 버튼 비활성", async () => {
  await withDom(async (w) => {
    const jobs = [
      {
        name: "market_data", label: "시장 데이터 수집", schedule: "매일 08:00",
        status: "success", last_start: "2026-07-11 08:00:12", next_run: "2026-07-12 08:00:00",
        staleness: { level: "ok", note: "2026-07-11" }, slo: { level: "ok", note: "" },
      },
      {
        name: "nps_sync", label: "NPS 동기화", schedule: "매주 월",
        status: "success", last_start: "2026-07-07 09:00:00", next_run: "2026-07-14 09:00:00",
        staleness: { level: "stale", note: "4일 지연" }, slo: { level: "watch", note: "지연 관찰" },
      },
      {
        name: "wiki_loop", label: "위키 인제션", schedule: "6시간마다",
        status: "running", last_start: "2026-07-11 12:00:00", next_run: null,
        staleness: { level: "missing", note: "데이터 없음" }, slo: { level: "breach", note: "24h 초과" },
      },
    ];
    const host = mount(w, w._renderBatchSection(jobs));
    const rows = [...host.querySelectorAll("tbody tr")];
    assert.equal(rows.length, 3);

    // ok — 데이터 셀은 초록 배지에 최신 날짜 노트.
    const okCell = rows[0].children[2].querySelector("span");
    assert.ok(okCell.classList.contains("admin-status-ok"));
    assert.equal(okCell.textContent, "2026-07-11");
    assert.match(rows[0].children[1].textContent, /✓ 성공/);
    assert.match(rows[0].children[3].textContent, /정상/);

    // stale — ⚠ + 노트가 위험 색으로.
    const staleCell = rows[1].children[2].querySelector("span");
    assert.ok(staleCell.classList.contains("admin-status-fail"));
    assert.equal(staleCell.textContent, "⚠ 4일 지연");
    assert.ok(rows[1].children[3].classList.contains("admin-status-run"));
    assert.match(rows[1].children[3].textContent, /주의/);

    // missing — ✗ + SLO 위반, running 작업은 실행 버튼 disabled.
    const missingCell = rows[2].children[2].querySelector("span");
    assert.ok(missingCell.classList.contains("admin-status-fail"));
    assert.equal(missingCell.textContent, "✗ 데이터 없음");
    assert.ok(rows[2].children[3].classList.contains("admin-status-fail"));
    assert.match(rows[2].children[3].textContent, /위반/);
    assert.match(rows[2].children[1].textContent, /실행 중/);
    const [runBtn] = rows[2].querySelectorAll("button");
    assert.equal(runBtn.disabled, true);
    const [okRunBtn] = rows[0].querySelectorAll("button");
    assert.equal(okRunBtn.disabled, false);
  });
});

// (b) 사용자 테이블 — 부트 렌더 + 검색어/역할 필터.
test("사용자 테이블: 부트 렌더 3명, 검색어·역할 필터가 행을 좁히고 무결과 문구를 낸다", async () => {
  await withDom(async (w) => {
    installFetch(w);
    await w.loadAdminView();

    const body = () => w.document.getElementById("adminUsersBody");
    assert.equal(body().querySelectorAll("tr").length, 3);
    assert.match(w.document.getElementById("adminUsersSection").textContent, /3명/);
    assert.match(body().textContent, /홍길동/);
    assert.match(body().textContent, /김관리/);

    // 검색어 필터 — 이메일 부분 일치(대소문자 무시).
    w.document.getElementById("adminUserSearch").value = "HONG@";
    w.filterAdminUsers();
    assert.equal(body().querySelectorAll("tr").length, 1);
    assert.match(body().textContent, /홍길동/);
    assert.doesNotMatch(body().textContent, /김관리/);

    // 역할 필터 — 관리자만.
    w.document.getElementById("adminUserSearch").value = "";
    w.document.getElementById("adminRoleFilter").value = "admin";
    w.filterAdminUsers();
    assert.equal(body().querySelectorAll("tr").length, 1);
    assert.match(body().textContent, /김관리/);

    // 무결과 — 안내 행으로 대체.
    w.document.getElementById("adminUserSearch").value = "zzz-없는사용자";
    w.filterAdminUsers();
    assert.match(body().textContent, /표시할 사용자가 없습니다/);
  });
});

// (c) 서브시스템 이벤트 요약 — 24h 카운트, 이상 징후 칩, 이벤트 없음 폴백.
test("이벤트 요약: 정상 ✓ / 이상 징후 ⚠+칩 / 오류 ✗ / 기록 없는 소스는 '이벤트 없음'", async () => {
  await withDom(async (w) => {
    const now = new Date().toISOString();
    const summary = {
      by_source: {
        snapshot_nav: { info: 5, warning: 0, error: 0 },
        wiki_ingestion: { info: 4, warning: 0, error: 0 },
        benchmark_history: { info: 1, warning: 0, error: 2 },
      },
      latest: {
        snapshot_nav: { kind: "snapshot", ts: now, details_obj: { users_failed: [] } },
        wiki_ingestion: {
          kind: "tick", ts: now,
          details_obj: { skipped_by_reason: { rejected_by_whitelist: 51 }, failed: 0 },
        },
        benchmark_history: { kind: "daily_update", ts: now, details_obj: {} },
      },
    };
    const host = mount(w, w._renderSubsystemSummary(summary));
    const cards = [...host.querySelectorAll(".admin-card")];
    assert.equal(cards.length, 4); // 알려진 서브시스템 4종 고정

    const byLabel = (label) => cards.find((c) => c.querySelector(".admin-card-label").textContent === label);

    const nav = byLabel("포트폴리오 NAV 스냅샷");
    assert.ok(nav.querySelector(".admin-card-value").classList.contains("admin-status-ok"));
    assert.match(nav.textContent, /✓/);
    assert.match(nav.textContent, /24h: info 5 · warn 0 · error 0/);

    // info 로 기록됐어도 화이트리스트 탈락 이상 징후가 ⚠ 로 승격 + 칩 노출.
    const wiki = byLabel("위키 인제션 루프");
    assert.ok(wiki.querySelector(".admin-card-value").classList.contains("admin-status-run"));
    assert.match(wiki.textContent, /⚠/);
    const chip = wiki.querySelector(".admin-anomaly-chips .admin-event-kv");
    assert.ok(chip.classList.contains("admin-status-fail"));
    assert.equal(chip.textContent, "화이트리스트 탈락 51건");

    const benchmark = byLabel("벤치마크 일별 증분");
    assert.ok(benchmark.querySelector(".admin-card-value").classList.contains("admin-status-fail"));
    assert.match(benchmark.textContent, /✗/);
    assert.match(benchmark.textContent, /error 2/);

    // 이벤트가 한 번도 없던 소스는 폴백 문구.
    assert.match(byLabel("장중 스냅샷").textContent, /이벤트 없음/);
  });
});

// (d) HTTP 메트릭 — 임계값 색상(에러>0, max_ms 1s/3s)과 빈 상태.
test("HTTP 메트릭: 경로별 행 렌더 + 에러/지연 임계 색상, 빈 목록은 안내 행", async () => {
  await withDom(async (w) => {
    const host = mount(w, w._renderHttpMetricsSection({
      hours: 6,
      endpoints: [
        { path: "/api/portfolio", count: 12, errors: 2, avg_ms: 850.4, max_ms: 3500, last_ts: new Date().toISOString() },
        { path: "/api/quotes", count: 5, errors: 0, avg_ms: 300, max_ms: 1500, last_ts: null },
      ],
    }));
    assert.match(host.textContent, /최근 6시간/);
    assert.match(host.textContent, /2개 경로/);
    const rows = [...host.querySelectorAll("tbody tr")];
    assert.equal(rows.length, 2);

    // 에러 2건 → 에러 셀 fail, max 3500ms(≥3s) → max 셀 fail.
    const [, count1, err1, avg1, max1, last1] = rows[0].children;
    assert.equal(count1.textContent, "12");
    assert.ok(err1.classList.contains("admin-status-fail"));
    assert.equal(avg1.textContent, "850"); // 반올림 표기
    assert.ok(max1.classList.contains("admin-status-fail"));
    assert.equal(max1.textContent, "3,500");
    assert.match(last1.textContent, /전/); // 상대 시각

    // max 1500ms(1s~3s) → 주의 색, 에러 0 → 색 없음, last_ts 없음 → '-'.
    const [, , err2, , max2, last2] = rows[1].children;
    assert.equal(err2.classList.contains("admin-status-fail"), false);
    assert.ok(max2.classList.contains("admin-status-run"));
    assert.equal(last2.textContent.trim(), "-");

    const empty = mount(w, w._renderHttpMetricsSection({ hours: 24, endpoints: [] }));
    assert.match(empty.textContent, /기록된 느린 요청\/에러 없음/);
  });
});

// (e) 이벤트 피드 — 이상 징후가 level 을 warning 으로 증폭, details kv 칩.
test("이벤트 피드: 이상 징후 있는 info 는 ⚠ warning 으로 표시되고 kv 칩이 강조된다", async () => {
  await withDom(async (w) => {
    const now = new Date().toISOString();
    const host = mount(w, w._renderEventsSection([
      {
        ts: now, level: "info", source: "wiki_ingestion", kind: "tick", stock_code: null,
        details_obj: { skipped_by_reason: { rejected_by_whitelist: 51 }, summarized: 3 },
      },
      { ts: now, level: "error", source: "http", kind: "5xx", stock_code: "005930", details_obj: { path: "/api/x" } },
    ]));
    const rows = [...host.querySelectorAll("tbody tr")];
    assert.equal(rows.length, 2);
    assert.match(host.textContent, /2건/);

    // writer 가 info 로 남겼어도 화이트리스트 탈락 이상 징후가 warning 으로 증폭.
    const levelCell = rows[0].children[1];
    assert.ok(levelCell.classList.contains("admin-status-run"));
    assert.match(levelCell.textContent, /⚠ warning/);
    const anomalyChip = [...rows[0].querySelectorAll(".admin-event-kv")]
      .find((el) => el.textContent.startsWith("skipped_by_reason="));
    assert.ok(anomalyChip.classList.contains("admin-status-fail"));

    // error 이벤트는 ✗ + 종목 코드 노출.
    assert.ok(rows[1].children[1].classList.contains("admin-status-fail"));
    assert.match(rows[1].children[1].textContent, /✗ error/);
    assert.equal(rows[1].querySelector("code").textContent, "005930");

    const empty = mount(w, w._renderEventsSection([]));
    assert.match(empty.textContent, /이벤트 없음/);
  });
});

// 폼 제출/클릭 핸들러의 fire-and-forget 비동기 렌더를 기다린다.
async function waitFor(check, { timeout = 2000 } = {}) {
  const start = Date.now();
  for (;;) {
    if (check()) return;
    if (Date.now() - start > timeout) throw new Error("waitFor timeout");
    await new Promise((r) => setTimeout(r, 5));
  }
}

// (g) 위키 진단 — 폼 제출이 /api/admin/diag/wiki 를 조회해 verdict·funnel
// 카드·PDF 캐시 칩·샘플/실패 표를 그리고, 빈 코드는 요청 없이 안내한다.
test("위키 진단 폼: 제출 → 진단 결과(verdict·funnel·칩·표) 렌더, 빈 코드는 요청 없이 안내", async () => {
  await withDom(async (w) => {
    const data = installFetch(w);
    data["/api/admin/diag/wiki"] = {
      stock_code: "051910",
      verdict: "화이트리스트 정상 — 신규 PDF 없음",
      naver: {
        total: 12, has_pdf: 9, passes_whitelist: 7,
        samples: [
          { date: "2026-07-10", firm: "미래에셋", title: "2Q 프리뷰", status: "ok" },
          { date: "2026-07-08", firm: "NH", title: "목표가 상향", status: "rejected" },
        ],
      },
      db: {
        wiki_entries: 5,
        pdf_cache_by_status: { parsed: 4, failed: 2 },
        recent_failures: [{ status: "failed", error: "HTTP 403 from host" }],
      },
    };
    // 진단 요청 URL(코드 쿼리) 검증용 호출 추적.
    const origFetch = w.fetch;
    const diagCalls = [];
    w.fetch = (url, init) => {
      if (String(url).startsWith("/api/admin/diag/wiki")) diagCalls.push(String(url));
      return origFetch(url, init);
    };
    const host = mount(w, w._renderDiagSection());

    w.document.getElementById("diagWikiCode").value = "051910";
    w.document.getElementById("diagWikiForm")
      .dispatchEvent(new w.Event("submit", { bubbles: true, cancelable: true }));
    await waitFor(() => host.querySelector(".admin-diag-verdict"));

    assert.deepEqual(diagCalls, ["/api/admin/diag/wiki?code=051910"]);
    const result = w.document.getElementById("diagWikiResult");
    assert.match(result.querySelector(".admin-diag-verdict").textContent, /051910/);
    assert.match(result.querySelector(".admin-diag-verdict").textContent, /화이트리스트 정상 — 신규 PDF 없음/);

    // funnel 카드 — Naver total → pdf_url → 화이트리스트 → DB 엔트리.
    const cardValue = (label) => [...result.querySelectorAll(".admin-card")]
      .find((c) => c.querySelector(".admin-card-label").textContent === label)
      .querySelector(".admin-card-value").textContent;
    assert.equal(cardValue("Naver total"), "12");
    assert.equal(cardValue("pdf_url 보유"), "9");
    assert.equal(cardValue("화이트리스트 통과"), "7");
    assert.equal(cardValue("DB 위키 엔트리"), "5");

    // PDF 캐시 칩 — parsed 는 정상색, 그 외 상태는 위험색.
    const chips = [...result.querySelectorAll(".admin-event-kv")];
    const parsedChip = chips.find((c) => c.textContent === "parsed: 4");
    const failedChip = chips.find((c) => c.textContent === "failed: 2");
    assert.ok(parsedChip.classList.contains("admin-status-ok"));
    assert.ok(failedChip.classList.contains("admin-status-fail"));

    // 샘플 표(상태 색상) + 최근 실패 표.
    const sampleRows = [...result.querySelectorAll("table")[0].querySelectorAll("tbody tr")];
    assert.equal(sampleRows.length, 2);
    assert.ok(sampleRows[0].children[3].classList.contains("admin-status-ok"));
    assert.ok(sampleRows[1].children[3].classList.contains("admin-status-fail"));
    assert.match(result.textContent, /HTTP 403 from host/);

    // 빈 코드 — 요청 없이 인라인 안내만.
    w.document.getElementById("diagWikiCode").value = "  ";
    w.document.getElementById("diagWikiForm")
      .dispatchEvent(new w.Event("submit", { bubbles: true, cancelable: true }));
    await waitFor(() => /종목 코드를 입력하세요/.test(result.textContent));
    assert.equal(diagCalls.length, 1); // 추가 요청 없음
  });
});

// (h) 포트폴리오 검색 — Enter 제출 → 결과 표 렌더, 빈 검색어/무결과 안내.
test("포트폴리오 검색: Enter 제출이 결과 표를 그리고, 빈 검색어는 요청 없이 안내, 무결과는 안내 행", async () => {
  await withDom(async (w) => {
    const data = installFetch(w);
    data["/api/admin/portfolio-search"] = {
      rows: [{
        stock_code: "005930", stock_name: "삼성전자", group_name: "코어",
        name: "홍길동", email: "hong@example.com",
        quantity: 1000, avg_price: 72500, currency: "KRW",
        portfolio_url: "https://192.168.68.67:3691/api/admin/users/sub-1/portfolio.html",
      }],
    };
    const origFetch = w.fetch;
    const searchCalls = [];
    w.fetch = (url, init) => {
      if (String(url).startsWith("/api/admin/portfolio-search")) searchCalls.push(String(url));
      return origFetch(url, init);
    };
    mount(w, w._renderUsersSection(USERS_FIXTURE));
    const result = () => w.document.getElementById("adminPortfolioSearchResult");

    // 빈 검색어 — 요청 없이 안내.
    await w.searchAdminPortfolios();
    assert.match(result().textContent, /검색어를 입력하세요/);
    assert.equal(searchCalls.length, 0);

    // Enter 제출(인라인 onkeydown 와이어링) → 결과 표.
    const input = w.document.getElementById("adminPortfolioSearch");
    input.value = "삼성";
    input.dispatchEvent(new w.KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));
    await waitFor(() => result().querySelector("table"));

    assert.equal(searchCalls.length, 1);
    assert.match(searchCalls[0], /q=%EC%82%BC%EC%84%B1/); // encodeURIComponent('삼성')
    assert.match(searchCalls[0], /limit=80/);
    assert.match(result().textContent, /포트폴리오 검색 "삼성" · 1건/);
    const row = result().querySelector("tbody tr");
    assert.match(row.children[0].textContent, /삼성전자/);
    assert.match(row.children[0].textContent, /코어/);
    assert.match(row.children[1].textContent, /홍길동/);
    assert.match(row.children[1].textContent, /hong@example\.com/);
    assert.equal(row.children[2].textContent, "1,000");
    assert.equal(row.children[3].textContent.trim(), "72,500 KRW");
    assert.equal(
      row.querySelector("a.admin-link-button").getAttribute("href"),
      "https://192.168.68.67:3691/api/admin/users/sub-1/portfolio.html",
    );

    // 무결과 — 안내 행으로 대체.
    data["/api/admin/portfolio-search"] = { rows: [] };
    input.value = "없는종목";
    await w.searchAdminPortfolios();
    assert.match(result().textContent, /검색 결과가 없습니다/);
  });
});

// (i) 모바일 카드 계약(M14) — 카드화 대상 3표(배치/사용자/HTTP)는
// admin-table-cards 클래스와 td data-label 을 갖고(≤720px CSS 가 카드로
// 재배치), 상세 프리뷰가 넓은 이벤트 피드는 가로 스크롤 표로 남는다.
test("모바일 카드 계약: 배치/사용자/HTTP 표는 admin-table-cards + data-label, 이벤트 피드는 스크롤 표 유지", async () => {
  await withDom(async (w) => {
    const jobs = [{
      name: "market_data", label: "시장 데이터 수집", schedule: "매일 08:00",
      status: "success", last_start: "2026-07-11 08:00:00", next_run: "2026-07-12 08:00:00",
      staleness: { level: "ok", note: "2026-07-11" }, slo: { level: "ok", note: "" },
    }];
    const batch = mount(w, w._renderBatchSection(jobs));
    const batchTable = batch.querySelector("table");
    assert.ok(batchTable.classList.contains("admin-table-cards"));
    const batchCells = batch.querySelector("tbody tr").children;
    assert.ok(batchCells[0].classList.contains("admin-cell-full")); // 작업명 — 라벨 없이 전체 폭
    assert.equal(batchCells[1].dataset.label, "실행 상태");
    assert.equal(batchCells[2].dataset.label, "최신 데이터");
    assert.equal(batchCells[3].dataset.label, "SLO");
    assert.ok(batchCells[6].classList.contains("admin-cell-full")); // 버튼 셀 — 전체 폭

    const users = mount(w, w._renderUsersSection(USERS_FIXTURE));
    assert.ok(users.querySelector("table").classList.contains("admin-table-cards"));
    const userCells = users.querySelector("#adminUsersBody tr").children;
    assert.ok(userCells[0].classList.contains("admin-cell-full"));
    assert.equal(userCells[1].dataset.label, "역할");
    assert.equal(userCells[3].dataset.label, "최근 로그인");

    const http = mount(w, w._renderHttpMetricsSection({
      hours: 24,
      endpoints: [{ path: "/api/x", count: 1, errors: 0, avg_ms: 10, max_ms: 20, last_ts: null }],
    }));
    assert.ok(http.querySelector("table").classList.contains("admin-table-cards"));
    const httpCells = http.querySelector("tbody tr").children;
    assert.ok(httpCells[0].classList.contains("admin-cell-full"));
    assert.equal(httpCells[1].dataset.label, "건수");
    assert.ok(httpCells[1].classList.contains("admin-num")); // 데스크톱 우측 정렬은 클래스로

    // 이벤트 피드는 카드화 대상이 아니다 — 상세 프리뷰 셀이 넓어 가로 스크롤 유지.
    const events = mount(w, w._renderEventsSection([
      { ts: new Date().toISOString(), level: "info", source: "http", kind: "slow", stock_code: null, details_obj: {} },
    ]));
    assert.equal(events.querySelector("table").classList.contains("admin-table-cards"), false);
  });
});

// (f) 라이브 샘플 push — 5초 갱신 루프의 1회분(_updateLiveStats)이 KPI 와
// 서버 차트를 새 샘플로 갱신한다.
test("라이브 샘플: _updateLiveStats 가 CPU/메모리 KPI 와 서버 차트 폴리라인을 갱신한다", async () => {
  await withDom(async (w) => {
    const data = installFetch(w);
    await w.loadAdminView();

    const kpiValue = (id) => w.document.querySelector(`#${id} .admin-kpi-value`).textContent;
    const kpiFill = (id) => w.document.querySelector(`#${id} .admin-progress-fill`).style.width;
    assert.equal(kpiValue("adminCpuLoad"), "17%");
    assert.equal(kpiValue("adminMemory"), "50%");
    const points = () => w.document
      .querySelector("#adminServerChart polyline")
      .getAttribute("points").split(" ").length;
    assert.equal(points(), 1); // 부트 시딩 샘플 1개

    // 다음 폴링 응답을 변형: CPU 60%/60.5°C, 가용 메모리 축소 → 75%.
    Object.assign(data["/api/admin/server-stats"], {
      cpu_pct: 60, cpu_temp: 60.5,
      memory: { MemTotal: 8000000, MemAvailable: 2000000 },
    });
    await w._updateLiveStats();

    assert.equal(kpiValue("adminCpuLoad"), "60%");
    assert.equal(kpiFill("adminCpuLoad"), "60%");
    assert.match(w.document.querySelector("#adminCpuLoad .admin-kpi-note").textContent, /CPU 60\.5°C/);
    assert.equal(kpiValue("adminMemory"), "75%");
    assert.equal(kpiFill("adminMemory"), "75%");
    assert.equal(points(), 2); // push 된 샘플이 차트에 반영
  });
});
