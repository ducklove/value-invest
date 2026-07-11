// jsdom behavior test for static/js/portfolio-risk.js (성과 탭 '리스크' 카드).
//
// 실제 소스(utils → store → render → risk)를 브라우저와 같은 순서로 올리고
// apiFetch 만 모킹해 검증한다: 지표 포맷/색상, 데이터 부족 빈 상태,
// 윈도 전환 시 재조회 + 윈도별 메모(재선택 시 재요청 없음).

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");
const read = (...parts) => readFileSync(join(root, ...parts), "utf8");

const SOURCES = [
  read("static", "app-config.js"),
  read("static", "js", "utils.js"),
  read("static", "js", "portfolio-store.js"),
  read("static", "js", "portfolio-render.js"), // returnClass 의 홈 (fmtPct 는 utils.js)
  read("static", "js", "portfolio-risk.js"),
];

// index.html 의 pfRiskWrap 마크업과 동일한 구조.
const PANEL_HTML = `
  <div class="pf-nav-chart-wrap" id="pfRiskWrap">
    <div class="pf-nav-header">
      <h3>리스크</h3>
      <div class="valuation-period-btns" id="pfRiskWindowBtns"></div>
    </div>
    <div id="pfRiskContent" class="pf-risk-content">
      <div class="pf-risk-empty">성과 탭을 열면 리스크 지표를 불러옵니다.</div>
    </div>
  </div>`;

const FULL_PAYLOAD = {
  window: "1Y",
  start_date: "2025-06-10",
  end_date: "2026-06-09",
  points: 250,
  metrics: {
    cumulative_return_pct: 8.9,
    annualized_return_pct: 21.0,
    annualized_volatility_pct: 18.3,
    max_drawdown_pct: -10.0,
    max_drawdown_peak_date: "2026-06-02",
    max_drawdown_trough_date: "2026-06-03",
    current_drawdown_pct: -1.0,
    sharpe_ratio: 1.45,
    best_day: { date: "2026-06-02", return_pct: 10.0 },
    worst_day: { date: "2026-06-03", return_pct: -10.0 },
  },
  benchmark: { code: "IDX_KOSPI", name: "코스피", beta: 1.1, correlation: 0.8, overlap_returns: 240 },
  insufficient: false,
};

const INSUFFICIENT_PAYLOAD = {
  window: "1Y", start_date: null, end_date: null, points: 1,
  metrics: null, benchmark: null, insufficient: true,
};

function loadRiskPanel(payloadByWindow = { "1Y": FULL_PAYLOAD }) {
  const dom = new JSDOM(`<!doctype html><html><body>${PANEL_HTML}</body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  w.fetch = () => Promise.reject(new Error("no raw fetch in test"));
  for (const src of SOURCES) {
    const script = w.document.createElement("script");
    script.textContent = src;
    w.document.body.appendChild(script);
  }
  const calls = [];
  // 전역 함수 선언은 globalThis 의 쓰기 가능한 속성 — 모킹으로 교체.
  w.apiFetch = (path) => {
    calls.push(path);
    const win = new w.URL(path, "https://app.example.com").searchParams.get("window");
    const payload = payloadByWindow[win];
    if (!payload) return Promise.resolve({ ok: false, status: 404, json: async () => ({}) });
    return Promise.resolve({ ok: true, status: 200, json: async () => payload });
  };
  return { w, calls };
}

test("리스크 타일 — 공용 포맷터(fmtPct/returnClass) 기반 포맷과 색상 클래스", async () => {
  const { w, calls } = loadRiskPanel();
  await w.pfLoadRiskPanel();

  assert.equal(calls.length, 1);
  assert.match(calls[0], /^\/api\/portfolio\/risk\?window=1Y$/);

  const content = w.document.getElementById("pfRiskContent");
  const tiles = [...content.querySelectorAll(".pf-nav-ret-card")];
  assert.equal(tiles.length, 10);

  const tileByLabel = (label) =>
    tiles.find((t) => t.querySelector(".pf-nav-ret-label").textContent === label);
  const valueOf = (label) => tileByLabel(label).querySelector(".pf-nav-ret-value");

  // 수익률류는 부호 포함 2자리 %, positive=빨강/negative=파랑 클래스.
  assert.equal(valueOf("누적 수익률").textContent, "+8.90%");
  assert.ok(valueOf("누적 수익률").className.includes("positive"));
  assert.equal(valueOf("연환산 수익률").textContent, "+21.00%");
  // 변동성은 부호 없는 절대 퍼센트, 색상 없음.
  assert.equal(valueOf("변동성 (연환산)").textContent, "18.30%");
  assert.ok(!valueOf("변동성 (연환산)").className.includes("positive"));
  // MDD 는 음수 파랑 + 고점→저점 기간 서브라벨.
  assert.equal(valueOf("최대 낙폭 (MDD)").textContent, "-10.00%");
  assert.ok(valueOf("최대 낙폭 (MDD)").className.includes("negative"));
  assert.equal(
    tileByLabel("최대 낙폭 (MDD)").querySelector(".pf-risk-sub").textContent,
    "2026-06-02 → 2026-06-03",
  );
  assert.equal(valueOf("현재 낙폭").textContent, "-1.00%");
  // 무차원 지표는 % 없이 소수 2자리.
  assert.equal(valueOf("샤프 지수").textContent, "1.45");
  assert.equal(valueOf("베타").textContent, "1.10");
  assert.equal(valueOf("상관계수").textContent, "0.80");
  assert.equal(tileByLabel("베타").querySelector(".pf-risk-sub").textContent, "vs 코스피");
  // 최고/최악 일간 + 날짜 서브라벨.
  assert.equal(valueOf("최고 일간").textContent, "+10.00%");
  assert.equal(tileByLabel("최악 일간").querySelector(".pf-risk-sub").textContent, "2026-06-03");
  // 기간/벤치마크 메타 라인.
  const range = content.querySelector(".pf-chart-range");
  assert.match(range.textContent, /2025-06-10 ~ 2026-06-09/);
  assert.match(range.textContent, /벤치마크 코스피/);
});

test("데이터 부족(insufficient) — 친절한 빈 상태 문구", async () => {
  const { w } = loadRiskPanel({ "1Y": INSUFFICIENT_PAYLOAD });
  await w.pfLoadRiskPanel();

  const content = w.document.getElementById("pfRiskContent");
  assert.equal(content.querySelectorAll(".pf-nav-ret-card").length, 0);
  assert.match(content.textContent, /데이터가 부족합니다/);
  assert.match(content.textContent, /2일 이상 쌓이면/);
});

test("윈도 전환 시 재조회하고, 재선택은 메모로 재요청 없이 그린다", async () => {
  const payload3m = {
    ...FULL_PAYLOAD,
    window: "3M",
    metrics: { ...FULL_PAYLOAD.metrics, cumulative_return_pct: 3.3 },
  };
  const { w, calls } = loadRiskPanel({ "1Y": FULL_PAYLOAD, "3M": payload3m });
  await w.pfLoadRiskPanel();
  assert.equal(calls.length, 1);

  // 윈도 버튼 6개 + 활성 표시.
  const btns = [...w.document.querySelectorAll("#pfRiskWindowBtns .vp-btn")];
  assert.deepEqual(btns.map((b) => b.dataset.window), ["1M", "3M", "6M", "1Y", "YTD", "ALL"]);
  assert.ok(btns.find((b) => b.dataset.window === "1Y").classList.contains("active"));

  // 3M 전환 → 새 fetch + 새 값 렌더.
  await w.pfRiskSetWindow("3M");
  assert.equal(calls.length, 2);
  assert.match(calls[1], /window=3M$/);
  assert.match(w.document.getElementById("pfRiskContent").textContent, /\+3\.30%/);
  const active = w.document.querySelector("#pfRiskWindowBtns .vp-btn.active");
  assert.equal(active.dataset.window, "3M");

  // 1Y 재선택 → 메모 사용, 추가 fetch 없음.
  await w.pfRiskSetWindow("1Y");
  assert.equal(calls.length, 2);
  assert.match(w.document.getElementById("pfRiskContent").textContent, /\+8\.90%/);

  // 같은 윈도 재클릭은 no-op.
  await w.pfRiskSetWindow("1Y");
  assert.equal(calls.length, 2);
});

test("요청 실패 시 토스트 없이 패널 안 안내 문구만 보인다(silent)", async () => {
  const { w } = loadRiskPanel({});
  let toasts = 0;
  w.showToast = () => { toasts += 1; };
  await w.pfLoadRiskPanel();

  assert.match(w.document.getElementById("pfRiskContent").textContent, /불러오지 못했습니다/);
  assert.equal(toasts, 0);
});
