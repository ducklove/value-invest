// jsdom behavior test for static/js/portfolio-rebalance.js (성과 탭 '리밸런싱' 카드).
//
// 실제 소스(utils → store → render → rebalance)를 브라우저와 같은 순서로
// 올리고 apiFetch 만 모킹해 검증한다: 드리프트 보고서 렌더(배지/포맷/이탈
// 강조/제안), 빈 상태 문구, 목표 에디터의 PUT 전체 교체 페이로드,
// rebalance_drift 알림 토글 호출.

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
  read("static", "js", "portfolio-render.js"), // returnClass 의 홈 (fmtPct/fmtKrw 는 utils.js)
  read("static", "js", "portfolio-rebalance.js"),
];

// index.html 의 pfRebalanceWrap 마크업과 동일한 구조.
const PANEL_HTML = `
  <div class="pf-nav-chart-wrap" id="pfRebalanceWrap">
    <div class="pf-nav-header">
      <h3>리밸런싱</h3>
      <div class="pf-rebal-header-actions">
        <label class="pf-rebal-alert-toggle">
          <input type="checkbox" id="pfRebalanceAlertCb"> 이탈 시 알림
        </label>
        <button class="pf-mini-btn" type="button" id="pfRebalanceEditBtn"></button>
      </div>
    </div>
    <div id="pfRebalanceEditor" class="pf-rebal-editor" style="display:none;"></div>
    <div id="pfRebalanceContent" class="pf-rebal-content"></div>
  </div>`;

const REPORT_PAYLOAD = {
  as_of: "2026-06-09",
  total_value: 1000000.0,
  breached_count: 1,
  items: [
    {
      scope: "stock", key: "005930", label: "삼성전자",
      target_weight_pct: 50.0, tolerance_pct: 5.0,
      current_weight_pct: 60.0, current_value: 600000.0, drift_pct: 10.0,
      breached: true, action: "매도", action_amount: 100000,
      approx_price: 60000.0, approx_shares: 2,
    },
    {
      scope: "group", key: "해외주식", label: "해외주식",
      target_weight_pct: 40.0, tolerance_pct: 5.0,
      current_weight_pct: 38.0, current_value: 380000.0, drift_pct: -2.0,
      breached: false, action: "매수", action_amount: 20000,
      approx_price: null, approx_shares: null,
    },
  ],
};

const EMPTY_PAYLOAD = { as_of: null, total_value: 0.0, breached_count: 0, items: [] };

function loadPanel({ report = REPORT_PAYLOAD, alerts = [] } = {}) {
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
  const json = (payload, status = 200) =>
    Promise.resolve({ ok: status < 400, status, json: async () => payload });
  w.apiFetch = (path, options = {}) => {
    calls.push({ path, options });
    if (path === "/api/portfolio/rebalance") return json(report);
    if (path === "/api/portfolio/rebalance/targets") return json({ ok: true, targets: [] });
    if (path === "/api/notifications/alerts" && !options.method) return json(alerts);
    if (path === "/api/notifications/alerts" && options.method === "POST") {
      return json({ id: 7, alert_type: "rebalance_drift", enabled: true });
    }
    if (/^\/api\/notifications\/alerts\/\d+$/.test(path)) return json({ ok: true });
    return json({}, 404);
  };
  return { w, calls };
}

const tick = () => new Promise((r) => setTimeout(r, 0));

test("드리프트 보고서 — 배지/비중 포맷/이탈 강조/제안/푸터 요약", async () => {
  const { w, calls } = loadPanel();
  await w.pfLoadRebalancePanel();
  await tick(); // 알림 토글 백그라운드 동기화까지 흡수

  assert.ok(calls.some((c) => c.path === "/api/portfolio/rebalance"));

  const content = w.document.getElementById("pfRebalanceContent");
  const rows = [...content.querySelectorAll("tbody tr")];
  assert.equal(rows.length, 2);

  // 종목 행: 배지 + 라벨, 비중은 절대 % (부호 없음), 이탈은 부호 포함 + 강조.
  const stockRow = rows[0];
  assert.equal(stockRow.querySelector(".pf-rebal-badge").textContent, "종목");
  assert.match(stockRow.querySelector(".pf-rebal-label").textContent, /삼성전자/);
  const cells = stockRow.querySelectorAll("td");
  assert.match(cells[1].textContent, /60\.00%/);
  assert.match(cells[2].textContent, /50\.00%/);
  assert.match(cells[2].textContent, /±5%p/);
  const drift = stockRow.querySelector(".pf-rebal-drift");
  assert.match(drift.textContent, /\+10\.00%/);
  assert.ok(drift.className.includes("breached"));
  assert.ok(drift.className.includes("positive"));
  assert.ok(stockRow.className.includes("pf-rebal-row-breached"));
  // 제안: 매도 금액 + 근사 주식 수.
  assert.match(cells[4].textContent, /매도 100,000원/);
  assert.match(cells[4].textContent, /~2주/);

  // 그룹 행: 그룹 배지, 미이탈은 강조 없음, 주식 수 없는 매수 제안.
  const groupRow = rows[1];
  assert.equal(groupRow.querySelector(".pf-rebal-badge").textContent, "그룹");
  assert.ok(!groupRow.className.includes("pf-rebal-row-breached"));
  const gDrift = groupRow.querySelector(".pf-rebal-drift");
  assert.match(gDrift.textContent, /-2\.00%/);
  assert.ok(!gDrift.className.includes("breached"));
  assert.ok(gDrift.className.includes("negative"));
  assert.match(groupRow.querySelectorAll("td")[4].textContent, /매수 20,000원/);
  assert.ok(!groupRow.querySelectorAll("td")[4].textContent.includes("주)"));

  // 푸터: 기준일 + 이탈 건수 요약.
  const footer = content.querySelector(".pf-chart-range");
  assert.match(footer.textContent, /기준일 2026-06-09/);
  assert.match(footer.textContent, /이탈 1건/);
});

test("목표 미설정(빈 items) — 안내 빈 상태 문구", async () => {
  const { w } = loadPanel({ report: EMPTY_PAYLOAD });
  await w.pfLoadRebalancePanel();

  const content = w.document.getElementById("pfRebalanceContent");
  assert.equal(content.querySelectorAll("table").length, 0);
  assert.match(content.textContent, /목표 비중을 설정하면 이탈 현황이 표시됩니다\./);
});

test("목표 에디터 — 기존 목표 프리필 + 저장 시 PUT 전체 교체 페이로드", async () => {
  const { w, calls } = loadPanel();
  w.PfStore.items = [
    { stock_code: "005930", stock_name: "삼성전자" },
    { stock_code: "000660", stock_name: "SK하이닉스" },
  ];
  w.PfStore.groups = [{ group_name: "해외주식" }, { group_name: "국내주식" }];
  await w.pfLoadRebalancePanel();
  await tick();

  w.pfRebalanceToggleEditor();
  const editor = w.document.getElementById("pfRebalanceEditor");
  assert.notEqual(editor.style.display, "none");
  let editRows = [...editor.querySelectorAll(".pf-rebal-editor-row")];
  assert.equal(editRows.length, 2); // 기존 목표 2건 프리필
  assert.equal(editRows[0].querySelector(".pf-rebal-scope").value, "stock");
  assert.equal(editRows[0].querySelector(".pf-rebal-key").value, "005930");
  assert.equal(editRows[0].querySelector(".pf-rebal-target").value, "50");
  assert.equal(editRows[1].querySelector(".pf-rebal-scope").value, "group");
  assert.equal(editRows[1].querySelector(".pf-rebal-key").value, "해외주식");

  // 행 추가 → 종목 dropdown 은 보유 종목, scope 변경 시 그룹 dropdown 으로.
  w.pfRebalanceEditorAddRow();
  editRows = [...editor.querySelectorAll(".pf-rebal-editor-row")];
  assert.equal(editRows.length, 3);
  const added = editRows[2];
  const keySelect = added.querySelector(".pf-rebal-key");
  assert.deepEqual([...keySelect.options].map((o) => o.value), ["005930", "000660"]);
  const scopeSelect = added.querySelector(".pf-rebal-scope");
  scopeSelect.value = "group";
  w.pfRebalanceScopeChanged(scopeSelect);
  assert.deepEqual([...keySelect.options].map((o) => o.value), ["해외주식", "국내주식"]);
  keySelect.value = "국내주식";
  added.querySelector(".pf-rebal-target").value = "10";
  added.querySelector(".pf-rebal-tol").value = ""; // 비우면 서버 기본값 — 페이로드에서 생략

  const before = calls.length;
  await w.pfRebalanceSave();
  const put = calls.find((c, i) => i >= before && c.options.method === "PUT"
    && c.path === "/api/portfolio/rebalance/targets");
  assert.ok(put, "PUT /api/portfolio/rebalance/targets must be called");
  assert.equal(put.options.headers["Content-Type"], "application/json");
  assert.deepEqual(JSON.parse(put.options.body), {
    targets: [
      { scope: "stock", key: "005930", target_weight_pct: 50, tolerance_pct: 5 },
      { scope: "group", key: "해외주식", target_weight_pct: 40, tolerance_pct: 5 },
      { scope: "group", key: "국내주식", target_weight_pct: 10 },
    ],
  });
  // 저장 성공 → 에디터 닫힘 + 보고서 force 재조회.
  assert.equal(editor.style.display, "none");
  assert.ok(calls.filter((c) => c.path === "/api/portfolio/rebalance").length >= 2);
});

test("저장 검증 실패(400) — 서버 detail 토스트 + 에디터 유지", async () => {
  const { w, calls } = loadPanel();
  await w.pfLoadRebalancePanel();
  await tick();
  w.apiFetch = (path, options = {}) => {
    calls.push({ path, options });
    if (options.method === "PUT") {
      return Promise.resolve({
        ok: false, status: 400,
        json: async () => ({ detail: "종목 목표 비중 합이 100%를 넘습니다 (110%)." }),
      });
    }
    return Promise.resolve({ ok: true, status: 200, json: async () => REPORT_PAYLOAD });
  };
  const toasts = [];
  w.showToast = (msg) => { toasts.push(msg); };

  w.pfRebalanceToggleEditor();
  await w.pfRebalanceSave();

  assert.equal(toasts.length, 1);
  assert.match(toasts[0], /종목 목표 비중 합이 100%를 넘습니다/);
  assert.notEqual(w.document.getElementById("pfRebalanceEditor").style.display, "none");
});

test("이탈 시 알림 토글 — 켜기 POST rebalance_drift / 끄기 PUT enabled:false", async () => {
  const existingRule = { id: 7, alert_type: "rebalance_drift", enabled: true };
  const { w, calls } = loadPanel({ alerts: [existingRule] });
  await w.pfLoadRebalancePanel();
  await tick();

  // 기존 규칙이 enabled 면 체크박스가 켜진 상태로 동기화된다.
  const cb = w.document.getElementById("pfRebalanceAlertCb");
  assert.equal(cb.checked, true);

  await w.pfRebalanceToggleAlert(false);
  const put = calls.find((c) => c.path === "/api/notifications/alerts/7" && c.options.method === "PUT");
  assert.ok(put, "disable must PUT the singleton rule");
  assert.deepEqual(JSON.parse(put.options.body), { enabled: false });

  await w.pfRebalanceToggleAlert(true);
  const post = calls.find((c) => c.path === "/api/notifications/alerts" && c.options.method === "POST");
  assert.ok(post, "enable must POST the rebalance_drift rule");
  assert.deepEqual(JSON.parse(post.options.body), { alert_type: "rebalance_drift" });
});
