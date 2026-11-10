// jsdom behavior test for static/js/portfolio-exposure.js (심층 분석 탭 X-Ray 카드).
//
// 실제 소스를 브라우저 순서로 올리고 apiFetch 만 모킹한다: 커버리지·기준일 표시, 모르는 값의
// '-' 표기, 지주 환산 분리 안내, 계좌 필터·메모, 늦은 응답 무시, 실패·빈 상태.

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
  read("static", "js", "portfolio-exposure.js"),
];

const PANEL_HTML = '<div id="pfExposureWrap"><div id="pfExposureContent"></div></div>';

const PAYLOAD = {
  available: true,
  account_id: null,
  computed_at: "2026-11-08T20:50:00",
  totals: { total_value: 5200000, cash_value: 1000000, cash_pct: 19.23, invested_value: 4200000, unpriced_count: 1, stale_quotes: 0 },
  exposures: {
    rows: [
      { code: "005930", name: "삼성전자", direct_value: 1800000, derivative_value: 0, etf_value: 300000, net_pct: 50, holdco_pct: null },
      { code: "000660", name: "SK<하이닉스>", direct_value: 0, derivative_value: -300000, etf_value: 200000, net_pct: -2.38, holdco_pct: 12.5 },
    ],
    row_count: 2,
    etf_unknown_pct: 16.67,
    leveraged_etf: false,
  },
  concentration: {
    direct: { top5_pct: 70 },
    look_through: { top5_pct: 84.09, hhi: 2670.45, effective_n: 3.74 },
  },
  etfs: {
    coverage_pct: 41.67,
    rows: [
      { code: "069500", name: "KODEX 200", weight_pct: 23.8, coverage_pct: 50, status: "covered", leveraged: false, top: [{ code: "005930", name: "삼성전자", weight_pct: 30 }] },
      { code: "VWO", name: "신흥국", weight_pct: 4.76, coverage_pct: 0, status: "no_holdings", leveraged: false, top: [] },
    ],
  },
  holdcos: {
    estimated: true,
    as_of: "2026-11-09",
    total_look_through_value: 2000000,
    rows: [
      { code: "000670", name: "영풍", value: 500000, look_through_value: 2000000, multiple: 4, hedged: false, missing_prices: 0, subsidiaries: [{ name: "고려아연", value: 2000000 }] },
      { code: "000880", name: "한화", value: 200000, look_through_value: 900000, multiple: null, hedged: true, missing_prices: 1, subsidiaries: [{ name: "한화에어로스페이스", value: 900000 }] },
    ],
    subsidiary_rows: [
      { code: "010130", name: "고려아연", value: 2000000, pct: 47.6, net_pct: 0, via: ["영풍"] },
      { code: "012450", name: "한화에어로스페이스", value: 900000, pct: 21.4, net_pct: null, via: ["한화"] },
    ],
  },
  breakdowns: {
    currency: [{ label: "KRW", pct: 67.3, cash_value: 0 }, { label: "USD", pct: 32.7, cash_value: 1000000 }],
    asset_class: [{ label: "주식", pct: 67.3 }, { label: "선물·파생", pct: -5.8 }, { label: "현금", pct: 19.2, is_cash: true }],
    sector: {
      classified_pct: 59.5,
      rows: [
        { label: "통신 및 방송 장비 제조업", pct: 50, kind: "sector" },
        { label: "미분류", pct: 5, kind: "unclassified" },
        { label: "ETF 미공개 구성", pct: 16.67, kind: "undisclosed" },
      ],
    },
  },
  risk: {
    available: true,
    portfolio_volatility_pct: 20.62,
    coverage_pct: 55.4,
    start_date: "2025-11-10",
    end_date: "2026-11-06",
    sample_returns: 242,
    diversification_ratio: 1.54,
    implied_avg_correlation: -0.69,
    rows: [
      { code: "A", name: "에이", weight_pct: 75, volatility_pct: 33, risk_contribution_pct: 116.67 },
      { code: "B", name: "비", weight_pct: 25, volatility_pct: 27, risk_contribution_pct: -16.67 },
    ],
    excluded: [{ code: "VOO", reason: "no_series" }, { code: "0222E0", reason: "short_history" }],
    correlation: { codes: ["A", "B"], names: ["에이", "비"], matrix: [[1, -0.69], [-0.69, null]] },
  },
  sources: [
    { id: "eiayn", label: "ETF 구성", as_of: "2026-09-30", age_days: 39, stale: true, available: true },
    { id: "kind", label: "업종", as_of: "2026-11-08", age_days: 0, stale: false, available: true },
    { id: "finance_pi", label: "일봉", as_of: null, age_days: null, stale: false, available: false },
  ],
  notes: ["계산 방식 안내"],
};

function load(handler) {
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
  w.apiFetch = (path) => {
    calls.push(path);
    return handler(path);
  };
  return { w, calls, content: () => w.document.getElementById("pfExposureContent") };
}

const ok = (payload) => Promise.resolve({ ok: true, status: 200, json: async () => payload });

test("커버리지·기준일·별도 현금을 그리고 낡은 ETF 자료를 경고한다", async () => {
  const { w, calls, content } = load(() => ok(PAYLOAD));
  await w.pfLoadExposurePanel();
  assert.deepEqual(calls, ["/api/portfolio/exposure"]);
  const text = content().textContent;
  assert.match(text, /전체 계좌 합산/);
  assert.match(text, /ETF 커버리지41\.7%/);
  assert.match(text, /현금 \(별도\)/);
  assert.match(text, /총자산의 19\.2%/);
  // 기준일: 낡은 자료는 표시하고 경고하며, 받지 못한 자료도 숨기지 않는다.
  const eiayn = content().querySelector('[data-source="eiayn"]');
  assert.match(eiayn.textContent, /2026-09-30 \(39일 전\)/);
  assert.ok(eiayn.classList.contains("is-stale"));
  assert.ok(!content().querySelector('[data-source="kind"]').classList.contains("is-stale"));
  assert.match(content().querySelector('[data-source="finance_pi"]').textContent, /받지 못함/);
  assert.match(text, /ETF 구성 자료가 2026-09-30 기준입니다/);
  assert.match(text, /시세가 없는 1종목은 합계에서 뺐습니다/);
});

test("노출 표는 지주 환산을 분리하고 모르는 값은 0 이 아니라 '-' 다", async () => {
  const { w, content } = load(() => ok(PAYLOAD));
  await w.pfLoadExposurePanel();
  const rows = [...content().querySelectorAll("#pfXrayExposureTable tbody tr")];
  assert.equal(rows.length, 2);
  const cells = (row) => [...row.querySelectorAll("td")].map((td) => td.textContent);
  assert.equal(cells(rows[0])[0], "50.0%"); // 순노출은 모바일에서도 바로 보이는 첫 숫자 열이다.
  assert.equal(cells(rows[0])[1], "180만"); // 억·만 단위만 써서 크기를 잘못 읽지 않게 한다.
  assert.equal(cells(rows[0])[2], "30만");
  assert.equal(cells(rows[0])[3], "-"); // 지주 환산 없음은 0.0% 가 아니다.
  assert.equal(cells(rows[1])[0], "-2.4%");
  assert.equal(cells(rows[1])[3], "12.5%");
  // 이름은 이스케이프된다.
  assert.equal(rows[1].querySelector("th").textContent.trim(), "SK<하이닉스> 000660");
  assert.match(content().textContent, /순노출·합계·HHI에 더하지 않습니다/);
  assert.match(content().querySelector("#pfXrayExposureTable thead").textContent, /지주 환산\(추정\)/);
});

test("지주사 환산은 추정치로 표시하고 기준일과 한계를 드러낸다", async () => {
  const { w, content } = load(() => ok(PAYLOAD));
  await w.pfLoadExposurePanel();
  const basis = content().querySelector("#pfXrayHoldcoBasis").textContent;
  assert.match(basis, /추정치입니다/);
  assert.match(basis, /설정값 \(지분 파일 2026-11-09\)/);
  assert.match(basis, /공시와 다를 수 있으니 정본으로 쓰지 마세요/);
  assert.match(basis, /총액에 더하지 않았습니다/);

  const holdcos = [...content().querySelectorAll("#pfXrayHoldcoTable tbody tr")];
  assert.equal(holdcos[0].querySelectorAll("td")[2].textContent, "4.00배");
  // 헤지가 있으면 배수를 내지 않고, 시세 없는 자회사를 숨기지 않는다.
  assert.equal(holdcos[1].querySelectorAll("td")[2].textContent, "헤지 포함");
  assert.match(holdcos[1].textContent, /시세 없음 1개/);

  const subsidiaries = [...content().querySelectorAll("#pfXraySubsidiaryTable tbody tr")];
  assert.equal(subsidiaries.length, 2);
  const cells = (row) => [...row.querySelectorAll("td")].map((td) => td.textContent);
  assert.deepEqual(cells(subsidiaries[0]).slice(1), ["47.6%", "0.0%", "영풍"]);
  assert.equal(cells(subsidiaries[1])[2], "-"); // 순노출을 모르면 0 이 아니다.
});

test("지분 파일 날짜를 모르면 날짜를 지어내지 않는다", async () => {
  const payload = { ...PAYLOAD, holdcos: { ...PAYLOAD.holdcos, as_of: null } };
  const { w, content } = load(() => ok(payload));
  await w.pfLoadExposurePanel();
  const basis = content().querySelector("#pfXrayHoldcoBasis").textContent;
  assert.match(basis, /설정값이라 공시와 다를 수 있으니/);
  assert.doesNotMatch(basis, /지분 파일/);
});

test("ETF 는 구성 자료가 없어도 커버리지를 표시한다", async () => {
  const { w, content } = load(() => ok(PAYLOAD));
  await w.pfLoadExposurePanel();
  const vwo = content().querySelector('tr[data-etf="VWO"]');
  assert.equal(vwo.querySelectorAll("td")[1].textContent, "0.0%");
  assert.match(vwo.textContent, /구성 자료 없음/);
  assert.match(content().querySelector('tr[data-etf="069500"]').textContent, /삼성전자 30\.0/);
});

test("업종은 미분류·미공개를 그대로 보여 주고 음수 막대를 구분한다", async () => {
  const { w, content } = load(() => ok(PAYLOAD));
  await w.pfLoadExposurePanel();
  const sector = content().querySelector("#pfXraySector");
  assert.match(sector.textContent, /미분류/);
  assert.match(sector.textContent, /ETF 미공개 구성/);
  assert.equal(sector.querySelectorAll(".pf-xray-bar.is-muted").length, 2);
  assert.ok(content().querySelector(".pf-xray-bar.is-negative"));
  assert.match(content().textContent, /분류 59\.5%/);
  // 분류 행과 분류 밖 행은 한 차트라 축척이 같다: 가장 큰 50% 가 100%, 16.67% 는 33.3%.
  const widths = [...sector.querySelectorAll(".pf-xray-track i")].map((bar) => bar.style.width);
  assert.deepEqual(widths, ["100%", "10%", "33.3%"]);
});

test("위험 기여·상관을 그리고 표본 부족은 제외 사유로 드러낸다", async () => {
  const { w, content } = load(() => ok(PAYLOAD));
  await w.pfLoadExposurePanel();
  const rows = [...content().querySelectorAll("#pfXrayRiskTable tbody tr")];
  assert.equal(rows[0].querySelectorAll("td")[0].textContent, "+116.7%");
  assert.equal(rows[1].querySelectorAll("td")[0].textContent, "-16.7%");
  assert.match(content().textContent, /표본 242일/);
  assert.match(content().textContent, /제외 일봉 없음 1개 · 표본 부족 1개/);
  const cells = [...content().querySelectorAll("#pfXrayHeatmap tbody td")];
  assert.equal(cells.length, 4);
  assert.equal(cells[1].textContent, "-0.69");
  assert.equal(cells[3].textContent, "-"); // 상관을 못 구하면 null → '-'.
  // 행 번호로 열 번호와 맞춰 읽는다.
  assert.match(content().querySelector("#pfXrayHeatmap tbody th").textContent, /^1\. 에이$/);
});

test("해외 종목이 모델에 들면 원화 환산·종가 기준을 알리고 묵은 일봉은 제외 사유로 보인다", async () => {
  const risk = { ...PAYLOAD.risk, foreign_count: 3, excluded: [{ code: "OLD", reason: "stale_rows" }] };
  const { w, content } = load(() => ok({ ...PAYLOAD, risk }));
  await w.pfLoadExposurePanel();
  const note = content().querySelector("#pfXrayRiskForeign");
  assert.match(note.textContent, /해외 3종목은 원화로 환산/);
  assert.match(note.textContent, /미국·유럽은 전날 종가/);
  assert.match(content().textContent, /제외 일봉 지연 1개/);

  // 해외 종목이 없으면 안내를 붙이지 않는다.
  const domestic = load(() => ok(PAYLOAD));
  await domestic.w.pfLoadExposurePanel();
  assert.equal(domestic.content().querySelector("#pfXrayRiskForeign"), null);
});

test("긴 표는 앞 10행만 펼치고 접힌 행 수와 전체 개수를 정확히 알려 준다", async () => {
  const rows = Array.from({ length: 12 }, (_, i) => ({
    code: `C${i}`, name: `종목${i}`, direct_value: 1000 - i, derivative_value: 0, etf_value: 0, net_pct: 10 - i, holdco_pct: null,
  }));
  const payload = { ...PAYLOAD, exposures: { ...PAYLOAD.exposures, rows, row_count: 60 } };
  const { w, content } = load(() => ok(payload));
  await w.pfLoadExposurePanel();
  assert.equal(content().querySelectorAll("#pfXrayExposureTable tbody tr").length, 10);
  const summaries = [...content().querySelectorAll("summary")].map((el) => el.textContent);
  // 응답은 행 수를 제한하므로 접힌 2행이 나머지 전부인 것처럼 적지 않는다.
  assert.ok(summaries.includes("다음 2개 보기 (전체 60개)"));

  const whole = load(() => ok({ ...PAYLOAD, exposures: { ...PAYLOAD.exposures, rows, row_count: 12 } }));
  await whole.w.pfLoadExposurePanel();
  const wholeSummaries = [...whole.content().querySelectorAll("summary")].map((el) => el.textContent);
  assert.ok(wholeSummaries.includes("나머지 2개 보기"));
});

test("위험 모델이 없으면 값을 지어내지 않는다", async () => {
  const payload = { ...PAYLOAD, risk: { available: false, portfolio_volatility_pct: null, coverage_pct: null } };
  const { w, content } = load(() => ok(payload));
  await w.pfLoadExposurePanel();
  assert.equal(content().querySelector("#pfXrayRiskTable"), null);
  assert.match(content().textContent, /위험 기여를 계산하지 못했습니다/);
  assert.match(content().textContent, /변동성 \(모델\)-/);
});

test("선택 계좌를 쿼리로 보내고 계좌별로 메모한다", async () => {
  const { w, calls, content } = load((path) => ok({ ...PAYLOAD, account_id: path.includes("acc") ? "acc/1" : null }));
  w.PfAccounts = { rows: [{ account_id: "acc/1", name: "연금 계좌" }] };
  await w.pfLoadExposurePanel();
  w.eval("PfStore.accountId = 'acc/1'");
  await w.pfLoadExposurePanel();
  assert.deepEqual(calls, ["/api/portfolio/exposure", "/api/portfolio/exposure?account_id=acc%2F1"]);
  // 탭 재진입은 재요청 없이 그린다. force 면 다시 받는다.
  await w.pfLoadExposurePanel();
  assert.equal(calls.length, 2);
  await w.pfLoadExposurePanel({ force: true });
  assert.equal(calls.length, 3);
  assert.equal(content().querySelector("#pfXrayScope").textContent.split(" · ")[0], "연금 계좌");
});

test("늦게 도착한 이전 계좌 응답은 그리지 않는다", async () => {
  let release;
  const slow = new Promise((resolve) => { release = resolve; });
  const { w, content } = load((path) => (path.includes("account_id") ? ok({ ...PAYLOAD, computed_at: "2026-11-08T21:00:00" }) : slow));
  const first = w.pfLoadExposurePanel();
  w.eval("PfStore.accountId = 'acc-b'");
  await w.pfLoadExposurePanel();
  release({ ok: true, status: 200, json: async () => ({ ...PAYLOAD, computed_at: "2026-11-08T09:00:00" }) });
  await first;
  assert.match(content().textContent, /21:00 계산/);
});

test("실패·로그아웃·빈 포트폴리오 안내", async () => {
  const failed = load(() => Promise.resolve({ ok: false, status: 500, json: async () => ({}) }));
  let toasts = 0;
  failed.w.showToast = () => { toasts += 1; };
  await failed.w.pfLoadExposurePanel();
  assert.match(failed.content().textContent, /불러오지 못했습니다/);
  assert.equal(toasts, 0);

  const anon = load(() => Promise.resolve({ ok: false, status: 401, json: async () => ({}) }));
  await anon.w.pfLoadExposurePanel();
  assert.match(anon.content().textContent, /로그인 후/);

  const empty = load(() => ok({ available: false }));
  await empty.w.pfLoadExposurePanel();
  assert.match(empty.content().textContent, /평가할 보유 종목이 없습니다/);
});

test("로그아웃 리셋은 다른 사용자에게 메모를 남기지 않는다", async () => {
  const { w, calls, content } = load(() => ok(PAYLOAD));
  await w.pfLoadExposurePanel();
  w.pfResetExposurePanel();
  assert.match(content().textContent, /심층 분석을 열면/);
  await w.pfLoadExposurePanel();
  assert.equal(calls.length, 2);
});
