// jsdom behavior tests for the Today/MTD/YTD summary cards' 손익/평가액 병기.
//
// 입금은 수익이 아니다 — 카드의 색 있는 주 금액(손익)은 기간 순입출금을
// 차감한 값이어야 하고, 평가액 변동(입금 포함 잔고 증감)은 손익과 다를
// 때만 중립색 보조 줄로 병기된다. MTD/YTD 는 서버가 내려주는
// net_cashflow 를, Today 는 today_net_cashflow 를 쓴다.

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
const RENDER_SRC = read("static", "js", "portfolio-render.js");

function appendScript(w, source) {
  const script = w.document.createElement("script");
  script.textContent = source;
  w.document.body.appendChild(script);
}

function loadSummaryDom() {
  const dom = new JSDOM(`<!doctype html><html><body>
    <div id="pfSummary"></div>
    <table id="pfTable"><tbody id="pfBody"></tbody><tfoot id="pfFoot"></tfoot></table>
    <div id="pfEmpty"></div>
  </body></html>`, {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  const { window: w } = dom;
  // utils.js 는 부팅 side effect 가 많아 카드 경로가 쓰는 포맷터만 이식.
  w.quotePriceOrNull = (q) => (q && q.price !== null && q.price !== undefined ? Number(q.price) : null);
  w.fmtKrw = (n) => (n === null || n === undefined ? "-" : Number(Math.round(n)).toLocaleString());
  w.fmtSignedKrw = (n) => (n === null ? "-" : (n > 0 ? "+" : "") + w.fmtKrw(n));
  w.fmtPct = (n) => (n === null || n === undefined ? "-" : (n > 0 ? "+" : "") + n.toFixed(2) + "%");
  w._pfRenderColToggles = () => {};
  appendScript(w, STORE_SRC);
  appendScript(w, RENDER_SRC);
  w._renderSummarySparklines = () => {};
  return w;
}

// 보유 1종목: 평가액 1,000,000 (전일 정산 950,000).
function seedPortfolio(w, { todayCashflow = 0, mtdNetCashflow = null } = {}) {
  w.PfStore.items = [{
    stock_code: "005930", stock_name: "삼성전자", quantity: 10, avg_price: 90000,
    currency: "KRW", quote: { price: 100000, change: 0, change_pct: 0 },
  }];
  w.PfStore.navHistory = [
    { date: "2026-08-25", nav: 100, total_units: 9500, total_value: 950000 },
  ];
  w.PfStore.snapshots.prevDay = {
    date: "2026-08-25", total_value: 950000, nav: 100, stock_values: { "005930": 950000 },
    today_net_cashflow: todayCashflow,
    today_cashflows: todayCashflow
      ? [{ type: "deposit", amount: todayCashflow, signed_amount: todayCashflow, units_change: null }]
      : [],
    today_cashflows_by_stock: todayCashflow ? { CASH_KRW: todayCashflow } : {},
  };
  w.PfStore.snapshots.monthEnd = {
    date: "2026-07-31", total_value: 900000, nav: 95,
    stock_values: { "005930": 900000 },
    ...(mtdNetCashflow !== null ? { net_cashflow: mtdNetCashflow, cashflows_by_stock: { CASH_KRW: mtdNetCashflow } } : {}),
  };
  w.PfStore.snapshots.yearStart = null;
}

function cardByLabel(w, label) {
  return [...w.document.querySelectorAll(".pf-summary-card")]
    .find((c) => c.querySelector(".pf-summary-label")?.textContent.trim().startsWith(label));
}

test("오늘 입금은 Today 손익에서 빠지고 평가액 줄에만 나타난다", () => {
  const w = loadSummaryDom();
  // 전일 950,000 → 현재 1,000,000, 그중 50,000 은 오늘 입금.
  seedPortfolio(w, { todayCashflow: 50000 });
  w.renderPortfolio({ summaryOnly: true });

  const today = cardByLabel(w, "Today");
  const subs = [...today.querySelectorAll(".pf-summary-sub")];
  assert.equal(subs.length, 2);
  assert.match(subs[0].textContent, /손익\s*0/);          // 50,000 상승 − 50,000 입금
  assert.match(subs[1].textContent, /평가액\s*\+50,000/);  // 잔고 증감 그대로
  // 평가액 줄은 중립 — 손익 색상 클래스가 붙지 않는다.
  assert.ok(!subs[1].classList.contains("positive"));
  assert.ok(!subs[1].classList.contains("negative"));
  assert.ok(subs[1].classList.contains("pf-summary-sub-flow"));
  // Today % 도 손익 기준 (0.00%) — 입금이 수익률로 잡히지 않는다.
  assert.match(today.querySelector(".pf-summary-value").textContent, /0\.00%/);
});

test("MTD 손익은 서버 net_cashflow 를 차감하고 평가액 변동을 병기한다", () => {
  const w = loadSummaryDom();
  // 월말 900,000 → 현재 1,000,000, 기간 중 입금 50,000 (오늘 입금 포함).
  seedPortfolio(w, { todayCashflow: 50000, mtdNetCashflow: 50000 });
  w.renderPortfolio({ summaryOnly: true });

  const mtd = cardByLabel(w, "MTD");
  const subs = [...mtd.querySelectorAll(".pf-summary-sub")];
  assert.equal(subs.length, 2);
  assert.match(subs[0].textContent, /손익\s*\+50,000/);    // 100,000 − 50,000 입금
  assert.ok(subs[0].classList.contains("positive"));
  assert.match(subs[1].textContent, /평가액\s*\+100,000/);
  assert.ok(!subs[1].classList.contains("positive"));
});

test("기간 중 입출금이 없으면 평가액 줄은 생략된다", () => {
  const w = loadSummaryDom();
  seedPortfolio(w, { todayCashflow: 0, mtdNetCashflow: 0 });
  w.renderPortfolio({ summaryOnly: true });

  for (const label of ["Today", "MTD"]) {
    const card = cardByLabel(w, label);
    const subs = [...card.querySelectorAll(".pf-summary-sub")];
    assert.equal(subs.length, 1, `${label} 카드는 손익 한 줄만`);
    assert.match(subs[0].textContent, /손익/);
    assert.ok(!subs[0].textContent.includes("평가액"));
  }
});

test("net_cashflow 를 아직 안 내려주는 응답(구버전 캐시)에서도 손익은 렌더된다", () => {
  const w = loadSummaryDom();
  seedPortfolio(w, { todayCashflow: 0, mtdNetCashflow: null });
  w.renderPortfolio({ summaryOnly: true });

  const mtd = cardByLabel(w, "MTD");
  const subs = [...mtd.querySelectorAll(".pf-summary-sub")];
  assert.equal(subs.length, 1);
  assert.match(subs[0].textContent, /손익\s*\+100,000/);
});
