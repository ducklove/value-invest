// jsdom behavior test for static/js/portfolio-journal.js (투자 일지).
//
// 실제 소스(utils → store → render → journal)를 브라우저와 같은 순서로 올리고
// apiFetch 만 모킹해 두 표면을 검증한다: 성과 탭 타임라인 렌더(당시→현재
// 수익률 색상, 당시 목표가, note escapeHtml), 분석 화면 폼의 POST 페이로드,
// note 인라인 수정(PATCH), 삭제(confirm 게이트), 빈 상태, 메모/force.

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
  read("static", "js", "portfolio-journal.js"),
];

// index.html 의 두 표면과 동일한 구조(분석 화면 + 성과 탭 카드).
const PAGE_HTML = `
  <h2 id="companyName">삼성전자 (005930)</h2>
  <div class="stock-journal" id="stockJournalSection">
    <div id="stockJournalForm"></div>
    <div id="stockJournalList"></div>
  </div>
  <div class="pf-nav-chart-wrap" id="pfJournalWrap">
    <div id="pfJournalContent" class="pf-journal-content">
      <div class="pf-risk-empty">성과 탭을 열면 투자 일지를 불러옵니다.</div>
    </div>
  </div>`;

const BUY_ENTRY = {
  id: 1,
  stock_code: "005930",
  stock_name: "삼성전자",
  entry_type: "buy",
  note: "PBR 0.9 — 저평가 & <b>주의: 사이클</b>",
  price_at_entry: 70000,
  quantity: 10,
  target_price_at_entry: 100000,
  created_at: "2026-06-01T09:30:00",
  updated_at: "2026-06-01T09:30:00",
  current_price: 77000,
  since_entry_return_pct: 10.0,
};

const MEMO_ENTRY = {
  id: 2,
  stock_code: "AAPL",
  stock_name: "Apple",
  entry_type: "memo",
  note: "실적 발표 대기",
  price_at_entry: null,
  quantity: null,
  target_price_at_entry: null,
  created_at: "2026-06-05T10:00:00",
  updated_at: "2026-06-05T10:00:00",
  current_price: null,
  since_entry_return_pct: null,
};

function loadPage({ entries = [BUY_ENTRY, MEMO_ENTRY], failLoads = false } = {}) {
  const dom = new JSDOM(`<!doctype html><html><body>${PAGE_HTML}</body></html>`, {
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
  w.apiFetch = (path, options = {}) => {
    calls.push({ path, options, method: (options.method || "GET").toUpperCase() });
    if (failLoads) return Promise.resolve({ ok: false, status: 500, json: async () => ({}) });
    const method = (options.method || "GET").toUpperCase();
    if (method === "GET") {
      return Promise.resolve({ ok: true, status: 200, json: async () => ({ entries }) });
    }
    if (method === "POST") {
      const body = JSON.parse(options.body);
      return Promise.resolve({
        ok: true, status: 200,
        json: async () => ({ ...BUY_ENTRY, id: 99, ...body }),
      });
    }
    if (method === "PATCH") {
      const body = JSON.parse(options.body);
      return Promise.resolve({
        ok: true, status: 200,
        json: async () => ({ ...BUY_ENTRY, note: body.note, updated_at: "2026-06-10T12:00:00" }),
      });
    }
    return Promise.resolve({ ok: true, status: 200, json: async () => ({ ok: true }) });
  };
  return { w, calls };
}

test("성과 탭 타임라인 — 당시→현재 수익률 색상, 당시 목표가, note escapeHtml", async () => {
  const { w, calls } = loadPage();
  await w.pfLoadJournalPanel();

  assert.equal(calls.length, 1);
  assert.equal(calls[0].path, "/api/portfolio/journal");

  const content = w.document.getElementById("pfJournalContent");
  const cards = [...content.querySelectorAll(".pf-journal-card")];
  assert.equal(cards.length, 2);

  // 매수 카드: 배지 + 종목 링크 + 당시 70,000 → 현재 77,000 (+10.00%).
  const buy = cards[0];
  assert.equal(buy.querySelector(".pf-journal-badge").textContent, "매수");
  assert.ok(buy.querySelector(".pf-journal-badge").classList.contains("buy"));
  assert.equal(buy.querySelector(".pf-journal-stock").textContent, "삼성전자");
  assert.match(buy.querySelector(".pf-journal-prices").textContent, /당시 70,000 → 현재 77,000/);
  const pct = buy.querySelector(".pf-return");
  assert.ok(pct.classList.contains("positive"));
  assert.equal(pct.textContent, "+10.00%");
  assert.match(buy.textContent, /수량 10주/);
  assert.match(buy.textContent, /당시 목표가 100,000/);
  // note 는 escapeHtml — 원시 HTML 이 요소로 살아나면 안 된다.
  assert.equal(buy.querySelector(".pf-journal-note b"), null);
  assert.match(buy.querySelector(".pf-journal-note").textContent, /<b>주의: 사이클<\/b>/);

  // 메모 카드: 가격 스냅샷 없음 → '당시 가격 미기록', 수익률 없음.
  const memo = cards[1];
  assert.equal(memo.querySelector(".pf-journal-badge").textContent, "메모");
  assert.match(memo.textContent, /당시 가격 미기록/);
  assert.equal(memo.querySelector(".pf-return"), null);

  // 메모 재호출은 재요청 없이, force 는 다시 가져온다.
  await w.pfLoadJournalPanel();
  assert.equal(calls.length, 1);
  await w.pfLoadJournalPanel({ force: true });
  assert.equal(calls.length, 2);
});

test("분석 화면 폼 — POST 페이로드(stock_code/entry_type/note/quantity/표시명)", async () => {
  const { w, calls } = loadPage({ entries: [] });
  await w.loadStockJournal("005930");
  assert.equal(calls[0].path, "/api/portfolio/journal?stock_code=005930");

  // 빈 상태 + 폼 렌더.
  assert.match(w.document.getElementById("stockJournalList").textContent, /아직 기록이 없습니다/);
  const form = w.document.getElementById("stockJournalForm");
  assert.ok(form.querySelector("#stockJournalType"));

  w.document.getElementById("stockJournalType").value = "buy";
  w.document.getElementById("stockJournalQty").value = "10";
  w.document.getElementById("stockJournalNote").value = "PBR 0.9 저평가 매수";
  await w.stockJournalSubmit();

  const post = calls.find((c) => c.method === "POST");
  assert.ok(post, "POST 호출 없음");
  assert.equal(post.path, "/api/portfolio/journal");
  assert.deepEqual(JSON.parse(post.options.body), {
    stock_code: "005930",
    entry_type: "buy",
    note: "PBR 0.9 저평가 매수",
    quantity: 10,
    stock_name: "삼성전자", // companyName '삼성전자 (005930)' 의 앞부분
  });
  // 성공 시 입력 초기화 + 목록 재조회(GET 2회째).
  assert.equal(w.document.getElementById("stockJournalNote").value, "");
  assert.equal(calls.filter((c) => c.method === "GET").length, 2);
});

test("빈 note 로는 POST 하지 않고 토스트만 띄운다", async () => {
  const { w, calls } = loadPage({ entries: [] });
  await w.loadStockJournal("005930");
  let toasts = 0;
  w.showToast = () => { toasts += 1; };
  w.document.getElementById("stockJournalNote").value = "   ";
  await w.stockJournalSubmit();
  assert.equal(calls.filter((c) => c.method === "POST").length, 0);
  assert.equal(toasts, 1);
});

test("note 인라인 수정 — PATCH 페이로드와 화면 갱신, 스냅샷 표시는 유지", async () => {
  const { w, calls } = loadPage();
  await w.pfLoadJournalPanel();
  const content = w.document.getElementById("pfJournalContent");

  w.pfJournalEditNote(1);
  const input = content.querySelector('[data-journal-edit-input="1"]');
  assert.ok(input, "인라인 에디터 미표시");
  input.value = "수정된 이유";
  await w.pfJournalSaveNote(1);

  const patch = calls.find((c) => c.method === "PATCH");
  assert.equal(patch.path, "/api/portfolio/journal/1");
  assert.deepEqual(JSON.parse(patch.options.body), { note: "수정된 이유" });

  // 재렌더: 새 note + (수정됨) 마커, 가격 스냅샷 라인은 그대로.
  const card = content.querySelector('[data-journal-id="1"]');
  assert.equal(card.querySelector('[data-journal-edit-input="1"]'), null);
  assert.match(card.querySelector(".pf-journal-note").textContent, /수정된 이유/);
  assert.match(card.textContent, /수정됨/);
  assert.match(card.textContent, /당시 70,000 → 현재 77,000/);
});

test("삭제 — confirm 승인 시 DELETE 후 카드 제거, 거부 시 no-op", async () => {
  const { w, calls } = loadPage();
  await w.pfLoadJournalPanel();
  const content = w.document.getElementById("pfJournalContent");

  w.confirm = () => false;
  await w.pfJournalDelete(1);
  assert.equal(calls.filter((c) => c.method === "DELETE").length, 0);
  assert.ok(content.querySelector('[data-journal-id="1"]'));

  w.confirm = () => true;
  await w.pfJournalDelete(1);
  const del = calls.find((c) => c.method === "DELETE");
  assert.equal(del.path, "/api/portfolio/journal/1");
  assert.equal(content.querySelector('[data-journal-id="1"]'), null);
  assert.ok(content.querySelector('[data-journal-id="2"]'), "다른 카드는 유지");
});

test("빈 상태/로드 실패 — 패널 안 안내 문구, 토스트 없음(silent)", async () => {
  const empty = loadPage({ entries: [] });
  await empty.w.pfLoadJournalPanel();
  assert.match(
    empty.w.document.getElementById("pfJournalContent").textContent,
    /기록된 투자 일지가 없습니다/);

  const failed = loadPage({ failLoads: true });
  let toasts = 0;
  failed.w.showToast = () => { toasts += 1; };
  await failed.w.pfLoadJournalPanel();
  await failed.w.loadStockJournal("005930");
  assert.match(
    failed.w.document.getElementById("pfJournalContent").textContent,
    /불러오지 못했습니다/);
  assert.match(
    failed.w.document.getElementById("stockJournalList").textContent,
    /불러오지 못했습니다/);
  assert.equal(toasts, 0);
});
