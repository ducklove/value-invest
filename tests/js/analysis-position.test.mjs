import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { JSDOM } from "jsdom";

const read = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), "utf8");
const SOURCES = ["static/js/utils.js", "static/js/analysis-position.js"].map(read);

function load({ items = [], storeItems = [], user = { sub: "u" }, price = 60000 } = {}) {
  const dom = new JSDOM(`<!doctype html><html><body>
    <section class="analysis-position" id="analysisPositionStrip" hidden></section>
    <div id="cards"></div>
  </body></html>`, { url: "https://app.example.com/analysis", runScripts: "dangerously" });
  const w = dom.window;
  w.APP_CONFIG = { integrations: {} };
  for (const src of SOURCES) {
    const script = w.document.createElement("script");
    script.textContent = src;
    w.document.body.appendChild(script);
  }
  w.eval(`currentUser = ${JSON.stringify(user)}; activeStockCode = "005930"; activeQuoteSnapshot = { price: ${price} };`);
  w.PfStore = { items: storeItems, accountId: null };
  w.__calls = [];
  w.apiFetchJson = async (path) => { w.__calls.push(path); return items; };
  w.__views = [];
  w.switchView = (v) => w.__views.push(v);
  w.analyzeStock = (c) => w.__views.push(`analyze:${c}`);
  w.pfFocusHolding = (c) => w.__views.push(`focus:${c}`);
  w.pfOpenAssetInsight = (c) => w.__views.push(`insight:${c}`);
  w.pfOpenMemoEditor = (c, anchor, item) => w.__views.push(`memo:${c}:${item?.memo || ""}`);
  return w;
}

const HOLDING = { stock_code: "005930", stock_name: "삼성전자", quantity: 10, avg_price: 50000, avg_price_krw: 50000,
  target_price: 90000, memo: "HBM <확인>", group_name: "코어" };

test("보유 종목을 분석하면 포트폴리오를 안 열어도 내 포지션·메모를 자동으로 보여준다", async () => {
  const w = load({ items: [HOLDING] });
  await w.renderAnalysisPosition("005930");
  const host = w.document.getElementById("analysisPositionStrip");
  assert.equal(host.hidden, false);
  assert.deepEqual(w.__calls, ["/api/portfolio"]);
  const text = host.textContent.replace(/\s+/g, " ");
  assert.match(text, /수량\s*10/);
  assert.match(text, /평가손익\s*\+100,000원/);
  assert.match(text, /수익률\s*\+20\.00%/);
  assert.match(text, /목표가\s*90,000원/);
  assert.ok(host.innerHTML.includes("HBM &lt;확인&gt;"), "메모는 이스케이프한다");

  w.showToast = () => {};
  host.querySelector(".js-ap-focus").click();
  await new Promise((r) => setTimeout(r, 0));
  host.querySelector(".js-ap-insight").click();
  host.querySelector(".js-ap-memo").click();
  assert.deepEqual(w.__views, ["portfolio", "focus:005930", "insight:005930", "memo:005930:HBM <확인>"]);

  // 1분 캐시 — 다음 종목 전환에서 다시 받지 않는다.
  await w.renderAnalysisPosition("000660");
  assert.equal(host.hidden, true);
  assert.equal(w.__calls.length, 1);
});

test("포트폴리오를 이미 열었으면 PfStore 를 쓰고, 비로그인·미보유면 숨긴다", async () => {
  const w = load({ storeItems: [HOLDING] });
  await w.renderAnalysisPosition("005930");
  assert.equal(w.__calls.length, 0);
  assert.equal(w.document.getElementById("analysisPositionStrip").hidden, false);

  const guest = load({ items: [HOLDING], user: null });
  await guest.renderAnalysisPosition("005930");
  assert.equal(guest.document.getElementById("analysisPositionStrip").hidden, true);
  assert.equal(guest.__calls.length, 0);
});

test("메모 저장 이벤트와 실시간 시세에 맞춰 바를 다시 그린다", async () => {
  const w = load({ items: [HOLDING] });
  await w.renderAnalysisPosition("005930");
  w.document.dispatchEvent(new w.CustomEvent("pf:memo-saved", { detail: { code: "005930", memo: "새 메모" } }));
  await new Promise((r) => setTimeout(r, 0));
  const host = w.document.getElementById("analysisPositionStrip");
  assert.match(host.textContent, /새 메모/);
  w.eval("activeQuoteSnapshot = { price: 45000 };");
  w.analysisPositionRefreshPrice();
  assert.match(host.textContent.replace(/\s+/g, " "), /수익률\s*-10\.00%/);
});

test("공통 종목 링크는 일반 클릭만 SPA 로 종목분석을 열고 새 탭 클릭은 브라우저에 맡긴다", () => {
  const w = load();
  const cards = w.document.getElementById("cards");
  cards.innerHTML = w.stockLinkHtml("005930", "삼성전자") + w.stockLinkHtml("AAPL", "<b>애플</b>");
  const links = cards.querySelectorAll("a.vc-stock-link");
  assert.equal(links.length, 1, "국내 종목코드만 링크로 만든다");
  assert.equal(links[0].getAttribute("href"), "/analysis?code=005930");
  assert.ok(cards.innerHTML.includes("<b>애플</b>"));

  const plain = new w.MouseEvent("click", { bubbles: true, cancelable: true, button: 0 });
  links[0].dispatchEvent(plain);
  assert.equal(plain.defaultPrevented, true);
  assert.deepEqual(w.__views, ["analysis", "analyze:005930"]);

  const newTab = new w.MouseEvent("click", { bubbles: true, cancelable: true, button: 0, metaKey: true });
  links[0].dispatchEvent(newTab);
  assert.equal(newTab.defaultPrevented, false);
  assert.equal(w.__views.length, 2);

  // 기존 스크리너·투자 논거의 /analysis 링크도 전체 새로고침 없이 연다.
  cards.innerHTML = '<a href="/analysis?code=000660">종목 분석 열기</a>';
  cards.querySelector("a").dispatchEvent(new w.MouseEvent("click", { bubbles: true, cancelable: true, button: 0 }));
  assert.deepEqual(w.__views.slice(2), ["analysis", "analyze:000660"]);
});

test("로그아웃·사용자 전환 시 이전 사용자의 포지션을 즉시 지우고 늦은 응답도 버린다", async () => {
  const w = load({ items: [HOLDING] });
  await w.renderAnalysisPosition("005930");
  const host = w.document.getElementById("analysisPositionStrip");
  assert.equal(host.hidden, false);
  w.eval("currentUser = null;");
  w.analysisPositionAuthChanged();
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(host.hidden, true);
  assert.equal(host.innerHTML, "");
  assert.equal(w.analysisPositionCachedItem("005930"), null);

  // 다른 사용자로 로그인 — 캐시를 다시 받는다.
  w.eval('currentUser = { google_sub: "other" };');
  w.analysisPositionAuthChanged();
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(w.__calls.length, 2);
});

test("[포트폴리오에서 보기]는 보유 탭으로 가고, 종목이 없는 개별 계좌면 합산으로 바꾼다", async () => {
  const w = load({ items: [HOLDING] });
  await w.renderAnalysisPosition("005930");
  const tabs = [], accounts = [], toasts = [];
  w.PfStore.accountId = "acc-2";
  w.PfStore.loading = false;
  w.pfSwitchTab = async (t) => tabs.push(t);
  w.loadPortfolio = async () => { w.PfStore.items = [{ stock_code: "000660" }]; };
  w.pfSelectAccount = async (id) => { accounts.push(id); w.PfStore.accountId = id; w.PfStore.items = [HOLDING]; };
  w.showToast = (m) => toasts.push(m);
  w.reportApiError = () => {};
  await w.analysisPositionGoToHolding("005930");
  assert.deepEqual(tabs, ["holdings"]);
  assert.deepEqual(accounts, [""]);
  assert.ok(w.__views.includes("focus:005930"));
  assert.match(toasts.join(" "), /합산으로 전환/);
  assert.match(toasts.join(" "), /필터나 검색어/, "행이 렌더되지 않았으면 가려졌다고 알린다");
});
