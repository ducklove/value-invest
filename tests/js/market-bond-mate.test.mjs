// jsdom behavior tests for static/js/market-bond-mate.js.
//
// bond-mate 는 국채·환율의 source of record 지만 정적 사이트라 30분 주기다.
// 반면 /api/market-summary 는 60초 TTL 라이브라 같은 코드면 로컬이 더 신선하다.
// 여기서 검증하는 계약은 그 우선순위와, bond-mate 가 없을 때의 무해한 폴백이다.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const read = (p) => readFileSync(join(__dirname, "..", "..", "static", "js", p), "utf8");
const UTILS = read("utils.js");
const BOND_MATE = read("market-bond-mate.js");

function load(integrations) {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    runScripts: "dangerously",
    url: "https://app.example.com/",
  });
  dom.window.APP_CONFIG = { integrations: integrations || {} };
  for (const src of [UTILS, BOND_MATE]) {
    const script = dom.window.document.createElement("script");
    script.textContent = src;
    dom.window.document.body.appendChild(script);
  }
  return dom.window;
}

const BOND_MATE_CONFIG = {
  bondMate: {
    baseUrl: "https://ducklove.github.io/bond-mate",
    dataUrl: "https://ducklove.github.io/bond-mate/data/current.json",
  },
};

const SNAPSHOT = {
  generated_at: "2026-08-29T02:05:00+00:00",
  rates: {
    US10Y: { value: 4.73, change: 0.06, change_pct: 1.29, date: "2026-08-28", country: "US", maturity: 10, tenor: "10년" },
    KR3Y: { value: 3.788, change: 0.033, change_pct: 0.88, date: "2026-08-28", country: "KR", maturity: 3, tenor: "3년" },
    KR1Y: { value: 3.423, change: -0.01, change_pct: -0.29, date: "2026-08-28", country: "KR", maturity: 1, tenor: "1년" },
    KR_ON: { value: 2.988, change: 0, date: "2026-08-27", country: "KR", maturity: 0, tenor: "익일물" },
    MX10Y: { value: 9.204, change: 0.02, change_pct: 0.22, date: "2026-08-28", country: "MX", maturity: 10, tenor: "10년" },
  },
  fx: {
    USD_KRW: { value: 1380.5, change: -1.5, change_pct: -0.11, date: "2026-08-28", label: "달러/원" },
    EUR_USD: { value: 1.1684, change: 0.0005, change_pct: 0.04, date: "2026-08-21", label: "유로/달러" },
  },
};

test("bond-mate 가 이 앱에 없던 만기·국가를 카탈로그에 채운다", () => {
  const win = load(BOND_MATE_CONFIG);
  const catalog = { US10Y: { label: "미국10년물", category: "국채", country: "US", maturity: 10 } };
  const { catalog: merged } = win.mergeBondMate(catalog, {}, SNAPSHOT);

  assert.equal(merged.MX10Y.category, "국채");
  assert.equal(merged.MX10Y.country, "MX");
  assert.equal(merged.MX10Y.label, "멕시코10년");
  assert.equal(merged.KR1Y.maturity, 1);
});

test("시리즈 ID 별칭이 이 앱 코드로 매핑된다", () => {
  const win = load(BOND_MATE_CONFIG);
  const { catalog, dataMap } = win.mergeBondMate({}, {}, SNAPSHOT);

  // bond-mate 의 KR_ON 은 이 앱에서 KOFR 로 불린다.
  assert.ok(catalog.KOFR, "KOFR 로 매핑돼야 한다");
  assert.equal(catalog.KR_ON, undefined);
  assert.equal(dataMap.KOFR.value, "2.99");
});

test("로컬 라이브 값이 있으면 bond-mate 값을 덮어쓰지 않는다", () => {
  // 로컬은 60초 TTL, bond-mate 는 30분 주기 — 같은 코드면 로컬이 더 신선하다.
  const win = load(BOND_MATE_CONFIG);
  const local = { US10Y: { value: "4.75", change: "0.08", change_pct: "1.71%", direction: "up" } };
  const { dataMap } = win.mergeBondMate({}, local, SNAPSHOT);

  assert.equal(dataMap.US10Y.value, "4.75");
});

test("로컬에 값이 없는 코드는 bond-mate 값으로 채운다", () => {
  const win = load(BOND_MATE_CONFIG);
  const local = { US10Y: { value: "", change: "", change_pct: "", direction: "" } };
  const { dataMap } = win.mergeBondMate({}, local, SNAPSHOT);

  assert.equal(dataMap.US10Y.value, "4.73");
  assert.equal(dataMap.US10Y.direction, "up");
  assert.equal(dataMap.US10Y.change, "0.06");
});

test("환율은 통화쌍별 자릿수를 지킨다", () => {
  const win = load(BOND_MATE_CONFIG);
  const { dataMap } = win.mergeBondMate({}, {}, SNAPSHOT);

  assert.equal(dataMap.USD_KRW.value, "1380.50");
  assert.equal(dataMap.USD_KRW.direction, "down");
  // 유로/달러는 소수 4자리라야 의미가 있다.
  assert.equal(dataMap.EUR_USD.value, "1.1684");
});

test("변동이 표시 정밀도보다 작으면 보합으로 둔다", () => {
  const win = load(BOND_MATE_CONFIG);
  const { dataMap } = win.mergeBondMate({}, {}, SNAPSHOT);

  assert.equal(dataMap.KOFR.direction, "");
  assert.equal(dataMap.KOFR.change, "");
});

test("bond-mate 스냅샷이 없으면 입력을 그대로 돌려준다", () => {
  // 배포 전이거나 Pages 가 죽어도 화면은 기존과 똑같이 떠야 한다.
  const win = load(BOND_MATE_CONFIG);
  const catalog = { US10Y: { label: "미국10년물", category: "국채" } };
  const dataMap = { US10Y: { value: "4.75" } };

  for (const empty of [null, undefined, {}]) {
    const result = win.mergeBondMate(catalog, dataMap, empty);
    assert.equal(result.catalog, catalog);
    assert.equal(result.dataMap, dataMap);
    assert.equal(result.merged, false);
  }
});

test("병합은 입력 객체를 변형하지 않는다", () => {
  const win = load(BOND_MATE_CONFIG);
  const catalog = {};
  const dataMap = {};
  win.mergeBondMate(catalog, dataMap, SNAPSHOT);

  assert.deepEqual(catalog, {});
  assert.deepEqual(dataMap, {});
});

test("딥링크는 화면 키를 tab 파라미터로 넘긴다", () => {
  const win = load(BOND_MATE_CONFIG);
  assert.equal(win.bondMateLink("government"), "https://ducklove.github.io/bond-mate/?tab=government");
  assert.equal(win.bondMateLink("fx"), "https://ducklove.github.io/bond-mate/?tab=fx");
});

test("연동 설정이 없으면 링크가 빈 문자열이라 렌더가 생략된다", () => {
  const win = load({});
  assert.equal(win.bondMateBaseUrl(), "");
  assert.equal(win.bondMateLink("fx"), "");
});

test("설정이 없으면 스냅샷을 받으러 가지 않는다", async () => {
  const win = load({});
  let called = false;
  win.fetch = () => { called = true; return Promise.reject(new Error("불려선 안 됨")); };

  assert.equal(await win.loadBondMateSnapshot(), null);
  assert.equal(called, false);
});

test("스냅샷 요청이 실패해도 null 로 조용히 물러난다", async () => {
  const win = load(BOND_MATE_CONFIG);
  win.fetch = () => Promise.resolve({ ok: false, status: 404 });

  assert.equal(await win.loadBondMateSnapshot(true), null);
});

test("같은 (국가, 만기) 자리는 bond-mate 코드로 통일된다", () => {
  // 한국 1년: 이 앱은 통안채(KR_MSB1Y), bond-mate 는 국고채(KR1Y).
  // 둘을 함께 두면 커브가 어느 쪽을 그릴지 객체 순서에 좌우된다.
  const win = load(BOND_MATE_CONFIG);
  const catalog = {
    KR_MSB1Y: { label: "통안채1년", category: "국채", country: "KR", maturity: 1 },
    RU10Y: { label: "러시아10년물", category: "국채", country: "RU", maturity: 10 },
  };
  const dataMap = { KR_MSB1Y: { value: "3.27" }, RU10Y: { value: "12.5" } };
  const { catalog: merged, dataMap: mergedData } = win.mergeBondMate(catalog, dataMap, SNAPSHOT);

  assert.equal(merged.KR_MSB1Y, undefined, "겹치는 로컬 코드는 빠져야 한다");
  assert.equal(mergedData.KR_MSB1Y, undefined);
  assert.equal(merged.KR1Y.label, "한국1년");
  // bond-mate 가 다루지 않는 국가는 로컬 코드가 그대로 남는다.
  assert.equal(merged.RU10Y.label, "러시아10년물");
  assert.equal(mergedData.RU10Y.value, "12.5");
});

test("같은 코드끼리는 지워지지 않는다", () => {
  const win = load(BOND_MATE_CONFIG);
  const catalog = { US10Y: { label: "미국10년물", category: "국채", country: "US", maturity: 10 } };
  const dataMap = { US10Y: { value: "4.75" } };
  const { catalog: merged, dataMap: mergedData } = win.mergeBondMate(catalog, dataMap, SNAPSHOT);

  assert.equal(merged.US10Y.label, "미국10년물");
  assert.equal(mergedData.US10Y.value, "4.75", "로컬 라이브 값이 유지돼야 한다");
});

// --- 도구 허브 '채권·금리' 임베드 (loadBondsView) ---
//
// 값 병합과 달리 이 화면은 bond-mate 원본을 그대로 iframe 으로 가져온다.
// 계약은 세 가지 — 탭 목록은 서버 config(views), URL 은 ?embed=<탭>&theme=,
// 재로딩은 최초 1회 + force + 탭 전환에서만(스크롤 보존).

const BOND_MATE_EMBED_CONFIG = {
  bondMate: {
    baseUrl: "https://ducklove.github.io/bond-mate",
    dataUrl: "https://ducklove.github.io/bond-mate/data/current.json",
    embedUrl: "https://ducklove.github.io/bond-mate/?embed=",
    views: ["overview", "government", "policy", "fx", "credit", "issuance"],
  },
};

function loadWithBondsView(integrations) {
  const win = load(integrations);
  win.document.body.innerHTML = '<a id="bondsOpenLink" href="#"></a>'
    + '<div class="bonds-tabs" id="bondsTabs"></div>'
    + '<div id="bondsContent" class="bonds-embed"></div>';
  return win;
}

test("임베드 탭 목록은 서버 config 의 views 계약을 따른다", () => {
  const win = loadWithBondsView(BOND_MATE_EMBED_CONFIG);
  assert.deepEqual(win.bondMateEmbedViews().map((v) => v.key),
    ["overview", "government", "policy", "fx", "credit", "issuance"]);
  assert.deepEqual(win.bondMateEmbedViews().map((v) => v.label),
    ["개요", "국채", "기준금리", "환율", "신용", "발행"]);

  // 서버가 모르는 키를 주면 무시한다(라벨 없는 탭은 그리지 않는다).
  const partial = loadWithBondsView({ bondMate: { baseUrl: "https://x/", views: ["credit", "nope"] } });
  assert.deepEqual(partial.bondMateEmbedViews().map((v) => v.key), ["credit"]);
});

test("첫 진입은 개요 화면을 임베드하고 탭·새 창 링크를 그린다", () => {
  const win = loadWithBondsView(BOND_MATE_EMBED_CONFIG);
  win.loadBondsView();

  const frame = win.document.querySelector("#bondsContent iframe.bonds-frame");
  assert.ok(frame, "iframe 이 만들어져야 한다");
  assert.ok(frame.src.startsWith("https://ducklove.github.io/bond-mate/?embed=overview&theme=light"),
    `unexpected src: ${frame.src}`);
  assert.equal(win.document.querySelectorAll("#bondsTabs .bonds-tab").length, 6);
  assert.equal(win.document.querySelector("#bondsTabs .bonds-tab.active").textContent, "개요");
  assert.equal(win.document.getElementById("bondsOpenLink").href,
    "https://ducklove.github.io/bond-mate/?tab=overview");
});

test("같은 탭 재진입은 iframe 을 재사용하고, 탭 전환·새로고침만 다시 받는다", () => {
  const win = loadWithBondsView(BOND_MATE_EMBED_CONFIG);
  win.loadBondsView();
  const first = win.document.querySelector("#bondsContent iframe.bonds-frame");

  win.loadBondsView();   // 탭 재방문 — 스크롤 보존을 위해 그대로 둔다
  assert.equal(win.document.querySelector("#bondsContent iframe.bonds-frame"), first);

  win.loadBondsView({ view: "credit" });
  const second = win.document.querySelector("#bondsContent iframe.bonds-frame");
  assert.notEqual(second, first, "탭을 바꾸면 새로 받아야 한다");
  assert.ok(second.src.includes("?embed=credit&"), `unexpected src: ${second.src}`);
  assert.equal(win.document.querySelector("#bondsTabs .bonds-tab.active").textContent, "신용");

  win.loadBondsView({ force: true });
  assert.notEqual(win.document.querySelector("#bondsContent iframe.bonds-frame"), second);
});

test("테마를 바꾸면 임베드도 같은 테마로 다시 로드된다", () => {
  const win = loadWithBondsView(BOND_MATE_EMBED_CONFIG);
  win.loadBondsView({ view: "policy" });
  win.document.documentElement.setAttribute("data-theme", "dark");
  win.syncBondsFrameTheme();

  const frame = win.document.querySelector("#bondsContent iframe.bonds-frame");
  assert.ok(frame.src.includes("?embed=policy&theme=dark"), `unexpected src: ${frame.src}`);
});

test("bond-mate 설정이 없으면 안내만 남기고 iframe 을 만들지 않는다", () => {
  const win = loadWithBondsView({});
  win.loadBondsView();

  assert.equal(win.document.querySelector("#bondsContent iframe.bonds-frame"), null);
  assert.match(win.document.getElementById("bondsContent").textContent, /찾지 못했습니다/);
});
