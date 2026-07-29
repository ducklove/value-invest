// jsdom behavior tests for the managed-modal scroll lock.
//
// 재발 방지 대상: 모달을 연 채 화면(뷰)을 전환하면 body 의 스크롤 잠금이 남아
// 새 화면에서 마우스 휠도 스크롤바도 죽던 버그. 모달 마크업은 자기 화면 컨테이너
// 안에 있어(#pfAssetInsightModal ⊂ #portfolioView) 화면과 함께 눈에서는 사라지는
// 탓에, 증상이 "종목분석 탭이 스크롤되지 않는다" 로만 보였다.
//
// 재현 경로: 포트폴리오 → 종목 인사이트 모달 → 모달 안 "분석 화면" 버튼
//            → switchView('analysis') (모달을 닫지 않음) → 스크롤 사망.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const __dirname = dirname(fileURLToPath(import.meta.url));
const UTILS_SRC = readFileSync(
  join(__dirname, "..", "..", "static", "js", "utils.js"),
  "utf8",
);

// 실제 화면과 같은 중첩(뷰 컨테이너 안에 모달)을 만들어 둔다.
function loadUtils() {
  const dom = new JSDOM(
    `<!doctype html><html><body>
       <div id="portfolioView">
         <div class="pf-modal-overlay" id="pfAssetInsightModal" style="display:none;">
           <button class="pf-modal-close">닫기</button>
         </div>
       </div>
       <div id="analysisView" style="display:none;"></div>
       <div class="chart-modal-overlay" id="chartModal" style="display:none;">
         <button class="chart-modal-close">닫기</button>
       </div>
     </body></html>`,
    { runScripts: "dangerously", url: "https://app.example.com/" },
  );
  const script = dom.window.document.createElement("script");
  script.textContent = UTILS_SRC;
  dom.window.document.body.appendChild(script);
  return dom.window;
}

test("모달을 열면 body 스크롤이 잠기고 닫으면 원래대로 풀린다", () => {
  const w = loadUtils();
  const modal = w.document.getElementById("pfAssetInsightModal");

  assert.equal(w.document.body.style.overflow, "");
  w.openManagedModal(modal, { initialFocus: ".pf-modal-close" });
  assert.equal(w.document.body.style.overflow, "hidden");
  w.closeManagedModal(modal);
  assert.equal(w.document.body.style.overflow, "");
});

test("모달을 닫지 않고 화면을 바꿔도 스크롤 잠금이 남지 않는다", () => {
  const w = loadUtils();
  const modal = w.document.getElementById("pfAssetInsightModal");

  w.openManagedModal(modal, { initialFocus: ".pf-modal-close" });
  assert.equal(w.document.body.style.overflow, "hidden");

  // switchView 가 하는 일: 이전 화면을 숨기고 모달 상태를 되감는다.
  w.document.getElementById("portfolioView").style.display = "none";
  w.closeAllManagedModals();

  assert.equal(w.document.body.style.overflow, "", "휠·스크롤바가 살아 있어야 한다");
  assert.equal(modal.style.display, "none");
  assert.equal(modal.hasAttribute("data-managed-modal-open"), false);
});

test("여러 모달이 쌓여 있어도 화면 전환에서 전부 되감긴다", () => {
  const w = loadUtils();
  const insight = w.document.getElementById("pfAssetInsightModal");
  const chart = w.document.getElementById("chartModal");

  w.openManagedModal(insight, { initialFocus: ".pf-modal-close" });
  w.openManagedModal(chart, { initialFocus: ".chart-modal-close" });
  assert.equal(w.document.body.style.overflow, "hidden");

  w.closeAllManagedModals();

  assert.equal(w.document.body.style.overflow, "");
  assert.equal(insight.style.display, "none");
  assert.equal(chart.style.display, "none");
});

test("같은 모달을 닫지 않고 다시 열어도 '원래 overflow' 가 hidden 으로 굳지 않는다", () => {
  const w = loadUtils();
  const chart = w.document.getElementById("chartModal");

  // 차트 카드를 바꿔가며 확대하면 같은 #chartModal 을 연달아 연다.
  w.openManagedModal(chart, { initialFocus: ".chart-modal-close" });
  w.openManagedModal(chart, { initialFocus: ".chart-modal-close" });
  assert.equal(w.document.body.style.overflow, "hidden");

  w.closeManagedModal(chart);
  assert.equal(w.document.body.style.overflow, "", "재오픈이 잠금 해제를 막으면 안 된다");
});

test("closeAllManagedModals 는 열린 모달이 없을 때 아무것도 건드리지 않는다", () => {
  const w = loadUtils();
  w.document.body.style.overflow = "auto";
  w.closeAllManagedModals();
  assert.equal(w.document.body.style.overflow, "auto");
});
