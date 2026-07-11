"""관리자 페이지 + 도구(Labs: 스크리너·마스터스) 구조 계약."""
# 구 test_frontend_structure.py(1,500줄 단일 파일)를 2026-07-07 화면별로 분할.
# 새 화면 계약 테스트는 해당 화면 파일에 추가한다. 공용 경로/상수/헬퍼는
# tests/_frontend_structure.py 에 있다.
import json
import re

from _frontend_structure import ADMIN_SPLIT_FILES, JS, ROOT, STATIC, _all_css


def test_admin_page_loads_admin_split_scripts_in_contract_order():
    html = (STATIC / "admin.html").read_text(encoding="utf-8")

    positions = []
    for name in ADMIN_SPLIT_FILES:
        marker = f'/js/{name}'
        pos = html.find(marker)
        assert pos != -1, f"{name} is not loaded by admin.html"
        positions.append(pos)
    assert positions == sorted(positions), "admin split script order changed"
    # 공용 헬퍼는 utils.js 단일 소스(apiFetchJson/reportApiError/showToast) —
    # 모든 admin 스크립트보다 먼저 로드된다. 인라인 apiFetch 사본은 드리프트
    # (timeoutMs no-op, reportApiError ReferenceError)를 낳아 금지.
    utils_pos = html.find('/js/utils.js')
    assert utils_pos != -1, "admin.html must load utils.js"
    assert utils_pos < positions[0]
    assert "async function apiFetch(" not in html, "inline apiFetch copy must not come back"
    # 부트스트랩 호출(loadAdminView)은 모든 스크립트가 로드된 뒤에 실행된다.
    assert positions[-1] < html.find("loadAdminView();")

def test_admin_split_files_keep_feature_homes():
    admin = (JS / "admin.js").read_text(encoding="utf-8")
    observability = (JS / "admin-observability.js").read_text(encoding="utf-8")
    linked = (JS / "admin-linked-projects.js").read_text(encoding="utf-8")

    # 본체: 부트스트랩 오케스트레이션, AI 운영 관리, 공용 헬퍼.
    assert "async function loadAdminView()" in admin
    assert "function _renderAdmin(" in admin
    assert "function _renderAiConfigSection(" in admin
    assert "async function saveAiKey()" in admin
    assert "async function saveAiModels()" in admin
    assert "function _esc(" in admin
    assert "function _adminInputStyle()" in admin
    # 관측성: 배치/사용자/DB/이벤트/HTTP 패널 + 5초 라이브 갱신 +
    # 수동 작업 실행 + 위키 진단 폼.
    # 배포/서버 카드는 admin.js 운영 콘솔 KPI 로 대체되어 2026-07-11 제거 —
    # 호출부 없는 죽은 코드가 다시 생기지 않는다.
    assert "function _renderDeployCard(" not in observability
    assert "function _renderServerCard(" not in observability
    assert "function _startLiveUpdates()" in observability
    assert "async function _updateLiveStats()" in observability
    assert "function _renderBatchSection(" in observability
    assert "function _renderUsersSection(" in observability
    assert "function _renderDbSection(" in observability
    assert "function _renderEventsSection(" in observability
    assert "function _renderHttpMetricsSection(" in observability
    assert "function _renderSubsystemSummary(" in observability
    # 데이터 품질 카드: event-summary 의 data_quality(check_summary) 한 행으로 렌더.
    assert "function _renderDataQualitySection(" in observability
    assert "_renderUsersSection(users)" in admin
    assert "_renderDataQualitySection(summary.data_quality)" in admin
    assert admin.index("_renderUsersSection(users)") < admin.index("_renderDataQualitySection(summary.data_quality)")
    assert 'details class="admin-section admin-collapsible" id="httpMetricsSection"' in observability
    assert 'details class="admin-section admin-collapsible" id="dbStatsSection"' in observability
    assert 'details class="admin-section admin-collapsible" id="eventsSection"' in observability
    assert 'details class="admin-section admin-collapsible" id="dataSyncSection"' in linked
    assert "function _adminPortfolioUrl(" in observability
    assert "https://192.168.68.67:3691/api/admin/users/" in observability
    assert ">포트폴리오</a>" in observability
    assert "내부망 포트폴리오" not in observability
    assert "async function triggerJob(" in observability
    assert "async function runWikiDiag()" in observability
    # 연결 프로젝트 config + 우선주/해외 배당 관리.
    assert "function _renderLinkedProjectConfigSection(" in linked
    assert "async function saveLinkedProjectConfig(" in linked
    assert "async function saveGoldGapConfig()" in linked
    assert "async function saveHoldingConfigItem()" in linked
    assert "async function savePreferredConfigItem()" in linked
    assert "function _renderDataSyncSection()" in linked
    assert "async function refreshPreferredDividends()" in linked
    assert "async function refreshForeignDividends()" in linked
    assert "async function submitForeignDividend()" in linked
    assert "function prefillPreferredConfigFromDividend(" in linked
    # 오류 보고는 reportApiError 헬퍼 경유를 유지한다(765d8a5).
    assert "reportApiError(e, '실행 요청');" in observability
    assert "reportApiError(e, '삭제');" in linked
    # 원시 다이얼로그(alert/confirm/prompt) 금지 — showToast 와 관리형 모달
    # 헬퍼(adminConfirm/adminPromptDate, admin.js) 경유(2026-07-11).
    assert "function adminConfirm(" in admin
    assert "function adminPromptDate(" in admin
    for src in (admin, observability, linked):
        assert not re.search(r"(?<!\w)(alert|confirm|prompt)\(", src)

def test_admin_split_files_stay_below_maintenance_ceiling():
    for name in ADMIN_SPLIT_FILES:
        lines = (JS / name).read_text(encoding="utf-8").splitlines()
        assert len(lines) < 1000, f"{name} grew to {len(lines)} lines; split it before extending"

def test_screener_labs_view_is_wired_as_deep_linkable_panel():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    shell = (JS / "portfolio-shell.js").read_text(encoding="utf-8")
    app_main = (JS / "app-main.js").read_text(encoding="utf-8")
    screener = (JS / "screener.js").read_text(encoding="utf-8")
    styles = _all_css()

    # 뷰 컨테이너는 다른 Labs 뷰(insightsView) 다음에 산다.
    assert 'id="screenerView"' in html
    assert html.find('id="insightsView"') < html.find('id="screenerView"')
    # Labs 허브(/labs) 카드로 진입한다(인사이트 보드 카드와 같은 패턴).
    assert 'href="/screener"' in html
    assert '밸류 스크리너' in html
    # 스크립트는 insights.js 다음, app-main.js 이전에 로드된다(계약 순서).
    assert html.find("./js/insights.js") < html.find("./js/screener.js")
    assert html.find("./js/screener.js") < html.find("./js/app-main.js")
    # SPA 직접 URL — 서버가 index.html 로 서빙(core/static_routes.py SPA_PATHS).
    assert "/screener" in (STATIC / ".." / "core" / "static_routes.py").resolve().read_text(encoding="utf-8")

    # switchView 가 screener 뷰를 토글하고 진입 시 loadScreener 를 부른다.
    assert "screenerView" in shell
    assert "view === 'screener'" in shell
    assert "typeof loadScreener === 'function'" in shell
    # 경로↔뷰 매핑은 portfolio-shell.js 의 PF_PATH_TO_VIEW 하나를 공유하며
    # (app-main.js 의 최초 라우팅과 popstate 복원이 이를 재사용), /screener → 'screener' 를 갖는다.
    assert "'/screener': 'screener'" in shell
    assert "PF_PATH_TO_VIEW" in app_main

    # 기능 홈: spec 로드, 필터 렌더, 실행/페이징은 screener.js.
    assert "async function loadScreener(" in screener
    assert "/api/screener/spec" in screener
    assert "/api/screener/run" in screener
    assert "function _renderScreenerFilters(" in screener
    assert "function _runScreener(" in screener
    assert "function _renderScreenerResults(" in screener
    # 독립 뷰이므로 portfolio-render.js 전역(fmtKrw/fmtPct)에 의존하지 않는다.
    assert "function fmtKrw(" not in screener
    assert "function fmtPct(" not in screener
    # 커버리지 안내(검색 대상 = finance-pi full-universe 스냅샷)가 표시된다.
    assert "screenerCoverage" in html

    # 스타일: 페이지/필터 그리드/테이블/페이저.
    assert ".screener-page" in styles
    assert ".screener-filters" in styles
    assert ".screener-table" in styles
    assert ".screener-pager" in styles

def test_masters_labs_view_is_wired_as_deep_linkable_panel():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()
    shell = (JS / "portfolio-shell.js").read_text(encoding="utf-8")
    masters = (JS / "masters.js").read_text(encoding="utf-8")
    static_routes = (ROOT / "core" / "static_routes.py").read_text(encoding="utf-8")

    # 뷰 컨테이너는 스크리너 다음, main 닫힘 전에 산다.
    assert 'id="mastersView"' in html
    assert html.find('id="screenerView"') < html.find('id="mastersView"')
    # Labs 허브(/labs) 카드로 진입한다(스크리너/인사이트 카드와 같은 패턴).
    assert 'href="/masters"' in html
    # 스크립트는 screener.js 다음, 부트스트랩(app-main.js) 앞.
    assert html.find("./js/screener.js") < html.find("./js/masters.js")
    assert html.find("./js/masters.js") < html.find("./js/app-main.js")
    # 직접 URL(/masters) 새로고침이 SPA 로 라우트된다.
    assert '"/masters"' in static_routes

    # switchView 가 masters 뷰를 토글하고 진입 시 loadMasters 를 부른다.
    assert "mastersView" in shell
    assert "view === 'masters'" in shell
    assert "masters: '/masters'" in shell
    assert "'/masters': 'masters'" in shell
    # 도구 허브 하위 화면 — 상단 탭은 "도구"가 활성으로 남는다.
    assert "'labs', 'nps', 'insights', 'screener', 'masters'" in shell

    # 기능 홈: 카탈로그 로드, 카드/상세/비교표, 성향 시뮬레이션은 masters.js.
    assert "async function loadMasters(" in masters
    assert "/api/masters/strategies" in masters
    assert "/api/masters/simulate" in masters
    assert "function _renderMastersCards(" in masters
    assert "function _renderMasterDetail(" in masters
    assert "function _renderMastersCompare(" in masters
    assert "async function _runMastersSimulation(" in masters
    assert "function _mastersAllocationBar(" in masters
    # 시뮬레이션은 대가 1명 지정 + 상품(ETF) 단위 실행 포트폴리오.
    assert "mastersSimStrategy" in masters
    assert "mastersSimAmount" in masters
    assert "function _mastersPortfolioTable(" in masters
    assert "function _mastersSelectMaster(" in masters
    # 대가의 시선 포트폴리오 진단(LLM) — 로그인 필수, 장타임아웃, 마크다운 폴백.
    assert 'id="mastersReviewControls"' in html
    assert 'id="mastersReviewResult"' in html
    assert "/api/masters/review" in masters
    assert "async function _runMastersReview(" in masters
    assert "function _renderMastersReviewControls(" in masters
    assert "function _mastersRenderMarkdown(" in masters
    assert "timeoutMs: 180000" in masters
    # 전략 내용은 서버 카탈로그가 단일 소스 — JS 에 전략 본문을 하드코딩하지 않는다.
    assert "워런 버핏" not in masters
    assert "올웨더" not in masters

    # 교육·참고용 고지: 카탈로그 데이터가 단일 소스이고 화면 컨테이너가 존재한다.
    catalog = json.loads((ROOT / "data" / "investment_masters.json").read_text(encoding="utf-8"))
    assert "투자 조언이 아닌 참고용" in catalog["disclaimer"]
    assert 'id="mastersDisclaimer"' in html

    assert ".masters-page" in styles
    assert ".masters-cards" in styles
    assert ".masters-compare-table" in styles
    assert ".masters-sim-card" in styles
    assert ".masters-bar" in styles
    assert ".masters-portfolio-table" in styles
    assert ".masters-review-md" in styles

def test_masters_feature_files_stay_below_maintenance_ceiling():
    lines = (JS / "masters.js").read_text(encoding="utf-8").splitlines()
    assert len(lines) < 1000, f"masters.js grew to {len(lines)} lines; split it before extending"
