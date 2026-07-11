"""앱 셸 구조 계약 — CSS 로드 순서·시황 테이프·PWA·접근성·모바일 셸·뷰 전환·알림 진입점."""
# 구 test_frontend_structure.py(1,500줄 단일 파일)를 2026-07-07 화면별로 분할.
# 새 화면 계약 테스트는 해당 화면 파일에 추가한다. 공용 경로/상수/헬퍼는
# tests/_frontend_structure.py 에 있다.
import json
import re

from _frontend_structure import CSS_SPLIT_FILES, JS, STATIC, _all_css


def test_css_split_files_load_in_contract_order():
    for html_name, prefix in (("index.html", "./css/"), ("admin.html", "/css/")):
        html = (STATIC / html_name).read_text(encoding="utf-8")
        positions = []
        for name in CSS_SPLIT_FILES:
            needle = f'href="{prefix}{name}"'
            assert needle in html, f"{html_name}: {needle} 링크 누락"
            positions.append(html.index(needle))
        assert positions == sorted(positions), f"{html_name}: CSS 로드 순서가 계약과 다름"
    assert not (STATIC / "styles.css").exists(), "styles.css 는 분할로 제거됨 — 부활 금지"

def test_market_tape_is_bottom_frame_outside_main():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()

    # 하단 탭바(.mobile-tabbar)와 시황 테이프는 모두 .main 바깥(아래)에 위치하고,
    # 차트 모달보다 앞선다. 탭바는 main 의 닫는 </div> 바로 뒤에 온다.
    main_open = html.index('<main class="main" id="mainContent" tabindex="-1">')
    tabbar_pos = html.index('id="mobileTabbar"')
    tape_pos = html.index('id="marketTape"')
    chart_modal_pos = html.index('id="chartModal"')
    assert main_open < tabbar_pos < tape_pos < chart_modal_pos
    assert "</main>\n\n<div class=\"profile-modal-overlay\"" in html
    assert "</div>\n\n<nav class=\"mobile-tabbar\"" in html
    assert "position: fixed;" in styles
    assert "bottom: 0;" in styles
    assert "border-top: 1px solid var(--border);" in styles
    assert "padding-bottom: 52px;" in styles

def test_market_tape_down_events_have_blue_alert_background():
    styles = _all_css()

    assert ".market-tape-item.breaking.down" in styles
    assert ".market-tape-item.alert.down" in styles
    assert "rgba(37, 99, 235, 0.08)" in styles
    assert "#1d4ed8" in styles

def test_stylesheet_defines_all_css_variables_it_uses():
    css = _all_css()
    used = set(re.findall(r"var\(--([A-Za-z0-9_-]+)", css))
    defined = set(re.findall(r"--([A-Za-z0-9_-]+)\s*:", css))
    assert used - defined == set()

def test_pwa_manifest_declares_installable_app():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    manifest = json.loads((STATIC / "manifest.webmanifest").read_text(encoding="utf-8"))

    # index.html wires the manifest + theme color + iOS raster touch icon.
    assert '<link rel="manifest" href="./manifest.webmanifest">' in html
    assert '<meta name="theme-color" content="#2563eb">' in html
    assert '<link rel="apple-touch-icon" href="./static/icon-640.jpg">' in html

    # Installability contract: standalone display from '/', Korean app name.
    assert manifest["lang"] == "ko"
    assert manifest["name"] == "Value Compass — 가치투자 나침반"
    assert manifest["short_name"] == "Value Compass"
    assert manifest["start_url"] == "/"
    assert manifest["scope"] == "/"
    assert manifest["display"] == "standalone"
    # Colors mirror styles.css :root (--bg / --primary).
    assert manifest["background_color"] == "#f5f5f5"
    assert manifest["theme_color"] == "#2563eb"
    icons = {icon["src"]: icon for icon in manifest["icons"]}
    assert icons["/favicon.svg"]["type"] == "image/svg+xml"
    assert icons["/favicon.svg"]["sizes"] == "any"
    assert icons["/static/icon-640.jpg"]["type"] == "image/jpeg"
    assert icons["/static/icon-640.jpg"]["sizes"] == "640x640"
    # maskable 설치 아이콘 — 풀블리드 배경 + safe zone(반지름 40%) 안 콘텐츠.
    assert icons["/static/icon-maskable.svg"]["type"] == "image/svg+xml"
    assert icons["/static/icon-maskable.svg"]["purpose"] == "maskable"
    maskable = (STATIC / "icon-maskable.svg").read_text(encoding="utf-8")
    assert '<rect width="64" height="64"' in maskable, "maskable 배경은 풀블리드(rx 없는 전체 rect)"
    assert "scale(0.9)" in maskable, "콘텐츠는 safe zone 안으로 축소"

def test_base_font_stack_includes_korean_fallbacks():
    # Windows/구형 macOS 에서 시스템 UI 폰트 다음 폴백이 브라우저 기본(굴림/바탕
    # 계열)으로 빠지지 않게 한글 폰트를 명시한다. 시스템 폰트 뒤·sans-serif 앞.
    base = (STATIC / "css" / "base.css").read_text(encoding="utf-8")
    assert (
        "--font-base: -apple-system, BlinkMacSystemFont, 'Segoe UI', "
        "'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif;"
    ) in base

def test_reduced_motion_rules_are_merged_into_single_block():
    # mobile-shell.css 에 같은 @media 블록이 2회 있던 것을 통합 — 전역 * 선언은
    # 동일했고 블록별 고유 선택자는 합집합으로 보존한다.
    shell_css = (STATIC / "css" / "mobile-shell.css").read_text(encoding="utf-8")
    assert shell_css.count("@media (prefers-reduced-motion: reduce)") == 1
    assert ".market-tape-track { animation: none !important; }" in shell_css
    assert '.chart-dot-pulse, [style*="chartDotPulse"] { animation: none !important; }' in shell_css
    assert ".flash-update, .market-tape.flash-update::after { animation: none !important; }" in shell_css

def test_mobile_sidebar_overrides_live_only_in_superset_media_block():
    # mobile-overrides.css 의 body/사이드바 규칙은 (≤900px)+(가로 짧은 화면)
    # 복합 미디어쿼리 블록 한 곳에만 산다 — ≤900px 블록의 중복은 제거됨.
    css = (STATIC / "css" / "mobile-overrides.css").read_text(encoding="utf-8")
    assert css.count("body.mobile-auth .sidebar { display: none; }") == 1
    assert css.count("border-right: none;") == 1
    assert css.count("width: 100%;\n    border-right: none;") == 1

def test_pwa_service_worker_keeps_conservative_cache_contract():
    sw = (STATIC / "sw.js").read_text(encoding="utf-8")
    app_main = (JS / "app-main.js").read_text(encoding="utf-8")

    # Registration is feature-detected and non-fatal (app-main.js tail).
    assert "'serviceWorker' in navigator" in app_main
    assert "navigator.serviceWorker.register('/sw.js')" in app_main
    assert ".catch(" in app_main

    # The SW must never break server-side ?v= cache busting:
    # HTML is network-first and never cached; /api/* is never intercepted.
    assert "NO HTML caching" in sw
    assert "network-only" in sw
    assert "request.mode === 'navigate'" in sw
    assert "url.pathname.startsWith('/api/')) return;" in sw
    # cache-first applies only to immutable ?v=-stamped URLs + manifest/icons.
    assert "url.searchParams.has('v')" in sw
    assert "isVersionStampedAsset(url) || isPrecachedShellExtra(url)" in sw
    assert "'/manifest.webmanifest'" in sw
    assert "'/favicon.svg'" in sw
    assert "'/static/icon-640.jpg'" in sw
    # No offline app shell — only a tiny inline navigation fallback.
    assert "OFFLINE_HTML" in sw
    assert "status: 503" in sw
    # Versioned cache name + activate-time cleanup of old caches.
    assert "const CACHE_NAME = 'vc-static-v" in sw
    assert "keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))" in sw
    # Guard against strategy regressions: no stale-while-revalidate, no
    # caching inside the navigation branch.
    assert "staleWhileRevalidate" not in sw
    navigate_branch = sw.split("request.mode === 'navigate'", 1)[1]
    assert "cache.put" not in navigate_branch

def test_status_and_loading_feedback_are_announced_to_assistive_tech():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    analysis = (JS / "analysis.js").read_text(encoding="utf-8")

    def tag_for(node_id: str) -> str:
        match = re.search(rf'<[^>]*\bid="{re.escape(node_id)}"[^>]*>', html)
        assert match, f"{node_id} tag missing"
        return match.group(0)

    for node_id in (
        "dailyMarketStatus",
        "econCalContent",
        "preferenceStatus",
        "wikiQaStatus",
        "filingReviewStatus",
        "reportsLoading",
        "pfPeriodReportStatus",
        "npsContent",
    ):
        tag = tag_for(node_id)
        assert 'role="status"' in tag
        assert 'aria-live="polite"' in tag

    loading = tag_for("loadingOverlay")
    assert 'role="status"' in loading
    assert 'aria-live="polite"' in loading
    assert 'aria-busy="false"' in loading
    progress = tag_for("progressBar")
    assert 'role="progressbar"' in progress
    assert 'aria-label="분석 진행률"' in progress
    assert 'aria-valuemin="0"' in progress
    assert 'aria-valuemax="100"' in progress
    assert 'aria-valuenow="0"' in progress
    assert "function setAnalysisProgress(percent)" in analysis
    assert "progressBar.setAttribute('aria-valuenow', String(value));" in analysis
    assert "overlay.setAttribute('aria-busy', 'true');" in analysis
    assert "overlay.setAttribute('aria-busy', 'false');" in analysis

def test_app_shell_has_main_landmark_h1_and_skip_link():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()
    utils = (JS / "utils.js").read_text(encoding="utf-8")

    assert '<a class="skip-link" href="#mainContent">본문으로 바로가기</a>' in html
    assert '<main class="main" id="mainContent" tabindex="-1">' in html
    assert '<h1 class="sr-only">Value Compass</h1>' in html
    assert "</main>" in html
    assert ".sr-only" in styles
    assert ".skip-link" in styles
    assert ".skip-link:focus-visible" in styles
    assert "function initSkipLink()" in utils
    assert "function focusMainContent()" in utils
    assert "document.addEventListener('DOMContentLoaded', initSkipLink);" in utils
    assert "skipLink.addEventListener('keydown'" in utils

def test_mobile_simple_mode_is_read_only_and_nav_uses_bottom_tabbar():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()
    shell = (JS / "portfolio-shell.js").read_text(encoding="utf-8")
    auth = (JS / "auth.js").read_text(encoding="utf-8")

    assert 'id="pfSimpleToggle"' in html
    assert "function pfSyncMobileFixedView()" in shell
    assert "function switchView(view, options = {})" in shell
    assert "pfSwitchTab('holdings')" in shell
    assert "pfSyncMobileFixedView();" in auth

    # 하단 탭바 도입으로 로그인 모바일을 포트폴리오에 고정하던 lock 은 해제됐다.
    # _mobileFixedView 는 항상 null 을 반환하고, 첫 진입 기본값만 initApp 에서 처리한다.
    assert "function _mobileFixedView()" in shell
    assert "return currentUser ? 'portfolio' : null;" not in shell
    # 모바일 내비게이션은 하단 탭바(.mnav-btn)이며 switchView 가 상단/하단 탭을 함께 동기화.
    assert 'id="mobileTabbar"' in html
    assert 'class="mnav-btn' in html
    assert ".nav-btn, .mnav-btn" in shell

    # 간편 모드는 편집/부가 UI 를 감춰 읽기 전용으로 동작한다.
    assert "body.pf-mobile-simple .pf-tab[data-tab=\"performance\"]" in styles
    assert "body.pf-mobile-simple .pf-filter-bar" in styles
    assert "display: none !important;" in styles
    assert "body.pf-mobile-simple .pf-add-bar" in styles
    assert "body.pf-mobile-simple .pf-col-toggle-wrap" in styles
    assert "body.pf-mobile-simple .pf-col-act" in styles

def test_switch_view_syncs_browser_history():
    shell = (JS / "portfolio-shell.js").read_text(encoding="utf-8")
    app_main = (JS / "app-main.js").read_text(encoding="utf-8")

    # switchView 는 브라우저 히스토리를 쓴다(옛날엔 DOM 만 바뀌고 URL은 그대로라
    # 뒤로가기가 앱을 이탈시켰다 — UX 감사 P1②). skipHistory 로만 우회한다.
    assert "if (!options.skipHistory)" in shell
    assert "history.pushState({ pfView: view }, '', path)" in shell

    # 최초 진입 라우팅(이미 그 URL에 있음)과 popstate 복원(브라우저가 이미 URL을 바꿈)은
    # 둘 다 재-push 하면 안 되므로 skipHistory 를 명시한다.
    assert "switchView(viewFromPath, { skipHistory: true })" in app_main
    assert "switchView('analysis', { skipHistory: true })" in app_main
    assert "switchView('portfolio', { skipHistory: true })" in app_main

    # popstate(뒤로/앞으로가기) 리스너가 등록되어 URL을 다시 화면 상태로 되돌린다.
    assert "addEventListener('popstate'" in app_main
    assert "PF_PATH_TO_VIEW[path] || 'investing'" in app_main

def test_mobile_search_shows_recent_and_starred_chips_on_empty_focus():
    search = (JS / "search.js").read_text(encoding="utf-8")
    auth = (JS / "auth.js").read_text(encoding="utf-8")
    styles = _all_css()

    # ≤900px는 사이드바(최근 검색/관심 목록)가 숨겨져 재진입 경로가 없다 — 검색창을
    # 빈 채로 포커스하면 같은 데이터를 드롭다운 칩으로 보여준다(UX 감사 P1③).
    # isCompactMobileViewport 로 데스크톱에서는 켜지지 않게 막는다.
    assert "function showRecentStarredSearchPanel()" in search
    assert "if (!isCompactMobileViewport()) return;" in search
    assert "searchInput.addEventListener('focus'" in search
    # 최근 검색은 이미 로드돼 있는 recentListItems 를 재사용 — 별도 fetch 없음.
    assert "recentListItems.slice(0, 8)" in search
    # 관심 목록은 sidebar 탭 전환(desktop 전용)에 의존하지 않고 직접 가져와 캐시한다.
    assert "/api/cache/list?tab=starred" in search
    assert "_searchStarredChipsCache" in search
    # 관심종목 토글 직후 캐시를 무효화해 다음에 열 때 최신 상태를 반영한다.
    assert "invalidateSearchStarredChipsCache" in search
    assert "invalidateSearchStarredChipsCache" in auth
    assert ".dropdown-section-label" in styles

def test_my_alerts_unify_stock_and_portfolio_rules_behind_the_profile_modal():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    alerts = (JS / "portfolio-alerts.js").read_text(encoding="utf-8")

    # 알림 설정이 종목분석/포트폴리오/경제캘린더 3곳에 흩어져 있고 전체를 보는 곳이
    # 없었다(UX 감사 P2⑧). pfAlertsModal 은 원래 stock_code 필터 없이 /alerts 를
    # 불러와 종목+포트폴리오 규칙을 이미 함께 보여주고 있었으므로, 새 렌더러 대신
    # 이 모달을 프로필에서도 열 수 있게 만 한다.
    assert 'onclick="closeProfileModal(); pfOpenAlerts();"' in html
    assert 'class="profile-alerts-link"' in html

    # pfAlertsModal 이 portfolioView 안에 있으면 다른 탭에서는 조상의 display:none
    # 때문에 열리지 않는다 — 다른 전역 모달(stockAlertModal 등)처럼 최상위로
    # 옮겨졌는지 순서로 확인한다.
    portfolio_view_close = html.find('</div><!-- /portfolioView -->')
    alerts_modal = html.find('id="pfAlertsModal"')
    assert portfolio_view_close != -1 and alerts_modal != -1
    assert alerts_modal > portfolio_view_close, "pfAlertsModal must live outside #portfolioView"

    # 경제캘린더 구독은 event_id만 저장돼 여기서 개별 나열이 불가능하므로 건수
    # 요약 + 캘린더 섹션 바로가기로 대체한다.
    assert 'id="pfAlertCalendarSummary"' in html
    assert "async function pfAlertsLoadCalendarSummary()" in alerts
    assert "pfAlertsApi('/calendar')" in alerts
    assert "function pfAlertsGoToCalendar()" in alerts
    assert "econCalSection" in alerts

def test_ux_polish_theme_wiki_badge_and_nps_reload_and_tab_bar_divider():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()
    search = (JS / "search.js").read_text(encoding="utf-8")
    shell = (JS / "portfolio-shell.js").read_text(encoding="utf-8")

    # P3: 테마 버튼 aria-label (아이콘만 있고 접근 가능한 이름이 없었다).
    assert '<button class="theme-toggle" onclick="toggleTheme()" title="테마 전환" aria-label="테마 전환">' in html

    # P3: 저장된 선택이 없으면 OS 다크모드 설정을 따른다.
    assert "prefers-color-scheme: dark" in search

    # P3: 위키 누적 수집량 배지는 운영 지표라 헤더(전 사용자 노출)에서
    # 프로필 모달 하단으로 내려갔다 — id는 유지해 loadWikiStats() 무변경.
    # main-nav 바로 다음(옛 헤더 위치)엔 더는 없고, profileModal 안에만 한 번 있다.
    assert html.count('id="wikiStats"') == 1
    assert 'class="wiki-stats profile-wiki-stats" id="wikiStats"' in html
    assert html.find('id="profileModal"') < html.find('id="wikiStats"') < html.find('id="stockAlertModal"')

    # P3: 국민연금 iframe — 세션 내 1회 로드 + 수동 새로고침 버튼.
    assert 'onclick="loadNpsView({ force: true })"' in html
    assert "function loadNpsView({ force = false } = {})" in shell
    assert "if (existing && !force) return;" in shell

    # P2⑨: 뷰 전환 탭(보유종목/심층 분석)과 표시 옵션(액션/간편/통화) 사이의 구분선.
    assert ".pf-action-toggle::before" in styles
