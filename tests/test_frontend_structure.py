import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static"
JS = STATIC / "js"


PORTFOLIO_SPLIT_FILES = [
    "portfolio-shell.js",
    "portfolio-data.js",
    "portfolio-order.js",
    "portfolio-sparklines.js",
    "portfolio-render.js",
    "portfolio-action-board.js",
    "portfolio-add-search.js",
    "portfolio-actions.js",
    "portfolio-insights.js",
    "portfolio-groups-market.js",
    "portfolio-ai.js",
    "portfolio-reports.js",
    "portfolio-performance.js",
    "portfolio-risk.js",
    "portfolio-rebalance.js",
    "portfolio-dividends-calendar.js",
    "portfolio-journal.js",
    "portfolio-trends-benchmark.js",
    "portfolio-trends.js",
    "portfolio-trends-group-weight.js",
    "portfolio-group-composition.js",
    "portfolio-cashflows.js",
    "portfolio-tag-summary.js",
    "portfolio-events.js",
]

# Dependencies-first: charts and filings define globals (chart state,
# allReports, loadWiki, ...) that analysis.js orchestrates at runtime.
ANALYSIS_SPLIT_FILES = [
    "analysis-charts.js",
    "analysis-filings.js",
    "analysis.js",
]

# Dependencies-first: admin.js provides the shared helpers (_esc,
# _adminInputStyle) and bootstrapping; the panel modules load after it.
ADMIN_SPLIT_FILES = [
    "admin.js",
    "admin-charts.js",
    "admin-observability.js",
    "admin-linked-projects.js",
]


def test_economic_calendar_section_and_script_present():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    js = (JS / "economic-calendar.js").read_text(encoding="utf-8")
    dashboard = (JS / "market-dashboard.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    # 투자정보 뷰 안, 금일 시황 섹션 다음의 풀폭 섹션으로 산다.
    assert 'id="econCalSection"' in html
    assert 'id="econCalContent"' in html
    assert "./js/economic-calendar.js" in html
    assert html.find('id="dailyMarketSection"') < html.find('id="econCalSection"')
    assert html.find('id="econCalSection"') < html.find("<!-- /investingView -->")
    # 투자정보 대시보드 로드 시 함께 로드된다.
    assert "if (typeof loadEconomicCalendar === 'function') loadEconomicCalendar();" in dashboard
    assert "async function loadEconomicCalendar()" in js
    assert ".econ-cal-section" in styles
    assert ".ec-row" in styles


def test_economic_calendar_filter_is_dates_plus_per_importance_settings():
    js = (JS / "economic-calendar.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    # 기간은 시작/종료일 입력 + 기본 이번 주. 빠른 범위 버튼(오늘/이번주/다음주)은 제거.
    assert "function _ecThisWeek()" in js
    assert "id=\"econCalStart\"" in js
    assert "id=\"econCalEnd\"" in js
    assert "ec-range-btn" not in js
    # 국가·중요도는 ⚙ 설정 패널의 '중요도별 국가 선택'으로.
    assert "function _ecRenderSettings()" in js
    assert "id=\"econCalSettingsToggle\"" in js
    assert "function _ecLevelParam(lvl)" in js
    # 기본값: 상=전체, 중·하=한국만.
    assert "high: 'all', mid: new Set(['kr']), low: new Set(['kr'])" in js
    assert "params.set('high'" in js
    assert ".ec-settings" in styles
    assert ".ec-set-row" in styles


def test_etf_deep_links_to_eiayn_in_analysis_and_portfolio():
    analysis = (JS / "analysis.js").read_text(encoding="utf-8")
    insights = (JS / "portfolio-insights.js").read_text(encoding="utf-8")

    # 분석뷰 밸류에이션 그리드: ETF면 외부 카드 합류(우선주/지주사와 동일 패턴).
    assert "const e = links.etf;" in analysis
    assert "ETF 상세" in analysis
    assert "data.preferred || data.holding || data.etf" in analysis
    # 포트폴리오 인사이트 모달: ETF 액션 링크(eiayn 새 탭).
    assert "function _etfInfoAction(etf)" in insights
    assert "_renderInsightActionLinks(code, goldGap, holding, etf)" in insights
    assert "const etf = data.etf || null;" in insights


def test_index_loads_portfolio_split_scripts_in_contract_order():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert "<title>Value Compass</title>" in html
    assert "Value is All You Need" in html
    assert "한국 주식 가치투자 분석" not in html

    positions = []
    for name in PORTFOLIO_SPLIT_FILES:
        marker = f'./js/{name}'
        pos = html.find(marker)
        assert pos != -1, f"{name} is not loaded by index.html"
        positions.append(pos)
    assert positions == sorted(positions), "portfolio split script order changed"
    assert './js/portfolio.js"' not in html, "legacy monolith entrypoint should not be loaded by new HTML"


def test_portfolio_feature_files_stay_below_maintenance_ceiling():
    for name in PORTFOLIO_SPLIT_FILES:
        lines = (JS / name).read_text(encoding="utf-8").splitlines()
        assert len(lines) < 1000, f"{name} grew to {len(lines)} lines; split it before extending"


def test_index_loads_analysis_split_scripts_in_contract_order():
    html = (STATIC / "index.html").read_text(encoding="utf-8")

    positions = []
    for name in ANALYSIS_SPLIT_FILES:
        marker = f'./js/{name}'
        pos = html.find(marker)
        assert pos != -1, f"{name} is not loaded by index.html"
        positions.append(pos)
    assert positions == sorted(positions), "analysis split script order changed"
    # 분할 파일은 검색(search.js) 다음, 나머지 뷰 스크립트(stock-alerts.js) 앞에 묶여 산다.
    assert html.find("./js/search.js") < positions[0]
    assert positions[-1] < html.find("./js/stock-alerts.js")


def test_analysis_split_files_keep_feature_homes():
    charts = (JS / "analysis-charts.js").read_text(encoding="utf-8")
    filings = (JS / "analysis-filings.js").read_text(encoding="utf-8")
    analysis = (JS / "analysis.js").read_text(encoding="utf-8")

    # 차트: 주간/연간 그리드, 목표가 오버레이, 차트 모달, 기간 전환.
    assert "async function renderChartGrid(" in charts
    assert "async function _overlayTargetPrices(" in charts
    assert "function openChartModal(" in charts
    assert "function closeChartModal()" in charts
    assert "async function switchValuationPeriod(" in charts
    # 공시/리포트: DART AI 리뷰, 리포트 테이블, 위키 요약.
    assert "function renderFilingReview(" in filings
    assert "async function generateFilingReview(" in filings
    assert "const FILING_REVIEW_STATUS_TIMEOUT_MS = 60000;" in filings
    assert "const FILING_REVIEW_GENERATE_TIMEOUT_MS = 10 * 60 * 1000;" in filings
    assert "function renderReportsTable(" in filings
    assert "async function loadWiki(" in filings
    # 본체: 분석 SSE 오케스트레이션, 시세 요약, 위키 Q&A.
    assert "async function analyzeStock(" in analysis
    assert "async function renderResult(" in analysis
    assert "function renderQuoteSnapshot(" in analysis
    assert "async function askWikiQuestion()" in analysis
    # SSE 스트리밍 호출은 apiFetch 타임아웃 제외 플래그를 유지한다.
    assert "stream: true" in analysis


def test_analysis_split_files_stay_below_maintenance_ceiling():
    for name in ANALYSIS_SPLIT_FILES:
        lines = (JS / name).read_text(encoding="utf-8").splitlines()
        assert len(lines) < 1000, f"{name} grew to {len(lines)} lines; split it before extending"


def test_admin_page_loads_admin_split_scripts_in_contract_order():
    html = (STATIC / "admin.html").read_text(encoding="utf-8")

    positions = []
    for name in ADMIN_SPLIT_FILES:
        marker = f'/js/{name}'
        pos = html.find(marker)
        assert pos != -1, f"{name} is not loaded by admin.html"
        positions.append(pos)
    assert positions == sorted(positions), "admin split script order changed"
    # 인라인 apiFetch 헬퍼(credentials:'include')는 모든 admin 스크립트보다 먼저.
    assert html.find("async function apiFetch(") != -1
    assert html.find("async function apiFetch(") < positions[0]
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
    # 관측성: 배포/서버/배치/사용자/DB/이벤트/HTTP 패널 + 5초 라이브 갱신 +
    # 수동 작업 실행 + 위키 진단 폼.
    assert "function _renderDeployCard(" in observability
    assert "function _renderServerCard(" in observability
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


def test_admin_split_files_stay_below_maintenance_ceiling():
    for name in ADMIN_SPLIT_FILES:
        lines = (JS / name).read_text(encoding="utf-8").splitlines()
        assert len(lines) < 1000, f"{name} grew to {len(lines)} lines; split it before extending"


def test_portfolio_default_sort_is_unset():
    # Sort state lives in PfStore (portfolio-store.js), not bare shell globals.
    source = (JS / "portfolio-store.js").read_text(encoding="utf-8")

    assert "sort: { key: null, asc: false, groupSort: false }," in source


def test_market_tape_is_bottom_frame_outside_main():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

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
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    assert ".market-tape-item.breaking.down" in styles
    assert ".market-tape-item.alert.down" in styles
    assert "rgba(37, 99, 235, 0.08)" in styles
    assert "#1d4ed8" in styles


def test_portfolio_ai_result_has_framed_output_contract():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    source = (JS / "portfolio-ai.js").read_text(encoding="utf-8")

    assert 'id="pfAiOutput"' in html
    assert 'id="pfAiStatus"' in html
    assert "function _pfAiSectionKind" in source
    assert "function _decoratePfAiResult" in source
    assert "function _normalizePfAiEmptySections" in source
    assert "_renderPfAiMarkdown(result, mdText, { decorate: true" in source
    assert "d.truncated" in source
    assert "d.finish_reason" in source
    assert "context_holdings" in source
    assert "data-kind=\"visual\"" in styles or 'data-kind="visual"' in styles
    assert ".pf-ai-output" in styles
    assert ".pf-ai-section" in styles
    assert ".pf-ai-group-heading" in styles
    assert '.pf-ai-section[data-kind="action"]' in styles
    assert ".pf-ai-result:not(.pf-ai-empty)" in styles
    assert "repeat(auto-fit" in styles


def test_today_card_does_not_fallback_to_quote_session_return():
    source = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    forbidden = "totalDailyPnl / prevMV"
    assert forbidden not in source, "TODAY card must stay on the 20:00 settlement baseline, not quote-session math"


def test_today_card_percent_uses_same_settlement_base_as_amount():
    source = (JS / "portfolio-render.js").read_text(encoding="utf-8")

    assert "const _dailyBaseValue = _periodBaseValue(PfStore.snapshots.prevDay);" in source
    assert "dailyNavPct = totalDailyPnlDisplay / _dailyBaseValue * 100;" in source
    assert "const _liveNavValueKrw" in source
    assert "let _pendingUnitsChange = 0;" in source
    assert "let _pendingCashflowWithoutUnits = 0;" in source
    assert "Number(cf?.units_change)" in source
    assert "Number(cf?.signed_amount || 0)" in source
    assert "Number(latestSnap.total_units) + _pendingUnitsChange" in source
    assert "grandTotalMarketValue - _pendingCashflowWithoutUnits" in source
    assert "_renderSummarySparklines(_l ? _liveNavValueKrw : null);" in source


def test_today_sparkline_uses_8_to_20_session_axis():
    source = (JS / "portfolio-render.js").read_text(encoding="utf-8")

    assert "function _sparkAxisHoursFromTs" in source
    assert "function _sparkNowKstIsoMinute" in source
    assert "function _sparkTodayCashflowThroughTs" in source
    # TODAY 축은 세션일 08:00~20:00(KST) 고정 — 결산창의 야간 빈 구간을 잘라낸다.
    # 세션일은 intraday 최신 점 날짜(주말·공휴일에도 정합), 없으면 현재 KST.
    assert "const SPARK_DAILY_START_HOUR = 8;" in source
    assert "const SPARK_DAILY_END_HOUR = 20;" in source
    assert "function _sparkDailyAxis" in source
    assert "const _dailyAxis = _sparkDailyAxis();" in source
    assert "const axisStartTs = _dailyAxis.start;" in source
    assert "const axisEndTs = _dailyAxis.end;" in source
    assert "const _dailyAxisHours = SPARK_DAILY_END_HOUR - SPARK_DAILY_START_HOUR;" in source
    assert "_sparkAxisHoursFromTs(d.ts, axisStartTs, axisEndTs)" in source
    assert "_sparkAxisHoursFromTs(_sparkNowKstIsoMinute(), axisStartTs, axisEndTs)" in source
    # y 는 직전 20:00 결산(prevClose) 대비 등락% 유지(축만 바뀜).
    assert "const adjustedTotal = Number(d.total_value) - _sparkTodayCashflowThroughTs(d.ts);" in source
    assert "adjustedTotal / _prevClose - 1" in source
    assert "_drawSparklinePoints('sparkDaily', raw, lastPct >= 0 ? '#dc2626' : '#2563eb', _dailyAxisHours);" in source
    assert "raw.filter(p => p.y" not in source
    # 24시간 결산창 축은 제거됨.
    assert "_sparkAxisEndTs" not in source
    assert "visibleAxisMaxHours" not in source
    assert "hour + 2" not in source
    assert "endsWith('T00:00')" not in source


def test_filtered_today_card_excludes_attributed_cashflows():
    source = (JS / "portfolio-render.js").read_text(encoding="utf-8")

    assert "const _periodCashflowValue = (snap) =>" in source
    assert "snap.today_cashflows_by_stock || {}" in source
    assert "for (const r of rows) total += Number(byStock[r.stock_code] || 0);" in source
    assert "const pnl = (_currentFxVal - _periodCashflowValue(snap)) - baseVal;" in source


def test_cashflow_history_renders_before_nav_charts_finish():
    source = (JS / "portfolio-performance.js").read_text(encoding="utf-8")

    assert "void cashflowPromise.then(cfData =>" in source
    assert "renderCashflows(cfData, cachedNav || PfStore.navHistory || _navChartData);" in source
    assert source.find("void cashflowPromise.then(cfData =>") < source.find("const navData = await navPromise;")
    assert "renderCashflows(cfData, navData);" in source


def test_performance_tab_includes_period_report_panel():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    reports = (JS / "portfolio-reports.js").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")

    assert 'id="pfPeriodReportWrap"' in html
    assert 'id="pfPeriodReportType"' in html
    assert 'id="pfPeriodReportKey"' in html
    assert 'id="pfPeriodReportGenerateBtn"' in html
    assert html.find('id="pfNavChart"') < html.find('id="pfPeriodReportWrap"')
    assert html.find('id="pfRiskWrap"') < html.find('id="pfPeriodReportWrap"')
    assert "./js/portfolio-ai.js" in html
    assert html.find("./js/portfolio-ai.js") < html.find("./js/portfolio-reports.js")
    assert html.find("./js/portfolio-reports.js") < html.find("./js/portfolio-performance.js")
    assert "if (typeof pfLoadPeriodReportsPanel === 'function') pfLoadPeriodReportsPanel();" in performance
    assert "async function pfLoadPeriodReportsPanel(" in reports
    assert "async function pfGeneratePeriodReport()" in reports
    assert "/api/portfolio/period-reports/periods" in reports
    assert "/api/portfolio/period-reports/generate" in reports
    assert "report.composition_changes || {}" in reports
    assert "function _pfCompositionActivityLabel" in reports
    assert "futures_short: '선물 매도'" in reports
    assert "매수·편입 구성 변화" in reports
    assert "매도·축소 구성 변화" in reports
    assert "schema v" in reports
    assert "pfDownloadPeriodReportMarkdown" in reports
    assert ".pf-period-report-card" in styles
    assert ".pf-period-report-table" in styles
    assert ".pf-period-note" in styles


def test_cashflow_mutations_refresh_today_and_holdings_before_rerender():
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    cashflows = (JS / "portfolio-cashflows.js").read_text(encoding="utf-8")

    assert "async function loadPortfolio({ force = false } = {})" in data
    assert "const todayStatePromise = pfRefreshTodayState({ force: true, render: false }).catch(() => ({ updated: false }));" in data
    assert "await todayStatePromise;" in data
    assert data.find("await todayStatePromise;") < data.find("_savePortfolioSnapshot(PfStore.items);")
    assert "async function refreshPortfolioAfterCashflowMutation()" in cashflows
    assert "loadPortfolio({ force: true })" in cashflows
    assert "pfRefreshTodayState({ force: true, render: false })" in cashflows
    assert "await refreshPortfolioAfterCashflowMutation();" in cashflows
    assert "loadPerformanceData();" in cashflows


def test_trade_value_column_uses_two_decimal_compact_format():
    source = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    assert "function fmtTradingValueKrw(n)" in source
    assert "return fmtKrw(n, 2);" in source
    assert "fmtKrw(r.tradingValue)" not in source


def test_stylesheet_defines_all_css_variables_it_uses():
    css = (STATIC / "styles.css").read_text(encoding="utf-8")
    used = set(re.findall(r"var\(--([A-Za-z0-9_-]+)", css))
    defined = set(re.findall(r"--([A-Za-z0-9_-]+)\s*:", css))
    assert used - defined == set()


def test_portfolio_actions_support_alphanumeric_krx_codes():
    source = (JS / "portfolio-actions.js").read_text(encoding="utf-8")

    assert "function _normalizePortfolioCode" in source
    assert "^[0-9][0-9A-Z]{5}$" in source
    assert "^[0-9]{5}[1-9A-Z]$" in source
    assert "code.length === 6 && /^\\d{5}/.test(code)" not in source


def test_group_management_daily_profit_uses_full_amount_format():
    source = (JS / "portfolio-groups-market.js").read_text(encoding="utf-8")

    assert "function _fmtSignedFullKrw" in source
    assert "${_fmtSignedFullKrw(dailyPnl)}" in source
    assert "${fmtSignedKrw(dailyPnl)}" not in source


def test_portfolio_delete_uses_encoded_url_and_server_reload():
    source = (JS / "portfolio-actions.js").read_text(encoding="utf-8")

    assert "/api/portfolio/${encodeURIComponent(stockCode)}" in source
    assert "await loadPortfolio();" in source


def test_portfolio_reorder_persists_snapshot_and_checks_save_response():
    store = (JS / "portfolio-store.js").read_text(encoding="utf-8")
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    order = (JS / "portfolio-order.js").read_text(encoding="utf-8")
    actions = (JS / "portfolio-actions.js").read_text(encoding="utf-8")
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    assert "manualOrder: { pendingCodes: null, revision: 0, saveInFlight: false }," in store
    assert "const loadOrderRevision = PfStore.manualOrder.revision;" in data
    assert "const preservePendingManualOrder = !!PfStore.manualOrder.pendingCodes;" in data
    assert "nextPortfolioItems = pfApplyManualOrder(nextPortfolioItems, PfStore.manualOrder.pendingCodes);" in data
    assert data.find("nextPortfolioItems = pfApplyManualOrder(nextPortfolioItems, PfStore.manualOrder.pendingCodes);") < data.find("PfStore.items = nextPortfolioItems;")
    assert "function pfApplyManualOrder(items, orderedCodes)" in data
    assert "function _pfNextOrderAfterDrop(items, fromCode, toCode, dropPosition = 'before')" in order
    assert "const targetIdx = next.findIndex(i => i.stock_code === toCode);" in order
    assert "const insertIdx = dropPosition === 'after' ? targetIdx + 1 : targetIdx;" in order
    assert "function _pfClearPortfolioDragOver(root = document)" in order
    assert "function _pfDropPositionForEvent(e, row)" in order
    assert "async function pfFlushManualOrderSave()" in order
    assert "while (PfStore.manualOrder.pendingCodes && PfStore.manualOrder.pendingCodes.length)" in order
    assert "if (!_pfSameOrderCodes(PfStore.manualOrder.pendingCodes, orderCodes) || PfStore.manualOrder.revision !== orderRevision)" in order
    assert "_legacyPfDropRowImmediateSave" not in order
    assert "PfStore.manualOrder.pendingCodes = orderCodes;" in order
    assert "PfStore.manualOrder.revision += 1;" in order
    assert "PfStore.manualOrder.saveInFlight = true;" in order
    assert "_pfSetPortfolioSortOrder(orderCodes);" in order
    assert "_savePortfolioSnapshot(PfStore.items);" in order
    assert "if (!resp.ok)" in order
    assert "throw new Error(data.detail || 'Portfolio order save failed');" in order
    assert "await loadPortfolio({ force: true });" in order
    assert "async function pfDropRow" not in actions
    assert "const canManualDrag = PfStore.filters.group === null && !PfStore.sort.key && !PfStore.sort.groupSort && !searchText && currentUser && !PfStore.edit.code;" in render
    assert "_pfDropPositionForEvent(e, tr)" in render
    assert "_pfClearPortfolioDragOver(tbody)" in render
    assert "pfDropRow(fromCode, toCode, dropPosition)" in render
    assert ".pf-table tbody tr.drag-over-before td" in styles
    assert ".pf-table tbody tr.drag-over-after td" in styles


def test_portfolio_stock_click_uses_explicit_insight_link_handler():
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    events = (JS / "portfolio-events.js").read_text(encoding="utf-8")

    assert 'const stockIdentity = `<span class="pf-stock-main"><span class="pf-stock-line"><a href="#" class="pf-stock-link js-pf-open-insight"' in render
    assert '${dragHandle}${stockIdentity}' in render
    assert "const stockCellClass = canManualDrag ? 'pf-stock-cell pf-stock-cell-with-drag js-pf-analyze' : 'pf-stock-cell js-pf-analyze';" in render
    assert '<td class="${stockCellClass}">${dragHandle}${stockIdentity}</td>' in render
    assert ".pf-stock-cell-with-drag" in styles
    assert "grid-template-columns: 16px minmax(0, 1fr);" in styles
    assert ".pf-stock-main" in styles
    assert "display: inline-flex;" in styles
    assert ".pf-stock-line" in styles
    assert "flex-direction: column;" in styles
    assert "flex-wrap: wrap;" in styles
    assert "overflow-wrap: anywhere;" in styles
    assert "pf-stock-link js-pf-open-insight" in render
    save_branch = "((el = t.closest('.js-pf-save')))"
    open_branch = "((el = t.closest('.js-pf-open-insight')))"
    analyze_branch = "((el = t.closest('.js-pf-analyze')))"
    assert events.find(save_branch) != -1
    assert events.find(save_branch) < events.find(open_branch)
    assert events.find(save_branch) < events.find(analyze_branch)
    assert events.find(open_branch) != -1
    assert events.find(open_branch) < events.find(analyze_branch)
    assert "target.closest('#pfBody tr[data-code]')" in events
    assert "isPassivePortfolioRowTarget" in events
    assert "!PfStore.edit.code && (el = portfolioRowFromTarget(t))" in events
    assert "if (code) pfGoAnalyze(code, e);" in events


def test_portfolio_tag_summary_popup_uses_weighted_daily_return():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    events = (JS / "portfolio-events.js").read_text(encoding="utf-8")
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    source = (JS / "portfolio-tag-summary.js").read_text(encoding="utf-8")

    assert html.find("./js/portfolio-tag-summary.js") < html.find("./js/portfolio-events.js")
    assert "js-pf-open-tag-summary" in data
    assert "data-tag=\"${safeTag}\"" in data
    assert "js-pf-open-group-summary" in data
    assert '<td class="pf-col-group">${groupHtml}</td>' in render
    assert '<td class="pf-col-group"><select class="pf-group-select js-pf-group"${editAttrs}>${groupOpts}</select></td>' in render
    assert '<td class="pf-col-group"><select class="pf-group-select js-pf-group">${groupOpts}</select></td>' not in render
    assert "pfOpenTagSummary(el.dataset.tag || '')" in events
    assert "pfOpenGroupSummary(el.dataset.group || '')" in events
    assert "function pfOpenTagSummary(tag)" in source
    assert "function pfOpenGroupSummary(groupName)" in source
    assert "/api/portfolio/group-weight-history" in source
    assert "baseValue > 0 ? dailyPnl / baseValue * 100 : null" in source
    assert "sliceStats.dailyReturnPct - allStats.dailyReturnPct" in source
    assert "pf-tag-summary-table" in source


def test_portfolio_insight_modal_renders_valuation_cards():
    source = (JS / "portfolio-insights.js").read_text(encoding="utf-8")

    assert "/api/portfolio/asset-insight/${encodeURIComponent(stockCode)}?_=${Date.now()}" in source
    assert "{ cache: 'no-store' }" in source
    assert "const valuation = data.valuation || {}" in source
    assert "function _fmtInsightMultiple(value)" in source
    assert "_renderInsightCard('PBR'" in source
    assert "_renderInsightCard('PER'" in source
    assert "_renderInsightCard('ROE'" in source
    assert "_renderInsightCard('자사주 비율'" in source
    assert "valuation.treasuryShareRatioPct" in source
    assert "valuation.applicable" in source


def test_portfolio_search_and_registration_are_separate_controls():
    html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
    shell = (JS / "portfolio-shell.js").read_text(encoding="utf-8")
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    actions = (JS / "portfolio-actions.js").read_text(encoding="utf-8")
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")

    assert 'id="pfSearchInput"' in html
    assert 'id="pfAddToggle"' in html
    assert 'id="pfAddPanel"' in html
    assert html.find('id="pfAddToggle"') < html.find('id="pfCsvToggle"')
    # Search text lives in PfStore.filters (portfolio-store.js).
    assert "filters: { group: null, searchText: '' }," in (JS / "portfolio-store.js").read_text(encoding="utf-8")
    assert "PfStore.filters.searchText" in shell or "PfStore.filters.searchText" in data
    assert "function pfRowMatchesSearch" in data
    assert "...pfGetTags(item)" in data
    assert "function pfSetAddPanelOpen" in actions
    assert "pfInitPortfolioTextSearch()" in actions
    assert "pfSearchMeta" in render
    assert "검색 결과가 없습니다." in render


def test_holding_target_formula_keeps_external_quotes_fresh():
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    actions = (JS / "portfolio-actions.js").read_text(encoding="utf-8")
    insights = (JS / "portfolio-insights.js").read_text(encoding="utf-8")

    assert "function _syncHoldingContext(holding)" in insights
    assert "function _holdingValueAction(stockCode)" in insights
    assert "_renderInsightActionLinks(code, goldGap, holding, etf)" in insights
    assert "_renderInsightCard('자회사 비율 추이'" in insights
    assert "holdingValuePerShare: it.current?.holdingValuePerShare" in insights
    assert "function _targetFormulaUses(item, variableName)" in actions
    assert "meta.holdingValuePerShare" in actions
    assert "r.target_price == null || _targetFormulaUses(r, '보유지분')" in render
    assert "r.target_price == null || _targetFormulaUses(r, '본주가격')" in render
    assert "if (_applyHoldingPayload(data, true) && typeof renderPortfolio === 'function')" in insights


def test_portfolio_edit_save_is_row_scoped_and_safe():
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    actions = (JS / "portfolio-actions.js").read_text(encoding="utf-8")
    events = (JS / "portfolio-events.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    assert 'type="button" class="pf-row-btn save js-pf-save"' in render
    # Edit state lives in PfStore.edit (portfolio-store.js).
    assert "edit: { code: null, savingCode: null }," in (JS / "portfolio-store.js").read_text(encoding="utf-8")
    assert "const isSaving = PfStore.edit.savingCode === r.stock_code" in render
    assert 'class="pf-row-saving" aria-busy="true"' in render
    assert "pf-save-spinner" in render
    assert 'class="pf-edit-input pf-stock-name-edit js-pf-edit-name"' in render
    assert 'id="pfEditName"' in render
    assert "pf-stock-cell-editing" in render
    assert 'class="pf-edit-input js-pf-edit-qty"' in render
    assert 'class="pf-edit-input js-pf-edit-price"' in render
    assert 'class="pf-price-currency-select js-pf-edit-price-currency"' in render
    assert "function pfAvgPriceKrw" in render
    assert "pfFmtAvgPriceCell(r, _fp)" in render
    assert 'class="pf-edit-input js-pf-edit-target"' in render
    assert "savePortfolioEdit(code, undefined, el.closest('tr[data-code]'))" in events
    assert "function _pfFindEditRow(stockCode, row)" in actions
    assert "function _pfSetEditSaving(stockCode, saving, row)" in actions
    assert "if (PfStore.edit.savingCode) return;" in actions
    assert "_pfSetEditSaving(stockCode, true, editRow)" in actions
    assert ".pf-stock-name-edit" in styles
    assert ".pf-stock-cell-editing" in styles
    assert "const nameEl = editRow?.querySelector('.js-pf-edit-name')" in actions
    assert "stockName = nameEl ? nameEl.value.trim()" in actions
    assert "showToast('종목명을 입력해 주세요.');" in actions
    assert "editRow?.querySelector('.js-pf-edit-qty')" in actions
    assert "editRow?.querySelector('.js-pf-edit-price')" in actions
    assert "editRow?.querySelector('.js-pf-edit-price-currency')" in actions
    assert "avg_price_currency: avgPriceCurrency" in actions
    assert "const hadExplicitTarget = !!(" in actions
    assert "const targetUnchanged = !!existingItem && (" in actions
    assert "Do not resend an unchanged target/formula." in actions
    assert "body.target_price_disabled = true;" in actions
    assert "기존 목표가를 비우면 -로 고정됩니다" in render
    assert "/api/portfolio/${encodeURIComponent(stockCode)}" in actions
    assert "/api/portfolio/${encodeURIComponent(stockCode)}/benchmark" in actions


def test_portfolio_add_canonicalizes_alias_before_save():
    source = (JS / "portfolio-actions.js").read_text(encoding="utf-8")

    assert "portfolio code canonicalization failed" in source
    assert "/api/portfolio/resolve-name?code=${encodeURIComponent(resolvedCode)}" in source
    assert "/api/portfolio/${encodeURIComponent(resolvedCode)}" in source
    assert "pfApplySavedPortfolioItem(saved, resolvedCode, resolvedName, resolvedCurrency)" in source
    assert "startPortfolioEdit(saved.stock_code || resolvedCode)" in source
    assert "loadPortfolio({ force: true }).catch" in source


def test_portfolio_add_has_fast_foreign_search_path():
    source = (
        (JS / "portfolio-add-search.js").read_text(encoding="utf-8")
        + "\n"
        + (JS / "portfolio-actions.js").read_text(encoding="utf-8")
    )

    assert "function pfCanonicalDirectTicker" in source
    assert "function pfInferTickerCurrency" in source
    assert "/api/portfolio/search-foreign?q=${encodeURIComponent(raw)}" in source
    assert "/api/portfolio/search-foreign?q=${encodeURIComponent(q)}" in source
    assert "if (resolvedCurrency) body.currency = resolvedCurrency;" in source


def test_quote_manager_polls_stale_websocket_quotes_as_rest_fallback():
    source = (JS / "quote-manager.js").read_text(encoding="utf-8")

    assert "const QUOTE_MANAGER_STALE_WS_MS = 55_000;" in source
    assert "lastWsQuoteAt: {}" in source
    assert "_markWsQuoteFresh(msg.code)" in source
    rest_fetch_loop = source.split("const data = await resp.json();", 1)[1].split("} catch", 1)[0]
    assert "_markWsQuoteFresh" not in rest_fetch_loop
    assert "async _pollAll()" in source
    assert "this._getStaleWsCodes().forEach(c => allCodes.add(c));" in source


def test_frontend_displays_stale_quotes_but_keeps_refreshing_them():
    utils = (JS / "utils.js").read_text(encoding="utf-8")
    quote_manager = (JS / "quote-manager.js").read_text(encoding="utf-8")
    app_main = (JS / "app-main.js").read_text(encoding="utf-8")
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")

    assert "function quoteIsUsable(q)" in utils
    assert "q._stale !== true" in utils
    assert "function quotePriceOrNull(q)" in utils
    assert "return q && q.price !== null && q.price !== undefined ? q.price : null;" in utils
    assert "function shouldAcceptQuoteSnapshot(current, incoming)" in utils
    assert "function quoteSnapshotTimeValue(q)" in utils
    assert "q.ts ?? q.fetched_at ?? q.fetchedAt ?? q._receivedAt" in utils
    assert "if (incomingDate < currentDate) return false;" in utils
    assert "const currentRank = quoteSourceRank(current);" in utils
    assert "const incomingRank = quoteSourceRank(incoming);" in utils
    assert "if (incomingRank > currentRank) return true;" in utils
    # 랭크 강등 거부는 보호 시간 내 신선한 시세에만 적용 — WS 틱이 끊긴
    # 종목이 같은 날짜 안에서 폴링 갱신까지 막혀 동결되지 않게 한다.
    assert "const QUOTE_RANK_PROTECT_MS = 20_000;" in utils
    assert "function quoteSnapshotIsRecent(q)" in utils
    assert "if (incomingRank < currentRank && quoteSnapshotIsRecent(current)) return false;" in utils
    assert utils.find("if (incomingRank < currentRank && quoteSnapshotIsRecent(current)) return false;") < utils.find("const currentTime = quoteSnapshotTimeValue(current);")
    assert "function mergeQuoteSupplementalFields(current, incoming)" in utils
    assert "const fields = ['previous_close', 'trade_value'];" in utils
    assert "if (!quoteValuePresent(next.change_pct))" in utils
    assert "(price - previousClose) / previousClose * 100" in utils
    assert "function mergeQuoteSnapshot(current, incoming)" in utils
    assert "if (!shouldAcceptQuoteSnapshot(current, incoming)) return mergeQuoteSupplementalFields(current, incoming);" in utils
    assert "function quoteSnapshotDisplayChanged(before, after)" in utils
    assert "if (!quoteIsUsable(i.quote)) missing.add(i.stock_code);" in quote_manager
    assert "const QUOTE_MANAGER_BATCH_SIZE = 30;" in quote_manager
    assert "const QUOTE_MANAGER_BATCH_PARALLEL = 1;" in quote_manager
    assert "await this._fetchQuotes(wsCodes, { fresh: false, scheduleRetry: false });" in quote_manager
    assert "await this._fetchQuotes(missing, { fresh: true });" in quote_manager
    assert "const nextQuote = mergeQuoteSnapshot(pfItem.quote, q);" in app_main
    assert "pfQuoteAccepted = quoteSnapshotDisplayChanged(pfItem.quote, nextQuote);" in app_main
    assert "pfItem.quote = nextQuote;" in app_main
    assert "const _PF_PORTFOLIO_SNAPSHOT_QUOTE_TTL_MS = 2 * 60 * 1000;" in data
    assert "quote: { ...item.quote, _stale: true }" in data
    assert "if (quoteIsUsable(i.quote)) prevQuotes[i.stock_code] = i.quote;" in data
    assert "item.quote = mergeQuoteSnapshot(prevQuote, item.quote);" in data
    assert "const price = quotePriceOrNull(q);" in data
    assert "const price = quotePriceOrNull(q);" in render


def test_market_bar_and_benchmark_polling_do_not_clear_good_values_on_empty_refresh():
    shell = (JS / "portfolio-shell.js").read_text(encoding="utf-8")
    groups = (JS / "portfolio-groups-market.js").read_text(encoding="utf-8")
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    actions = (JS / "portfolio-actions.js").read_text(encoding="utf-8")

    assert "function pfMergeBenchmarkQuote(code, incoming)" in shell
    assert "incoming._stale !== true || !currentHasChange" in shell
    assert "let mbLastDataMap = {}" in groups
    assert "function _mbMergeDataMap(dataMap)" in groups
    assert "_mbRenderBar(_mbMergeDataMap(dataMap));" in groups
    assert "for (const [k, v] of Object.entries(fresh)) pfMergeBenchmarkQuote(k, v);" in groups
    assert "for (const [k, v] of Object.entries(fresh)) pfMergeBenchmarkQuote(k, v);" in data
    assert "pfMergeBenchmarkQuote(data.effective_benchmark" in actions


def test_benchmark_picker_only_opens_in_edit_mode():
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    actions = (JS / "portfolio-actions.js").read_text(encoding="utf-8")
    events = (JS / "portfolio-events.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    assert '<td class="pf-col-num pf-col-benchmark js-pf-bench-picker" title="벤치마크 변경">' in render
    assert '<td class="pf-col-num pf-col-benchmark" title="수정모드에서 변경">' in render
    assert "if (code && PfStore.edit.code === code) pfShowBenchmarkPicker(code, el);" in events
    assert events.find("((el = t.closest('.js-pf-bench-set')))") < events.find("((el = t.closest('.js-pf-bench-picker')))")
    assert "if (PfStore.edit.code !== stockCode)" in actions
    assert ".pf-col-benchmark.js-pf-bench-picker { cursor: pointer; }" in styles


def test_nav_chart_shows_selected_benchmark_beta_overlay():
    benchmark = (JS / "portfolio-trends-benchmark.js").read_text(encoding="utf-8")
    trends = (JS / "portfolio-trends.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    # beta/R² 통계와 오버레이 정의는 portfolio-trends-benchmark.js 가 기능 홈.
    assert "function _computeReturnStats" in benchmark
    assert "function _navBenchmarkBeta" in benchmark
    assert "function _updateNavBetaOverlay" in benchmark
    assert "NAV beta vs selected benchmark" in benchmark
    assert "\\u03b2" in benchmark
    assert "\\u00b2" in benchmark
    # NAV 차트 렌더(본체)가 초기/줌 양쪽에서 오버레이를 갱신한다.
    assert "_updateNavBetaOverlay(labels, navValues, benchCodes, fullWindow.startIdx, fullWindow.endIdx);" in trends
    assert "_updateNavBetaOverlay(labels, navValues, benchCodes, startIdx, endIdx);" in trends
    assert ".pf-nav-beta-overlay" in styles
    assert ".pf-nav-beta-chip" in styles


def test_trends_split_files_keep_feature_homes():
    benchmark = (JS / "portfolio-trends-benchmark.js").read_text(encoding="utf-8")
    trends = (JS / "portfolio-trends.js").read_text(encoding="utf-8")
    group_weight = (JS / "portfolio-trends-group-weight.js").read_text(encoding="utf-8")
    composition = (JS / "portfolio-group-composition.js").read_text(encoding="utf-8")
    html = (STATIC / "index.html").read_text(encoding="utf-8")

    # 본체: NAV/평가금액 차트 렌더, 수익률 카드, 기간/Y축 동기화, 공용 차트 헬퍼.
    assert "async function renderNavChart(data)" in trends
    assert "async function renderValueChart(data)" in trends
    assert "function renderNavReturns(data)" in trends
    assert "function onNavYZeroToggle()" in trends
    assert "function onValueYZeroToggle()" in trends
    assert "function _navZoomToDays(days)" in trends
    assert "function _valueZoomToDays(days)" in trends
    assert "function _updateNavCagrCard(" in trends
    assert "function _updateValueCagrCard(" in trends
    assert "function _chartZoomWindow(" in trends
    assert "function _chartWindowFromInstance(" in trends
    assert "function _applyVisibleYAxis(" in trends
    assert "function _updateChartRangeLabel(" in trends
    assert "function _isMobileChartMode()" in trends
    # 비교지수: 칩 선택 → 히스토리 fetch/캐시 → NAV 재렌더 + beta 오버레이.
    assert "async function onBenchToggle()" in benchmark
    assert "function _getSelectedBenchmarks()" in benchmark
    assert "let _benchCache = {};" in benchmark
    assert "let _benchRatios = {};" in benchmark
    assert "/api/portfolio/benchmark-history" in benchmark
    assert "await renderNavChart(_navChartData);" in benchmark
    # 그룹 비중: 100% 누적 차트 + 구성 drill-down 진입. 색상 팔레트는
    # portfolio-group-composition.js 와 공유하므로 그보다 먼저 로드된다.
    assert "function _prepareGroupWeightChartData(rows)" in group_weight
    assert "async function renderGroupWeightChart(rows)" in group_weight
    assert "const _GROUP_WEIGHT_COLORS = [" in group_weight
    assert "pfShowGroupComposition(card.dataset.group || '')" in group_weight
    assert "_GROUP_WEIGHT_COLORS[idx % _GROUP_WEIGHT_COLORS.length]" in composition
    assert html.find("./js/portfolio-trends-group-weight.js") < html.find("./js/portfolio-group-composition.js")
    # 이동한 심볼이 본체에 중복 정의로 남지 않는다.
    assert "function _computeReturnStats" not in trends
    assert "function _updateNavBetaOverlay(" not in trends
    assert "_GROUP_WEIGHT_COLORS = [" not in trends
    assert "function renderGroupWeightChart" not in trends


def test_performance_tab_includes_group_weight_trend():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")
    trends = (JS / "portfolio-trends.js").read_text(encoding="utf-8")
    group_weight = (JS / "portfolio-trends-group-weight.js").read_text(encoding="utf-8")
    chart = (JS / "portfolio-trend-chart.js").read_text(encoding="utf-8")
    composition = (JS / "portfolio-group-composition.js").read_text(encoding="utf-8")

    assert "pfGroupWeightChart" in html
    assert "pfGroupCompositionChart" in html
    assert "#pfGroupWeightChart" in styles
    assert "#pfGroupCompositionChart" in styles
    assert ".pf-group-composition-wrap" in styles
    assert "background: transparent" in styles
    assert "100% 누적 면적 차트" in html
    assert "async function pfLoadNavHistory" in data
    assert "_pfNavHistoryPromise" in data
    assert "/api/portfolio/group-weight-history" in performance
    assert "pfLoadGroupWeightHistory" in performance
    assert "_performanceLoadSeq" in performance
    assert "const [navResp, cfResp, groupResp] = await Promise.all" not in performance
    assert "renderGroupWeightChart(groupWeightData)" in performance
    assert "async function renderGroupWeightChart(rows)" in group_weight
    assert "async function _waitForChartContainer" in trends
    assert "container.offsetParent === null" in trends
    assert "container.offsetParent === null" in group_weight
    assert "/api/portfolio/group-constituent-history" in composition
    assert "async function pfShowGroupComposition(groupName)" in composition
    assert "_groupCompositionRequestSeq" in composition
    assert "stack: 'groupComposition'" in composition
    assert "stock_count" in group_weight
    assert "stack: 'groupWeight'" in group_weight
    assert "areaStyle: { opacity:" in group_weight
    assert "stack: series.stack || null" in chart
    assert "stackedIndexes.add(seriesIdx)" in chart
    assert "#pfPerformanceTab" in styles
    assert "width: 100%;" in styles
    assert "max-width: 1180px" not in styles[styles.find("#pfPerformanceTab"):styles.find(".pf-deep-head")]
    assert "grid-template-columns: minmax(0, 1fr);" in styles
    assert "return `${parts[0]}-${parts[1]}`;" in chart
    assert "{ label: '전일 NAV'" not in trends
    assert "_periodPctByCalendarDays(data, navValues, 7)" in trends


def test_performance_tab_includes_risk_panel():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")
    risk = (JS / "portfolio-risk.js").read_text(encoding="utf-8")

    # '리스크' 카드는 성과 탭의 NAV 수익률 요약(#pfNavReturns) 바로 다음,
    # 평가금액 추이 앞에 산다. 윈도 버튼은 기존 .vp-btn 스타일 재사용.
    assert 'id="pfRiskWrap"' in html
    assert 'id="pfRiskWindowBtns"' in html
    assert 'id="pfRiskContent"' in html
    assert html.find('id="pfNavReturns"') < html.find('id="pfRiskWrap"')
    assert html.find('id="pfRiskWrap"') < html.find('id="pfValueChart"')
    # 성과 탭이 보일 때만 lazy 로드(pfSwitchTab 경유) — 앱 시작 시 조회 금지.
    assert "if (typeof pfLoadRiskPanel === 'function') pfLoadRiskPanel();" in performance
    # 기능 홈: fetch/렌더/윈도 전환/메모는 portfolio-risk.js.
    assert "async function pfLoadRiskPanel(" in risk
    assert "function pfRiskSetWindow(" in risk
    assert "function _pfRenderRiskPanel(" in risk
    assert "/api/portfolio/risk?window=" in risk
    assert "const _pfRiskCache = {};" in risk
    # 백그라운드 로드 — 오류는 silent 보고 + 패널 내 안내.
    assert "reportApiError(e, '리스크 지표', { silent: true });" in risk
    # 데이터 부족 시 친절한 빈 상태 문구.
    assert "데이터가 부족합니다" in risk
    # 포맷터는 portfolio-render.js / utils.js 공용 헬퍼 재사용(중복 정의 금지).
    assert "function fmtPct(" not in risk
    assert "function returnClass(" not in risk
    assert "function escapeHtml(" not in risk
    # 스타일: 타일은 .pf-nav-ret-card 재사용, 좁은 화면에선 2열 그리드.
    assert ".pf-risk-grid" in styles
    assert ".pf-risk-empty" in styles
    assert ".pf-risk-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }" in styles


def test_performance_tab_includes_rebalance_panel():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")
    rebalance = (JS / "portfolio-rebalance.js").read_text(encoding="utf-8")
    alerts = (JS / "portfolio-alerts.js").read_text(encoding="utf-8")

    # '리밸런싱' 카드는 기간 보고서 다음, 평가금액 추이 앞에 산다.
    # 에디터는 모달이 아닌 인라인 리스트.
    assert 'id="pfRebalanceWrap"' in html
    assert 'id="pfRebalanceContent"' in html
    assert 'id="pfRebalanceEditor"' in html
    assert 'id="pfRebalanceAlertCb"' in html
    assert html.find('id="pfPeriodReportWrap"') < html.find('id="pfRebalanceWrap"')
    assert html.find('id="pfRebalanceWrap"') < html.find('id="pfValueChart"')
    # 성과 탭이 보일 때만 lazy 로드(pfSwitchTab 경유) — 앱 시작 시 조회 금지.
    assert "if (typeof pfLoadRebalancePanel === 'function') pfLoadRebalancePanel();" in performance
    # 기능 홈: 보고서 fetch/렌더, 목표 에디터(PUT 전체 교체), 알림 토글.
    assert "async function pfLoadRebalancePanel(" in rebalance
    assert "function _pfRenderRebalanceReport(" in rebalance
    assert "'/api/portfolio/rebalance'" in rebalance
    assert "'/api/portfolio/rebalance/targets'" in rebalance
    assert "method: 'PUT'" in rebalance
    assert "목표 비중을 설정하면 이탈 현황이 표시됩니다." in rebalance
    # 이탈 시 알림 = rebalance_drift 규칙(사용자당 singleton, 임계값 없음).
    assert "alert_type: 'rebalance_drift'" in rebalance
    assert "/api/notifications/alerts" in rebalance
    assert "리밸런싱 — 목표 비중 이탈 시" in alerts
    # 백그라운드 로드는 silent, 사용자 조작(저장/토글)은 토스트.
    assert "reportApiError(e, '리밸런싱 현황', { silent: true });" in rebalance
    assert "reportApiError(e, '리밸런싱 목표 저장');" in rebalance
    assert "reportApiError(e, '리밸런싱 이탈 알림');" in rebalance
    # 포맷터는 공용 헬퍼 재사용(중복 정의 금지).
    assert "function fmtPct(" not in rebalance
    assert "function fmtKrw(" not in rebalance
    assert "function escapeHtml(" not in rebalance
    # 스타일: 빈 상태는 .pf-risk-empty 재사용, 에디터 행은 모바일 2열 접기.
    assert ".pf-rebal-table" in styles
    assert ".pf-rebal-editor-row" in styles
    assert ".pf-rebal-editor-row { grid-template-columns: repeat(2, minmax(0, 1fr)); }" in styles


def test_portfolio_includes_action_board_and_linked_signal_badges():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    action_board = (JS / "portfolio-action-board.js").read_text(encoding="utf-8")

    # 액션 보드는 opt-in: 기본 hidden 상태이고 탭 바의 작은 버튼으로만 연다.
    assert 'id="pfActionBoard"' in html
    assert 'id="pfActionBoardContent"' in html
    assert 'id="pfActionBoardToggle"' in html
    assert 'aria-controls="pfActionBoard"' in html
    assert html.find('id="pfSummary"') < html.find('id="pfActionBoard"')
    assert html.find('id="pfActionBoard"') < html.find('class="pf-tab-bar"')
    assert "./js/portfolio-render.js" in html
    assert html.find("./js/portfolio-render.js") < html.find("./js/portfolio-action-board.js")

    # 포트폴리오 데이터 로드 후에도 사용자가 켠 경우에만 오늘 액션을 갱신한다.
    assert "typeof pfActionBoardIsEnabled === 'function'" in data
    assert "&& pfActionBoardIsEnabled()" in data
    assert "void pfLoadActionBoard({ force: true });" in data
    assert "/api/portfolio/action-board" in action_board
    assert "/api/portfolio/action-board/queue/" in action_board
    assert "PF_ACTION_BOARD_ENABLED_KEY" in action_board
    assert "function pfToggleActionBoard(" in action_board
    assert "function pfActionBoardIsEnabled(" in action_board
    assert "board.hidden = true;" in action_board
    assert "function pfActionBoardBadgesForCode(" in action_board
    assert "PfActionBoard.signalsByCode" in action_board

    # 연결 프로젝트 신호 배지도 보드 opt-in 후에만 보유종목 행 옆에 노출된다.
    assert "if (!pfActionBoardIsEnabled()) return '';" in action_board
    assert "pfActionBoardBadgesForCode(r.stock_code)" in render
    assert ".pf-action-board" in styles
    assert ".pf-action-toggle" in styles
    assert ".pf-action-card" in styles
    assert ".pf-linked-signal-badge" in styles


def test_performance_tab_includes_dividend_calendar_panel():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")
    divcal = (JS / "portfolio-dividends-calendar.js").read_text(encoding="utf-8")

    # '배당 캘린더' 카드는 성과 탭의 리밸런싱 카드(#pfRebalanceWrap) 바로
    # 다음, 평가금액 추이 앞에 산다.
    assert 'id="pfDivCalWrap"' in html
    assert 'id="pfDivCalContent"' in html
    assert html.find('id="pfRebalanceWrap"') < html.find('id="pfDivCalWrap"')
    assert html.find('id="pfDivCalWrap"') < html.find('id="pfValueChart"')
    # 성과 탭이 보일 때만 lazy 로드(pfSwitchTab 경유) — 앱 시작 시 조회 금지.
    assert "if (typeof pfLoadDividendCalendarPanel === 'function') pfLoadDividendCalendarPanel();" in performance
    # 기능 홈: fetch/렌더/월 펼침은 portfolio-dividends-calendar.js.
    assert "async function pfLoadDividendCalendarPanel(" in divcal
    assert "function _pfRenderDividendCalendar(" in divcal
    assert "function pfDivCalToggleMonth(" in divcal
    assert "/api/portfolio/dividend-calendar?months=12" in divcal
    # 빈 상태 + 확정/예상 배지 + 기준일은 월 합계 제외 안내.
    assert "보유 종목의 배당 정보가 수집되면 표시됩니다." in divcal
    assert "확정" in divcal and "예상" in divcal
    assert "월 합계에서 제외" in divcal
    # 백그라운드 로드는 silent 보고 + 패널 내 안내.
    assert "reportApiError(e, '배당 캘린더', { silent: true });" in divcal
    # 포맷터는 공용 헬퍼 재사용(중복 정의 금지).
    assert "function fmtKrw(" not in divcal
    assert "function escapeHtml(" not in divcal
    # 스타일: 월 행/이벤트 그리드 + 예상(점선·흐림)/임박 강조 + 모바일 접기.
    assert ".pf-divcal-month" in styles
    assert ".pf-divcal-event" in styles
    assert ".pf-divcal-badge.confirmed" in styles
    assert ".pf-divcal-event.pf-divcal-est" in styles
    assert ".pf-divcal-event.pf-divcal-upcoming .pf-divcal-date" in styles
    assert ".pf-divcal-event { grid-template-columns: minmax(0, 1fr) auto; }" in styles


def test_investment_journal_lives_in_analysis_view_and_performance_tab():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    analysis = (JS / "analysis.js").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")
    journal = (JS / "portfolio-journal.js").read_text(encoding="utf-8")

    # 표면 ① 종목 분석 화면: companyInfo 안(위키 Q&A 다음)의 '📝 투자 일지'
    # 섹션 — renderResult 가 활성 종목으로 loadStockJournal 을 호출한다.
    assert 'id="stockJournalSection"' in html
    assert 'id="stockJournalForm"' in html
    assert 'id="stockJournalList"' in html
    assert "📝 투자 일지" in html
    assert html.find('id="wikiQa"') < html.find('id="stockJournalSection"')
    assert html.find('id="stockJournalSection"') < html.find('id="emptyState"')
    assert "if (typeof loadStockJournal === 'function') loadStockJournal(data.stock_code);" in analysis
    # 표면 ② 성과 탭 '투자 일지' 카드: 배당 캘린더 다음, 평가금액 추이 앞.
    # 전 종목 타임라인(읽기/복기) — 성과 탭이 보일 때만 lazy 로드.
    assert 'id="pfJournalWrap"' in html
    assert 'id="pfJournalContent"' in html
    assert html.find('id="pfDivCalWrap"') < html.find('id="pfJournalWrap"')
    assert html.find('id="pfJournalWrap"') < html.find('id="pfValueChart"')
    assert "if (typeof pfLoadJournalPanel === 'function') pfLoadJournalPanel();" in performance
    # 기능 홈: fetch/렌더/폼/인라인 수정/삭제는 portfolio-journal.js.
    assert "async function pfLoadJournalPanel(" in journal
    assert "async function loadStockJournal(" in journal
    assert "async function stockJournalSubmit()" in journal
    assert "async function pfJournalSaveNote(" in journal
    assert "async function pfJournalDelete(" in journal
    assert "'/api/portfolio/journal'" in journal
    assert "/api/portfolio/journal?stock_code=" in journal
    assert "method: 'PATCH'" in journal
    assert "method: 'DELETE'" in journal
    # 백그라운드 로드는 silent, 사용자 조작(기록/수정/삭제)은 토스트.
    assert "reportApiError(e, '투자 일지', { silent: true });" in journal
    assert "reportApiError(e, '투자 일지 기록');" in journal
    assert "reportApiError(e, '투자 일지 수정');" in journal
    assert "reportApiError(e, '투자 일지 삭제');" in journal
    # note 는 escapeHtml 로만 렌더(원시 HTML 금지) + 삭제는 confirm 게이트.
    assert "${escapeHtml(entry.note)}" in journal
    assert "window.confirm(" in journal
    # 빈 상태/로그인 안내 문구.
    assert "아직 기록이 없습니다" in journal
    assert "기록된 투자 일지가 없습니다" in journal
    # 포맷터는 공용 헬퍼 재사용(중복 정의 금지).
    assert "function fmtPct(" not in journal
    assert "function returnClass(" not in journal
    assert "function escapeHtml(" not in journal
    # 스타일: 카드/배지/폼 + 모바일 2열 접기(.pf-rebal-editor-row 패턴).
    assert ".pf-journal-card" in styles
    assert ".pf-journal-badge.buy" in styles
    assert ".stock-journal" in styles
    assert ".pf-journal-form-row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); }" in styles


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
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
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


def test_portfolio_quote_ticks_refresh_summary_without_debouncing_forever():
    app = (JS / "app-main.js").read_text(encoding="utf-8")
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")

    assert "function _queuePortfolioSummaryRender()" in app
    assert "renderPortfolio({ summaryOnly: true })" in app
    assert "_queuePortfolioSummaryRender();" in app
    assert "if (_pfDeferredRenderTimer) return;" in app
    assert "clearTimeout(_pfDeferredRenderTimer)" not in app
    assert "function renderPortfolio(options = {})" in render
    assert "const summaryOnly = !!(options && options.summaryOnly);" in render
    assert "if (summaryOnly) return;" in render


def test_mobile_simple_mode_is_read_only_and_nav_uses_bottom_tabbar():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
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


def test_screener_labs_view_is_wired_as_deep_linkable_panel():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    shell = (JS / "portfolio-shell.js").read_text(encoding="utf-8")
    app_main = (JS / "app-main.js").read_text(encoding="utf-8")
    screener = (JS / "screener.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

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
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

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


def test_analysis_wiki_qa_and_journal_are_collapsed_by_default():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    # 위키 Q&A·투자 일지는 매번 쓰는 요소가 아니라 밸류에이션 차트보다 화면 우선순위가
    # 낮다(UX 감사 P2⑦). <details> 는 JS 없이 접힌 채로 시작하고, 내부 id 는 그대로라
    # 기존 스크립트(wiki.js/portfolio-journal.js)가 값을 읽고 쓰는 데 영향이 없다.
    assert '<details class="analysis-collapsible" id="wikiQaDetails">' in html
    assert '<details class="analysis-collapsible" id="stockJournalDetails">' in html
    assert '<summary class="analysis-collapsible-summary">💬 이 종목에 대해 질문하기</summary>' in html
    assert '<summary class="analysis-collapsible-summary">📝 투자 일지</summary>' in html
    assert 'id="wikiQa"' in html
    assert 'id="stockJournalSection"' in html
    # 순서 계약은 기존 테스트(investment_journal)가 이미 지킨다 — 여기서는 접힘 래퍼
    # 자체와, summary 로 대체된 라벨이 접근성 이름으로는 남아있는지만 확인한다.
    assert '<label for="wikiQaInput" class="wiki-qa-label sr-only">' in html
    assert "list-style: none" in styles
    assert "::-webkit-details-marker" in styles


def test_performance_tab_has_sticky_anchor_nav_covering_every_card():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    # 심층 분석 탭이 카드 10개짜리 단일 스크롤이라 탐색성이 낮았다(UX 감사 P2⑥).
    # 순수 <a href="#id"> 앵커라 JS 없이 동작 — 대상 id 10개가 전부 존재해야 한다.
    anchor_targets = [
        "pfNavWrap", "pfRiskWrap", "pfPeriodReportWrap", "pfRebalanceWrap",
        "pfDivCalWrap", "pfJournalWrap", "pfValueWrap", "pfGroupWeightWrap",
        "pfCashflowWrap", "pfAiWrap",
    ]
    nav_start = html.find('<nav class="pf-deep-anchors"')
    nav_end = html.find("</nav>", nav_start)
    assert nav_start != -1 and nav_end != -1
    anchor_block = html[nav_start:nav_end]
    for target_id in anchor_targets:
        assert f'href="#{target_id}"' in anchor_block, f"missing anchor link for #{target_id}"
        assert f'id="{target_id}"' in html, f"missing scroll target #{target_id}"
    # 점프 내비는 앵커 바보다 먼저, 첫 카드(NAV)보다는 앞에 온다.
    assert html.find('id="pfNavWrap"') > nav_end

    # 모바일 슬림 헤더(--m-header-h, 데스크톱에선 미정의라 0px 로 빠짐) 밑에 붙고,
    # 점프 대상은 헤더 + 이 바 높이만큼 scroll-margin 을 둬 제목이 가려지지 않는다.
    assert "top: var(--m-header-h, 0px);" in styles
    assert "scroll-margin-top: calc(var(--m-header-h, 0px) + 52px);" in styles
