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
    "portfolio-actions.js",
    "portfolio-insights.js",
    "portfolio-groups-market.js",
    "portfolio-ai.js",
    "portfolio-performance.js",
    "portfolio-trends.js",
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


def test_economic_calendar_result_alert_checkbox_contract():
    js = (JS / "economic-calendar.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    # 결과 미발표 + 아직 지나지 않은 행에만 🔔 체크박스(과거 일정은 숨김).
    assert "ec-bell-cb" in js
    assert "function _ecBellCell(ev)" in js
    assert "function _ecIsPast(ev)" in js
    assert "if (!eid || _ecIsPast(ev)) return" in js
    # 구독한 일정의 결과가 발표되면 🔔 마커 + 행 배경 강조로 눈에 띄게 한다.
    assert "ec-bell-done" in js
    assert "ec-row-alerted" in js
    assert ".ec-row-alerted" in styles
    assert "async function _ecToggleSubscription(cb)" in js
    # 게이트: 비로그인 → 로그인 팝업 / 채널 무 → 포트폴리오 알림 안내.
    assert "function _ecPromptLogin()" in js
    assert "function _ecPromptChannel()" in js
    assert "if (!currentUser) { cb.checked = false; _ecPromptLogin(); return; }" in js
    # 채널 보유는 서버 409 가 단일 진실원(클라이언트 사전체크 오판 방지).
    assert "_ecHasActiveChannel" not in js
    assert "if (r.status === 409) { cb.checked = false; _ecPromptChannel(); return; }" in js
    # currentUser 는 utils.js 의 전역 let — window 속성이 아니므로 bare 참조여야 한다.
    assert "window.currentUser" not in js
    assert "/api/notifications/calendar" in js
    assert "pfOpenAlerts" in js
    assert ".ec-bell-cb:checked + .ec-bell-ico" in styles


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
    main_open = html.index('<div class="main">')
    tabbar_pos = html.index('id="mobileTabbar"')
    tape_pos = html.index('id="marketTape"')
    chart_modal_pos = html.index('id="chartModal"')
    assert main_open < tabbar_pos < tape_pos < chart_modal_pos
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
    assert forbidden not in source, "TODAY card must stay on the 22:00 settlement baseline, not quote-session math"


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
    # y 는 직전 22:00 결산(prevClose) 대비 등락% 유지(축만 바뀜).
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
    source = (JS / "portfolio-tag-summary.js").read_text(encoding="utf-8")

    assert html.find("./js/portfolio-tag-summary.js") < html.find("./js/portfolio-events.js")
    assert "js-pf-open-tag-summary" in data
    assert "data-tag=\"${safeTag}\"" in data
    assert "pfOpenTagSummary(el.dataset.tag || '')" in events
    assert "function pfOpenTagSummary(tag)" in source
    assert "baseValue > 0 ? dailyPnl / baseValue * 100 : null" in source
    assert "tagStats.dailyReturnPct - allStats.dailyReturnPct" in source
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

    assert 'type="button" class="pf-row-btn save js-pf-save"' in render
    # Edit state lives in PfStore.edit (portfolio-store.js).
    assert "edit: { code: null, savingCode: null }," in (JS / "portfolio-store.js").read_text(encoding="utf-8")
    assert "const isSaving = PfStore.edit.savingCode === r.stock_code" in render
    assert 'class="pf-row-saving" aria-busy="true"' in render
    assert "pf-save-spinner" in render
    assert 'class="pf-edit-input js-pf-edit-qty"' in render
    assert 'class="pf-edit-input js-pf-edit-price"' in render
    assert 'class="pf-edit-input js-pf-edit-target"' in render
    assert "savePortfolioEdit(code, undefined, el.closest('tr[data-code]'))" in events
    assert "function _pfFindEditRow(stockCode, row)" in actions
    assert "function _pfSetEditSaving(stockCode, saving, row)" in actions
    assert "if (PfStore.edit.savingCode) return;" in actions
    assert "_pfSetEditSaving(stockCode, true, editRow)" in actions
    assert "editRow?.querySelector('.js-pf-edit-qty')" in actions
    assert "editRow?.querySelector('.js-pf-edit-price')" in actions
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
    assert "PfStore.edit.code = resolvedCode" in source


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
    assert "if (incomingRank < currentRank) return false;" in utils
    assert "if (incomingRank > currentRank) return true;" in utils
    assert utils.find("if (incomingRank < currentRank) return false;") < utils.find("const currentTime = quoteSnapshotTimeValue(current);")
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
    trends = (JS / "portfolio-trends.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    assert "function _computeReturnStats" in trends
    assert "function _navBenchmarkBeta" in trends
    assert "function _updateNavBetaOverlay" in trends
    assert "NAV beta vs selected benchmark" in trends
    assert "\\u03b2" in trends
    assert "\\u00b2" in trends
    assert "_updateNavBetaOverlay(labels, navValues, benchCodes, fullWindow.startIdx, fullWindow.endIdx);" in trends
    assert "_updateNavBetaOverlay(labels, navValues, benchCodes, startIdx, endIdx);" in trends
    assert ".pf-nav-beta-overlay" in styles
    assert ".pf-nav-beta-chip" in styles


def test_performance_tab_includes_group_weight_trend():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")
    data = (JS / "portfolio-data.js").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")
    trends = (JS / "portfolio-trends.js").read_text(encoding="utf-8")
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
    assert "async function renderGroupWeightChart(rows)" in trends
    assert "async function _waitForChartContainer" in trends
    assert "container.offsetParent === null" in trends
    assert "/api/portfolio/group-constituent-history" in composition
    assert "async function pfShowGroupComposition(groupName)" in composition
    assert "_groupCompositionRequestSeq" in composition
    assert "stack: 'groupComposition'" in composition
    assert "stock_count" in trends
    assert "stack: 'groupWeight'" in trends
    assert "areaStyle: { opacity:" in trends
    assert "stack: series.stack || null" in chart
    assert "stackedIndexes.add(seriesIdx)" in chart


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
