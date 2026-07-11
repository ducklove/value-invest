"""포트폴리오 화면 구조 계약 — 보유종목 테이블·성과 탭 카드·현금흐름·시세 폴링."""
# 구 test_frontend_structure.py(1,500줄 단일 파일)를 2026-07-07 화면별로 분할.
# 새 화면 계약 테스트는 해당 화면 파일에 추가한다. 공용 경로/상수/헬퍼는
# tests/_frontend_structure.py 에 있다.
from _frontend_structure import JS, PORTFOLIO_SPLIT_FILES, ROOT, STATIC, _all_css


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

def test_portfolio_ai_result_has_framed_output_contract():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()
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

def test_portfolio_ai_card_uses_css_classes_instead_of_inline_styles():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()

    # 긴 인라인 스타일은 portfolio.css 클래스로 이전(시각 동일) —
    # pfAiModelPicker 의 display:none 만 JS 계약(portfolio-ai.js 가
    # style.display='' 로 해제)이라 인라인에 남는다.
    assert 'id="pfAiModelPicker" class="pf-ai-model-picker" style="display:none;"' in html
    assert 'id="pfAiModelInput" class="pf-ai-model-input"' in html
    assert 'class="bt-run pf-ai-run-btn"' in html
    assert 'id="pfAiQuery" class="pf-ai-query"' in html
    assert "width:260px" not in html
    assert "resize:vertical" not in html
    assert ".pf-ai-model-picker { margin-left: 8px; }" in styles
    assert ".pf-ai-model-input" in styles
    assert ".pf-ai-run-btn { margin-left: auto; padding: 6px 14px; font-size: 12px; }" in styles
    assert ".pf-ai-query" in styles

def test_portfolio_summary_card_has_single_merged_definition():
    # 기본 정의(§23)와 파일 말미 액션트 보더 블록의 이중 정의를 병합 —
    # border 축약 '뒤'에 border-left 가 와야 캐스케이드 의미가 보존된다.
    portfolio_css = (ROOT / "static" / "css" / "portfolio.css").read_text(encoding="utf-8")
    assert portfolio_css.count(".pf-summary-card {") == 1
    base_block = portfolio_css.split(".pf-summary-card {", 1)[1].split("}", 1)[0]
    assert "border: 1px solid var(--border)" in base_block
    assert base_block.find("border: 1px solid var(--border)") < base_block.find("border-left: 3px solid transparent")
    assert "transition: border-color 0.2s ease" in base_block
    assert ".pf-summary-card:has(.positive) { border-left-color: var(--up); }" in portfolio_css
    assert ".pf-summary-card:has(.negative) { border-left-color: var(--down); }" in portfolio_css

def test_household_palette_reads_css_tokens_with_fallback():
    source = (JS / "portfolio-household.js").read_text(encoding="utf-8")
    styles = _all_css()

    # 색은 하드코딩 대신 CSS 토큰(--hh-*)을 렌더 시점에 읽는 pfHhColor 를 쓴다
    # (_sparkTrendColor 선례). 토큰 미정의 시 종전 팔레트 폴백.
    assert "function pfHhColor" in source
    assert "PF_HH_COLOR_FALLBACKS" in source
    assert "PF_HH_COLORS" not in source
    assert "`--hh-${" in source
    # 라이트/다크 쌍이 base.css 에 정의돼 있다.
    assert styles.count("--hh-portfolio:") == 2
    assert styles.count("--hh-liability:") == 2
    assert "--hh-real-estate:" in styles
    # 다크 전환 시 재렌더 경로 — data-theme 를 관찰해 인사이트를 다시 그린다.
    assert "attributeFilter: ['data-theme']" in source
    assert "_pfHouseholdRenderInsights();" in source

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
    # 색은 하드코딩 대신 CSS 토큰(--up/--down)을 읽는 _sparkTrendColor 를 쓴다.
    assert "function _sparkTrendColor" in source
    assert "_drawSparklinePoints('sparkDaily', raw, _sparkTrendColor(lastPct >= 0), _dailyAxisHours);" in source
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
    styles = _all_css()
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
    styles = _all_css()

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
    assert "await apiFetchJson('/api/portfolio/order', {" in order
    assert "errorMessage: 'Portfolio order save failed'," in order
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
    styles = _all_css()
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
    assert "cache: 'no-store'," in source
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
    styles = _all_css()

    assert 'type="button" class="pf-row-btn save js-pf-save"' in render
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
    rest_fetch_loop = source.split("const data = await apiFetchJson('/api/asset-quotes',", 1)[1].split("} catch", 1)[0]
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
    styles = _all_css()

    assert '<td class="pf-col-num pf-col-benchmark js-pf-bench-picker" title="벤치마크 변경">' in render
    assert '<td class="pf-col-num pf-col-benchmark" title="수정모드에서 변경">' in render
    assert "if (code && PfStore.edit.code === code) pfShowBenchmarkPicker(code, el);" in events
    assert events.find("((el = t.closest('.js-pf-bench-set')))") < events.find("((el = t.closest('.js-pf-bench-picker')))")
    assert "if (PfStore.edit.code !== stockCode)" in actions
    assert ".pf-col-benchmark.js-pf-bench-picker { cursor: pointer; }" in styles

def test_nav_chart_shows_selected_benchmark_beta_overlay():
    benchmark = (JS / "portfolio-trends-benchmark.js").read_text(encoding="utf-8")
    trends = (JS / "portfolio-trends.js").read_text(encoding="utf-8")
    styles = _all_css()

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
    styles = _all_css()
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
    styles = _all_css()
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
    styles = _all_css()
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
    styles = _all_css()
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
    styles = _all_css()
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

def test_performance_tab_has_sticky_anchor_nav_covering_every_card():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()

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
