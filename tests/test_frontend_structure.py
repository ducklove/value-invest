from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static"
JS = STATIC / "js"


PORTFOLIO_SPLIT_FILES = [
    "portfolio-shell.js",
    "portfolio-data.js",
    "portfolio-render.js",
    "portfolio-actions.js",
    "portfolio-insights.js",
    "portfolio-groups-market.js",
    "portfolio-ai.js",
    "portfolio-performance.js",
    "portfolio-trends.js",
    "portfolio-group-composition.js",
    "portfolio-cashflows.js",
    "portfolio-events.js",
]


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


def test_today_card_does_not_fallback_to_quote_session_return():
    source = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    forbidden = "totalDailyPnl / prevMV"
    assert forbidden not in source, "TODAY card must stay on the 22:00 settlement baseline, not quote-session math"


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


def test_portfolio_stock_click_uses_explicit_insight_link_handler():
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    events = (JS / "portfolio-events.js").read_text(encoding="utf-8")

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
    assert "!pfEditingCode && (el = portfolioRowFromTarget(t))" in events
    assert "if (code) pfGoAnalyze(code, e);" in events


def test_portfolio_edit_save_is_row_scoped_and_safe():
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    actions = (JS / "portfolio-actions.js").read_text(encoding="utf-8")
    events = (JS / "portfolio-events.js").read_text(encoding="utf-8")

    assert 'type="button" class="pf-row-btn save js-pf-save"' in render
    assert "let pfSavingEditCode = null" in (JS / "portfolio-shell.js").read_text(encoding="utf-8")
    assert "const isSaving = pfSavingEditCode === r.stock_code" in render
    assert 'class="pf-row-saving" aria-busy="true"' in render
    assert "pf-save-spinner" in render
    assert 'class="pf-edit-input js-pf-edit-qty"' in render
    assert 'class="pf-edit-input js-pf-edit-price"' in render
    assert 'class="pf-edit-input js-pf-edit-target"' in render
    assert "savePortfolioEdit(code, undefined, el.closest('tr[data-code]'))" in events
    assert "function _pfFindEditRow(stockCode, row)" in actions
    assert "function _pfSetEditSaving(stockCode, saving, row)" in actions
    assert "if (pfSavingEditCode) return;" in actions
    assert "_pfSetEditSaving(stockCode, true, editRow)" in actions
    assert "editRow?.querySelector('.js-pf-edit-qty')" in actions
    assert "editRow?.querySelector('.js-pf-edit-price')" in actions
    assert "/api/portfolio/${encodeURIComponent(stockCode)}" in actions
    assert "/api/portfolio/${encodeURIComponent(stockCode)}/benchmark" in actions


def test_portfolio_add_canonicalizes_alias_before_save():
    source = (JS / "portfolio-actions.js").read_text(encoding="utf-8")

    assert "portfolio code canonicalization failed" in source
    assert "/api/portfolio/resolve-name?code=${encodeURIComponent(resolvedCode)}" in source
    assert "/api/portfolio/${encodeURIComponent(resolvedCode)}" in source
    assert "pfEditingCode = resolvedCode" in source


def test_quote_manager_polls_stale_websocket_quotes_as_rest_fallback():
    source = (JS / "quote-manager.js").read_text(encoding="utf-8")

    assert "QUOTE_MANAGER_STALE_WS_MS" in source
    assert "lastQuoteAt: {}" in source
    assert "_markQuoteFresh(msg.code)" in source
    assert "_markQuoteFresh(code)" in source
    assert "async _pollAll()" in source
    assert "this._getStaleWsCodes().forEach(c => allCodes.add(c));" in source


def test_benchmark_picker_only_opens_in_edit_mode():
    render = (JS / "portfolio-render.js").read_text(encoding="utf-8")
    actions = (JS / "portfolio-actions.js").read_text(encoding="utf-8")
    events = (JS / "portfolio-events.js").read_text(encoding="utf-8")
    styles = (STATIC / "styles.css").read_text(encoding="utf-8")

    assert '<td class="pf-col-num pf-col-benchmark js-pf-bench-picker" title="벤치마크 변경">' in render
    assert '<td class="pf-col-num pf-col-benchmark" title="수정모드에서 변경">' in render
    assert "if (code && pfEditingCode === code) pfShowBenchmarkPicker(code, el);" in events
    assert events.find("((el = t.closest('.js-pf-bench-set')))") < events.find("((el = t.closest('.js-pf-bench-picker')))")
    assert "if (pfEditingCode !== stockCode)" in actions
    assert ".pf-col-benchmark.js-pf-bench-picker { cursor: pointer; }" in styles


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
