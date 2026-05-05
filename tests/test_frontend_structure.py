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


def test_performance_tab_includes_group_weight_trend():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")
    trends = (JS / "portfolio-trends.js").read_text(encoding="utf-8")
    chart = (JS / "portfolio-trend-chart.js").read_text(encoding="utf-8")
    composition = (JS / "portfolio-group-composition.js").read_text(encoding="utf-8")

    assert "pfGroupWeightChart" in html
    assert "pfGroupCompositionChart" in html
    assert "100% 누적 면적 차트" in html
    assert "/api/portfolio/group-weight-history" in performance
    assert "renderGroupWeightChart(groupWeightData)" in performance
    assert "async function renderGroupWeightChart(rows)" in trends
    assert "/api/portfolio/group-constituent-history" in composition
    assert "async function pfShowGroupComposition(groupName)" in composition
    assert "stack: 'groupComposition'" in composition
    assert "stock_count" in trends
    assert "stack: 'groupWeight'" in trends
    assert "areaStyle: { opacity:" in trends
    assert "stack: series.stack || null" in chart
    assert "stackedIndexes.add(seriesIdx)" in chart
