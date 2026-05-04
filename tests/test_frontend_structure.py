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
