from datetime import date, timedelta

import asset_insights


def _rows(values):
    start = date(2025, 1, 1)
    return [
        {"date": (start + timedelta(days=i)).isoformat(), "close": value}
        for i, value in enumerate(values)
    ]


def test_classify_asset_covers_non_domestic_asset_types():
    assert asset_insights.classify_asset("CASH_USD")["assetClass"] == "cash"
    assert asset_insights.classify_asset("KRX_GOLD")["assetClass"] == "gold"
    assert asset_insights.classify_asset("CRYPTO_BTC")["assetClass"] == "crypto"
    assert asset_insights.classify_asset("TLT", "iShares 20+ Year Treasury Bond ETF", "USD")["assetClass"] == "bond_etf"
    assert asset_insights.classify_asset("SPY", "SPDR S&P 500 ETF Trust", "USD")["assetClass"] == "foreign_etf"
    assert asset_insights.classify_asset("AAPL", "Apple", "USD")["assetClass"] == "foreign_stock"
    assert asset_insights.classify_asset("MSFT", "Microsoft")["assetClass"] == "foreign_stock"


def test_calculate_history_metrics_returns_risk_and_window_stats():
    values = list(range(100, 400))
    metrics = asset_insights.calculate_history_metrics(_rows(values))

    assert metrics["historyPoints"] == 300
    assert metrics["returns"]["1m"] == asset_insights.percent_change(399, 378)
    assert metrics["returns"]["3m"] == asset_insights.percent_change(399, 336)
    assert metrics["volatility"]["20d"] is not None
    assert metrics["volatility"]["60d"] is not None
    assert metrics["maxDrawdownPct"] == 0.0
    assert metrics["fromHigh52Pct"] == 0.0
    assert metrics["fromLow52Pct"] > 0


def test_calculate_history_metrics_detects_drawdown():
    metrics = asset_insights.calculate_history_metrics(_rows([100, 120, 90, 110]))

    assert metrics["maxDrawdownPct"] == -25.0
    assert metrics["high52"] == 120
    assert metrics["low52"] == 90


def test_relative_returns_and_signals():
    relative = asset_insights.relative_returns({"1m": 4.5, "3m": -2.0}, {"1m": 1.5, "3m": 3.0})
    assert relative == {"1m": 3.0, "3m": -5.0}

    signals = asset_insights.build_signals(
        {"assetClass": "bond_etf"},
        {"returnPct": -20.0},
        {"fromHigh52Pct": -25.0, "volatility": {"60d": 40.0}},
        {"relativeReturns": {"3m": -6.0}},
    )
    levels = {signal["level"] for signal in signals}
    assert "warning" in levels
    assert "info" in levels
