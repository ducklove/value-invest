from datetime import datetime

from routes import analysis


def test_analysis_snapshot_negative_annual_price_is_invalid():
    snapshot = {
        "indicators": {
            "주가 (원)": [
                {"year": 2000, "value": -35781.39},
                {"year": 2001, "value": 47503.0},
            ],
            "ROE (%)": [{"year": 2000, "value": -10.5}],
        }
    }

    assert analysis._analysis_snapshot_has_invalid_prices(snapshot)


def test_analysis_snapshot_negative_non_price_metric_is_allowed():
    snapshot = {
        "indicators": {
            "ROE (%)": [{"year": 2000, "value": -10.5}],
        }
    }

    assert not analysis._analysis_snapshot_has_invalid_prices(snapshot)


def test_analysis_snapshot_missing_annual_price_is_invalid():
    snapshot = {
        "indicators": {
            "주가 (원)": [{"year": 2000, "value": None}],
        }
    }

    assert analysis._analysis_snapshot_has_invalid_prices(snapshot)


def test_analysis_snapshot_uses_first_indicator_when_price_label_is_mojibake():
    snapshot = {
        "indicators": {
            "?? (?)": [{"year": 2000, "value": None}],
            "PER": [{"year": 2000, "value": 1.2}],
        }
    }

    assert analysis._analysis_snapshot_has_invalid_prices(snapshot)


def test_legacy_snapshot_with_only_prices_and_dividends_is_rebuilt_once():
    snapshot = {
        "analyzed_at": datetime.now().isoformat(),
        "weekly_indicators": {
            "주가": [{"value": 1000}],
            "배당수익률 (%)": [{"value": 0}],
            **{key: [{"value": None}] for key in analysis._VALUATION_HISTORY_KEYS},
        },
    }

    assert not analysis._analysis_snapshot_is_usable(snapshot)
    # An issuer can legitimately lack annual results after trying both sources.
    snapshot["valuation_history_version"] = analysis._VALUATION_HISTORY_VERSION
    assert analysis._analysis_snapshot_is_usable(snapshot)
