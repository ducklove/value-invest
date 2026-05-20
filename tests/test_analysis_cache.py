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
