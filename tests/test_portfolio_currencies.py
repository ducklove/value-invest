from services.portfolio import currencies, foreign, history


def test_yahoo_currency_inference_is_shared_across_quote_and_history_paths():
    expected = {
        "7203.T": "JPY",
        "0005.HK": "HKD",
        "600519.SS": "CNY",
        "VOD.L": "GBP",
        "A200.AX": "AUD",
        "SHOP.TO": "CAD",
        "EUN2.DE": "EUR",
        "BRK-B": "USD",
    }

    for ticker, currency in expected.items():
        assert currencies.infer_yf_currency(ticker) == currency
        assert foreign.infer_yf_currency(ticker) == currency
        assert history.infer_yf_currency(ticker) == currency

    assert foreign.infer_yf_currency is currencies.infer_yf_currency
    assert history.infer_yf_currency is currencies.infer_yf_currency
