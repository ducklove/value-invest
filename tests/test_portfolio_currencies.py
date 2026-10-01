from unittest.mock import AsyncMock, patch

import pytest

from services.portfolio import currencies, foreign, history


def test_yahoo_currency_inference_is_shared_across_quote_and_history_paths():
    expected = {
        "0074K0": "KRW",
        " 0074k0 ": "KRW",
        "005930": "KRW",
        "005930.KS": "KRW",
        "247540.KQ": "KRW",
        "7203.T": "JPY",
        "0005.HK": "HKD",
        "83188.HK": "CNY",
        "83199.HK": "CNY",
        " 83188.hk ": "CNY",
        "80000.HK": "CNY",
        "89999.HK": "CNY",
        "8388.HK": "HKD",
        "08388.HK": "HKD",
        "83188.T": "JPY",
        "600519.SS": "CNY",
        "VOD.L": "GBP",
        "A200.AX": "AUD",
        "SHOP.TO": "CAD",
        "EUN2.DE": "EUR",
        "FUEVFVND.HM": "VND",
        "BVS.HN": "VND",
        "BRK-B": "USD",
    }

    for ticker, currency in expected.items():
        assert currencies.infer_yf_currency(ticker) == currency
        assert foreign.infer_yf_currency(ticker) == currency
        assert history.infer_yf_currency(ticker) == currency

    assert foreign.infer_yf_currency is currencies.infer_yf_currency
    assert history.infer_yf_currency is currencies.infer_yf_currency


@pytest.mark.asyncio
async def test_domestic_currency_detection_skips_foreign_lookup():
    with patch.object(foreign, "yfinance_find_ticker", new_callable=AsyncMock) as lookup:
        assert await foreign.detect_currency("0074K0") == "KRW"
        lookup.assert_not_awaited()
