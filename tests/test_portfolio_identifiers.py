from services.portfolio import identifiers as ids


def test_normalize_and_classify_portfolio_codes():
    assert ids.normalize_portfolio_code(" a200.ax ") == "A200.AX"
    assert ids.is_korean_stock("0074K0")
    assert ids.is_korean_stock("005930")
    assert ids.is_preferred_stock("33637K")
    assert not ids.is_preferred_stock("005930")
    assert ids.common_stock_code("33637K") == "336370"


def test_cash_and_special_asset_identifiers():
    assert ids.is_cash_asset("cash_usd")
    assert ids.is_special_asset("CASH_USD")
    assert ids.is_special_asset("KRX_GOLD")
    assert ids.CASH_FX_CODE["CASH_USD"] == "FX_USDKRW"
    assert ids.CASH_NAMES["CASH_KRW"] == "원화"


def test_static_foreign_ticker_shortcuts():
    a200 = ids.static_foreign_ticker("a200")
    eun2 = ids.static_foreign_ticker("EUN2.DE")
    brk_b_dot = ids.static_foreign_ticker("BRK.B")
    brk_b_slash = ids.static_foreign_ticker("BRK/B")

    assert a200 and a200["ticker"] == "A200.AX"
    assert eun2 and eun2["currency"] == "EUR"
    assert brk_b_dot and brk_b_dot["ticker"] == "BRK-B"
    assert brk_b_slash and brk_b_slash["ticker"] == "BRK-B"
    assert ids.static_foreign_ticker("UNKNOWN") is None
