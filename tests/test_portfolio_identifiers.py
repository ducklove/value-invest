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


def test_yahoo_symbol_maps_hub_reuters_codes():
    # Reuters 미국 거래소 접미사 제거, 호찌민(.HM) → Yahoo .VN.
    assert ids.yahoo_symbol("GOOGL.O") == "GOOGL"
    assert ids.yahoo_symbol("AGNC.O") == "AGNC"
    assert ids.yahoo_symbol("AAPL.OQ") == "AAPL"
    assert ids.yahoo_symbol("XOM.N") == "XOM"
    assert ids.yahoo_symbol("SOXL.K") == "SOXL"
    assert ids.yahoo_symbol("ABCD.PK") == "ABCD"
    assert ids.yahoo_symbol("FUEVFVND.HM") == "FUEVFVND.VN"
    assert ids.yahoo_symbol("googl.o") == "GOOGL"


def test_yahoo_symbol_keeps_yahoo_symbols_and_class_shares():
    for symbol in ("AAPL", "7203.T", "BP.L", "SAP.F", "ABC.V", "A200.AX", "EUN2.DE", "0005.HK",
                   "83188.HK", "600519.SS", "005930.KS", "^GSPC", "KRW=X", "GC=F", "BTC-USD", "BRK-B"):
        assert ids.yahoo_symbol(symbol) == symbol
    # 클래스 주식은 Yahoo 대시 표기. .A 는 NYSE American 이 아니라 클래스 A 로 본다.
    assert ids.yahoo_symbol("BRK.B") == "BRK-B"
    assert ids.yahoo_symbol("BRK/B") == "BRK-B"
    assert ids.yahoo_symbol("BF.A") == "BF-A"
    assert ids.yahoo_symbol("CASH_CNY") == "CASH_CNY"
    assert ids.yahoo_symbol("") == ""
    assert ids.yahoo_symbol(None) == ""


def test_yahoo_symbol_is_idempotent():
    for code in ("GOOGL.O", "FUEVFVND.HM", "BRK.B", "BRK/B", "7203.T", "BP.L", "AAPL"):
        once = ids.yahoo_symbol(code)
        assert ids.yahoo_symbol(once) == once
