from services.portfolio import quotes


def test_quote_from_ws_normalizes_realtime_payload():
    assert quotes.quote_from_ws({
        "date": "20260509",
        "price": 1000,
        "previous_close": 990,
        "change": 10,
        "change_pct": 1.01,
        "trade_value": 123456,
    }) == {
        "date": "20260509",
        "price": 1000,
        "previous_close": 990,
        "change": 10,
        "change_pct": 1.01,
        "trade_value": 123456,
    }
    assert quotes.quote_from_ws({"price": None}) is None
    assert quotes.quote_from_ws(None) is None


def test_portfolio_quote_cache_returns_fresh_copy():
    cache = quotes.PortfolioQuoteCache(ttl_seconds=60)
    assert cache.remember("005930", {"price": 1000, "change_pct": 1.2})

    fresh = cache.get_fresh("005930")
    assert fresh == {"price": 1000, "change_pct": 1.2}

    fresh["price"] = 1
    assert cache.get_fresh("005930") == {"price": 1000, "change_pct": 1.2}


def test_portfolio_quote_cache_marks_expired_fresh_quote_stale():
    cache = quotes.PortfolioQuoteCache(ttl_seconds=0)
    cache.remember("005930", {"price": 1000})

    assert cache.get_fresh("005930") is None
    assert cache.get_cached("005930") == {"price": 1000, "_stale": True}
    assert cache.get_fallback("005930") == {"price": 1000}


def test_portfolio_quote_cache_ignores_empty_or_priceless_quotes():
    cache = quotes.PortfolioQuoteCache()

    assert not cache.remember("005930", {})
    assert not cache.remember("005930", {"price": None})
    assert cache.get_cached("005930") == {}
