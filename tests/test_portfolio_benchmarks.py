from services.portfolio import benchmarks as bm


def test_default_benchmark_for_common_portfolio_assets():
    assert bm.default_benchmark_for_code("CASH_USD") == "FX_USDKRW"
    assert bm.default_benchmark_for_code("CASH_EUR") == "FX_EURKRW"
    assert bm.default_benchmark_for_code("KRX_GOLD") == "FX_USDKRW"
    assert bm.default_benchmark_for_code("AAPL") == "IDX_SP500"


def test_default_benchmark_for_korean_stocks_and_preferred_shares():
    assert bm.default_benchmark_for_code("005930") == "IDX_KOSPI"
    assert bm.default_benchmark_for_code("005930", market_type="KOSDAQ") == "IDX_KOSDAQ"
    assert bm.default_benchmark_for_code("33637K") == "336370"


def test_benchmark_name_fast_uses_builtins_cache_and_items():
    assert bm.benchmark_name_fast("IDX_KOSPI") == "코스피"
    assert bm.benchmark_name_fast("123456", name_cache={"123456": "캐시명"}) == "캐시명"
    assert bm.benchmark_name_fast("654321", items=[{"stock_code": "654321", "stock_name": "종목명"}]) == "종목명"
    assert bm.benchmark_name_fast("UNKNOWN") == "UNKNOWN"


def test_indicator_to_change_pct_returns_signed_float():
    assert bm.indicator_to_change_pct({"change_pct": "0.71%", "direction": "up"}) == 0.71
    assert bm.indicator_to_change_pct({"change_pct": "0.71%", "direction": "down"}) == -0.71
    assert bm.indicator_to_change_pct({"change_pct": "-0.71%", "direction": "down"}) == -0.71
    assert bm.indicator_to_change_pct({"change_pct": "", "direction": "up"}) is None


def test_benchmark_quote_cache_can_return_stale_marker():
    cache = bm.BenchmarkQuoteCache(ttl_seconds=0)
    cache.set("IDX_KOSPI", {"change_pct": 1.23})

    assert cache.get("IDX_KOSPI", allow_stale=False) is None
    assert cache.get("IDX_KOSPI") == {"change_pct": 1.23, "_stale": True}
