from unittest.mock import AsyncMock, patch
import time

import pytest

from routes import portfolio as portfolio_route
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


def test_quote_from_ws_rejects_old_payload_when_max_age_requested():
    old = {"price": 1000, "ts": time.time() - 120}

    assert quotes.quote_from_ws(old, max_age_seconds=90) is None
    assert quotes.quote_from_ws({**old, "ts": time.time()}, max_age_seconds=90)["price"] == 1000
    assert quotes.quote_from_ws({"price": 1000}, max_age_seconds=90) is None


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


def test_cached_quote_for_code_ignores_stale_polling_cache():
    cache = quotes.PortfolioQuoteCache(ttl_seconds=0)
    cache.remember("005930", {"price": 1000})

    with patch.object(portfolio_route, "_quote_cache", cache), \
         patch.object(portfolio_route.kis_ws_manager, "get_cached_quote", return_value=None):
        assert portfolio_route._cached_quote_for_code("005930") == {}


@pytest.mark.asyncio
async def test_asset_quotes_batch_fresh_korean_quotes_force_refresh_without_ws_cache():
    with patch.object(
        portfolio_route,
        "_fetch_quote",
        new=AsyncMock(return_value={"price": 2000}),
    ) as fetch_quote:
        result = await portfolio_route.asset_quotes_batch({
            "codes": ["005930"],
            "fresh": True,
        })

    assert result == {"005930": {"price": 2000}}
    fetch_quote.assert_awaited_once_with(
        "005930",
        force_refresh=True,
        use_ws_cache=False,
    )


@pytest.mark.asyncio
async def test_asset_quotes_batch_fresh_non_korean_quotes_keep_normal_cache_path():
    with patch.object(
        portfolio_route,
        "_fetch_quote",
        new=AsyncMock(return_value={"price": 2000}),
    ) as fetch_quote:
        result = await portfolio_route.asset_quotes_batch({
            "codes": ["AAPL"],
            "fresh": True,
        })

    assert result == {"AAPL": {"price": 2000}}
    fetch_quote.assert_awaited_once_with(
        "AAPL",
        force_refresh=False,
        use_ws_cache=True,
    )
