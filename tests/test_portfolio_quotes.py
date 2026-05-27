import asyncio
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
        "ts": 123.0,
    }) == {
        "date": "20260509",
        "price": 1000,
        "previous_close": 990,
        "change": 10,
        "change_pct": 1.01,
        "trade_value": 123456,
        "source": "ws",
        "ts": 123.0,
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


def test_portfolio_quote_cache_ignores_stale_quotes():
    cache = quotes.PortfolioQuoteCache()

    assert not cache.remember("005930", {"price": 1000, "_stale": True})
    assert cache.get_cached("005930") == {}


def test_portfolio_quote_cache_keeps_rest_quote_over_later_history_fallback():
    cache = quotes.PortfolioQuoteCache()

    assert cache.remember("005930", {
        "date": "2026-05-26",
        "price": 1000,
        "source": "rest",
        "fetched_at": "2026-05-26T09:01:00",
    })
    assert not cache.remember("005930", {
        "date": "2026-05-26",
        "price": 990,
        "source": "history",
        "fetched_at": "2026-05-26T09:02:00",
    })
    assert cache.get_fresh("005930") == {
        "date": "2026-05-26",
        "price": 1000,
        "source": "rest",
        "fetched_at": "2026-05-26T09:01:00",
    }


def test_portfolio_quote_cache_accepts_newer_same_source_quote():
    cache = quotes.PortfolioQuoteCache()

    assert cache.remember("005930", {
        "date": "2026-05-26",
        "price": 1000,
        "source": "rest",
        "fetched_at": "2026-05-26T09:01:00",
    })
    assert cache.remember("005930", {
        "date": "2026-05-26",
        "price": 1010,
        "source": "rest",
        "fetched_at": "2026-05-26T09:02:00",
    })
    assert cache.get_fresh("005930")["price"] == 1010


def test_cached_quote_for_code_ignores_stale_polling_cache():
    cache = quotes.PortfolioQuoteCache(ttl_seconds=0)
    cache.remember("005930", {"price": 1000})

    with patch.object(portfolio_route, "_quote_cache", cache), \
         patch.object(portfolio_route.kis_ws_manager, "get_cached_quote", return_value=None):
        assert portfolio_route._cached_quote_for_code("005930") == {}


def test_cached_quote_for_code_ignores_ws_cache_when_market_differs():
    cache = quotes.PortfolioQuoteCache(ttl_seconds=60)
    cache.remember("000660", {"price": 1818000, "source": "rest", "market": "NX"})

    with patch.object(portfolio_route, "_quote_cache", cache), \
         patch.object(portfolio_route.kis_ws_manager, "ws_cache_matches_rest_market", return_value=False), \
         patch.object(portfolio_route.kis_ws_manager, "get_cached_quote", return_value={
             "date": "20260521",
             "price": 1745000,
             "ts": time.time(),
         }):
        assert portfolio_route._cached_quote_for_code("000660") == {
            "price": 1818000,
            "source": "rest",
            "market": "NX",
        }


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


@pytest.mark.asyncio
async def test_asset_quotes_batch_returns_stale_fallback_on_fetch_timeout():
    cache = quotes.PortfolioQuoteCache()
    cache.remember("000660", {"price": 1786000})

    with patch.object(portfolio_route, "_quote_cache", cache), \
         patch.object(
             portfolio_route,
             "_fetch_quote",
             new=AsyncMock(side_effect=asyncio.TimeoutError()),
         ):
        result = await portfolio_route.asset_quotes_batch({
            "codes": ["000660"],
            "fresh": True,
        })

    assert result == {"000660": {"price": 1786000, "_stale": True}}


@pytest.mark.asyncio
async def test_asset_quotes_batch_returns_fallback_for_pending_batch_timeout():
    cache = quotes.PortfolioQuoteCache()
    cache.remember("000660", {"price": 1786000})

    async def slow_fetch(*args, **kwargs):
        await asyncio.sleep(1)
        return {"price": 1}

    with patch.object(portfolio_route, "_quote_cache", cache), \
         patch.object(portfolio_route, "_ASSET_QUOTES_BATCH_TIMEOUT", 0.01), \
         patch.object(portfolio_route, "_fetch_quote", new=slow_fetch):
        result = await portfolio_route.asset_quotes_batch({
            "codes": ["000660"],
            "fresh": True,
        })

    assert result == {"000660": {"price": 1786000, "_stale": True}}


@pytest.mark.asyncio
async def test_force_refreshed_rest_quote_returns_even_when_ws_cache_rank_wins():
    cache = quotes.PortfolioQuoteCache()
    cache.remember("005930", {
        "price": 70000,
        "source": "ws",
        "ts": time.time(),
    })
    rest_quote = {
        "date": "2026-05-27",
        "price": 70100,
        "previous_close": 69500,
        "change": 600,
        "change_pct": 0.86,
        "source": "rest",
        "fetched_at": "2026-05-27T19:45:00",
    }

    with patch.object(portfolio_route, "_quote_cache", cache), \
         patch.object(
             portfolio_route.stock_price,
             "fetch_quote_snapshot",
             new=AsyncMock(return_value=rest_quote),
         ):
        result = await portfolio_route._fetch_quote(
            "005930",
            force_refresh=True,
            use_ws_cache=False,
        )

    assert result == rest_quote
    assert cache.get_fresh("005930")["source"] == "ws"


@pytest.mark.asyncio
async def test_force_refreshed_stale_quote_keeps_fresh_cache_value():
    cache = quotes.PortfolioQuoteCache()
    fresh_quote = {
        "date": "2026-05-27",
        "price": 27100,
        "change_pct": -1.63,
        "source": "rest",
        "fetched_at": "2026-05-27T20:01:00",
    }
    cache.remember("000950", fresh_quote)
    stale_history = {
        "date": "2026-05-22",
        "price": 28000,
        "change_pct": -0.36,
        "source": "history",
        "_stale": True,
    }

    with patch.object(portfolio_route, "_quote_cache", cache), \
         patch.object(
             portfolio_route.stock_price,
             "fetch_quote_snapshot",
             new=AsyncMock(return_value=stale_history),
         ):
        result = await portfolio_route._fetch_quote(
            "000950",
            force_refresh=True,
            use_ws_cache=False,
        )

    assert result == fresh_quote
