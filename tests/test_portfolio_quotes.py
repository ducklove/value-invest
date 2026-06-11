import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, patch
import time

import pytest

from repositories import portfolio as portfolio_repo
from routes import portfolio as portfolio_route
from services import stock_quotes
from services.portfolio import quote_service
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
    with patch.object(quote_service.stock_quotes, "get_stock_cached", return_value=None):
        assert quote_service.cached_quote_for_code("005930") == {}


def test_cached_quote_for_code_reads_stock_service_cache_for_korean_stock():
    stock = stock_quotes.Stock(
        code="000660",
        current_price=1818000,
        previous_close=1745000,
        volume=778872,
        created_at=datetime(2026, 5, 21, 15, 31),
        source="rest",
        market="NX",
    )

    with patch.object(quote_service.stock_quotes, "get_stock_cached", return_value=stock):
        assert quote_service.cached_quote_for_code("000660") == {
            "code": "000660",
            "date": "2026-05-21",
            "price": 1818000,
            "previous_close": 1745000,
            "change": 73000,
            "change_pct": 4.18,
            "volume": 778872,
            "trade_value": None,
            "source": "rest",
            "market": "NX",
            "fetched_at": "2026-05-21T15:31:00",
        }


@pytest.mark.asyncio
async def test_asset_quotes_batch_fresh_korean_quotes_force_refresh_without_ws_cache():
    # Bulk source unavailable → falls through to the per-code path.
    with patch.object(
        stock_quotes,
        "get_bulk_quote_snapshots",
        new=AsyncMock(return_value={}),
    ), patch.object(
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
async def test_asset_quotes_batch_uses_bulk_for_korean_codes_without_per_code_calls():
    bulk_quote = {
        "price": 318500,
        "previous_close": 299500,
        "change": 19000,
        "change_pct": 6.34,
        "source": "naver",
        "date": "2026-05-29",
        "fetched_at": "2026-05-29T18:59:15",
    }
    remembered = stock_quotes.Stock(
        code="005930",
        current_price=318500,
        previous_close=299500,
        volume=None,
        created_at=datetime(2026, 5, 29, 18, 59, 15),
        source="naver",
    )
    with patch.object(
        stock_quotes.stock_price,
        "fetch_bulk_quotes_kr",
        new=AsyncMock(return_value={"005930": bulk_quote}),
    ), patch.object(
        stock_quotes,
        "remember_quote",
        return_value=remembered,
    ), patch.object(
        portfolio_route,
        "_fetch_quote",
        new=AsyncMock(),
    ) as fetch_quote:
        result = await portfolio_route.asset_quotes_batch({
            "codes": ["005930"],
            "fresh": True,
        })

    assert result["005930"]["price"] == 318500
    # The bulk fast path must satisfy domestic codes without per-code calls.
    fetch_quote.assert_not_awaited()


@pytest.mark.asyncio
async def test_stream_portfolio_quotes_streams_quote_and_benchmark():
    with patch.object(
        portfolio_route,
        "get_current_user",
        new=AsyncMock(return_value={"google_sub": "u1"}),
    ), patch.object(
        portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[{"stock_code": "005930"}]),
    ), patch.object(
        portfolio_route,
        "_prefetch_market_types",
        new=AsyncMock(),
    ), patch.object(
        portfolio_route,
        "_fetch_quote",
        new=AsyncMock(return_value={"price": 1000}),
    ), patch.object(
        portfolio_route,
        "_resolve_default_benchmark",
        new=AsyncMock(return_value="IDX_KOSPI"),
    ), patch.object(
        portfolio_route,
        "_fetch_benchmark_quote",
        new=AsyncMock(return_value={"change_pct": 1.2}),
    ):
        response = await portfolio_route.stream_portfolio_quotes(object())
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.decode() if isinstance(chunk, bytes) else chunk)

    body = "".join(chunks)
    assert '"stock_code": "005930"' in body
    assert '"price": 1000' in body
    assert '"benchmark_code": "IDX_KOSPI"' in body
    assert '"change_pct": 1.2' in body
    assert body.endswith('data: {"done": true}\n\n')


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
    stock = stock_quotes.Stock(
        code="000660",
        current_price=1786000,
        previous_close=None,
        volume=None,
        created_at=datetime(2026, 5, 28, 9, 1),
    )

    with patch.object(portfolio_route.stock_quotes, "get_stock_cached", return_value=stock), \
         patch.object(stock_quotes, "get_bulk_quote_snapshots", new=AsyncMock(return_value={})), \
         patch.object(
             portfolio_route,
             "_fetch_quote",
             new=AsyncMock(side_effect=asyncio.TimeoutError()),
         ):
        result = await portfolio_route.asset_quotes_batch({
            "codes": ["000660"],
            "fresh": True,
        })

    assert result["000660"]["price"] == 1786000
    assert result["000660"]["_stale"] is True


@pytest.mark.asyncio
async def test_asset_quotes_batch_returns_fallback_for_pending_batch_timeout():
    stock = stock_quotes.Stock(
        code="000660",
        current_price=1786000,
        previous_close=None,
        volume=None,
        created_at=datetime(2026, 5, 28, 9, 1),
    )

    async def slow_fetch(*args, **kwargs):
        await asyncio.sleep(1)
        return {"price": 1}

    with patch.object(portfolio_route.stock_quotes, "get_stock_cached", return_value=stock), \
         patch.object(stock_quotes, "get_bulk_quote_snapshots", new=AsyncMock(return_value={})), \
         patch.object(portfolio_route, "_ASSET_QUOTES_BATCH_TIMEOUT", 0.01), \
         patch.object(portfolio_route, "_fetch_quote", new=slow_fetch):
        result = await portfolio_route.asset_quotes_batch({
            "codes": ["000660"],
            "fresh": True,
        })

    assert result["000660"]["price"] == 1786000
    assert result["000660"]["_stale"] is True


@pytest.mark.asyncio
async def test_force_refreshed_rest_quote_returns_even_when_ws_cache_rank_wins():
    rest_quote = {
        "date": "2026-05-27",
        "price": 70100,
        "previous_close": 69500,
        "change": 600,
        "change_pct": 0.86,
        "source": "rest",
        "fetched_at": "2026-05-27T19:45:00",
    }

    stock = stock_quotes.Stock(
        code="005930",
        current_price=70100,
        previous_close=69500,
        volume=None,
        created_at=datetime(2026, 5, 27, 19, 45),
        source="rest",
    )

    with patch.object(portfolio_route.stock_quotes, "get_stock", new=AsyncMock(return_value=stock)) as get_stock:
        result = await portfolio_route._fetch_quote(
            "005930",
            force_refresh=True,
            use_ws_cache=False,
        )

    assert result["price"] == rest_quote["price"]
    assert result["previous_close"] == rest_quote["previous_close"]
    get_stock.assert_awaited_once_with(
        "005930",
        force_refresh=True,
        use_ws_cache=False,
        max_ws_age_seconds=quote_service.WS_QUOTE_MAX_AGE_SECONDS,
    )


@pytest.mark.asyncio
async def test_force_refreshed_stale_quote_keeps_fresh_cache_value():
    fresh_quote = {
        "date": "2026-05-27",
        "price": 27100,
        "change_pct": -1.63,
        "source": "rest",
        "fetched_at": "2026-05-27T20:01:00",
    }
    stock = stock_quotes.Stock(
        code="000950",
        current_price=27100,
        previous_close=27549,
        volume=None,
        created_at=datetime(2026, 5, 27, 20, 1),
        source="rest",
    )

    with patch.object(portfolio_route.stock_quotes, "get_stock", new=AsyncMock(return_value=stock)):
        result = await portfolio_route._fetch_quote(
            "000950",
            force_refresh=True,
            use_ws_cache=False,
        )

    assert result["price"] == fresh_quote["price"]
