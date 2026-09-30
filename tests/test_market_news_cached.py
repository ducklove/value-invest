"""services.market.news on cached_fetch (X1): single-flight + stale-while-error."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import httpx
import pytest

from services.market import news


@pytest.fixture(autouse=True)
def _clear_cache():
    news._news_cache.clear()
    yield
    news._news_cache.clear()


async def test_concurrent_cold_calls_hit_upstream_once():
    calls = 0

    async def loader():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return [{"title": f"t{i}"} for i in range(5)]

    with patch.object(news, "_load_market_news", loader):
        results = await asyncio.gather(*(news.fetch_market_news(3) for _ in range(6)))
        assert calls == 1
        assert all(len(r) == 3 for r in results)
        assert await news.fetch_market_news(8) == [{"title": f"t{i}"} for i in range(5)]
        assert calls == 1


async def test_upstream_error_serves_last_good_list_of_any_age():
    news._news_cache["mainnews"] = (time.monotonic() - 86400, [{"title": "old"}])

    async def failing():
        raise httpx.ConnectError("down")

    with patch.object(news, "_load_market_news", failing):
        assert await news.fetch_market_news() == [{"title": "old"}]


async def test_upstream_error_without_history_returns_empty():
    async def failing():
        raise httpx.ReadTimeout("slow")

    with patch.object(news, "_load_market_news", failing):
        assert await news.fetch_market_news() == []


async def test_parse_failure_is_swallowed():
    async def broken():
        raise AttributeError("layout changed")

    with patch.object(news, "_load_market_news", broken):
        assert await news.fetch_market_news() == []


async def test_empty_list_is_not_cached():
    async def empty():
        return []

    with patch.object(news, "_load_market_news", empty):
        assert await news.fetch_market_news() == []
    assert news._news_cache.get("mainnews") is None
