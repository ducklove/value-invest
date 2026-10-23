import asyncio
import time
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from domain.timeutil import KST
from services.realtime import indices
from services.realtime.hub import QuoteHub
from services.realtime.toss import instrument


def reference(at, value="2,800.00", change="100.00", direction="up"):
    return {"value": value, "change": change, "direction": direction, "as_of": at.isoformat(), "market_status": "OPEN"}


def row(at, code="KOSPI", price="2812.45"):
    return {"symbol": code, "lastPrice": price, "timestamp": at.isoformat()}


def test_index_points_and_change_use_same_session_base_without_stock_currency_conversion():
    at = datetime.now(KST) - timedelta(seconds=2)
    result = indices.normalize(row(at), reference(at), datetime.now(KST))
    assert result["value"] == "2,812.45" and result["change"] == "112.45" and result["change_pct"] == "4.16%"
    assert result["source"] == "toss_rest" and "currency" not in result
    down = indices.normalize(row(at, price="2690"), reference(at), datetime.now(KST))
    assert down["direction"] == "down" and down["change"] == "10.00"
    old = indices.normalize(row(at), reference(at - timedelta(days=1)), datetime.now(KST))
    assert old["change"] == old["change_pct"] == ""
    assert instrument("KOSPI") is None and instrument("KOSDAQ") is None


def test_index_rejects_bad_values_naive_future_and_older_snapshots():
    now = datetime.now(KST)
    for price in ("NaN", "Infinity", True, "0", "-5"):
        assert indices.normalize(row(now, price=price), {}, now) is None
    assert indices.normalize(row(now.replace(tzinfo=None)), {}, now) is None
    assert indices.normalize(row(now + timedelta(seconds=2)), {}, now) is None
    current = reference(now)
    assert indices.normalize(row(now - timedelta(seconds=5)), current, now) == current


@pytest.mark.asyncio
async def test_indices_share_existing_token_refresh_once_and_batch_both_symbols(monkeypatch):
    now = datetime.now(KST) - timedelta(seconds=1)
    token = SimpleNamespace(get=AsyncMock(side_effect=["old-token", "new-token"]))
    response = lambda status, **kw: httpx.Response(status, request=httpx.Request("GET", "https://example.test/indices"), **kw)
    client = SimpleNamespace(get=AsyncMock(side_effect=[response(401), response(200, json={"result":[row(now),row(now,"KOSDAQ","850")]})]))
    monkeypatch.setattr(indices, "get_http_client", AsyncMock(return_value=client))
    monkeypatch.setattr(indices.naver_indicators, "fetch_indicators", AsyncMock(return_value={"KOSPI":reference(now)}))
    published = []
    stream = indices.IndexStream(token, published.append)
    await stream.poll_once()
    token.get.assert_any_await(expired="old-token")
    assert token.get.await_count == 2 and client.get.await_count == 2
    assert client.get.call_args.kwargs["params"] == {"symbols":"KOSPI,KOSDAQ"}
    assert list(client.get.call_args.kwargs["headers"]) == ["Authorization"]
    assert set(stream.quotes(indices.CODES)) == set(indices.CODES)
    assert published[0]["KOSPI"]["update_mode"] == "polling"


@pytest.mark.asyncio
async def test_index_rate_limit_cools_down_and_falls_back_then_marks_failed_refresh_stale(monkeypatch):
    now = datetime.now(KST) - timedelta(seconds=1)
    token = SimpleNamespace(get=AsyncMock(return_value="token"))
    response = httpx.Response(429, headers={"Retry-After":"60"}, request=httpx.Request("GET", "https://example.test/indices"))
    client = SimpleNamespace(get=AsyncMock(return_value=response))
    monkeypatch.setattr(indices, "get_http_client", AsyncMock(return_value=client))
    refs = {code:reference(now) for code in indices.CODES}
    fallback = AsyncMock(return_value=refs)
    monkeypatch.setattr(indices.naver_indicators, "fetch_indicators", fallback)
    published = []
    stream = indices.IndexStream(token, published.append)
    await stream.poll_once()
    await stream.poll_once()
    assert client.get.await_count == 1 and fallback.await_count == 2
    assert stream.state == "fallback" and stream.reason == "toss_unavailable"
    fallback.return_value = {}
    stream.reference_at = time.monotonic() - 16
    await stream.poll_once()
    assert stream.state == "stale" and not stream.quotes(indices.CODES)
    assert all(tick["_stale"] for tick in published[-1].values())


@pytest.mark.asyncio
async def test_no_browser_means_no_upstream_requests_and_hub_coalesces_public_index_updates(monkeypatch):
    stream = indices.IndexStream(None, lambda _: None)
    stop = asyncio.Event()
    monkeypatch.setattr(stream, "poll_once", AsyncMock())
    task = asyncio.create_task(stream.run(stop, lambda: False))
    await asyncio.sleep(.01)
    stop.set()
    await task
    stream.poll_once.assert_not_awaited()
    hub = QuoteHub()
    owner, guest = hub.attach("owner"), hub.attach(None)
    first = {"KOSPI":reference(datetime.now(KST)), "KOSDAQ":reference(datetime.now(KST))}
    hub.publish_indices(first)
    hub.publish_indices({**first, "KOSPI":{**first["KOSPI"],"value":"2,900.00"}})
    for client in (owner, guest):
        assert len(client.pending) == 1
        assert (await client.next())["data"]["KOSPI"]["value"] == "2,900.00"
    assert not hub.cache and not hub.assignments


@pytest.mark.asyncio
async def test_market_api_uses_index_stream_ahead_of_minute_cache_and_rereads_after_slow_sources(monkeypatch):
    from services.market import indicators as market
    from services.realtime import hub as module
    hub = QuoteHub()
    stream = indices.IndexStream(None, hub.publish_indices)
    hub.indices = stream
    stream.cache, stream.updated = {"KOSPI":reference(datetime.now(KST))}, time.monotonic()
    monkeypatch.setattr(module, "get_hub", lambda: hub)
    async def cached(*args, **kwargs):
        stream.cache["KOSPI"] = {**stream.cache["KOSPI"], "value":"2,900.00"}
        return {"SPX":{"value":"6,000.00"}}
    fetch = AsyncMock(side_effect=cached)
    monkeypatch.setattr(market, "cached_fetch", fetch)
    assert (await market.fetch_indicators(["KOSPI"]))["KOSPI"]["value"] == "2,800.00"
    fetch.assert_not_awaited()
    result = await market.fetch_indicators(["KOSPI","SPX"])
    assert result["KOSPI"]["value"] == "2,900.00" and result["SPX"]["value"] == "6,000.00"
