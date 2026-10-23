import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from domain.timeutil import KST
from services.realtime import fx, indices
from services.realtime.hub import QuoteHub


def quote(at, value="1,390.00"):
    return {"value": value, "change": "2.00", "change_pct": "0.14%", "direction": "up",
            "as_of": at.isoformat(), "source": "naver_json"}


@pytest.mark.asyncio
async def test_fx_refresh_bypasses_minute_cache_preserves_provider_time_and_display_units(monkeypatch):
    at = datetime.now(KST) - timedelta(minutes=10)
    rows = {code: quote(at) for code in fx.CODES}
    rows["JPY_KRW"]["value"], rows["VND_KRW"]["value"] = "848.16", "5.36"
    fetch = AsyncMock(return_value=rows)
    client = object()
    monkeypatch.setattr(fx, "get_http_client", AsyncMock(return_value=client))
    monkeypatch.setattr(fx.naver_indicators, "fetch_indicators", fetch)
    published = []
    stream = fx.FxStream(published.append)
    await stream.poll_once()
    fetch.assert_awaited_once_with(client, list(fx.CODES), fx_max_age=1)
    assert stream.state == "polling" and fx.INTERVAL == 5
    assert set(stream.quotes(fx.CODES)) == set(fx.CODES)
    assert published[0]["JPY_KRW"]["value"] == "848.16"
    assert published[0]["VND_KRW"]["value"] == "5.36"
    assert all(row["as_of"] == at.isoformat() and row["refresh_seconds"] == 5 for row in published[0].values())


@pytest.mark.asyncio
async def test_partial_failures_and_backwards_fx_mark_only_affected_currency_stale_then_recover(monkeypatch):
    now = datetime.now(KST) - timedelta(seconds=1)
    rows = {code: quote(now) for code in fx.CODES}
    fetch = AsyncMock(return_value=rows)
    monkeypatch.setattr(fx, "get_http_client", AsyncMock())
    monkeypatch.setattr(fx.naver_indicators, "fetch_indicators", fetch)
    published = []
    stream = fx.FxStream(published.append)
    await stream.poll_once()
    fetch.return_value = {**rows, "JPY_KRW": quote(now - timedelta(seconds=1), "800.00")}
    del fetch.return_value["EUR_KRW"]
    await stream.poll_once()
    assert stream.state == "degraded"
    assert stream.cache["JPY_KRW"]["value"] == "1,390.00"
    assert stream.cache["JPY_KRW"]["_stale"] and stream.cache["EUR_KRW"]["_stale"]
    assert set(stream.quotes(fx.CODES)) == set(fx.CODES) - {"JPY_KRW", "EUR_KRW"}
    fetch.return_value = rows
    await stream.poll_once()
    assert stream.state == "polling" and not any(row.get("_stale") for row in stream.cache.values())
    stream.updated -= 16
    assert not stream.quotes(fx.CODES)


@pytest.mark.asyncio
async def test_idle_fx_does_not_poll_and_public_clients_merge_independent_index_fx_batches(monkeypatch):
    stream = fx.FxStream(lambda _: None)
    stop = asyncio.Event()
    monkeypatch.setattr(stream, "poll_once", AsyncMock())
    task = asyncio.create_task(stream.run(stop, lambda: False))
    await asyncio.sleep(.01)
    stop.set()
    await task
    stream.poll_once.assert_not_awaited()
    hub = QuoteHub()
    now = datetime.now(KST)
    hub.indices = indices.IndexStream(None, hub.publish_indices)
    hub.fx = fx.FxStream(hub.publish_indices)
    hub.indices.cache = {code: quote(now) for code in indices.CODES}
    hub.fx.cache = {"USD_KRW": quote(now)}
    hub.indices.updated = hub.fx.updated = time.monotonic()
    clients = (hub.attach(None), hub.attach("owner"))
    for value in ("1,391.00", "1,392.00"):
        hub.publish_indices({"USD_KRW": quote(now, value)})
        hub.publish_indices({"KOSDAQ": quote(now, "850.00")})
    for client in clients:
        message = await client.next()
        assert set(message["data"]) == {"KOSPI", "KOSDAQ", "USD_KRW"}
        assert message["data"]["KOSDAQ"]["value"] == "850.00"
        assert message["data"]["USD_KRW"]["value"] == "1,392.00"
        assert not client.pending
    assert set(hub.index_quotes(["KOSDAQ", "USD_KRW"])) == {"KOSDAQ", "USD_KRW"}
    assert not hub.cache and not hub.assignments
