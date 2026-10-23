import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from domain.timeutil import KST
from services import stock_quotes
from services.portfolio import foreign
from services.realtime.hub import QuoteHub
from services.realtime.references import DailyReferences, context


def tick(at, price=324.19, rate=1322.4):
    return {"code": "GOOGL", "source": "toss_ws", "price": price * rate, "currency": "KRW",
            "original_price": price, "original_currency": "USD", "fx_rate": rate,
            "as_of": at.isoformat(), "date": at.date().isoformat(), "ts": at.timestamp()}


@pytest.mark.parametrize(("at", "session"), [
    ("2026-10-22T19:59:59-04:00", "2026-10-22"),
    ("2026-10-22T20:00:00-04:00", "2026-10-23"),
    ("2026-12-03T20:00:00-05:00", "2026-12-04"),
    ("2026-10-23T00:10:00+09:00", "2026-10-22"),
])
def test_us_reference_session_follows_day_market_and_dst_instead_of_kst_midnight(at, session):
    assert context(tick(datetime.fromisoformat(at))) == ("GOOGL", "USD", session)


@pytest.mark.asyncio
async def test_native_reference_uses_completed_session_not_today_or_chart_range_base(monkeypatch):
    series = {"currency": "USD", "meta": {"symbol": "GOOGL", "chartPreviousClose": 999}, "rows": [
        {"session_date": "2026-10-21", "close": 334.76},
        {"session_date": "2026-10-22", "close": 324.22},
        {"session_date": "2026-10-23", "close": 325.0},
    ]}
    fetch = AsyncMock(return_value=series)
    monkeypatch.setattr(foreign, "fetch_yahoo_chart", fetch)
    assert (await stock_quotes.get_daily_reference("GOOGL", "USD", "2026-10-22"))["price"] == 334.76
    result = await stock_quotes.get_daily_reference("GOOGL", "USD", "2026-10-23")
    assert result["price"] == 324.22 and result["date"] == "2026-10-22"
    assert await stock_quotes.get_daily_reference("GOOGL", "USD", "2026-11-02") == {}
    series["meta"]["symbol"] = "GOOG"
    assert await stock_quotes.get_daily_reference("GOOGL", "USD", "2026-10-23") == {}
    series["meta"]["symbol"], series["currency"] = "GOOGL", "EUR"
    assert await stock_quotes.get_daily_reference("GOOGL", "USD", "2026-10-23") == {}


@pytest.mark.asyncio
async def test_reference_completion_hydrates_latest_tick_for_all_clients_and_keeps_native_pct_when_fx_changes(monkeypatch):
    at = datetime.now(KST) - timedelta(seconds=1)
    gate = asyncio.Event()
    async def load(code, currency, session):
        await gate.wait()
        return {"price": 324.22, "currency": currency, "session": session, "date": "2026-10-22", "source": "yahoo_daily"}
    fetch = AsyncMock(side_effect=load)
    monkeypatch.setattr(stock_quotes, "get_daily_reference", fetch)
    monkeypatch.setattr(stock_quotes, "get_stock_cached", Mock(return_value=None))
    monkeypatch.setattr(stock_quotes, "remember_quote", Mock())
    hub = QuoteHub()
    hub.references = references = DailyReferences(hub._reference_ready)
    clients = [hub.attach(None), hub.attach("owner")]
    for client in clients:
        hub.subscribe(client, {"portfolio": ["GOOGL"]})
    try:
        hub.publish(tick(at, 320))
        await asyncio.sleep(0)
        hub.publish(tick(at, 324.19))
        assert fetch.await_count == 1 and len(references.tasks) == 1
        assert hub.quote(None, "GOOGL")["change_pct"] is None
        # A personal quote must stay private when public reference hydration completes.
        private = {**tick(at), "source": "namuh_ws", "price": 500000, "previous_close": 499000}
        hub.publish(private, "owner")
        gate.set()
        await asyncio.gather(*list(references.tasks.values()))
        result = hub.quote(None, "GOOGL")
        assert result["original_price"] == 324.19 and result["as_of"] == at.isoformat()
        assert result["change_pct"] == pytest.approx((324.19 / 324.22 - 1) * 100)
        assert result["previous_close"] == pytest.approx(324.22 * 1322.4)
        assert hub.cache[("owner", "GOOGL")]["price"] == 500000
        assert (await clients[0].next())["change_pct"] == result["change_pct"]
        assert (await clients[1].next())["source"] == "namuh_ws"
        hub.publish(tick(at, 324.19, rate=1400))
        changed = hub.quote(None, "GOOGL")
        assert changed["change_pct"] == result["change_pct"]
        assert changed["change"] == pytest.approx((324.19-324.22)*1400)
        assert fetch.await_count == 1
    finally:
        await references.close()


@pytest.mark.asyncio
async def test_missing_reference_retries_without_losing_price_and_new_session_never_reuses_old_base(monkeypatch):
    fetch = AsyncMock(return_value={})
    monkeypatch.setattr(stock_quotes, "get_daily_reference", fetch)
    references = DailyReferences(lambda _: None)
    at = datetime.fromisoformat("2026-10-22T19:59:00-04:00")
    first = tick(at)
    try:
        pending = references.enrich(first)
        assert pending["price"] == first["price"] and pending["change_pct"] is None
        await asyncio.gather(*list(references.tasks.values()))
        references.enrich(first)
        assert fetch.await_count == 1 and not references.tasks
        references.retry.clear()
        fetch.side_effect = lambda code, currency, session: {"price": 334.76, "currency": currency,
            "session": session, "date": "2026-10-21", "source": "yahoo_daily"}
        references.enrich(first)
        await asyncio.gather(*list(references.tasks.values()))
        assert references.enrich(first)["change_pct"] is not None
        rolled = references.enrich(tick(at + timedelta(minutes=2)))
        assert rolled["reference_session"] == "2026-10-23" and rolled["previous_close"] is None
        assert rolled["change_pct"] is None
        assert rolled["reference_date"] is None and rolled["original_previous_close"] is None
    finally:
        await references.close()


@pytest.mark.asyncio
async def test_domestic_reference_bypasses_price_only_ws_cache_without_overwriting_its_price(monkeypatch):
    monkeypatch.setattr(stock_quotes, "get_stock_cached", Mock(return_value=stock_quotes.stock_from_quote(
        "222800", {"price": 112600, "source": "toss_ws", "date": "2026-10-23"})))
    upstream = AsyncMock(return_value={"222800": {"price": 110000, "previous_close": 111500, "date": "2026-10-23"}})
    monkeypatch.setattr(stock_quotes.stock_price, "fetch_bulk_quotes_kr", upstream)
    result = await stock_quotes.get_daily_reference("222800", "KRW", "2026-10-23")
    assert result["price"] == 111500
    assert stock_quotes.get_stock_cached("222800").price == 112600
    upstream.return_value["222800"]["date"] = "2026-10-22"
    assert await stock_quotes.get_daily_reference("222800", "KRW", "2026-10-23") == {}
