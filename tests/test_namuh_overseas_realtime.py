import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from services.brokers import overseas_realtime as overseas
from services.brokers import realtime
from services.portfolio import fx, quotes


def master_row(gic="USAAAPL", symbol="AAPL", market="NQQ", currency="USD"):
    row = bytearray(b" " * 163 + b"\n")
    for start, length, value in ((0, 15, gic), (15, 40, "테스트 종목"), (98, 12, symbol), (110, 3, market), (138, 3, currency)):
        row[start:start + length] = value.encode("cp949").ljust(length)
    return bytes(row)


@pytest.fixture(autouse=True)
def reset(monkeypatch):
    monkeypatch.setattr(overseas, "_instruments", overseas.parse_master(master_row()))
    overseas._master.clear()
    overseas._master.set("ready", True)
    realtime._quotes.clear()
    realtime._status.clear()
    yield
    overseas._master.clear()
    realtime._quotes.clear()
    realtime._status.clear()


def message(at, channel="RC", **fields):
    local = at.astimezone(ZoneInfo("America/New_York"))
    return {"header": {"tr_cd": channel, "tr_key": "USAAAPL"}, "body": {
        "gicz15": "USAAAPL", "exch_idz3": "NQQ", "trade_datez8": local.strftime("%Y%m%d"),
        "trade_timez6": local.strftime("%H%M%S"), "trdprc_1z17": "200.25",
        "netchng_clsz1": "5", "netchng_1z17": "-1.25", **fields,
    }}


def test_official_master_maps_exchange_class_shares_and_original_currency(monkeypatch):
    data = master_row() + master_row("USABRK.B", "BRK.B", "NYY") + master_row("HKG00700", "00700", "HKG", "HKD")
    data += master_row("HKG83199", "83199", "HKG", "CNY") + master_row("AUSA200", "A200", "ASX", "AUD")
    parsed = overseas.parse_master(data)
    monkeypatch.setattr(overseas, "_instruments", parsed)
    assert realtime.subscription("AAPL") == ("RC", "USAAAPL")
    assert realtime.subscription("BRK-B") == ("RC", "USABRK.B")
    assert realtime.subscription("0700.HK") == ("RC", "HKG00700")
    assert overseas.instrument("83199.HK")["currency"] == "CNY"
    assert overseas.instrument("A200") == overseas.instrument("A200.AX")
    assert realtime.select_codes([{"stock_code": c} for c in ("A200", "A200.AX", "AAPL")], 1, foreign=True) == ["A200", "A200.AX"]
    assert realtime.select_codes([{"stock_code": c} for c in ("KRX_GOLD", "AAPL", "005930")], 2) == ["KRX_GOLD", "005930"]
    for invalid in (data[:-1], b"", b" " * 164):
        with pytest.raises(ValueError):
            overseas.parse_master(invalid)


@pytest.mark.parametrize("at", [datetime(2026, 7, 10, 1, tzinfo=realtime._KST), datetime(2026, 1, 10, 1, tzinfo=realtime._KST)])
def test_foreign_trade_date_timezone_and_dst(at):
    tick = overseas.normalize(message(at), "AAPL", overseas.instrument("AAPL"), at)
    assert tick["price"] == 200.25 and tick["previous_close"] == 201.5
    assert tick["date"] == at.date().isoformat()
    assert datetime.fromisoformat(tick["as_of"]) == at
    assert tick["market_date"] != tick["date"].replace("-", "")


def test_extended_hour_and_invalid_or_delayed_ticks():
    now = datetime(2026, 7, 10, 13, 11, 12, tzinfo=realtime._KST)
    payload = message(now, trade_datez8="20260709", trade_timez6="241100")
    tick = overseas.normalize(payload, "AAPL", overseas.instrument("AAPL"), now)
    assert datetime.fromisoformat(tick["as_of"]) == now - timedelta(seconds=12)
    for fields in ({"trade_timez6": "486000"}, {"trade_datez8": "bad"}, {"trdprc_1z17": "NaN"},
                   {"trdprc_1z17": True}, {"netchng_1z17": "Infinity"}, {"gicz15": None}, {"exch_idz3": "NYY"}):
        assert overseas.normalize(message(now, **fields), "AAPL", overseas.instrument("AAPL"), now) is None
    for seconds in (-91, 1):
        assert overseas.normalize(message(now + timedelta(seconds=seconds)), "AAPL", overseas.instrument("AAPL"), now) is None
    assert overseas.normalize(message(now, channel="rc"), "AAPL", overseas.instrument("AAPL"), now) is None


@pytest.mark.asyncio
async def test_fx_conversion_preserves_original_units_and_rejects_missing_rate():
    now = datetime.now(realtime._KST)
    tick = overseas.normalize(message(now), "AAPL", overseas.instrument("AAPL"), now)
    with patch.object(fx, "fx_rate_for_currency", AsyncMock(return_value=1400)):
        won = await overseas.to_won(tick)
    assert (won["price"], won["change"], won["original_price"], won["original_currency"]) == (280350, -1750, 200.25, "USD")
    assert won["currency"] == "KRW" and won["as_of"] == tick["as_of"]
    with patch.object(fx, "fx_rate_for_currency", AsyncMock(return_value=1800)) as rate:
        pence = await overseas.to_won({**tick, "currency": "GBp"})
    rate.assert_awaited_once_with("GBP")
    assert pence["price"] == tick["price"] * 18
    with patch.object(fx, "fx_rate_for_currency", AsyncMock(side_effect=fx.FXUnavailableError("USD"))):
        assert await overseas.to_won(tick) is None


def test_fresh_nh_is_primary_and_expired_nh_yields_to_kis():
    now = datetime.now(realtime._KST)
    nh = {"price": 100, "source": "namuh_ws", "as_of": (now - timedelta(seconds=2)).isoformat(), "ts": now.timestamp()}
    kis = {"price": 101, "source": "kis_ws", "as_of": now.isoformat(), "ts": now.timestamp()}
    assert not quotes.should_accept_quote_snapshot(nh, kis)
    assert quotes.should_accept_quote_snapshot(kis, nh)
    old = {**nh, "as_of": (now - timedelta(seconds=91)).isoformat()}
    assert quotes.should_accept_quote_snapshot(old, kis)
    realtime._quotes.set(("owner", "AAPL"), nh)
    assert realtime.quote("owner", "AAPL") == nh
    assert realtime.quote("other", "AAPL") is None


@pytest.mark.asyncio
async def test_overseas_socket_uses_7080_and_permission_rejection_is_isolated():
    sent = []
    now = datetime.now(realtime._KST)

    class Socket:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def send(self, text): sent.append(json.loads(text)["body"])
        async def __aiter__(self):
            yield json.dumps({"header": {"rsp_cd": "WSS10006"}, "body": {"tr_key": "OTHER"}})
            yield json.dumps({"header": {"rsp_cd": "00000"}, "body": {"tr_key": "USAAAPL"}})
            yield json.dumps(message(now))
            raise asyncio.CancelledError

    realtime._status["owner"] = {"state": "subscribed", "subscribed": 1, "requested": 1}
    with patch.object(realtime.namuh, "token", AsyncMock(return_value="test-token")), \
         patch.object(realtime.websockets, "connect", return_value=Socket()) as connect, \
         patch.object(fx, "fx_rate_for_currency", AsyncMock(return_value=1400)):
        with pytest.raises(asyncio.CancelledError):
            await realtime.stream("owner", "cid", ["AAPL"], "live", foreign=True)
    assert connect.call_args.args[0] == "wss://api.nhplug.com:7080/websocket"
    assert sent == [{"tr_cd": "RC", "tr_key": "USAAAPL"}]
    assert realtime.quote("owner", "AAPL")["price"] == 280350
    state = realtime.status("owner")
    assert state["foreign"]["state"] == "live" and state["foreign"]["rejected"] == 1
    assert state["domestic"]["state"] == "subscribed"


@pytest.mark.asyncio
async def test_background_uses_two_connections_with_separate_30_registration_limits(monkeypatch):
    records = b"".join(master_row("USA" + str(i), "A" + str(i)) for i in range(35))
    monkeypatch.setattr(overseas, "_instruments", overseas.parse_master(records))
    stop = asyncio.Event()
    calls = []

    async def stream(user, cid, codes, env, **kwargs):
        calls.append((codes, kwargs))
        if len(calls) == 2: stop.set()

    link = {"google_sub": "owner", "credential_id": "cid", "account_id": "aid", "environment": "live"}
    rows = [{"stock_code": code} for code in ["KRX_GOLD", *[f"{i:06d}" for i in range(35)], *[f"A{i}" for i in range(35)]]]
    with patch.object(realtime.brokers, "list_links", AsyncMock(return_value=[link])), \
         patch.object(realtime.account_holdings, "list_positions", AsyncMock(return_value=rows)), \
         patch.object(realtime, "sync_account", AsyncMock()), patch.object(realtime, "stream", side_effect=stream):
        await asyncio.wait_for(realtime.run(stop), 2)
    assert len(calls) == 2 and all(len(codes) == 30 for codes, _ in calls)
    assert calls[0][0][0] == "KRX_GOLD" and calls[1][1] == {"foreign": True}
