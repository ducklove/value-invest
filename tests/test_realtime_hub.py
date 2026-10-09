import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from domain.timeutil import KST
from services.realtime.allocation import allocate, fair_codes, sanitize
from services.realtime.hub import QuoteHub
from services.realtime.kis import KisSource
from services.realtime.toss import Token, TossError, TossSource, instrument, normalize


class Source:
    def __init__(self, name, capacity, predicate=lambda code: True, *, user=None):
        self.id, self.capacity, self.predicate = name, capacity, predicate
        self.user = user
        self.approved, self.rejected, self.codes = set(), set(), []

    def supports(self, code): return self.predicate(code)
    def available(self, code): return code not in self.rejected
    async def set_codes(self, codes): self.codes = codes
    def snapshot(self):
        return {"id": self.id, "provider": self.id.split(':')[0], "state": "connected",
                "requested": len(self.codes), "subscribed": len(self.approved), "capacity": self.capacity}


def trade(code="005930", price="72000", **fields):
    market, symbol = instrument(code)
    return {"type": "message", "topic": f"trade:{market}:{symbol}", "data": {
        "price": price, "volume": "12", "timestamp": datetime.now(KST).isoformat(),
        "currency": "KRW" if market == "kr" else "USD", **fields}}


def test_demand_priority_fairness_dedup_and_invalid_inputs():
    assert sanitize({"portfolio": ["005930.KS", "005930", None, 1], "analysis": "AAPL"}) == {"portfolio": ["005930"]}
    assert fair_codes({"a": {"portfolio": ["1", "2", "3"], "analysis": ["6"]},
                       "b": {"portfolio": ["4", "1", "5"]}}) == ["1", "4", "2", "3", "5", "6"]
    assert len(sanitize({"portfolio": [str(n) for n in range(2000)]})["portfolio"]) == 1000


def test_allocation_capacity_stability_and_rejection_failover():
    kis, toss = Source("kis:0", 2, str.isdigit), Source("toss:0", 2)
    original = allocate(["1", "2", "AAPL", "MSFT", "3"], [kis, toss], {})
    assert original == {"1": "kis:0", "2": "kis:0", "AAPL": "toss:0", "MSFT": "toss:0"}
    assert allocate(["2", "AAPL", "MSFT", "1"], [kis, toss], original) == original
    kis.rejected.add("1")
    assert allocate(["1", "2", "AAPL"], [kis, toss], original)["1"] == "toss:0"


@pytest.mark.asyncio
async def test_clients_share_one_assignment_approval_controls_live_and_disconnect_keeps_other_client(monkeypatch):
    hub = QuoteHub()
    hub.running = True
    monkeypatch.setattr(hub, "_nh_coverage", lambda user: set())
    source = Source("kis:0", 40, user="a")
    hub.sources = [source]
    first, second = hub.attach("a"), hub.attach("a")
    for client in [first, second]: hub.subscribe(client, {"portfolio": ["005930"]})
    await hub.reconcile()
    assert source.codes == ["005930"]
    assert first.plan["ws"] == []
    assert first.plan["rest"] == ["005930"]
    assert first.plan["pending"] == ["005930"]
    source.approved.add("005930")
    await hub.reconcile()
    assert first.plan["ws"] == second.plan["ws"] == ["005930"]
    hub.detach(first)
    await hub.reconcile()
    assert source.codes == ["005930"]
    source.approved.clear()
    await hub.reconcile()
    assert second.plan["ws"] == []
    assert hub.status(second)["receiving"] == 0


@pytest.mark.asyncio
async def test_nh_approved_symbols_do_not_consume_shared_capacity_and_loss_uses_other_provider(monkeypatch):
    hub = QuoteHub()
    nh = {"005930"}
    monkeypatch.setattr(hub, "_nh_coverage", lambda user: nh)
    hub.sources = [Source("toss:0", 100)]
    client = hub.attach("owner")
    hub.subscribe(client, {"portfolio": ["005930", "AAPL"]})
    await hub.reconcile()
    assert hub.sources[0].codes == ["AAPL"]
    assert client.plan["ws"] == ["005930"]
    nh.clear()
    await hub.reconcile()
    assert set(hub.sources[0].codes) == {"005930", "AAPL"}


@pytest.mark.asyncio
async def test_kis_allocation_approval_and_diagnostics_are_scoped_but_toss_is_shared(monkeypatch):
    hub = QuoteHub()
    monkeypatch.setattr(hub, "_nh_coverage", lambda user: set())
    alice_kis = Source("kis:0", 2, str.isdigit, user="alice")
    bob_kis = Source("kis:1", 1, str.isdigit, user="bob")
    toss = Source("toss:0", 100)
    hub.sources = [alice_kis, bob_kis, toss]
    alice, bob, unlinked = hub.attach("alice"), hub.attach("bob"), hub.attach("unlinked")
    hub.subscribe(alice, {"portfolio": ["005930", "000660", "AAPL"]})
    hub.subscribe(bob, {"portfolio": ["005930", "005380", "AAPL"]})
    hub.subscribe(unlinked, {"portfolio": ["005930", "AAPL"]})
    await hub.reconcile()
    assert alice_kis.codes == ["005930", "000660"]
    assert bob_kis.codes == ["005930"]
    assert set(toss.codes) == {"005930", "005380", "AAPL"}
    # No approval from Alice's connection may count for Bob or an unlinked user.
    alice_kis.approved = {"005930", "000660"}
    toss.approved = {"AAPL", "005380"}
    await hub.reconcile()
    assert alice.plan["ws"] == ["005930", "000660", "AAPL"]
    assert bob.plan["ws"] == ["005380", "AAPL"] and bob.plan["rest"] == ["005930"]
    assert unlinked.plan["ws"] == ["AAPL"] and unlinked.plan["rest"] == ["005930"]
    assert {row["id"] for row in hub.status(alice)["sources"]} == {"kis:0", "toss:0"}
    assert {row["id"] for row in hub.status(bob)["sources"]} == {"kis:1", "toss:0"}
    assert {row["id"] for row in hub.status(unlinked)["sources"]} == {"toss:0"}
    assert [row["scope"] for row in hub.status(alice)["sources"]] == ["account", "shared"]
    # A rejected personal subscription falls back to Toss, never to Bob's spare key.
    alice_kis.rejected.add("000660")
    await hub.reconcile()
    assert "000660" in toss.codes and "000660" not in bob_kis.codes


@pytest.mark.asyncio
async def test_kis_ticks_stay_private_and_toss_ticks_reach_both_accounts(monkeypatch):
    hub = QuoteHub()
    alice, bob = hub.attach("alice"), hub.attach("bob")
    for client in (alice, bob):
        hub.subscribe(client, {"portfolio": ["005930"]})
    hub.publish({**normalize(trade(), "005930"), "source": "kis_ws"})
    assert not alice.pending and not bob.pending and not hub.cache
    from services import stock_quotes
    remember = Mock()
    monkeypatch.setattr(stock_quotes, "remember_quote", remember)
    source = KisSource(SimpleNamespace(slot_id=0), hub.publish, user="alice", credential_id="alice-key")
    source.conn._confirmed_subs = {("005930", "H0UNCNT0")}
    relay = asyncio.create_task(source._relay())
    try:
        await source.conn.listener.put(normalize(trade(), "005930"))
        await eventually(lambda: bool(alice.pending))
        assert (await alice.next())["source"] == "kis_ws"
        assert not bob.pending and hub.quote("bob", "005930") is None
        assert hub.quote(None, "005930") is None
        remember.assert_not_called()
    finally:
        relay.cancel()
        await asyncio.gather(relay, return_exceptions=True)
    # Use the real synchronous cache boundary for the public quote.
    monkeypatch.setattr(stock_quotes, "remember_quote", Mock())
    hub.publish(normalize(trade(price="73000"), "005930"))
    assert (await bob.next())["source"] == "toss_ws"
    assert (await alice.next())["source"] == "toss_ws"


@pytest.mark.asyncio
async def test_linked_kis_socket_uses_own_credentials_and_unlink_cleans_up(monkeypatch):
    from repositories.broker_secrets import BrokerError
    from services.realtime import hub as module
    hub = QuoteHub()
    hub.running = True
    credential = {"google_sub": "alice", "credential_id": "cid", "provider": "kis", "environment": "live",
                  "app_key": "PRIVATE-KEY", "app_secret": "PRIVATE-SECRET", "hts_id": "hts"}
    with pytest.raises(BrokerError):
        await hub.watch_kis_account("bob", "cid", credential, lambda _: None)
    assert not hub.sources
    registered = asyncio.Event()
    async def watch(source, user, cid, hts, changed):
        registered.set()
        await asyncio.Future()
    monkeypatch.setattr(module.KisSource, "watch_account", watch)
    monkeypatch.setattr(module.KisSource, "close", AsyncMock())
    task = asyncio.create_task(hub.watch_kis_account("alice", "cid", credential, lambda _: None))
    await registered.wait()
    source = hub.sources[0]
    assert source.user == "alice" and source.credential_id == "cid"
    assert source.slot.app_key == "PRIVATE-KEY" and source.conn.shared_cache is False
    assert "PRIVATE" not in str(source.snapshot()) and "alice" not in str(source.snapshot())
    hub.publish({**normalize(trade(), "005930"), "source": "kis_ws"}, user="alice")
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    source.close.assert_awaited_once()
    assert not hub.sources and hub.quote("alice", "005930") is None


@pytest.mark.asyncio
async def test_guests_share_toss_capacity_receive_public_ticks_and_never_borrow_personal_kis(monkeypatch):
    hub = QuoteHub()
    monkeypatch.setattr(hub, "_nh_coverage", lambda user: set())
    kis, toss = Source("kis:0", 40, user="owner"), Source("toss:0", 1)
    hub.sources = [kis, toss]
    guest, second, owner = hub.attach(None), hub.attach(None), hub.attach("owner")
    for client in (guest, second):
        hub.subscribe(client, {"analysis": ["005930", "000660", "KRX_GOLD"]})
    hub.subscribe(owner, {"analysis": ["AAPL"]})
    await hub.reconcile()
    assert toss.codes == ["005930"]  # Two guests request one upstream registration.
    assert kis.codes == ["AAPL"]  # Fake supports all symbols, but is exclusive to owner.
    toss.approved = {"005930"}
    kis.approved = {"000660"}
    await hub.reconcile()
    assert guest.plan["ws"] == second.plan["ws"] == ["005930"]
    assert guest.plan["rest"] == ["000660", "KRX_GOLD"]
    for client in (guest, second, owner):
        client.pending.clear()
    tick = normalize(trade(), "005930")
    hub.publish({**tick, "source": "kis_ws"}, user="owner")
    assert not guest.pending and not second.pending
    hub.publish(tick)
    for client in (guest, second):
        assert (await client.next())["source"] == "toss_ws"
    hub.detach(guest)
    await hub.reconcile()
    assert toss.codes == ["005930"]


def test_admin_diagnostics_include_nh_without_exposing_user_identity(monkeypatch):
    from services.brokers import realtime
    hub = QuoteHub()
    hub.holdings = {'PRIVATE-OWNER': {'portfolio':['005930']}}
    monkeypatch.setattr(hub, '_nh_coverage', lambda _: {'005930'})
    monkeypatch.setattr(realtime, 'status', lambda _: {'domestic': {'state':'subscribed','requested':1,'subscribed':1}})
    result = hub.status()
    assert result['subscribed'] == 1
    assert result['sources'][0]['provider'] == 'namuh'
    assert 'PRIVATE-OWNER' not in str(result)


@pytest.mark.asyncio
async def test_prices_are_filtered_by_requested_user_and_slow_clients_coalesce(monkeypatch):
    hub = QuoteHub()
    alice, bob = hub.attach("alice"), hub.attach("bob")
    for client in [alice, bob]: hub.subscribe(client, {"portfolio": ["005930"]})
    tick = normalize(trade(), "005930")
    hub.publish({**tick, "source": "namuh_ws"}, "alice")
    assert (await alice.next())["code"] == "005930"
    assert not bob.pending
    for price in range(100, 500):
        hub.publish({**tick, "price": price, "code": "000660"})
    assert not bob.pending
    for price in range(100, 500): hub.publish({**tick, "price": price})
    assert len(bob.pending) == 1
    assert (await bob.next())["price"] == 499
    hub.publish({**tick, "as_of": (datetime.now(KST) - timedelta(seconds=91)).isoformat(), "price": 1})
    assert hub.quote("bob", "005930")["price"] == 499


def test_toss_wire_time_currency_and_trade_volume_are_validated():
    quote = normalize(trade(), "005930")
    assert quote["source"] == "toss_ws" and quote["market"] == "UN"
    assert "volume" not in quote and "previous_close" not in quote
    for fields in [{"timestamp": "bad"}, {"timestamp": "2026-10-08T09:00:00"}, {"currency": "USD"}, {"price": True}, {"price": "NaN"}, {"price": "0"}]:
        assert normalize(trade(**fields), "005930") is None
    assert instrument("A200") is None
    assert instrument("EUN2.DE") is None
    assert instrument("AAPL") == ("us", "AAPL")
    assert normalize(trade("AAPL"), "MSFT") is None


@pytest.mark.asyncio
async def test_toss_token_is_single_flight_and_error_body_is_not_exposed(monkeypatch):
    response = httpx.Response(200, json={"access_token": "PRIVATE", "expires_in": 86400})
    client = SimpleNamespace(post=AsyncMock(return_value=response))
    from services.realtime import toss
    monkeypatch.setattr(toss, "get_http_client", AsyncMock(return_value=client))
    token = Token()
    assert await asyncio.gather(token.get(), token.get(), token.get()) == ["PRIVATE"] * 3
    assert client.post.await_count == 1
    assert client.post.call_args.kwargs['data']['grant_type'] == 'client_credentials'
    client.post.return_value = httpx.Response(403, json={"error_description": "PRIVATE-SECRET"})
    with pytest.raises(TossError, match="^ip_not_allowed$"):
        await token.get(expired="PRIVATE")
    with pytest.raises(TossError, match="^ip_not_allowed$"):
        await token.get(expired="PRIVATE")
    assert client.post.await_count == 2  # Failure cooldown is also shared by both sockets.


async def eventually(predicate):
    for _ in range(100):
        if predicate(): return
        await asyncio.sleep(.005)
    assert predicate()


@pytest.mark.asyncio
async def test_toss_ack_partial_rejection_normal_close_and_secret_safe_error(monkeypatch):
    import json

    from services.realtime import toss
    class Socket:
        def __init__(self): self.incoming = asyncio.Queue(); self.sent = []
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def send(self, data): self.sent.append(json.loads(data))
        async def recv(self):
            value = await self.incoming.get()
            if isinstance(value, Exception): raise value
            return json.dumps(value)
    socket, published = Socket(), []
    monkeypatch.setattr(toss.websockets, "connect", lambda *args, **kwargs: socket)
    source = TossSource(0, SimpleNamespace(get=AsyncMock(return_value="PRIVATE")), published.append)
    try:
        await source.set_codes(["005930", "AAPL"])
        await eventually(lambda: bool(socket.sent))
        assert not source.approved
        request_id = socket.sent[-1][0]['id']
        await socket.incoming.put({"type": "subscriptions", "id": request_id, "subscribed": ["trade:kr:005930"],
                                   "rejected": [{"target": "trade:us:AAPL", "code": "stock-not-found", "message": "PRIVATE"}]})
        await eventually(lambda: bool(source.approved))
        assert source.approved == {"005930"}
        assert source.rejected == {"AAPL": "stock-not-found"}
        await socket.incoming.put(trade())
        await eventually(lambda: bool(published))
        monkeypatch.setattr(source, '_to_won', AsyncMock(side_effect=lambda tick: tick))
        await source.set_codes(['BRK.B', 'BRK-B'])
        declaration = socket.sent[-1]
        assert declaration[1]['codes'] == ['BRK.B']
        await socket.incoming.put({'type':'subscriptions', 'id':declaration[0]['id'],
                                   'subscribed':['trade:us:BRK.B'], 'rejected':[]})
        await eventually(lambda: len(source.approved) == 2)
        await socket.incoming.put(trade('BRK.B', price='200', currency='USD'))
        await eventually(lambda: len(published) == 3)
        assert {tick['code'] for tick in published[1:]} == {'BRK.B', 'BRK-B'}
        await socket.incoming.put(OSError("PRIVATE-TOKEN"))
        await eventually(lambda: source.state == 'reconnecting')
        assert not source.approved
        assert source.snapshot()["reason"] == "connection_error"
    finally:
        await source.close()


@pytest.mark.asyncio
async def test_kis_notice_reservation_and_ack_are_separate_from_transport(monkeypatch):
    slot = SimpleNamespace(slot_id=0, _approval_key="PRIVATE")
    source = KisSource(slot, lambda quote: None, user="owner", credential_id="cid")
    source.conn.extra_subscriptions = {("hts", "H0STCNI0"), ("hts", "H0GSCNI0")}
    source.conn._ws = AsyncMock()
    source.conn.state = "connected"
    source.conn._current_subs = {("005930", "H0UNCNT0")}
    assert source.capacity == 38
    assert source.approved == set()
    await source.conn._handle_subscription_result({"header": {"tr_id": "H0UNCNT0", "tr_key": "005930"}, "body": {"rt_cd": "0"}})
    assert source.approved == {"005930"}
    assert source.snapshot()["state"] == "subscribed"
    await source.conn._handle_subscription_result({"header": {"tr_id": "H0UNCNT0", "tr_key": "005930"},
                                                  "body": {"rt_cd": "0", "msg_cd": "OPSP0001"}})
    assert source.approved == set()  # An unsubscribe ACK must not report the new request as live.
    await source.conn._handle_subscription_result({"header": {"tr_id": "H0UNCNT0", "tr_key": "005930"}, "body": {"rt_cd": "1"}})
    assert source.approved == set()


@pytest.mark.asyncio
async def test_toss_us_prices_convert_to_won_without_inventing_previous_close(monkeypatch):
    from services.brokers import overseas_realtime
    monkeypatch.setattr(overseas_realtime.fx, 'fx_rate_for_currency', AsyncMock(return_value=1400))
    quote = normalize(trade('AAPL', price='200'), 'AAPL')
    result = await TossSource._to_won(quote)
    assert result['price'] == 280000
    assert result['original_price'] == 200 and result['original_currency'] == 'USD'
    assert result['currency'] == 'KRW' and 'previous_close' not in result


@pytest.mark.asyncio
async def test_process_owner_lock_prevents_duplicate_connections_and_is_released(tmp_path, monkeypatch):
    from repositories import db
    from services.realtime import hub as module
    monkeypatch.setattr(db, 'DB_PATH', tmp_path / 'cache.db')
    monkeypatch.setattr(module.kis_key_manager, 'all_slots', lambda: pytest.fail('Server KIS keys must not become shared sources'))
    monkeypatch.setenv('TOSS_CLIENT_ID', '')
    first, second = QuoteHub(), QuoteHub()
    monkeypatch.setattr(first, '_load_holdings', AsyncMock())
    stop = asyncio.Event()
    task = asyncio.create_task(first.run(stop))
    try:
        await eventually(lambda: first.running)
        await second.run(asyncio.Event())
        assert second.reason == 'owner_conflict' and not second.sources
    finally:
        stop.set()
        await task
    third, done = QuoteHub(), asyncio.Event()
    done.set()
    await third.run(done)
    assert third.reason is None
