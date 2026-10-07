import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from repositories.broker_secrets import BrokerError
from services.brokers import namuh_ws


@pytest.mark.asyncio
async def test_exclusive_key_recovery_closes_owned_sockets_once_and_gates_new_connections(monkeypatch):
    cid = "managed-recovery"
    started, release, reconnected = asyncio.Event(), asyncio.Event(), asyncio.Event()
    events = []

    class Socket:
        async def __aenter__(self):
            events.append("open")
            return self

        async def __aexit__(self, *args):
            pass

        async def close(self):
            events.append("close")

    async def reset(*args):
        assert events == ["open", "close"]
        started.set()
        await release.wait()
        events.append("reset")

    monkeypatch.setattr(namuh_ws.websockets, "connect", lambda *args, **kwargs: Socket())
    monkeypatch.setattr(namuh_ws.brokers, "get_credential", AsyncMock(return_value={"ws_session_management": True}))
    reset_mock = AsyncMock(side_effect=reset)
    monkeypatch.setattr(namuh_ws.namuh, "close_ws_sessions", reset_mock)

    async def reopen():
        async with namuh_ws.connect(cid, "foreign", "wss://example"):
            reconnected.set()

    async with namuh_ws.connect(cid, "domestic", "wss://example"):
        recovery = asyncio.create_task(namuh_ws.recover("owner", cid, 0))
        await asyncio.wait_for(started.wait(), 1)
        concurrent = asyncio.create_task(namuh_ws.recover("owner", cid, 0))
        new_socket = asyncio.create_task(reopen())
        try:
            await asyncio.sleep(0)
            assert not reconnected.is_set()
        finally:
            release.set()
            assert await recovery
            assert not await concurrent
            await new_socket
    assert events == ["open", "close", "reset", "open"]
    reset_mock.assert_awaited_once_with("owner", cid)
    assert cid not in namuh_ws._connections


@pytest.mark.asyncio
async def test_shared_keys_and_other_keys_are_not_reset_and_failures_back_off(monkeypatch):
    secret = AsyncMock(return_value={})
    reset = AsyncMock()
    monkeypatch.setattr(namuh_ws.brokers, "get_credential", secret)
    monkeypatch.setattr(namuh_ws.namuh, "close_ws_sessions", reset)
    assert not await namuh_ws.recover("owner", "shared-key", 0)
    reset.assert_not_awaited()
    secret.return_value = {"ws_session_management": True}
    reset.side_effect = BrokerError("PRIVATE-TOKEN")
    with pytest.raises(asyncio.CancelledError):
        # Cancellation is never swallowed as successful recovery.
        reset.side_effect = asyncio.CancelledError
        await namuh_ws.recover("owner", "cancel-key", 0)
    reset.side_effect = BrokerError("PRIVATE-TOKEN")
    assert not await namuh_ws.recover("owner", "failed-key", 0)
    assert not await namuh_ws.recover("owner", "failed-key", namuh_ws.generation("failed-key"))
    assert reset.await_count == 2
    reset.side_effect = None
    assert await namuh_ws.recover("owner", "independent-key", 0)
    assert reset.await_count == 3


@pytest.mark.asyncio
async def test_two_connections_per_key_cancellation_and_independent_keys():
    release = asyncio.Event()
    opened = asyncio.Queue()

    async def connect(cid, role="quotes"):
        async with namuh_ws.slot(cid, role):
            await opened.put(cid)
            await release.wait()

    first = asyncio.create_task(connect("same-key"))
    second = asyncio.create_task(connect("same-key"))
    third = asyncio.create_task(connect("same-key", "scanner"))
    other = asyncio.create_task(connect("other-key"))
    try:
        entered = [await asyncio.wait_for(opened.get(), 1) for _ in range(3)]
        assert entered.count("same-key") == 2
        assert entered.count("other-key") == 1
        assert opened.empty()
        assert namuh_ws.requested("same-key", "scanner")
        assert not namuh_ws.requested("other-key", "scanner")
        third.cancel()
        await asyncio.gather(third, return_exceptions=True)
        assert not namuh_ws.requested("same-key", "scanner")
    finally:
        release.set()
        await asyncio.gather(first, second, third, other, return_exceptions=True)
    assert "same-key" not in namuh_ws._slots
    assert "other-key" not in namuh_ws._slots
    assert not namuh_ws.requested("same-key", "quotes")


@pytest.mark.asyncio
async def test_cancellation_releases_registrations_before_socket_and_slot(monkeypatch):
    monkeypatch.setattr(namuh_ws.asyncio, "sleep", AsyncMock())
    sent = []
    closed = False

    class Socket:
        async def __aenter__(self):
            return self

        async def send(self, raw):
            assert not closed
            assert namuh_ws.requested("cleanup-key", "quotes")
            sent.append(json.loads(raw))

        async def __aexit__(self, *args):
            nonlocal closed
            assert [message["header"]["tr_type"] for message in sent] == ["1", "1", "2", "2"]
            closed = True

    with pytest.raises(asyncio.CancelledError):
        async with namuh_ws.slot("cleanup-key"), Socket() as ws, namuh_ws.registrations(ws, "cleanup-key", "token", [("mc", "005930"), ("d2", "")]):
            await namuh_ws.subscribe(ws, "cleanup-key", "token", "mc", "005930")
            await namuh_ws.subscribe(ws, "cleanup-key", "token", "d2", "")
            raise asyncio.CancelledError
    assert closed
    assert [message["body"] for message in sent[:2]] == [message["body"] for message in sent[2:]]
    assert not namuh_ws.requested("cleanup-key", "quotes")


@pytest.mark.asyncio
async def test_failed_deregistration_preserves_original_cancellation(monkeypatch):
    monkeypatch.setattr(namuh_ws.asyncio, "sleep", AsyncMock())
    ws = AsyncMock()
    ws.send.side_effect = OSError("closed")
    with pytest.raises(asyncio.CancelledError):
        async with namuh_ws.registrations(ws, "closed-key", "token", [("mc", "005930")]):
            raise asyncio.CancelledError
