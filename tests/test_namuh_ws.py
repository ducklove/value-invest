import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from services.brokers import namuh_ws


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
