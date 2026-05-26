from unittest.mock import AsyncMock

import pytest

import kis_ws_manager
from routes import ws_quotes


class _FakeConn:
    def __init__(self):
        self.requested = None

    def update_subscriptions(self, requested):
        self.requested = requested


def test_multi_connection_plan_splits_live_codes_by_connection_capacity():
    codes = [f"{idx:06d}" for idx in range(45)]
    session = ws_quotes._Session()
    first = _FakeConn()
    second = _FakeConn()
    session.ws_conns = [first, second]

    result = ws_quotes._apply_multi_connection_plan(session, {"portfolio": codes})

    assert result["ws"] == codes
    assert result["rest"] == []
    assert first.requested == {"portfolio": codes[:kis_ws_manager.MAX_SUBSCRIPTIONS]}
    assert second.requested == {"portfolio": codes[kis_ws_manager.MAX_SUBSCRIPTIONS:]}


@pytest.mark.asyncio
async def test_session_capacity_expansion_evicts_other_session_when_slot_is_busy(monkeypatch):
    codes = [f"{idx:06d}" for idx in range(45)]
    session = ws_quotes._Session()
    session.is_active = True
    session.ws_conns = [_FakeConn()]
    session.key_slots = ["slot-1"]

    acquired = [None, "slot-2"]
    evict = AsyncMock(return_value=True)

    async def acquire():
        return acquired.pop(0)

    async def start_connection(_websocket, active_session, key_slot):
        active_session.key_slots.append(key_slot)
        active_session.ws_conns.append(_FakeConn())

    monkeypatch.setattr(ws_quotes.kis_key_manager, "total_count", lambda: 2)
    monkeypatch.setattr(ws_quotes.kis_key_manager, "acquire", acquire)
    monkeypatch.setattr(ws_quotes, "_evict_oldest_session", evict)
    monkeypatch.setattr(ws_quotes, "_start_connection", start_connection)

    await ws_quotes._ensure_session_capacity(None, session, {"portfolio": codes})

    evict.assert_awaited_once_with(exclude=session)
    assert len(session.ws_conns) == 2
    assert session.key_slots == ["slot-1", "slot-2"]
