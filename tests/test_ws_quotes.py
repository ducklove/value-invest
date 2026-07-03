from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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
    session.conns = [
        ws_quotes._Conn("slot-1", first),
        ws_quotes._Conn("slot-2", second),
    ]

    result = ws_quotes._apply_multi_connection_plan(session, {"portfolio": codes})

    assert result["ws"] == codes
    assert result["rest"] == []
    assert first.requested == {"portfolio": codes[:kis_ws_manager.MAX_SUBSCRIPTIONS]}
    assert second.requested == {"portfolio": codes[kis_ws_manager.MAX_SUBSCRIPTIONS:]}


def test_can_takeover_requires_admin_user():
    assert ws_quotes._can_takeover({"google_sub": "admin", "is_admin": True}) is True
    assert ws_quotes._can_takeover({"google_sub": "u1", "is_admin": False}) is False
    assert ws_quotes._can_takeover(None) is False


def test_websocket_takeover_requires_admin_session(monkeypatch):
    app = FastAPI()
    app.include_router(ws_quotes.router)

    monkeypatch.setattr(ws_quotes, "_origin_allowed", lambda _origin: True)
    monkeypatch.setattr(ws_quotes.kis_ws_manager, "ws_cache_matches_rest_market", lambda: True)
    monkeypatch.setattr(ws_quotes.kis_ws_manager, "get_all_cached_quotes", lambda: {})
    monkeypatch.setattr(ws_quotes.kis_key_manager, "available_count", lambda: 1)
    monkeypatch.setattr(ws_quotes.kis_key_manager, "total_count", lambda: 1)

    with TestClient(app) as client:
        with client.websocket_connect("/ws/quotes", headers={"origin": "http://testserver"}) as websocket:
            status = websocket.receive_json()
            assert status["type"] == "ws_status"
            assert status["can_takeover"] is False

            websocket.send_json({"action": "takeover"})
            denied = websocket.receive_json()
            assert denied["type"] == "ws_status"
            assert denied["active"] is False
            assert denied["forbidden"] is True
            assert denied["can_takeover"] is False


@pytest.mark.asyncio
async def test_session_capacity_expansion_does_not_evict_other_session_when_slot_is_busy(monkeypatch):
    codes = [f"{idx:06d}" for idx in range(45)]
    session = ws_quotes._Session()
    session.is_active = True
    session.conns = [ws_quotes._Conn("slot-1", _FakeConn())]

    acquired = [None]
    evict = AsyncMock(return_value=True)

    async def acquire():
        return acquired.pop(0)

    async def start_connection(_websocket, active_session, key_slot):
        active_session.conns.append(ws_quotes._Conn(key_slot, _FakeConn()))

    monkeypatch.setattr(ws_quotes.kis_key_manager, "total_count", lambda: 2)
    monkeypatch.setattr(ws_quotes.kis_key_manager, "acquire", acquire)
    monkeypatch.setattr(ws_quotes, "_evict_oldest_session", evict)
    monkeypatch.setattr(ws_quotes, "_start_connection", start_connection)

    await ws_quotes._ensure_session_capacity(None, session, {"portfolio": codes})

    evict.assert_not_awaited()
    assert len(session.conns) == 1
    assert [entry.key_slot for entry in session.conns] == ["slot-1"]
