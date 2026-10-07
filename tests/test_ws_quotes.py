import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, WebSocketDisconnect
from fastapi.testclient import TestClient

from core.config import DEFAULT_CORS_ORIGINS
from routes import ws_quotes
from services.realtime.hub import QuoteHub


def test_origin_allowed_accepts_local_dev_preview_and_rejects_other_origins(monkeypatch):
    monkeypatch.setattr(ws_quotes, "get_settings", lambda: SimpleNamespace(cors_allowed_origins=DEFAULT_CORS_ORIGINS))
    for origin in ["http://localhost:8021", "http://127.0.0.1:8021", "http://localhost:8000"]:
        assert ws_quotes._origin_allowed(origin)
    assert not ws_quotes._origin_allowed("http://evil.example")
    assert not ws_quotes._origin_allowed(None)


def setup_app(monkeypatch, user):
    hub = QuoteHub()
    hub.running = True
    monkeypatch.setattr(hub, '_nh_coverage', lambda _: set())
    monkeypatch.setattr(ws_quotes, "get_hub", lambda: hub)
    monkeypatch.setattr(ws_quotes, "_origin_allowed", lambda origin: True)
    monkeypatch.setattr(ws_quotes, "get_current_user", AsyncMock(return_value=user))

    @asynccontextmanager
    async def lifespan(app):
        async def pump():
            while True:
                await hub.reconcile()
                await asyncio.sleep(.02)
        task = asyncio.create_task(pump())
        try: yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    app = FastAPI(lifespan=lifespan)
    app.include_router(ws_quotes.router)
    return app, hub


def receive_type(socket, kind, predicate=lambda message: True):
    for _ in range(20):
        message = socket.receive_json()
        if message['type'] == kind and predicate(message): return message
    raise AssertionError(f'{kind} not received')


def test_multiple_browsers_automatically_share_without_takeover_or_slot_acquisition(monkeypatch):
    app, hub = setup_app(monkeypatch, {'google_sub': 'owner', 'is_admin': False})
    with TestClient(app) as client:
        with client.websocket_connect('/ws/quotes') as first, client.websocket_connect('/ws/quotes') as second:
            for socket in [first, second]:
                status = receive_type(socket, 'ws_status')
                assert status['active'] and status['shared'] and not status['can_takeover']
                socket.send_json({'action': 'subscribe', 'requested': {'portfolio': ['005930']}})
                assert receive_type(socket, 'subscriptions', lambda message: bool(message['rest']))['rest'] == ['005930']
            first.send_json({'action': 'takeover'})
            assert receive_type(first, 'ws_status')['active']
            assert len(hub.clients) == 2
            second.send_json({'action': 'ping'})
            assert receive_type(second, 'pong')['shared']
        assert len(hub.clients) == 0


def test_anonymous_client_cannot_consume_live_resources(monkeypatch):
    app, hub = setup_app(monkeypatch, None)
    with TestClient(app) as client, client.websocket_connect('/ws/quotes') as socket:
        assert not socket.receive_json()['active']
        socket.send_json({'action': 'subscribe', 'requested': {'portfolio': ['005930']}})
        assert socket.receive_json()['rest'] == ['005930']
        assert not hub.clients


def test_expired_authentication_closes_socket(monkeypatch):
    app, hub = setup_app(monkeypatch, {'google_sub': 'owner'})
    monkeypatch.setattr(ws_quotes, 'get_current_user', AsyncMock(side_effect=[{'google_sub': 'owner'}, None]))
    with TestClient(app) as client, client.websocket_connect('/ws/quotes') as socket:
        socket.receive_json()
        socket.send_json({'action': 'ping'})
        with pytest.raises(WebSocketDisconnect) as closed:
            receive_type(socket, 'pong')
        assert closed.value.code == 1008


@pytest.mark.parametrize('user,status', [(None,403), ({'google_sub':'owner'},403), ({'google_sub':'owner','is_admin':True},200)])
def test_connection_diagnostics_requires_admin(monkeypatch,user,status):
    app, _ = setup_app(monkeypatch,user)
    with TestClient(app) as client:
        assert client.get('/api/realtime/status').status_code == status
