"""services/market/sources/finance_pi.py — finance-pi 단일 provider."""

import asyncio
import importlib
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from services.market.sources import close_price as close_price_client
from services.market.sources import finance_pi


@pytest.fixture(autouse=True)
def _reset_cooldown():
    finance_pi.reset_cooldown()
    yield
    finance_pi.reset_cooldown()


def _reload_with_env(monkeypatch, env):
    for name in ("FINANCE_PI_BASE_URL", "CLOSE_PRICE_API_BASE_URL", "FINANCE_PI_API_TOKEN", "CLOSE_PRICE_API_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return importlib.reload(finance_pi)


def test_legacy_env_names_win_when_both_set_to_preserve_existing_env(monkeypatch):
    # 운영 .env 에 이미 있는 구 이름 값이 도입 전과 똑같이 쓰여야 한다
    # (예전 코드: BASE_URL 은 구 이름만, 토큰은 CLOSE_PRICE_API_TOKEN 우선).
    try:
        mod = _reload_with_env(monkeypatch, {
            "FINANCE_PI_BASE_URL": "http://pi.test:8400/",
            "CLOSE_PRICE_API_BASE_URL": "http://legacy.test/",
            "FINANCE_PI_API_TOKEN": "std-token",
            "CLOSE_PRICE_API_TOKEN": "legacy-token",
        })
        assert mod.BASE_URL == "http://legacy.test"
        assert mod.API_TOKEN == "legacy-token"

        mod = _reload_with_env(monkeypatch, {
            "FINANCE_PI_BASE_URL": "http://pi.test:8400/",
            "FINANCE_PI_API_TOKEN": " std-token ",
        })
        assert mod.BASE_URL == "http://pi.test:8400"
        assert mod.API_TOKEN == "std-token"

        mod = _reload_with_env(monkeypatch, {})
        assert mod.BASE_URL == finance_pi.DEFAULT_BASE_URL
        assert mod.API_TOKEN == ""
        assert mod.auth_headers() == {}
    finally:
        monkeypatch.undo()
        importlib.reload(finance_pi)


def test_close_price_client_reexports_provider_settings():
    assert close_price_client.BASE_URL == finance_pi.BASE_URL
    assert close_price_client.ENABLED == finance_pi.ENABLED
    assert close_price_client.API_TOKEN == finance_pi.API_TOKEN


def test_request_builds_url_and_auth_header_on_named_shared_client():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"ok": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    getter = AsyncMock(return_value=client)

    async def run():
        with patch.object(finance_pi, "get_http_client", getter), \
             patch.object(finance_pi, "BASE_URL", "http://pi.test"), \
             patch.object(finance_pi, "API_TOKEN", "tok"):
            data = await finance_pi.get_json("/api/prices/close", {"ticker": "005930"})
            response = await finance_pi.request(
                "POST", "/api/research/basis-analysis", json={"a": 1},
                client_name=finance_pi.RESEARCH_CLIENT_NAME,
            )
        return data, response

    data, response = asyncio.run(run())
    assert data == {"ok": True} and response.status_code == 200
    assert [str(r.url) for r in seen] == [
        "http://pi.test/api/prices/close?ticker=005930",
        "http://pi.test/api/research/basis-analysis",
    ]
    assert all(r.headers["X-Admin-Token"] == "tok" for r in seen)
    assert [c.args[0] for c in getter.await_args_list] == ["finance_pi", "quant_research"]


def test_price_calls_skip_network_during_cooldown_and_quant_does_not():
    from services.quant import service as quant_service

    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/api/prices/close":
            return httpx.Response(503)
        return httpx.Response(200, json={"status": "ready"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def run():
        with patch.object(finance_pi, "get_http_client", AsyncMock(return_value=client)), \
             patch.object(finance_pi, "ENABLED", True), \
             patch.object(close_price_client, "ENABLED", True):
            with pytest.raises(close_price_client.ClosePriceClientError):
                await close_price_client.get_daily_closes("005930", since="2026-09-01", until="2026-09-30")
            assert finance_pi.cooldown_active()
            # 쿨다운 중 가격 조회는 네트워크 없이 빈 결과.
            assert await close_price_client.get_daily_closes(
                "005930", since="2026-09-01", until="2026-09-30"
            ) == []
            # 연구 엔드포인트는 기존처럼 쿨다운과 무관하다.
            assert await quant_service.fetch("/api/ready") == {"status": "ready"}

    asyncio.run(run())
    assert calls == ["/api/prices/close", "/api/ready"]


def test_cooldown_bypassed_keeps_the_later_deadline():
    async def run():
        with patch.object(finance_pi, "FAILURE_COOLDOWN_SECONDS", 60.0):
            finance_pi.mark_failure()
            before = finance_pi._skip_until
            with finance_pi.cooldown_bypassed():
                assert not finance_pi.cooldown_active()
            assert finance_pi._skip_until == before
            with finance_pi.cooldown_bypassed():
                finance_pi._skip_until = before + 30
            assert finance_pi._skip_until == before + 30

    asyncio.run(run())


@pytest.mark.parametrize("status,expected", [(500, True), (503, True), (429, True), (404, False), (400, False)])
def test_should_mark_failure(status, expected):
    request = httpx.Request("GET", "http://pi.test/x")
    exc = httpx.HTTPStatusError("x", request=request, response=httpx.Response(status, request=request))
    assert finance_pi.should_mark_failure(exc) is expected
    assert finance_pi.should_mark_failure(httpx.ConnectError("down")) is True
