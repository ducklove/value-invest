"""Tests for core/http.py — shared httpx client manager."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from core import http as http_manager


@pytest.fixture(autouse=True)
def _reset_manager(monkeypatch):
    """각 테스트가 격리된 매니저를 쓰도록 전역 상태를 초기화."""
    monkeypatch.setattr(http_manager, "_manager", None)
    yield
    # teardown: 테스트가 만든 매니저 정리(동기 컨텍스트에서는 직접 close)
    manager = http_manager._manager
    if manager is not None:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(manager.close_all())
        finally:
            loop.close()


@pytest.mark.asyncio
async def test_get_returns_same_client_for_same_name():
    """같은 서비스 이름은 같은 클라이언트 인스턴스를 재사용한다."""
    a = await http_manager.get_http_client("naver")
    b = await http_manager.get_http_client("naver")
    assert a is b


@pytest.mark.asyncio
async def test_get_returns_different_clients_for_different_names():
    """서로 다른 이름은 별도 클라이언트(별도 풀)를 만든다."""
    a = await http_manager.get_http_client("naver")
    b = await http_manager.get_http_client("dart")
    assert a is not b


@pytest.mark.asyncio
async def test_timeout_profile_applied():
    """클라이언트 기본 timeout이 서비스 프로파일 값을 따른다."""
    client = await http_manager.get_http_client("dart")
    # httpx 는 기본 timeout 을 Timeout 객체로 저장한다.
    expected = http_manager.timeout_for("dart")
    # read timeout 이 기본값과 일치하는지 확인(Timeout.read)
    assert client.timeout.read == pytest.approx(expected)


@pytest.mark.asyncio
async def test_unknown_name_falls_back_to_default():
    """미등록 이름은 default 프로파일(30s)을 쓴다."""
    client = await http_manager.get_http_client("does-not-exist")
    assert client.timeout.read == pytest.approx(http_manager.timeout_for("default"))


@pytest.mark.asyncio
async def test_init_and_close_lifecycle():
    """init_http_clients → close_http_clients 가 클라이언트를 만들고 닫는다."""
    await http_manager.init_http_clients(["naver", "dart"])
    assert http_manager._manager is not None
    naver = await http_manager.get_http_client("naver")
    assert naver is not None
    await http_manager.close_http_clients()
    # close 후 매니저는 None 으로 해제된다.
    assert http_manager._manager is None


@pytest.mark.asyncio
async def test_close_all_clears_clients():
    """close_all 이 클라이언트를 닫고 레지스트리를 비운다."""
    await http_manager.get_http_client("naver")
    manager = http_manager._manager
    assert manager is not None and manager._clients
    await manager.close_all()
    assert manager._clients == {}


@pytest.mark.asyncio
async def test_batch_http_client_creates_short_lived_client_when_no_manager():
    """앱 매니저가 없으면 batch_http_client 가 단기 클라이언트를 만들고 닫는다."""
    # 매니저 미초기화 상태
    assert http_manager._manager is None
    async with http_manager.batch_http_client(name="naver") as client:
        assert isinstance(client, httpx.AsyncClient)
        assert client.timeout.read == pytest.approx(http_manager.timeout_for("naver"))
    # 컨텍스트 종료 후 클라이언트는 닫혔어야 한다 — is_closed 플래그로 확인.
    # (httpx.AsyncClient.aclose 호출 시 is_closed True)
    assert client.is_closed


@pytest.mark.asyncio
async def test_batch_http_client_reuses_app_manager_when_present():
    """앱 매니저가 살아 있으면 batch_http_client 는 그것을 재사용(단기 생성 X)."""
    await http_manager.init_http_clients(["naver"])
    app_client = await http_manager.get_http_client("naver")
    async with http_manager.batch_http_client(name="naver") as client:
        # 같은 매니저 클라이언트 인스턴스
        assert client is app_client
        # 닫히지 않은 채로 빠져나감(app lifespan 이 close 책임)
        assert not client.is_closed


@pytest.mark.asyncio
async def test_concurrent_get_does_not_create_duplicates():
    """동시 get 호출이 같은 이름에 대해 클라이언트를 1개만 만든다(락 보호)."""
    clients = await asyncio.gather(
        *(http_manager.get_http_client("naver") for _ in range(10))
    )
    first = clients[0]
    assert all(c is first for c in clients)
    manager = http_manager._manager
    assert manager is not None
    assert len(manager._clients) == 1
