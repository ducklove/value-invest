"""Shared httpx client registry for external service calls.

대부분의 외부 HTTP 호출이 요청마다 ``async with httpx.AsyncClient()`` 를
열어 새 커넥션 풀·TLS 핸드셰이크를 발생시킨다. 단일 호스트(라즈베리파이)
배포에서 FD/소켓 한계가 유한하므로, 동시 폭발 시 고갈 위험이 있다.

이 모듈은 앱 lifespan 이 소유하는 서비스별 장기 클라이언트 레지스트리를
제공한다. 호출부는 서비스 이름으로 클라이언트를 조회하며, per-request
timeout은 호출 시점 인자로 덮어쓸 수 있다(httpx 는
``client.get(url, timeout=X)`` 를 지원).

설계 노트:

* **서비스별 프로파일**. 외부 서비스마다 적정 timeout 이 다르다(DART 45s,
  OpenRouter 90s, Naver bulk 15s). 매니저는 서비스 이름 → 기본 timeout
  매핑을 갖고, 같은 서비스 호출은 같은 클라이언트(커넥션 풀)를 재사용한다.
* **lazy init**. ``get()`` 는 클라이언트가 없으면 생성한다. 덕분에 lifespan
  을 거치지 않는 배치 스크립트(snapshot_*.py)에서도 동작한다 — 다만 명시적
  close 가 없으면 커넥션이 프로세스 종료 시까지 남으므로, 배치에서는
  ``batch_http_client()`` 컨텍스트 매니저 사용을 권장한다.
* **기존 자체 싱글톤 유지**. ``kis_proxy_client``·``close_price_client`` 는
  rate limiting·회로차단 등 추가 로직을 가진 자체 클라이언트를 이미 쓴다.
  이 레지스트리는 그 두 클라이언트를 대체하지 않고, 나머지 ``async with``
  패턴을 재사용으로 전환한다.
* **per-request timeout**. 서비스 기본 timeout 이 부적절한 특정 호출은
  ``client.get(url, timeout=…)`` 로 덮어쓴다. 클라이언트의 기본 timeout 은
  해당 서비스의 일반적 상한선이다.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx

logger = logging.getLogger(__name__)

# 앱 전체 FD/소켓 예산을 공유하므로, 클라이언트 수만큼 풀이 쌓이지 않도록
# limits 는 매니저 공통값을 쓴다. httpx 기본값(100/20)을 유지하되, 향후
# 튜닝 시 한 곳에서 조정한다.
_DEFAULT_LIMITS = httpx.Limits(max_connections=100, max_keepalive_connections=20)

# 서비스 이름 → 기본 timeout (초). 호출부에서 per-request 로 덮어쓸 수 있다.
# 이 목록에 없는 이름은 "default" 프로파일(아래)을 쓴다.
_TIMEOUT_PROFILES: dict[str, float] = {
    "dart": 45.0,
    "openrouter": 90.0,
    "naver_bulk": 15.0,
    "naver": 8.0,
    "yahoo": 10.0,
    "report": 15.0,
    "kakao": 15.0,
    "telegram": 10.0,
    "wiki": 60.0,
    "upbit": 5.0,
    "gold_api": 10.0,
    "kis": 15.0,
    "external_tools": 8.0,
    "external_tools_etf": 20.0,
    "economic_calendar": 8.0,
    "preferred_dividends": 30.0,
    "market_indicators": 8.0,
    "default": 30.0,
}


def timeout_for(name: str) -> float:
    """서비스 이름의 기본 timeout. 미등록 이름은 default 프로파일."""
    return _TIMEOUT_PROFILES.get(name, _TIMEOUT_PROFILES["default"])


class HttpClientManager:
    """앱 lifespan 이 소유하는 서비스별 httpx 클라이언트 레지스트리.

    스레드 안전성: asyncio 단일 루프 가정. 클라이언트 생성에만 락을 잡고,
    조회(get)는 락 없이 dict 읽기로 빠르게 반환한다 — 클라이언트는 생성 후
    불변이므로 데이터 레이스가 없다.
    """

    def __init__(self, *, limits: httpx.Limits | None = None) -> None:
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._lock = asyncio.Lock()
        # limits 는 인스턴스별로 주입 가능(테스트용). 기본은 모듈 공통값.
        self._limits = limits or _DEFAULT_LIMITS

    async def get(self, name: str = "default") -> httpx.AsyncClient:
        """서비스 이름의 클라이언트를 반환. 없으면 lazy create."""
        client = self._clients.get(name)
        if client is not None:
            return client
        async with self._lock:
            # double-checked: 락 대기 중 다른 task 가 만들었을 수 있다.
            client = self._clients.get(name)
            if client is not None:
                return client
            client = httpx.AsyncClient(
                timeout=timeout_for(name),
                follow_redirects=True,
                limits=self._limits,
            )
            self._clients[name] = client
            logger.debug("http client created: name=%s", name)
            return client

    async def init_all(self, names: list[str] | None = None) -> None:
        """lifespan 시작에 미리 클라이언트를 생성한다(옵션).

        ``names`` 가 None 이면 등록된 모든 프로파일을, 아니면 해당 이름만.
        lazy init 이 이미 동작하므로 필수는 아니지만, 첫 요청 지연을 줄인다.
        """
        targets = names or list(_TIMEOUT_PROFILES.keys())
        for name in targets:
            await self.get(name)

    async def close_all(self) -> None:
        """lifespan 종료에 모든 클라이언트를 닫는다."""
        clients = list(self._clients.items())
        self._clients.clear()
        for name, client in clients:
            try:
                await client.aclose()
            except Exception as exc:  # noqa: BLE001 — 종료 경로, 한 클라이언트 실패가 다음을 막지 않게
                logger.warning("http client close failed: name=%s err=%s", name, exc)


# --- 전역 매니저 (앱 프로세스 1개 가정) ---
# lifespan 이 init/close 하고, 호출부는 get_http_client(name) 으로 조회.
_manager: HttpClientManager | None = None
_manager_lock = asyncio.Lock()


async def init_http_clients(names: list[str] | None = None) -> None:
    """앱 lifespan 시작에 호출. 전역 매니저를 만들고 클라이언트를预热."""
    global _manager
    async with _manager_lock:
        if _manager is None:
            _manager = HttpClientManager()
        await _manager.init_all(names)


async def close_http_clients() -> None:
    """앱 lifespan 종료에 호출. 전역 매니저의 클라이언트를 모두 닫는다."""
    global _manager
    async with _manager_lock:
        manager = _manager
        _manager = None
    if manager is not None:
        await manager.close_all()


async def get_http_client(name: str = "default") -> httpx.AsyncClient:
    """서비스 이름의 공유 클라이언트를 반환.

    매니저가 아직 초기화되지 않았으면(배치 스크립트 등 lifespan 밖) 임시
    매니저를 생성한다. 이 경우 명시적 close 를 호출하지 않으면 커넥션이
    프로세스 종료 시까지 남는다 — 배치에서는 ``batch_http_client()`` 사용 권장.
    """
    global _manager
    if _manager is None:
        async with _manager_lock:
            if _manager is None:
                _manager = HttpClientManager()
    return await _manager.get(name)


@asynccontextmanager
async def batch_http_client(
    *, name: str = "default", timeout: float | httpx.Timeout | None = None
) -> AsyncIterator[httpx.AsyncClient]:
    """배치 스크립트(snapshot_*.py 등 lifespan 밖)용 fallback.

    앱 매니저가 있으면 그것을 재사용하고, 없으면 이 스크립트 수명에만
    살고 닫히는 단기 클라이언트를 하나 만든다. ``timeout`` 을 주면
    per-request 기본 timeout 을 덮어쓴다(클라이언트 기본 timeout).

    사용법::

        async with batch_http_client(name="naver_bulk") as client:
            resp = await client.get(url)
    """
    if _manager is not None:
        # 앱 매니저가 살아 있으면 재사용 (lifespan 이 close 책임).
        yield await _manager.get(name)
        return
    # 배치 단독 실행: 이 컨텍스트에서만 살고 빠져나가며 닫힌다.
    client_timeout = timeout if timeout is not None else timeout_for(name)
    async with httpx.AsyncClient(
        timeout=client_timeout, follow_redirects=True, limits=_DEFAULT_LIMITS
    ) as client:
        yield client
