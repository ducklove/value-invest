"""finance-pi 내부 API provider — 주소·인증·클라이언트·장애 쿨다운의 단일 출처.

finance-pi(라즈베리파이 데이터레이크, 기본 ``http://192.168.68.84:8400``)는
종가·일봉·거시지표·기초재무·스크리너(``services.market.sources.close_price``, 구 루트 ``close_price_client``)와 퀀트 연구
엔드포인트(``services/quant``)를 제공한다. 예전에는 두 쪽이 각자 URL·
``X-Admin-Token`` 헤더를 만들었고 close_price_client 는 자체 AsyncClient 를
열었다. 이제 둘 다 이 모듈을 거친다.

환경변수 (새 이름 ``FINANCE_PI_*``, 구 이름 ``CLOSE_PRICE_API_*`` 도 동작):

* ``FINANCE_PI_BASE_URL``  / 구 이름 ``CLOSE_PRICE_API_BASE_URL``
* ``FINANCE_PI_API_TOKEN`` / 구 이름 ``CLOSE_PRICE_API_TOKEN``

둘 다 설정돼 있으면 **구 이름이 우선**한다 — 운영 ``.env`` 에 이미 있는 값이
이 모듈 도입 전과 똑같이 쓰이게 하기 위해서다(예전 코드는 BASE_URL 에 구
이름만 읽었고, 토큰은 ``CLOSE_PRICE_API_TOKEN`` 을 먼저 봤다). ``.env`` 를 새
이름으로 옮길 때는 구 이름 줄을 지운다.
* ``CLOSE_PRICE_API_ENABLED`` (0/false/no/off 면 비활성)
* ``CLOSE_PRICE_API_TIMEOUT_SECONDS`` / ``..._FUNDAMENTALS_TIMEOUT_SECONDS`` /
  ``..._FAILURE_COOLDOWN_SECONDS``
* ``CLOSE_PRICE_API_PRICE_TIMEOUT_SECONDS`` (기본 10) — 가격·거시 조회 제한 시간.
  예전에는 이 호출들이 ``timeout=None`` 으로 나가 finance-pi 가 멈추면 요청이
  끝없이 기다렸다. 전체 이력(1985~) 조회가 수 초 걸리므로 2.5초 기본값 대신 따로 둔다.

클라이언트는 ``core/http`` 공유 풀(``finance_pi`` = 가격·재무 조회,
``quant_research`` = 긴 연구 계산)을 쓴다. 쿨다운은 가격 엔드포인트의
5xx/429/연결 실패가 켜고, 켜져 있는 동안 가격 조회는 네트워크에 닿지 않는다.
연구 엔드포인트는 기존처럼 쿨다운과 무관하다(``request`` 는 쿨다운을 보지
않는다 — 호출부가 ``cooldown_active()`` 로 판단한다).
"""

from __future__ import annotations

import asyncio
import os
from contextlib import contextmanager
from typing import Any, Iterator

import httpx

from core.http import get_http_client

DEFAULT_BASE_URL = "http://192.168.68.84:8400"


def _env(*names: str, default: str = "") -> str:
    """첫 번째로 설정된(빈 문자열이 아닌) 환경변수 값."""
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value
    return default


# 구 이름 우선 — 기존 운영 .env 동작 보존(위 docstring 참고).
BASE_URL = _env("CLOSE_PRICE_API_BASE_URL", "FINANCE_PI_BASE_URL", default=DEFAULT_BASE_URL).strip().rstrip("/")
API_TOKEN = _env("CLOSE_PRICE_API_TOKEN", "FINANCE_PI_API_TOKEN").strip()
ENABLED = os.getenv("CLOSE_PRICE_API_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
TIMEOUT_SECONDS = float(os.getenv("CLOSE_PRICE_API_TIMEOUT_SECONDS", "2.5"))
FUNDAMENTALS_TIMEOUT_SECONDS = float(os.getenv("CLOSE_PRICE_API_FUNDAMENTALS_TIMEOUT_SECONDS", "6.0"))
FAILURE_COOLDOWN_SECONDS = float(os.getenv("CLOSE_PRICE_API_FAILURE_COOLDOWN_SECONDS", "60"))
PRICE_TIMEOUT_SECONDS = float(os.getenv("CLOSE_PRICE_API_PRICE_TIMEOUT_SECONDS", "10"))

CLIENT_NAME = "finance_pi"
RESEARCH_CLIENT_NAME = "quant_research"

_skip_until: float = 0.0


def auth_headers() -> dict[str, str]:
    return {"X-Admin-Token": API_TOKEN} if API_TOKEN else {}


def url(path: str) -> str:
    return f"{BASE_URL}{path}"


async def _get_client(client_name: str = CLIENT_NAME) -> httpx.AsyncClient:
    return await get_http_client(client_name)


async def request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json: Any = None,
    timeout: Any = httpx.USE_CLIENT_DEFAULT,
    client_name: str = CLIENT_NAME,
) -> httpx.Response:
    """인증 헤더를 붙여 finance-pi 에 요청하고 응답을 그대로 돌려준다.
    상태 코드 해석·쿨다운 판단은 호출부 몫이다."""
    client = await _get_client(client_name)
    kwargs: dict[str, Any] = {"headers": auth_headers() or None, "timeout": timeout}
    if params is not None:
        kwargs["params"] = params
    if json is not None:
        kwargs["json"] = json
    return await client.request(method, url(path), **kwargs)


async def get_json(path: str, params: dict[str, Any], *, timeout: float | None = None) -> Any:
    """가격·재무 조회용 GET → JSON. 비 2xx 는 ``httpx.HTTPStatusError``.

    ``timeout`` 을 주지 않으면 :data:`PRICE_TIMEOUT_SECONDS` (기본 10초)를 쓴다.
    제한 시간 초과는 ``httpx.TimeoutException`` (전송 오류)이라 호출부의
    쿨다운 판단에 걸린다."""
    effective = PRICE_TIMEOUT_SECONDS if timeout is None else timeout
    response = await request("GET", path, params=params, timeout=effective)
    response.raise_for_status()
    return response.json()


# --- 장애 쿨다운 (circuit) ------------------------------------------------

def cooldown_active() -> bool:
    if _skip_until <= 0:
        return False
    return asyncio.get_event_loop().time() < _skip_until


def mark_failure() -> None:
    global _skip_until
    if FAILURE_COOLDOWN_SECONDS > 0:
        _skip_until = asyncio.get_event_loop().time() + FAILURE_COOLDOWN_SECONDS


def should_mark_failure(exc: BaseException) -> bool:
    """5xx·429·전송 실패만 쿨다운 대상. 4xx 는 요청 문제라 켜지 않는다."""
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return True


def reset_cooldown() -> None:
    """테스트·운영 도구용."""
    global _skip_until
    _skip_until = 0.0


@contextmanager
def cooldown_bypassed() -> Iterator[None]:
    """블록 안에서만 쿨다운을 무시한다. 블록 안에서 새로 켜진 쿨다운과 기존
    쿨다운 중 늦은 쪽이 남는다."""
    global _skip_until
    saved = _skip_until
    _skip_until = 0.0
    try:
        yield
    finally:
        _skip_until = max(_skip_until, saved)
