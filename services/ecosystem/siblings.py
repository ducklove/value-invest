"""형제 대시보드 summary-first 로더 (docs/ecosystem/data-contract.md §7).

도구마다 독립적으로 ``<레지스트리 url>/summary.json`` 을 먼저 받는다. 다음이면 그
도구만 레거시 파일(current.json/data.json …)과 기존 요약기로 폴백한다:
HTTP 오류(404 포함) · JSON 파싱 실패 · ``validate_envelope`` 실패(해시 불일치 포함) ·
``schemaVersion != 1`` · ``tool`` 불일치 · 스키마 필수 키 누락.

캐시 규칙:

- summary 는 ``MemoryTTLCache`` 900초. 만료되면 ``If-None-Match`` 조건부 GET, 304 면 TTL 만 연장.
- 404/410 과 계약 위반은 **음성 캐시**(900초) — 형제가 아직 summary 를 발행하지 않는
  동안 요청이 두 배로 늘지 않게 곧장 레거시로 간다.
- 네트워크 오류면 마지막 성공 envelope 를 1일까지 쓴다(stale 표시).
- 신선도는 ``generatedAt`` 이 아니라 envelope ``asOf``(데이터 기준 시각)로 본다.

URL 은 전부 레지스트리(``core.ecosystem``)에서 만들고 도구의 ``envOverride`` 를 따른다.
``ECOSYSTEM_SUMMARIES=0`` 이면 summary 단계를 끄고 항상 레거시 경로를 쓴다(운영 비상
스위치 겸 테스트 기본값 — tests/conftest.py).
"""

from __future__ import annotations

import copy
import logging
import os
from typing import Any, Awaitable, Callable

import httpx

from cache_layer import MemoryTTLCache, now_iso
from core import ecosystem
from core.http import get_http_client
from services.ecosystem.envelope import SummaryRejected, summary_tool_ids, validate_summary
from services.ecosystem.fetch import FETCH_ERRORS, entry_age_seconds, single_flight

logger = logging.getLogger(__name__)

SUMMARY_TTL = 900
STALE_MAX_SECONDS = 24 * 3600
_HTTP_CLIENT = "external_tools"  # core/http 에 등록된 형제 JSON 프로파일(8초)
_TIMEOUT = httpx.Timeout(8.0, connect=4.0)
_HEADERS = {"User-Agent": "value-invest/1.0"}

_summary_cache = MemoryTTLCache("ecosystem.summary", SUMMARY_TTL)
_missing_cache = MemoryTTLCache("ecosystem.summary.missing", SUMMARY_TTL)
# 도구별 마지막 로드 결과 — /api/ecosystem 신선도 표시용(캐시가 아니라 상태 기록).
_status: dict[str, dict[str, Any]] = {}


class UnknownTool(KeyError):
    """레지스트리에 없는 도구 id."""


# ---------------------------------------------------------------------------
# registry URLs
# ---------------------------------------------------------------------------

def _tool(tool_id: str) -> dict[str, Any]:
    entry = ecosystem.tool(tool_id)
    if entry is None:
        raise UnknownTool(tool_id)
    return entry


def base_url(tool_id: str) -> str:
    """env override 를 반영한 도구 기본 URL(끝 슬래시 없음)."""
    url = ecosystem.resolved_url(_tool(tool_id))
    if not url:
        raise UnknownTool(tool_id)
    return url


def site_url(tool_id: str) -> str:
    """사용자에게 보여줄 도구 홈(끝 슬래시 포함) — external_tools.SITE 의 원천."""
    return base_url(tool_id) + "/"


def summary_url(tool_id: str) -> str:
    return base_url(tool_id) + "/summary.json"


def data_url(tool_id: str, data_id: str) -> str:
    """레지스트리 ``data[]`` 의 레거시 파일 URL.

    Pages 아래 파일(레지스트리 url 로 시작)은 envOverride 가 있으면 그 주소로 옮긴다.
    raw.githubusercontent 파일은 브랜치가 박힌 원본 위치라 그대로 쓴다.
    """
    entry = _tool(tool_id)
    item = next((d for d in entry.get("data") or [] if d.get("id") == data_id), None)
    if item is None:
        raise UnknownTool(f"{tool_id}:{data_id}")
    url = str(item["url"])
    registry_base = str(entry.get("url") or "").rstrip("/")
    resolved = ecosystem.resolved_url(entry) or registry_base
    if registry_base and url.startswith(registry_base + "/") and resolved != registry_base:
        url = resolved + url[len(registry_base):]
    return url


# ---------------------------------------------------------------------------
# summary loader
# ---------------------------------------------------------------------------

def summaries_enabled() -> bool:
    return os.getenv("ECOSYSTEM_SUMMARIES", "1").strip().lower() not in {"0", "false", "off", "no"}


def _record(tool_id: str, *, source: str, envelope: dict | None = None, stale: bool = False,
            error: str | None = None) -> None:
    _status[tool_id] = {
        "source": source,
        "asOf": envelope.get("asOf") if envelope else None,
        "generatedAt": envelope.get("generatedAt") if envelope else None,
        "stale": stale,
        "checkedAt": now_iso(),
        "error": error,
    }


def record_legacy(tool_id: str, *, error: str | None = None) -> None:
    """레거시 경로로 로드했음을 기록(asOf/generatedAt 은 모름 → null)."""
    previous = _status.get(tool_id) or {}
    if error is None and previous.get("source") == "legacy":
        error = previous.get("error")  # summary 를 못 쓴 이유는 남겨 둔다
    _record(tool_id, source="legacy", error=error)


def freshness(tool_ids: list[str] | None = None) -> dict[str, dict[str, Any]]:
    """도구별 데이터 신선도. 아직 로드한 적 없으면 모든 값이 null."""
    ids = tool_ids if tool_ids is not None else summary_tool_ids()
    empty = {"source": None, "asOf": None, "generatedAt": None, "stale": False, "checkedAt": None, "error": None}
    return {tool_id: dict(_status.get(tool_id) or empty) for tool_id in ids}


async def _http_get(url: str, etag: str | None) -> tuple[int, Any, str | None]:
    """(status, json-or-None, etag). 304/404 는 본문 없이 돌려준다."""
    headers = dict(_HEADERS)
    if etag:
        headers["If-None-Match"] = etag
    client = await get_http_client(_HTTP_CLIENT)
    resp = await client.get(url, headers=headers, timeout=_TIMEOUT)
    status = resp.status_code
    if status in (304, 404, 410):
        return status, None, resp.headers.get("etag")
    resp.raise_for_status()
    return status, resp.json(), resp.headers.get("etag")


async def _refresh(tool_id: str) -> dict | None:
    previous = _summary_cache.get_entry(tool_id, allow_stale=True)
    prev_value = previous.value if previous else None
    etag = prev_value.get("etag") if prev_value else None
    try:
        status, body, new_etag = await _http_get(summary_url(tool_id), etag)
    except ValueError as exc:  # 200 인데 JSON 이 아니다 — 계약 위반, 음성 캐시
        logger.warning("summary.json %s is not JSON: %s", tool_id, exc)
        _summary_cache.delete(tool_id)
        _missing_cache.set(tool_id, True)
        _record(tool_id, source="legacy", error=f"rejected: {str(exc)[:180]}")
        return None
    except FETCH_ERRORS as exc:
        age = entry_age_seconds(previous.cached_at) if previous else None
        if prev_value and age is not None and age <= STALE_MAX_SECONDS:
            logger.warning("summary.json %s fetch failed, serving stale: %s", tool_id, exc)
            _record(tool_id, source="summary", envelope=prev_value["envelope"], stale=True, error=str(exc)[:200])
            return prev_value["envelope"]
        logger.info("summary.json %s unavailable (%s) — legacy fallback", tool_id, exc)
        _record(tool_id, source="legacy", error=str(exc)[:200])
        return None
    if status == 304 and prev_value:
        _summary_cache.set(tool_id, prev_value)  # TTL 만 연장
        _record(tool_id, source="summary", envelope=prev_value["envelope"])
        return prev_value["envelope"]
    if status in (304, 404, 410):
        _summary_cache.delete(tool_id)
        _missing_cache.set(tool_id, True)
        _record(tool_id, source="legacy")
        return None
    try:
        envelope = validate_summary(body, tool_id)
    except SummaryRejected as exc:
        logger.warning("summary.json %s rejected: %s", tool_id, exc)
        _summary_cache.delete(tool_id)
        _missing_cache.set(tool_id, True)
        _record(tool_id, source="legacy", error=f"rejected: {str(exc)[:180]}")
        return None
    _summary_cache.set(tool_id, {"envelope": envelope, "etag": new_etag})
    _missing_cache.delete(tool_id)
    _record(tool_id, source="summary", envelope=envelope)
    return envelope


async def fetch_summary(tool_id: str) -> dict | None:
    """검증된 envelope(복사본) 또는 None(→ 호출부가 레거시로 폴백)."""
    if not summaries_enabled():
        return None
    cached = _summary_cache.get(tool_id)
    if cached is not None:
        return cached["envelope"]
    if _missing_cache.get(tool_id):
        return None
    envelope, joined = await single_flight(("ecosystem.summary", tool_id), lambda: _refresh(tool_id))
    if envelope is None:
        return None
    # 합류한 호출자끼리 중첩 data 를 공유하지 않게 깊은 복사(cached_fetch 와 같은 의미).
    return copy.deepcopy(envelope) if joined else envelope


def invalidate(tool_id: str, reason: str) -> None:
    """변환 단계에서 쓸 수 없던 summary 를 음성 캐시로 돌린다."""
    logger.warning("summary.json %s unusable: %s", tool_id, reason)
    _summary_cache.delete(tool_id)
    _missing_cache.set(tool_id, True)
    _record(tool_id, source="legacy", error=f"unusable: {reason[:180]}")


async def summary_or_legacy(
    tool_id: str,
    from_summary: Callable[[dict], Any],
    legacy: Callable[[], Awaitable[Any]],
) -> Any:
    """summary 가 있으면 ``from_summary(envelope)``, 없거나 쓸 수 없으면 ``await legacy()``."""
    envelope = await fetch_summary(tool_id)
    if envelope is not None:
        try:
            return from_summary(envelope)
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            invalidate(tool_id, f"{type(exc).__name__}: {exc}")
    value = await legacy()
    record_legacy(tool_id)
    return value


def reset() -> None:
    """테스트용 — 캐시와 상태 기록을 비운다."""
    _summary_cache.clear()
    _missing_cache.clear()
    _status.clear()
