"""X-Ray 입력 데이터 로더 — ETF 구성·지주사 지분·우선주 쌍·상장법인 업종.

요청 경로에서 큰 파일을 받지 않도록 메모리 → DB(``cache_values``) → 네트워크 순으로 읽는다.
재시작이 잦아도(배포마다) 다시 내려받지 않고, 원천이 실패하면 마지막 값을 낡은 채로 쓴다.
로더는 예외를 올리지 않는다. 자료가 없으면 ``available=False`` 를 돌려주고 화면이 그 사실을
보여 준다(도구 하나의 실패가 카드 전체를 막지 않는다).

ETF 구성을 왜 이렇게 받는가(``docs/ecosystem/data-contract.md`` §9 검토):

* ``summary.json`` 은 64 KB 예산이다. 비중 있는 구성 행만 약 5,500개, 200 KB 를 넘어 들어가지 않는다.
* 그래서 허브가 ``data/etfs.json``(9 MB)을 ETag 조건부로 받아, 파일이 바뀐 때만 파싱해
  필요한 필드만 남긴 압축본을 DB 에 둔다. 요청은 압축본만 읽으므로 경량 파일을 따로 발행할 이득이 작고,
  형제 저장소를 바꾸지 않아 그쪽 배포 상태에도 묶이지 않는다.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

import httpx

from cache_layer import MemoryTTLCache, now_iso
from core.http import get_http_client
from domain.timeutil import KST
from repositories import cache_values
from services.ecosystem import external_tools, siblings
from services.ecosystem.fetch import FETCH_ERRORS
from services.market.sources import kind, yahoo

logger = logging.getLogger(__name__)

_NAMESPACE = "portfolio.exposure"
_HEADERS = {"User-Agent": "value-invest/1.0"}
_ETF_TIMEOUT = httpx.Timeout(60.0, connect=5.0)
_HOLDCO_TIMEOUT = httpx.Timeout(10.0, connect=4.0)
# 실패 직후 요청마다 다시 시도하지 않는 간격.
_RETRY_SECONDS = 600


@dataclass(frozen=True)
class _Source:
    key: str
    ttl_seconds: float
    cache: MemoryTTLCache


_ETF_SOURCE = _Source("etf_holdings", 6 * 3600, MemoryTTLCache("portfolio.exposure.etf", None))
_HOLDCO_SOURCE = _Source("holdcos", 6 * 3600, MemoryTTLCache("portfolio.exposure.holdco", None))
_COMPANY_SOURCE = _Source("listed_companies", 30 * 24 * 3600, MemoryTTLCache("portfolio.exposure.company", None))
_pair_cache = MemoryTTLCache("portfolio.exposure.pairs", 3600)
_INSTRUMENT_KEY = "instrument_types"
_INSTRUMENT_TTL_SECONDS = 90 * 24 * 3600  # 종목 유형은 거의 바뀌지 않는다.
_INSTRUMENT_TIMEOUT_SECONDS = 6.0
_INSTRUMENT_CONCURRENCY = 3
_instrument_cache = MemoryTTLCache("portfolio.exposure.instrument", None)


def _unavailable(error: str) -> dict:
    return {"available": False, "as_of": None, "error": error[:200]}


async def _load_persisted(
    source: _Source,
    refresh: Callable[[dict | None], Awaitable[dict]],
) -> dict:
    """메모리(읽기 전용 원본) → DB → ``refresh`` 순으로 값을 얻는다.

    ``refresh(이전 값)`` 은 새 값(변경 없음이면 이전 값)을 돌려주거나 FETCH_ERRORS 를 올린다.
    실패하면 이전 값을 ``stale=True`` 로 쓰고, 없으면 ``available=False`` 를 돌려준다.
    """
    hit = source.cache.get(source.key, copy=False)
    if hit is not None:
        return hit

    async def run() -> dict:
        entry = await cache_values.get_cache_value_entry(_NAMESPACE, source.key, allow_stale=True)
        previous = entry.value if entry else None
        if entry is not None and not entry.stale:
            source.cache.set(source.key, previous, ttl_seconds=source.ttl_seconds, copy=False)
            return previous
        try:
            value = await refresh(previous)
        except FETCH_ERRORS as exc:
            logger.warning("X-Ray source %s refresh failed: %r", source.key, exc)
            fallback = {**previous, "stale": True} if previous else _unavailable(repr(exc))
            source.cache.set(source.key, fallback, ttl_seconds=_RETRY_SECONDS, copy=False)
            return fallback
        await cache_values.set_cache_value(_NAMESPACE, source.key, value, ttl_seconds=source.ttl_seconds)
        source.cache.set(source.key, value, ttl_seconds=source.ttl_seconds, copy=False)
        return value

    value, _ = await source.cache.flight.run(source.key, run)
    return value


def _kst_date(value: Any) -> str | None:
    """ISO 시각 문자열 → KST 날짜. 오프셋이 없거나 해석할 수 없으면 앞 10자."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text[:10]
    if parsed.tzinfo is None:
        return parsed.date().isoformat()
    return parsed.astimezone(KST).date().isoformat()


def compact_etf_snapshot(raw: bytes | str) -> dict:
    """eiayn ``data/etfs.json`` → 필요한 필드만 남긴 압축본(순수 함수).

    비중이 있는 구성만 ``rows`` 에 남기고, 이름만 있는 행은 개수(``unweighted``)로 접는다.
    형식이 어긋나면 ValueError.
    """
    payload = json.loads(raw)
    rows = payload.get("etfs") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        raise ValueError("etfs.json 에 etfs 배열이 없습니다")
    etfs: dict[str, dict] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        code = str(item.get("id") or item.get("ticker") or "").strip().upper()
        if not code:
            continue
        kept: list[list[Any]] = []
        unweighted = 0
        for holding in item.get("holdings") or []:
            if not isinstance(holding, dict):
                continue
            weight = holding.get("weight")
            if isinstance(weight, bool) or not isinstance(weight, (int, float)):
                unweighted += 1
                continue
            kept.append([holding.get("ticker"), holding.get("name"), weight])
        etfs[code] = {
            "name": str(item.get("shortName") or item.get("name") or code).strip(),
            "full_name": str(item.get("name") or "").strip(),
            "asset_class": item.get("assetClass"),
            "currency": item.get("currency"),
            "unweighted": unweighted,
            "rows": kept,
        }
    if not etfs:
        raise ValueError("etfs.json 에 유효한 ETF 가 없습니다")
    return {
        "available": True,
        "as_of": _kst_date(payload.get("generatedAt")),
        "generated_at": payload.get("generatedAt"),
        "etfs": etfs,
    }


async def _refresh_etfs(previous: dict | None) -> dict:
    headers = dict(_HEADERS)
    etag = (previous or {}).get("etag")
    if etag:
        headers["If-None-Match"] = etag
    client = await get_http_client("external_tools_etf")
    response = await client.get(siblings.data_url("eiayn", "etfs"), headers=headers, timeout=_ETF_TIMEOUT)
    if response.status_code == 304 and previous:
        return previous
    response.raise_for_status()
    value = await asyncio.to_thread(compact_etf_snapshot, response.content)
    value["etag"] = response.headers.get("etag")
    return value


async def load_etf_holdings() -> dict:
    """{available, as_of, etfs: {코드: {name, asset_class, currency, unweighted, rows}}, stale?}."""
    return await _load_persisted(_ETF_SOURCE, _refresh_etfs)


def normalize_holdco_payload(payload: Any) -> dict:
    """holding_value ``api/holdings.json`` → {available, as_of, holdcos: {지주사 코드: …}}(순수 함수)."""
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        raise ValueError("holdings.json 에 items 배열이 없습니다")
    holdcos: dict[str, dict] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("holdingCode") or "").strip().upper()
        if not code:
            continue
        holdcos[code] = {
            "name": item.get("holdingName") or code,
            "adjusted_shares": item.get("holdingAdjustedShares"),
            "total_shares": item.get("holdingTotalShares"),
            "subsidiaries": [
                {"code": str(sub.get("code") or "").strip().upper(), "name": sub.get("name"),
                 "shares_held": sub.get("sharesHeld")}
                for sub in item.get("subsidiaries") or []
                if isinstance(sub, dict)
            ],
        }
    if not holdcos:
        raise ValueError("holdings.json 에 유효한 지주사가 없습니다")
    return {"available": True, "as_of": _kst_date(payload.get("generatedAt")), "holdcos": holdcos}


async def _refresh_holdcos(_previous: dict | None) -> dict:
    client = await get_http_client("external_tools")
    response = await client.get(
        siblings.data_url("holding_value", "holdings"), headers=_HEADERS, timeout=_HOLDCO_TIMEOUT,
    )
    response.raise_for_status()
    return normalize_holdco_payload(response.json())


async def load_holdcos() -> dict:
    """{available, as_of, holdcos: {코드: {name, adjusted_shares, subsidiaries[]}}, stale?}."""
    return await _load_persisted(_HOLDCO_SOURCE, _refresh_holdcos)


async def load_preferred_pairs() -> dict:
    """{available, as_of, pairs: {우선주 코드: 보통주 코드}}.

    external_tools 의 summary 우선·레거시 폴백 경로를 그대로 쓴다.
    """
    cached = _pair_cache.get("pairs", copy=False)
    if cached is not None:
        return cached
    try:
        current, config = await external_tools.load_spread_pairs()
    except FETCH_ERRORS as exc:
        logger.warning("X-Ray preferred pairs failed: %r", exc)
        fallback = _pair_cache.get("pairs", allow_stale=True, copy=False)
        if fallback is not None:
            return {**fallback, "stale": True}
        value = _unavailable(repr(exc))
        _pair_cache.set("pairs", value, ttl_seconds=_RETRY_SECONDS, copy=False)
        return value
    pairs: dict[str, str] = {}
    for row in config or []:
        if not isinstance(row, dict):
            continue
        # 레거시 config 는 '005935.KS' 표기다.
        preferred = str(row.get("preferredTicker") or "").split(".")[0].strip().upper()
        common = str(row.get("commonTicker") or "").split(".")[0].strip().upper()
        if preferred and common:
            pairs[preferred] = common
    if not pairs:
        value = _unavailable("우선주 쌍이 비어 있습니다")
        _pair_cache.set("pairs", value, ttl_seconds=_RETRY_SECONDS, copy=False)
        return value
    value = {"available": True, "as_of": _kst_date((current or {}).get("lastUpdated")), "pairs": pairs}
    _pair_cache.set("pairs", value, copy=False)
    return value


async def _refresh_companies(_previous: dict | None) -> dict:
    companies = await kind.fetch_listed_companies()
    return {"available": True, "as_of": now_iso()[:10], "companies": companies}


async def load_listed_companies() -> dict:
    """{available, as_of, companies: {코드: {name, market, sector}}, stale?}."""
    return await _load_persisted(_COMPANY_SOURCE, _refresh_companies)


async def load_instrument_types(symbols: list[str]) -> dict[str, str]:
    """{Yahoo 심볼: instrumentType}. 모르는 심볼만 조회해 오래 보관한다.

    조회 실패한 심볼은 결과에서 빠지고(개별 종목 취급) 다음 계산 때 다시 시도한다.
    """
    if not symbols:
        return {}
    known = _instrument_cache.get(_INSTRUMENT_KEY, copy=False)
    if known is None:
        entry = await cache_values.get_cache_value_entry(_NAMESPACE, _INSTRUMENT_KEY)
        known = entry.value if entry else {}
        _instrument_cache.set(_INSTRUMENT_KEY, known, ttl_seconds=_INSTRUMENT_TTL_SECONDS, copy=False)
    missing = [symbol for symbol in symbols if symbol not in known]
    if missing:
        semaphore = asyncio.Semaphore(_INSTRUMENT_CONCURRENCY)

        async def lookup(symbol: str) -> tuple[str, str | None]:
            async with semaphore:
                try:
                    series = await asyncio.wait_for(
                        yahoo.fetch_close_series(symbol, range_="5d", session_date=False),
                        timeout=_INSTRUMENT_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    return symbol, None
            return symbol, (series.get("meta") or {}).get("instrumentType")

        found = {symbol: kind_ for symbol, kind_ in await asyncio.gather(*map(lookup, missing)) if kind_}
        if found:
            known = {**known, **found}
            await cache_values.set_cache_value(
                _NAMESPACE, _INSTRUMENT_KEY, known, ttl_seconds=_INSTRUMENT_TTL_SECONDS,
            )
            _instrument_cache.set(_INSTRUMENT_KEY, known, ttl_seconds=_INSTRUMENT_TTL_SECONDS, copy=False)
    return {symbol: known[symbol] for symbol in symbols if symbol in known}


def reset() -> None:
    """테스트용 — 메모리 캐시를 비운다."""
    for source in (_ETF_SOURCE, _HOLDCO_SOURCE, _COMPANY_SOURCE):
        source.cache.clear()
    _pair_cache.clear()
    _instrument_cache.clear()
