"""공개 NH 주식선물 종목마스터의 기초자산 코드."""

import asyncio
import logging
import re

import httpx

from cache_layer import MemoryTTLCache
from core.http import get_http_client

logger = logging.getLogger(__name__)
MASTER_URL = "https://www.nhplug.com/instruments/m_stkfut.mst"
_cache = MemoryTTLCache("namuh.futures_underlyings", 3600)
_lock = asyncio.Lock()


def parse_master(data: bytes) -> dict[str, str]:
    # 공식 m_stkfut.h: CP949, 97바이트. 선물 코드 0:9, 기초자산 69:75,
    # 시장 95:96 (1=코스피, 2=코스닥, 3=ETF), 마지막 LF.
    if not data or len(data) % 97:
        raise ValueError("NH 주식선물 종목마스터 길이 오류")
    codes = {}
    for start in range(0, len(data), 97):
        row = data[start:start + 97]
        code = row[:9].decode("ascii")
        underlying = row[69:75].decode("ascii")
        if (row[-1:] != b"\n" or not re.fullmatch(r"K[0-9A-Z]{8}", code)
                or not re.fullmatch(r"[0-9][0-9A-Z]{5}", underlying)
                or row[95:96] not in {b"1", b"2", b"3"} or code in codes):
            raise ValueError("NH 주식선물 종목마스터 형식 오류")
        codes[code] = underlying
    return codes


async def underlying_codes() -> dict[str, str]:
    cached = _cache.get("codes")
    if cached is not None:
        return cached
    async with _lock:
        cached = _cache.get("codes")
        if cached is not None:
            return cached
        if _cache.get("retry_later"):
            return _cache.get("codes", allow_stale=True) or {}
        try:
            client = await get_http_client("namuh")
            response = await client.get(MASTER_URL, timeout=5)
            response.raise_for_status()
            codes = parse_master(response.content)
            if len(codes) < 1000:
                raise ValueError("NH 주식선물 종목마스터 건수 부족")
        except (httpx.HTTPError, ValueError):
            logger.warning("NH 주식선물 기초자산 목록 조회 실패")
            _cache.set("retry_later", True, ttl_seconds=60)
            return _cache.get("codes", allow_stale=True) or {}
        _cache.set("codes", codes)
        return codes
