"""NH 잔고의 국내주식 상장 여부를 공개 종목마스터로 재확인한다."""

import asyncio
import io
import re
import zipfile

import httpx

from cache_layer import MemoryTTLCache
from core.http import get_http_client
from repositories.broker_secrets import BrokerError

_cache = MemoryTTLCache("namuh.account_listing", 3600)
_lock = asyncio.Lock()
NH_MASTER_URL = "https://www.nhplug.com/instruments/m_new_stock.mst"
KONEX_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/konex_code.mst.zip"


def parse_nh_master(data: bytes) -> set[str]:
    # 공식 m_new_stock.h: CP949, 237바이트, 코드 0:6, 시장 6:7, 마지막 LF.
    # 코스피(ETF 포함)·코스닥·ETN만 수록한다. 거래정지·관리종목은 상장 상태다.
    if not data or len(data) % 237:
        raise ValueError("NH 국내 종목마스터 길이 오류")
    codes = set()
    markets = set()
    for start in range(0, len(data), 237):
        row = data[start:start + 237]
        code, market = row[:6].decode("ascii"), row[6:7].decode("ascii")
        if row[-1:] != b"\n" or not re.fullmatch(r"[0-9][0-9A-Z]{5}", code) or market not in {"1", "4", "A"} or code in codes:
            raise ValueError("NH 국내 종목마스터 형식 오류")
        codes.add(code)
        markets.add(market)
    if markets != {"1", "4", "A"}:
        raise ValueError("NH 국내 종목마스터 시장 누락")
    return codes


def parse_konex_master(data: bytes) -> set[str]:
    # NH 국내 마스터에 없는 코넥스는 KIS 공개 마스터로 보완한다. 앱키는 불필요하다.
    # 공식 stocks_info/kis_konex_code_mst.py: 단축코드 0:9, 표준코드 9:21.
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        raw = archive.read("konex_code.mst")
    if not raw or not raw.endswith(b"\n"):
        raise ValueError("코넥스 종목마스터 누락")
    codes = set()
    for row in raw.splitlines():
        code = row[:9].decode("ascii").strip()
        isin = row[9:21].decode("ascii")
        if len(row) != 245 or not re.fullmatch(r"[0-9][0-9A-Z]{5}", code) or not re.fullmatch(r"KR7" + code + r"[0-9]{3}", isin) or code in codes:
            raise ValueError("코넥스 종목마스터 형식 오류")
        codes.add(code)
    return codes


async def listed_codes() -> set[str]:
    cached = _cache.get("codes")
    if cached is not None:
        return cached
    async with _lock:
        cached = _cache.get("codes")
        if cached is not None:
            return cached
        try:
            client = await get_http_client("namuh")
            response = await client.get(NH_MASTER_URL)
            response.raise_for_status()
            codes = parse_nh_master(response.content)
            response = await client.get(KONEX_MASTER_URL)
            response.raise_for_status()
            konex = parse_konex_master(response.content)
            # 행 경계에 맞춰 잘린 파일도 전체 목록으로 취급하지 않는다.
            if len(codes) < 1000 or len(konex) < 10:
                raise ValueError("상장 종목마스터 건수 부족")
        except (httpx.HTTPError, ValueError, KeyError, zipfile.BadZipFile) as exc:
            raise BrokerError("상장 종목 목록을 확인하지 못해 기존 잔고를 유지합니다. 잠시 후 다시 시도해 주세요.") from exc
        codes.update(konex)
        _cache.set("codes", codes)
        return codes
