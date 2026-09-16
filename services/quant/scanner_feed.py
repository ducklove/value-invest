"""나무 공개 계약 마스터와 조회·실시간 호가 변환."""

import asyncio
import time
from datetime import datetime

import httpx

from core.http import get_http_client
from repositories.broker_secrets import BrokerError
from repositories.quant import QuantError
from services.brokers import namuh
from services.quant.scanner_model import KST, book, master_rows, number

_master = []
_master_at = 0.0
_master_lock = asyncio.Lock()


async def catalog():
    global _master, _master_at
    async with _master_lock:
        if _master and time.time() - _master_at < 6 * 3600:
            return _master
        try:
            client = await get_http_client("namuh")
            r = await client.get("https://www.nhplug.com/instruments/m_stkfut.mst")
            r.raise_for_status()
            parsed = master_rows(r.content)
        except (httpx.HTTPError, UnicodeError, ValueError) as exc:
            raise QuantError("선물 계약 목록을 확인하지 못했습니다.") from exc
        _master, _master_at = parsed, time.time()
        return _master


async def snapshot(user, cid, contract, env, spot_cache):
    pages = await namuh.pages(user, cid, "/krfuture/quote/v1/day", {"iem_cd": contract["contract"]}, env)
    f = pages[0].get("Output_0")
    if not isinstance(f, dict) or str(f.get("iem_cd", "")).removeprefix("K") != contract["contract"][1:]:
        raise BrokerError("선물 코드 또는 응답 블록 불일치")
    now = datetime.now(KST)
    future = book(f["bidp1"], f["askp1"], f["bidp_rsqn1"], f["askp_rsqn1"], f["bsop_hour"], now)
    key = contract["spot_code"]
    cached = spot_cache.get(key)
    if cached and now.timestamp() - cached["received_at"] <= 30:
        spot = cached
    else:
        pages = await namuh.pages(user, cid, "/krstock/quote/v1/currentPrice", {"iem_cd": key, "market_cd": "KRX"}, env)
        s = pages[0].get("Output_0")
        if not isinstance(s, dict) or s.get("iem_cd") != key:
            raise BrokerError("현물 코드 또는 응답 블록 불일치")
        spot = book(s["bidp1"], s["askp1"], s["bidp_rsqn1"], s["askp_rsqn1"], s["hoga_bsop_hour"], datetime.now(KST))
        spot_cache[key] = spot
    if str(f.get("cncc_cls_code", "")) != "0" or f.get("dynmc_prc_lmt_yn") not in {"Y", "N"}:
        raise ValueError("동시호가 또는 상태 미확인")
    # Y는 동적 가격제한 적용이다. 매매 정지를 뜻하지 않는다. 제한 경계 호가는 제외한다.
    if f["dynmc_prc_lmt_yn"] == "Y":
        lower, upper = number(f.get("dynmc_lwlmtprc")), number(f.get("dynmc_uplmtprc"))
        if not 0 < lower < future["bid"] <= future["ask"] < upper:
            raise ValueError("동적 가격제한 경계 또는 범위 미확인")
    return spot, future, str(f["last_tr_date"])
