"""NH 해외 체결(RC)의 GIC 식별·현지 거래시각·원화 환산."""

import asyncio
import logging
import math
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

from cache_layer import MemoryTTLCache
from core.http import get_http_client
from services.portfolio import currencies, fx
from services.portfolio.identifiers import static_foreign_ticker

logger = logging.getLogger(__name__)
_master = MemoryTTLCache("namuh.overseas_master", 86400)
_instruments: dict[str, dict] = {}
_lock = asyncio.Lock()
_KST = ZoneInfo("Asia/Seoul")
_MARKETS = {
    "NQQ": ("", "America/New_York"), "NYY": ("", "America/New_York"),
    "ASQ": ("", "America/New_York"), "BTQ": ("", "America/New_York"),
    "HKG": (".HK", "Asia/Hong_Kong"), "TYO": (".T", "Asia/Tokyo"),
    "SHC": (".SS", "Asia/Shanghai"), "SHZ": (".SZ", "Asia/Shanghai"),
    "ASX": (".AX", "Australia/Sydney"), "GER": (".DE", "Europe/Berlin"),
    "LSE": (".L", "Europe/London"),
}


def parse_master(data: bytes) -> dict[str, dict]:
    # 공식 m_gtsstock.h: CP949, 164바이트 고정 길이, 마지막 1바이트 LF.
    if not data or len(data) % 164:
        raise ValueError("NH 해외 종목마스터 레코드 길이 오류")
    result = {}
    ambiguous = set()
    for start in range(0, len(data), 164):
        row = data[start:start + 164]
        if row[-1:] != b"\n":
            raise ValueError("NH 해외 종목마스터 종단 오류")
        gic, symbol, market, currency = (row[a:b].decode("cp949").rstrip() for a, b in
                                       ((0, 15), (98, 110), (110, 113), (138, 141)))
        if market not in _MARKETS or (currency != "GBp" and currency not in currencies.CURRENCY_TO_FX_CODE):
            continue
        suffix, zone = _MARKETS[market]
        if suffix in {".HK", ".T"} and symbol.isdigit():
            symbol = (symbol.lstrip("0") or "0").zfill(4)
        code = symbol.replace(".", "-") if not suffix else symbol + suffix
        if not re.fullmatch(r"[A-Z0-9.-]{1,24}", code) or not re.fullmatch(r"[A-Za-z0-9.\-/]{1,15}", gic):
            continue
        info = {"gic": gic, "market": market, "currency": currency, "zone": zone}
        if code in result and result[code] != info:
            ambiguous.add(code)
        result[code] = info
    for code in ambiguous:
        result.pop(code, None)
    if not result:
        raise ValueError("NH 해외 종목마스터에 지원 종목 없음")
    return result


async def ensure_master() -> None:
    global _instruments
    if _master.get("ready") or _master.get("retry"):
        return
    async with _lock:
        if _master.get("ready") or _master.get("retry"):
            return
        try:
            client = await get_http_client("namuh")
            response = await client.get("https://www.nhplug.com/instruments/m_gtsstock.mst")
            response.raise_for_status()
            _instruments = parse_master(response.content)
            _master.set("ready", True)
        except (httpx.HTTPError, ValueError, UnicodeError) as exc:
            _master.set("retry", True, ttl_seconds=300)
            logger.warning("NH 해외 종목마스터 갱신 실패: %s", type(exc).__name__)


def instrument(code: str) -> dict | None:
    if not _master.get("ready"):
        return None
    alias = static_foreign_ticker(code)
    return _instruments.get(alias["ticker"] if alias else code)


def normalize(message: dict, code: str, info: dict, now: datetime) -> dict | None:
    head, body = message.get("header"), message.get("body")
    # 소문자 rc는 지연 채널이다. 실시간 체결로 승격하지 않는다.
    if not isinstance(head, dict) or not isinstance(body, dict) or head.get("tr_cd") != "RC":
        return None
    if "rsp_cd" in head or "tr_type" in head or head.get("tr_key") not in (None, info["gic"]):
        return None
    if str(body.get("gicz15") or "").strip() != info["gic"] or str(body.get("exch_idz3") or "").strip() != info["market"]:
        return None
    try:
        if any(isinstance(body.get(key), bool) for key in ("trdprc_1z17", "netchng_1z17")):
            return None
        raw_time = str(body["trade_timez6"]).strip()
        if not re.fullmatch(r"\d{6}", raw_time):
            return None
        hour, minute, second = int(raw_time[:2]), int(raw_time[2:4]), int(raw_time[4:])
        if hour >= 48 or minute >= 60 or second >= 60:
            return None
        local = datetime.strptime(str(body["trade_datez8"]), "%Y%m%d") + timedelta(hours=hour, minutes=minute, seconds=second)
        at = local.replace(tzinfo=ZoneInfo(info["zone"])).astimezone(_KST)
        price = float(body["trdprc_1z17"])
        sign = {"1": 1, "2": 1, "3": 0, "4": -1, "5": -1}[str(body["netchng_clsz1"])]
        raw_change = float(body["netchng_1z17"])
        if not math.isfinite(raw_change) or (not sign and raw_change):
            return None
        change = abs(raw_change) * sign
        previous = price - change
        if not 0 < price < 1e12 or not math.isfinite(previous) or previous <= 0 or not 0 <= (now - at).total_seconds() < 90:
            return None
        return {"type": "quote", "code": code, "price": price, "previous_close": previous, "change": change,
                "change_pct": change / previous * 100, "currency": info["currency"], "market": info["market"],
                "source": "namuh_ws", "date": at.date().isoformat(), "as_of": at.isoformat(),
                "market_date": str(body["trade_datez8"]), "ts": now.timestamp(), "received_at": now.isoformat()}
    except (KeyError, ValueError, TypeError, OverflowError):
        return None


async def to_won(tick: dict) -> dict | None:
    currency = tick["currency"]
    try:
        rate = await fx.fx_rate_for_currency("GBP" if currency == "GBp" else currency)
    except fx.FXUnavailableError:
        return None
    if currency == "GBp":
        rate /= 100
    if not math.isfinite(rate) or rate <= 0:
        return None
    return {**tick, "original_price": tick["price"], "original_currency": currency, "fx_rate": rate,
            "currency": "KRW", **{key: round(tick[key] * rate, 6) for key in ("price", "previous_close", "change")}}
