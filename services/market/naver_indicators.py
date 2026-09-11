"""네이버 증권 JSON 시세. 구형 PC HTML 리다이렉트에 영향받지 않는다."""

import asyncio
import logging
import math
from datetime import datetime, timezone
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

DOMESTIC = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ", "KOSPI200": "KPI200"}
FOREIGN = {"SPX": ".INX", "IXIC": ".IXIC", "DJI": ".DJI", "NI225": ".N225", "HSI": ".HSI", "SHC": ".SSEC"}
FX = {f"{currency}_KRW": f"FX_{currency}KRW" for currency in ("USD", "EUR", "JPY", "CNY", "AUD", "VND")}
FX["USD_IDX"] = ".DXY"
CODES = frozenset(DOMESTIC) | frozenset(FOREIGN) | frozenset(FX)
YAHOO = {"KOSPI": "^KS11", "KOSDAQ": "^KQ11", "KOSPI200": "^KS200", "SPX": "^GSPC",
         "IXIC": "^IXIC", "DJI": "^DJI", "NI225": "^N225", "HSI": "^HSI", "SHC": "000001.SS"}


def _number(value: object) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError("시세 숫자 누락")
    result = float(str(value).replace(",", "").strip())
    if not math.isfinite(result):
        raise ValueError("유효하지 않은 시세 숫자")
    return result


def _quote(row: dict) -> dict:
    value = _number(row.get("closePrice"))
    if value <= 0:
        raise ValueError("유효하지 않은 지수 값")
    change_raw = row.get("compareToPreviousClosePrice", row.get("fluctuations"))
    ratio_raw = row.get("fluctuationsRatio")
    change = _number(change_raw) if change_raw not in (None, "", "-") else None
    ratio = _number(ratio_raw) if ratio_raw not in (None, "", "-") else None
    movement = change if change is not None else ratio
    return {
        "value": f"{value:,.2f}",
        "change": f"{abs(change):,.2f}" if change is not None else "",
        "change_pct": f"{abs(ratio):.2f}%" if ratio is not None else "",
        "direction": "up" if movement is not None and movement > 0 else "down" if movement is not None and movement < 0 else "",
        "as_of": row.get("localTradedAt") or "",
        "market_status": row.get("marketStatus") or "",
        "delay_minutes": row.get("delayTime", (row.get("stockExchangeType") or {}).get("delayTime", 0)),
        "source": "naver_json",
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


async def fetch_indicators(client: httpx.AsyncClient, codes: list[str]) -> dict[str, dict]:
    """국내 지수는 한 번에 조회하고 해외 지수·환율은 항목별 실패를 격리한다."""
    results = {}
    semaphore = asyncio.Semaphore(4)

    async def fetch(url: str, wanted: dict[str, str], kind: str):
        try:
            async with semaphore:
                response = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                response.raise_for_status()
                payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("시세 응답 형식 변경")
            rows = payload.get("datas") if kind == "domestic" else [payload.get("exchangeInfo", payload)] if kind == "fx" else [payload]
            if not isinstance(rows, list):
                raise ValueError("시세 목록 누락")
            for row in rows:
                if not isinstance(row, dict):
                    continue
                code = wanted.get(row.get("itemCode") if kind == "domestic" else row.get("reutersCode"))
                if not code:
                    continue
                try:
                    results[code] = _quote(row)
                except (TypeError, ValueError):
                    logger.warning("네이버 지표 값 누락: %s", code)
        except (httpx.HTTPError, TypeError, ValueError):
            logger.warning("네이버 지표 조회 실패: %s", ",".join(wanted.values()))

    tasks = []
    domestic = {DOMESTIC[c]: c for c in codes if c in DOMESTIC}
    if domestic:
        tasks.append(fetch("https://polling.finance.naver.com/api/realtime/domestic/index/" + ",".join(domestic), domestic, "domestic"))
    for code in dict.fromkeys(codes):
        if code in FOREIGN:
            symbol = FOREIGN[code]
            tasks.append(fetch(f"https://api.stock.naver.com/index/{symbol}/basic", {symbol: code}, "foreign"))
        elif code in FX:
            symbol = FX[code]
            tasks.append(fetch(f"https://api.stock.naver.com/marketindex/exchange/{symbol}", {symbol: code}, "fx"))
    await asyncio.gather(*tasks)

    async def fallback(code: str):
        try:
            async with semaphore:
                response = await client.get(
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(YAHOO[code], safe='')}?interval=1m&range=1d",
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                response.raise_for_status()
                meta = response.json()["chart"]["result"][0]["meta"]
            value = _number(meta.get("regularMarketPrice"))
            previous = _number(meta.get("previousClose"))
            # chartPreviousClose는 조회 구간 시작 기준일 수 있어 전일 종가로 쓰지 않는다.
            if previous <= 0:
                raise ValueError("전일 종가 누락")
            stamp = datetime.fromtimestamp(_number(meta.get("regularMarketTime")), timezone.utc).isoformat()
            result = _quote({"closePrice": value, "compareToPreviousClosePrice": value - previous,
                             "fluctuationsRatio": (value / previous - 1) * 100, "localTradedAt": stamp})
            result.update(source="yahoo_fallback", _degraded=True)
            results[code] = result
        except (httpx.HTTPError, TypeError, ValueError, KeyError, IndexError, OverflowError, OSError):
            logger.warning("대체 지수 조회 실패: %s", code)

    await asyncio.gather(*(fallback(code) for code in dict.fromkeys(codes) if code in YAHOO and code not in results))
    return results
