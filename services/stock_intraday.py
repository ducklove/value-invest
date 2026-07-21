"""종목 당일 일중(1일) 시세 — hover 툴팁용 경량 조회.

- 국내 주식: Naver fchart 분봉. count 파라미터와 무관하게 최근 수 세션 버퍼를
  통째로 주므로 최신 세션 날짜의 분만 추려 쓴다. 전일종가는 같은 API 의
  일봉(timeframe=day)에서 얻는다. 인증 불필요.
- 해외 주식/ETF: Yahoo v8 chart ``range=1d&interval=5m`` — meta 의
  chartPreviousClose·currentTradingPeriod·gmtoffset 으로 전일종가·정규장
  시간대를 거래소 현지 시각(HH:MM)으로 맞춘다. ticker 해석은 asset-insight 와
  동일 규칙(static ticker → ticker_map → direct ticker).
- 특수자산(현금/KRX 금/암호화폐): 미지원(``supported: False``) — gold_gap
  프로젝트가 일중 API 를 제공하면 그때 연결한다.

응답: {code, currency, source, supported, date, prevClose,
       session: {start, end} | None, points: [{t: "HH:MM", p: 가격}, ...]}
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from urllib.parse import quote

from cache_layer import MemoryTTLCache
from core.http import get_http_client
from services.portfolio import foreign
from services.portfolio import history as portfolio_history
from services.portfolio.identifiers import (
    is_korean_stock,
    is_special_asset,
    static_foreign_ticker,
)

logger = logging.getLogger(__name__)

INTRADAY_CACHE_TTL_SECONDS = 120
_intraday_cache = MemoryTTLCache("stock.intraday", INTRADAY_CACHE_TTL_SECONDS)

NAVER_FCHART_URL = "https://fchart.stock.naver.com/sise.nhn"
KR_SESSION = {"start": "09:00", "end": "15:30"}

_FCHART_ITEM_RE = re.compile(r'<item data="([^"]+)"')


def _payload(code: str, *, points: list[dict], currency: str | None, source: str | None,
             date: str | None = None, prev_close: float | None = None,
             session: dict | None = None, supported: bool = True) -> dict:
    return {
        "code": code,
        "currency": currency,
        "source": source,
        "supported": supported,
        "date": date,
        "prevClose": prev_close,
        "session": session,
        "points": points,
    }


def _safe_price(value: str) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_fchart_rows(text: str) -> list[list[str]]:
    return [m.group(1).split("|") for m in _FCHART_ITEM_RE.finditer(text or "")]


def extract_latest_session_points(minute_rows: list[list[str]]) -> tuple[str | None, list[dict]]:
    """fchart 분봉 rows(YYYYMMDDHHMM|시|고|저|종|누적량)에서 최신 세션만 추린다."""
    by_time: list[tuple[str, float]] = []
    for fields in minute_rows:
        if len(fields) < 5:
            continue
        stamp = fields[0].strip()
        close = _safe_price(fields[4])
        if len(stamp) != 12 or not stamp.isdigit() or close is None:
            continue
        by_time.append((stamp, close))
    if not by_time:
        return None, []
    by_time.sort(key=lambda pair: pair[0])
    latest_date = by_time[-1][0][:8]
    points = [
        {"t": f"{stamp[8:10]}:{stamp[10:12]}", "p": close}
        for stamp, close in by_time
        if stamp[:8] == latest_date
    ]
    iso_date = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}"
    return iso_date, points


def extract_prev_close(day_rows: list[list[str]], session_date_iso: str | None) -> float | None:
    """fchart 일봉 rows에서 세션 날짜 이전 마지막 종가(전일종가)를 얻는다."""
    session_key = (session_date_iso or "").replace("-", "")
    closes: list[tuple[str, float]] = []
    for fields in day_rows:
        if len(fields) < 5:
            continue
        stamp = fields[0].strip()
        close = _safe_price(fields[4])
        if len(stamp) != 8 or not stamp.isdigit() or close is None:
            continue
        closes.append((stamp, close))
    closes.sort(key=lambda pair: pair[0])
    prior = [close for stamp, close in closes if not session_key or stamp < session_key]
    return prior[-1] if prior else None


async def _fetch_fchart(symbol: str, timeframe: str, count: int) -> str:
    client = await get_http_client("naver")
    resp = await client.get(
        NAVER_FCHART_URL,
        params={"symbol": symbol, "timeframe": timeframe, "count": str(count), "requestType": "0"},
    )
    resp.raise_for_status()
    # EUC-KR 선언이지만 종목명 외 필드는 ASCII — 깨진 문자는 무시해도 무방.
    return resp.content.decode("euc-kr", errors="ignore")


async def _korean_intraday(code: str) -> dict:
    minute_text, day_text = await asyncio.gather(
        _fetch_fchart(code, "minute", 480),
        _fetch_fchart(code, "day", 5),
    )
    date_iso, points = extract_latest_session_points(parse_fchart_rows(minute_text))
    prev_close = extract_prev_close(parse_fchart_rows(day_text), date_iso)
    return _payload(
        code, points=points, currency="KRW", source="naver",
        date=date_iso, prev_close=prev_close, session=dict(KR_SESSION),
    )


def _resolve_yahoo_ticker(code: str) -> str:
    static = static_foreign_ticker(code)
    if static:
        return static["ticker"]
    return foreign._ticker_map.get(code) or foreign.yfinance_direct_ticker(code)


async def _yahoo_intraday(ticker: str) -> dict | None:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker, safe='')}"
    async with portfolio_history.YAHOO_SEM:
        client = await get_http_client("yahoo")
        resp = await client.get(
            url,
            params={"range": "1d", "interval": "5m", "includePrePost": "false"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=portfolio_history.YAHOO_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
    result = (((resp.json() or {}).get("chart") or {}).get("result") or [None])[0]
    if not result:
        return None
    meta = result.get("meta") or {}
    gmtoff = int(meta.get("gmtoffset") or 0)

    def _local(ts: int) -> datetime:
        return datetime.fromtimestamp(int(ts) + gmtoff, tz=timezone.utc)

    timestamps = result.get("timestamp") or []
    closes = ((((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}).get("close") or []
    points = []
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        points.append({"t": f"{_local(ts):%H:%M}", "p": round(float(close), 6)})

    session = None
    regular = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
    if regular.get("start") and regular.get("end"):
        session = {
            "start": f"{_local(regular['start']):%H:%M}",
            "end": f"{_local(regular['end']):%H:%M}",
        }
    prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
    date_iso = f"{_local(timestamps[-1]):%Y-%m-%d}" if timestamps else None
    return {
        "points": points,
        "session": session,
        "prev_close": float(prev_close) if prev_close is not None else None,
        "date": date_iso,
        "currency": (meta.get("currency") or "").upper() or None,
    }


async def _foreign_intraday(code: str) -> dict:
    await foreign.ensure_ticker_map()
    ticker = _resolve_yahoo_ticker(code)
    if not ticker:
        return _payload(code, points=[], currency=None, source=None, supported=False)
    chart = await _yahoo_intraday(ticker)
    if not chart:
        return _payload(code, points=[], currency=None, source="yahoo")
    return _payload(
        code, points=chart["points"], currency=chart["currency"], source="yahoo",
        date=chart["date"], prev_close=chart["prev_close"], session=chart["session"],
    )


async def get_intraday(code: str) -> dict:
    code = (code or "").strip().upper()
    if not code:
        return _payload(code, points=[], currency=None, source=None, supported=False)
    if is_special_asset(code):
        # gold_gap 일중 API 연결 전까지 미지원 — 프론트는 툴팁을 띄우지 않는다.
        return _payload(code, points=[], currency=None, source=None, supported=False)

    cached = _intraday_cache.get(code)
    if cached is not None:
        return cached

    try:
        if is_korean_stock(code):
            result = await _korean_intraday(code)
        else:
            result = await _foreign_intraday(code)
    except Exception as exc:
        logger.warning("일중 시세 조회 실패(%s): %s", code, exc)
        return _payload(code, points=[], currency=None, source=None)

    if result["points"] or not result["supported"]:
        _intraday_cache.set(code, result)
    return result
