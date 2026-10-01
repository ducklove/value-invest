"""Yahoo Finance v8 chart provider — services 계층의 ``/v8/finance/chart`` 호출 지점.

이전에는 foreign·history·stock_intraday·dividend_sources·naver_indicators 가
각자 URL 을 만들고 JSON 을 파싱했으며, 세마포어도 제각각(4+4+지표 4+…)이라
Yahoo 는 한 순간 8~14 개의 동시 요청을 받았다. 이 모듈이 그 경로를 하나로
모은다. ``services.market.indicators``(구 루트 market_indicators.py)의 ^TNX·GC=F·CL=F 지표도 이
provider 를 거친다.

* **fetch** — ``fetch_chart_json()`` 이 URL·쿼리·헤더를 만들고, 호스트 단위
  동시성 한도(``host_limit()``)와 429 처리를 거쳐 원본 JSON(dict)을 돌려준다.
  비 2xx 는 ``httpx.HTTPStatusError``, 잘못된 JSON 은 ``ValueError``.
* **parse** — ``parse_chart()`` 가 ``chart.result[0]`` 을 ``ChartResult``
  (meta, timestamp, OHLC/adjclose/volume, 배당 이벤트)로 만든다. 기존 파서들의
  상위집합이다. ``ChartResult.close_rows()`` 는 기존 foreign 파서와 같은
  ``{date, session_date, close}`` 행을 만든다(``session_date`` 는 거래소 현지
  날짜 — regular_close 가 해외 정산일 판단에 쓴다).
* **편의 함수** — ``fetch_close_series()`` 는 기존
  ``foreign.fetch_yahoo_chart`` 와 같은 ``{rows, currency, meta}`` 를 돌려주며
  실패해도 예외 대신 빈 결과를 준다.

429 정책: ``Retry-After``(없으면 지수 백오프) 가 짧으면 한 번 재시도하고,
그래도 429 이거나 대기가 길면 호스트 전체를 ``COOLDOWN_SECONDS`` 동안 쉬게
한다. 쿨다운 중 호출은 네트워크에 닿지 않고 ``YahooRateLimitedError`` 를
즉시 낸다. 이 예외는 ``httpx.HTTPError`` 이기도 해서, 기존에 HTTP 오류를
잡던 호출부가 그대로 "조회 실패" 로 처리한다.

``previous_close()`` 의 우선순위 — ``chartPreviousClose`` 는 "조회 구간 시작
직전 종가" 라 ``range=1d`` 일 때만 전일 종가와 같다. 다일 구간에서는
``previousClose`` 만 전일 종가로 본다.

상장 여부 — ``probe_listing()`` 은 심볼이 있음/없음/모름(present/absent/unknown)을
가른다. 없음은 404·``chart.error`` "Not Found"·통화·가격·봉이 전혀 없는 빈 껍데기
응답뿐이고, 시간 초과·5xx·429·쿨다운은 모름이다. ``fetch_chart_json()`` 은 모든
chart 조회에서 없음 증거를 ``MISSING_SYMBOL_TTL_SECONDS`` 동안 남겨
(``symbol_missing()``), 호출부가 방금 실패한 조회의 원인을 추가 호출 없이 안다.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx

from cache_layer import MemoryTTLCache
from core.errors import RateLimitError
from core.http import get_http_client

logger = logging.getLogger(__name__)

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
# 기존 foreign/history/intraday 가 쓰던 per-request timeout 과 동일.
DEFAULT_TIMEOUT = httpx.Timeout(6.0, connect=3.0)

# query1.finance.yahoo.com 에 동시에 나가는 요청 상한 (chart + search 공용).
HOST_CONCURRENCY = 6

RATE_LIMIT_RETRIES = 1
RETRY_BASE_DELAY = 1.0
MAX_INLINE_RETRY_DELAY = 3.0
COOLDOWN_SECONDS = 30.0
MAX_COOLDOWN_SECONDS = 300.0

LISTING_PRESENT = "present"
LISTING_ABSENT = "absent"
LISTING_UNKNOWN = "unknown"
# chart 조회가 "그런 심볼 없음"을 돌려준 증거를 남겨 두는 시간.
MISSING_SYMBOL_TTL_SECONDS = 900.0
_missing_symbols = MemoryTTLCache("yahoo.missing_symbol", MISSING_SYMBOL_TTL_SECONDS, evict_expired_after=0)

_cooldown_until = 0.0
# asyncio.Semaphore 는 처음 대기한 이벤트 루프에 묶인다. 운영은 루프 1개지만
# 배치 스크립트·테스트는 루프를 새로 만들므로 루프별로 하나씩 둔다.
_semaphores: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = weakref.WeakKeyDictionary()


class YahooRateLimitedError(RateLimitError, httpx.HTTPError):
    """Yahoo 가 429 를 돌려줬거나 호스트 쿨다운 중이다."""

    default_detail = "Yahoo Finance 호출 한도를 초과했습니다. 잠시 후 다시 시도해 주세요."

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        httpx.HTTPError.__init__(self, message)
        self.retry_after = retry_after


def host_limit() -> asyncio.Semaphore:
    """현재 이벤트 루프의 Yahoo 호스트 동시성 세마포어."""
    loop = asyncio.get_running_loop()
    semaphore = _semaphores.get(loop)
    if semaphore is None:
        semaphore = asyncio.Semaphore(HOST_CONCURRENCY)
        _semaphores[loop] = semaphore
    return semaphore


def cooldown_remaining() -> float:
    return max(0.0, _cooldown_until - time.monotonic())


def reset_rate_limit_state() -> None:
    """테스트·운영 도구용: 쿨다운을 해제한다."""
    global _cooldown_until
    _cooldown_until = 0.0


def symbol_missing(symbol: str, *, max_age_seconds: float | None = None) -> bool:
    """최근 ``MISSING_SYMBOL_TTL_SECONDS``(또는 ``max_age_seconds``) 안에 이 심볼의
    chart 조회가 "없음"(404·Not Found·빈 껍데기)이었고 그 뒤로 정상 응답이 없었는가."""
    key = (symbol or "").strip()
    if not _missing_symbols.get(key):
        return False
    if max_age_seconds is None:
        return True
    age = _missing_symbols.age_seconds(key)
    return age is not None and age <= max_age_seconds


def reset_missing_symbols() -> None:
    """테스트용: 없음 증거를 지운다."""
    _missing_symbols.clear()


def _start_cooldown(retry_after: float | None) -> float:
    global _cooldown_until
    seconds = min(max(retry_after or 0.0, COOLDOWN_SECONDS), MAX_COOLDOWN_SECONDS)
    _cooldown_until = max(_cooldown_until, time.monotonic() + seconds)
    return seconds


def _retry_after_seconds(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        value = float(raw.strip())
    except ValueError:
        return None  # HTTP-date 형식은 쓰지 않는다 — 기본 백오프로 처리.
    return value if math.isfinite(value) and value >= 0 else None


async def _get(
    url: str,
    *,
    params: dict[str, Any],
    client: httpx.AsyncClient | None,
    timeout: Any,
) -> httpx.Response:
    attempt = 0
    while True:
        remaining = cooldown_remaining()
        if remaining > 0:
            raise YahooRateLimitedError(
                f"Yahoo 호출 쿨다운 중 ({remaining:.0f}s 남음)", retry_after=remaining
            )
        http = client if client is not None else await get_http_client("yahoo")
        async with host_limit():
            response = await http.get(url, params=params, headers=HEADERS, timeout=timeout)
        if response.status_code != 429:
            response.raise_for_status()
            return response
        retry_after = _retry_after_seconds(response)
        delay = retry_after if retry_after is not None else RETRY_BASE_DELAY * (2 ** attempt)
        if attempt >= RATE_LIMIT_RETRIES or delay > MAX_INLINE_RETRY_DELAY:
            seconds = _start_cooldown(retry_after)
            logger.warning("Yahoo 429 — %.0fs 동안 호출 중단: %s", seconds, url)
            raise YahooRateLimitedError("Yahoo 429 Too Many Requests", retry_after=seconds)
        attempt += 1
        logger.info("Yahoo 429 — %.1fs 후 재시도 (%d/%d)", delay, attempt, RATE_LIMIT_RETRIES)
        await asyncio.sleep(delay)


def chart_url(symbol: str) -> str:
    return CHART_URL.format(symbol=quote(symbol, safe=""))


async def fetch_chart_json(
    symbol: str,
    *,
    range_: str | None = None,
    interval: str = "1d",
    period1: int | None = None,
    period2: int | None = None,
    events: str | None = None,
    include_pre_post: bool | None = None,
    client: httpx.AsyncClient | None = None,
    timeout: Any = DEFAULT_TIMEOUT,
) -> dict:
    """v8 chart 원본 JSON. ``client`` 를 주면 그 클라이언트로(테스트·지표 배치),
    아니면 공유 ``yahoo`` 클라이언트로 호출한다. ``timeout`` 에
    ``httpx.USE_CLIENT_DEFAULT`` 를 주면 클라이언트 기본값을 쓴다."""
    params: dict[str, Any] = {}
    if range_:
        params["range"] = range_
    if period1 is not None:
        params["period1"] = int(period1)
    if period2 is not None:
        params["period2"] = int(period2)
    params["interval"] = interval
    if include_pre_post is not None:
        params["includePrePost"] = "true" if include_pre_post else "false"
    if events:
        params["events"] = events
    try:
        response = await _get(chart_url(symbol), params=params, client=client, timeout=timeout)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            _missing_symbols.set(symbol, True)
        raise
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Yahoo chart 응답 형식 변경")
    if classify_listing(payload) == LISTING_ABSENT:
        _missing_symbols.set(symbol, True)
    else:
        _missing_symbols.delete(symbol)
    return payload


@dataclass(frozen=True)
class ChartResult:
    """``chart.result[0]`` 의 정규화 결과. 시계열 리스트는 timestamp 와 정렬돼
    있으며 값이 없으면 None 이다."""

    meta: dict
    timestamps: list = field(default_factory=list)
    opens: list = field(default_factory=list)
    highs: list = field(default_factory=list)
    lows: list = field(default_factory=list)
    closes: list = field(default_factory=list)
    adjcloses: list = field(default_factory=list)
    volumes: list = field(default_factory=list)
    dividends: list = field(default_factory=list)

    @property
    def currency(self) -> str | None:
        value = self.meta.get("currency")
        return str(value).upper() if value else None

    @property
    def gmtoffset(self) -> int:
        try:
            return int(self.meta.get("gmtoffset") or 0)
        except (TypeError, ValueError):
            return 0

    def close_rows(self, *, session_date: bool = True) -> list[dict]:
        """``{date(UTC), session_date(거래소 현지), close}`` 행. close 가 없는
        봉은 건너뛴다. 거래소 시간대를 해석할 수 없으면 기존 foreign 파서처럼
        (행마다 실패해) 빈 목록이 된다."""
        zone = None
        if session_date:
            try:
                zone = ZoneInfo(self.meta.get("exchangeTimezoneName") or "UTC")
            except (KeyError, ValueError, TypeError):
                return []
        rows = []
        for ts, close in zip(self.timestamps, self.closes):
            if close is None:
                continue
            try:
                stamp = int(ts)
                row = {"date": datetime.fromtimestamp(stamp, tz=timezone.utc).date().isoformat()}
                if zone is not None:
                    row["session_date"] = datetime.fromtimestamp(stamp, tz=zone).date().isoformat()
                row["close"] = round(float(close), 6)
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            rows.append(row)
        return rows


def _series(block: Any, key: str) -> list:
    values = block.get(key) if isinstance(block, dict) else None
    return list(values) if isinstance(values, list) else []


def parse_chart(payload: Any) -> ChartResult | None:
    """v8 chart JSON → ``ChartResult``. 결과가 없으면 None."""
    chart = payload.get("chart") if isinstance(payload, dict) else None
    results = chart.get("result") if isinstance(chart, dict) else None
    if not isinstance(results, list) or not results or not isinstance(results[0], dict):
        return None
    result = results[0]
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    indicators = result.get("indicators") if isinstance(result.get("indicators"), dict) else {}
    quotes = indicators.get("quote") if isinstance(indicators.get("quote"), list) else []
    quote_block = quotes[0] if quotes and isinstance(quotes[0], dict) else {}
    adj = indicators.get("adjclose") if isinstance(indicators.get("adjclose"), list) else []
    adj_block = adj[0] if adj and isinstance(adj[0], dict) else {}
    events = result.get("events") if isinstance(result.get("events"), dict) else {}
    dividends = events.get("dividends") if isinstance(events.get("dividends"), dict) else {}
    return ChartResult(
        meta=meta,
        timestamps=_series(result, "timestamp"),
        opens=_series(quote_block, "open"),
        highs=_series(quote_block, "high"),
        lows=_series(quote_block, "low"),
        closes=_series(quote_block, "close"),
        adjcloses=_series(adj_block, "adjclose"),
        volumes=_series(quote_block, "volume"),
        dividends=[row for row in dividends.values() if isinstance(row, dict)],
    )


def _positive(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def classify_listing(payload: Any) -> str:
    """2xx chart 응답 → ``LISTING_PRESENT``/``ABSENT``/``UNKNOWN``.

    없음: ``chart.error.code`` 가 "Not Found" 이거나, 결과는 있는데 통화·가격·봉·
    거래 시각이 하나도 없는 빈 껍데기다 — Yahoo 는 모르는 나스닥형 심볼(THF, GOO)에
    404 대신 이런 ECNQUOTE 결과를 준다. 있음: 통화와 가격(또는 봉)이 있다.
    그 밖의 애매한 모양은 모름이다."""
    chart = payload.get("chart") if isinstance(payload, dict) else None
    if not isinstance(chart, dict):
        return LISTING_UNKNOWN
    error = chart.get("error")
    if isinstance(error, dict) and str(error.get("code") or "").strip().lower() == "not found":
        return LISTING_ABSENT
    results = chart.get("result")
    if not isinstance(results, list) or not results or not isinstance(results[0], dict):
        return LISTING_UNKNOWN
    result = results[0]
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    has_bars = bool(result.get("timestamp"))
    has_price = _positive(meta.get("regularMarketPrice")) is not None
    if meta.get("currency") and (has_price or has_bars):
        return LISTING_PRESENT
    if not meta.get("currency") and not has_price and not has_bars and not meta.get("regularMarketTime"):
        return LISTING_ABSENT
    return LISTING_UNKNOWN


async def probe_listing(symbol: str, *, timeout: Any = DEFAULT_TIMEOUT) -> str:
    """Yahoo 심볼의 상장 여부 — chart 1회(5d). 404·Not Found·빈 껍데기만 없음,
    시간 초과·5xx·429·쿨다운·형식 오류는 모름(``LISTING_UNKNOWN``)이다."""
    symbol = (symbol or "").strip()
    if not symbol:
        return LISTING_UNKNOWN
    try:
        payload = await fetch_chart_json(
            symbol, range_="5d", interval="1d", include_pre_post=False, timeout=timeout,
        )
    except httpx.HTTPStatusError as exc:
        return LISTING_ABSENT if exc.response.status_code == 404 else LISTING_UNKNOWN
    except (httpx.HTTPError, ValueError) as exc:
        logger.info("Yahoo 상장 확인 실패 (%s): %s", symbol, exc)
        return LISTING_UNKNOWN
    return classify_listing(payload)


def previous_close(meta: dict, *, single_day_range: bool) -> float | None:
    """전일 종가. ``single_day_range`` (range=1d) 일 때만 chartPreviousClose 를
    우선하고, 그 외에는 previousClose 만 믿는다."""
    if single_day_range:
        chart_prev = _positive(meta.get("chartPreviousClose"))
        if chart_prev is not None:
            return chart_prev
    return _positive(meta.get("previousClose"))


def _empty_series() -> dict:
    return {"rows": [], "currency": None, "meta": {}}


async def fetch_close_series(
    symbol: str,
    *,
    range_: str = "1y",
    interval: str = "1d",
    session_date: bool = True,
    currency_fallback: Callable[[str], str] | None = None,
    timeout: Any = DEFAULT_TIMEOUT,
) -> dict:
    """``{rows, currency, meta}`` 종가 시계열. 실패하면 빈 결과(예외 없음)."""
    symbol = (symbol or "").strip()
    if not symbol:
        return _empty_series()
    try:
        payload = await fetch_chart_json(
            symbol, range_=range_, interval=interval, include_pre_post=False, timeout=timeout,
        )
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Yahoo chart fetch failed (%s): %s", symbol, exc)
        return _empty_series()
    chart = parse_chart(payload)
    if chart is None:
        return _empty_series()
    currency = chart.currency or (currency_fallback(symbol).upper() if currency_fallback else None)
    return {
        "rows": chart.close_rows(session_date=session_date),
        "currency": currency,
        "meta": chart.meta,
    }
