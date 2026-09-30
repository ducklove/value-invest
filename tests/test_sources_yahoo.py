"""services/market/sources/yahoo.py — 단일 Yahoo v8 chart provider.

골든 픽스처는 실제 v8 chart 응답 구조(^TNX 일봉, 호주 종목의 현지 세션일,
배당 이벤트·adjclose 포함)를 축약한 것이다.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from core.errors import RateLimitError
from services.market.sources import yahoo
from services.portfolio import foreign
from services.portfolio import history as portfolio_history


def _ts(y, m, d, hh=0, mm=0):
    return int(datetime(y, m, d, hh, mm, tzinfo=timezone.utc).timestamp())


TNX_CHART = {"chart": {"result": [{
    "meta": {"currency": "USD", "symbol": "^TNX", "exchangeTimezoneName": "America/New_York",
             "gmtoffset": -14400, "regularMarketPrice": 4.21, "chartPreviousClose": 4.05,
             "previousClose": 4.18, "regularMarketTime": _ts(2026, 9, 29, 20)},
    "timestamp": [_ts(2026, 9, 25, 13, 30), _ts(2026, 9, 26, 13, 30), _ts(2026, 9, 29, 13, 30)],
    "indicators": {"quote": [{"open": [4.1, 4.15, 4.2], "high": [4.2, 4.22, 4.25],
                              "low": [4.0, 4.1, 4.15], "close": [4.12, None, 4.21],
                              "volume": [0, 0, 0]}],
                   "adjclose": [{"adjclose": [4.12, None, 4.21]}]},
}], "error": None}}

# 시드니 장 마감(현지 16:00 = UTC 06:00)은 UTC 로는 같은 날이지만, 현지 10:00
# 봉(UTC 전날 23:00)은 UTC 날짜와 현지 세션일이 다르다.
AX_CHART = {"chart": {"result": [{
    "meta": {"currency": "AUD", "exchangeTimezoneName": "Australia/Sydney", "gmtoffset": 36000},
    "timestamp": [_ts(2026, 9, 28, 23), _ts(2026, 9, 29, 23)],
    "indicators": {"quote": [{"close": [100.123456789, 101.5]}]},
}]}}

DIV_CHART = {"chart": {"result": [{
    "meta": {"currency": "usd", "gmtoffset": -14400},
    "timestamp": [_ts(2026, 8, 29, 13, 30)],
    "indicators": {"quote": [{"close": [10.0]}], "adjclose": [{"adjclose": [9.9]}]},
    "events": {"dividends": {str(_ts(2026, 8, 29, 13, 30)): {"amount": 0.12, "date": _ts(2026, 8, 29, 13, 30)}}},
}]}}


@pytest.fixture(autouse=True)
def _reset_rate_limit():
    yahoo.reset_rate_limit_state()
    yield
    yahoo.reset_rate_limit_state()


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --- parse -----------------------------------------------------------------

def test_parse_chart_golden_tnx_keeps_all_series_aligned():
    chart = yahoo.parse_chart(TNX_CHART)
    assert chart.currency == "USD"
    assert chart.gmtoffset == -14400
    assert len(chart.timestamps) == len(chart.closes) == len(chart.adjcloses) == 3
    assert chart.opens[0] == 4.1 and chart.highs[2] == 4.25 and chart.lows[1] == 4.1
    assert chart.closes[1] is None
    # close 가 없는 봉은 행에서 빠진다.
    assert chart.close_rows() == [
        {"date": "2026-09-25", "session_date": "2026-09-25", "close": 4.12},
        {"date": "2026-09-29", "session_date": "2026-09-29", "close": 4.21},
    ]


def test_session_date_is_exchange_local_and_can_be_omitted():
    chart = yahoo.parse_chart(AX_CHART)
    assert chart.close_rows() == [
        {"date": "2026-09-28", "session_date": "2026-09-29", "close": 100.123457},
        {"date": "2026-09-29", "session_date": "2026-09-30", "close": 101.5},
    ]
    assert chart.close_rows(session_date=False) == [
        {"date": "2026-09-28", "close": 100.123457},
        {"date": "2026-09-29", "close": 101.5},
    ]


def test_unknown_exchange_zone_matches_legacy_empty_rows():
    chart = yahoo.parse_chart({"chart": {"result": [{
        "meta": {"exchangeTimezoneName": "Mars/Olympus"},
        "timestamp": [_ts(2026, 9, 29)], "indicators": {"quote": [{"close": [1.0]}]}}]}})
    assert chart.close_rows() == []
    assert chart.close_rows(session_date=False) == [{"date": "2026-09-29", "close": 1.0}]


def test_parse_chart_dividends_and_adjclose():
    chart = yahoo.parse_chart(DIV_CHART)
    assert chart.currency == "USD"
    assert chart.adjcloses == [9.9]
    assert chart.dividends == [{"amount": 0.12, "date": _ts(2026, 8, 29, 13, 30)}]


@pytest.mark.parametrize("payload", [None, [], {}, {"chart": None}, {"chart": {"result": None}},
                                     {"chart": {"result": []}}, {"chart": {"result": ["x"]}}])
def test_parse_chart_returns_none_for_missing_result(payload):
    assert yahoo.parse_chart(payload) is None


def test_parse_chart_tolerates_missing_blocks():
    chart = yahoo.parse_chart({"chart": {"result": [{"meta": None, "indicators": {"quote": None}}]}})
    assert chart.meta == {} and chart.closes == [] and chart.close_rows() == []


def test_previous_close_precedence():
    meta = {"chartPreviousClose": 4.05, "previousClose": 4.18}
    # range=1d: chartPreviousClose 가 곧 전일 종가.
    assert yahoo.previous_close(meta, single_day_range=True) == 4.05
    # 다일 구간: chartPreviousClose 는 구간 시작 직전 종가라 쓰지 않는다.
    assert yahoo.previous_close(meta, single_day_range=False) == 4.18
    assert yahoo.previous_close({"chartPreviousClose": 0, "previousClose": 7}, single_day_range=True) == 7
    assert yahoo.previous_close({"chartPreviousClose": 4.05}, single_day_range=False) is None
    assert yahoo.previous_close({"previousClose": "NaN"}, single_day_range=False) is None


# --- fetch -----------------------------------------------------------------

async def test_fetch_chart_json_builds_one_url_and_query():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=DIV_CHART)

    async with _client(handler) as client:
        payload = await yahoo.fetch_chart_json("BRK B/X", range_="2y", events="div",
                                               include_pre_post=False, client=client)
    assert payload == DIV_CHART
    request = seen[0]
    assert request.url.host == "query1.finance.yahoo.com"
    assert request.url.raw_path.decode().startswith("/v8/finance/chart/BRK%20B%2FX?")
    assert dict(request.url.params) == {"range": "2y", "interval": "1d", "includePrePost": "false", "events": "div"}
    assert request.headers["User-Agent"] == "Mozilla/5.0"


async def test_fetch_uses_shared_yahoo_client_by_default():
    async with _client(lambda request: httpx.Response(200, json=TNX_CHART)) as client:
        getter = AsyncMock(return_value=client)
        with patch.object(yahoo, "get_http_client", getter):
            await yahoo.fetch_chart_json("^TNX", range_="5d")
    getter.assert_awaited_once_with("yahoo")


async def test_non_json_and_http_errors_raise_for_callers():
    async with _client(lambda request: httpx.Response(200, text="<html>")) as client:
        with pytest.raises(ValueError):
            await yahoo.fetch_chart_json("X", client=client)
    async with _client(lambda request: httpx.Response(404)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await yahoo.fetch_chart_json("X", client=client)


async def test_one_host_limit_caps_concurrency_across_callers(monkeypatch):
    monkeypatch.setattr(yahoo, "HOST_CONCURRENCY", 2)
    active = 0
    peak = 0

    async def handler(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(200, json=TNX_CHART)

    async with _client(handler) as client:
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)):
            await asyncio.gather(
                foreign.fetch_yahoo_chart("AAPL", range_="5d"),
                foreign.fetch_yahoo_chart("MSFT", range_="5d"),
                portfolio_history.fetch_yahoo_chart("SPY", range_="1y"),
                yahoo.fetch_chart_json("^TNX", range_="1d", interval="1m", client=client),
                yahoo.fetch_chart_json("GC=F", range_="1d", interval="1m", client=client),
            )
    assert peak == 2


async def test_429_is_retried_once_after_backoff(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(yahoo.asyncio, "sleep", fake_sleep)
    responses = [httpx.Response(429, headers={"Retry-After": "2"}), httpx.Response(200, json=TNX_CHART)]
    async with _client(lambda request: responses.pop(0)) as client:
        payload = await yahoo.fetch_chart_json("^TNX", client=client)
    assert payload == TNX_CHART
    assert sleeps == [2.0]
    assert yahoo.cooldown_remaining() == 0


async def test_repeated_429_starts_host_cooldown_without_further_requests(monkeypatch):
    monkeypatch.setattr(yahoo.asyncio, "sleep", AsyncMock())
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(429)

    async with _client(handler) as client:
        with pytest.raises(yahoo.YahooRateLimitedError) as first:
            await yahoo.fetch_chart_json("^TNX", client=client)
        assert len(calls) == 2  # 원 요청 + 재시도 1회
        assert yahoo.cooldown_remaining() > yahoo.COOLDOWN_SECONDS - 5
        with pytest.raises(yahoo.YahooRateLimitedError):
            await yahoo.fetch_chart_json("GC=F", client=client)
    assert len(calls) == 2  # 쿨다운 중에는 네트워크에 닿지 않는다
    # 기존 호출부의 httpx.HTTPError 처리와 core.errors 계층 모두에 걸린다.
    assert isinstance(first.value, httpx.HTTPError)
    assert isinstance(first.value, RateLimitError)
    assert first.value.status_code == 429


async def test_long_retry_after_skips_inline_retry_and_extends_cooldown(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(yahoo.asyncio, "sleep", sleep)
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(429, headers={"Retry-After": "120"})

    async with _client(handler) as client:
        with pytest.raises(yahoo.YahooRateLimitedError):
            await yahoo.fetch_chart_json("^TNX", client=client)
    assert calls == [1]
    sleep.assert_not_awaited()
    assert 110 < yahoo.cooldown_remaining() <= 120


# --- fetch_close_series (foreign/history 계약) ------------------------------

async def test_foreign_and_history_wrappers_keep_their_row_shapes():
    async with _client(lambda request: httpx.Response(200, json=AX_CHART)) as client:
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)):
            foreign_payload = await foreign.fetch_yahoo_chart("A200.AX", range_="1mo")
            history_payload = await portfolio_history.fetch_yahoo_chart("A200.AX", range_="1mo")
    assert foreign_payload["currency"] == "AUD"
    assert foreign_payload["meta"]["exchangeTimezoneName"] == "Australia/Sydney"
    assert foreign_payload["rows"][0] == {"date": "2026-09-28", "session_date": "2026-09-29", "close": 100.123457}
    # insight 히스토리 응답에는 session_date 가 없다(기존 모양 유지).
    assert history_payload["rows"][0] == {"date": "2026-09-28", "close": 100.123457}


async def test_close_series_never_raises_and_infers_currency():
    no_currency = {"chart": {"result": [{"meta": {}, "timestamp": [_ts(2026, 9, 29)],
                                         "indicators": {"quote": [{"close": [1.5]}]}}]}}
    async with _client(lambda request: httpx.Response(200, json=no_currency)) as client:
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)):
            payload = await foreign.fetch_yahoo_chart("7203.T")
    assert payload["currency"] == "JPY"

    async with _client(lambda request: httpx.Response(500)) as client:
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)):
            assert await foreign.fetch_yahoo_chart("AAPL") == {"rows": [], "currency": None, "meta": {}}
    assert await yahoo.fetch_close_series("  ") == {"rows": [], "currency": None, "meta": {}}

    yahoo._start_cooldown(None)
    getter = AsyncMock()
    with patch.object(yahoo, "get_http_client", getter):
        assert await portfolio_history.fetch_yahoo_chart("AAPL") == {"rows": [], "currency": None, "meta": {}}
    getter.assert_not_awaited()


# --- market_indicators (legacy root) callers ------------------------------

def test_market_indicators_yahoo_quotes_go_through_provider():
    import market_indicators as mi

    seen = []

    def handler(request):
        symbol = request.url.raw_path.decode().split("?", 1)[0].rsplit("/", 1)[-1]
        seen.append((symbol, dict(request.url.params)))
        if symbol == "CL%3DF":
            return httpx.Response(500)
        return httpx.Response(200, json=TNX_CHART)

    client = _client(handler)

    async def run():
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)):
            return (
                await mi._fetch_us10y(),
                await mi._fetch_gold_live(),
                await mi._fetch_yahoo_commodity("CL=F"),
            )

    us10y, gold, wti = asyncio.run(run())
    # 전일값 = 마지막에서 두 번째 유효 종가(4.12) — 기존 파서와 같은 규칙.
    assert us10y == {"value": "4.21", "change": "0.09", "change_pct": "2.18%", "direction": "up"}
    assert gold == {"value": "4.21", "change": "0.09", "change_pct": "2.18%", "direction": "up"}
    assert wti == mi._EMPTY
    assert [symbol for symbol, _ in seen] == ["%5ETNX", "GC%3DF", "CL%3DF"]
    assert all(params == {"range": "5d", "interval": "1d"} for _, params in seen)


def test_market_indicators_yahoo_quote_falls_back_to_chart_previous_close():
    import market_indicators as mi

    payload = {"chart": {"result": [{
        "meta": {"regularMarketPrice": 1234.5, "chartPreviousClose": 1200.0},
        "timestamp": [_ts(2026, 9, 29)], "indicators": {"quote": [{"close": [1234.5]}]}}]}}
    client = _client(lambda request: httpx.Response(200, json=payload))

    async def run():
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)):
            return await mi._fetch_yahoo_commodity("GC=F")

    assert asyncio.run(run()) == {
        "value": "1,234.50", "change": "34.50", "change_pct": "2.88%", "direction": "up",
    }
