"""hover 툴팁용 당일 일중 시세 API — services/stock_intraday + /api/stocks/{code}/intraday."""
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx

from services import stock_intraday, stock_quotes
from services.market.sources import yahoo as yahoo_source

# fchart 분봉 응답 축약본 — 직전 세션(07-20) 꼬리 + 최신 세션(07-21) 분봉.
FCHART_MINUTE = """<?xml version="1.0" encoding="EUC-KR" ?>
<protocol>
<chartdata symbol="005930" name="?" count="5" timeframe="minute" precision="0" origintime="">
<item data="202607201529|null|null|null|243500|26804000" />
<item data="202607201530|null|null|null|244000|26804038" />
<item data="202607210900|null|null|null|247000|100000" />
<item data="202607210901|null|null|null|248500|200000" />
<item data="202607210902|null|null|null|246000|300000" />
</chartdata>
</protocol>"""

FCHART_DAY = """<?xml version="1.0" encoding="EUC-KR" ?>
<protocol>
<chartdata symbol="005930" name="?" count="3" timeframe="day" precision="0" origintime="19900103">
<item data="20260716|264500|265500|252500|255000|27001478" />
<item data="20260720|241000|257500|240000|244000|26804038" />
<item data="20260721|247000|259500|243000|246000|9720388" />
</chartdata>
</protocol>"""


def _fake_fchart(minute_text=FCHART_MINUTE, day_text=FCHART_DAY):
    async def fetch(symbol, timeframe, count):
        return minute_text if timeframe == "minute" else day_text
    return fetch


class StockIntradayParsingTests(unittest.TestCase):
    def test_latest_session_points_drop_prior_sessions_and_null_prices(self):
        rows = stock_intraday.parse_fchart_rows(FCHART_MINUTE)
        date_iso, points = stock_intraday.extract_latest_session_points(rows)
        self.assertEqual(date_iso, "2026-07-21")
        self.assertEqual(points, [
            {"t": "09:00", "p": 247000.0},
            {"t": "09:01", "p": 248500.0},
            {"t": "09:02", "p": 246000.0},
        ])

    def test_prev_close_is_last_close_before_session_date(self):
        rows = stock_intraday.parse_fchart_rows(FCHART_DAY)
        # 당일(07-21) 행은 장중 진행분이므로 전일종가 계산에서 제외돼야 한다.
        self.assertEqual(stock_intraday.extract_prev_close(rows, "2026-07-21"), 244000.0)
        self.assertEqual(stock_intraday.extract_prev_close(rows, "2026-07-20"), 255000.0)


class StockIntradayServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        stock_intraday._intraday_cache.clear()
        # 전일종가 단축 경로가 다른 테스트의 시세 캐시에 좌우되지 않게 한다.
        stock_quotes._stock_cache.clear()

    async def asyncTearDown(self):
        stock_intraday._intraday_cache.clear()
        stock_quotes._stock_cache.clear()

    async def test_korean_intraday_uses_naver_fchart(self):
        with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=_fake_fchart())) as fetch:
            result = await stock_intraday.get_intraday("005930")

        self.assertEqual(fetch.await_count, 2)  # minute + day
        self.assertTrue(result["supported"])
        self.assertEqual(result["source"], "naver")
        self.assertEqual(result["currency"], "KRW")
        self.assertEqual(result["date"], "2026-07-21")
        self.assertEqual(result["prevClose"], 244000.0)
        self.assertEqual(result["session"], {"start": "09:00", "end": "15:30"})
        self.assertEqual(len(result["points"]), 3)
        self.assertEqual(result["points"][-1], {"t": "09:02", "p": 246000.0})

    async def test_intraday_is_cached_per_code(self):
        with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=_fake_fchart())) as fetch:
            await stock_intraday.get_intraday("005930")
            await stock_intraday.get_intraday("005930")
        self.assertEqual(fetch.await_count, 2)  # 두 번째 호출은 캐시 hit

    async def test_special_assets_are_unsupported_without_upstream_calls(self):
        with (
            patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock()) as naver,
            patch.object(stock_intraday, "_yahoo_intraday", new=AsyncMock()) as yahoo,
        ):
            for code in ("CASH_KRW", "KRX_GOLD", "CRYPTO_BTC"):
                result = await stock_intraday.get_intraday(code)
                self.assertFalse(result["supported"])
                self.assertEqual(result["points"], [])
        naver.assert_not_awaited()
        yahoo.assert_not_awaited()

    async def test_foreign_intraday_resolves_ticker_and_uses_yahoo(self):
        chart = {
            "points": [{"t": "09:30", "p": 10.0}, {"t": "09:35", "p": 10.5}],
            "session": {"start": "09:30", "end": "16:00"},
            "prev_close": 9.8,
            "date": "2026-07-20",
            "currency": "USD",
        }
        with (
            patch.object(stock_intraday, "_yahoo_intraday", new=AsyncMock(return_value=chart)) as yahoo,
            patch.object(stock_intraday.foreign, "ensure_ticker_map", new=AsyncMock()),
        ):
            result = await stock_intraday.get_intraday("AAPL")

        yahoo.assert_awaited_once()
        self.assertEqual(yahoo.await_args.args[0], "AAPL")
        self.assertEqual(result["source"], "yahoo")
        self.assertEqual(result["currency"], "USD")
        self.assertEqual(result["prevClose"], 9.8)
        self.assertEqual(result["session"], {"start": "09:30", "end": "16:00"})
        self.assertEqual(len(result["points"]), 2)

    async def test_upstream_failure_returns_empty_and_is_not_cached(self):
        with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=RuntimeError("down"))):
            result = await stock_intraday.get_intraday("005930")
        self.assertEqual(result["points"], [])

        # 실패는 캐시되지 않는다 — 다음 호출은 즉시 업스트림 재시도.
        with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=_fake_fchart())) as fetch:
            result = await stock_intraday.get_intraday("005930")
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(len(result["points"]), 3)


def _cached_stock(code="005930", *, quote_date="2026-07-21", previous_close=244500.0, stale=False):
    return stock_quotes.Stock(
        code=code, current_price=246000.0, previous_close=previous_close, volume=None,
        created_at=datetime(2026, 7, 21, 9, 2), stale=stale, quote_date=quote_date,
    )


class StockIntradayPrevCloseCacheTests(unittest.IsolatedAsyncioTestCase):
    """X13 — 같은 거래일의 신선한 시세 캐시가 있으면 일봉(전일종가) 요청을 생략한다."""

    async def asyncSetUp(self):
        stock_intraday._intraday_cache.clear()
        stock_quotes._stock_cache.clear()

    async def asyncTearDown(self):
        stock_intraday._intraday_cache.clear()
        stock_quotes._stock_cache.clear()

    async def test_fresh_same_day_quote_skips_day_chart(self):
        stock_quotes._stock_cache.set("005930", _cached_stock())
        with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=_fake_fchart())) as fetch:
            result = await stock_intraday.get_intraday("005930")
        self.assertEqual([call.args[1] for call in fetch.await_args_list], ["minute"])
        self.assertEqual(result["prevClose"], 244500.0)
        self.assertEqual(result["date"], "2026-07-21")
        self.assertEqual(len(result["points"]), 3)

    async def test_quote_from_other_session_falls_back_to_day_chart(self):
        stock_quotes._stock_cache.set("005930", _cached_stock(quote_date="2026-07-20", previous_close=255000.0))
        with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=_fake_fchart())) as fetch:
            result = await stock_intraday.get_intraday("005930")
        self.assertEqual([call.args[1] for call in fetch.await_args_list], ["minute", "day"])
        self.assertEqual(result["prevClose"], 244000.0)  # 일봉 기준(캐시값 아님)

    async def test_stale_or_unusable_quotes_are_ignored(self):
        for stock in (_cached_stock(stale=True), _cached_stock(previous_close=None),
                      _cached_stock(previous_close=0.0), _cached_stock(quote_date=None)):
            stock_intraday._intraday_cache.clear()
            stock_quotes._stock_cache.set("005930", stock)
            with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=_fake_fchart())) as fetch:
                result = await stock_intraday.get_intraday("005930")
            self.assertEqual(fetch.await_count, 2)
            self.assertEqual(result["prevClose"], 244000.0)

    async def test_expired_quote_cache_is_not_used(self):
        stock_quotes._stock_cache.set("005930", _cached_stock(), ttl_seconds=-1)
        with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=_fake_fchart())) as fetch:
            result = await stock_intraday.get_intraday("005930")
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(result["prevClose"], 244000.0)


# range=1d / 5분봉 — 뉴욕 정규장(09:30~16:00, gmtoffset -4h).
AAPL_INTRADAY_CHART = {"chart": {"result": [{
    "meta": {"currency": "usd", "gmtoffset": -14400, "chartPreviousClose": 227.5, "previousClose": 226.0,
             "currentTradingPeriod": {"regular": {"start": 1789133400, "end": 1789156800}}},
    "timestamp": [1789133400, 1789133700, 1789134000],
    "indicators": {"quote": [{"close": [228.1, None, 229.25]}]},
}]}}


class StockIntradayYahooProviderTests(unittest.IsolatedAsyncioTestCase):
    """해외 일중 차트는 공용 Yahoo provider(services.market.sources.yahoo)를 쓴다."""

    async def asyncSetUp(self):
        yahoo_source.reset_rate_limit_state()

    async def asyncTearDown(self):
        yahoo_source.reset_rate_limit_state()

    async def test_yahoo_intraday_parses_chart_via_shared_provider(self):
        seen = []

        def handler(request):
            seen.append(request)
            return httpx.Response(200, json=AAPL_INTRADAY_CHART)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(yahoo_source, "get_http_client", AsyncMock(return_value=client)) as getter:
                chart = await stock_intraday._yahoo_intraday("AAPL")
        getter.assert_awaited_once_with("yahoo")
        self.assertEqual(seen[0].url.path, "/v8/finance/chart/AAPL")
        self.assertEqual(dict(seen[0].url.params), {"range": "1d", "interval": "5m", "includePrePost": "false"})
        self.assertEqual(chart["points"], [{"t": "09:30", "p": 228.1}, {"t": "09:40", "p": 229.25}])
        self.assertEqual(chart["session"], {"start": "09:30", "end": "16:00"})
        self.assertEqual(chart["prev_close"], 227.5)  # range=1d → chartPreviousClose 우선
        self.assertEqual(chart["date"], "2026-09-11")
        self.assertEqual(chart["currency"], "USD")

    async def test_yahoo_intraday_without_result_is_none(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"chart": {"result": []}}))) as client:
            with patch.object(yahoo_source, "get_http_client", AsyncMock(return_value=client)):
                self.assertIsNone(await stock_intraday._yahoo_intraday("AAPL"))


class StockIntradayRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_strips_code_and_delegates_to_service(self):
        from routes import stocks as stocks_routes

        with patch.object(stock_intraday, "get_intraday",
                          new=AsyncMock(return_value={"code": "005930", "points": []})) as svc:
            result = await stocks_routes.get_stock_intraday(" 005930 ")

        svc.assert_awaited_once_with("005930")
        self.assertEqual(result["code"], "005930")
