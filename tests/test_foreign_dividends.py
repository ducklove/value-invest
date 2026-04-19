"""Tests for foreign_dividends module + cache helpers + admin endpoints.

Key invariant being guarded: source='manual' rows are preserved across
auto refresh. If that contract ever breaks, the admin's manual input
gets silently clobbered on the next yfinance call — which is the exact
regression mode the table's `source` column exists to prevent.
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import cache
from routes import admin as admin_route


def _admin_request() -> Request:
    scope = {
        "type": "http", "method": "POST", "path": "/api/admin/foreign-dividend",
        "headers": [], "query_string": b"",
        "client": ("127.0.0.1", 12345), "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


class FetchOneResolutionOrderTests(unittest.TestCase):
    """_fetch_one_sync 의 배당 필드 우선순위 검증.
    yfinance 자체를 모킹해서 각 시나리오 직접 재현.
    """

    def _patch_yf(self, info: dict):
        """yf.Ticker(...).info 가 주어진 dict 를 반환하도록 패치하는 헬퍼."""
        from unittest.mock import MagicMock, patch as _patch
        fake_ticker = MagicMock()
        fake_ticker.info = info
        fake_yf = MagicMock()
        fake_yf.Ticker.return_value = fake_ticker
        return _patch.dict("sys.modules", {"yfinance": fake_yf})

    def test_trailing_preferred_when_positive(self):
        import foreign_dividends as fd
        with self._patch_yf({
            "trailingAnnualDividendRate": 1.03, "dividendRate": 1.04,
            "yield": 0.004, "regularMarketPrice": 270.0, "currency": "USD",
        }):
            r = fd._fetch_one_sync("AAPL")
        self.assertEqual(r["dps_native"], 1.03)
        self.assertEqual(r["currency"], "USD")

    def test_forward_used_when_trailing_zero(self):
        import foreign_dividends as fd
        with self._patch_yf({
            "trailingAnnualDividendRate": 0.0, "dividendRate": 2264.0,
            "currency": "KRW",
            # 유효 응답 시그널 (name / price 둘 중 하나는 있어야 _looks_like_empty_info 통과)
            "longName": "Samsung Electronics",
            "regularMarketPrice": 216000.0,
        }):
            r = fd._fetch_one_sync("005930.KS")
        self.assertEqual(r["dps_native"], 2264.0)

    def test_yield_times_price_used_when_trailing_and_forward_missing(self):
        """핵심 회귀: 83199.HK 처럼 trailing=0, forward 없음, yield 만
        있는 채권 ETF. yield × price 로 역산해야 함."""
        import foreign_dividends as fd
        with self._patch_yf({
            "trailingAnnualDividendRate": 0.0,
            "yield": 0.0345, "regularMarketPrice": 104.75,
            "currency": "CNY", "longName": "CSOP China 5yr Bond",
        }):
            r = fd._fetch_one_sync("83199.HK")
        # 0.0345 * 104.75 = 3.6139
        self.assertAlmostEqual(r["dps_native"], 3.6139, places=4)
        self.assertEqual(r["currency"], "CNY")

    def test_yield_fallback_uses_current_price_or_previous_close(self):
        """regularMarketPrice 가 없으면 currentPrice / previousClose 순으로 fallback."""
        import foreign_dividends as fd
        with self._patch_yf({
            "trailingAnnualDividendRate": None,
            "yield": 0.04, "previousClose": 100.0,
            "currency": "USD",
        }):
            r = fd._fetch_one_sync("X")
        self.assertEqual(r["dps_native"], 4.0)

    def test_all_zero_stays_zero(self):
        """trailing=0 forward 없음 yield=0 → 0.0 ('확정 배당 없음')."""
        import foreign_dividends as fd
        with self._patch_yf({
            "trailingAnnualDividendRate": 0.0,
            "yield": 0.0, "regularMarketPrice": 100.0,
            "currency": "USD",
        }):
            r = fd._fetch_one_sync("GROWTH")
        self.assertEqual(r["dps_native"], 0.0)

    def test_reuters_suffix_stripped_on_retry(self):
        """`.O`, `.K` 같은 Reuters 접미사는 yfinance 에서 404. 파이프라인이
        접미사 제거 후 재시도해서 DAX.O → DAX 로 resolve 해야 함 (회귀
        가드)."""
        import foreign_dividends as fd
        from unittest.mock import MagicMock, patch as _patch

        def make_ticker_factory():
            call_log = []
            def fake_ticker(sym):
                call_log.append(sym)
                m = MagicMock()
                if sym == "DAX.O":
                    # 빈 info (404 흉내)
                    m.info = {}
                elif sym == "DAX":
                    # stripped 로 재시도했을 때 유효
                    m.info = {
                        "longName": "Global X DAX Germany ETF",
                        "yield": 0.0162, "regularMarketPrice": 45.97,
                        "currency": "USD",
                    }
                    m.dividends = None  # 이 경로로 안 떨어져야
                else:
                    m.info = {}
                return m
            return fake_ticker, call_log

        factory, call_log = make_ticker_factory()
        fake_yf = MagicMock()
        fake_yf.Ticker.side_effect = factory
        with _patch.dict("sys.modules", {"yfinance": fake_yf}):
            r = fd._fetch_one_sync("DAX.O")
        # 두 번 호출: 원본 + stripped
        self.assertEqual(call_log, ["DAX.O", "DAX"])
        self.assertAlmostEqual(r["dps_native"], 0.0162 * 45.97, places=2)
        self.assertEqual(r["currency"], "USD")

    def test_dividends_series_fallback_when_info_fields_missing(self):
        """EUN2.DE 처럼 info 는 정상이지만 배당 관련 필드 전부 None 인
        경우 ticker.dividends Series 최근 365일 합을 사용."""
        import foreign_dividends as fd
        import pandas as pd
        from unittest.mock import MagicMock, patch as _patch

        # 최근 1년 내 4회 지급 (총 1.4874)
        now = pd.Timestamp.now(tz="UTC")
        series = pd.Series(
            [0.4999, 0.7519, 0.1338, 0.1018],
            index=[
                now - pd.Timedelta(days=300),
                now - pd.Timedelta(days=200),
                now - pd.Timedelta(days=100),
                now - pd.Timedelta(days=30),
            ],
        )
        # 366일 넘긴 오래된 지급 — 합산에서 제외되어야
        stale = pd.Series([9.99], index=[now - pd.Timedelta(days=400)])
        full = pd.concat([stale, series])

        fake_ticker = MagicMock()
        fake_ticker.info = {
            "longName": "iShares Core EURO STOXX 50 UCITS ETF EUR",
            "regularMarketPrice": 61.86, "currency": "EUR",
            # 모든 배당 필드 None
            "trailingAnnualDividendRate": None,
            "dividendRate": None,
            "yield": None,
        }
        fake_ticker.dividends = full
        fake_yf = MagicMock()
        fake_yf.Ticker.return_value = fake_ticker
        with _patch.dict("sys.modules", {"yfinance": fake_yf}):
            r = fd._fetch_one_sync("EUN2.DE")
        # 최근 365일 4회 합 = 1.4874, stale 9.99 는 제외
        self.assertAlmostEqual(r["dps_native"], 1.4874, places=3)
        self.assertEqual(r["currency"], "EUR")

    def test_invalid_ticker_returns_none(self):
        """접미사 strip 후에도 여전히 빈 info 면 None. auto-refresh 가
        이 ticker 를 DB 에 0 으로 쓰는 것 대신 failures 리스트에 넣음."""
        import foreign_dividends as fd
        from unittest.mock import MagicMock, patch as _patch
        fake_ticker = MagicMock()
        fake_ticker.info = {}
        fake_yf = MagicMock()
        fake_yf.Ticker.return_value = fake_ticker
        with _patch.dict("sys.modules", {"yfinance": fake_yf}):
            r = fd._fetch_one_sync("TOTALLY_BOGUS")
        self.assertIsNone(r)

    def test_currency_defaults_to_usd(self):
        import foreign_dividends as fd
        with self._patch_yf({
            "trailingAnnualDividendRate": 1.0,
            "longName": "Weird Inc", "regularMarketPrice": 10.0,
            # currency 필드 자체 누락
        }):
            r = fd._fetch_one_sync("WEIRD")
        self.assertEqual(r["currency"], "USD")


class CacheHelpersTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_upsert_auto_basic(self):
        await cache.upsert_foreign_dividends_auto([
            {"stock_code": "AAPL", "dps_native": 0.96, "currency": "USD", "dps_krw": 1320.0},
            {"stock_code": "GOOGL", "dps_native": 0.8, "currency": "USD", "dps_krw": 1100.0},
        ])
        rows = await cache.list_foreign_dividends()
        self.assertEqual(len(rows), 2)
        aapl = next(r for r in rows if r["stock_code"] == "AAPL")
        self.assertEqual(aapl["source"], "yfinance")
        self.assertEqual(aapl["dps_krw"], 1320.0)
        self.assertEqual(aapl["currency"], "USD")

    async def test_manual_overrides_are_not_clobbered_by_auto(self):
        """핵심 불변식 — 수동 override 는 yfinance refresh 에 살아남아야 함."""
        await cache.upsert_foreign_dividend_manual("AAPL", 9999.0, note="임시 override")
        # Auto refresh 가 같은 코드로 들어옴 → ON CONFLICT WHERE 가드가 막아야.
        await cache.upsert_foreign_dividends_auto([
            {"stock_code": "AAPL", "dps_native": 0.96, "currency": "USD", "dps_krw": 1320.0},
        ])
        rows = await cache.list_foreign_dividends()
        aapl = next(r for r in rows if r["stock_code"] == "AAPL")
        self.assertEqual(aapl["source"], "manual")
        self.assertEqual(aapl["dps_krw"], 9999.0)
        self.assertEqual(aapl["manual_note"], "임시 override")

    async def test_manual_second_call_overwrites_previous_manual(self):
        """같은 코드에 수동 두 번 넣으면 최근 값이 이김 — 수정 UX."""
        await cache.upsert_foreign_dividend_manual("AAPL", 1000.0, note="v1")
        await cache.upsert_foreign_dividend_manual("AAPL", 2000.0, note="v2")
        rows = await cache.list_foreign_dividends()
        aapl = next(r for r in rows if r["stock_code"] == "AAPL")
        self.assertEqual(aapl["dps_krw"], 2000.0)
        self.assertEqual(aapl["manual_note"], "v2")

    async def test_auto_updates_only_yfinance_rows(self):
        # 먼저 yfinance 값 1320, 그 다음 manual 9999 로 덮음.
        await cache.upsert_foreign_dividends_auto([
            {"stock_code": "AAPL", "dps_native": 0.96, "currency": "USD", "dps_krw": 1320.0},
        ])
        await cache.upsert_foreign_dividend_manual("AAPL", 9999.0, note=None)
        # 다시 auto 가 1500 으로 갱신하려 해도 manual 이 이김.
        await cache.upsert_foreign_dividends_auto([
            {"stock_code": "AAPL", "dps_native": 1.1, "currency": "USD", "dps_krw": 1500.0},
            # 다른 코드는 정상 반영돼야.
            {"stock_code": "GOOGL", "dps_native": 0.8, "currency": "USD", "dps_krw": 1100.0},
        ])
        rows = {r["stock_code"]: r for r in await cache.list_foreign_dividends()}
        self.assertEqual(rows["AAPL"]["source"], "manual")
        self.assertEqual(rows["AAPL"]["dps_krw"], 9999.0)
        self.assertEqual(rows["GOOGL"]["source"], "yfinance")
        self.assertEqual(rows["GOOGL"]["dps_krw"], 1100.0)

    async def test_delete_removes_row(self):
        await cache.upsert_foreign_dividend_manual("AAPL", 1000.0)
        self.assertTrue(await cache.delete_foreign_dividend("AAPL"))
        rows = await cache.list_foreign_dividends()
        self.assertEqual(rows, [])
        # Removing a non-existent code is a no-op (returns False, doesn't raise).
        self.assertFalse(await cache.delete_foreign_dividend("AAPL"))

    async def test_get_single_row(self):
        """포트폴리오 PUT 후 dispatch 로직이 '이미 값 있으면 skip' 판단에
        사용. row 존재 / 부재 / 빈 코드 모두 no-raise."""
        self.assertIsNone(await cache.get_foreign_dividend("AAPL"))
        self.assertIsNone(await cache.get_foreign_dividend(""))
        self.assertIsNone(await cache.get_foreign_dividend(None))
        await cache.upsert_foreign_dividends_auto([
            {"stock_code": "AAPL", "dps_native": 0.96, "currency": "USD", "dps_krw": 1320.0},
        ])
        got = await cache.get_foreign_dividend("AAPL")
        self.assertIsNotNone(got)
        self.assertEqual(got["stock_code"], "AAPL")
        self.assertEqual(got["source"], "yfinance")
        self.assertEqual(got["dps_krw"], 1320.0)
        # 수동 override 후엔 source 값이 전환되어야 — dispatch 가 '이미
        # 있다' 판정하는 데 값 있음만 보면 됨 (source 무관).
        await cache.upsert_foreign_dividend_manual("AAPL", 9999.0, "override")
        got2 = await cache.get_foreign_dividend("AAPL")
        self.assertEqual(got2["source"], "manual")
        self.assertEqual(got2["dps_krw"], 9999.0)

    async def test_manual_order_first_in_list(self):
        """관리자 UI 가 수동 엔트리를 먼저 보여줄 수 있도록 정렬 회귀 가드."""
        await cache.upsert_foreign_dividends_auto([
            {"stock_code": "AAPL", "dps_native": 0.96, "currency": "USD", "dps_krw": 1320.0},
            {"stock_code": "ZZZZ", "dps_native": 0.0, "currency": "USD", "dps_krw": 0.0},
        ])
        await cache.upsert_foreign_dividend_manual("GOOGL", 2000.0)
        rows = await cache.list_foreign_dividends()
        self.assertEqual(rows[0]["stock_code"], "GOOGL")
        self.assertEqual(rows[0]["source"], "manual")


class GetTrailingDividendsResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_foreign_ticker_resolves_via_foreign_dividends(self):
        """해외 코드는 market_data 에 없으니 foreign_dividends 에서만 매치."""
        await cache.upsert_foreign_dividends_auto([
            {"stock_code": "AAPL", "dps_native": 0.96, "currency": "USD", "dps_krw": 1320.0},
        ])
        dps = await cache.get_trailing_dividends(["AAPL"])
        self.assertEqual(dps.get("AAPL"), 1320.0)

    async def test_manual_override_wins_over_preferred_sheet(self):
        """foreign_dividends (manual 포함) 가 preferred_dividends 보다 우선
        — 관리자가 의도적으로 넣은 값이 더 authoritative."""
        # 우선주 시트에도 값이 있고
        await cache.upsert_preferred_dividends([{
            "stock_code": "005935", "dividend_per_share": 1445.0,
            "source_name": "삼성전자우", "common_code": "005930",
            "sheet_year": 2025,
        }])
        # foreign_dividends 에도 manual 로 다른 값이 있음.
        await cache.upsert_foreign_dividend_manual("005935", 9999.0)
        dps = await cache.get_trailing_dividends(["005935"])
        self.assertEqual(dps.get("005935"), 9999.0)

    async def test_etf_override_beats_zero_market_data(self):
        """채권 ETF 시나리오. market_data 수집 파이프라인이 ETF 분배금을
        커버하지 못해 dps=0 으로 저장되는데, 실제 분배금은 있음.
        관리자가 foreign_dividends 수동 override 로 정확한 값을 넣으면
        그것이 market_data 의 0 보다 우선돼야 한다 ('-' → 실제값).

        반면 양수가 저장된 일반 종목 (삼성전자 등) 은 market_data 가
        여전히 최상위라 override 가 덮어쓰지 못함 — 별도 테스트에서 보장.
        """
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        # 채권 ETF (273130 = KODEX 종합채권) — 파이프라인이 0 으로 저장.
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            ("273130", current_year - 1, 100000, 0.0),
        )
        await db.commit()
        # override 없을 때: 0 반환 (배당 없음이 아니라 '수집 실패' 지만
        # 동일하게 0 표시).
        dps1 = await cache.get_trailing_dividends(["273130"])
        self.assertEqual(dps1.get("273130"), 0.0)
        # 관리자가 실제 분배금 입력:
        await cache.upsert_foreign_dividend_manual("273130", 3500.0, note="채권 ETF 수동")
        dps2 = await cache.get_trailing_dividends(["273130"])
        self.assertEqual(dps2.get("273130"), 3500.0)

    async def test_market_data_still_wins_over_foreign(self):
        """direct market_data 매치는 여전히 최상위 — '해외' 테이블 이름
        과 달리 혹시 한국 보통주가 들어가도 authoritative 한 DART/KRX
        파이프라인 값을 이기진 못함."""
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            ("005930", current_year - 1, 72000, 1444.0),
        )
        await db.commit()
        await cache.upsert_foreign_dividend_manual("005930", 9999.0)
        dps = await cache.get_trailing_dividends(["005930"])
        self.assertEqual(dps.get("005930"), 1444.0)


class AdminEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def _mk_admin(self) -> dict:
        return {"google_sub": "u1", "email": "a@b.c", "is_admin": True}

    async def test_manual_upsert_endpoint(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            out = await admin_route.upsert_foreign_dividend_endpoint(
                _admin_request(), {"stock_code": "aapl", "dps_krw": 1000, "note": "note"},
            )
        self.assertTrue(out["ok"])
        self.assertEqual(out["stock_code"], "AAPL")  # 자동 upper-case
        rows = await cache.list_foreign_dividends()
        self.assertEqual(rows[0]["stock_code"], "AAPL")
        self.assertEqual(rows[0]["source"], "manual")

    async def test_manual_upsert_rejects_missing_code(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.upsert_foreign_dividend_endpoint(
                    _admin_request(), {"stock_code": "", "dps_krw": 1000},
                )
        self.assertEqual(exc_info.exception.status_code, 400)

    async def test_manual_upsert_rejects_non_numeric_dps(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.upsert_foreign_dividend_endpoint(
                    _admin_request(), {"stock_code": "AAPL", "dps_krw": "abc"},
                )
        self.assertEqual(exc_info.exception.status_code, 400)

    async def test_manual_upsert_allows_zero(self):
        """0 은 '배당 없음 확정' 의 유효 값."""
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            out = await admin_route.upsert_foreign_dividend_endpoint(
                _admin_request(), {"stock_code": "ETF", "dps_krw": 0},
            )
        self.assertEqual(out["dps_krw"], 0.0)

    async def test_delete_endpoint(self):
        await cache.upsert_foreign_dividend_manual("AAPL", 1000.0)
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            out = await admin_route.delete_foreign_dividend_endpoint(_admin_request(), "AAPL")
        self.assertTrue(out["ok"])
        self.assertEqual(await cache.get_foreign_dividends_count(), 0)

    async def test_delete_endpoint_404(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.delete_foreign_dividend_endpoint(_admin_request(), "NOPE")
        self.assertEqual(exc_info.exception.status_code, 404)

    async def test_list_endpoint_requires_admin(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value={"is_admin": False})):
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.list_foreign_dividends_endpoint(_admin_request())
        self.assertEqual(exc_info.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()
