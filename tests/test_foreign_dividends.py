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
