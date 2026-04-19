import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cache


class PortfolioTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        # Previous test may have left cache._conn pointing at a now-deleted
        # temp DB or a closed handle. close_db() is idempotent and resets
        # the singleton so init_db() opens a fresh conn on the patched path.
        await cache.close_db()
        await cache.init_db()

        db = await cache.get_db()
        await db.execute(
            "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "user@example.com", "User", "", 1, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        await db.executemany(
            "INSERT INTO corp_codes (stock_code, corp_code, corp_name, updated_at) VALUES (?, ?, ?, ?)",
            [
                ("005930", "00126380", "삼성전자", "2026-01-01"),
                ("000660", "00164779", "SK하이닉스", "2026-01-01"),
            ],
        )
        await db.commit()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    # --- CRUD ---

    async def test_empty_portfolio(self):
        items = await cache.get_portfolio("u1")
        self.assertEqual(items, [])

    async def test_add_item(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["stock_code"], "005930")
        self.assertEqual(items[0]["quantity"], 100)
        self.assertEqual(items[0]["avg_price"], 65000)

    async def test_update_item(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 200, 70000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["quantity"], 200)
        self.assertEqual(items[0]["avg_price"], 70000)

    async def test_delete_item(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.delete_portfolio_item("u1", "005930")
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 0)

    async def test_get_portfolio_exposes_created_at(self):
        """UI 의 '등록일자' 컬럼이 비어있지 않도록, get_portfolio SELECT 에
        created_at 이 반드시 포함돼야 한다는 계약 고정."""
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertIn("created_at", items[0])
        self.assertTrue(items[0]["created_at"])  # non-empty ISO string

    async def test_save_portfolio_preserves_created_at_on_edit(self):
        """수량/매입가만 편집할 때 등록일자가 리셋되면 안 된다."""
        import asyncio
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        original = (await cache.get_portfolio("u1"))[0]["created_at"]
        # Wait a beat so the timestamp would differ if we accidentally reset
        # created_at to now() during the second save.
        await asyncio.sleep(0.01)
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 200, 70000)
        updated = (await cache.get_portfolio("u1"))[0]["created_at"]
        self.assertEqual(updated, original)

    async def test_save_portfolio_accepts_explicit_created_at(self):
        """등록일자 edit form 에서 넘어온 명시적 값은 존중되어야 함."""
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        explicit = "2025-01-15T00:00:00"
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 100, 65000, created_at=explicit,
        )
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["created_at"], explicit)

    async def test_get_trailing_dividends(self):
        """market_data 의 가장 최근 positive 배당금을 종목별로 반환.
        0 또는 NULL 인 해는 건너뛰고, 올해는 아직 공시 전일 수 있으므로
        제외 (stock_price.py 의 dividend fallback 과 동일한 원칙)."""
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.executemany(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            [
                # 삼성전자: 3년치 중 최근 (current_year - 1) 은 0 → 그 전 해로 fallback
                ("005930", current_year - 2, 70000, 1444.0),
                ("005930", current_year - 1, 72000, 0.0),
                ("005930", current_year, 75000, None),
                # SK하이닉스: 단일 positive 연도
                ("000660", current_year - 1, 100000, 1200.0),
                # 네이버: 전부 0 → None 반환 (dict 에 미포함)
                ("035420", current_year - 1, 200000, 0.0),
            ],
        )
        await db.commit()
        dps = await cache.get_trailing_dividends(["005930", "000660", "035420", "999999"])
        self.assertEqual(dps.get("005930"), 1444.0)
        self.assertEqual(dps.get("000660"), 1200.0)
        self.assertNotIn("035420", dps)  # only zeros → excluded
        self.assertNotIn("999999", dps)  # no rows → excluded

    async def test_get_trailing_dividends_empty_list(self):
        self.assertEqual(await cache.get_trailing_dividends([]), {})

    async def test_delete_nonexistent(self):
        await cache.delete_portfolio_item("u1", "999999")
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 0)

    async def test_multiple_items(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 2)

    # --- Ordering ---

    async def test_reorder(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        await cache.save_portfolio_order("u1", ["000660", "005930"])
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["stock_code"], "000660")
        self.assertEqual(items[1]["stock_code"], "005930")

    async def test_new_item_goes_to_top(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_order("u1", ["005930"])
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["stock_code"], "000660")

    # --- Delete + re-add ---

    async def test_delete_then_readd(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.delete_portfolio_item("u1", "005930")
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 50, 72000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["quantity"], 50)

    async def test_delete_one_keeps_others(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        await cache.delete_portfolio_item("u1", "005930")
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["stock_code"], "000660")

    # --- Preferred stock name resolution ---

    async def test_resolve_common_stock(self):
        name = await cache.resolve_stock_name("005930")
        self.assertEqual(name, "삼성전자")

    async def test_resolve_returns_none_for_preferred(self):
        """Preferred stocks are not in corp_codes; resolve returns None (Naver fallback in route)."""
        name = await cache.resolve_stock_name("005935")
        self.assertIsNone(name)

    # --- Bulk / clear ---

    async def test_clear_portfolio(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        await cache.clear_portfolio("u1")
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 0)

    async def test_clear_then_add(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.clear_portfolio("u1")
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 50, 190000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["stock_code"], "000660")

    async def test_bulk_add_preserves_existing(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 2)

    async def test_bulk_replace_clears_first(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.clear_portfolio("u1")
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 50, 190000)
        items = await cache.get_portfolio("u1")
        self.assertNotIn("005930", [i["stock_code"] for i in items])
        self.assertIn("000660", [i["stock_code"] for i in items])

    async def test_resolve_unknown_stock(self):
        name = await cache.resolve_stock_name("999999")
        self.assertIsNone(name)
