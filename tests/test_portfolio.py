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
        await cache.init_db()

        db = await cache.get_db()
        try:
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
        finally:
            await db.close()

    async def asyncTearDown(self):
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

    async def test_resolve_preferred_stock_5(self):
        name = await cache.resolve_stock_name("005935")
        self.assertEqual(name, "삼성전자(우)")

    async def test_resolve_preferred_stock_7(self):
        name = await cache.resolve_stock_name("005937")
        self.assertEqual(name, "삼성전자(우)")

    async def test_resolve_preferred_stock_9(self):
        name = await cache.resolve_stock_name("005939")
        self.assertEqual(name, "삼성전자(우)")

    async def test_resolve_preferred_stock_K(self):
        name = await cache.resolve_stock_name("00593K")
        self.assertEqual(name, "삼성전자(우)")

    async def test_resolve_unknown_stock(self):
        name = await cache.resolve_stock_name("999999")
        self.assertIsNone(name)
