import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cache


class CacheOrderTests(unittest.IsolatedAsyncioTestCase):
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
                "INSERT INTO analysis_meta (stock_code, corp_name, analyzed_at, payload_json) VALUES (?, ?, ?, ?)",
                [
                    ("111111", "Alpha", "2026-01-01T00:00:00", "{}"),
                    ("222222", "Beta", "2026-01-02T00:00:00", "{}"),
                    ("333333", "Gamma", "2026-01-03T00:00:00", "{}"),
                ],
            )
            await db.executemany(
                "INSERT INTO user_recent_analyses (google_sub, stock_code, viewed_at) VALUES (?, ?, ?)",
                [
                    ("u1", "111111", "2026-01-01T00:00:00"),
                    ("u1", "222222", "2026-01-02T00:00:00"),
                    ("u1", "333333", "2026-01-03T00:00:00"),
                ],
            )
            await db.commit()
        finally:
            await db.close()

    async def asyncTearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    async def test_save_user_stock_order_reorders_recent_items(self):
        await cache.save_user_stock_order("u1", ["222222", "111111", "333333"])
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertEqual([item["stock_code"] for item in items], ["222222", "111111", "333333"])
