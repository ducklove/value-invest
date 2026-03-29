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
                    ("444444", "Delta", "2026-01-04T00:00:00", "{}"),
                    ("555555", "Epsilon", "2026-01-05T00:00:00", "{}"),
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

    # --- Recent tab ---

    async def test_save_user_stock_order_reorders_recent_items(self):
        await cache.save_user_stock_order("u1", ["222222", "111111", "333333"])
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertEqual([item["stock_code"] for item in items], ["222222", "111111", "333333"])

    async def test_new_item_appears_at_top_after_reorder(self):
        await cache.save_user_stock_order("u1", ["222222", "111111", "333333"])
        await cache.touch_user_recent_analysis("u1", "444444")
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertEqual(items[0]["stock_code"], "444444")

    async def test_existing_item_moves_to_top_on_re_search(self):
        await cache.save_user_stock_order("u1", ["111111", "222222", "333333"])
        await cache.touch_user_recent_analysis("u1", "333333")
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertEqual(items[0]["stock_code"], "333333")

    async def test_overflow_trims_oldest(self):
        db = await cache.get_db()
        try:
            codes = [f"{i:06d}" for i in range(100, 125)]
            await db.executemany(
                "INSERT OR IGNORE INTO analysis_meta (stock_code, corp_name, analyzed_at, payload_json) VALUES (?, ?, ?, ?)",
                [(c, f"Corp{c}", "2026-01-01T00:00:00", "{}") for c in codes],
            )
            await db.commit()
        finally:
            await db.close()

        for c in codes[:20]:
            await cache.touch_user_recent_analysis("u1", c)
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertEqual(len(items), 20)

        await cache.touch_user_recent_analysis("u1", codes[20])
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertLessEqual(len(items), 20)
        self.assertEqual(items[0]["stock_code"], codes[20])

    async def test_deleted_item_reappears_on_re_search(self):
        await cache.save_user_stock_order("u1", ["111111", "222222", "333333"])
        await cache.delete_user_recent_analysis("u1", "222222")
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertNotIn("222222", [i["stock_code"] for i in items])

        await cache.touch_user_recent_analysis("u1", "222222")
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertEqual(items[0]["stock_code"], "222222")

    async def test_delete_all_then_re_add(self):
        for code in ["111111", "222222", "333333"]:
            await cache.delete_user_recent_analysis("u1", code)
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertEqual(len(items), 0)

        await cache.touch_user_recent_analysis("u1", "111111")
        items = await cache.get_cached_analyses(google_sub="u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["stock_code"], "111111")

    # --- Starred tab ---

    async def _star(self, stock_code):
        await cache.save_user_stock_preference("u1", stock_code, is_starred=True)

    async def _unstar(self, stock_code):
        await cache.unstar_stock("u1", stock_code)

    async def test_starred_tab_shows_only_starred(self):
        await self._star("111111")
        await self._star("333333")
        items = await cache.get_cached_analyses(google_sub="u1", tab="starred")
        codes = [i["stock_code"] for i in items]
        self.assertIn("111111", codes)
        self.assertIn("333333", codes)
        self.assertNotIn("222222", codes)

    async def test_starred_item_appears_at_top(self):
        await self._star("333333")
        await self._star("111111")
        items = await cache.get_cached_analyses(google_sub="u1", tab="starred")
        self.assertEqual(items[0]["stock_code"], "111111")

    async def test_starred_order_reorder(self):
        await self._star("111111")
        await self._star("222222")
        await self._star("333333")
        await cache.save_starred_order("u1", ["333333", "111111", "222222"])
        items = await cache.get_cached_analyses(google_sub="u1", tab="starred")
        self.assertEqual([i["stock_code"] for i in items], ["333333", "111111", "222222"])

    async def test_unstar_removes_from_starred_tab(self):
        await self._star("111111")
        await self._star("222222")
        await self._unstar("111111")
        items = await cache.get_cached_analyses(google_sub="u1", tab="starred")
        codes = [i["stock_code"] for i in items]
        self.assertNotIn("111111", codes)
        self.assertIn("222222", codes)

    async def test_unstar_then_re_star(self):
        await self._star("111111")
        await self._star("222222")
        await self._unstar("111111")
        await self._star("111111")
        items = await cache.get_cached_analyses(google_sub="u1", tab="starred")
        self.assertEqual(items[0]["stock_code"], "111111")

    async def test_unstar_does_not_affect_recent_tab(self):
        await self._star("222222")
        await self._unstar("222222")
        items = await cache.get_cached_analyses(google_sub="u1", tab="recent")
        codes = [i["stock_code"] for i in items]
        self.assertIn("222222", codes)

    async def test_delete_from_recent_does_not_affect_starred(self):
        await self._star("222222")
        await cache.delete_user_recent_analysis("u1", "222222")
        recent = await cache.get_cached_analyses(google_sub="u1", tab="recent")
        starred = await cache.get_cached_analyses(google_sub="u1", tab="starred")
        self.assertNotIn("222222", [i["stock_code"] for i in recent])
        self.assertIn("222222", [i["stock_code"] for i in starred])

    async def test_starred_tab_empty_by_default(self):
        items = await cache.get_cached_analyses(google_sub="u1", tab="starred")
        self.assertEqual(len(items), 0)
