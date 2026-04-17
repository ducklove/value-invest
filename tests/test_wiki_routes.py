"""Tests for /api/analysis/{code}/wiki route — list endpoint only. Q&A
endpoint tests are added in Phase 3."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cache
from routes import wiki as wiki_route


class WikiListRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    async def _seed(self, stock_code: str, n: int = 3):
        for i in range(n):
            await cache.save_wiki_entry({
                "stock_code": stock_code,
                "source_type": "broker_report",
                "source_ref": f"sha-{i}",
                "report_date": f"2026-04-{10+i:02d}",
                "firm": f"증권{i}",
                "title": f"리포트 {i}",
                "recommendation": "Buy",
                "target_price": 90000.0 + i,
                "summary_md": f"summary {i}",
                "key_points_md": f"- point {i}",
                "model": "m",
                "tokens_in": 1,
                "tokens_out": 1,
                "created_at": f"2026-04-{10+i:02d}T00:00:00",
            })

    async def test_empty_wiki(self):
        resp = await wiki_route.get_stock_wiki("005930", limit=10)
        self.assertEqual(resp["count"], 0)
        self.assertEqual(resp["entries"], [])

    async def test_returns_entries_recent_first(self):
        await self._seed("005930", n=3)
        resp = await wiki_route.get_stock_wiki("005930", limit=10)
        self.assertEqual(resp["count"], 3)
        # Newest first.
        dates = [e["report_date"] for e in resp["entries"]]
        self.assertEqual(dates, sorted(dates, reverse=True))

    async def test_limit_enforced(self):
        await self._seed("005930", n=5)
        resp = await wiki_route.get_stock_wiki("005930", limit=2)
        self.assertEqual(resp["count"], 2)
        self.assertEqual(len(resp["entries"]), 2)

    async def test_scoped_to_stock(self):
        await self._seed("005930", n=2)
        await self._seed("000660", n=1)
        resp = await wiki_route.get_stock_wiki("005930", limit=10)
        for e in resp["entries"]:
            self.assertEqual(e["stock_code"], "005930")
        resp2 = await wiki_route.get_stock_wiki("000660", limit=10)
        self.assertEqual(resp2["count"], 1)


if __name__ == "__main__":
    unittest.main()
