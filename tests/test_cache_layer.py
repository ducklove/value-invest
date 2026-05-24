import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import cache
from cache_layer import MemoryTTLCache


class MemoryTTLCacheTests(unittest.TestCase):
    def test_memory_cache_tracks_timestamps_and_stale_state(self):
        layer = MemoryTTLCache("test.memory", default_ttl_seconds=60)
        entry = layer.set("alpha", {"value": 1})

        self.assertEqual(entry.value, {"value": 1})
        self.assertFalse(entry.stale)
        self.assertIsNotNone(entry.cached_at)
        self.assertIsNotNone(entry.expires_at)

        value = layer.get("alpha")
        value["value"] = 2
        self.assertEqual(layer.get("alpha"), {"value": 1})

    def test_memory_cache_can_return_stale_when_requested(self):
        layer = MemoryTTLCache("test.memory", default_ttl_seconds=0)
        layer.set("alpha", {"value": 1})

        self.assertIsNone(layer.get("alpha"))
        stale = layer.get_entry("alpha", allow_stale=True)
        self.assertIsNotNone(stale)
        self.assertTrue(stale.stale)

    def test_legacy_tuple_seed_keeps_expiry_semantics(self):
        layer = MemoryTTLCache("test.memory", default_ttl_seconds=1)
        layer["alpha"] = (time.monotonic() - 2, {"value": 1})

        self.assertIsNone(layer.get("alpha"))
        stale = layer.get_entry("alpha", allow_stale=True)
        self.assertIsNotNone(stale)
        self.assertTrue(stale.stale)


class PersistentCacheValueTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_persistent_cache_uses_single_entry_shape(self):
        await cache.set_cache_value("test.persistent", "alpha", {"value": 1})

        entry = await cache.get_cache_value_entry(
            "test.persistent",
            "alpha",
            ttl_seconds=60,
        )
        self.assertIsNotNone(entry)
        self.assertEqual(entry.value, {"value": 1})
        self.assertFalse(entry.stale)
        self.assertIsNotNone(entry.cached_at)
        self.assertIsNotNone(entry.expires_at)

        entry.value["value"] = 2
        reread = await cache.get_cache_value_entry(
            "test.persistent",
            "alpha",
            ttl_seconds=60,
        )
        self.assertEqual(reread.value, {"value": 1})

    async def test_persistent_cache_honors_stale_policy(self):
        await cache.set_cache_value("test.persistent", "alpha", {"value": 1})

        self.assertIsNone(
            await cache.get_cache_value_entry(
                "test.persistent",
                "alpha",
                ttl_seconds=0,
            )
        )
        stale = await cache.get_cache_value_entry(
            "test.persistent",
            "alpha",
            ttl_seconds=0,
            allow_stale=True,
        )
        self.assertIsNotNone(stale)
        self.assertTrue(stale.stale)

    async def test_report_cache_wrappers_expose_cache_metadata(self):
        await cache.save_latest_report("005930", {"title": "report"})

        report = await cache.get_latest_report("005930", ttl_minutes=15)
        self.assertEqual(report["title"], "report")
        self.assertFalse(report["_stale"])
        self.assertIsNotNone(report["_cached_at"])
        self.assertIsNotNone(report["_expires_at"])

        self.assertIsNone(await cache.get_latest_report("005930", ttl_minutes=0))
        stale_report = await cache.get_latest_report("005930", ttl_minutes=None)
        self.assertEqual(stale_report["title"], "report")
