"""services/ecosystem — summary.json envelope 검증, summary-first 로더, 공용 cached_fetch."""

import asyncio
import copy
import json
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from cache_layer import MemoryTTLCache
from services.ecosystem import envelope, fetch, siblings

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ecosystem"
SUMMARY_TOOLS = (
    "all-about-gold", "bond-mate", "buybacks", "common_preferred_spread", "eiayn",
    "gold_gap", "holding_value", "nps-tracker", "spac-hunter",
)


def fixture(tool_id: str) -> dict:
    return json.loads((FIXTURES / f"{tool_id}.summary.json").read_text(encoding="utf-8"))


def rehash(env: dict) -> dict:
    env["contentHash"] = envelope.vc_publish().content_hash(env["data"])
    return env


class EnvelopeValidationTests(unittest.TestCase):
    def test_every_fixture_validates_against_its_schema(self):
        self.assertEqual(envelope.summary_tool_ids(), sorted(SUMMARY_TOOLS))
        for tool_id in SUMMARY_TOOLS:
            env = envelope.validate_summary(fixture(tool_id), tool_id)
            self.assertEqual(env["tool"], tool_id)

    def test_tool_mismatch_is_rejected(self):
        with self.assertRaises(envelope.SummaryRejected):
            envelope.validate_summary(fixture("holding_value"), "common_preferred_spread")

    def test_schema_version_other_than_1_is_rejected(self):
        env = fixture("gold_gap")
        env["schemaVersion"] = 2
        with self.assertRaises(envelope.SummaryRejected):
            envelope.validate_summary(env, "gold_gap")

    def test_tampered_data_fails_hash_check(self):
        env = fixture("gold_gap")
        env["data"]["assets"][0]["gap"] = 99
        with self.assertRaises(envelope.SummaryRejected):
            envelope.validate_summary(env, "gold_gap")

    def test_missing_required_data_key_is_rejected_even_with_valid_hash(self):
        env = fixture("spac-hunter")
        del env["data"]["spacs"][0]["valuationBasis"]
        rehash(env)
        with self.assertRaises(envelope.SummaryRejected) as ctx:
            envelope.validate_summary(env, "spac-hunter")
        self.assertIn("valuationBasis", str(ctx.exception))

    def test_wrong_container_type_is_rejected(self):
        env = fixture("buybacks")
        env["data"]["ratios"] = []
        rehash(env)
        with self.assertRaises(envelope.SummaryRejected):
            envelope.validate_summary(env, "buybacks")

    def test_not_an_object_is_rejected(self):
        with self.assertRaises(envelope.SummaryRejected):
            envelope.validate_summary(["nope"], "gold_gap")


class RegistryUrlTests(unittest.TestCase):
    def test_urls_come_from_registry_and_honour_env_override(self):
        self.assertEqual(siblings.summary_url("spac-hunter"), "https://ducklove.github.io/spac-hunter/summary.json")
        self.assertEqual(siblings.site_url("eiayn"), "https://ducklove.github.io/eiayn/")
        with patch.dict(os.environ, {"GOLD_GAP_BASE_URL": "https://mirror.example/gg/"}):
            self.assertEqual(siblings.summary_url("gold_gap"), "https://mirror.example/gg/summary.json")
            # Pages 아래 레거시 파일도 override 를 따른다.
            self.assertEqual(siblings.data_url("gold_gap", "data"), "https://mirror.example/gg/data.json")
        with patch.dict(os.environ, {"HOLDING_VALUE_BASE_URL": "https://mirror.example/hv"}):
            # raw.githubusercontent 원본 위치는 그대로다.
            self.assertTrue(siblings.data_url("holding_value", "current").startswith("https://raw.githubusercontent.com/"))

    def test_gold_gap_legacy_source_is_pages_not_master(self):
        # data.json 은 gold_gap master 에 없다(orphan data 브랜치 → Pages).
        url = siblings.data_url("gold_gap", "data")
        self.assertEqual(url, "https://ducklove.github.io/gold_gap/data.json")
        self.assertNotIn("/master/", url)

    def test_unknown_tool_raises(self):
        with self.assertRaises(siblings.UnknownTool):
            siblings.summary_url("nope")
        with self.assertRaises(siblings.UnknownTool):
            siblings.data_url("gold_gap", "nope")


class SummaryLoaderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        siblings.reset()
        self._env = patch.dict(os.environ, {"ECOSYSTEM_SUMMARIES": "1"})
        self._env.start()

    def tearDown(self):
        self._env.stop()
        siblings.reset()

    async def test_valid_summary_is_cached_and_recorded(self):
        http = AsyncMock(return_value=(200, fixture("gold_gap"), '"abc"'))
        with patch.object(siblings, "_http_get", http):
            first = await siblings.fetch_summary("gold_gap")
            second = await siblings.fetch_summary("gold_gap")
        self.assertEqual(first["tool"], "gold_gap")
        self.assertEqual(first, second)
        self.assertEqual(http.await_count, 1)
        self.assertEqual(http.await_args.args, ("https://ducklove.github.io/gold_gap/summary.json", None))
        status = siblings.freshness(["gold_gap"])["gold_gap"]
        self.assertEqual(status["source"], "summary")
        self.assertEqual(status["asOf"], "2026-09-27T08:47+09:00")
        self.assertEqual(status["generatedAt"], "2026-09-30T09:00:00+09:00")
        self.assertFalse(status["stale"])

    async def test_404_is_negative_cached(self):
        http = AsyncMock(return_value=(404, None, None))
        with patch.object(siblings, "_http_get", http):
            self.assertIsNone(await siblings.fetch_summary("holding_value"))
            self.assertIsNone(await siblings.fetch_summary("holding_value"))
        self.assertEqual(http.await_count, 1)
        self.assertEqual(siblings.freshness(["holding_value"])["holding_value"]["source"], "legacy")

    async def test_rejected_summary_is_negative_cached(self):
        bad = fixture("holding_value")
        bad["tool"] = "buybacks"
        http = AsyncMock(return_value=(200, bad, None))
        with patch.object(siblings, "_http_get", http):
            self.assertIsNone(await siblings.fetch_summary("holding_value"))
            self.assertIsNone(await siblings.fetch_summary("holding_value"))
        self.assertEqual(http.await_count, 1)
        self.assertIn("rejected", siblings.freshness(["holding_value"])["holding_value"]["error"])

    async def test_non_json_body_is_negative_cached(self):
        http = AsyncMock(side_effect=json.JSONDecodeError("Expecting value", "<html>", 0))
        with patch.object(siblings, "_http_get", http):
            self.assertIsNone(await siblings.fetch_summary("buybacks"))
            self.assertIsNone(await siblings.fetch_summary("buybacks"))
        self.assertEqual(http.await_count, 1)

    async def test_expired_entry_revalidates_with_etag_and_304_keeps_envelope(self):
        http = AsyncMock(side_effect=[(200, fixture("gold_gap"), '"v1"'), (304, None, '"v1"')])
        with patch.object(siblings, "_http_get", http):
            await siblings.fetch_summary("gold_gap")
            entry = siblings._summary_cache.get_entry("gold_gap")
            siblings._summary_cache.set("gold_gap", entry.value, ttl_seconds=-1)  # 만료
            env = await siblings.fetch_summary("gold_gap")
        self.assertEqual(env["tool"], "gold_gap")
        self.assertEqual(http.await_args_list[1].args[1], '"v1"')
        self.assertIsNotNone(siblings._summary_cache.get("gold_gap"))  # TTL 연장

    async def test_network_error_serves_stale_envelope(self):
        http = AsyncMock(side_effect=[(200, fixture("gold_gap"), None), httpx.ConnectError("offline")])
        with patch.object(siblings, "_http_get", http):
            await siblings.fetch_summary("gold_gap")
            entry = siblings._summary_cache.get_entry("gold_gap")
            siblings._summary_cache.set("gold_gap", entry.value, ttl_seconds=-1)
            env = await siblings.fetch_summary("gold_gap")
        self.assertEqual(env["tool"], "gold_gap")
        self.assertTrue(siblings.freshness(["gold_gap"])["gold_gap"]["stale"])

    async def test_network_error_without_previous_falls_back(self):
        http = AsyncMock(side_effect=httpx.ConnectError("offline"))
        with patch.object(siblings, "_http_get", http):
            self.assertIsNone(await siblings.fetch_summary("gold_gap"))

    async def test_disabled_switch_skips_network(self):
        http = AsyncMock()
        with patch.dict(os.environ, {"ECOSYSTEM_SUMMARIES": "0"}), patch.object(siblings, "_http_get", http):
            self.assertIsNone(await siblings.fetch_summary("gold_gap"))
        http.assert_not_awaited()

    async def test_concurrent_cold_misses_share_one_request(self):
        async def slow(url, etag):
            await asyncio.sleep(0.01)
            return 200, fixture("gold_gap"), None

        http = AsyncMock(side_effect=slow)
        with patch.object(siblings, "_http_get", http):
            results = await asyncio.gather(*(siblings.fetch_summary("gold_gap") for _ in range(5)))
        self.assertEqual(http.await_count, 1)
        self.assertTrue(all(r["tool"] == "gold_gap" for r in results))

    async def test_summary_or_legacy_prefers_summary_then_falls_back_on_adapter_error(self):
        legacy = AsyncMock(return_value="legacy")
        with patch.object(siblings, "_http_get", AsyncMock(return_value=(200, fixture("gold_gap"), None))):
            self.assertEqual(await siblings.summary_or_legacy("gold_gap", lambda env: env["tool"], legacy), "gold_gap")
            legacy.assert_not_awaited()

            def broken(env):
                raise KeyError("missing")

            self.assertEqual(await siblings.summary_or_legacy("gold_gap", broken, legacy), "legacy")
            # 쓸 수 없던 summary 는 음성 캐시 — 다음 호출은 곧장 레거시.
            self.assertEqual(await siblings.summary_or_legacy("gold_gap", lambda env: "summary", legacy), "legacy")
        self.assertEqual(legacy.await_count, 2)
        self.assertEqual(siblings.freshness(["gold_gap"])["gold_gap"]["source"], "legacy")

    async def test_http_get_uses_shared_client_and_conditional_header(self):
        class Resp:
            status_code = 304
            headers = {"etag": '"v2"'}

        client = AsyncMock()
        client.get = AsyncMock(return_value=Resp())
        with patch.object(siblings, "get_http_client", AsyncMock(return_value=client)) as get_client:
            status, body, etag = await siblings._http_get("https://x.test/summary.json", '"v1"')
        get_client.assert_awaited_once_with("external_tools")
        self.assertEqual((status, body, etag), (304, None, '"v2"'))
        self.assertEqual(client.get.await_args.kwargs["headers"]["If-None-Match"], '"v1"')


class CachedFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_hit_single_flight_stale_and_no_failure_caching(self):
        cache = MemoryTTLCache("test.cached_fetch", 60)
        calls = []

        async def factory():
            calls.append(1)
            await asyncio.sleep(0.01)
            return {"v": len(calls)}

        results = await asyncio.gather(*(fetch.cached_fetch(cache, "k", factory) for _ in range(4)))
        self.assertEqual(len(calls), 1)
        self.assertEqual(results, [{"v": 1}] * 4)
        results[0]["v"] = 99  # 호출자 변경이 캐시·다른 호출자에 새지 않는다
        self.assertEqual(await fetch.cached_fetch(cache, "k", factory), {"v": 1})
        self.assertEqual(results[1], {"v": 1})

        cache.set("k", {"v": "old"}, ttl_seconds=-1)
        failing = AsyncMock(side_effect=httpx.ConnectError("down"))
        self.assertEqual(await fetch.cached_fetch(cache, "k", failing), {"v": "old"})
        cache.clear()
        with self.assertRaises(httpx.ConnectError):
            await fetch.cached_fetch(cache, "k", failing)
        self.assertIsNone(cache.get("k", allow_stale=True))

    async def test_stale_older_than_a_day_is_not_served(self):
        cache = MemoryTTLCache("test.cached_fetch.old", 60)
        cache.set("k", copy.deepcopy({"v": 1}), ttl_seconds=-1, cached_at="2000-01-01T00:00:00")
        with self.assertRaises(ValueError):
            await fetch.cached_fetch(cache, "k", AsyncMock(side_effect=ValueError("bad json")))


if __name__ == "__main__":
    unittest.main()
