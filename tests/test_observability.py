"""Tests for observability.py + cache system_events helpers + admin routes.

These three layers are tested together because they only make sense as a
stack: record_event writes through cache, admin endpoints read through
cache, and the end-to-end contract we care about is "operator clicks
something and sees the right rows".
"""
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import cache
import observability
from routes import admin as admin_route


def _admin_request() -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/admin/events",
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


class SystemEventCacheTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_insert_and_read(self):
        row_id = await cache.insert_system_event(
            level="info", source="wiki_ingestion", kind="tick_ok",
            details=json.dumps({"summarized": 3}),
        )
        self.assertGreater(row_id, 0)

        rows = await cache.get_system_events()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "wiki_ingestion")
        self.assertEqual(rows[0]["kind"], "tick_ok")
        self.assertIn("summarized", rows[0]["details"])

    async def test_filter_by_source_level_stock(self):
        await cache.insert_system_event(level="info", source="wiki_ingestion", kind="tick_ok")
        await cache.insert_system_event(level="error", source="snapshot_nps", kind="scrape_failed")
        await cache.insert_system_event(
            level="warning", source="wiki_ingestion", kind="pdf_parse_failed",
            stock_code="005930",
        )

        # Source filter.
        wiki = await cache.get_system_events(source="wiki_ingestion")
        self.assertEqual(len(wiki), 2)
        # Level filter.
        errs = await cache.get_system_events(level="error")
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0]["source"], "snapshot_nps")
        # Stock filter.
        by_stock = await cache.get_system_events(stock_code="005930")
        self.assertEqual(len(by_stock), 1)

    async def test_since_filter(self):
        past = (datetime.now() - timedelta(hours=2)).isoformat(timespec="seconds")
        recent = datetime.now().isoformat(timespec="seconds")
        await cache.insert_system_event(level="info", source="X", kind="old", ts=past)
        await cache.insert_system_event(level="info", source="X", kind="new", ts=recent)
        cutoff = (datetime.now() - timedelta(hours=1)).isoformat(timespec="seconds")
        rows = await cache.get_system_events(since=cutoff)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "new")

    async def test_newest_first_ordering(self):
        ts_old = (datetime.now() - timedelta(minutes=5)).isoformat(timespec="seconds")
        ts_new = datetime.now().isoformat(timespec="seconds")
        await cache.insert_system_event(level="info", source="X", kind="a", ts=ts_old)
        await cache.insert_system_event(level="info", source="X", kind="b", ts=ts_new)
        rows = await cache.get_system_events()
        self.assertEqual(rows[0]["kind"], "b")  # newest first

    async def test_limit_clamped(self):
        for i in range(5):
            await cache.insert_system_event(level="info", source="X", kind=f"k{i}")
        # Negative / zero → minimum 1, huge → capped at 1000
        rows = await cache.get_system_events(limit=0)
        self.assertEqual(len(rows), 1)
        rows = await cache.get_system_events(limit=10000)
        self.assertEqual(len(rows), 5)

    async def test_summarize(self):
        await cache.insert_system_event(level="info", source="wiki_ingestion", kind="tick_ok")
        await cache.insert_system_event(level="info", source="wiki_ingestion", kind="tick_ok")
        await cache.insert_system_event(level="error", source="wiki_ingestion", kind="tick_crashed")
        await cache.insert_system_event(level="info", source="snapshot_nav", kind="tick_ok")
        since = (datetime.now() - timedelta(days=1)).isoformat(timespec="seconds")
        summary = await cache.summarize_system_events(since)
        self.assertEqual(summary["wiki_ingestion"]["info"], 2)
        self.assertEqual(summary["wiki_ingestion"]["error"], 1)
        self.assertEqual(summary["snapshot_nav"]["info"], 1)

    async def test_latest_event(self):
        await cache.insert_system_event(
            level="info", source="wiki_ingestion", kind="tick_ok",
            ts=(datetime.now() - timedelta(minutes=10)).isoformat(timespec="seconds"),
        )
        await cache.insert_system_event(
            level="info", source="wiki_ingestion", kind="tick_ok",
            ts=datetime.now().isoformat(timespec="seconds"),
        )
        row = await cache.get_latest_event("wiki_ingestion")
        self.assertIsNotNone(row)
        # Returns the newer one.
        self.assertIsNotNone(row["ts"])

    async def test_prune_by_age(self):
        # Insert one row 40 days old and one recent.
        old_ts = (datetime.now() - timedelta(days=40)).isoformat(timespec="seconds")
        await cache.insert_system_event(level="info", source="X", kind="old", ts=old_ts)
        await cache.insert_system_event(level="info", source="X", kind="new")
        deleted = await cache.prune_system_events(max_age_days=30)
        self.assertEqual(deleted, 1)
        rows = await cache.get_system_events()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "new")

    async def test_prune_row_cap(self):
        for i in range(20):
            await cache.insert_system_event(level="info", source="X", kind=f"k{i}")
        deleted = await cache.prune_system_events(max_age_days=30, max_rows=10)
        self.assertEqual(deleted, 10)
        rows = await cache.get_system_events(limit=100)
        self.assertEqual(len(rows), 10)


class RecordEventTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_record_event_wait_writes_synchronously(self):
        await observability.record_event(
            "wiki_ingestion", "tick_ok",
            level="info", details={"summarized": 5}, wait=True,
        )
        rows = await cache.get_system_events()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "tick_ok")
        self.assertIn('"summarized": 5', rows[0]["details"])

    async def test_record_event_normalizes_bad_level(self):
        # Invalid level should silently clamp to 'info' rather than raising.
        await observability.record_event(
            "X", "weird", level="BOGUS", wait=True,
        )
        rows = await cache.get_system_events()
        self.assertEqual(rows[0]["level"], "info")

    async def test_record_event_handles_unserializable_details(self):
        # datetime is not JSON-native — default=str fallback kicks in.
        dt = datetime(2026, 1, 1, 12, 0, 0)
        await observability.record_event(
            "X", "with_datetime", details={"when": dt}, wait=True,
        )
        rows = await cache.get_system_events()
        self.assertIn("2026", rows[0]["details"])

    async def test_record_event_never_raises_on_db_failure(self):
        # Patch cache.insert_system_event to explode; observability must swallow.
        with patch.object(cache, "insert_system_event", AsyncMock(side_effect=RuntimeError("boom"))):
            # Must not raise.
            await observability.record_event("X", "test", wait=True)


class AdminEventRouteTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_list_events_requires_admin(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value={"is_admin": False})):
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.list_events(_admin_request())
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_list_events_returns_parsed_details(self):
        await cache.insert_system_event(
            level="info", source="wiki_ingestion", kind="tick_ok",
            details=json.dumps({"summarized": 7, "skipped": 3}),
        )
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            rows = await admin_route.list_events(_admin_request())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["details_obj"]["summarized"], 7)
        self.assertEqual(rows[0]["details_obj"]["skipped"], 3)

    async def test_list_events_applies_filters(self):
        await cache.insert_system_event(level="info", source="wiki_ingestion", kind="tick_ok")
        await cache.insert_system_event(level="error", source="snapshot_nps", kind="scrape_failed")
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            rows = await admin_route.list_events(_admin_request(), level="error")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "snapshot_nps")

    async def test_event_summary_groups_correctly(self):
        await cache.insert_system_event(level="info", source="wiki_ingestion", kind="tick_ok")
        await cache.insert_system_event(level="error", source="wiki_ingestion", kind="tick_crashed")
        await cache.insert_system_event(level="info", source="snapshot_nav", kind="tick_ok")
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            summary = await admin_route.event_summary(_admin_request(), hours=24)
        self.assertEqual(summary["by_source"]["wiki_ingestion"]["info"], 1)
        self.assertEqual(summary["by_source"]["wiki_ingestion"]["error"], 1)
        # Known sources always appear in `latest` (value None if no row yet).
        self.assertIn("snapshot_nps", summary["latest"])
        self.assertIsNone(summary["latest"]["snapshot_nps"])


class DeployStatusRouteTests(unittest.IsolatedAsyncioTestCase):
    """Guard the 'did my push land' endpoint. If this ever breaks, the
    admin dashboard's only non-SSH way to tell which commit is live
    goes with it."""

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

    async def test_requires_admin(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value={"is_admin": False})):
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.deploy_status(_admin_request())
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_returns_build_info(self):
        # The module-level _BUILD_INFO captured the SHA at import time;
        # we verify the endpoint surfaces it without corrupting shape.
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            out = await admin_route.deploy_status(_admin_request())
        self.assertIn("build", out)
        self.assertIn("service_started", out)
        self.assertIn("actions_runner", out)
        # build dict always has the four keys even if git call failed —
        # empty string is fine, None/missing is not.
        for key in ("sha", "short_sha", "subject", "committed_at"):
            self.assertIn(key, out["build"])

    async def test_runner_status_handles_missing_systemctl(self):
        # Environment without systemd — the helper must degrade to
        # {active: False} rather than raising.
        with patch("subprocess.run", side_effect=FileNotFoundError()):
            result = admin_route._runner_status()
        self.assertFalse(result["active"])


class WikiDiagRouteTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_requires_admin(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value={"is_admin": False})):
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.diag_wiki(_admin_request(), code="051910")
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_verdict_no_pdf_scenario(self):
        # Simulate the 058650 pattern — 25 total, 0 with pdf.
        fake_reports = [
            {"date": "2026-03-06", "firm": "신한투자증권", "title": f"R{i}", "pdf_url": None}
            for i in range(25)
        ]
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())), \
             patch("report_client.fetch_reports", AsyncMock(return_value=fake_reports)):
            out = await admin_route.diag_wiki(_admin_request(), code="058650")
        self.assertEqual(out["naver"]["total"], 25)
        self.assertEqual(out["naver"]["has_pdf"], 0)
        self.assertEqual(out["naver"]["passes_whitelist"], 0)
        self.assertIn("pdf_url 이 전부 비어있음", out["verdict"])

    async def test_verdict_empty_response_scenario(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())), \
             patch("report_client.fetch_reports", AsyncMock(return_value=[])):
            out = await admin_route.diag_wiki(_admin_request(), code="999999")
        self.assertEqual(out["naver"]["total"], 0)
        self.assertIn("Naver 에서 리포트 0건", out["verdict"])

    async def test_verdict_normal_scenario(self):
        fake_reports = [
            {"date": "2026-03-06", "firm": "한국IR협의회", "title": "R1",
             "pdf_url": "https://stock.pstatic.net/stock-research/company/74/x.pdf"},
        ]
        # Seed DB as though ingestion has already succeeded.
        await cache.save_wiki_entry({
            "stock_code": "051910", "source_type": "broker_report",
            "source_ref": "sha1abc", "report_date": "2026-03-06",
            "firm": "한국IR협의회", "title": "R1", "recommendation": "Buy",
            "target_price": 500000.0, "summary_md": "s", "key_points_md": "- k",
            "model": "m", "tokens_in": 1, "tokens_out": 1, "created_at": "2026-03-06T00:00:00",
        })
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())), \
             patch("report_client.fetch_reports", AsyncMock(return_value=fake_reports)):
            out = await admin_route.diag_wiki(_admin_request(), code="051910")
        self.assertEqual(out["db"]["wiki_entries"], 1)
        self.assertIn("정상", out["verdict"])

    async def test_scraper_exception_surfaces_in_verdict(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())), \
             patch("report_client.fetch_reports", AsyncMock(side_effect=RuntimeError("timeout"))):
            out = await admin_route.diag_wiki(_admin_request(), code="051910")
        self.assertIn("Naver 응답 실패", out["verdict"])
        self.assertEqual(out["naver"]["total"], 0)


if __name__ == "__main__":
    unittest.main()
