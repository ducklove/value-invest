"""POST /api/admin/trigger/{job} — in-process run under the NAV lock."""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

from routes import admin, internal

ADMIN = {"google_sub": "admin", "email": "a@example.com", "is_admin": True}


def _request(job: str, body: dict | None = None) -> Request:
    raw = json.dumps(body if body is not None else {}).encode()

    async def receive():
        return {"type": "http.request", "body": raw, "more_body": False}

    return Request({
        "type": "http",
        "method": "POST",
        "path": f"/api/admin/trigger/{job}",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }, receive)


class AdminManualTriggerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        admin._running_jobs.clear()
        self.patches = [
            patch.object(admin, "_require_admin_mutation", new=AsyncMock(return_value=ADMIN)),
            patch.object(admin.observability, "record_event", new=AsyncMock()),
        ]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        for task in list(admin._running_jobs.values()):
            task.cancel()
        await asyncio.sleep(0)
        admin._running_jobs.clear()
        for p in reversed(self.patches):
            p.stop()

    async def _wait_job(self, job: str):
        task = admin._running_jobs.get(job)
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

    async def test_snapshot_runs_in_process_with_date_and_no_subprocess(self):
        run = AsyncMock()
        with patch("snapshot_nav.run_all_snapshots", new=run), \
             patch("asyncio.create_subprocess_exec", new=AsyncMock()) as spawn:
            result = await admin.trigger_job("portfolio-snapshot", _request("portfolio-snapshot", {"date": "2026-09-29"}))
            await self._wait_job("portfolio-snapshot")
        self.assertEqual(result, {"message": "portfolio-snapshot 실행 시작", "date": "2026-09-29"})
        run.assert_awaited_once_with("2026-09-29", manage_db=False)
        spawn.assert_not_called()
        self.assertNotIn("portfolio-snapshot", admin._running_jobs)

    async def test_snapshot_without_date_passes_none(self):
        run = AsyncMock()
        with patch("snapshot_nav.run_all_snapshots", new=run):
            result = await admin.trigger_job("portfolio-snapshot", _request("portfolio-snapshot"))
            await self._wait_job("portfolio-snapshot")
        self.assertIsNone(result["date"])
        run.assert_awaited_once_with(None, manage_db=False)

    async def test_snapshot_waits_for_shared_nav_lock(self):
        run = AsyncMock()
        # Fresh lock bound to this test's loop — the module-level lock must not
        # get bound to a loop that later tests no longer run.
        with patch("snapshot_nav.run_all_snapshots", new=run), \
             patch.object(internal, "_nav_snapshot_lock", asyncio.Lock()):
            await internal._nav_snapshot_lock.acquire()
            try:
                await admin.trigger_job("portfolio-snapshot", _request("portfolio-snapshot"))
                for _ in range(5):
                    await asyncio.sleep(0)
                run.assert_not_awaited()  # blocked behind the timer's NAV settlement
                with self.assertRaises(HTTPException) as ctx:
                    await admin.trigger_job("portfolio-snapshot", _request("portfolio-snapshot"))
                self.assertEqual(ctx.exception.status_code, 409)
            finally:
                internal._nav_snapshot_lock.release()
            await self._wait_job("portfolio-snapshot")
        run.assert_awaited_once()

    async def test_intraday_runs_in_process_and_ignores_date(self):
        run = AsyncMock()
        with patch("services.portfolio.intraday_snapshot.run", new=run):
            await admin.trigger_job("portfolio-intraday", _request("portfolio-intraday", {"date": "2026-09-29"}))
            await self._wait_job("portfolio-intraday")
        run.assert_awaited_once_with(manage_db=False)

    async def test_failure_is_logged_not_raised(self):
        run = AsyncMock(side_effect=RuntimeError("upstream down"))
        with patch("snapshot_nav.run_all_snapshots", new=run), \
             self.assertLogs("routes.admin", level="ERROR") as logs:
            await admin.trigger_job("portfolio-snapshot", _request("portfolio-snapshot"))
            await self._wait_job("portfolio-snapshot")
        self.assertTrue(any("Manual job failed: portfolio-snapshot" in line for line in logs.output))
        self.assertNotIn("portfolio-snapshot", admin._running_jobs)

    async def test_rejects_unknown_job_and_bad_date(self):
        with self.assertRaises(HTTPException) as ctx:
            await admin.trigger_job("rm-rf", _request("rm-rf"))
        self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(HTTPException) as ctx:
            await admin.trigger_job("portfolio-snapshot", _request("portfolio-snapshot", {"date": "2026-13-40; rm"}))
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(admin._running_jobs, {})

    async def test_every_listed_timer_has_a_runner(self):
        self.assertEqual({t["name"] for t in admin._TIMERS}, set(admin._JOB_RUNNERS))


if __name__ == "__main__":
    unittest.main()
