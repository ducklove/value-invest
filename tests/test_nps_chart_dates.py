import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import cache
from routes import nps as nps_route
import snapshot_nps


class NpsChartDateTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_repair_filters_holidays_and_fills_kospi_by_date(self):
        await cache.save_benchmark_rows("KOSPI", [
            {"date": "2026-05-04", "close": 6936.99},
            {"date": "2026-05-06", "close": 7384.56},
            {"date": "2026-05-07", "close": 7490.05},
            {"date": "2026-05-08", "close": 7498.00},
        ])
        html = """
<script>
const NPS_NAV_DATA     = [{"date":"2026-05-01","nav":1001},{"date":"2026-05-04","nav":1004},{"date":"2026-05-05","nav":1005},{"date":"2026-05-06","nav":1006},{"date":"2026-05-07","nav":1007},{"date":"2026-05-08","nav":1008}];
const NPS_KOSPI_DATA   = [{"date":"2026-05-04","value":6936.99},{"date":"2026-05-06","value":7384.56}];
const NPS_VALUE_DATA   = [{"date":"2026-05-01","total_value":1},{"date":"2026-05-04","total_value":4},{"date":"2026-05-05","total_value":5},{"date":"2026-05-06","total_value":6},{"date":"2026-05-07","total_value":7},{"date":"2026-05-08","total_value":8}];
</script>
"""

        fixed = await nps_route._repair_nps_chart_html(html)
        nav = nps_route._extract_json_const(fixed, "NPS_NAV_DATA")
        kospi = nps_route._extract_json_const(fixed, "NPS_KOSPI_DATA")
        value = nps_route._extract_json_const(fixed, "NPS_VALUE_DATA")

        self.assertEqual([r["date"] for r in nav], ["2026-05-04", "2026-05-06", "2026-05-07", "2026-05-08"])
        self.assertEqual([r["date"] for r in kospi], ["2026-05-04", "2026-05-06", "2026-05-07", "2026-05-08"])
        self.assertEqual([r["date"] for r in value], ["2026-05-04", "2026-05-06", "2026-05-07", "2026-05-08"])

    async def test_trading_day_uses_kospi_calendar_not_weekday_only(self):
        with patch.object(snapshot_nps, "_fetch_kospi_history", new=AsyncMock(return_value=[])):
            self.assertFalse(await snapshot_nps._is_trading_day(date(2026, 5, 1)))
        with patch.object(snapshot_nps, "_fetch_kospi_history", new=AsyncMock(return_value=[{"date": "2026-05-04", "value": 6936.99}])):
            self.assertTrue(await snapshot_nps._is_trading_day(date(2026, 5, 4)))
