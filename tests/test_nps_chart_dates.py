import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import json

import cache
from routes import nps as nps_route
import snapshot_nps


class NpsScriptEscapingTests(unittest.TestCase):
    """The NPS view injects server HTML and executes its inline <script>, so any
    data embedded in that script must not be able to break out of it."""

    def test_json_for_script_neutralizes_script_breakout(self):
        evil = [{"name": "ACME</script><script>alert(1)//", "value": 1}]
        out = snapshot_nps._json_for_script(evil)
        self.assertNotIn("</script>", out)
        self.assertNotIn("<script>", out)
        self.assertIn("\\u003c", out)
        # JSON/JS-equivalent: parsed data is unchanged.
        self.assertEqual(json.loads(out), evil)

    def test_route_json_for_script_matches_generator(self):
        evil = [{"date": "2026-01-01", "value": 1, "x": "a<b>&c"}]
        out = nps_route._json_for_script(evil)
        self.assertNotIn("</script>", out)
        self.assertEqual(json.loads(out), evil)

    def test_replace_json_const_inserts_escaped_payload_and_round_trips(self):
        html = "x const NPS_KOSPI_DATA   = []; y"
        rows = [{"date": "2026-01-01", "value": 1, "note": "a</script>b"}]
        replaced = nps_route._replace_json_const(html, "NPS_KOSPI_DATA", rows)
        self.assertNotIn("</script>", replaced)
        # The const must still be readable back as the original data.
        self.assertEqual(nps_route._extract_json_const(replaced, "NPS_KOSPI_DATA"), rows)


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

    async def test_repair_drops_incomplete_price_snapshot_points(self):
        await cache.save_benchmark_rows("KOSPI", [
            {"date": "2026-05-19", "close": 7271.66},
            {"date": "2026-05-20", "close": 7208.95},
            {"date": "2026-05-21", "close": 7815.59},
            {"date": "2026-05-22", "close": 7847.71},
            {"date": "2026-05-26", "close": 8047.51},
            {"date": "2026-05-27", "close": 8228.70},
        ])
        html = """
<script>
const NPS_NAV_DATA     = [{"date":"2026-05-19","nav":1870.88},{"date":"2026-05-20","nav":597.70},{"date":"2026-05-21","nav":2007.76},{"date":"2026-05-22","nav":2023.78},{"date":"2026-05-26","nav":2023.78},{"date":"2026-05-27","nav":2023.78}];
const NPS_KOSPI_DATA   = [{"date":"2026-05-19","value":7271.66},{"date":"2026-05-20","value":7208.95},{"date":"2026-05-21","value":7815.59},{"date":"2026-05-22","value":7847.71},{"date":"2026-05-26","value":8047.51},{"date":"2026-05-27","value":8228.70}];
const NPS_VALUE_DATA   = [{"date":"2026-05-19","total_value":436},{"date":"2026-05-20","total_value":139},{"date":"2026-05-21","total_value":468},{"date":"2026-05-22","total_value":472},{"date":"2026-05-26","total_value":472},{"date":"2026-05-27","total_value":472}];
</script>
"""

        fixed = await nps_route._repair_nps_chart_html(html)
        nav = nps_route._extract_json_const(fixed, "NPS_NAV_DATA")
        kospi = nps_route._extract_json_const(fixed, "NPS_KOSPI_DATA")
        value = nps_route._extract_json_const(fixed, "NPS_VALUE_DATA")

        self.assertEqual([r["date"] for r in nav], ["2026-05-19", "2026-05-21", "2026-05-22"])
        self.assertEqual([r["date"] for r in kospi], ["2026-05-19", "2026-05-21", "2026-05-22"])
        self.assertEqual([r["date"] for r in value], ["2026-05-19", "2026-05-21", "2026-05-22"])

    async def test_trading_day_uses_kospi_calendar_not_weekday_only(self):
        with patch.object(snapshot_nps, "_fetch_kospi_history", new=AsyncMock(return_value=[])):
            self.assertFalse(await snapshot_nps._is_trading_day(date(2026, 5, 1)))
        with patch.object(snapshot_nps, "_fetch_kospi_history", new=AsyncMock(return_value=[{"date": "2026-05-04", "value": 6936.99}])):
            self.assertTrue(await snapshot_nps._is_trading_day(date(2026, 5, 4)))
