import json
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import economic_calendar
from routes import stocks as stocks_route

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _fixture() -> dict:
    return json.loads((FIXTURES / "zeroin_economic_calendar.json").read_text(encoding="utf-8"))


class CalendarParserTests(unittest.TestCase):
    def test_parse_normalizes_all_rows_sorted_ascending(self):
        events = economic_calendar._parse_calendar(_fixture())
        self.assertEqual(len(events), 14)
        # 업스트림은 내림차순 → 정규화는 시간 오름차순(가장 이른 날짜가 먼저).
        dts = [e["datetime"] for e in events]
        self.assertEqual(dts, sorted(dts))
        self.assertEqual(events[0]["date"], "2026-06-03")
        self.assertEqual(events[-1]["date"], "2026-06-06")

    def test_first_row_fields(self):
        # 06-03 중국 PMI 행이 정렬 후 맨 앞에 온다.
        ev = economic_calendar._parse_calendar(_fixture())[0]
        self.assertEqual(ev["country"], "cn")
        self.assertEqual(ev["country_name"], "중국")
        self.assertEqual(ev["flag"], "🇨🇳")
        self.assertEqual(ev["datetime"], "2026-06-03 10:45:00")
        self.assertEqual(ev["date"], "2026-06-03")
        self.assertEqual(ev["time"], "10:45")
        self.assertEqual(ev["importance"], "mid")
        self.assertEqual(ev["importance_label"], "중")
        self.assertEqual(ev["actual"], "54.4")
        self.assertEqual(ev["forecast"], "52.3")
        self.assertEqual(ev["previous"], "52.6")
        self.assertIn("PMI", ev["event"])

    def test_importance_mapping_and_event_strip(self):
        events = economic_calendar._parse_calendar(_fixture())
        # importance_class → level/label 매핑이 세 단계 모두 등장.
        levels = {e["importance"] for e in events}
        self.assertSetEqual(levels, {"high", "mid", "low"})
        kr = [e for e in events if e["country"] == "kr"]
        self.assertTrue(kr)
        self.assertEqual(kr[0]["event"], "현충일")  # 후행 공백 제거
        self.assertEqual(kr[0]["importance"], "low")
        self.assertEqual(kr[0]["importance_label"], "하")

    def test_parse_handles_empty_and_non_dict(self):
        self.assertEqual(economic_calendar._parse_calendar({}), [])
        self.assertEqual(economic_calendar._parse_calendar(None), [])
        self.assertEqual(economic_calendar._parse_calendar([1, 2, 3]), [])


class NationParamTests(unittest.TestCase):
    def test_build_nation_params_pairs_en_ko_in_order(self):
        nation, natcd = economic_calendar.build_nation_params(["kr", "us"])
        self.assertEqual(nation, "South Korea|대한민국|United States|미국")
        self.assertEqual(natcd, "kr|us")

    def test_unknown_codes_dropped(self):
        nation, natcd = economic_calendar.build_nation_params(["kr", "zz", "eu"])
        self.assertEqual(nation, "South Korea|대한민국|Euro Area|유럽연합")
        self.assertEqual(natcd, "kr|eu")


class CalendarEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_range_is_today_through_plus_six(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_economic_calendar", new=fake):
            await stocks_route.get_economic_calendar()
        kw = fake.await_args.kwargs
        today = date.today()
        self.assertEqual(kw["start_date"], today.isoformat())
        self.assertEqual(kw["end_date"], (today + timedelta(days=6)).isoformat())

    async def test_csv_params_are_parsed_and_lowercased(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_economic_calendar", new=fake):
            await stocks_route.get_economic_calendar(
                start="2026-06-01", end="2026-06-07",
                countries="KR, us ,EU", importance="HIGH,mid",
            )
        kw = fake.await_args.kwargs
        self.assertEqual(kw["start_date"], "2026-06-01")
        self.assertEqual(kw["end_date"], "2026-06-07")
        self.assertEqual(kw["countries"], ["kr", "us", "eu"])
        self.assertEqual(kw["importance"], ["high", "mid"])

    async def test_reversed_dates_are_swapped(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_economic_calendar", new=fake):
            await stocks_route.get_economic_calendar(start="2026-06-10", end="2026-06-01")
        kw = fake.await_args.kwargs
        self.assertEqual(kw["start_date"], "2026-06-01")
        self.assertEqual(kw["end_date"], "2026-06-10")

    async def test_oversized_span_is_clamped_to_62_days(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_economic_calendar", new=fake):
            await stocks_route.get_economic_calendar(start="2026-01-01", end="2026-12-31")
        kw = fake.await_args.kwargs
        self.assertEqual(kw["start_date"], "2026-01-01")
        self.assertEqual(kw["end_date"], (date(2026, 1, 1) + timedelta(days=62)).isoformat())


if __name__ == "__main__":
    unittest.main()
