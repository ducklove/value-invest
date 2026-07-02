import json
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from starlette.requests import Request

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


def _req(query: str) -> Request:
    return Request({"type": "http", "method": "GET", "query_string": query.encode(), "headers": []})


class CalendarEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_selection_when_no_level_params(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_calendar_by_level", new=fake):
            await stocks_route.get_economic_calendar(_req(""))
        kw = fake.await_args.kwargs
        today = date.today()
        self.assertEqual(kw["start_date"], today.isoformat())
        self.assertEqual(kw["end_date"], (today + timedelta(days=6)).isoformat())
        self.assertEqual(kw["selection"], {"high": "all", "mid": ["kr"], "low": ["kr"]})

    async def test_per_level_country_selection_parsed(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_calendar_by_level", new=fake):
            await stocks_route.get_economic_calendar(
                _req("start=2026-06-01&end=2026-06-07&high=all&mid=KR,us&low="),
                start="2026-06-01", end="2026-06-07", high="all", mid="KR,us", low="",
            )
        kw = fake.await_args.kwargs
        self.assertEqual(kw["start_date"], "2026-06-01")
        self.assertEqual(kw["end_date"], "2026-06-07")
        # high='all', mid 소문자화, low 는 빈 값(쿼리에 있으나)이라 비활성→생략.
        self.assertEqual(kw["selection"], {"high": "all", "mid": ["kr", "us"]})

    async def test_all_levels_empty_selection_is_respected(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_calendar_by_level", new=fake):
            await stocks_route.get_economic_calendar(
                _req("high=&mid=&low="), high="", mid="", low="",
            )
        # 명시적으로 모두 비웠으면 기본값이 아니라 빈 선택을 전달.
        self.assertEqual(fake.await_args.kwargs["selection"], {})

    async def test_reversed_dates_are_swapped(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_calendar_by_level", new=fake):
            await stocks_route.get_economic_calendar(_req(""), start="2026-06-10", end="2026-06-01")
        kw = fake.await_args.kwargs
        self.assertEqual(kw["start_date"], "2026-06-01")
        self.assertEqual(kw["end_date"], "2026-06-10")

    async def test_oversized_span_is_clamped_to_62_days(self):
        fake = AsyncMock(return_value={"events": []})
        with patch.object(economic_calendar, "fetch_calendar_by_level", new=fake):
            await stocks_route.get_economic_calendar(_req(""), start="2026-01-01", end="2026-12-31")
        kw = fake.await_args.kwargs
        self.assertEqual(kw["start_date"], "2026-01-01")
        self.assertEqual(kw["end_date"], (date(2026, 1, 1) + timedelta(days=62)).isoformat())


class FetchByLevelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        economic_calendar._cache.clear()

    async def test_fetch_calendar_uses_shared_http_client(self):
        class Resp:
            content = json.dumps(_fixture(), ensure_ascii=False).encode("utf-8")

            def raise_for_status(self):
                pass

        class Client:
            def __init__(self):
                self.calls = []

            async def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                return Resp()

        client = Client()
        fake_get_client = AsyncMock(return_value=client)
        economic_calendar._cache.clear()
        with patch.object(economic_calendar, "get_http_client", new=fake_get_client):
            result = await economic_calendar.fetch_economic_calendar(
                start_date="2026-06-01",
                end_date="2026-06-07",
                countries=["kr", "us"],
                importance=["high"],
            )

        self.assertTrue(result["events"])
        fake_get_client.assert_awaited_once_with("economic_calendar")
        self.assertEqual(client.calls[0][0], economic_calendar._DATA_URL)
        self.assertEqual(client.calls[0][1]["headers"]["Referer"], economic_calendar._REFERER)
        self.assertIs(client.calls[0][1]["timeout"], economic_calendar._HTTP_TIMEOUT)
        self.assertEqual(client.calls[0][1]["params"]["str_natcd"], "kr|us")

    @staticmethod
    async def _fake_fetch(*, start_date, end_date, countries, importance):
        # (country, level) 조합마다 한 건씩 — 그룹핑/병합 검증용.
        evs = []
        for c in countries:
            for lvl in importance:
                evs.append({
                    "index_id": f"{c}-{lvl}", "datetime": f"2026-06-01 {len(evs):04d}",
                    "country": c, "importance": lvl, "event": f"{c} {lvl}",
                })
        return {"events": evs}

    async def test_distinct_country_sets_fetched_separately(self):
        fake = AsyncMock(side_effect=self._fake_fetch)
        with patch.object(economic_calendar, "fetch_economic_calendar", new=fake):
            result = await economic_calendar.fetch_calendar_by_level(
                start_date="2026-06-01", end_date="2026-06-07",
                selection={"high": "all", "mid": ["kr"]},
            )
        ids = {e["index_id"] for e in result["events"]}
        self.assertEqual(fake.await_count, 2)        # all-국가(high) + kr(mid)
        self.assertIn("us-high", ids)                # high 는 전체 국가
        self.assertIn("kr-mid", ids)                 # mid 는 한국만
        self.assertNotIn("us-mid", ids)              # mid 비한국은 안 가져옴

    async def test_levels_sharing_country_set_use_one_fetch(self):
        fake = AsyncMock(side_effect=self._fake_fetch)
        with patch.object(economic_calendar, "fetch_economic_calendar", new=fake):
            await economic_calendar.fetch_calendar_by_level(
                start_date="2026-06-01", end_date="2026-06-07",
                selection={"high": ["kr"], "mid": ["kr"]},
            )
        self.assertEqual(fake.await_count, 1)
        self.assertEqual(set(fake.await_args.kwargs["importance"]), {"high", "mid"})

    async def test_empty_selection_fetches_nothing(self):
        fake = AsyncMock(side_effect=self._fake_fetch)
        with patch.object(economic_calendar, "fetch_economic_calendar", new=fake):
            result = await economic_calendar.fetch_calendar_by_level(
                start_date="2026-06-01", end_date="2026-06-07", selection={},
            )
        self.assertEqual(result["events"], [])
        self.assertEqual(fake.await_count, 0)


if __name__ == "__main__":
    unittest.main()
