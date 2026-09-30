"""external_tools × 생태계 레지스트리/summary.json — 소스와 무관하게 같은 응답 모양, 캐시, 폴백."""

import json
import os
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import external_tools
from services.ecosystem import adapters, siblings
from services.portfolio import spac

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ecosystem"


def summary(tool_id: str) -> dict:
    return json.loads((FIXTURES / f"{tool_id}.summary.json").read_text(encoding="utf-8"))


def data(tool_id: str) -> dict:
    return summary(tool_id)["data"]


# ---------------------------------------------------------------------------
# 같은 내용의 레거시 파일(current.json/config.json/data.json …) — 형제가 지금 발행하는 모양
# ---------------------------------------------------------------------------

def legacy_files() -> dict[str, object]:
    hv = data("holding_value")
    cps = data("common_preferred_spread")
    gold = data("gold_gap")
    sp = data("spac-hunter")
    nps = data("nps-tracker")
    bond = summary("bond-mate")
    bb = data("buybacks")
    etf = data("eiayn")
    files: dict[str, object] = {}
    files["holding_value/current"] = {
        "lastUpdated": hv["lastUpdated"],
        "summary": {"averageRatio": hv["averageRatio"], "pairCount": hv["pairCount"]},
        "pairs": [{"id": p["id"], "ratio": p["ratio"], "ratioChange": p["ratioChange"],
                   "holdingValue": p["holdingValue"], "marketCap": p["marketCap"], "quoteSource": "kis"}
                  for p in hv["pairs"]],
    }
    files["holding_value/config"] = [
        {"id": p["id"], "name": p["name"], "holdingName": p["holdingName"], "holdingTicker": p["code"] + ".KS",
         "subsidiaries": []}
        for p in hv["pairs"]
    ]
    files["common_preferred_spread/current"] = {
        "lastUpdated": cps["lastUpdated"], "averageSpread": cps["averageSpread"],
        "averageSpreadChange": cps["averageSpreadChange"],
        "prices": {p["id"]: {"spread": p["spread"], "spreadChange": p["spreadChange"],
                             "commonPrice": p["commonPrice"], "preferredPrice": p["preferredPrice"],
                             "date": p["date"]} for p in cps["pairs"]},
    }
    files["common_preferred_spread/config"] = [
        {"id": p["id"], "name": p["name"], "commonTicker": p["commonCode"] + ".KS",
         "preferredTicker": p["preferredCode"] + ".KS", "preferredName": p["preferredName"]}
        for p in cps["pairs"]
    ]
    files["gold_gap/data"] = {
        "updated_at": gold["updatedAt"],
        **{a["key"]: {"gap_pct": [0.01, a["gap"]], "dates": ["2026-01-01", a["date"]]} for a in gold["assets"]},
    }
    files["spac-hunter/current"] = {
        "lastUpdated": sp["lastUpdated"], "summary": sp["summary"],
        "prices": {s["code"]: {"name": s["name"], "currentPrice": s["currentPrice"], "ipoPrice": s["ipoPrice"],
                               "annualizedReturn": s["annualizedReturn"], "ratio": s["ratio"]} for s in sp["spacs"]},
    }
    files["spac-hunter/data"] = {"spacs": sp["spacs"], "valuationAssumptions": sp["valuationAssumptions"],
                                 "lastUpdated": sp["lastUpdated"]}
    files["nps-tracker/current"] = {
        "lastUpdated": nps["lastUpdated"], "asOf": nps["summary"]["asOf"], "summary": nps["summary"],
        "allocation": nps["allocation"],
        "holdings": [{"stock_code": h["code"], "stock_name": h["name"], "weight": h["weight"],
                      "market_value": h["marketValue"], "change_pct": h["changePct"]} for h in nps["top"]],
    }
    bd = bond["data"]
    hl = bd["highlights"]
    off = bd["latestOffering"]
    files["bond-mate/current"] = {
        "generated_at": adapters._utc_text(bond["asOf"]),
        "rates": {k: {"value": v["value"], "change": v["change"], "change_pct": v["changePct"], "date": v["date"],
                      "history": [1, 2, 3]} for k, v in bd["rates"].items()},
        "fx": {k: {"value": v["value"], "change": v["change"], "change_pct": v["changePct"], "date": v["date"]}
               for k, v in bd["fx"].items()},
        "credit": {r: {"oas": {"value": bp / 100}} for r, bp in bd["creditSpreadBp"].items()},
        "highlights": {"us_curve_spread_bp": hl["usCurveSpreadBp"], "us_curve_inverted": hl["usCurveInverted"],
                       "kr_curve_spread_bp": hl["krCurveSpreadBp"], "ig_hy_spread_bp": hl["igHySpreadBp"],
                       "latest_offering": {"issuer": off["issuer"], "issuer_name": off["issuerName"],
                                           "filing_date": off["filingDate"], "total_amount": off["totalAmount"],
                                           "tranches": off["tranches"]}},
    }
    files["buybacks/holding-snapshots"] = [
        {"stock_code": r["code"], "corp_name": r["name"], "stock_kind": r["stockKind"], "as_of_date": r["asOf"],
         "treasury_ratio": r["treasuryRatioPct"] / 100, "ending_qty": r["endingQty"],
         "issued_shares": r["issuedShares"], "report_code": "11011"}
        for r in bb["top"]
    ] + [  # 더 오래된 스냅샷은 최신 선택 규칙에서 밀린다
        {"stock_code": bb["top"][0]["code"], "corp_name": bb["top"][0]["name"], "stock_kind": "보통주",
         "as_of_date": "2020-12-31", "treasury_ratio": 0.9},
    ]
    files["eiayn/rankings"] = {
        "etfs": [{"rank": r["rank"], "shortName": r["name"], "ticker": r["code"], "aiynScore": r["score"],
                  "market": r["market"], "link": external_tools.etf_deep_link(r["code"])} for r in etf["rankings"]],
        "count": len(etf["rankings"]),
        "generatedAt": summary("eiayn")["asOf"],
    }
    return files


def legacy_url_map() -> dict[str, object]:
    files = legacy_files()
    return {siblings.data_url(*key.split("/", 1)): value for key, value in files.items()}


def shape(value):
    if isinstance(value, dict):
        return {k: shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [shape(value[0])] if value else []
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "num"
    if value is None:
        return "null"
    return type(value).__name__


def reset_caches():
    external_tools._cache.clear()
    external_tools._raw_cache.clear()
    external_tools._etf_universe_cache.clear()
    siblings.reset()


class _Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_caches()
        self.urls = legacy_url_map()
        self.legacy_calls: list[str] = []

    def tearDown(self):
        reset_caches()

    async def fake_get_json(self, url):
        self.legacy_calls.append(url)
        if url not in self.urls:
            raise httpx.HTTPStatusError("404", request=httpx.Request("GET", url), response=httpx.Response(404))
        return json.loads(json.dumps(self.urls[url]))

    def patches(self, *, summaries: bool, summary_http=None):
        async def universe():
            self.legacy_calls.append("etfs.json")
            return {"VOO", "069500"}

        env = patch.dict(os.environ, {"ECOSYSTEM_SUMMARIES": "1" if summaries else "0"})
        stack = [
            env,
            patch.object(external_tools, "_get_json", new=AsyncMock(side_effect=self.fake_get_json)),
            patch.object(external_tools, "_legacy_etf_universe", new=AsyncMock(side_effect=universe)),
            patch.object(external_tools, "_fill_etf_changes", new=AsyncMock()),
        ]
        if summary_http is not None:
            stack.append(patch.object(siblings, "_http_get", new=AsyncMock(side_effect=summary_http)))
        return stack

    async def run_with(self, stack, coro_factory):
        for p in stack:
            p.start()
        try:
            return await coro_factory()
        finally:
            for p in reversed(stack):
                p.stop()


async def all_summaries(url, etag):
    tool_id = next(t["id"] for t in siblings.ecosystem.tools() if t.get("url") and url == siblings.summary_url(t["id"]))
    return 200, summary(tool_id), f'"{tool_id}"'


async def no_summaries(url, etag):
    return 404, None, None


INSIGHT_KEYS = {"holding", "spread", "goldGap", "spac", "nps", "etfPicks", "buybacks", "bondMate"}


class InsightsSourceParityTests(_Base):
    async def test_summary_and_legacy_paths_render_the_same_cards(self):
        from_summary = await self.run_with(
            self.patches(summaries=True, summary_http=all_summaries), external_tools.fetch_external_insights)
        self.assertEqual(self.legacy_calls, [])  # 레거시 파일은 하나도 받지 않았다
        reset_caches()
        from_legacy = await self.run_with(self.patches(summaries=False), external_tools.fetch_external_insights)

        self.assertEqual(set(from_summary), INSIGHT_KEYS)
        self.assertEqual(set(from_legacy), INSIGHT_KEYS)
        for key in INSIGHT_KEYS:
            with self.subTest(key=key):
                self.assertEqual(shape(from_summary[key]), shape(from_legacy[key]))
        for key in ("holding", "spread", "goldGap", "spac", "nps", "etfPicks", "bondMate"):
            with self.subTest(key=key):
                self.assertEqual(from_summary[key], from_legacy[key])
        # buybacks: summary 의 count 는 전체 ratios 수(레거시 fixture 는 top 만 가짐), 비율은 부동소수 오차.
        bs, bl = from_summary["buybacks"], from_legacy["buybacks"]
        self.assertEqual([r["code"] for r in bs["top"]], [r["code"] for r in bl["top"]])
        for rs, rl in zip(bs["top"], bl["top"]):
            self.assertAlmostEqual(rs["treasuryRatioPct"], rl["treasuryRatioPct"], places=6)
            self.assertEqual(rs["name"], rl["name"])
        self.assertEqual((bs["asOf"], bs["url"]), (bl["asOf"], bl["url"]))
        self.assertEqual(bs["count"], data("buybacks")["count"])

    async def test_stock_links_and_spac_data_have_the_same_shape_from_either_source(self):
        async def run():
            return (await external_tools.fetch_stock_links("005935"),
                    await external_tools.fetch_stock_links("000670"),
                    await external_tools.fetch_spac_data())

        s_pref, s_hold, s_spac = await self.run_with(
            self.patches(summaries=True, summary_http=all_summaries), run)
        reset_caches()
        l_pref, l_hold, l_spac = await self.run_with(self.patches(summaries=False), run)
        self.assertEqual(s_pref, l_pref)
        self.assertEqual(s_hold, l_hold)
        self.assertIn("preferred", s_pref)
        self.assertIn("holding", s_hold)
        self.assertEqual(s_spac, l_spac)
        self.assertEqual(s_spac, adapters.spac_valuation(data("spac-hunter")))


class SummaryFallbackTests(_Base):
    async def test_summary_404_falls_back_per_tool(self):
        async def only_gold(url, etag):
            if url == siblings.summary_url("gold_gap"):
                return 200, summary("gold_gap"), None
            return 404, None, None

        out = await self.run_with(self.patches(summaries=True, summary_http=only_gold),
                                  external_tools.fetch_external_insights)
        self.assertEqual(set(out), INSIGHT_KEYS)
        self.assertNotIn(siblings.data_url("gold_gap", "data"), self.legacy_calls)
        self.assertIn(siblings.data_url("holding_value", "current"), self.legacy_calls)
        fresh = siblings.freshness(["gold_gap", "holding_value", "buybacks"])
        self.assertEqual(fresh["gold_gap"]["source"], "summary")
        self.assertEqual(fresh["gold_gap"]["asOf"], summary("gold_gap")["asOf"])
        self.assertEqual(fresh["holding_value"]["source"], "legacy")
        self.assertIsNone(fresh["holding_value"]["asOf"])

    async def test_invalid_summary_falls_back_to_legacy(self):
        async def tampered(url, etag):
            env = summary("holding_value")
            env["data"]["averageRatio"] = 1  # 해시 불일치
            return (200, env, None) if url == siblings.summary_url("holding_value") else (404, None, None)

        out = await self.run_with(self.patches(summaries=True, summary_http=tampered),
                                  external_tools._holding_summary)
        self.assertEqual(out["averageRatio"], data("holding_value")["averageRatio"])
        self.assertIn(siblings.data_url("holding_value", "current"), self.legacy_calls)

    async def test_both_fail_serves_stale_or_empty(self):
        async def down(url, etag):
            raise httpx.ConnectError("offline")

        self.urls = {}  # 레거시도 전부 실패
        out = await self.run_with(self.patches(summaries=True, summary_http=down),
                                  external_tools.fetch_external_insights)
        self.assertEqual(out, {})
        spac_data = await self.run_with(self.patches(summaries=True, summary_http=down),
                                        external_tools.fetch_spac_data)
        self.assertEqual(spac_data, {})

        # 마지막 성공값(만료됨)이 있으면 그걸 쓴다.
        external_tools._raw_cache.set("gold_gap/latest", adapters.gold_data(data("gold_gap")), ttl_seconds=-1)
        gold = await self.run_with(self.patches(summaries=True, summary_http=down), external_tools._gold_summary)
        self.assertEqual([a["key"] for a in gold["assets"]], ["gold", "bitcoin", "eth", "usdt"])


class SiblingCachingTests(_Base):
    async def test_action_board_fetches_each_sibling_file_once_within_ttl(self):
        async def run():
            codes = ["005930", "000670", "VOO", "KRX_GOLD", data("buybacks")["top"][0]["code"]]
            first = await external_tools.fetch_portfolio_signals(codes)
            second = await external_tools.fetch_portfolio_signals(codes)
            await external_tools.fetch_stock_links("005930")
            await external_tools.fetch_external_insights()
            return first, second

        first, second = await self.run_with(self.patches(summaries=False), run)
        self.assertEqual(first, second)
        self.assertEqual(len(self.legacy_calls), len(set(self.legacy_calls)), self.legacy_calls)
        for key in ("buybacks/holding-snapshots", "gold_gap/data", "holding_value/current",
                    "common_preferred_spread/config"):
            self.assertIn(siblings.data_url(*key.split("/", 1)), self.legacy_calls)
        kinds = {code: {s["kind"] for s in sigs} for code, sigs in first.items()}
        self.assertEqual(kinds["005930"], {"preferred"})
        self.assertEqual(kinds["KRX_GOLD"], {"goldGap"})
        self.assertIn("buybacks", kinds[data("buybacks")["top"][0]["code"]])

    async def test_large_files_are_cached_slim(self):
        await self.run_with(self.patches(summaries=False), external_tools._gold_summary)
        cached = external_tools._raw_cache.get("gold_gap/latest")
        self.assertEqual(cached["gold"]["gap_pct"], [data("gold_gap")["assets"][0]["gap"]])
        self.assertEqual(external_tools.peek_gold_latest(), cached)
        await self.run_with(self.patches(summaries=False), external_tools._buybacks_summary)
        self.assertEqual(set(external_tools._raw_cache.get("buybacks/index")), {"card", "byCode"})

    async def test_env_override_moves_site_links(self):
        with patch.dict(os.environ, {"SPAC_HUNTER_BASE_URL": "https://mirror.example/spac/"}):
            self.assertEqual(external_tools.SITE["spac"], "https://mirror.example/spac/")
            self.assertEqual(external_tools._summarize_spac({"prices": {}})["url"], "https://mirror.example/spac/")
        self.assertEqual(external_tools.SITE["spac"], "https://ducklove.github.io/spac-hunter/")
        self.assertEqual(set(external_tools.SITE), set(external_tools._SITE_TOOLS))


class BuybackSummaryNameTests(_Base):
    async def test_summary_ratios_without_names_use_hub_stock_names(self):
        bb = data("buybacks")
        named = {r["code"] for r in bb["top"]}
        code = next(c for c in bb["ratios"] if c not in named)

        async def run():
            with patch("repositories.corp_codes.get_corp_name", new=AsyncMock(return_value="허브종목명")) as lookup:
                out = await external_tools.fetch_portfolio_signals([code])
            return out, lookup

        out, lookup = await self.run_with(self.patches(summaries=True, summary_http=all_summaries), run)
        signal = next(s for s in out[code] if s["kind"] == "buybacks")
        self.assertEqual(signal["title"], "허브종목명 자사주")
        self.assertAlmostEqual(signal["metric"], bb["ratios"][code])
        lookup.assert_awaited_once_with(code)
        as_of = bb["ratioAsOf"].get(code, bb["asOf"])
        self.assertIn(f"{as_of} 기준", signal["detail"])


class SpacPipelineValueTests(unittest.TestCase):
    def setUp(self):
        self.item = dict(data("spac-hunter")["spacs"][0])
        self.context = {"applicable": True, "item": self.item, "assumptions": {}, "updatedAt": None}

    def test_pipeline_value_for_the_same_day_wins(self):
        as_of = date(2026, 9, 29)
        port = spac.build_spac_insight(self.context, {"price": 1900}, as_of=as_of)["currentLiquidationValue"]
        self.item["currentLiquidationValue"] = 2001.5
        self.item["currentLiquidationValueAsOf"] = "2026-09-29"
        result = spac.build_spac_insight(self.context, {"price": 1900}, as_of=as_of)
        self.assertEqual(result["currentLiquidationValue"], 2001.5)
        self.assertNotEqual(port, 2001.5)
        self.assertEqual(result["liquidationDiscountPct"], round((2001.5 - 1900) / 2001.5 * 100, 2))

    def test_other_day_or_invalid_pipeline_value_uses_python_port(self):
        as_of = date(2026, 9, 29)
        port = spac.build_spac_insight(self.context, {"price": 1900}, as_of=as_of)["currentLiquidationValue"]
        for value, day in ((2001.5, "2026-09-28"), (-1, "2026-09-29"), (None, "2026-09-29"), ("x", "2026-09-29")):
            self.item["currentLiquidationValue"] = value
            self.item["currentLiquidationValueAsOf"] = day
            self.assertEqual(
                spac.build_spac_insight(self.context, {"price": 1900}, as_of=as_of)["currentLiquidationValue"], port)


if __name__ == "__main__":
    unittest.main()
