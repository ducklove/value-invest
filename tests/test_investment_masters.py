"""투자 대가 전략 카탈로그·시뮬레이션 테스트.

- 카탈로그(data/investment_masters.json) 스키마·비중 합 검증: 새 대가를
  JSON 에 추가할 때 오타·비중 오류가 여기서 먼저 터진다.
- personalize 의 결정론적 조정 로직(성향 이동·자산군 제외·반올림 보존).
- HTTP 계층: create_app + httpx ASGITransport 통합 (인증 없이 공개 접근).
"""

import itertools
import unittest

import httpx

from core.app_factory import create_app
from core.config import PROJECT_ROOT, AppSettings
from services import investment_masters as im


def _equity_total(result: dict) -> float:
    return sum(r["weight"] for r in (result["allocation"] or []) if r["group"] == "equity")


def _allocation_sum(result: dict) -> float:
    return sum(r["weight"] for r in (result["allocation"] or []))


class CatalogTests(unittest.TestCase):
    def test_catalog_loads_and_passes_schema_validation(self):
        catalog = im.load_catalog()
        self.assertGreaterEqual(len(catalog["strategies"]), 4)
        self.assertIn("참고용", catalog["disclaimer"])
        self.assertIn("투자 조언이 아닌", catalog["disclaimer"])

    def test_catalog_includes_flagship_masters(self):
        ids = {s["id"] for s in im.load_catalog()["strategies"]}
        self.assertLessEqual({"buffett", "graham", "lynch", "dalio"}, ids)

    def test_every_strategy_base_allocation_sums_to_100(self):
        for s in im.load_catalog()["strategies"]:
            total = sum(row["weight"] for row in s["base_allocation"])
            self.assertAlmostEqual(total, 100.0, places=2, msg=s["id"])

    def test_validation_rejects_bad_weight_sum(self):
        broken = {
            "disclaimer": "참고용",
            "asset_groups": {"equity": {"label": "주식"}},
            "asset_classes": {"equity_kr": {"label": "국내 주식", "group": "equity"}},
            "profile_options": {
                "risk": [{"id": "balanced", "label": "중립형"}],
                "horizon": [{"id": "long", "label": "장기"}],
            },
            "personalization": {
                "risk_equity_shift_pt": {"balanced": 0},
                "horizon_equity_shift_pt": {"long": 0},
                "shift_min_pt": -25,
                "shift_max_pt": 15,
                "defensive_floor_pt": 5,
            },
            "strategies": [
                {
                    "id": "x", "master": "m", "title": "t", "tagline": "g", "summary": "s",
                    "allocation_basis": "b",
                    "principles": ["p"], "pros": ["p"], "cons": ["c"], "references": ["r"],
                    "fit": {"risk": ["balanced"], "horizon": ["long"], "description": "d"},
                    "risk_level": 3, "effort_level": 3,
                    "rebalancing": {"frequency": "연 1회", "ideas": ["i"]},
                    "base_allocation": [{"asset": "equity_kr", "weight": 90}],
                }
            ],
        }
        with self.assertRaises(im.MastersCatalogError):
            im._validate_catalog(broken)


class PersonalizeTests(unittest.TestCase):
    def _base_equity(self, strategy_id: str) -> float:
        catalog = im.load_catalog()
        strategy = next(s for s in catalog["strategies"] if s["id"] == strategy_id)
        return sum(
            row["weight"] for row in strategy["base_allocation"]
            if catalog["asset_classes"][row["asset"]]["group"] == "equity"
        )

    def _result_for(self, payload: dict, strategy_id: str) -> dict:
        out = im.personalize(payload)
        return next(r for r in out["results"] if r["strategy_id"] == strategy_id)

    def test_conservative_short_profile_reduces_equity_weight(self):
        result = self._result_for({"risk": "conservative", "horizon": "short"}, "lynch")
        self.assertLess(_equity_total(result), self._base_equity("lynch"))
        self.assertTrue(any("주식 비중" in a for a in result["adjustments"]))

    def test_aggressive_long_profile_raises_equity_weight(self):
        result = self._result_for({"risk": "aggressive", "horizon": "long"}, "graham")
        self.assertGreater(_equity_total(result), self._base_equity("graham"))

    def test_balanced_mid_profile_keeps_base_allocation(self):
        result = self._result_for({"risk": "balanced", "horizon": "mid"}, "dalio")
        self.assertEqual(result["adjustments"], [])
        self.assertAlmostEqual(_equity_total(result), self._base_equity("dalio"), places=1)

    def test_excluded_asset_group_is_redistributed(self):
        result = self._result_for(
            {"risk": "balanced", "horizon": "mid", "asset_groups": ["equity", "bond", "cash"]},
            "dalio",
        )
        assets = {r["asset"] for r in result["allocation"]}
        self.assertNotIn("gold", assets)
        self.assertNotIn("commodities", assets)
        self.assertAlmostEqual(_allocation_sum(result), 100.0, places=6)
        self.assertTrue(any("비례 배분" in a for a in result["adjustments"]))

    def test_all_assets_excluded_returns_note_instead_of_allocation(self):
        # 버핏 예시 배분은 주식+현금성뿐 — 대체자산/채권만 선호하면 구성 불가.
        result = self._result_for(
            {"risk": "balanced", "horizon": "mid", "asset_groups": ["alternative"]},
            "buffett",
        )
        self.assertIsNone(result["allocation"])
        self.assertTrue(result["note"])

    def test_every_profile_combination_preserves_weight_sum(self):
        catalog = im.load_catalog()
        risks = [o["id"] for o in catalog["profile_options"]["risk"]]
        horizons = [o["id"] for o in catalog["profile_options"]["horizon"]]
        for risk, horizon in itertools.product(risks, horizons):
            out = im.personalize({"risk": risk, "horizon": horizon})
            for result in out["results"]:
                if result["allocation"] is None:
                    continue
                self.assertAlmostEqual(
                    _allocation_sum(result), 100.0, places=6,
                    msg=f"{result['strategy_id']} @ {risk}/{horizon}",
                )
                for row in result["allocation"]:
                    self.assertGreater(row["weight"], 0)

    def test_results_are_sorted_by_fit_score_desc(self):
        out = im.personalize({"risk": "conservative", "horizon": "long"})
        scores = [r["fit_score"] for r in out["results"]]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_response_always_carries_disclaimer(self):
        out = im.personalize({"risk": "balanced", "horizon": "long"})
        self.assertIn("참고용", out["disclaimer"])

    def test_invalid_profile_values_raise_masters_error(self):
        for bad in (
            {"risk": "yolo", "horizon": "long"},
            {"risk": "balanced", "horizon": "eternal"},
            {"risk": "balanced", "horizon": "long", "asset_groups": []},
            {"risk": "balanced", "horizon": "long", "asset_groups": ["crypto"]},
        ):
            with self.assertRaises(im.MastersError, msg=bad):
                im.personalize(bad)


class MastersApiTests(unittest.IsolatedAsyncioTestCase):
    def _settings(self) -> AppSettings:
        return AppSettings(
            environment="production",
            project_root=PROJECT_ROOT,
            app_title="Test Compass",
            public_api_base_url="https://api.example.test",
            cors_allowed_origins=("https://app.example.test",),
            enable_docs=False,
        )

    async def _client(self):
        app = create_app(self._settings())
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        return httpx.AsyncClient(transport=transport, base_url="https://testserver")

    async def test_strategies_endpoint_returns_catalog(self):
        async with await self._client() as client:
            resp = await client.get("/api/masters/strategies")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertGreaterEqual(len(data["strategies"]), 4)
        self.assertIn("참고용", data["disclaimer"])
        self.assertIn("asset_classes", data)
        self.assertIn("profile_options", data)

    async def test_simulate_endpoint_personalizes_and_keeps_disclaimer(self):
        async with await self._client() as client:
            resp = await client.post(
                "/api/masters/simulate",
                json={"profile": {"risk": "conservative", "horizon": "short"}},
            )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("참고용", data["disclaimer"])
        scores = [r["fit_score"] for r in data["results"]]
        self.assertEqual(scores, sorted(scores, reverse=True))
        for result in data["results"]:
            if result["allocation"] is not None:
                self.assertAlmostEqual(
                    sum(r["weight"] for r in result["allocation"]), 100.0, places=6,
                )

    async def test_simulate_rejects_invalid_profile_with_400(self):
        async with await self._client() as client:
            resp = await client.post(
                "/api/masters/simulate",
                json={"profile": {"risk": "yolo", "horizon": "long"}},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("risk", resp.json()["detail"])

    async def test_masters_spa_path_serves_index_html(self):
        async with await self._client() as client:
            resp = await client.get("/masters")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("text/html", resp.headers["content-type"])
        self.assertIn('id="mastersView"', resp.text)


if __name__ == "__main__":
    unittest.main()
