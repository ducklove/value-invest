from __future__ import annotations

import unittest

from _harness import TempDbMixin, seed_user
from fastapi import HTTPException
from fastapi.testclient import TestClient

from core.app_factory import create_app
from repositories import household_assets as household_repo
from routes.household_assets import normalize_payload
from services.household_assets import RETIREMENT_REFERENCE, WEALTH_DISTRIBUTION, reference_payload

SUB = "household-user"


class HouseholdAssetsRepositoryTests(TempDbMixin):
    async def seed(self):
        await seed_user(sub=SUB)

    async def test_replace_round_trip_is_atomic_and_preserves_types(self):
        items = [
            {
                "asset_id": "home-1",
                "category": "real_estate",
                "name": "거주 주택",
                "owner": "household",
                "amount": 900_000_000,
                "retirement_eligible": False,
            },
            {
                "asset_id": "loan-1",
                "category": "liability",
                "name": "주택담보대출",
                "owner": "household",
                "amount": 220_000_000,
                "retirement_eligible": True,
            },
        ]
        profile = {
            "household_type": "couple",
            "current_age": 46,
            "retirement_age": 65,
            "plan_to_age": 92,
            "monthly_spending": 3_200_000,
            "monthly_public_pension": 1_500_000,
            "monthly_other_income": 300_000,
            "monthly_contribution": 1_000_000,
            "annual_return_pct": 4.2,
            "inflation_pct": 2.0,
        }

        saved = await household_repo.replace_household_plan(SUB, items, profile)

        self.assertEqual([row["asset_id"] for row in saved["items"]], ["home-1", "loan-1"])
        self.assertIs(saved["items"][0]["retirement_eligible"], False)
        self.assertIs(saved["items"][1]["retirement_eligible"], True)
        self.assertEqual(saved["retirement"]["monthly_spending"], 3_200_000)
        self.assertEqual(saved["retirement"]["annual_return_pct"], 4.2)
        self.assertIsNotNone(saved["updated_at"])

        replacement = [
            {
                "asset_id": "cash-1",
                "category": "cash",
                "name": "비상금",
                "owner": "self",
                "amount": 50_000_000,
                "retirement_eligible": True,
            }
        ]
        replaced = await household_repo.replace_household_plan(SUB, replacement, profile)
        self.assertEqual([row["asset_id"] for row in replaced["items"]], ["cash-1"])

    async def test_unsaved_user_gets_latest_retirement_defaults(self):
        data = await household_repo.get_household_plan(SUB)
        self.assertEqual(data["items"], [])
        self.assertEqual(data["retirement"]["household_type"], "couple")
        self.assertEqual(data["retirement"]["monthly_spending"], 2_981_000)


class HouseholdAssetsValidationTests(unittest.TestCase):
    def test_normalize_payload_accepts_household_assets_and_profile(self):
        items, profile = normalize_payload({
            "items": [{
                "id": "draft-1",
                "category": "cash",
                "name": "여유자금",
                "owner": "spouse",
                "amount": "120000000",
                "retirement_eligible": True,
            }],
            "retirement": {
                "household_type": "single",
                "current_age": 41,
                "retirement_age": 65,
                "plan_to_age": 90,
            },
        })
        self.assertEqual(items[0]["asset_id"], "draft-1")
        self.assertEqual(items[0]["amount"], 120_000_000)
        self.assertEqual(profile["household_type"], "single")
        self.assertEqual(profile["monthly_spending"], 2_981_000)

    def test_normalize_payload_rejects_bad_category_and_age_window(self):
        with self.assertRaises(HTTPException):
            normalize_payload({"items": [{"category": "crypto-wallet", "name": "x", "amount": 1}]})
        with self.assertRaises(HTTPException):
            normalize_payload({
                "items": [],
                "retirement": {"current_age": 90, "retirement_age": 65, "plan_to_age": 90},
            })

    def test_reference_payload_uses_2025_official_boundaries(self):
        data = reference_payload()
        points = {row["percentile"]: row["amount"] for row in data["distribution"]["official_percentiles"]}
        self.assertEqual(points[50], 238_600_000)
        self.assertEqual(points[80], 693_800_000)
        self.assertEqual(points[90], 1_100_200_000)
        self.assertEqual(WEALTH_DISTRIBUTION["as_of"], "2025-03-31")
        self.assertEqual(RETIREMENT_REFERENCE["adequate_monthly_spending"]["couple"], 2_981_000)


class HouseholdAssetsRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(create_app())

    def test_endpoint_requires_login(self):
        self.assertEqual(self.client.get("/api/household-assets").status_code, 401)
        self.assertEqual(self.client.put("/api/household-assets", json={"items": []}).status_code, 401)
