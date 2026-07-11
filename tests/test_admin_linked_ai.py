import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from _harness import TempDbMixin

import ai_config
import linked_project_admin
from repositories import app_settings as app_settings_repo
from repositories import portfolio as portfolio_repo


class LinkedProjectAdminTests(unittest.TestCase):
    def test_save_preferred_config_validates_and_writes_local_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project_dir = root / "common_preferred_spread"
            project_dir.mkdir()
            payload = [
                {
                    "id": "sample",
                    "name": "Sample",
                    "commonTicker": "005930.KS",
                    "preferredTicker": "005935.KS",
                    "commonName": "삼성전자",
                    "preferredName": "삼성전자우",
                }
            ]

            saved = linked_project_admin.save_project_config(
                "preferredSpread",
                payload,
                workspace_root=root,
            )

            self.assertTrue(saved["saved"])
            self.assertEqual(saved["summary"]["count"], 1)
            written = json.loads((project_dir / "config.json").read_text(encoding="utf-8"))
            self.assertEqual(written[0]["preferredTicker"], "005935.KS")

    def test_preferred_config_rejects_duplicate_preferred_ticker(self):
        payload = [
            {
                "id": "a",
                "name": "A",
                "commonTicker": "005930.KS",
                "preferredTicker": "005935.KS",
                "commonName": "A",
                "preferredName": "A우",
            },
            {
                "id": "b",
                "name": "B",
                "commonTicker": "005930.KS",
                "preferredTicker": "005935.KS",
                "commonName": "B",
                "preferredName": "B우",
            },
        ]

        with self.assertRaises(linked_project_admin.LinkedProjectConfigError):
            linked_project_admin.validate_config("preferred", payload)

    def test_preferred_config_syncs_public_rows_into_local_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project_dir = root / "common_preferred_spread"
            project_dir.mkdir()
            local_payload = [
                {
                    "id": "samsung",
                    "name": "삼성전자",
                    "commonTicker": "005930.KS",
                    "preferredTicker": "005935.KS",
                    "commonName": "삼성전자",
                    "preferredName": "삼성전자우",
                }
            ]
            public_payload = [
                *local_payload,
                {
                    "id": "daeduck_electronics",
                    "name": "대덕전자",
                    "commonTicker": "353200.KS",
                    "preferredTicker": "35320K.KS",
                    "commonName": "대덕전자",
                    "preferredName": "대덕전자1우",
                },
            ]
            (project_dir / "config.json").write_text(json.dumps(local_payload), encoding="utf-8")

            with patch.object(linked_project_admin, "_read_remote_json", return_value=(public_payload, None)):
                config = linked_project_admin.get_project_config("preferredSpread", workspace_root=root)

            self.assertEqual(config["source"], "local")
            self.assertEqual(config["summary"]["count"], 2)
            self.assertEqual(config["diagnostics"]["missingLocallyCount"], 0)
            self.assertTrue(config["diagnostics"]["sync"]["updated"])
            self.assertEqual(config["diagnostics"]["sync"]["addedFromPublicCount"], 1)
            self.assertEqual(config["config"][1]["preferredTicker"], "35320K.KS")
            self.assertNotIn("_configSource", config["config"][1])
            written = json.loads((project_dir / "config.json").read_text(encoding="utf-8"))
            self.assertEqual(written[1]["preferredTicker"], "35320K.KS")


class AiAdminConfigTests(TempDbMixin):
    async def test_key_status_masks_secret_and_models_are_runtime_configurable(self):
        with patch.dict("os.environ", {}, clear=True):
            await ai_config.set_openrouter_key("sk-or-test-secret-123456", "admin@example.com")
            status = await ai_config.openrouter_key_status()
            self.assertTrue(status["configured"])
            self.assertEqual(status["source"], "admin-db")
            self.assertNotIn("test-secret", status["masked"])
            self.assertEqual(
                await ai_config.get_model_for_feature("wiki_qa"),
                "qwen/qwen3.6-flash",
            )
            self.assertEqual(
                await ai_config.get_model_for_feature("portfolio_fast"),
                "qwen/qwen3.6-flash",
            )
            # 대가 포트폴리오 진단은 gemini-3.5-flash 고정 기본값.
            self.assertEqual(
                await ai_config.get_model_for_feature("masters_review"),
                "google/gemini-3.5-flash",
            )

            await ai_config.save_tier_models({"light": "openai/gpt-5.5"}, "admin@example.com")
            self.assertEqual(await ai_config.get_model_for_feature("wiki_qa"), "openai/gpt-5.5")
            self.assertEqual(await ai_config.get_model_for_feature("daily_briefing"), "openai/gpt-5.5")
            self.assertEqual(
                await ai_config.get_model_for_feature("dart_report_review"),
                "google/gemini-3.5-flash",
            )

    async def test_feature_routing_uses_three_configurable_external_tiers(self):
        with patch.dict("os.environ", {}, clear=True):
            expected = {
                "portfolio_fast": "LIGHT",
                "portfolio_balanced": "BALANCED",
                "portfolio_premium": "PREMIUM",
                "wiki_qa": "LIGHT",
                "wiki_ingestion": "LIGHT",
                "dart_report_review": "BALANCED",
                "market_daily": "BALANCED",
                "daily_briefing": "LIGHT",
                "masters_review": "BALANCED",
            }
            self.assertEqual(
                {key: spec["tier"] for key, spec in ai_config.MODEL_FEATURES.items()},
                expected,
            )
            self.assertEqual(await ai_config.get_model_for_feature("portfolio_premium"), "openai/gpt-5.6-terra")

            await ai_config.save_tier_models(
                {"LIGHT": "vendor/light", "BALANCED": "vendor/balanced", "PREMIUM": "vendor/premium"},
                "admin@example.com",
            )
            config = await ai_config.ai_admin_config()
            self.assertEqual([row["key"] for row in config["tiers"]], ["LOCAL", "LIGHT", "BALANCED", "PREMIUM"])
            self.assertEqual(
                {row["key"]: row["model"] for row in config["features"]},
                {key: f"vendor/{tier.lower()}" for key, tier in expected.items()},
            )

    async def test_legacy_feature_overrides_no_longer_bypass_tier_routing(self):
        with patch.dict("os.environ", {}, clear=True):
            await app_settings_repo.set_app_setting(
                "AI_MODEL::wiki_qa",
                "legacy/custom-model",
                updated_by="admin@example.com",
            )
            result = await ai_config.migrate_legacy_model_defaults()
            self.assertEqual(result, {"migrated": False, "reason": "tier_routing_active"})
            self.assertEqual(await ai_config.get_model_for_feature("wiki_qa"), "qwen/qwen3.6-flash")

    async def test_usage_summary_groups_by_feature_and_model(self):
        await ai_config.record_usage(
            google_sub="u1",
            feature="portfolio_analysis",
            model="openai/gpt-5.5",
            model_profile="premium",
            input_tokens=100,
            output_tokens=50,
            cost_usd=0.12,
            latency_ms=1234,
            ok=True,
        )

        summary = await ai_config.ai_admin_config(days=30)
        row = summary["usage"]["by_feature"][0]
        self.assertEqual(row["feature"], "portfolio_analysis")
        self.assertEqual(row["model"], "openai/gpt-5.5")
        self.assertEqual(row["calls"], 1)
        self.assertAlmostEqual(row["cost_usd"], 0.12)

    def test_reasoning_controls_accept_feature_specific_effort(self):
        controls = ai_config.openrouter_reasoning_controls("google/gemini-3.5-flash", effort="low")

        self.assertEqual(controls["reasoning"], {"effort": "low", "exclude": True})
        self.assertFalse(controls["include_reasoning"])

        fallback = ai_config.openrouter_reasoning_controls("google/gemini-3.5-flash", effort="wild")
        self.assertEqual(fallback["reasoning"], {"effort": "minimal", "exclude": True})

    async def test_preferred_dividend_rows_are_listed_for_admin_coverage(self):
        await portfolio_repo.upsert_preferred_dividends([
            {
                "stock_code": "35320K",
                "dividend_per_share": 450.0,
                "source_name": "대덕전자1우",
                "common_code": "353200",
                "sheet_year": 2025,
            }
        ])

        rows = await portfolio_repo.list_preferred_dividends()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stock_code"], "35320K")
        self.assertEqual(rows[0]["source_name"], "대덕전자1우")


if __name__ == "__main__":
    unittest.main()
