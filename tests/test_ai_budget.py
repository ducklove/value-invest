"""Tests for AI daily budget caps (ST-11).

ai_config.enforce_budget_caps() reads AI_DAILY_BUDGET_USD (site-wide) and
AI_USER_DAILY_BUDGET_USD (per-user) from env, queries the day's spent USD
from ai_usage_events, and raises BudgetExceededError (-> HTTP 429 via
RateLimitError) when a cap is hit. Caps default to 0 = disabled.
"""

from __future__ import annotations

import os
from datetime import datetime
from unittest.mock import patch

from _harness import TempDbMixin

import ai_config
from repositories import ai_usage as ai_usage_repo


class BudgetCapTests(TempDbMixin):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        # Each test patches env explicitly; clear budget vars to start clean.
        self._env_patch = patch.dict(
            os.environ,
            {"AI_DAILY_BUDGET_USD": "", "AI_USER_DAILY_BUDGET_USD": ""},
            clear=False,
        )
        self._env_patch.start()

    async def asyncTearDown(self):
        self._env_patch.stop()
        await super().asyncTearDown()

    async def _record(self, *, google_sub=None, cost_usd=0.0):
        await ai_config.record_usage(
            google_sub=google_sub,
            feature="test",
            model="m",
            model_profile=None,
            cost_usd=cost_usd,
        )

    async def test_caps_disabled_by_default(self):
        # No env -> caps are 0 -> guard returns None and never raises.
        result = await ai_config.enforce_budget_caps("user-1")
        self.assertIsNone(result)

    async def test_user_cap_blocks_when_exceeded(self):
        with patch.dict(os.environ, {"AI_USER_DAILY_BUDGET_USD": "0.50"}):
            await self._record(google_sub="user-1", cost_usd=0.60)
            with self.assertRaises(ai_config.BudgetExceededError) as ctx:
                await ai_config.enforce_budget_caps("user-1")
            self.assertEqual(ctx.exception.scope, "user:user-1")
            self.assertAlmostEqual(ctx.exception.spent, 0.60, places=4)
            self.assertAlmostEqual(ctx.exception.cap, 0.50, places=4)

    async def test_user_cap_allows_below_threshold(self):
        with patch.dict(os.environ, {"AI_USER_DAILY_BUDGET_USD": "1.00"}):
            await self._record(google_sub="user-1", cost_usd=0.30)
            result = await ai_config.enforce_budget_caps("user-1")
            # Returns summary dict when within budget.
            self.assertIsNotNone(result)
            self.assertEqual(result["user_cap"], 1.00)

    async def test_site_cap_checks_total_across_users(self):
        with patch.dict(os.environ, {"AI_DAILY_BUDGET_USD": "1.00"}):
            await self._record(google_sub="user-1", cost_usd=0.40)
            await self._record(google_sub="user-2", cost_usd=0.70)  # total 1.10
            with self.assertRaises(ai_config.BudgetExceededError) as ctx:
                # site cap is checked regardless of which user calls.
                await ai_config.enforce_budget_caps("user-3")
            self.assertEqual(ctx.exception.scope, "site")

    async def test_user_cap_does_not_affect_other_users(self):
        with patch.dict(os.environ, {"AI_USER_DAILY_BUDGET_USD": "0.50"}):
            await self._record(google_sub="user-1", cost_usd=0.60)
            # user-2 is well under their own cap.
            result = await ai_config.enforce_budget_caps("user-2")
            self.assertIsNotNone(result)

    async def test_user_cap_skipped_when_no_sub(self):
        # Anonymous (system/batch) call: only site cap applies.
        with patch.dict(os.environ, {"AI_USER_DAILY_BUDGET_USD": "0.10"}):
            # Should not raise even if a user cap is set, because google_sub is None.
            result = await ai_config.enforce_budget_caps(None)
            # user_cap is configured but skipped for anonymous callers; the
            # summary still reflects the configured cap, but no error is raised.
            self.assertIsNotNone(result)
            self.assertEqual(result["google_sub"], None)

    async def test_budget_error_maps_to_429(self):
        # BudgetExceededError subclasses RateLimitError -> AppError handler maps 429.
        from core.errors import RateLimitError
        self.assertTrue(issubclass(ai_config.BudgetExceededError, RateLimitError))
        self.assertEqual(ai_config.BudgetExceededError.status_code, 429)

    async def test_daily_cost_aggregates_only_today(self):
        # Insert an event dated yesterday — must NOT count toward today's cap.
        db = await ai_usage_repo.get_db()
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).isoformat()
        await db.execute(
            "INSERT INTO ai_usage_events (ts, google_sub, feature, model, cost_usd) "
            "VALUES (?, ?, ?, ?, ?)",
            (yesterday, "user-1", "test", "m", 100.0),
        )
        await db.commit()
        with patch.dict(os.environ, {"AI_USER_DAILY_BUDGET_USD": "0.50"}):
            # Yesterday's huge spend should not block today's call.
            result = await ai_config.enforce_budget_caps("user-1")
            self.assertIsNotNone(result)

    async def test_invalid_env_value_falls_back_to_disabled(self):
        with patch.dict(os.environ, {"AI_DAILY_BUDGET_USD": "not-a-number"}):
            result = await ai_config.enforce_budget_caps("user-1")
            # Garbage -> cap disabled -> None.
            self.assertIsNone(result)
