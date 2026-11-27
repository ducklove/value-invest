"""Regression tests for narrowed exception handlers across services/.

Verifies both halves of the contract:
- Expected external/DB/parse failures are caught and handled gracefully.
- Unexpected internal programming errors (e.g. ZeroDivisionError) propagate
  rather than being silently swallowed, except at intentional isolation
  boundaries (channel/user fan-out, background loops).
"""

from __future__ import annotations

import csv
import sqlite3
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from core.errors import DBError, ExternalServiceError, RateLimitError
from services import ai_client, daily_briefing
from services.dividends import foreign as foreign_dividends
from services.dividends import preferred as preferred_dividends
from services.ecosystem import external_tools
from services.market import economic_calendar, indicators, movers
from services.market.sources import close_price, finance_pi, kis_proxy
from services.notifications import channels, engine, kakao, telegram
from services.portfolio import foreign as portfolio_foreign
from services.portfolio import fx, intraday_snapshot


class _UnexpectedBug(Exception):
    """Sentinel representing an unexpected internal bug outside narrowed tuples."""


class NotificationExceptionNarrowingTests(unittest.IsolatedAsyncioTestCase):
    async def test_telegram_catches_external_errors_and_propagates_unexpected(self):
        with patch.object(telegram, "_api", new=AsyncMock(side_effect=httpx.ConnectError("offline"))):
            self.assertIsNone(await telegram.get_me("tok"))
            self.assertFalse(await telegram.send_message("tok", "123", "hi"))

        with patch.object(telegram, "_api", new=AsyncMock(side_effect=ExternalServiceError("bad token"))):
            self.assertIsNone(await telegram.get_recent_chat_id("tok"))

        with patch.object(telegram, "_api", new=AsyncMock(side_effect=_UnexpectedBug("bug"))):
            with self.assertRaises(_UnexpectedBug):
                await telegram.get_me("tok")

    async def test_kakao_refresh_handles_api_and_db_errors_but_propagates_unexpected(self):
        with (
            patch.object(kakao, "_refresh_token", new=AsyncMock(side_effect=httpx.ReadTimeout("slow"))),
            patch.object(
                kakao.notifications_repo,
                "upsert_notification_channel",
                new=AsyncMock(side_effect=DBError("locked")),
            ),
        ):
            ok = await kakao._refresh_into("u1", {"refresh_token": "r", "rest_key": "k"}, True)
        self.assertFalse(ok)

        with patch.object(kakao, "_refresh_token", new=AsyncMock(side_effect=_UnexpectedBug("bug"))):
            with self.assertRaises(_UnexpectedBug):
                await kakao._refresh_into("u1", {"refresh_token": "r", "rest_key": "k"}, True)

    async def test_channels_has_active_channel_handles_db_error_and_dispatch_isolates_channels(self):
        with patch.object(
            channels.notifications_repo,
            "list_notification_channels",
            new=AsyncMock(side_effect=sqlite3.OperationalError("locked")),
        ):
            self.assertFalse(await channels.has_active_channel("u1"))

        with patch.object(
            channels.notifications_repo,
            "list_notification_channels",
            new=AsyncMock(side_effect=_UnexpectedBug("bug")),
        ):
            with self.assertRaises(_UnexpectedBug):
                await channels.has_active_channel("u1")

        # Intentional fan-out boundary: first channel crashes unexpectedly, second still delivers.
        fake_channels = [
            {"channel": "telegram", "enabled": True, "verified": True, "config": {"bot_token": "t", "chat_id": "1"}},
            {"channel": "kakao", "enabled": True, "verified": True, "config": {}},
        ]
        with (
            patch.object(channels.notifications_repo, "list_notification_channels", new=AsyncMock(return_value=fake_channels)),
            patch.object(channels.telegram, "send_message", new=AsyncMock(side_effect=_UnexpectedBug("tg bug"))),
            patch.object(channels.kakao, "send_to_user", new=AsyncMock(return_value=True)),
        ):
            sent = await channels.dispatch("u1", "hello")
        self.assertEqual(sent, 1)

    async def test_engine_safe_quote_and_nav_narrowed_but_guard_user_isolates(self):
        with patch.object(engine.runtime_quotes, "fetch_quote", new=AsyncMock(side_effect=TimeoutError())):
            self.assertEqual(await engine._safe_quote("005930"), {})

        with patch.object(engine.runtime_quotes, "fetch_quote", new=AsyncMock(side_effect=_UnexpectedBug("bug"))):
            with self.assertRaises(_UnexpectedBug):
                await engine._safe_quote("005930")

        with patch.object(
            intraday_snapshot,
            "_fetch_total_value",
            new=AsyncMock(side_effect=intraday_snapshot.IntradaySnapshotIncomplete("missing")),
        ):
            self.assertIsNone(await engine._portfolio_nav("u1"))

        # Intentional per-user boundary: _guard_user isolates arbitrary failure.
        async def _boom():
            raise _UnexpectedBug("user bug")

        self.assertIsNone(await engine._guard_user("u1", _boom()))


class DailyBriefingExceptionNarrowingTests(unittest.IsolatedAsyncioTestCase):
    async def test_daily_price_changes_catches_client_error_and_propagates_unexpected(self):
        with patch.object(
            daily_briefing.close_price_client,
            "get_daily_prices_batch",
            new=AsyncMock(side_effect=close_price.ClosePriceClientError("down")),
        ):
            self.assertEqual(
                await daily_briefing._daily_price_changes_by_code(["005930"], "2026-01-01", "2026-01-02"),
                {},
            )

        with patch.object(
            daily_briefing.close_price_client,
            "get_daily_prices_batch",
            new=AsyncMock(side_effect=_UnexpectedBug("bug")),
        ):
            with self.assertRaises(_UnexpectedBug):
                await daily_briefing._daily_price_changes_by_code(["005930"], "2026-01-01", "2026-01-02")

    async def test_generate_briefing_falls_back_on_openrouter_or_rate_limit_error(self):
        fake_ctx = {
            "google_sub": "u1",
            "date": "2026-09-01",
            "briefing_type": "morning",
            "briefing_name": "모닝 브리핑",
            "briefing_title": "🌅 모닝 브리핑",
            "briefing_schedule": "07:30",
            "briefing_focus": "",
            "briefing_outline": "",
            "market": ["- KOSPI: 2,500 (+0.5%)"],
        }
        for exc in (ai_client.OpenRouterError("502", status_code=502), RateLimitError("budget")):
            with (
                patch.object(daily_briefing, "build_briefing_context", new=AsyncMock(return_value=fake_ctx)),
                patch.object(daily_briefing, "get_custom_instructions", new=AsyncMock(return_value="")),
                patch.object(daily_briefing.ai_config, "get_model_for_feature", new=AsyncMock(return_value="m")),
                patch.object(daily_briefing.ai_config, "openrouter_reasoning_controls", return_value={}),
                patch.object(daily_briefing.ai_client, "post_chat_completion", new=AsyncMock(side_effect=exc)),
            ):
                result = await daily_briefing.generate_briefing("u1", "morning")
            self.assertEqual(result["source"], "template")


class MarketExceptionNarrowingTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_price_wraps_http_error_and_propagates_unexpected(self):
        finance_pi.reset_cooldown()
        with (
            patch.object(close_price, "ENABLED", True),
            patch.object(close_price, "_get_json", new=AsyncMock(side_effect=httpx.ConnectError("down"))),
        ):
            with self.assertRaises(close_price.ClosePriceClientError):
                await close_price.get_daily_closes("005930", since="2026-01-01", until="2026-01-10")
        finance_pi.reset_cooldown()

        with (
            patch.object(close_price, "ENABLED", True),
            patch.object(close_price, "_get_json", new=AsyncMock(side_effect=_UnexpectedBug("bug"))),
        ):
            with self.assertRaises(_UnexpectedBug):
                await close_price.get_daily_closes("005930", since="2026-01-01", until="2026-01-10")
        self.assertFalse(finance_pi.cooldown_active())

    async def test_kis_proxy_get_wraps_http_and_propagates_unexpected(self):
        class _BugClient:
            async def get(self, *args, **kwargs):
                raise _UnexpectedBug("bug")

        with (
            patch.object(kis_proxy, "_get_client", new=AsyncMock(return_value=_BugClient())),
            patch.object(kis_proxy, "_acquire_rate_slot", new=AsyncMock()),
        ):
            with self.assertRaises(_UnexpectedBug):
                await kis_proxy._get("/v1/stocks/005930/quote")

    async def test_indicators_boj_tona_catches_bad_zip_and_night_futures_catches_kis_error(self):
        page_resp = MagicMock(status_code=200, text='<a href="d_release/md/test.xlsx">x</a>')
        bad_zip_resp = MagicMock(status_code=200, content=b"not-a-zip")
        client = MagicMock()
        client.get = AsyncMock(side_effect=[page_resp, bad_zip_resp])
        out = await indicators._fetch_boj_tona(client)
        self.assertEqual(out, indicators._EMPTY)

        with patch.object(
            indicators.kis_proxy_client,
            "get_night_futures_quote",
            new=AsyncMock(side_effect=kis_proxy.KISProxyError("down")),
        ):
            self.assertEqual(await indicators._fetch_night_futures(), indicators._EMPTY)

        with patch.object(
            indicators.kis_proxy_client,
            "get_night_futures_quote",
            new=AsyncMock(side_effect=_UnexpectedBug("bug")),
        ):
            with self.assertRaises(_UnexpectedBug):
                await indicators._fetch_night_futures()

    async def test_movers_and_calendar_catch_http_and_propagate_unexpected(self):
        movers._movers_cache.clear()
        with patch.object(movers, "_get_html", new=AsyncMock(side_effect=httpx.ReadTimeout("slow"))):
            self.assertEqual(await movers.fetch_market_movers("rising", "kospi"), [])

        with patch.object(movers, "_get_html", new=AsyncMock(side_effect=_UnexpectedBug("bug"))):
            with self.assertRaises(_UnexpectedBug):
                await movers.fetch_market_movers("rising", "kospi")

        economic_calendar._cache.clear()
        with patch.object(
            economic_calendar,
            "get_http_client",
            new=AsyncMock(side_effect=httpx.ConnectError("down")),
        ):
            res = await economic_calendar.fetch_economic_calendar(start_date="2026-09-01", end_date="2026-09-01")
        self.assertEqual(res["events"], [])


class ForeignAndDividendsAndExternalToolsTests(unittest.IsolatedAsyncioTestCase):
    async def test_portfolio_foreign_catches_fx_and_db_errors_and_propagates_unexpected(self):
        payload = {"rows": [{"close": 100.0}], "currency": "USD", "meta": {"regularMarketPrice": 100.0}}
        with (
            patch.object(portfolio_foreign, "fetch_yahoo_chart", new=AsyncMock(return_value=payload)),
            patch.object(portfolio_foreign.fx, "fx_rate_for_currency", new=AsyncMock(side_effect=fx.FXUnavailableError("USD"))),
        ):
            self.assertEqual(await portfolio_foreign.yfinance_fetch_quote_fast("AAPL"), {})

        with patch.object(portfolio_foreign, "fetch_yahoo_chart", new=AsyncMock(side_effect=_UnexpectedBug("bug"))):
            with self.assertRaises(_UnexpectedBug):
                await portfolio_foreign.yfinance_fetch_quote_fast("AAPL")

        with patch.object(
            portfolio_foreign.ticker_map_repo,
            "save_ticker",
            new=AsyncMock(side_effect=DBError("disk full")),
        ):
            await portfolio_foreign.save_ticker("TEST_ERR", "TEST_ERR.L")

    async def test_preferred_dividends_catches_http_csv_db_and_propagates_unexpected(self):
        with patch.object(preferred_dividends, "fetch_csv", new=AsyncMock(side_effect=httpx.ConnectError("offline"))):
            res = await preferred_dividends.refresh_preferred_dividends()
        self.assertFalse(res["ok"])

        with (
            patch.object(preferred_dividends, "fetch_csv", new=AsyncMock(return_value="csv")),
            patch.object(preferred_dividends, "parse_sheet_csv", side_effect=csv.Error("bad csv")),
        ):
            res = await preferred_dividends.refresh_preferred_dividends()
        self.assertFalse(res["ok"])
        self.assertIn("parse:", res["error"])

        with (
            patch.object(preferred_dividends, "fetch_csv", new=AsyncMock(return_value="csv")),
            patch.object(preferred_dividends, "parse_sheet_csv", return_value=[]),
            patch.object(
                preferred_dividends.portfolio_repo,
                "upsert_preferred_dividends",
                new=AsyncMock(side_effect=DBError("locked")),
            ),
        ):
            res = await preferred_dividends.refresh_preferred_dividends()
        self.assertFalse(res["ok"])
        self.assertIn("upsert:", res["error"])

        with patch.object(preferred_dividends, "fetch_csv", new=AsyncMock(side_effect=_UnexpectedBug("bug"))):
            with self.assertRaises(_UnexpectedBug):
                await preferred_dividends.refresh_preferred_dividends()

    async def test_foreign_dividends_handles_db_and_fx_errors(self):
        with patch.object(
            foreign_dividends,
            "select_foreign_target_codes",
            new=AsyncMock(side_effect=sqlite3.OperationalError("no such table")),
        ):
            res = await foreign_dividends.refresh_foreign_dividends()
        self.assertFalse(res["ok"])

        with (
            patch.object(foreign_dividends.portfolio_quotes, "load_ticker_map", new=AsyncMock(return_value={})),
            patch.object(foreign_dividends, "_fetch_one", new=AsyncMock(return_value={"dps_native": 1.0, "currency": "USD"})),
            patch.object(
                foreign_dividends.portfolio_quotes,
                "fx_to_krw",
                new=AsyncMock(side_effect=fx.FXUnavailableError("USD")),
            ),
            patch.object(
                foreign_dividends.foreign_dividends_repo,
                "upsert_foreign_dividends_auto",
                new=AsyncMock(return_value=0),
            ),
        ):
            res = await foreign_dividends.refresh_foreign_dividends(["AAPL"])
        self.assertTrue(res["ok"])
        self.assertEqual(res["failures"], ["AAPL"])

    async def test_external_tools_stock_links_isolates_fetch_errors_and_propagates_unexpected(self):
        async def _fake_pair(repo: str):
            if repo == "common_preferred_spread":
                raise httpx.ConnectError("spread offline")
            return (
                {"pairs": [{"id": "yp", "ratio": 780.0}]},
                [{"id": "yp", "name": "영풍", "holdingTicker": "000670.KS"}],
            )

        with (
            patch.object(external_tools, "_load_pair", new=AsyncMock(side_effect=_fake_pair)),
            patch.object(external_tools, "etf_link_for", new=AsyncMock(return_value=None)),
        ):
            links = await external_tools.fetch_stock_links("000670")
        self.assertNotIn("preferred", links)
        self.assertIn("holding", links)

        with patch.object(external_tools, "_load_pair", new=AsyncMock(side_effect=_UnexpectedBug("bug"))):
            with self.assertRaises(_UnexpectedBug):
                await external_tools.fetch_stock_links("000670")
