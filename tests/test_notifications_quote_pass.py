"""알림 평가 패스의 시세 공유 (O10a/X11).

evaluate_all 은 전 사용자 규칙을 먼저 계획해 코드 합집합을 한 번만 조회한다 —
국내는 벌크 1회, 나머지는 bounded gather. 휴장일에는 국내 REST 강제 조회를 하지
않는다. 같은 맵이 NAV 평가에도 쓰여 보유종목을 다시 조회하지 않는다.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin

from repositories import db as db_repo
from repositories import notifications as notifications_repo
from services.notifications import alert_delivery, channels, engine

USERS = ("u1", "u2", "u3")


def _kr_quote(price: float, change_pct: float = 0.5) -> dict:
    return {"price": price, "previous_close": price / (1 + change_pct / 100), "change_pct": change_pct, "source": "naver"}


class QuotePassHarness(TempDbMixin):
    async def seed(self) -> None:
        clock = patch.object(alert_delivery, "now_kst", return_value=datetime(2026, 9, 23, 12, tzinfo=alert_delivery.KST))
        clock.start()
        self.addCleanup(clock.stop)
        engine._disc_cache.clear()
        engine._rep_cache.clear()
        db = await db_repo.get_db()
        for sub in USERS:
            await db.execute(
                "INSERT OR IGNORE INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
                " VALUES (?, ?, 'U', '', 1, 't', 't')",
                (sub, f"{sub}@x"),
            )
            # 세 사용자 모두 005930·000660 보유, u3 는 해외 AAPL 도 보유.
            holdings = [("005930", "삼성전자", 10), ("000660", "SK하이닉스", 2)]
            if sub == "u3":
                holdings.append(("AAPL", "Apple", 3))
            for code, name, qty in holdings:
                await db.execute(
                    "INSERT OR IGNORE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, 1000, 't', 't')",
                    (sub, code, name, qty),
                )
        await db.commit()
        for index, sub in enumerate(USERS):
            await notifications_repo.upsert_notification_channel(
                sub, "telegram", config={"chat_id": 100 + index, "username": "t"}, enabled=True, verified=True
            )
            await notifications_repo.create_portfolio_alert(
                sub, scope="stock", alert_type="price_above", threshold=72000.0, stock_code="005930"
            )
        await notifications_repo.create_portfolio_alert("u2", scope="all_stocks", alert_type="daily_change_abs", threshold=5.0)
        self.nav_rule = await notifications_repo.create_portfolio_alert(
            "u3", scope="portfolio", alert_type="nav_above", threshold=1.0
        )

    async def _nav_last_value(self) -> float | None:
        rules = await notifications_repo.list_portfolio_alerts("u3")
        return next(r["last_value"] for r in rules if r["id"] == self.nav_rule)


class EvaluateAllSharesQuotesTests(QuotePassHarness):
    async def test_three_users_sharing_codes_make_one_bulk_call(self):
        bulk = AsyncMock(return_value={"005930": _kr_quote(80000.0), "000660": _kr_quote(200000.0, -6.0)})
        fetch_quote = AsyncMock(return_value={"price": 300.0, "change_pct": 1.0})
        with patch.object(engine.runtime_quotes, "kr_trading_day", return_value=True), \
             patch.object(engine.runtime_quotes, "fetch_bulk_kr_quotes", new=bulk), \
             patch.object(engine.runtime_quotes, "fetch_quote", new=fetch_quote), \
             patch.object(engine, "_regular_daily_quote", new=AsyncMock(return_value={})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            result = await engine.evaluate_all()

        self.assertEqual(result["evaluated"], 3)
        # 공유 국내 코드는 사용자 수와 무관하게 벌크 1회.
        bulk.assert_awaited_once()
        self.assertEqual(sorted(bulk.await_args.args[0]), ["000660", "005930"])
        # 벌크가 채운 국내 코드는 개별 조회하지 않는다. 해외(AAPL)만 1회 —
        # u3 의 NAV 평가도 같은 맵을 써서 다시 조회하지 않는다.
        self.assertEqual([c.args[0] for c in fetch_quote.await_args_list], ["AAPL"])
        # price_above 3건 + u2 daily_change_abs(000660 -6%) 1건 + u3 nav_above 1건.
        self.assertEqual(disp.await_count, 5)
        self.assertEqual(await self._nav_last_value(), 10 * 80000.0 + 2 * 200000.0 + 3 * 300.0)

    async def test_bulk_misses_fall_back_to_forced_rest_on_trading_day(self):
        bulk = AsyncMock(return_value={"005930": _kr_quote(80000.0)})
        fetch_quote = AsyncMock(side_effect=lambda code, **_: {"price": 300.0} if code == "AAPL" else {"price": 150000.0})
        with patch.object(engine.runtime_quotes, "kr_trading_day", return_value=True), \
             patch.object(engine.runtime_quotes, "fetch_bulk_kr_quotes", new=bulk), \
             patch.object(engine.runtime_quotes, "fetch_quote", new=fetch_quote), \
             patch.object(engine, "_regular_daily_quote", new=AsyncMock(return_value={})), \
             patch.object(channels, "dispatch", new=AsyncMock()):
            await engine.evaluate_all()

        bulk.assert_awaited_once()
        calls = {c.args[0]: c.kwargs for c in fetch_quote.await_args_list}
        self.assertEqual(sorted(calls), ["000660", "AAPL"])  # 각 코드 1회
        self.assertEqual(calls["000660"], {"force_refresh": True, "use_ws_cache": False})
        self.assertEqual(await self._nav_last_value(), 10 * 80000.0 + 2 * 150000.0 + 3 * 300.0)

    async def test_holiday_pass_never_forces_rest_refresh(self):
        bulk = AsyncMock(return_value={})  # 벌크 전부 실패 → 모두 개별 경로
        fetch_quote = AsyncMock(return_value={"price": 70000.0, "change_pct": 0.0})
        with patch.object(engine.runtime_quotes, "kr_trading_day", return_value=False), \
             patch.object(engine.runtime_quotes, "fetch_bulk_kr_quotes", new=bulk), \
             patch.object(engine.runtime_quotes, "fetch_quote", new=fetch_quote), \
             patch.object(engine, "_regular_daily_quote", new=AsyncMock(return_value={})), \
             patch.object(channels, "dispatch", new=AsyncMock()):
            await engine.evaluate_all()

        self.assertTrue(fetch_quote.await_args_list)
        forced = [c for c in fetch_quote.await_args_list if c.kwargs.get("force_refresh")]
        self.assertEqual(forced, [])
        # 코드당 1회 — 사용자·NAV 사이에서 중복 조회가 없다.
        codes = [c.args[0] for c in fetch_quote.await_args_list]
        self.assertEqual(sorted(codes), ["000660", "005930", "AAPL"])


    async def test_pass_nav_equals_legacy_per_user_nav(self):
        quotes = {"005930": _kr_quote(80000.0), "000660": _kr_quote(200000.0, -6.0), "AAPL": {"price": 300.0}}
        legacy_fetch = AsyncMock(side_effect=lambda code, **_: dict(quotes[code]))
        with patch.object(engine.runtime_quotes, "fetch_quote", new=legacy_fetch):
            legacy_nav = await engine._portfolio_nav("u3")
        # 종전 경로: 보유 종목마다 개별 조회(국내는 REST 강제).
        self.assertEqual(legacy_fetch.await_count, 3)

        bulk = AsyncMock(return_value={code: dict(q) for code, q in quotes.items() if code.isdigit()})
        with patch.object(engine.runtime_quotes, "kr_trading_day", return_value=True), \
             patch.object(engine.runtime_quotes, "fetch_bulk_kr_quotes", new=bulk), \
             patch.object(engine.runtime_quotes, "fetch_quote", new=AsyncMock(side_effect=lambda code, **_: dict(quotes[code]))), \
             patch.object(engine, "_regular_daily_quote", new=AsyncMock(return_value={})), \
             patch.object(channels, "dispatch", new=AsyncMock()):
            await engine.evaluate_all()

        self.assertIsNotNone(legacy_nav)
        self.assertEqual(await self._nav_last_value(), legacy_nav)

    async def test_bulk_failure_falls_back_to_per_code_quotes(self):
        bulk = AsyncMock(side_effect=RuntimeError("naver down"))
        fetch_quote = AsyncMock(side_effect=lambda code, **_: {"price": 300.0} if code == "AAPL" else {"price": 80000.0})
        with patch.object(engine.runtime_quotes, "kr_trading_day", return_value=True), \
             patch.object(engine.runtime_quotes, "fetch_bulk_kr_quotes", new=bulk), \
             patch.object(engine.runtime_quotes, "fetch_quote", new=fetch_quote), \
             patch.object(engine, "_regular_daily_quote", new=AsyncMock(return_value={})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            result = await engine.evaluate_all()

        self.assertEqual(result["evaluated"], 3)
        self.assertEqual(sorted(c.args[0] for c in fetch_quote.await_args_list), ["000660", "005930", "AAPL"])
        self.assertGreaterEqual(disp.await_count, 3)  # price_above 3건은 그대로 발송


class EvaluateUserStandaloneTests(QuotePassHarness):
    async def test_single_user_path_keeps_per_code_forced_quotes(self):
        # 단독 evaluate_user 는 종전처럼 종목별 강제 조회(벌크 없음).
        bulk = AsyncMock(side_effect=AssertionError("standalone evaluation must not bulk-fetch"))
        fetch_quote = AsyncMock(return_value={"price": 80000.0})
        with patch.object(engine.runtime_quotes, "fetch_bulk_kr_quotes", new=bulk), \
             patch.object(engine.runtime_quotes, "fetch_quote", new=fetch_quote), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 1)

        fetch_quote.assert_awaited_once_with("005930", force_refresh=True, use_ws_cache=False)
        self.assertEqual(disp.await_count, 1)


class PassQuotesUnitTests(TempDbMixin):
    async def test_daily_only_price_is_hidden_from_non_daily_users_and_nav(self):
        plan_daily = engine._UserPlan("a", [], {}, 0, needed={"SIVR"}, daily_metric_codes={"SIVR"})
        plan_plain = engine._UserPlan("b", [], {}, 0, needed={"SIVR"})
        quotes = engine._PassQuotes(quotes={"SIVR": {"price": 10.0, "change_pct": 1.0, "_daily_only": True}})
        self.assertEqual(quotes.for_user(plan_daily)["SIVR"]["price"], 10.0)
        self.assertEqual(quotes.for_user(plan_plain)["SIVR"], {})
        self.assertNotIn("SIVR", quotes.nav_map())

    async def test_safe_quote_marks_price_taken_from_regular_daily_quote(self):
        with patch.object(engine.runtime_quotes, "fetch_quote", new=AsyncMock(return_value={})), \
             patch.object(engine, "_regular_daily_quote", new=AsyncMock(return_value={"price": 10.0, "change_pct": 2.0})):
            quote = await engine._safe_quote("SIVR", regular_daily_change=True)
        self.assertTrue(quote["_daily_only"])
        self.assertEqual(quote["change_pct"], 2.0)


class _ForeignProbes:
    """해외 종목 탐색 경로(yfinance·Yahoo chart·네이버/야후 HTTP·KIS 해외)를 모두 막고
    호출 여부를 기록한다. 호출부가 예외를 삼키므로 side_effect 대신 await 여부로 본다."""

    def __init__(self) -> None:
        from services.portfolio import foreign

        self.mocks = {
            "yfinance": AsyncMock(return_value=None),
            "yahoo_chart": AsyncMock(return_value={}),
            "http": AsyncMock(),
            "kis_overseas": AsyncMock(return_value={}),
        }
        self._patches = [
            patch.object(foreign, "yf_run", new=self.mocks["yfinance"]),
            patch.object(foreign.yahoo, "fetch_chart_json", new=self.mocks["yahoo_chart"]),
            patch.object(foreign, "get_http_client", new=self.mocks["http"]),
            patch.object(foreign.kis_proxy_client, "get_overseas_quote", new=self.mocks["kis_overseas"]),
        ]

    def __enter__(self):
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self._patches):
            p.stop()

    def calls(self) -> dict[str, int]:
        return {name: mock.await_count for name, mock in self.mocks.items() if mock.await_count}


PSEUDO_HOLDINGS = ("CASH_KRW", "CASH_CNY", "CASH_EUR", "CASH_AUD", "KRX_GOLD", "CMA_RP_KRW", "CRYPTO_BTC", "FUTURES_PNL_KRW")


class PseudoCodeAlertPassTests(TempDbMixin):
    async def test_alert_pass_with_cash_and_special_holdings_makes_zero_foreign_probes(self):
        from services.portfolio import quote_service

        db = await db_repo.get_db()
        await db.execute(
            "INSERT OR IGNORE INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
            " VALUES ('u1', 'u1@x', 'U', '', 1, 't', 't')"
        )
        for code in PSEUDO_HOLDINGS:
            await db.execute(
                "INSERT OR IGNORE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at)"
                " VALUES ('u1', ?, ?, 1, 1, 't', 't')",
                (code, code),
            )
        await db.commit()
        await notifications_repo.upsert_notification_channel(
            "u1", "telegram", config={"chat_id": 100, "username": "t"}, enabled=True, verified=True
        )
        # 일간 등락률 blanket 규칙은 전 보유종목을 '정규장 일간 시세' 보강 대상으로 만든다 —
        # 예전에는 여기서 CASH_* 가 Yahoo chart → yfinance 16회 → 네이버 22회 탐색으로 흘렀다.
        await notifications_repo.create_portfolio_alert("u1", scope="all_stocks", alert_type="daily_change_abs", threshold=5.0)

        with _ForeignProbes() as probes, \
             patch.object(engine.runtime_quotes, "kr_trading_day", return_value=True), \
             patch.object(quote_service.fx, "fetch_fx_daily_change",
                          new=AsyncMock(return_value={"price": 190.0, "change": 1.0, "change_pct": 0.5})), \
             patch.object(quote_service.special_assets, "fetch_krx_gold_quote",
                          new=AsyncMock(return_value={"price": 150000.0, "change": 150.0, "change_pct": 0.1})), \
             patch.object(quote_service.special_assets, "fetch_crypto_quote",
                          new=AsyncMock(return_value={"price": 1.0e8, "change": 1.0e6, "change_pct": 1.0})), \
             patch.object(channels, "dispatch", new=AsyncMock()):
            result = await engine.evaluate_all()

        self.assertEqual(result["evaluated"], 1)
        self.assertEqual(probes.calls(), {})

    async def test_regular_daily_quote_skips_pseudo_codes_without_io(self):
        with _ForeignProbes() as probes:
            for code in PSEUDO_HOLDINGS:
                self.assertEqual(await engine._regular_daily_quote(code), {})
        self.assertEqual(probes.calls(), {})

    async def test_regular_daily_quote_uses_yahoo_symbol_for_reuters_codes(self):
        from services.portfolio import foreign

        fast = AsyncMock(return_value={"price": 20000.0, "change_pct": 1.5})
        resolve = AsyncMock(side_effect=AssertionError("mapped symbol must not need discovery"))
        # 운영처럼 ticker_map 에 네이버 reutersCode 가 그대로 있어도 Yahoo 심볼로 조회한다.
        with patch.dict(foreign._ticker_map, {"AGNC.O": "AGNC.O"}, clear=True), \
             patch.object(foreign, "ensure_ticker_map", new=AsyncMock()), \
             patch.object(foreign, "yfinance_fetch_quote_fast", new=fast), \
             patch.object(foreign, "resolve_foreign_reuters", new=resolve):
            self.assertEqual((await engine._regular_daily_quote("AGNC.O"))["change_pct"], 1.5)
            self.assertEqual((await engine._regular_daily_quote("GOOGL.O"))["change_pct"], 1.5)
        self.assertEqual([c.args[0] for c in fast.await_args_list], ["AGNC", "GOOGL"])


class AlertQuoteDeadlineTests(TempDbMixin):
    async def test_regular_daily_quote_is_bounded(self):
        from services.portfolio import foreign

        async def hang(*_args, **_kwargs):
            await asyncio.sleep(30)

        with patch.object(engine, "REGULAR_DAILY_QUOTE_TIMEOUT_SECONDS", 0.05), \
             patch.object(foreign, "ensure_ticker_map", new=AsyncMock()), \
             patch.object(foreign, "yfinance_fetch_quote_fast", new=hang):
            started = time.monotonic()
            self.assertEqual(await engine._regular_daily_quote("AAPL"), {})
        self.assertLess(time.monotonic() - started, 2.0)

    async def test_safe_quote_is_bounded_per_code(self):
        async def hang(*_args, **_kwargs):
            await asyncio.sleep(30)

        with patch.object(engine, "ALERT_QUOTE_TIMEOUT_SECONDS", 0.05), \
             patch.object(engine.runtime_quotes, "fetch_quote", new=hang):
            started = time.monotonic()
            self.assertEqual(await engine._safe_quote("AAPL"), {})
        self.assertLess(time.monotonic() - started, 2.0)

    async def test_pass_budget_leaves_unfinished_codes_unpriced(self):
        async def fetch(code, **_kwargs):
            if code == "SLOW":
                await asyncio.sleep(30)
            return {"price": 100.0, "change_pct": 1.0}

        with patch.object(engine, "ALERT_QUOTE_PASS_BUDGET_SECONDS", 0.1), \
             patch.object(engine.runtime_quotes, "fetch_quote", new=fetch):
            started = time.monotonic()
            quotes = await engine._gather_safe_quotes(["AAPL", "SLOW", "005930"], set(), market_open=True)
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertEqual(quotes["SLOW"], {})
        self.assertEqual(quotes["AAPL"]["price"], 100.0)
        self.assertEqual(quotes["005930"]["price"], 100.0)
