from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
from _harness import TempDbMixin, seed_user
from fastapi import FastAPI

from core.errors import register_exception_handlers
from domain.broker_activity import stamp
from repositories import account_holdings, accounts, broker_activity, broker_cash_reviews, brokers, snapshots
from repositories.broker_secrets import BrokerError
from repositories.db import get_db, transaction
from routes import broker_accounts
from services.brokers import activity, sync


def cash(amount, currency="KRW"):
    return {"stock_code": "CASH_" + currency, "stock_name": currency, "quantity": amount,
            "avg_price": 1, "avg_price_currency": currency, "currency": currency}


class CashReviewTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "other@example.com")
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="연동 계좌"))["account_id"]
        cid = await brokers.store_credential("u1", "cash-review-key", "cash-review-secret")
        await brokers.link_account("u1", self.aid, cid, "12345678901", "live")
        self.link = await brokers.get_link("u1", self.aid)

    async def sync(self, *rows, entries=None):
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(list(rows), {}))), \
             patch.object(activity, "fetch", AsyncMock(return_value=entries if entries is not None else [])), \
             patch.object(sync, "ACTIVITY_MIN_INTERVAL", 0):
            return await sync.sync_account("u1", self.aid, include_activity=entries is not None)

    def event(self, key, amount, kind="dividend", currency="KRW", **extra):
        return {"source_key": key, "source_revision": str(amount), "auto_kind": kind,
                "date": stamp()[:10], "description": "배당금", "net_amount": amount,
                "income_amount": amount if kind in {"dividend", "interest", "other_income"} else None,
                "currency": currency, "fx_rate": 1 if currency == "KRW" else None, **extra}

    async def pending(self):
        return (await broker_cash_reviews.history("u1"))["items"]

    async def test_baseline_repeat_sync_and_each_currency(self):
        await self.sync(cash(10000), cash(10, "USD"))
        self.assertEqual(await self.pending(), [])
        await self.sync(cash(12000), cash(5, "USD"))
        await self.sync(cash(12000), cash(5, "USD"))
        rows = await self.pending()
        self.assertEqual({r["currency"]: r["unexplained"] for r in rows}, {"KRW": 2000, "USD": -5})
        self.assertTrue(all(r["account_name"] == "연동 계좌" for r in rows))
        self.assertEqual((await broker_cash_reviews.history("u2"))["items"], [])

    async def test_recording_reason_preserves_cash_nav_and_revision(self):
        await self.sync(cash(10000))
        await self.sync(cash(8000))
        row = (await self.pending())[0]
        before = await account_holdings.list_positions("u1", self.aid)
        await broker_cash_reviews.annotate("u1", row["id"], revision=row["revision"], category="distribution", reason="10월 배당 인출")
        self.assertEqual(await self.pending(), [])
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)
        self.assertEqual(await snapshots.get_cashflows("u1"), [])
        with self.assertRaisesRegex(BrokerError, "변경됐습니다"):
            await broker_cash_reviews.annotate("u1", row["id"], revision=row["revision"], category="living_expense")
        with self.assertRaisesRegex(BrokerError, "찾을 수"):
            await broker_cash_reviews.annotate("u2", row["id"], revision=2, category="living_expense")
        await self.sync(cash(8000))
        history = await broker_cash_reviews.history("u1", pending=False)
        self.assertEqual(history["items"][0]["category"], "distribution")
        db = await get_db()
        self.assertEqual((await (await db.execute("SELECT COUNT(*) FROM broker_cash_review_notes")).fetchone())[0], 1)

    async def test_known_income_tax_internal_trade_and_lending_excluded(self):
        await self.sync(cash(10000))
        for i, (kind, label, amount) in enumerate([
            ("dividend", "분배금", 100), ("interest", "이자", 200), ("other_income", "대여수수료 입금", 300),
            ("fee", "양도소득세 출금", -400), ("internal", "관리 계좌 간 이체", 500), ("trade", "매도 결제", 600),
        ]):
            await broker_activity.store("u1", self.link, [self.event(str(i), amount, kind, description=label)])
        await self.sync(cash(11300))
        self.assertEqual(await self.pending(), [])
        await self.sync(cash(11400))
        latest = (await broker_cash_reviews.history("u1", pending=False))["items"][0]
        self.assertEqual((latest["unexplained"], latest["auto_resolution"]), (100, "minor"))  # 수입을 재사용하지 않는다.

    async def test_unknown_transfer_and_other_income_are_not_inferred(self):
        await self.sync(cash(10000))
        await broker_activity.store("u1", self.link, [self.event("external", 500, "transfer"), self.event("unknown", 500, "other_income")])
        await self.sync(cash(11000))
        row = (await broker_cash_reviews.history("u1", pending=False))["items"][0]
        self.assertEqual((row["unexplained"], row["auto_resolution"]), (1000, "minor"))

    async def test_delayed_dividend_partial_exclusion_and_source_correction(self):
        await self.sync(cash(10000))
        await self.sync(cash(12500))
        row = (await self.pending())[0]
        await broker_cash_reviews.annotate("u1", row["id"], revision=1, category="external_deposit")
        await self.sync(cash(12500), entries=[self.event("div", 500)])
        row = (await self.pending())[0]
        self.assertEqual((row["amount"], row["unexplained"], row["category"]), (2500, 2000, ""))
        self.assertGreater(row["revision"], 1)
        await self.sync(cash(12500), entries=[self.event("div", 600)])  # 원본 정정은 review가 되어 자동 제외를 되돌린다.
        self.assertEqual((await self.pending())[0]["unexplained"], 2500)

    async def test_foreign_dividend_domestic_tax_and_prior_income_baseline(self):
        await self.sync(cash(10000), cash(10, "USD"), entries=[self.event("old", 1000)])
        await self.sync(cash(9900), cash(18.5, "USD"), entries=[self.event("new", 8.5, currency="USD", domestic_tax_krw=100)])
        self.assertEqual(await self.pending(), [])
        await self.sync(cash(11900), cash(18.5, "USD"))
        self.assertEqual((await self.pending())[0]["unexplained"], 2000)

    async def test_large_income_is_not_sliced_to_hide_mixed_cash_movements(self):
        await self.sync(cash(10000))
        await self.sync(cash(15000), entries=[self.event("div", 10000)])
        self.assertEqual((await self.pending())[0]["unexplained"], 5000)

    async def test_known_same_day_transactions_observed_in_separate_syncs(self):
        await self.sync(cash(10000))
        await self.sync(cash(10100))
        await self.sync(cash(10300))
        await self.sync(cash(10600))
        await self.sync(cash(10600), entries=[self.event("first", 100), self.event("second", 200), self.event("third", 300)])
        self.assertTrue(all(row["unexplained"] == 0 for row in (await broker_cash_reviews.history("u1", pending=False))["items"]))
        self.assertEqual(await self.pending(), [])

    async def test_http_validation_authentication_and_save(self):
        await self.sync(cash(10000))
        await self.sync(cash(12000))
        app = FastAPI()
        app.include_router(broker_accounts.router)
        register_exception_handlers(app)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            self.assertEqual((await client.get("/api/portfolio/cash-reviews")).status_code, 401)
            with patch.object(broker_accounts, "user_id", AsyncMock(return_value="u1")):
                response = await client.get("/api/portfolio/cash-reviews")
                self.assertEqual(response.status_code, 200)
                row = response.json()["items"][0]
                path = f"/api/portfolio/cash-reviews/{row['id']}"
                self.assertEqual((await client.patch(path, json={"revision": True, "category": "external_deposit"})).status_code, 422)
                self.assertEqual((await client.get("/api/portfolio/cash-reviews?offset=-1")).status_code, 422)
                response = await client.patch(path, json={"revision": 1, "category": "external_deposit", "reason": "투자금 추가"})
                self.assertEqual(response.status_code, 200)
                self.assertEqual((await client.get("/api/portfolio/cash-reviews")).json()["total"], 0)
                history = (await client.get("/api/portfolio/cash-reviews?pending=false")).json()
                self.assertEqual(history["items"][0]["reason"], "투자금 추가")

    async def test_input_validation_direction_and_generic_reason(self):
        await self.sync(cash(10000))
        await self.sync(cash(12000))
        row = (await self.pending())[0]
        for category, reason in [("external_withdrawal", ""), ("other", " "), ("transfer", ""), ("invalid", "내용")]:
            with self.assertRaises(BrokerError):
                await broker_cash_reviews.annotate("u1", row["id"], revision=1, category=category, reason=reason)
        await broker_cash_reviews.annotate("u1", row["id"], revision=1, category="external_deposit")
        self.assertEqual(await self.pending(), [])

    async def test_failure_rolls_back_observation_with_snapshot(self):
        await self.sync(cash(10000))
        with patch.object(account_holdings, "rebuild", AsyncMock(side_effect=BrokerError("후속 검증 실패"))):
            with self.assertRaises(BrokerError):
                await self.sync(cash(12000))
        self.assertEqual(await self.pending(), [])
        self.assertEqual((await account_holdings.list_positions("u1", self.aid))[0]["quantity"], 10000)
        await self.sync(cash(12000))
        self.assertEqual((await self.pending())[0]["unexplained"], 2000)

    async def test_disconnect_reconnect_baseline_and_stock_changes(self):
        await self.sync(cash(10000))
        stock = {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 1, "avg_price": 1000, "currency": "KRW", "avg_price_currency": "KRW"}
        await self.sync(cash(8000), stock)
        self.assertTrue((await self.pending())[0]["positions_changed"])
        await brokers.disconnect("u1", self.aid)
        self.assertEqual(await self.pending(), [])
        self.assertEqual((await broker_cash_reviews.history("u1", pending=False))["total"], 1)

    async def test_derivative_valuation_and_mock_are_not_cash(self):
        db = await get_db()
        from repositories.db import transaction
        async with transaction():
            await broker_cash_reviews.observe(db, "u1", {**self.link, "product": "krfuture"}, [cash(1000)], [])
            await broker_cash_reviews.observe(db, "u1", {**self.link, "environment": "mock"}, [cash(1000)], [])
        self.assertEqual((await broker_cash_reviews.history("u1", pending=False))["total"], 0)

    async def test_small_deposit_and_withdrawal_need_no_reason_but_keep_amounts(self):
        await self.sync(cash(10000))
        await self.sync(cash(10500))
        await self.sync(cash(10200))
        self.assertEqual(await self.pending(), [])
        rows = (await broker_cash_reviews.history("u1", pending=False))["items"]
        self.assertEqual([r["unexplained"] for r in rows], [-300, 500])
        self.assertTrue(all(r["auto_resolution"] == "minor" and not r["review_required"] for r in rows))
        self.assertEqual(await snapshots.get_cashflows("u1"), [])

    async def test_repeated_small_changes_use_absolute_daily_total_and_reopen_when_material(self):
        await self.sync(cash(10000))
        await self.sync(cash(10600))
        self.assertEqual(await self.pending(), [])
        await self.sync(cash(10100))  # 순액 100원이지만 절대금액 합계는 1,100원
        rows = await self.pending()
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["auto_resolution"] is None for row in rows))

    async def test_ratio_limit_and_cap_do_not_hide_large_transfers(self):
        await self.sync(cash(50000000))
        await self.sync(cash(50004000))  # 현금의 약 0.008%
        self.assertEqual(await self.pending(), [])
        row = (await broker_cash_reviews.history("u1", pending=False))["items"][0]
        self.assertAlmostEqual(row["minor_limit"], 5000.4)
        await self.sync(cash(50006000))
        self.assertEqual(len(await self.pending()), 2)

    async def test_different_days_have_separate_small_change_budgets(self):
        with patch.object(broker_cash_reviews, "stamp", return_value="2026-10-20T09:00:00"):
            await self.sync(cash(10000))
            await self.sync(cash(10800))
        with patch.object(broker_cash_reviews, "stamp", return_value="2026-10-21T09:00:00"):
            await self.sync(cash(11600))
        self.assertEqual(await self.pending(), [])
        self.assertEqual((await broker_cash_reviews.history("u1", pending=False))["total"], 2)

    async def test_old_large_unexplained_change_is_not_hidden_by_large_current_cash_balance(self):
        await self.sync(cash(1000000000))
        await self.sync(cash(1000020000))
        self.assertEqual((await self.pending())[0]["unexplained"], 20000)

    async def test_small_changes_are_counted_after_known_income_is_subtracted(self):
        await self.sync(cash(10000))
        await self.sync(cash(13500), entries=[self.event("div", 3000)])
        self.assertEqual(await self.pending(), [])
        row = (await broker_cash_reviews.history("u1", pending=False))["items"][0]
        self.assertEqual((row["amount"], row["unexplained"], row["auto_resolution"]), (3500, 500, "minor"))

    async def test_legacy_pending_small_changes_are_suppressed_without_new_sync(self):
        await self.sync(cash(10000))
        await self.sync(cash(11000))
        async with transaction() as db:
            await db.execute("UPDATE broker_cash_reviews SET cash_basis=NULL,minor_limit_native=NULL")
        self.assertEqual(await self.pending(), [])
        row = (await broker_cash_reviews.history("u1", pending=False))["items"][0]
        self.assertEqual((row["auto_resolution"], row["minor_limit"]), ("minor", 1000))
        await broker_cash_reviews.annotate("u1", row["id"], revision=row["revision"], category="interest")
        row = (await broker_cash_reviews.history("u1", pending=False))["items"][0]
        self.assertEqual((row["category"], row["auto_resolution"]), ("interest", None))

    async def test_foreign_small_amounts_require_real_fx_and_saved_limit_survives_cache_loss(self):
        with patch("services.portfolio.fx.cached_rate_for_currency", return_value=1400):
            await self.sync(cash(100, "USD"))
            await self.sync(cash(100.5, "USD"))
        self.assertEqual(await self.pending(), [])  # 조회할 때 환율 캐시가 사라져도 관측 기준을 유지한다.
        row = (await broker_cash_reviews.history("u1", pending=False))["items"][0]
        self.assertEqual(row["auto_resolution"], "minor")
        await self.sync(cash(1100.5, "USD"))
        self.assertTrue(await self.pending())  # 1,000달러를 1,000원으로 취급하지 않는다.

    def income_batch(self, amount, *, kind="interest"):
        today = date.fromisoformat(stamp()[:10])
        summaries = [{"date": today.isoformat(), "booked_date": today.isoformat(), "currency": "KRW", "kind": kind, "amount": amount}] if amount is not None else []
        return activity.ActivityBatch([], summaries, today, today)

    async def test_large_api_interest_is_automatically_explained_and_refetch_is_idempotent(self):
        await self.sync(cash(10000))
        await self.sync(cash(52300))
        self.assertEqual(len(await self.pending()), 1)
        await self.sync(cash(52300), entries=self.income_batch(42300))
        await self.sync(cash(52300), entries=self.income_batch(42300))
        self.assertEqual(await self.pending(), [])
        row = (await broker_cash_reviews.history("u1", pending=False))["items"][0]
        self.assertEqual((row["unexplained"], row["auto_resolution"]), (0, "confirmed"))
        db = await get_db()
        self.assertEqual((await (await db.execute("SELECT COUNT(*) FROM broker_cash_review_evidence")).fetchone())[0], 1)
        self.assertEqual((await (await db.execute("SELECT COUNT(*) FROM broker_transactions")).fetchone())[0], 0)
        self.assertEqual(await snapshots.get_cashflows("u1"), [])

    async def test_daily_income_summary_can_explain_multiple_observations_without_reusing_amount(self):
        await self.sync(cash(10000))
        await self.sync(cash(12000))
        await self.sync(cash(15000))
        await self.sync(cash(15000), entries=self.income_batch(5000))
        self.assertEqual(await self.pending(), [])
        self.assertTrue(all(row["unexplained"] == 0 for row in (await broker_cash_reviews.history("u1", pending=False))["items"]))

    async def test_income_exceeding_all_observed_deposits_does_not_hide_mixed_withdrawals(self):
        await self.sync(cash(10000))
        await self.sync(cash(15000))
        await self.sync(cash(20000), entries=[self.event("div", 10000)])
        await self.sync(cash(20000), entries=self.income_batch(10000))
        self.assertEqual(len(await self.pending()), 2)

    async def test_same_day_income_growth_uses_only_amount_after_baseline_and_corrections_reopen(self):
        await self.sync(cash(10000), entries=self.income_batch(5000))
        await self.sync(cash(12000), entries=self.income_batch(7000))
        self.assertEqual(await self.pending(), [])
        await self.sync(cash(12000), entries=self.income_batch(5000))  # 소급 정정으로 설명액 제거
        self.assertEqual((await self.pending())[0]["unexplained"], 2000)
        await self.sync(cash(12000), entries=self.income_batch(None))
        self.assertEqual((await self.pending())[0]["unexplained"], 2000)

    async def test_income_summary_and_existing_income_ledger_are_not_counted_twice(self):
        await self.sync(cash(10000))
        await self.sync(cash(14000), entries=[self.event("interest", 2000, "interest")])
        await self.sync(cash(14000), entries=self.income_batch(2000))
        self.assertEqual((await self.pending())[0]["unexplained"], 2000)

    async def test_failed_snapshot_rolls_back_income_evidence_too(self):
        await self.sync(cash(10000))
        with patch.object(account_holdings, "rebuild", AsyncMock(side_effect=BrokerError("검증 실패"))):
            with self.assertRaises(BrokerError):
                await self.sync(cash(15000), entries=self.income_batch(5000))
        db = await get_db()
        self.assertEqual((await (await db.execute("SELECT COUNT(*) FROM broker_cash_review_evidence")).fetchone())[0], 0)
        self.assertEqual(await self.pending(), [])

    async def test_legacy_rows_recover_their_balance_instead_of_lowering_new_daily_limit(self):
        await self.sync(cash(80000000))
        await self.sync(cash(80006200))
        await self.sync(cash(80005100))
        await self.sync(cash(80005250))
        async with transaction() as db:
            await db.execute("UPDATE broker_cash_reviews SET cash_basis=NULL,minor_limit_native=NULL")
        await self.sync(cash(80005350))
        self.assertEqual(await self.pending(), [])
        rows = (await broker_cash_reviews.history("u1", pending=False))["items"]
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(row["auto_resolution"] == "minor" and row["minor_limit"] > 8000 for row in rows))
        self.assertEqual(rows[-1]["cash_basis"], 80006200)
        db = await get_db()
        self.assertEqual((await (await db.execute("SELECT COUNT(*) FROM broker_cash_reviews WHERE cash_basis IS NULL")).fetchone())[0], 3)

    async def test_recovered_old_limit_uses_original_amount_not_current_cash_or_unexplained(self):
        await self.sync(cash(10000))
        await self.sync(cash(15000), entries=[self.event("div", 3000)])
        async with transaction() as db:
            await db.execute("UPDATE broker_cash_reviews SET cash_basis=NULL,minor_limit_native=NULL")
        await self.sync(cash(100015000))
        newest = (await self.pending())[0]
        await broker_cash_reviews.annotate("u1", newest["id"], revision=newest["revision"], category="external_deposit")
        old = (await self.pending())[0]
        self.assertEqual((old["cash_basis"], old["minor_limit"], old["unexplained"]), (15000, 1000, 2000))

    async def test_inconsistent_anchor_does_not_invent_historical_large_balances(self):
        await self.sync(cash(10000))
        await self.sync(cash(12000))
        async with transaction() as db:
            await db.execute("UPDATE broker_cash_reviews SET cash_basis=NULL,minor_limit_native=NULL")
        await self.sync(cash(13000))
        async with transaction() as db:
            await db.execute("UPDATE broker_cash_reviews SET cash_basis=99999999 WHERE id=(SELECT MAX(id) FROM broker_cash_reviews)")
        rows = (await broker_cash_reviews.history("u1", pending=False))["items"]
        self.assertIsNone(rows[-1]["cash_basis"])
        self.assertTrue(rows[-1]["review_required"])

    async def test_exact_later_lending_receipt_is_not_subtracted_from_earlier_interest(self):
        await self.sync(cash(80000000))
        await self.sync(cash(80006200))
        await self.sync(cash(80006350), entries=self.income_batch(150, kind="other_income"))
        rows = (await broker_cash_reviews.history("u1", pending=False))["items"]
        self.assertEqual([row["unexplained"] for row in rows], [0, 6200])

    async def test_mmw_valuation_interest_and_later_deductions_and_lending_are_fully_explained(self):
        today = date.fromisoformat(stamp()[:10])
        await self.sync(cash(80000000))
        await self.sync(cash(80006200))
        await self.sync(cash(80005100))
        batch = activity.ActivityBatch([], [
            {"date": str(today), "booked_date": str(today), "currency": "KRW", "kind": "mmw_interest", "amount": 6200},
            {"date": str(today), "booked_date": str(today), "currency": "KRW", "kind": "mmw_charges", "amount": -1100},
            {"date": str(today), "booked_date": str(today), "currency": "KRW", "kind": "other_income", "amount": 150},
        ], today, today)
        await self.sync(cash(80005250), entries=batch)
        await self.sync(cash(80005250), entries=batch)
        rows = (await broker_cash_reviews.history("u1", pending=False))["items"]
        self.assertTrue(all(row["unexplained"] == 0 and row["auto_resolution"] == "confirmed" for row in rows))
        self.assertEqual(await snapshots.get_cashflows("u1"), [])

    async def test_negative_summary_already_in_baseline_is_not_reused(self):
        today = date.fromisoformat(stamp()[:10])
        batch = activity.ActivityBatch([], [{"date": str(today), "booked_date": str(today), "currency": "KRW", "kind": "mmw_charges", "amount": -2000}], today, today)
        await self.sync(cash(10000), entries=batch)
        await self.sync(cash(8000), entries=batch)
        self.assertEqual((await self.pending())[0]["unexplained"], -2000)
