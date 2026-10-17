from unittest.mock import AsyncMock, patch

import httpx
from _harness import TempDbMixin, seed_user
from fastapi import FastAPI

from core.errors import register_exception_handlers
from domain.broker_activity import stamp
from repositories import account_holdings, accounts, broker_activity, broker_cash_reviews, brokers, snapshots
from repositories.broker_secrets import BrokerError
from repositories.db import get_db
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
             patch.object(activity, "fetch", AsyncMock(return_value=entries or [])), \
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
        await self.sync(cash(9000))
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
        await self.sync(cash(9000))
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
        self.assertEqual((await self.pending())[0]["unexplained"], 100)  # 같은 수입을 재사용하지 않는다.

    async def test_unknown_transfer_and_other_income_are_not_inferred(self):
        await self.sync(cash(10000))
        await broker_activity.store("u1", self.link, [self.event("external", 500, "transfer"), self.event("unknown", 500, "other_income")])
        await self.sync(cash(11000))
        self.assertEqual((await self.pending())[0]["unexplained"], 1000)

    async def test_delayed_dividend_partial_exclusion_and_source_correction(self):
        await self.sync(cash(10000))
        await self.sync(cash(11500))
        row = (await self.pending())[0]
        await broker_cash_reviews.annotate("u1", row["id"], revision=1, category="external_deposit")
        await self.sync(cash(11500), entries=[self.event("div", 500)])
        row = (await self.pending())[0]
        self.assertEqual((row["amount"], row["unexplained"], row["category"]), (1500, 1000, ""))
        self.assertGreater(row["revision"], 1)
        await self.sync(cash(11500), entries=[self.event("div", 600)])  # 원본 정정은 review가 되어 자동 제외를 되돌린다.
        self.assertEqual((await self.pending())[0]["unexplained"], 1500)

    async def test_foreign_dividend_domestic_tax_and_prior_income_baseline(self):
        await self.sync(cash(10000), cash(10, "USD"), entries=[self.event("old", 1000)])
        await self.sync(cash(9900), cash(18.5, "USD"), entries=[self.event("new", 8.5, currency="USD", domestic_tax_krw=100)])
        self.assertEqual(await self.pending(), [])
        await self.sync(cash(10900), cash(18.5, "USD"))
        self.assertEqual((await self.pending())[0]["unexplained"], 1000)

    async def test_large_income_is_not_sliced_to_hide_mixed_cash_movements(self):
        await self.sync(cash(10000))
        await self.sync(cash(10500), entries=[self.event("div", 1000)])
        self.assertEqual((await self.pending())[0]["unexplained"], 500)

    async def test_known_same_day_transactions_observed_in_separate_syncs(self):
        await self.sync(cash(10000))
        await self.sync(cash(10100))
        await self.sync(cash(10300))
        await self.sync(cash(10600))
        await self.sync(cash(10600), entries=[self.event("first", 100), self.event("second", 200), self.event("third", 300)])
        self.assertEqual(await self.pending(), [])

    async def test_http_validation_authentication_and_save(self):
        await self.sync(cash(10000))
        await self.sync(cash(11000))
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
        await self.sync(cash(11000))
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
                await self.sync(cash(11000))
        self.assertEqual(await self.pending(), [])
        self.assertEqual((await account_holdings.list_positions("u1", self.aid))[0]["quantity"], 10000)
        await self.sync(cash(11000))
        self.assertEqual((await self.pending())[0]["unexplained"], 1000)

    async def test_disconnect_reconnect_baseline_and_stock_changes(self):
        await self.sync(cash(10000))
        stock = {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 1, "avg_price": 1000, "currency": "KRW", "avg_price_currency": "KRW"}
        await self.sync(cash(9000), stock)
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
