from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import seed_user
from fastapi import FastAPI

from repositories import bootstrap, financial, investment_insights, journal, snapshots
from routes import investment_insights as routes
from services.portfolio import attribution, fx, snapshot_attribution, theses


def nav(day, value):
    return {"date": day, "total_value": value}


def stock(day, price, *, code="AAPL", quantity=10, currency="USD", rate=1300, stale=False):
    return {"date": day, "stock_code": code, "quantity": quantity, "unit_price": price,
            "market_value": quantity*price, "currency": currency, "fx_rate": rate, "priced_from_fallback": stale}


def components(result):
    return {row["key"]: row["amount"] for row in result["components"]}


def test_price_fx_interaction_dividend_fee_and_cashflow_reconcile():
    result = attribution.decompose(
        [nav("2026-06-01", 1300000), nav("2026-06-02", 1654000)],
        [stock("2026-06-01", 130000), stock("2026-06-02", 154000, rate=1400)],
        [{"date": "2026-06-02", "type": "deposit", "amount": 100000}],
        [{"date": "2026-06-02", "kind": "dividend", "amount_krw": 15000},
         {"date": "2026-06-02", "kind": "fee", "amount_krw": 1000}],
    )
    amounts = components(result)
    assert amounts == {"external_flow": 100000, "price": 130000, "fx": 110000, "combined": 0,
                       "dividend": 15000, "fee": -1000, "unclassified": 0}
    assert sum(amounts.values()) == result["value_change"]
    assert result["investment_pnl"] == 254000


@pytest.mark.parametrize("quantity", [10, -10])
def test_missing_fx_is_combined_and_short_positions_keep_sign(quantity):
    result = attribution.decompose(
        [nav("2026-06-01", 1000), nav("2026-06-02", 1000+quantity*10)],
        [stock("2026-06-01", 100, quantity=quantity, rate=None), stock("2026-06-02", 110, quantity=quantity, rate=None)], [], [])
    assert components(result)["combined"] == quantity*10
    assert components(result)["price"] == 0
    assert components(result)["unclassified"] == 0


@pytest.mark.parametrize("change", ["quantity", "stale", "missing"])
def test_unreliable_intervals_are_not_presented_as_price_profit(change):
    rows = [stock("2026-06-01", 100), stock("2026-06-02", 200)]
    if change == "quantity":
        rows[1]["quantity"] = 20
    elif change == "stale":
        rows[1]["priced_from_fallback"] = True
    else:
        rows.pop()
    result = attribution.decompose([nav("2026-06-01", 1000), nav("2026-06-02", 2000)], rows, [], [])
    assert components(result)["unclassified"] == 1000
    assert result["coverage"]["eligible_intervals"] == 0
    assert result["reconciliation_error"] == 0


def test_boundary_cashflows_are_counted_by_applied_snapshot_date():
    result = attribution.decompose([nav("2026-06-01", 1000), nav("2026-06-03", 1200)], [], [
        {"date": "2026-05-25", "applied_snapshot_date": "2026-06-02", "type": "deposit", "amount": 200},
        {"date": "2026-06-01", "type": "deposit", "amount": 500},
        {"date": "2026-06-04", "type": "deposit", "amount": 500},
    ], [])
    assert components(result)["external_flow"] == 200
    assert result["investment_pnl"] == 0


def test_pending_backdated_cashflow_does_not_rewrite_historical_profit():
    result = attribution.decompose([nav("2026-06-01", 1000), nav("2026-06-03", 1200)], [], [
        {"date": "2026-06-02", "applied_snapshot_date": None, "units_change": None, "type": "deposit", "amount": 9999},
    ], [])
    assert components(result)["external_flow"] == 0
    assert result["investment_pnl"] == 200


def thesis(**kwargs):
    return {"stock_code": "005930", "thesis": "이익 회복", "invalidation": "영업이익률 10% 미만이면 재검토",
            "metric": "operating_margin_pct", "operator": "lt", "threshold": 10, **kwargs}


def test_thesis_threshold_missing_and_stale_data_are_distinct():
    source = {"financials": [{"year": 2025, "revenue": 100, "operating_profit": 9}]}
    assert theses.evaluate(thesis(), source, date(2026, 9, 5))["status"] == "breached"
    assert theses.evaluate(thesis(), {"financials": []}, date(2026, 9, 5))["status"] == "unknown"
    source["financials"][0]["year"] = 2020
    assert theses.evaluate(thesis(), source, date(2026, 9, 5))["status"] == "unknown"
    assert theses.evaluate(thesis(metric="manual", threshold=None, deadline="2026-09-05"), {}, date(2026, 9, 5))["status"] == "due"


def test_revenue_growth_requires_adjacent_years_and_positive_baseline():
    item = thesis(metric="revenue_growth_pct")
    source = {"financials": [{"year": 2025, "revenue": 90}, {"year": 2024, "revenue": 100}]}
    assert theses.evaluate(item, source, date(2026, 9, 5))["value"] == -10
    source["financials"][1]["year"] = 2023
    assert theses.evaluate(item, source, date(2026, 9, 5))["value"] is None


@pytest.mark.asyncio
async def test_thesis_history_changes_only_with_evidence_and_user_cannot_access_anothers(temp_db):
    await seed_user("u1")
    await seed_user("u2", "other@example.com")
    await financial.save_financial_data("005930", [{"year": 2025, "revenue": 100, "operating_profit": 9}])
    app = FastAPI()
    app.include_router(routes.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
            response = await client.post("/api/portfolio/theses", json=thesis())
            assert response.status_code == 201
            ident = response.json()["id"]
            first = (await client.get("/api/portfolio/theses")).json()["items"][0]["check"]
            second = (await client.get("/api/portfolio/theses")).json()["items"][0]["check"]
            assert first["event_id"] == second["event_id"]
            await financial.save_financial_data("005930", [{"year": 2025, "revenue": 100, "operating_profit": 15}])
            third = (await client.get("/api/portfolio/theses")).json()["items"][0]["check"]
            assert third["status"] == "monitoring" and third["event_id"] != first["event_id"]
            assert len((await client.get(f"/api/portfolio/theses/{ident}/history")).json()["items"]) == 2
            await client.put(f"/api/portfolio/theses/{ident}", json=thesis(archived=True))
            assert (await client.get("/api/portfolio/theses")).json()["items"][0]["check"]["status"] == "archived"
            await client.put(f"/api/portfolio/theses/{ident}", json=thesis())
            resumed = (await client.get("/api/portfolio/theses")).json()["items"][0]["check"]
            assert resumed["event_id"] != third["event_id"]
        with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
            assert (await client.put(f"/api/portfolio/theses/{ident}", json=thesis())).status_code == 404
            assert (await client.get(f"/api/portfolio/theses/{ident}/history")).status_code == 404
            assert (await client.get("/api/portfolio/theses")).json()["items"] == []
        with patch.object(routes, "get_current_user", AsyncMock(return_value=None)):
            assert (await client.get("/api/portfolio/attribution")).status_code == 401


@pytest.mark.asyncio
async def test_income_classification_never_mutates_holdings_and_is_owner_scoped(temp_db):
    await seed_user()
    app = FastAPI()
    app.include_router(routes.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
            body = {"date": "2026-06-02", "kind": "dividend", "amount_krw": 1234}
            response = await client.post("/api/portfolio/income-events", json=body)
            assert response.status_code == 201
            ident = response.json()["id"]
            assert await snapshots.get_cashflows("u1") == []
            assert len(await investment_insights.income_events("u1", "2026-01-01", "2026-12-31")) == 1
            assert (await client.post("/api/portfolio/income-events", json={**body, "amount_krw": "Infinity"})).status_code == 422
        with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
            assert (await client.delete(f"/api/portfolio/income-events/{ident}")).status_code == 404
        with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
            assert (await client.delete(f"/api/portfolio/income-events/{ident}")).status_code == 200


@pytest.mark.asyncio
async def test_journal_link_requires_same_user_and_stock_and_survives_journal_delete(temp_db):
    await seed_user()
    await seed_user("u2", "other@example.com")
    entry = await journal.insert_entry("u1", "005930", "memo", "투자 근거")
    model = routes.ThesisInput(**thesis(journal_entry_id=entry["id"]))
    with pytest.raises(routes.HTTPException):
        await routes.store_thesis("u2", model)
    ident = (await routes.store_thesis("u1", model))["id"]
    await journal.delete_entry("u1", entry["id"])
    assert (await investment_insights.list_theses("u1"))[0]["journal_entry_id"] is None
    assert ident


@pytest.mark.asyncio
async def test_metadata_does_not_backfill_historical_fx_with_todays_rate():
    with patch("services.portfolio.fx.fx_rate_for_currency", AsyncMock(return_value=1400)) as fetch:
        result = await snapshot_attribution.metadata({"stock_code": "AAPL", "currency": "USD"}, stale=False,
                                                     snap_date="2026-06-01", today="2026-09-05")
        assert result == {"currency": "USD", "fx_rate": None}
        fetch.assert_not_awaited()


def test_snapshot_metadata_reads_only_fresh_fx_cache():
    fx._fx_daily_cache.clear()
    fx._fx_cache.clear()
    try:
        fx._fx_daily_cache.set("FX_USDKRW", {"price": 1400})
        assert fx.cached_rate_for_currency("USD") == 1400
        fx._fx_daily_cache.set("FX_USDKRW", {"price": 1400}, ttl_seconds=-1)
        assert fx.cached_rate_for_currency("USD") is None
        fx._fx_cache.set("rates", {"FX_JPYKRW": 900})
        assert fx.cached_rate_for_currency("JPY") == 9
        assert fx.cached_rate_for_currency("UNKNOWN") is None
    finally:
        fx._fx_daily_cache.clear()
        fx._fx_cache.clear()


@pytest.mark.asyncio
async def test_new_schema_is_idempotent_and_snapshot_metadata_roundtrips(temp_db):
    await seed_user()
    await bootstrap.init_db()
    await snapshots.save_snapshot("u1", "2026-06-01", 1300000, 1000000, 1000, 1300)
    await snapshots.save_snapshot("u1", "2026-06-02", 1540000, 1000000, 1100, 1300)
    for day, price, rate in [("2026-06-01", 130000, 1300), ("2026-06-02", 154000, 1400)]:
        await snapshots.save_stock_snapshots("u1", day, [stock(day, price, rate=rate)])
    result = await attribution.build_attribution("u1", "2026-06-01", "2026-06-02")
    assert components(result)["price"] == 130000
    assert components(result)["fx"] == 110000
    assert result["baseline_mode"] == "first_in_period"
    assert not (await attribution.build_attribution("u1", "2026-07-01", "2026-07-02"))["available"]
