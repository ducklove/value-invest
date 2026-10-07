import json
from unittest.mock import AsyncMock, patch

from _harness import seed_user

from repositories import snapshots
from repositories.db import transaction
from routes.response_models import PeriodStartResponse, PreviousDayResponse
from services.portfolio import snapshot_views, summary_contributors


async def insert_trade(user, request_id, created_at, *, currency="KRW", side="buy", quantity=2, cash_change=-310):
    result = {"stock_code": "005930", "stock_name": "삼성전자", "currency": currency,
              "side": side, "quantity": quantity, "cash_change": cash_change}
    async with transaction() as db:
        await db.execute(
            "INSERT INTO portfolio_trades (google_sub, request_id, stock_code, fingerprint, result_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user, request_id, result["stock_code"], request_id, json.dumps(result), created_at),
        )


async def test_trade_window_is_user_scoped_after_the_korean_settlement_cutoff_without_a_limit(temp_db):
    await seed_user()
    await seed_user("u2", "other@example.com")
    # 15:30 KST = 06:30 UTC. The marker itself belongs to the baseline.
    await insert_trade("u1", "before", "2026-09-30T06:29:59+00:00")
    await insert_trade("u1", "boundary", "2026-09-30T06:30:00+00:00")
    await insert_trade("u2", "other", "2026-09-30T06:31:00+00:00")
    for n in range(25):
        await insert_trade("u1", f"buy-{n}", "2026-09-30T06:31:00+00:00")
    await insert_trade("u1", "sell", "2026-09-30T07:00:00+00:00", side="sell", quantity=3, cash_change=595)
    details = await summary_contributors.baseline_details("u1", {"date": "2026-09-30"}, [
        {"stock_code": "005930", "quantity": 10, "group_name": "국내", "currency": "KRW"},
    ])
    assert details["stock_positions"]["005930"]["quantity"] == 10
    flow = details["stock_trade_flows"]["005930"]
    assert flow == {"stock_name": "삼성전자", "currency": "KRW", "quantity_change": 47,
                    "cash_change": -7155, "buy_amount": 7750, "fx_rate": 1, "comparable": True}


async def test_explicit_snapshot_cutoff_and_unknown_fx_are_preserved(temp_db):
    await seed_user()
    await insert_trade("u1", "before", "2026-09-30T10:59:59+00:00", currency="USD")
    await insert_trade("u1", "after", "2026-09-30T11:00:01+00:00", currency="USD")
    with patch.object(summary_contributors.fx, "cached_rate_for_currency", return_value=None):
        result = await summary_contributors.baseline_details("u1", {
            "date": "2026-09-30", "cashflow_cutoff_at": "2026-09-30T20:00:00+09:00",
        }, [{"stock_code": "OLD", "quantity": None}])
    assert result["stock_trade_flows"]["005930"]["quantity_change"] == 2
    assert result["stock_trade_flows"]["005930"]["fx_rate"] is None
    assert result["stock_positions"]["OLD"]["quantity"] is None


async def test_snapshot_responses_keep_position_and_trade_details_on_the_same_baseline(temp_db):
    await seed_user()
    await snapshots.save_snapshot("u1", "2026-09-30", 1000, 900, 1000, 1, 1300)
    await snapshots.save_stock_snapshots("u1", "2026-09-30", [
        {"stock_code": "005930", "market_value": 1000, "quantity": 10, "unit_price": 100, "group_name": "국내"},
    ])
    await insert_trade("u1", "after", "2026-09-30T07:00:00+00:00")
    previous = await snapshot_views.previous_day("u1", "2026-09-30")
    serialized = PreviousDayResponse.model_validate(previous).model_dump()
    assert serialized["stock_values"] == {"005930": 1000}
    assert serialized["stock_positions"]["005930"]["quantity"] == 10
    assert serialized["stock_trade_flows"]["005930"]["cash_change"] == -310
    snap = await snapshots.get_snapshot_by_date("u1", "2026-09-30")
    with patch.object(snapshots, "get_month_end_snapshot", AsyncMock(return_value=snap)), \
         patch.object(snapshots, "get_year_start_snapshot", AsyncMock(return_value=snap)):
        for yearly in (False, True):
            response = PeriodStartResponse.model_validate(await snapshot_views.period_start("u1", yearly=yearly)).model_dump()
            assert response["stock_values"] == serialized["stock_values"]
            assert response["stock_positions"] == serialized["stock_positions"]
            assert response["stock_trade_flows"] == serialized["stock_trade_flows"]


async def test_currency_change_in_one_position_is_not_silently_combined(temp_db):
    await seed_user()
    await insert_trade("u1", "krw", "2026-09-30T07:00:00+00:00")
    await insert_trade("u1", "usd", "2026-09-30T07:01:00+00:00", currency="USD")
    result = await summary_contributors.baseline_details("u1", {"date": "2026-09-30"}, [])
    assert result["stock_trade_flows"]["005930"]["comparable"] is False
