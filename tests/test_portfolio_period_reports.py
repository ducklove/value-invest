from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from _harness import seed_user
from starlette.requests import Request

import cache
from routes import portfolio_reports as reports_route
from services.portfolio import period_reports


def _request(path: str = "/api/portfolio/period-reports") -> Request:
    return Request({
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    })


async def _seed_period_fixture():
    await seed_user("u1", "user@example.com", "User")
    db = await cache.get_db()
    await db.executemany(
        """
        INSERT INTO portfolio_snapshots
        (google_sub, date, total_value, total_invested, nav, total_units, fx_usdkrw)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("u1", "2026-05-31", 1_000_000, 900_000, 1000.0, 1000.0, 1300.0),
            ("u1", "2026-06-10", 1_250_000, 1_100_000, 1050.0, 1047.619, 1320.0),
            ("u1", "2026-06-30", 1_400_000, 1_100_000, 1100.0, 1047.619, 1310.0),
        ],
    )
    await db.executemany(
        """
        INSERT INTO portfolio_stock_snapshots
        (google_sub, date, stock_code, market_value, group_name, quantity, unit_price, avg_price_krw, cost_basis)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("u1", "2026-05-31", "AAA", 700_000, "한국주식", 7, 100_000, 90_000, 630_000),
            ("u1", "2026-05-31", "BBB", 300_000, "해외주식", 3, 100_000, 80_000, 240_000),
            ("u1", "2026-06-30", "AAA", 900_000, "한국주식", 9, 100_000, 90_000, 810_000),
            ("u1", "2026-06-30", "CCC", 500_000, "해외주식", 5, 100_000, 100_000, 500_000),
        ],
    )
    await db.executemany(
        """
        INSERT INTO portfolio_cashflows
        (google_sub, date, type, amount, nav_at_time, units_change, memo, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("u1", "2026-06-15", "deposit", 200_000, 1050.0, 190.476, "monthly add", "2026-06-15T12:00:00"),
            ("u1", "2026-06-20", "withdrawal", 50_000, 1060.0, -47.169, "trim cash", "2026-06-20T12:00:00"),
        ],
    )
    await db.commit()


@pytest.mark.asyncio
async def test_monthly_period_report_builds_change_snapshot(temp_db):
    await _seed_period_fixture()

    with patch.object(period_reports, "today_kst_date", return_value=date(2026, 7, 1)):
        report = await period_reports.build_period_report("u1", "monthly", "2026-06")

    assert report["schema_version"] == 2
    assert report["period"]["type"] == "monthly"
    assert report["period"]["key"] == "2026-06"
    assert report["period"]["baseline_date"] == "2026-05-31"
    assert report["period"]["ending_snapshot_date"] == "2026-06-30"
    assert report["period"]["is_complete"] is True
    assert report["summary"]["nav_return_pct"] == 10.0
    assert report["summary"]["value_change"] == 400_000
    assert report["cashflows"]["net_cashflow"] == 150_000
    assert report["cashflows"]["count"] == 2
    assert report["risk"]["points"] == 3
    assert report["data_quality"]["status"] == "ok"

    composition = report["composition_changes"]
    comp_summary = composition["summary"]
    assert comp_summary["new_positions"] == 1
    assert comp_summary["closed_positions"] == 1
    assert comp_summary["increased_positions"] == 1
    assert comp_summary["buy_like_count"] == 2
    assert comp_summary["sell_like_count"] == 1
    assert comp_summary["gross_buy_value_estimate"] == 700_000
    assert comp_summary["gross_sell_value_estimate"] == 300_000
    assert comp_summary["net_trade_value_estimate"] == 400_000
    assert comp_summary["quantity_basis_count"] == 3
    assert composition["top_buys"][0]["stock_code"] == "CCC"
    assert composition["top_buys"][0]["activity"] == "new_position"
    assert composition["top_buys"][1]["stock_code"] == "AAA"
    assert composition["top_buys"][1]["quantity_change"] == 2
    assert composition["top_sells"][0]["stock_code"] == "BBB"
    assert composition["top_sells"][0]["activity"] == "closed_position"

    counts = report["holdings"]["changes"]["counts"]
    assert counts["added"] == 1
    assert counts["removed"] == 1
    assert counts["increased"] == 1
    added = [r for r in report["holdings"]["changes"]["all"] if r["status"] == "added"]
    removed = [r for r in report["holdings"]["changes"]["all"] if r["status"] == "removed"]
    assert added[0]["stock_code"] == "CCC"
    assert removed[0]["stock_code"] == "BBB"
    assert report["allocation"]["groups"][0]["group_name"] in {"한국주식", "해외주식"}
    assert report["source_hash"]


def test_composition_changes_classifies_negative_quantity_as_futures_short():
    composition = period_reports._composition_changes(
        [
            {
                "stock_code": "BASE",
                "stock_name": "Base",
                "group_name": "기타",
                "market_value": 1_000_000,
                "quantity": 1,
                "unit_price": 1_000_000,
            },
        ],
        [
            {
                "stock_code": "BASE",
                "stock_name": "Base",
                "group_name": "기타",
                "market_value": 1_000_000,
                "quantity": 1,
                "unit_price": 1_000_000,
            },
            {
                "stock_code": "FUT1",
                "stock_name": "선물",
                "group_name": "파생",
                "market_value": 0,
                "quantity": -100,
                "unit_price": 5_000,
            },
        ],
    )

    short = composition["top_sells"][0]
    assert short["stock_code"] == "FUT1"
    assert short["activity"] == "futures_short"
    assert short["quantity_change"] == -100
    assert short["trade_value_estimate"] == -500_000
    assert short["end_weight_pct"] < 0
    assert composition["summary"]["futures_short_positions"] == 1
    assert composition["summary"]["sell_like_count"] == 1


@pytest.mark.asyncio
async def test_period_report_resolves_preferred_stock_snapshot_names(temp_db):
    await seed_user("u1", "user@example.com", "User")
    db = await cache.get_db()
    await db.executemany(
        """
        INSERT INTO portfolio_snapshots
        (google_sub, date, total_value, total_invested, nav, total_units, fx_usdkrw)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("u1", "2026-05-31", 100_000, 90_000, 1000.0, 100.0, 1300.0),
            ("u1", "2026-06-30", 120_000, 90_000, 1100.0, 100.0, 1300.0),
        ],
    )
    await db.executemany(
        """
        INSERT INTO portfolio_stock_snapshots
        (google_sub, date, stock_code, market_value, group_name, quantity, unit_price, avg_price_krw, cost_basis)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("u1", "2026-05-31", "005935", 100_000, "한국주식", 10, 10_000, 9_000, 90_000),
            ("u1", "2026-06-30", "005935", 120_000, "한국주식", 10, 12_000, 9_000, 90_000),
        ],
    )
    await db.commit()

    resolver = AsyncMock(return_value="삼성전자우")
    with patch.object(period_reports.foreign, "resolve_name", new=resolver), \
            patch.object(period_reports, "today_kst_date", return_value=date(2026, 7, 1)):
        report = await period_reports.build_period_report("u1", "monthly", "2026-06")

    row = report["composition_changes"]["all"][0]
    assert row["stock_code"] == "005935"
    assert row["stock_name"] == "삼성전자우"
    resolver.assert_awaited_with("005935")


@pytest.mark.asyncio
async def test_period_report_save_list_and_route_contract(temp_db):
    await _seed_period_fixture()
    user = {"google_sub": "u1", "email": "user@example.com"}

    with patch.object(period_reports, "today_kst_date", return_value=date(2026, 7, 1)):
        saved = await period_reports.generate_and_save_period_report("u1", "monthly", "2026-06")

    assert saved["period_type"] == "monthly"
    assert saved["period_key"] == "2026-06"
    assert saved["report"]["summary"]["nav_return_pct"] == 10.0
    assert saved["report"]["composition_changes"]["summary"]["net_trade_value_estimate"] == 400_000
    assert "# 포트폴리오 기간 보고서" in saved["report_md"]
    assert "## 매수/매도 구성 변화" in saved["report_md"]

    listed = await period_reports.list_saved_period_reports("u1")
    assert listed[0]["period_key"] == "2026-06"
    assert listed[0]["nav_return_pct"] == 10.0

    with patch("routes.portfolio_reports.get_current_user", AsyncMock(return_value=user)):
        got = await reports_route.get_period_report("monthly", "2026-06", _request())
        periods = await reports_route.get_period_report_periods(_request())

    assert got["source_hash"] == saved["source_hash"]
    assert any(row["period_key"] == "2026-06" for row in periods["saved"])
    assert any(row["key"] == "2026-06" for row in periods["monthly"])


@pytest.mark.asyncio
async def test_generate_route_validates_and_saves(temp_db):
    await _seed_period_fixture()
    user = {"google_sub": "u1", "email": "user@example.com"}

    with patch.object(period_reports, "today_kst_date", return_value=date(2026, 7, 1)), \
            patch("routes.portfolio_reports.get_current_user", AsyncMock(return_value=user)):
        out = await reports_route.generate_period_report(
            _request(),
            {"period_type": "monthly", "period_key": "2026-06"},
        )

    assert out["period_type"] == "monthly"
    assert out["report"]["period"]["key"] == "2026-06"
    assert out["report"]["cashflows"]["net_cashflow"] == 150_000
