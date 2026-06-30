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
        (google_sub, date, stock_code, market_value, group_name)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("u1", "2026-05-31", "AAA", 700_000, "한국주식"),
            ("u1", "2026-05-31", "BBB", 300_000, "해외주식"),
            ("u1", "2026-06-30", "AAA", 800_000, "한국주식"),
            ("u1", "2026-06-30", "CCC", 600_000, "해외주식"),
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

    assert report["schema_version"] == 1
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


@pytest.mark.asyncio
async def test_period_report_save_list_and_route_contract(temp_db):
    await _seed_period_fixture()
    user = {"google_sub": "u1", "email": "user@example.com"}

    with patch.object(period_reports, "today_kst_date", return_value=date(2026, 7, 1)):
        saved = await period_reports.generate_and_save_period_report("u1", "monthly", "2026-06")

    assert saved["period_type"] == "monthly"
    assert saved["period_key"] == "2026-06"
    assert saved["report"]["summary"]["nav_return_pct"] == 10.0
    assert "# 포트폴리오 기간 보고서" in saved["report_md"]

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
