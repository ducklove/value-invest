"""정산 기준(price_basis) 경계 NAV 체인 링크 — 저장 행은 그대로, 조회만 연결한다."""

import json
import logging
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import seed_user

from core.app_factory import create_app
from repositories import snapshots
from repositories.db import transaction
from routes import portfolio
from services.portfolio import nav_link, period_reports, snapshot_views

LEGACY, REGULAR = "legacy_latest", "regular_close_v1"

# 운영 DB 사용자 A·B 의 실제 경계 행 (2026-09-29 legacy → 2026-09-30 regular).
A_D0 = {"date": "2026-09-29", "total_value": 6693228966.08, "nav": 986.4788943804621, "total_units": 6784969.25,
        "return_factor": 1.0, "price_basis": LEGACY, "cashflow_cutoff_at": "2026-09-29T20:05:00.808100"}
A_D1 = {"date": "2026-09-30", "total_value": 6669406884.52, "nav": 1000.0, "total_units": 6669406.88,
        "return_factor": 1.0, "price_basis": REGULAR, "cashflow_cutoff_at": "2026-09-30T15:30:00.000"}
B_D0 = {"date": "2026-09-29", "total_value": 965422411.0, "nav": 857.5386025666769, "total_units": 965422411.0 / 857.5386025666769,
        "return_factor": 1.0, "price_basis": LEGACY, "cashflow_cutoff_at": "2026-09-29T20:15:00.815382"}
B_D1 = {"date": "2026-09-30", "total_value": 957945750.95, "nav": 1000.0, "total_units": 957945.75095,
        "return_factor": 1.0, "price_basis": REGULAR, "cashflow_cutoff_at": "2026-09-30T15:30:00.000"}


async def save(row: dict, user: str = "u1"):
    await snapshots.save_snapshot(
        user, row["date"], row["total_value"], row.get("total_invested", row["total_value"]), row["nav"],
        row["total_units"], cashflow_cutoff_at=row.get("cashflow_cutoff_at"),
        return_factor=row.get("return_factor"), price_basis=row["price_basis"],
    )


def snap(day, value, nav, basis, cutoff=None, **extra):
    return {"date": day, "total_value": value, "nav": nav, "total_units": value / nav if nav else 1,
            "price_basis": basis, "cashflow_cutoff_at": cutoff, **extra}


async def add_flow(kind, amount, created_at, applied=None, user="u1"):
    async with transaction() as db:
        await db.execute(
            "INSERT INTO portfolio_cashflows (google_sub,date,type,amount,applied_snapshot_date,created_at) VALUES (?,?,?,?,?,?)",
            (user, created_at[:10], kind, amount, applied, created_at),
        )


async def add_distribution(amount, created_at, applied, user="u1"):
    async with transaction() as db:
        await db.execute(
            "INSERT INTO portfolio_distributions (google_sub,request_id,date,currency,amount,amount_krw,applied_snapshot_date,"
            "fingerprint,result_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (user, "r1", created_at[:10], "KRW", amount, amount, applied, "f", json.dumps({"memo": ""}), created_at),
        )


# ---------------------------------------------------------------- 순수 계산


def test_link_factor_with_real_production_boundaries():
    r_a = nav_link.transition_return(A_D0, A_D1, 0)
    assert r_a * 100 == pytest.approx(-0.35591314, abs=1e-7)
    k_a = nav_link.link_factor(A_D0, A_D1, 0)
    assert k_a == pytest.approx(1.01732723, abs=1e-7)
    assert A_D0["nav"] * k_a == pytest.approx(1003.5718, abs=1e-3)
    k_b = nav_link.link_factor(B_D0, B_D1, 0)
    assert B_D0["nav"] * k_b == pytest.approx(1007.8049, abs=1e-3)
    # 연결 뒤 전환일 NAV 수익률 = 입출금 차감 평가액 수익률(Today 카드와 같은 정의).
    linked = nav_link.apply_links([dict(A_D0), dict(A_D1)], {A_D1["date"]: k_a})
    assert linked[1]["nav"] / linked[0]["nav"] - 1 == pytest.approx(r_a)


def test_unlinkable_inputs_return_none():
    assert nav_link.link_factor({**A_D0, "total_value": 0}, A_D1, 0) is None
    assert nav_link.link_factor({**A_D0, "total_value": None}, A_D1, 0) is None
    assert nav_link.link_factor(A_D0, {**A_D1, "total_value": None}, 0) is None
    assert nav_link.link_factor(A_D0, A_D1, A_D1["total_value"] * 2) is None  # 1 + r <= 0
    assert nav_link.link_factor({**A_D0, "nav": 0, "return_nav": 0}, A_D1, 0) is None


def test_apply_links_is_identity_without_boundaries():
    rows = [snap("2026-01-01", 100, 1000, LEGACY), snap("2026-01-02", 110, 1100, LEGACY)]
    assert nav_link.find_boundaries(rows) == []
    assert nav_link.apply_links(rows, {}) == rows


def test_signed_cashflow_matches_today_baseline_rule():
    assert [nav_link.signed_cashflow({"type": t, "amount": 10}) for t in ("deposit", "withdrawal", "distribution", "other")] == [10, -10, -10, 0]


# ---------------------------------------------------------------- DB 조회


@pytest.mark.asyncio
async def test_nav_history_links_legacy_rows_and_include_legacy_returns_raw(temp_db):
    await seed_user()
    first = {**A_D0, "date": "2019-03-12", "total_value": 1_000_000.0, "nav": 1000.0, "total_units": 1000.0}
    for row in (first, A_D0, A_D1):
        await save(row)

    raw = await nav_link.get_nav_history("u1", include_legacy=True)
    assert [r["nav"] for r in raw] == [1000.0, A_D0["nav"], 1000.0]
    assert not any("linked" in r for r in raw)

    linked = await nav_link.get_nav_history("u1")
    assert [r["date"] for r in linked] == ["2019-03-12", "2026-09-29", "2026-09-30"]
    k = nav_link.link_factor(A_D0, A_D1, 0)
    assert linked[1]["nav"] == pytest.approx(1003.5718, abs=1e-3)
    assert linked[1]["return_nav"] == pytest.approx(linked[1]["nav"])
    assert linked[0]["nav"] == pytest.approx(1000 * k)
    assert all(r["linked"] and r["nav_link_factor"] == pytest.approx(k) and r["price_basis"] == LEGACY for r in linked[:2])
    assert linked[1]["raw_nav"] == A_D0["nav"]
    # 금액·좌수는 실제 값 그대로.
    assert (linked[1]["total_value"], linked[1]["total_units"]) == (A_D0["total_value"], A_D0["total_units"])
    assert "linked" not in linked[2] and linked[2]["nav"] == 1000.0
    assert linked[2]["return_nav"] / linked[1]["return_nav"] - 1 == pytest.approx(A_D1["total_value"] / A_D0["total_value"] - 1)
    # 저장 행은 바뀌지 않는다.
    assert (await snapshots.get_snapshot_by_date("u1", "2026-09-29"))["nav"] == A_D0["nav"]
    # 기간 실적(심층 분석 연/월 표)도 연결된 전체 이력을 쓴다.
    perf = period_reports.build_period_performance(linked, today=date(2026, 9, 30))
    sept = next(m for m in perf["years"][0]["months"] if m["key"] == "2026-09")
    assert (sept["baseline_date"], sept["ending_date"]) == ("2019-03-12", "2026-09-30")
    assert sept["return_pct"] == pytest.approx((1000 / linked[0]["nav"] - 1) * 100, abs=1e-4)


@pytest.mark.asyncio
@pytest.mark.parametrize("flows,d1_value,expected_r", [
    # 두 정산 사이 입금 500 (d1 에 반영) → (1600 − 500)/1000 − 1 = +10%
    ([("deposit", 500, "2026-09-30T10:00:00", "2026-09-30")], 1600, 0.10),
    # 출금 200 → (700 + 200)/1000 − 1 = −10%
    ([("withdrawal", 200, "2026-09-30T09:00:00", "2026-09-30")], 700, -0.10),
    # 미반영이어도 d1 정산 시각 이전에 생긴 거래는 잔고에 들어 있다.
    ([("deposit", 100, "2026-09-30T15:00:00", None)], 1200, 0.10),
    # d0 에 이미 반영된 거래, d1 정산 뒤 거래, d1 다음 정산에 반영된 거래는 제외.
    ([("deposit", 300, "2026-09-29T19:00:00", "2026-09-29"),
      ("deposit", 400, "2026-09-30T16:00:00", None),
      ("deposit", 500, "2026-09-30T17:00:00", "2026-10-01")], 1100, 0.10),
])
async def test_transition_return_removes_flows_between_the_two_cutoffs(temp_db, flows, d1_value, expected_r):
    await seed_user()
    await save(snap("2026-09-29", 1000, 1000, LEGACY, "2026-09-29T20:05:00.808100"))
    await save(snap("2026-09-30", d1_value, 1000, REGULAR, "2026-09-30T15:30:00.000"))
    for kind, amount, created, applied in flows:
        await add_flow(kind, amount, created, applied)
    linked = await nav_link.get_nav_history("u1")
    assert linked[1]["nav"] / linked[0]["nav"] - 1 == pytest.approx(expected_r)
    assert linked[0]["nav"] == pytest.approx(1000 / (1 + expected_r))


@pytest.mark.asyncio
async def test_distribution_between_cutoffs_counts_as_outflow(temp_db):
    await seed_user()
    await save(snap("2026-09-29", 1000, 1000, LEGACY, "2026-09-29T20:05:00"))
    await save(snap("2026-09-30", 850, 1000, REGULAR, "2026-09-30T15:30:00.000"))
    await add_distribution(100, "2026-09-30T09:00:00", "2026-09-30")
    linked = await nav_link.get_nav_history("u1")
    assert linked[1]["nav"] / linked[0]["nav"] - 1 == pytest.approx(-0.05)  # (850 + 100)/1000 − 1


@pytest.mark.asyncio
async def test_two_boundaries_compose_into_one_continuous_series(temp_db):
    await seed_user()
    rows = [
        snap("2026-01-02", 1000, 500, LEGACY), snap("2026-01-03", 1100, 550, LEGACY),
        snap("2026-01-04", 1210, 1000, REGULAR), snap("2026-01-05", 1331, 1100, REGULAR),
        snap("2026-01-06", 1464.1, 1000, "regular_close_v2"), snap("2026-01-07", 1610.51, 1100, "regular_close_v2"),
    ]
    for row in rows:
        await save(row)
    linked = await nav_link.get_nav_history("u1")
    navs = [r["return_nav"] for r in linked]
    assert [b / a for a, b in zip(navs, navs[1:])] == [pytest.approx(1.1)] * 5
    assert navs[-1] == 1100
    assert [bool(r.get("linked")) for r in linked] == [True] * 4 + [False] * 2
    assert linked[0]["nav_link_factor"] == pytest.approx(linked[2]["nav_link_factor"] * (1000 / (550 * 1.1)))


@pytest.mark.asyncio
async def test_unlinkable_boundary_hides_earlier_rows_and_warns(temp_db, caplog):
    await seed_user()
    await save(snap("2026-09-28", 1000, 1000, LEGACY))
    await save({**snap("2026-09-29", 0, 1000, LEGACY), "total_units": 0})
    await save(snap("2026-09-30", 900, 1000, REGULAR, "2026-09-30T15:30:00.000"))
    with caplog.at_level(logging.WARNING, logger="services.portfolio.nav_link"):
        linked = await nav_link.get_nav_history("u1")
    assert [r["date"] for r in linked] == ["2026-09-30"]
    assert "cannot be linked" in caplog.text
    assert len(await nav_link.get_nav_history("u1", include_legacy=True)) == 3


@pytest.mark.asyncio
async def test_single_basis_history_is_returned_unchanged(temp_db):
    await seed_user()
    await save(snap("2026-09-29", 1000, 1000, REGULAR))
    await save(snap("2026-09-30", 1100, 1100, REGULAR))
    assert await nav_link.get_nav_history("u1") == await nav_link.get_nav_history("u1", include_legacy=True)


@pytest.mark.asyncio
async def test_nav_history_route_serializes_link_markers(temp_db):
    await seed_user()
    for row in (A_D0, A_D1):
        await save(row)
    app = create_app()
    with patch.object(portfolio, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            linked = (await client.get("/api/portfolio/nav-history")).json()
            raw = (await client.get("/api/portfolio/nav-history?include_legacy=true")).json()
    assert len(linked) == len(raw) == 2
    assert linked[0]["linked"] is True and linked[0]["raw_nav"] == A_D0["nav"]
    assert linked[0]["nav"] == pytest.approx(1003.5718, abs=1e-3)
    assert "linked" not in linked[1] and "linked" not in raw[0]
    assert raw[0]["nav"] == A_D0["nav"]


# ---------------------------------------------------------------- MTD / YTD 기준점


def _period_dates():
    today = date.today()
    first = today.replace(day=1)
    month_end = first - timedelta(days=1)
    year_end = date(today.year - 1, 12, 31)
    return first.isoformat(), month_end.isoformat(), year_end.isoformat()


@pytest.mark.asyncio
async def test_period_start_links_month_end_and_year_start_across_basis_change(temp_db):
    await seed_user()
    first, month_end, year_end = _period_dates()
    if year_end != month_end:
        await save(snap(year_end, 1000, 800, LEGACY))
    await save(snap(month_end, 1200, 960, LEGACY))
    await save(snap(first, 1260, 1000, REGULAR, first + "T15:30:00.000"))  # 전환일 +5%
    k = 1000 / (960 * 1.05)

    mtd = await snapshot_views.period_start("u1")
    assert "comparison_unavailable" not in mtd
    assert mtd["linked"] is True and mtd["price_basis"] == LEGACY
    assert mtd["return_nav"] == pytest.approx(960 * k)
    assert 1000 / mtd["return_nav"] - 1 == pytest.approx(0.05)  # MTD = 전환일 수익률
    assert mtd["total_value"] == 1200  # 금액은 원래 값

    ytd = await snapshot_views.period_start("u1", yearly=True)
    assert "comparison_unavailable" not in ytd
    expected_nav = (800 if year_end != month_end else 960) * k
    assert ytd["return_nav"] == pytest.approx(expected_nav)
    assert ytd["nav"] == pytest.approx(expected_nav)


@pytest.mark.asyncio
async def test_period_start_same_basis_is_not_marked_linked(temp_db):
    await seed_user()
    first, month_end, _ = _period_dates()
    await save(snap(month_end, 1200, 960, REGULAR))
    await save(snap(first, 1260, 1008, REGULAR))
    mtd = await snapshot_views.period_start("u1")
    assert "linked" not in mtd and mtd["nav"] == 960


@pytest.mark.asyncio
async def test_period_start_unlinkable_boundary_keeps_comparison_unavailable(temp_db):
    await seed_user()
    first, month_end, _ = _period_dates()
    await save({**snap(month_end, 0, 960, LEGACY), "total_units": 0})
    await save(snap(first, 1260, 1000, REGULAR))
    mtd = await snapshot_views.period_start("u1")
    assert mtd["comparison_unavailable"] is True
