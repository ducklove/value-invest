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


# ---------------------------------------------------------------- 정산 브리핑(regular_performance)


@pytest.mark.asyncio
async def test_regular_performance_first_new_basis_day_matches_linked_history(temp_db):
    """운영 경계 수치: 9/30 브리핑 성과 = 연결 이력의 전환일 수익률(−0.356%)."""
    await seed_user()
    await save(A_D0)
    await save(A_D1)
    summary = await snapshot_views.regular_performance("u1", A_D1["date"])
    linked = await nav_link.get_nav_history("u1")
    assert summary["comparison_unavailable"] is False
    assert summary["prev_date"] == A_D0["date"]
    assert summary["change_pct"] == pytest.approx(-0.35591314, abs=1e-7)
    assert summary["change_pct"] == pytest.approx((linked[1]["return_nav"] / linked[0]["return_nav"] - 1) * 100)
    assert summary["change_krw"] == pytest.approx(A_D1["total_value"] - A_D0["total_value"])
    assert summary["prev_nav_link_factor"] == pytest.approx(1.01732723, abs=1e-7)


@pytest.mark.asyncio
async def test_regular_performance_boundary_removes_flows_between_cutoffs(temp_db):
    await seed_user()
    await save(snap("2026-09-29", 1000, 1000, LEGACY, "2026-09-29T20:05:00.808100"))
    await save(snap("2026-09-30", 1600, 1000, REGULAR, "2026-09-30T15:30:00.000"))
    # 전일 20:05 정산 뒤 ~ 금일 정산 전 입금(미반영이어도 잔고에 들어 있음)
    await add_flow("deposit", 500, "2026-09-29T21:00:00", None)
    summary = await snapshot_views.regular_performance("u1", "2026-09-30")
    assert summary["net_cashflow"] == 500
    assert summary["change_krw"] == pytest.approx(100)
    assert summary["change_pct"] == pytest.approx(10.0)
    linked = await nav_link.get_nav_history("u1")
    assert summary["change_pct"] == pytest.approx((linked[1]["nav"] / linked[0]["nav"] - 1) * 100)


@pytest.mark.asyncio
async def test_regular_performance_unlinkable_boundary_stays_unavailable(temp_db):
    await seed_user()
    await save({**snap("2026-09-29", 0, 960, LEGACY), "total_units": 0})
    await save(snap("2026-09-30", 1260, 1000, REGULAR, "2026-09-30T15:30:00.000"))
    summary = await snapshot_views.regular_performance("u1", "2026-09-30")
    assert summary["comparison_unavailable"] is True
    assert summary["change_pct"] is None and summary["change_krw"] is None and summary["prev_date"] is None


# ---------------------------------------------------------------- 입출금 내역 발행 NAV


K_A = 1.017327233057


async def add_issued_flow(day, nav_at_time, units, applied, *, kind="deposit", amount=1000.0, user="u1"):
    async with transaction() as db:
        await db.execute(
            "INSERT INTO portfolio_cashflows (google_sub,date,type,amount,nav_at_time,units_change,applied_snapshot_date,created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (user, day, kind, amount, nav_at_time, units, applied, f"{day}T10:00:00"),
        )


def test_cumulative_factor_matches_apply_links():
    factors = {"2026-09-30": 1.5, "2026-10-10": 2.0}
    assert nav_link.cumulative_factor("2026-10-10", factors) == 1.0
    assert nav_link.cumulative_factor("2026-10-01", factors) == 2.0
    assert nav_link.cumulative_factor("2026-09-29", factors) == 3.0
    assert nav_link.cumulative_factor("2026-09-29", {"2026-09-30": None, "2026-10-10": 2.0}) is None
    assert nav_link.cumulative_factor("2026-10-01", {"2026-09-30": None, "2026-10-10": 2.0}) == 2.0
    rows = [{"date": d, "nav": 100.0} for d in ("2026-09-29", "2026-10-01", "2026-10-10")]
    linked = nav_link.apply_links(rows, factors)
    assert [r["nav"] for r in linked] == [100.0 * nav_link.cumulative_factor(r["date"], factors) for r in rows]


@pytest.mark.asyncio
async def test_cashflow_nav_links_flows_before_boundary_with_real_factor(temp_db):
    await seed_user()
    for row in (A_D0, A_D1):
        await save(row)
    await add_issued_flow("2026-09-29", A_D0["nav"], 1000 / A_D0["nav"], "2026-09-29")
    await add_issued_flow("2026-09-28", 980.0, -2.0, None, kind="withdrawal")  # 미반영 preset → date 로 구간 판정
    # 새 구간 반영분은 경계 두 정산 사이가 아니어서 k 에 영향이 없다(실제 k 유지).
    await add_issued_flow("2026-10-01", 1000.0, 1.0, "2026-10-01")
    await add_issued_flow("2026-09-28", None, None, "2026-09-28")  # NAV 없는 행은 경계 이전이라도 None
    await add_distribution(500, "2026-09-29T11:00:00", "2026-09-29")
    raw = await snapshots.get_cashflows("u1")
    linked = await nav_link.link_cashflows("u1", raw)
    by_id = {r["id"]: r for r in linked}
    raw_by_id = {r["id"]: r for r in raw}

    old = by_id[1]
    assert old["nav_link_factor"] == pytest.approx(K_A, abs=1e-9)
    assert old["nav_at_time"] == pytest.approx(1003.57184409, abs=1e-6)
    assert old["raw_nav_at_time"] == A_D0["nav"]
    assert old["units_change"] == raw_by_id[1]["units_change"] == pytest.approx(1000 / A_D0["nav"])
    # 연결된 NAV 이력의 9/29 값과 같다.
    history = {r["date"]: r for r in await nav_link.get_nav_history("u1")}
    assert old["nav_at_time"] == pytest.approx(history["2026-09-29"]["nav"])

    pending = by_id[2]
    assert pending["nav_at_time"] == pytest.approx(980.0 * K_A) and pending["units_change"] == -2.0

    for cid in (3, 4, -1):
        assert by_id[cid] == raw_by_id[cid]
        assert "raw_nav_at_time" not in by_id[cid] and "nav_link_factor" not in by_id[cid]
    assert by_id[3]["nav_at_time"] == 1000.0
    assert by_id[4]["nav_at_time"] is None and by_id[-1]["nav_at_time"] is None

    # 저장 행은 그대로다.
    assert {r["id"]: r["nav_at_time"] for r in await snapshots.get_cashflows("u1")}[1] == A_D0["nav"]


@pytest.mark.asyncio
async def test_cashflow_nav_unchanged_without_boundary(temp_db):
    await seed_user()
    await save(snap("2026-09-29", 1000, 1000, REGULAR))
    await save(snap("2026-09-30", 1100, 1100, REGULAR))
    await add_issued_flow("2026-09-29", 1000.0, 1.0, "2026-09-29")
    raw = await snapshots.get_cashflows("u1")
    assert await nav_link.link_cashflows("u1", raw) == raw


@pytest.mark.asyncio
async def test_cashflow_nav_unlinkable_boundary_blanks_older_nav(temp_db):
    await seed_user()
    await save(snap("2026-09-28", 1000, 1000, LEGACY))
    await save({**snap("2026-09-29", 0, 1000, LEGACY), "total_units": 0})
    await save(snap("2026-09-30", 900, 1000, REGULAR, "2026-09-30T15:30:00.000"))
    await add_issued_flow("2026-09-28", 1000.0, 1.0, "2026-09-28")
    await add_issued_flow("2026-09-30", 1000.0, 1.0, "2026-09-30")
    by_id = {r["id"]: r for r in await nav_link.link_cashflows("u1", await snapshots.get_cashflows("u1"))}
    assert by_id[1]["nav_at_time"] is None and by_id[1]["nav_link_unavailable"] is True
    assert by_id[1]["raw_nav_at_time"] == 1000.0 and by_id[1]["units_change"] == 1.0
    assert by_id[2]["nav_at_time"] == 1000.0 and "nav_link_unavailable" not in by_id[2]


@pytest.mark.asyncio
async def test_cashflows_route_serves_linked_nav_with_raw_marker(temp_db):
    await seed_user()
    for row in (A_D0, A_D1):
        await save(row)
    await add_issued_flow("2026-09-29", A_D0["nav"], 1.5, "2026-09-29")
    await add_issued_flow("2026-10-01", 1000.0, 1.0, "2026-10-01")
    app = create_app()
    with patch.object(portfolio, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            rows = {r["id"]: r for r in (await client.get("/api/portfolio/cashflows")).json()}
    assert rows[1]["nav_at_time"] == pytest.approx(1003.57184409, abs=1e-6)
    assert rows[1]["raw_nav_at_time"] == A_D0["nav"]
    assert rows[1]["nav_link_factor"] == pytest.approx(K_A, abs=1e-9)
    assert rows[1]["units_change"] == 1.5
    assert rows[2]["nav_at_time"] == 1000.0 and "raw_nav_at_time" not in rows[2]
