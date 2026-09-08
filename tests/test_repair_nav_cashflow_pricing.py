import copy

import pytest

from scripts.repair_nav_cashflow_pricing import rebuild_pricing


def snapshot(day, value, units):
    return {"date": day, "total_value": value, "total_units": units, "nav": value / units}


def flow(cf_id, day, amount, units, applied=None):
    return {"id": cf_id, "date": day, "type": "deposit" if amount > 0 else "withdrawal",
            "amount": abs(amount), "units_change": units, "nav_at_time": abs(amount / units) if units else None,
            "applied_snapshot_date": applied, "created_at": day + "T12:00:00"}


def test_repair_uses_settlement_value_and_preserves_cash_and_market_values():
    snaps = [snapshot("2026-05-15", 10000, 10), snapshot("2026-05-18", 12100, 11.1), snapshot("2026-05-19", 12320, 11.1)]
    flows = [flow(1, "2026-05-16", 1100, 1.1, "2026-05-18"), flow(2, "2026-05-20", -200, None)]
    original = copy.deepcopy((snaps, flows))
    plan = rebuild_pricing(snaps, flows)
    assert plan["snapshots"][0]["new_nav"] == pytest.approx(1100)
    assert plan["snapshots"][1]["new_nav"] == pytest.approx(1120)
    assert plan["cashflows"][0]["new_units"] == pytest.approx(1)
    assert len(plan["cashflows"]) == 1  # 미정산 거래는 다음 정산의 몫.
    assert (snaps, flows) == original


def test_repair_is_idempotent_after_applying_its_plan():
    snaps = [snapshot("2026-05-15", 10000, 10), snapshot("2026-05-18", 12100, 11.1), snapshot("2026-05-19", 12320, 11.1)]
    flows = [flow(1, "2026-05-16", 1100, 1.1, "2026-05-18")]
    plan = rebuild_pricing(snaps, flows)
    for correction in plan["snapshots"]:
        row = next(s for s in snaps if s["date"] == correction["date"])
        row.update(nav=correction["new_nav"], total_units=correction["new_units"])
    for correction in plan["cashflows"]:
        row = next(f for f in flows if f["id"] == correction["id"])
        row.update(nav_at_time=correction["new_nav"], units_change=correction["new_units"])
    assert rebuild_pricing(snaps, flows) == {"snapshots": [], "cashflows": []}


def test_backdated_entry_does_not_change_nav_before_actual_settlement():
    snaps = [snapshot("2026-05-15", 10000, 10), snapshot("2026-05-18", 11000, 10), snapshot("2026-05-19", 12100, 11.1)]
    flows = [flow(1, "2026-05-16", 1100, 1.1, "2026-05-19")]
    plan = rebuild_pricing(snaps, flows)
    assert [r["date"] for r in plan["snapshots"]] == ["2026-05-19"]
    assert plan["snapshots"][0]["new_nav"] == pytest.approx(1100)


def test_repair_refuses_unexplained_units_instead_of_guessing():
    snaps = [snapshot("2026-05-15", 10000, 10), snapshot("2026-05-18", 12100, 11.1), snapshot("2026-05-19", 12320, 15)]
    flows = [flow(1, "2026-05-16", 1100, 1.1, "2026-05-18")]
    with pytest.raises(ValueError, match="원장으로 설명되지 않는"):
        rebuild_pricing(snaps, flows)


def test_deposit_and_withdrawal_same_day_are_both_repriced():
    snaps = [snapshot("2026-05-15", 10000, 10), snapshot("2026-05-18", 11000, 10)]
    flows = [flow(1, "2026-05-18", 1000, 1, "2026-05-18"), flow(2, "2026-05-18", -1000, -1, "2026-05-18")]
    plan = rebuild_pricing(snaps, flows)
    assert plan["snapshots"] == []
    assert [r["new_units"] for r in plan["cashflows"]] == pytest.approx([1000 / 1100, -1000 / 1100])
