"""scripts/repair_nav_history.py 의 유닛 검증·보정 로직 테스트.

전체 이력을 원장에서 재구성하지 않는다 — 입출금 추적 이전 시대의 유닛
성장은 원장에 없으므로 저장 시계열이 진실이다. 기록된 입출금의
units_change 가 스냅샷 유닛 델타에 나타나는지만 대조하고, 누락분을 그
시점부터 누적 보정한다.
"""

import importlib.util
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "repair_nav_history.py"
spec = importlib.util.spec_from_file_location("repair_nav_history", SCRIPT_PATH)
repair_nav_history = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(repair_nav_history)

rebuild_user_units = repair_nav_history.rebuild_user_units


def _snap(date, value, nav, units):
    return {"date": date, "total_value": value, "nav": nav, "total_units": units}


def _cf(id, date, type_, amount, units_change=None, nav_at_time=None, created_at=""):
    return {
        "id": id, "date": date, "type": type_, "amount": amount,
        "units_change": units_change, "nav_at_time": nav_at_time,
        "created_at": created_at, "applied_snapshot_date": None,
    }


def test_lost_deposit_units_are_restored_into_later_snapshots():
    """정산이 놓친 주말 입금: units_change 는 기록돼 있지만(구 라우트가
    미리 계산) 분모에 들어가지 못한 케이스 — 그 시점부터 누적 보정."""
    snapshots = [
        _snap("2026-05-15", 10_000_000, 1000.0, 10_000),
        # 토요일 입금(+1,000 유닛)이 유실된 월요일: 유닛 그대로, NAV 가짜 상승
        _snap("2026-05-18", 12_000_000, 1200.0, 10_000),
        _snap("2026-05-19", 12_100_000, 1210.0, 10_000),
    ]
    cashflows = [_cf(1, "2026-05-16", "deposit", 1_000_000, units_change=1000.0, nav_at_time=1000.0)]

    plan = rebuild_user_units(snapshots, cashflows)

    assert [u["date"] for u in plan["snapshot_updates"]] == ["2026-05-18", "2026-05-19"]
    fix = plan["snapshot_updates"][0]
    assert abs(fix["new_units"] - 11_000) < 1e-6
    assert abs(fix["new_nav"] - 12_000_000 / 11_000) < 1e-9
    # 이후 날짜에도 보정이 누적 유지된다.
    assert abs(plan["snapshot_updates"][1]["new_units"] - 11_000) < 1e-6
    assert plan["cashflow_updates"][0]["applied_snapshot_date"] == "2026-05-18"
    # 기록된 units_change 는 소급 재해석하지 않는다.
    assert plan["cashflow_updates"][0]["units_change"] == 1000.0


def test_applied_cashflow_matching_stored_delta_needs_no_fix():
    snapshots = [
        _snap("2026-05-15", 10_000_000, 1000.0, 10_000),
        _snap("2026-05-18", 12_100_000, 1100.0, 11_000),  # 델타 +1,000 = 반영됨
    ]
    cashflows = [_cf(1, "2026-05-16", "deposit", 1_100_000, units_change=1000.0, nav_at_time=1100.0)]

    plan = rebuild_user_units(snapshots, cashflows)

    assert plan["snapshot_updates"] == []
    assert plan["cashflow_updates"][0]["applied_snapshot_date"] == "2026-05-18"


def test_pre_ledger_unit_growth_is_left_alone():
    """입출금 추적 이전 시대: 원장 없이 유닛이 성장한 구간은 저장 시계열이
    진실 — 재구성하거나 보정하면 안 된다. (실데이터에서 112만→669만 성장)"""
    snapshots = [
        _snap("2019-03-13", 359_000_000, 319.46, 1_122_686),
        _snap("2020-06-01", 900_000_000, 450.0, 2_000_000),
        _snap("2021-01-04", 2_100_000_000, 700.0, 3_000_000),
    ]
    plan = rebuild_user_units(snapshots, [])
    assert plan["snapshot_updates"] == []


def test_backfill_jitter_below_tolerance_is_ignored():
    """백필 시대 저장 유닛의 ±수 유닛 잔떨림은 누락이 아니다."""
    snapshots = [
        _snap("2019-03-13", 359_000_000, 319.46, 1_122_686.64),
        _snap("2019-03-14", 359_100_000, 319.94, 1_122_688.27),  # +1.6 유닛 잔떨림
        _snap("2019-03-16", 360_100_000, 320.0, 1_122_690.00),
    ]
    # 3/15 입금이 정상 반영됐고 저장 델타(+1.73)와 units_change(+3.0)가
    # 잔떨림 수준(<5)에서만 어긋나는 경우 — 보정하지 않는다.
    cashflows = [_cf(1, "2019-03-15", "deposit", 1_000, units_change=3.0, nav_at_time=320.0)]
    plan = rebuild_user_units(snapshots, cashflows)
    assert plan["snapshot_updates"] == []


def test_null_units_cashflow_is_left_pending():
    """신규 라우트 입력(units 미정) 행은 미정산 상태 그대로 — 수정된
    정산이 다음 실행에서 집어간다. 스크립트가 손대면 안 된다."""
    snapshots = [
        _snap("2026-05-15", 10_000_000, 1000.0, 10_000),
        _snap("2026-05-18", 12_100_000, 1210.0, 10_000),
    ]
    cashflows = [_cf(1, "2026-05-16", "deposit", 1_100_000)]  # units_change None

    plan = rebuild_user_units(snapshots, cashflows)

    assert plan["snapshot_updates"] == []
    assert plan["cashflow_updates"] == []


def test_cashflows_before_first_snapshot_are_marked_but_not_issued():
    snapshots = [_snap("2026-05-15", 10_000_000, 1000.0, 10_000)]
    cashflows = [_cf(1, "2026-05-10", "deposit", 5_000_000, units_change=5000.0, nav_at_time=1000.0)]

    plan = rebuild_user_units(snapshots, cashflows)

    assert plan["snapshot_updates"] == []
    assert plan["cashflow_updates"][0]["applied_snapshot_date"] == "2026-05-15"


def test_cashflow_after_last_snapshot_stays_pending():
    snapshots = [_snap("2026-05-15", 10_000_000, 1000.0, 10_000)]
    cashflows = [_cf(1, "2026-05-20", "deposit", 1_000_000, units_change=1000.0)]

    plan = rebuild_user_units(snapshots, cashflows)

    assert plan["snapshot_updates"] == []
    assert plan["cashflow_updates"] == []  # 다음 정산의 몫


def test_lost_withdrawal_units_are_clawed_back():
    snapshots = [
        _snap("2026-05-15", 10_000_000, 1000.0, 10_000),
        _snap("2026-05-18", 8_900_000, 890.0, 10_000),  # 출금 유닛 미회수 → NAV 가짜 하락
    ]
    cashflows = [_cf(1, "2026-05-16", "withdrawal", 1_100_000, units_change=-1100.0, nav_at_time=1000.0)]

    plan = rebuild_user_units(snapshots, cashflows)

    fix = plan["snapshot_updates"][0]
    assert abs(fix["new_units"] - 8_900) < 1e-6
    assert abs(fix["new_nav"] - 1000.0) < 1e-9


def test_backdated_lost_deposit_corrects_from_entry_settlement_not_nominal_date():
    """소급 입력 유실분(8/24 자로 적고 8/26 에 입력): 현금이 평가액에 들어온
    건 입력 시점 이후 첫 정산이므로 보정도 거기부터 — 명목일부터 걸면
    그 사이 NAV 가 반대로 과소 왜곡된다."""
    snapshots = [
        _snap("2026-08-21", 6_477_000_000, 969.15, 6_682_900),
        _snap("2026-08-24", 6_470_000_000, 968.21, 6_682_900),
        _snap("2026-08-25", 6_500_000_000, 972.59, 6_682_900),
        _snap("2026-08-26", 6_914_000_000, 1034.57, 6_682_900),  # 입금액 유입 + 유닛 유실
    ]
    cashflows = [_cf(
        1, "2026-08-24", "deposit", 383_962_000,
        units_change=394_781.22, nav_at_time=972.59,
        created_at="2026-08-26T14:24:00",
    )]

    plan = rebuild_user_units(snapshots, cashflows)

    # 8/24·8/25 는 건드리지 않고 8/26 만 보정.
    assert [u["date"] for u in plan["snapshot_updates"]] == ["2026-08-26"]
    fix = plan["snapshot_updates"][0]
    assert abs(fix["new_units"] - (6_682_900 + 394_781.22)) < 1e-6
    assert plan["cashflow_updates"][0]["applied_snapshot_date"] == "2026-08-26"


def test_lost_flow_entered_after_last_settlement_is_reset_to_pending():
    """마지막 정산 이후 입력된 유실 행: 현금이 아직 어떤 스냅샷에도 평가되지
    않았으므로 보정 대상이 아니다 — 미정산으로 되돌려 다음 정산이 발행."""
    snapshots = [
        _snap("2026-08-25", 6_500_000_000, 972.59, 6_682_900),
        _snap("2026-08-26", 6_510_000_000, 974.09, 6_682_900),
    ]
    cashflows = [_cf(
        1, "2026-08-26", "deposit", 100_000_000,
        units_change=102_800.0, nav_at_time=972.59,
        created_at="2026-08-26T22:30:00",  # 20:00 정산 이후
    )]

    plan = rebuild_user_units(snapshots, cashflows)

    assert plan["snapshot_updates"] == []
    cu = plan["cashflow_updates"][0]
    assert cu["applied_snapshot_date"] is None
    assert cu["units_change"] is None
    assert cu["nav_at_time"] is None


def test_repair_is_idempotent_after_created_at_reassignment():
    """교정 1회차가 유실분을 created_at 정산(8/26)에 반영하고 나면, 2회차는
    applied_snapshot_date 기준으로 그 정산의 델타(+394,781)를 확인하고
    아무것도 제안하지 않아야 한다 — 명목일(8/24) 델타 0 을 또 유실로
    오판하면 이중 보정으로 데이터가 망가진다."""
    snapshots = [
        _snap("2026-08-24", 6_470_000_000, 992.92, 6_516_624.50),
        _snap("2026-08-25", 6_500_000_000, 997.41, 6_516_624.50),
        _snap("2026-08-26", 6_914_000_000, 1000.36, 6_516_624.50 + 394_781.22),
    ]
    cf = _cf(
        1, "2026-08-24", "deposit", 383_962_000,
        units_change=394_781.22, nav_at_time=972.59,
        created_at="2026-08-26T14:09:54",
    )
    cf["applied_snapshot_date"] = "2026-08-26"  # 1회차 교정 결과

    plan = rebuild_user_units(snapshots, [cf])

    assert plan["snapshot_updates"] == []
