"""scripts/repair_nav_history.py 의 유닛 시계열 재구성 로직 테스트.

과거 정산이 놓친 입출금(주말/정산 후/소급 입력)의 유닛을 스냅샷 이력
replay 로 찾아 교정하는 순수 함수 rebuild_user_units 를 고정한다.
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


def test_lost_legacy_deposit_units_are_restored_into_later_snapshots():
    """구버전에서 유실된 주말 입금: units_change 는 기록돼 있지만(라우트가
    미리 계산) 정산 분모에는 들어가지 못한 케이스 — 그 값 그대로 복원."""
    snapshots = [
        _snap("2026-05-15", 10000, 1000.0, 10.0),
        # 토요일 입금 1000 (units_change=1.0 기록)이 유실된 월요일 정산:
        # 저장된 units 10 그대로, nav 가 1200 으로 가짜 상승했다.
        _snap("2026-05-18", 12000, 1200.0, 10.0),
    ]
    cashflows = [_cf(1, "2026-05-16", "deposit", 1000, units_change=1.0, nav_at_time=1000.0)]

    plan = rebuild_user_units(snapshots, cashflows)

    assert len(plan["snapshot_updates"]) == 1
    fix = plan["snapshot_updates"][0]
    assert fix["date"] == "2026-05-18"
    assert abs(fix["new_units"] - 11.0) < 1e-9
    assert abs(fix["new_nav"] - 12000 / 11.0) < 1e-9
    assert plan["cashflow_updates"][0]["applied_snapshot_date"] == "2026-05-18"
    # 기록된 units_change 는 소급 재해석하지 않는다.
    assert plan["cashflow_updates"][0]["units_change"] == 1.0


def test_null_units_cashflow_is_issued_at_ex_cashflow_nav():
    snapshots = [
        _snap("2026-05-15", 10000, 1000.0, 10.0),
        _snap("2026-05-18", 12100, 1210.0, 10.0),  # 입금 1100 유실 상태
    ]
    cashflows = [_cf(1, "2026-05-16", "deposit", 1100)]

    plan = rebuild_user_units(snapshots, cashflows)

    # 발행 NAV = (12100-1100)/10 = 1100 → 1.0 unit
    cu = plan["cashflow_updates"][0]
    assert abs(cu["units_change"] - 1.0) < 1e-9
    assert abs(cu["nav_at_time"] - 1100.0) < 1e-9
    fix = plan["snapshot_updates"][0]
    assert abs(fix["new_units"] - 11.0) < 1e-9
    assert abs(fix["new_nav"] - 1100.0) < 1e-9


def test_correct_history_produces_no_snapshot_updates():
    snapshots = [
        _snap("2026-05-15", 10000, 1000.0, 10.0),
        _snap("2026-05-18", 12100, 1100.0, 11.0),  # 이미 올바르게 반영됨
    ]
    cashflows = [_cf(1, "2026-05-16", "deposit", 1100, units_change=1.0, nav_at_time=1100.0)]

    plan = rebuild_user_units(snapshots, cashflows)

    assert plan["snapshot_updates"] == []
    # 귀속 정산일 마킹은 여전히 교정된다.
    assert plan["cashflow_updates"][0]["applied_snapshot_date"] == "2026-05-18"


def test_cashflows_before_first_snapshot_are_marked_but_not_issued():
    snapshots = [_snap("2026-05-15", 10000, 1000.0, 10.0)]
    cashflows = [_cf(1, "2026-05-10", "deposit", 5000)]

    plan = rebuild_user_units(snapshots, cashflows)

    assert plan["snapshot_updates"] == []
    assert plan["cashflow_updates"][0]["applied_snapshot_date"] == "2026-05-15"
    # 첫 스냅샷 유닛(value/BASE_NAV)에 이미 녹아 있으므로 발행 없음.
    assert plan["cashflow_updates"][0]["units_change"] is None


def test_cashflow_after_last_snapshot_stays_pending():
    snapshots = [_snap("2026-05-15", 10000, 1000.0, 10.0)]
    cashflows = [_cf(1, "2026-05-20", "deposit", 1000)]

    plan = rebuild_user_units(snapshots, cashflows)

    assert plan["snapshot_updates"] == []
    assert plan["cashflow_updates"] == []  # 다음 정산의 몫


def test_withdrawal_units_are_negative():
    snapshots = [
        _snap("2026-05-15", 10000, 1000.0, 10.0),
        _snap("2026-05-18", 8900, 890.0, 10.0),  # 출금 1100 유실
    ]
    cashflows = [_cf(1, "2026-05-16", "withdrawal", 1100)]

    plan = rebuild_user_units(snapshots, cashflows)

    cu = plan["cashflow_updates"][0]
    # 발행 NAV = (8900+1100)/10 = 1000 → -1.1 units
    assert abs(cu["units_change"] + 1.1) < 1e-9
    fix = plan["snapshot_updates"][0]
    assert abs(fix["new_units"] - 8.9) < 1e-9
    assert abs(fix["new_nav"] - 1000.0) < 1e-9
