"""NAV 이력 재계산 — 정산이 놓친 입출금 유닛 교정.

applied_snapshot_date 도입(2026-08) 이전 정산은 '정산일 == date 정확 일치
+ 그날 첫 정산'인 입출금만 유닛으로 전환했다. 주말 입금(토·일 date),
20:05 정산 이후 입력분, 소급 입력분은 total_units 에 영구 누락돼 다음
정산에서 NAV 가 입금액만큼 가짜 상승(출금은 하락)했다.

스냅샷의 total_value 시계열은 시세 기반 원장(ground truth)이고 유닛·NAV 는
파생값이므로, 입출금 원장과 함께 유닛 시계열을 결정적으로 재구성한다:

- 첫 스냅샷의 (total_units, nav) 는 앵커로 신뢰한다. date <= 첫 스냅샷
  날짜인 입출금은 이미 유닛에 녹아 있는 것으로 간주(반영 완료 마킹만).
- 각 입출금은 date <= 정산일 인 **첫** 스냅샷에 귀속된다.
- units_change 가 기록된 행은 그 값을 그대로 쓴다 (당시 정산·구 라우트가
  계산한 값 — 과거 정책을 소급 재해석하지 않는다). NULL 인 행만 해당
  정산의 ex-cashflow NAV 로 새로 계산한다.
- 재구성한 유닛이 저장값과 다른 스냅샷만 total_units·nav 를 갱신하고,
  모든 입출금의 applied_snapshot_date 를 귀속 정산일로 교정한다.

사용:
    python3 scripts/repair_nav_history.py            # dry-run (기본)
    python3 scripts/repair_nav_history.py --apply    # 실제 반영
    python3 scripts/repair_nav_history.py --user 접두어  # 특정 사용자만
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from repositories import bootstrap  # noqa: E402
from repositories import db as db_repo  # noqa: E402

BASE_NAV = 1000.0
UNITS_EPS = 1e-6


def _signed(cf: dict) -> float:
    return cf["amount"] if cf["type"] == "deposit" else -cf["amount"]


def rebuild_user_units(snapshots: list[dict], cashflows: list[dict]) -> dict:
    """유닛 시계열 재구성 (순수 함수 — 테스트 대상).

    Returns {
      "snapshot_updates": [{date, old_units, new_units, old_nav, new_nav}],
      "cashflow_updates": [{id, applied_snapshot_date, units_change, nav_at_time}],
    }
    스냅샷이 없으면 빈 결과.
    """
    result = {"snapshot_updates": [], "cashflow_updates": []}
    if not snapshots:
        return result
    snaps = sorted(snapshots, key=lambda s: s["date"])
    flows = sorted(cashflows, key=lambda c: (c["date"], c.get("created_at") or "", c["id"]))

    first = snaps[0]
    units = float(first["total_units"] or 0)
    if units <= 0:
        units = float(first["total_value"]) / BASE_NAV
        result["snapshot_updates"].append({
            "date": first["date"],
            "old_units": first["total_units"], "new_units": units,
            "old_nav": first["nav"], "new_nav": BASE_NAV,
        })
    fi = 0
    # 첫 스냅샷 이전/당일 입출금: 유닛에 이미 반영된 것으로 간주.
    while fi < len(flows) and flows[fi]["date"] <= first["date"]:
        result["cashflow_updates"].append({
            "id": flows[fi]["id"],
            "applied_snapshot_date": first["date"],
            "units_change": flows[fi]["units_change"],
            "nav_at_time": flows[fi]["nav_at_time"],
        })
        fi += 1

    prev_nav = float(first["nav"] or BASE_NAV)
    for snap in snaps[1:]:
        pending = []
        while fi < len(flows) and flows[fi]["date"] <= snap["date"]:
            pending.append(flows[fi])
            fi += 1
        total_value = float(snap["total_value"])
        net_fresh = sum(_signed(cf) for cf in pending if cf["units_change"] is None)
        issue_nav = prev_nav
        if units > 0:
            candidate = (total_value - net_fresh) / units
            if candidate > 0:
                issue_nav = candidate
        for cf in pending:
            if cf["units_change"] is not None:
                delta = float(cf["units_change"])
                nav_at = cf["nav_at_time"]
            elif issue_nav > 0:
                delta = _signed(cf) / issue_nav
                nav_at = issue_nav
            else:
                delta = 0.0
                nav_at = cf["nav_at_time"]
            units += delta
            result["cashflow_updates"].append({
                "id": cf["id"],
                "applied_snapshot_date": snap["date"],
                "units_change": delta if cf["units_change"] is None else cf["units_change"],
                "nav_at_time": nav_at,
            })
        if units > 0:
            new_nav = total_value / units
        else:
            new_nav = BASE_NAV
            units = total_value / BASE_NAV if total_value > 0 else 0.0
        if abs(units - float(snap["total_units"] or 0)) > UNITS_EPS:
            result["snapshot_updates"].append({
                "date": snap["date"],
                "old_units": snap["total_units"], "new_units": units,
                "old_nav": snap["nav"], "new_nav": new_nav,
            })
        prev_nav = new_nav

    # 마지막 스냅샷 이후 date 의 입출금은 아직 미반영이 정상 — 건드리지
    # 않는다 (다음 정산이 집어간다).
    return result


async def repair(apply: bool, user_prefix: str | None) -> None:
    db = await db_repo.get_db()
    cursor = await db.execute("SELECT DISTINCT google_sub FROM portfolio_snapshots ORDER BY google_sub")
    users = [row["google_sub"] for row in await cursor.fetchall()]
    if user_prefix:
        users = [u for u in users if u.startswith(user_prefix)]

    total_snap_fixes = 0
    for sub in users:
        cursor = await db.execute(
            "SELECT date, total_value, nav, total_units FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date",
            (sub,),
        )
        snapshots = [dict(r) for r in await cursor.fetchall()]
        cursor = await db.execute(
            "SELECT id, date, type, amount, nav_at_time, units_change, applied_snapshot_date, created_at "
            "FROM portfolio_cashflows WHERE google_sub = ? ORDER BY date, created_at, id",
            (sub,),
        )
        cashflows = [dict(r) for r in await cursor.fetchall()]

        plan = rebuild_user_units(snapshots, cashflows)
        if not plan["snapshot_updates"] and not any(
            cf["applied_snapshot_date"] != next(
                (u["applied_snapshot_date"] for u in plan["cashflow_updates"] if u["id"] == cf["id"]), cf["applied_snapshot_date"]
            )
            for cf in cashflows
        ):
            print(f"{sub[:8]}: OK (스냅샷 {len(snapshots)}개, 입출금 {len(cashflows)}건 — 변경 없음)")
            continue

        print(f"{sub[:8]}: 스냅샷 수정 {len(plan['snapshot_updates'])}건")
        for u in plan["snapshot_updates"]:
            print(
                f"  {u['date']}: units {u['old_units']:.4f} -> {u['new_units']:.4f}, "
                f"nav {u['old_nav']:.2f} -> {u['new_nav']:.2f}"
            )
        total_snap_fixes += len(plan["snapshot_updates"])

        if apply:
            async with db_repo.transaction() as tdb:
                for u in plan["snapshot_updates"]:
                    await tdb.execute(
                        "UPDATE portfolio_snapshots SET total_units = ?, nav = ? WHERE google_sub = ? AND date = ?",
                        (u["new_units"], u["new_nav"], sub, u["date"]),
                    )
                for cu in plan["cashflow_updates"]:
                    await tdb.execute(
                        "UPDATE portfolio_cashflows SET applied_snapshot_date = ?, units_change = ?, nav_at_time = ? WHERE id = ?",
                        (cu["applied_snapshot_date"], cu["units_change"], cu["nav_at_time"], cu["id"]),
                    )
            print("  -> 반영 완료")
    mode = "APPLY" if apply else "DRY-RUN"
    print(f"[{mode}] 사용자 {len(users)}명, 스냅샷 수정 {total_snap_fixes}건")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="실제 UPDATE 실행 (기본은 dry-run)")
    parser.add_argument("--user", help="google_sub 접두어로 대상 사용자 제한")
    args = parser.parse_args()
    await bootstrap.init_db()
    try:
        await repair(apply=args.apply, user_prefix=args.user)
    finally:
        await bootstrap.close_db()


if __name__ == "__main__":
    asyncio.run(main())
