"""NAV 이력 재계산 — 정산이 놓친 입출금 유닛 교정.

applied_snapshot_date 도입(2026-08) 이전 정산은 '정산일 == date 정확 일치
+ 그날 첫 정산'인 입출금만 유닛으로 전환했다. 주말 입금(토·일 date),
20:05 정산 이후 입력분, 소급 입력분은 total_units 에 영구 누락돼 다음
정산에서 NAV 가 입금액만큼 가짜 상승(출금은 하락)했다.

주의: 전체 이력을 원장에서 재구성하지 **않는다**. 입출금 추적(2026-04)
이전의 유닛 성장은 스냅샷 시계열에만 존재하고 원장에는 없으므로, 저장된
유닛 시계열을 기본 진실로 두고 **기록된 입출금만 검증**한다:

- 각 입출금은 date <= 정산일 인 **첫** 스냅샷에 귀속된다.
- 그 정산의 저장 유닛 델타(stored_units - 직전 stored_units)와 귀속
  입출금의 units_change 합을 대조한다. 차이가 유의하면(절대 5유닛 초과
  그리고 합의 1% 초과) 누락으로 판단, 그 시점부터 누적 보정치에 더한다.
- 보정치가 붙은 스냅샷만 total_units += 보정, nav = total_value/유닛
  으로 갱신한다. 원장 밖의 유닛 변화(추적 이전 시대·백필 잔떨림)는
  건드리지 않는다.
- units_change 가 NULL 인 행(신규 라우트 입력, 아직 미정산)은 손대지
  않는다 — 수정된 정산이 다음 실행에서 집어간다.
- 검증한 입출금의 applied_snapshot_date 를 귀속 정산일로 교정한다.

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
    """기록된 입출금의 유닛이 스냅샷 시계열에 반영됐는지 검증·보정 (순수 함수).

    Returns {
      "snapshot_updates": [{date, old_units, new_units, old_nav, new_nav}],
      "cashflow_updates": [{id, applied_snapshot_date, units_change, nav_at_time}],
    }
    스냅샷이 없으면 빈 결과. units_change 가 NULL 인 입출금은 미정산
    상태 그대로 두고 결과에 포함하지 않는다.
    """
    result = {"snapshot_updates": [], "cashflow_updates": []}
    if not snapshots:
        return result
    snaps = sorted(snapshots, key=lambda s: s["date"])
    flows = sorted(
        [cf for cf in cashflows if cf["units_change"] is not None],
        key=lambda c: (c["date"], c.get("created_at") or "", c["id"]),
    )

    first = snaps[0]
    fi = 0
    # 첫 스냅샷 이전/당일 입출금: 유닛에 이미 녹아 있는 것으로 간주 —
    # 마킹만 교정하고 시계열은 건드리지 않는다.
    while fi < len(flows) and flows[fi]["date"] <= first["date"]:
        result["cashflow_updates"].append({
            "id": flows[fi]["id"],
            "applied_snapshot_date": first["date"],
            "units_change": flows[fi]["units_change"],
            "nav_at_time": flows[fi]["nav_at_time"],
        })
        fi += 1

    correction = 0.0
    prev_stored = float(first["total_units"] or 0)
    for snap in snaps[1:]:
        assigned = []
        while fi < len(flows) and flows[fi]["date"] <= snap["date"]:
            assigned.append(flows[fi])
            fi += 1
        stored = float(snap["total_units"] or 0)
        if assigned:
            expected = sum(float(cf["units_change"]) for cf in assigned)
            actual_delta = stored - prev_stored
            missing = expected - actual_delta
            # 백필 시대의 저장 유닛에는 ±수 유닛 잔떨림이 있다 — 유의미한
            # 누락만 보정한다.
            if abs(missing) > max(5.0, 0.01 * abs(expected)):
                correction += missing
            for cf in assigned:
                result["cashflow_updates"].append({
                    "id": cf["id"],
                    "applied_snapshot_date": snap["date"],
                    "units_change": cf["units_change"],
                    "nav_at_time": cf["nav_at_time"],
                })
        if abs(correction) > UNITS_EPS:
            new_units = stored + correction
            new_nav = float(snap["total_value"]) / new_units if new_units > 0 else snap["nav"]
            result["snapshot_updates"].append({
                "date": snap["date"],
                "old_units": stored, "new_units": new_units,
                "old_nav": snap["nav"], "new_nav": new_nav,
            })
        prev_stored = stored

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
