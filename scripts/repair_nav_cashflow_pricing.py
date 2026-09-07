"""기록된 입출금의 발행가를 정산일 입출금 제외 NAV로 재계산한다.

원장 추적 이전 구간과 평가액은 보존한다. 추적 구간에 설명되지 않는
좌수 변동이 있으면 추정하지 않고 중단한다. 기본은 읽기 전용 점검이며
--apply는 SQLite 온라인 백업 후 변경 전 상태를 검증하고 한 번에 반영한다.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from repositories import db as db_repo  # noqa: E402

SNAP_SQL = "SELECT date, total_value, nav, total_units FROM portfolio_snapshots WHERE google_sub=? ORDER BY date"
FLOW_SQL = (
    "SELECT id, date, type, amount, nav_at_time, units_change, applied_snapshot_date, created_at "
    "FROM portfolio_cashflows WHERE google_sub=? ORDER BY id"
)


def rebuild_pricing(snapshots: list[dict], cashflows: list[dict]) -> dict:
    result = {"snapshots": [], "cashflows": []}
    settled = [cf for cf in cashflows if cf.get("applied_snapshot_date") and cf.get("units_change") is not None]
    if not settled or not snapshots:
        return result
    snaps = sorted(snapshots, key=lambda s: s["date"])
    by_day: dict[str, list[dict]] = {}
    for cf in settled:
        by_day.setdefault(cf["applied_snapshot_date"], []).append(cf)
    start = min(by_day)
    if start <= snaps[0]["date"]:
        raise ValueError("첫 입출금 이전의 기준 정산이 없어 자동 복구할 수 없습니다.")
    if not set(by_day).issubset({s["date"] for s in snaps}):
        raise ValueError("입출금의 반영 정산일이 이력에 없습니다.")
    previous = snaps[0]
    corrected_units = float(previous["total_units"])
    for snap in snaps[1:]:
        if snap["date"] < start:
            previous = snap
            corrected_units = float(snap["total_units"])
            continue
        rows = by_day.get(snap["date"], [])
        old_delta = snap["total_units"] - previous["total_units"]
        recorded_delta = sum(cf["units_change"] for cf in rows)
        if not math.isclose(old_delta, recorded_delta, rel_tol=1e-9, abs_tol=1e-5):
            raise ValueError(f"{snap['date']}: 원장으로 설명되지 않는 좌수 변동이 있어 복구를 중단합니다.")
        net = sum(cf["amount"] if cf["type"] == "deposit" else -cf["amount"] for cf in rows)
        issue_nav = (snap["total_value"] - net) / corrected_units if corrected_units > 0 else previous["nav"]
        if not math.isfinite(issue_nav) or issue_nav <= 0:
            raise ValueError(f"{snap['date']}: 발행 기준가를 확정할 수 없습니다.")
        for cf in rows:
            units = cf["amount"] / issue_nav * (1 if cf["type"] == "deposit" else -1)
            corrected_units += units
            if not math.isclose(units, cf["units_change"], rel_tol=1e-12, abs_tol=1e-7) or cf["nav_at_time"] is None or not math.isclose(issue_nav, cf["nav_at_time"], abs_tol=1e-9):
                result["cashflows"].append({
                    "id": cf["id"], "old_units": cf["units_change"], "new_units": units,
                    "old_nav": cf["nav_at_time"], "new_nav": issue_nav,
                })
        if abs(corrected_units) < 1e-8 and abs(snap["total_value"]) < 1e-6:
            corrected_units, nav = 0.0, issue_nav
        elif corrected_units > 0:
            nav = snap["total_value"] / corrected_units
        else:
            raise ValueError(f"{snap['date']}: 음수 좌수가 발생해 복구를 중단합니다.")
        if not math.isclose(corrected_units, snap["total_units"], rel_tol=1e-12, abs_tol=1e-7) or not math.isclose(nav, snap["nav"], abs_tol=1e-9):
            result["snapshots"].append({
                "date": snap["date"], "old_units": snap["total_units"], "new_units": corrected_units,
                "old_nav": snap["nav"], "new_nav": nav,
            })
        previous = {**snap, "nav": nav}
    return result


def read_data(path: Path) -> list[dict]:
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        users = [r[0] for r in db.execute("SELECT DISTINCT google_sub FROM portfolio_cashflows ORDER BY google_sub")]
        return [{
            "user": user,
            "snapshots": [dict(r) for r in db.execute(SNAP_SQL, (user,))],
            "cashflows": [dict(r) for r in db.execute(FLOW_SQL, (user,))],
        } for user in users]


async def apply_plans(path: Path, data: list[dict], plans: list[dict]) -> Path:
    backup_dir = path.parent / ".deploy-repairs" / ("nav-pricing-" + datetime.now().strftime("%Y%m%d-%H%M%S-%f"))
    backup_dir.mkdir(parents=True, mode=0o700)
    backup = backup_dir / "before.db"
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as source, sqlite3.connect(backup) as dest:
        source.backup(dest)
        if dest.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("백업 무결성 검사에 실패했습니다.")
    os.chmod(backup, 0o600)
    db_repo.DB_PATH = path
    try:
        async with db_repo.transaction() as db:
            for account, plan in zip(data, plans):
                for sql, key in ((SNAP_SQL, "snapshots"), (FLOW_SQL, "cashflows")):
                    cursor = await db.execute(sql, (account["user"],))
                    if [dict(r) for r in await cursor.fetchall()] != account[key]:
                        raise ValueError("점검 이후 원장이 변경됐습니다. 다시 점검 후 적용해 주세요.")
                for row in plan["snapshots"]:
                    await db.execute(
                        "UPDATE portfolio_snapshots SET nav=?, total_units=? WHERE google_sub=? AND date=?",
                        (row["new_nav"], row["new_units"], account["user"], row["date"]),
                    )
                for row in plan["cashflows"]:
                    await db.execute(
                        "UPDATE portfolio_cashflows SET nav_at_time=?, units_change=? WHERE google_sub=? AND id=?",
                        (row["new_nav"], row["new_units"], account["user"], row["id"]),
                    )
        (backup_dir / "changes.json").write_text(json.dumps(plans, ensure_ascii=False, indent=2), encoding="utf-8")
    finally:
        await db_repo.close_db()
    return backup


async def refresh_reports(path: Path, data: list[dict]) -> int:
    from services.portfolio.period_reports import generate_and_save_period_report

    db_repo.DB_PATH = path
    refreshed = 0
    try:
        db = await db_repo.get_db()
        for account in data:
            dates = [cf["applied_snapshot_date"] for cf in account["cashflows"] if cf["applied_snapshot_date"]]
            if not dates:
                continue
            cursor = await db.execute(
                "SELECT period_type, period_key FROM portfolio_period_reports WHERE google_sub=? AND end_date>=?",
                (account["user"], min(dates)),
            )
            for row in await cursor.fetchall():
                await generate_and_save_period_report(account["user"], row["period_type"], row["period_key"])
                refreshed += 1
    finally:
        await db_repo.close_db()
    return refreshed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=ROOT / "cache.db")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--refresh-reports", action="store_true", help="--apply와 함께 영향받은 저장 보고서도 재생성")
    args = parser.parse_args()
    data = read_data(args.db)
    plans = [rebuild_pricing(a["snapshots"], a["cashflows"]) for a in data]
    for index, plan in enumerate(plans, 1):
        print(json.dumps({"account": index, "snapshots": len(plan["snapshots"]), "cashflows": len(plan["cashflows"]),
                          "first": plan["snapshots"][:1], "last": plan["snapshots"][-1:]}, ensure_ascii=False))
    if args.apply and any(p["snapshots"] or p["cashflows"] for p in plans):
        backup = asyncio.run(apply_plans(args.db, data, plans))
        remaining = read_data(args.db)
        if any(any(rebuild_pricing(a["snapshots"], a["cashflows"]).values()) for a in remaining):
            raise ValueError("복구 후 재점검 결과가 일치하지 않습니다.")
        print(json.dumps({"applied": True, "backup": str(backup), "recheck": "ok"}, ensure_ascii=False))
    if args.apply and args.refresh_reports:
        print(json.dumps({"refreshed_reports": asyncio.run(refresh_reports(args.db, data))}))


if __name__ == "__main__":
    main()
