"""보존된 마감 잔고·가격으로만 과거 정산을 재계산한다. 기본은 복사본에서 점검한다.

원본 백업과 변경 명세를 남기고, 모든 날짜를 복원할 수 있을 때만 --apply를 허용한다.
기존 일봉이나 현재 잔고로 부족한 자료를 추정하지 않는다.
"""

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from repositories import bootstrap, settlement_inputs  # noqa: E402
from repositories import db as db_repo  # noqa: E402
from services.portfolio import nav_snapshot as snapshot_nav  # noqa: E402
from services.portfolio import regular_close  # noqa: E402

SNAPSHOT_TABLES = ("portfolio_snapshots", "portfolio_stock_snapshots", "portfolio_group_snapshots", "portfolio_stock_weight_snapshots")
LEDGERS = {
    "portfolio_cashflows": ("applied_snapshot_date", "units_change", "nav_at_time"),
    "portfolio_distributions": ("applied_snapshot_date",),
    "portfolio_dividend_receipts": ("applied_snapshot_date",),
    "portfolio_income_events": ("date",),
}


def fingerprint(db):
    digest = hashlib.sha256()
    for table in (*SNAPSHOT_TABLES, *LEDGERS, "user_portfolio", "account_holdings"):
        rows = db.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
        digest.update(json.dumps([tuple(r) for r in rows], ensure_ascii=False).encode())
    return digest.hexdigest()


async def rebuild(path: Path, start: str, apply: bool = False) -> dict:
    if not path.is_file():
        raise ValueError("DB 파일이 없습니다.")
    datetime.strptime(start, "%Y-%m-%d")
    output = path.parent / ".deploy-repairs" / ("regular-close-" + datetime.now().strftime("%Y%m%d-%H%M%S-%f"))
    output.mkdir(parents=True, mode=0o700)
    backup, candidate = output / "before.db", output / "candidate.db"
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as source, sqlite3.connect(backup) as copy:
        source.backup(copy)
    with sqlite3.connect(backup) as source, sqlite3.connect(candidate) as copy:
        if source.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("원본 백업 무결성 오류")
        original_hash = fingerprint(source)
        source.backup(copy)
        source.row_factory = sqlite3.Row
        originals = [dict(r) for r in source.execute("SELECT * FROM portfolio_snapshots WHERE date>=? ORDER BY date,google_sub", (start,))]
    for file in (backup, candidate):
        os.chmod(file, 0o600)
    report = {"start": start, "backup": str(backup), "candidate": str(candidate), "blocked": [], "changes": [], "applied": False}
    old_path = db_repo.DB_PATH
    await db_repo.close_db()
    db_repo.DB_PATH = candidate
    prepared = []
    try:
        await bootstrap.init_db()
        for row in originals:
            user, day = row["google_sub"], row["date"]
            try:
                close = regular_close.closing_at(day)
                if close is None:
                    raise ValueError("기존 휴장일 정산: 별도 검토 필요")
                cutoff = close.replace(tzinfo=None).isoformat(timespec="milliseconds")
                inputs = await settlement_inputs.load(user, cutoff)
                prices = await settlement_inputs.prices(user, day)
                if not prices or prices.get("basis") != regular_close.BASIS:
                    raise ValueError("검증된 정규장 가격·환율 보관 자료 없음")
                values = regular_close.value_inputs(inputs, prices)
                prepared.append((row, cutoff, inputs, prices, values))
            except Exception as exc:
                report["blocked"].append({"user": user, "date": day, "reason": str(exc)})
        if not report["blocked"]:
            async with db_repo.transaction() as db:
                for table in SNAPSHOT_TABLES:
                    await db.execute(f"DELETE FROM {table} WHERE date>=?", (start,))
                for table in tuple(LEDGERS)[:3]:
                    extra = ",units_change=NULL,nav_at_time=NULL" if table == "portfolio_cashflows" else ""
                    await db.execute(f"UPDATE {table} SET applied_snapshot_date=NULL{extra} WHERE applied_snapshot_date>=?", (start,))
                for row, cutoff, inputs, prices, values in prepared:
                    user, day = row["google_sub"], row["date"]
                    # 원장에 다시 기록한 앞선 정산의 좌수·귀속을 다음 날짜에서 사용한다.
                    for table in tuple(LEDGERS)[:3]:
                        for item in inputs[table]:
                            current = await (await db.execute(f"SELECT * FROM {table} WHERE id=? AND google_sub=?", (item["id"], user))).fetchone()
                            if not current:
                                raise ValueError(f"{day}: 원장 행 삭제로 자동 복원 불가")
                            for field in LEDGERS[table]:
                                item[field] = current[field]
                    await snapshot_nav._persist_snapshot(user, day, *values, cutoff, frozen=inputs,
                                                         frozen_fx=prices["rates"].get("USD", {}).get("rate"))
                    fresh = await (await db.execute("SELECT nav,total_value,total_units FROM portfolio_snapshots WHERE google_sub=? AND date=?", (user, day))).fetchone()
                    report["changes"].append({"user": user, "date": day, "old": {k: row[k] for k in fresh.keys()}, "new": dict(fresh)})
    finally:
        await db_repo.close_db()
        db_repo.DB_PATH = old_path
    if apply and not report["blocked"] and report["changes"]:
        with sqlite3.connect(candidate) as rebuilt, sqlite3.connect(path) as original:
            rebuilt.row_factory = sqlite3.Row
            original.execute("PRAGMA foreign_keys=ON")
            original.execute("BEGIN IMMEDIATE")
            if fingerprint(original) != original_hash:
                raise ValueError("점검 중 원본 자료가 변경됐습니다. 다시 실행해야 합니다.")
            for table in SNAPSHOT_TABLES:
                rows = rebuilt.execute(f"SELECT * FROM {table} WHERE date>=?", (start,)).fetchall()
                original.execute(f"DELETE FROM {table} WHERE date>=?", (start,))
                if rows:
                    columns = list(rows[0].keys())
                    original.executemany(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", [tuple(r) for r in rows])
            for table, fields in LEDGERS.items():
                for row in rebuilt.execute(f"SELECT id,{','.join(fields)} FROM {table}"):
                    original.execute(f"UPDATE {table} SET {','.join(f'{c}=?' for c in fields)} WHERE id=?", (*[row[c] for c in fields], row["id"]))
            original.commit()
            report["applied"] = True
        db_repo.DB_PATH = path
        try:
            from services.portfolio.period_reports import generate_and_save_period_report
            db = await db_repo.get_db()
            saved_reports = await (await db.execute("SELECT google_sub,period_type,period_key FROM portfolio_period_reports WHERE end_date>=?", (start,))).fetchall()
            report["refreshed_reports"] = 0
            for row in saved_reports:
                await generate_and_save_period_report(row["google_sub"], row["period_type"], row["period_key"])
                report["refreshed_reports"] += 1
        except Exception as exc:
            report["report_refresh_error"] = str(exc)
        finally:
            await db_repo.close_db()
            db_repo.DB_PATH = old_path
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
    report["report_path"] = str(output / "report.json")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(rebuild(args.db, args.start, args.apply))
    print(json.dumps({key: result[key] for key in ("applied", "report_path")}, ensure_ascii=False))
    print(f"재계산 {len(result['changes'])}일 · 복원 불가 {len(result['blocked'])}일")
    return 1 if args.apply and (result["blocked"] or result.get("report_refresh_error")) else 0


if __name__ == "__main__":
    raise SystemExit(main())
