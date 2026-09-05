"""투자 논거·점검 이력·실제 수입 분류와 수익 분해용 읽기."""

import hashlib
import json
from datetime import datetime, timezone

from repositories.db import get_db, transaction


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


async def list_theses(user: str) -> list[dict]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT t.*,COALESCE(up.stock_name,cc.corp_name,t.stock_code) AS stock_name FROM investment_theses t "
        "LEFT JOIN user_portfolio up ON up.google_sub=t.google_sub AND up.stock_code=t.stock_code "
        "LEFT JOIN corp_codes cc ON cc.stock_code=t.stock_code WHERE t.google_sub=? ORDER BY t.archived,t.id DESC", (user,)
    )).fetchall()
    return [dict(row) for row in rows]


async def save_thesis(user: str, data: dict, thesis_id: int | None = None) -> int | None:
    fields = ("journal_entry_id", "stock_code", "thesis", "invalidation", "metric", "operator",
              "threshold", "deadline", "evidence_url", "archived")
    values = [data.get(key) for key in fields]
    async with transaction() as db:
        if thesis_id is None:
            cursor = await db.execute(
                f"INSERT INTO investment_theses (google_sub,{','.join(fields)},created_at,updated_at) "
                f"VALUES ({','.join('?' for _ in range(len(fields) + 3))})", [user, *values, now(), now()])
            return cursor.lastrowid
        cursor = await db.execute(
            f"UPDATE investment_theses SET {','.join(key + '=?' for key in fields)},updated_at=? "
            "WHERE google_sub=? AND id=?", [*values, now(), user, thesis_id])
        return thesis_id if cursor.rowcount else None


async def record_check(user: str, thesis_id: int, payload: dict, revision: str) -> dict | None:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    fingerprint = hashlib.sha256(raw.encode()).hexdigest()
    async with transaction() as db:
        # 조회 후 사용자가 수정한 규칙에는 이전 평가를 덮어쓰지 않는다.
        owner = await (await db.execute(
            "SELECT id FROM investment_theses WHERE id=? AND google_sub=? AND updated_at=?",
            (thesis_id, user, revision))).fetchone()
        if owner is None:
            return None
        previous = await (await db.execute(
            "SELECT id,fingerprint,checked_at FROM investment_thesis_checks WHERE thesis_id=? ORDER BY id DESC LIMIT 1",
            (thesis_id,))).fetchone()
        if previous and previous["fingerprint"] == fingerprint:
            return {**payload, "event_id": previous["id"], "changed_at": previous["checked_at"]}
        stamp = now()
        cursor = await db.execute(
            "INSERT INTO investment_thesis_checks (thesis_id,fingerprint,payload_json,checked_at) VALUES (?,?,?,?)",
            (thesis_id, fingerprint, raw, stamp))
        return {**payload, "event_id": cursor.lastrowid, "changed_at": stamp}


async def check_history(user: str, thesis_id: int) -> list[dict]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT c.id,c.payload_json,c.checked_at FROM investment_thesis_checks c "
        "JOIN investment_theses t ON t.id=c.thesis_id WHERE t.google_sub=? AND t.id=? ORDER BY c.id DESC LIMIT 30",
        (user, thesis_id))).fetchall()
    return [{**json.loads(row["payload_json"]), "event_id": row["id"], "changed_at": row["checked_at"]} for row in rows]


async def thesis_source(stock_code: str) -> dict:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT * FROM financial_data WHERE stock_code=? ORDER BY year DESC LIMIT 2", (stock_code,))).fetchall()
    filing = await (await db.execute(
        "SELECT rcept_no,report_name,report_date FROM dart_report_reviews WHERE stock_code=? "
        "ORDER BY report_date DESC,id DESC LIMIT 1", (stock_code,))).fetchone()
    return {"financials": [dict(row) for row in rows], "filing": dict(filing) if filing else None}


async def stock_history(user: str, start: str, end: str) -> list[dict]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT ps.*,COALESCE(up.stock_name,ps.stock_code) AS stock_name FROM portfolio_stock_snapshots ps "
        "LEFT JOIN user_portfolio up ON up.google_sub=ps.google_sub AND up.stock_code=ps.stock_code "
        "WHERE ps.google_sub=? AND ps.date>=? AND ps.date<=? ORDER BY ps.date,ps.stock_code",
        (user, start, end))).fetchall()
    return [dict(row) for row in rows]


async def income_events(user: str, start: str, end: str) -> list[dict]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT * FROM portfolio_income_events WHERE google_sub=? AND date>=? AND date<=? ORDER BY date DESC,id DESC",
        (user, start, end))).fetchall()
    return [dict(row) for row in rows]


async def add_income(user: str, data: dict) -> int:
    async with transaction() as db:
        cursor = await db.execute(
            "INSERT INTO portfolio_income_events (google_sub,date,stock_code,kind,amount_krw,memo,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (user, data["date"], data["stock_code"], data["kind"], data["amount_krw"], data["memo"], now()))
        return cursor.lastrowid


async def delete_income(user: str, event_id: int) -> bool:
    async with transaction() as db:
        cursor = await db.execute("DELETE FROM portfolio_income_events WHERE google_sub=? AND id=?", (user, event_id))
        return cursor.rowcount > 0
