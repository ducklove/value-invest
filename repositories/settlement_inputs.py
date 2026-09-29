"""마감 당시의 잔고·원장을 보존한다. 수동 변경과 증권사 동기화도 같은 이력에 남는다."""

import json

from repositories.db import get_db, read_snapshot, transaction

TABLES = {
    "user_portfolio": ("stock_code", "stock_code stock_name quantity avg_price avg_price_currency currency group_name pair_long_code"),
    "account_holdings": ("account_id || ':' || stock_code", "account_id stock_code quantity avg_price avg_price_currency currency"),
    "portfolio_cashflows": ("id", "id date type amount nav_at_time units_change applied_snapshot_date created_at"),
    "portfolio_distributions": ("id", "id date amount_krw applied_snapshot_date created_at"),
    "portfolio_dividend_receipts": ("id", "id income_event_id applied_snapshot_date created_at"),
}
STAMP = "strftime('%Y-%m-%dT%H:%M:%f','now','+9 hours')"


async def initialize(db):
    await db.execute("CREATE TABLE IF NOT EXISTS settlement_history_start (id INTEGER PRIMARY KEY CHECK(id=1), started_at TEXT NOT NULL)")
    await db.execute("CREATE TABLE IF NOT EXISTS settlement_versions (id INTEGER PRIMARY KEY, table_name TEXT NOT NULL, google_sub TEXT NOT NULL, row_key TEXT NOT NULL, recorded_at TEXT NOT NULL, payload TEXT)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_settlement_versions ON settlement_versions(google_sub,table_name,recorded_at,row_key,id)")
    await db.execute("CREATE TABLE IF NOT EXISTS settlement_price_inputs (google_sub TEXT NOT NULL, date TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(google_sub,date))")
    await db.execute("CREATE TRIGGER IF NOT EXISTS settlement_user_delete AFTER DELETE ON users BEGIN DELETE FROM settlement_versions WHERE google_sub=OLD.google_sub; DELETE FROM settlement_price_inputs WHERE google_sub=OLD.google_sub; END")
    first = not await (await db.execute("SELECT 1 FROM settlement_history_start")).fetchone()
    for table, (key, column_string) in TABLES.items():
        columns = column_string.split()

        def payload(prefix):
            return "json_object(" + ",".join(f"'{c}',{prefix}{c}" for c in columns) + ")"

        if first:
            await db.execute(f"INSERT INTO settlement_versions(table_name,google_sub,row_key,recorded_at,payload) SELECT '{table}',google_sub,{key},{STAMP},{payload('')} FROM {table}")
        for event, prefix in (("INSERT", "NEW."), ("UPDATE", "NEW."), ("DELETE", "OLD.")):
            row_key = key
            for part in key.split(" || "):
                if not part.startswith("'"):
                    row_key = row_key.replace(part, prefix + part)
            value = "NULL" if event == "DELETE" else payload(prefix)
            condition = " WHEN " + " OR ".join(f"OLD.{c} IS NOT NEW.{c}" for c in columns) if event == "UPDATE" else ""
            await db.execute(f"CREATE TRIGGER IF NOT EXISTS settlement_{table}_{event.lower()} AFTER {event} ON {table}{condition} BEGIN INSERT INTO settlement_versions(table_name,google_sub,row_key,recorded_at,payload) VALUES ('{table}',{prefix}google_sub,{row_key},{STAMP},{value}); END")
    if first:
        await db.execute(f"INSERT INTO settlement_history_start VALUES(1,{STAMP})")


@read_snapshot()
async def load(user: str, cutoff: str) -> dict:
    db = await get_db()
    start = await (await db.execute("SELECT started_at FROM settlement_history_start WHERE id=1")).fetchone()
    if not start or start["started_at"] > cutoff:
        raise ValueError("해당 마감 시각의 잔고 변경 이력이 없습니다. 현재 잔고로 소급 정산하지 않습니다.")
    rows = await (await db.execute(
        "SELECT table_name,payload FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY table_name,row_key ORDER BY recorded_at DESC,id DESC) AS n FROM settlement_versions WHERE google_sub=? AND recorded_at<=?) WHERE n=1 AND payload IS NOT NULL",
        (user, cutoff),
    )).fetchall()
    result = {table: [] for table in TABLES}
    for row in rows:
        result[row["table_name"]].append(json.loads(row["payload"]))
    positions = result["account_holdings"]
    for item in result["user_portfolio"]:
        item["account_positions"] = [r for r in positions if r["stock_code"] == item["stock_code"]]
    return result


async def prices(user: str, day: str) -> dict | None:
    db = await get_db()
    row = await (await db.execute("SELECT payload FROM settlement_price_inputs WHERE google_sub=? AND date=?", (user, day))).fetchone()
    return json.loads(row["payload"]) if row else None


async def save_prices(user: str, day: str, payload: dict):
    async with transaction() as db:
        await db.execute("INSERT INTO settlement_price_inputs VALUES(?,?,?) ON CONFLICT(google_sub,date) DO UPDATE SET payload=excluded.payload", (user, day, json.dumps(payload, ensure_ascii=False)))
