"""저장된 증권사 스냅샷에서 공개 계약 가격만 읽는다. 계좌·수량은 반환하지 않는다."""

import json

from repositories.db import get_db


async def futures_quote(code: str) -> dict:
    db = await get_db()
    row = await (await db.execute(
        "SELECT p.value AS position,a.broker_snapshot_json AS snapshot "
        "FROM portfolio_accounts a,json_each(a.broker_snapshot_json,'$.positions') p "
        "WHERE json_extract(a.broker_snapshot_json,'$.display')='holdings' "
        "AND json_extract(p.value,'$.stock_code')=? "
        "ORDER BY json_extract(a.broker_snapshot_json,'$.synced_at') DESC LIMIT 1", (code,),
    )).fetchone()
    if not row:
        return {}
    position, snapshot = json.loads(row["position"]), json.loads(row["snapshot"])
    return {"price": position["current_price"], "date": snapshot["as_of_date"],
            "fetched_at": snapshot["synced_at"], "source": "namuh_balance"}
