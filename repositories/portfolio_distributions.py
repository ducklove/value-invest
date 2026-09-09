"""세후 배당 누적액 범위의 분배금 출금. 현금과 분배 원장을 원자적으로 갱신한다."""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from domain.portfolio_distributions import DistributionCreate, DistributionInput, calculate_distribution
from domain.portfolio_trades import TradeConflict, TradeError
from repositories import dividend_receipts, portfolio, snapshots
from repositories.db import get_db, read_snapshot, transaction


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


@read_snapshot()
async def balances(user: str) -> list[dict]:
    received = {row["currency"]: row for row in await dividend_receipts.receipt_totals(user)}
    db = await get_db()
    rows = await (await db.execute("SELECT currency,amount FROM portfolio_distributions WHERE google_sub=?", (user,))).fetchall()
    paid = {}
    for row in rows:
        paid[row["currency"]] = paid.get(row["currency"], Decimal(0)) + Decimal(str(row["amount"]))
    return [{**row, "distributed_amount": float(paid.get(currency, 0)),
             "available_amount": float(Decimal(str(row["net_amount"])) - paid.get(currency, Decimal(0)))}
            for currency, row in sorted(received.items())]


async def _state(user: str, payload: DistributionInput):
    cash = await portfolio.get_portfolio_item(user, f"CASH_{payload.currency}")
    total = next((row for row in await balances(user) if row["currency"] == payload.currency), {})
    latest = await snapshots.get_latest_snapshot(user)
    if not latest or latest["total_units"] <= 0:
        raise TradeError("NAV 좌수가 정산된 포트폴리오에서 분배금을 출금할 수 있습니다.")
    available = Decimal(str(total.get("available_amount", 0)))
    return cash, available, _digest([cash, total, latest])


@read_snapshot()
async def preview_distribution(user: str, payload: DistributionInput):
    cash, available, revision = await _state(user, payload)
    return {**calculate_distribution(payload, cash, available), "revision": revision}


async def record_distribution(user: str, payload: DistributionCreate):
    request_id = str(payload.request_id)
    fingerprint = _digest(payload.model_dump(mode="json", exclude={"request_id"}))
    async with transaction() as db:
        existing = await (await db.execute(
            "SELECT fingerprint,result_json FROM portfolio_distributions WHERE google_sub=? AND request_id=?", (user, request_id),
        )).fetchone()
        if existing:
            if existing["fingerprint"] != fingerprint:
                raise TradeConflict("같은 요청 번호에 다른 분배금 내용을 저장할 수 없습니다.")
            return {**json.loads(existing["result_json"]), "replayed": True}
        cash, available, revision = await _state(user, payload)
        if revision != payload.expected_revision:
            raise TradeConflict("현금·배당 누적액·NAV 정산 상태가 변경됐습니다. 다시 확인해 주세요.")
        result = calculate_distribution(payload, cash, available)
        now = datetime.now(timezone(timedelta(hours=9))).replace(tzinfo=None).isoformat()
        result.update(request_id=request_id, date=now[:10], created_at=now, replayed=False)
        await db.execute("UPDATE user_portfolio SET quantity=?,updated_at=? WHERE google_sub=? AND stock_code=?",
                         (result["cash_after"], now, user, f"CASH_{payload.currency}"))
        await db.execute(
            "INSERT INTO portfolio_distributions (google_sub,request_id,date,currency,amount,amount_krw,fingerprint,result_json,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (user, request_id, now[:10], payload.currency, result["amount"], result["amount_krw"], fingerprint, json.dumps(result, ensure_ascii=False), now),
        )
    return result


async def list_distributions(user: str, limit: int = 20):
    db = await get_db()
    rows = await (await db.execute(
        "SELECT result_json FROM portfolio_distributions WHERE google_sub=? ORDER BY id DESC LIMIT ?", (user, limit),
    )).fetchall()
    return [json.loads(row["result_json"]) for row in rows]
