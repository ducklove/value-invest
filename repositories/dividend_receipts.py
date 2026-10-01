"""배당 세후 현금·수익 분류·수취 원장을 한 번에 저장한다."""

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal

from domain.dividend_receipts import DividendCreate, DividendInput, calculate_dividend
from domain.dividend_verification import annotate_receipts, nh_duplicate
from domain.portfolio_trades import TradeConflict
from repositories import broker_activity, investment_insights, portfolio
from repositories.account_holdings import account_operation, current
from repositories.db import get_db, read_snapshot, transaction


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


async def _state(user: str, receipt: DividendInput) -> tuple:
    db = await get_db()
    if receipt.source_key:
        existing = await (await db.execute(
            "SELECT id FROM portfolio_dividend_receipts WHERE google_sub=? AND source_key=?", (user, receipt.source_key),
        )).fetchone()
        if existing:
            raise TradeConflict("이미 수취 기록한 배당 스케줄입니다. 최근 수취 내역을 확인해 주세요.")
    # NH 연동 계좌에만 있는 종목의 배당은 NH 입금으로 이미 가져왔다. 수동 계좌에 다시 넣으면 현금이 중복된다.
    nh_only = await broker_activity.nh_only_codes(user)
    if receipt.stock_code in nh_only:
        duplicate = nh_duplicate(receipt.model_dump(mode="json"), await broker_activity.dividend_records(user), nh_only)
        if duplicate:
            raise TradeConflict(f"NH 계좌에서 {duplicate.get('date')} 배당 입금을 이미 가져왔습니다. "
                                "이 종목은 NH 연동 계좌에만 있어 수동 수취를 기록하면 현금이 중복됩니다.")
    cash = await portfolio.get_portfolio_item(user, f"CASH_{receipt.currency}")
    return cash, _digest(cash)


@account_operation
@read_snapshot()
async def preview_dividend(user: str, receipt: DividendInput) -> dict:
    cash, revision = await _state(user, receipt)
    return {**calculate_dividend(receipt, cash), "revision": revision}


@account_operation
async def record_dividend(user: str, receipt: DividendCreate) -> dict:
    request_id = str(receipt.request_id)
    fingerprint = _digest(receipt.model_dump(mode="json", exclude={"request_id"} | ({"account_id"} if not receipt.account_id else set())))
    async with transaction() as db:
        existing = await (await db.execute(
            "SELECT fingerprint,result_json FROM portfolio_dividend_receipts WHERE google_sub=? AND request_id=?", (user, request_id),
        )).fetchone()
        if existing:
            if existing["fingerprint"] != fingerprint:
                raise TradeConflict("같은 요청 번호에 다른 배당을 저장할 수 없습니다.")
            return {**json.loads(existing["result_json"]), "replayed": True}
        cash, revision = await _state(user, receipt)
        if revision != receipt.expected_revision:
            raise TradeConflict("현금 잔고가 변경됐습니다. 수취 내용을 다시 확인해 주세요.")
        result = calculate_dividend(receipt, cash)
        now = datetime.now(timezone.utc).isoformat()
        await portfolio.save_portfolio_item(user, result["cash_code"], (cash or {}).get("stock_name", f"{receipt.currency} 현금"),
                                            result["cash_after"], (cash or {}).get("avg_price", 1), receipt.currency,
                                            avg_price_currency=(cash or {}).get("avg_price_currency", receipt.currency))
        income_id = None
        if result["amount_krw"] > 0:
            income_id = await investment_insights.add_income(user, {
                "date": result["applied_date"], "stock_code": receipt.stock_code, "kind": "dividend",
                "amount_krw": result["amount_krw"], "memo": f"배당 수취 ({result['received_date']}) {receipt.memo}"[:500],
            })
        result.update(account_id=current(user), request_id=request_id, created_at=now, income_event_id=income_id, replayed=False)
        await db.execute(
            "INSERT INTO portfolio_dividend_receipts (google_sub,request_id,source_key,stock_code,income_event_id,fingerprint,result_json,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (user, request_id, receipt.source_key, receipt.stock_code, income_id, fingerprint, json.dumps(result, ensure_ascii=False), now),
        )
    return result


async def list_receipts(user: str, limit: int = 20, *, verify: bool = True) -> list[dict]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT result_json FROM portfolio_dividend_receipts WHERE google_sub=? ORDER BY id DESC LIMIT ?", (user, limit),
    )).fetchall()
    receipts = [json.loads(row["result_json"]) for row in rows]
    if not verify or not receipts:
        return receipts
    # NH 배당 입금과 대응되면 'NH 확인', 아니면 '미확인'. 저장된 원장은 바꾸지 않는다.
    return annotate_receipts(receipts, await broker_activity.dividend_records(user))


async def received_source_keys(user: str) -> set[str]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT source_key FROM portfolio_dividend_receipts WHERE google_sub=? AND source_key IS NOT NULL", (user,),
    )).fetchall()
    return {row["source_key"] for row in rows}


async def receipt_totals(user: str, account_id: str | None = None) -> list[dict]:
    from repositories import accounts
    default_id = await accounts.get_default_account_id(user) if account_id else None
    db = await get_db()
    rows = await (await db.execute("SELECT result_json FROM portfolio_dividend_receipts WHERE google_sub=?", (user,))).fetchall()
    totals = {}
    for row in rows:
        result = json.loads(row["result_json"])
        if account_id and (result.get("account_id") or default_id) != account_id:
            continue
        currency = result["currency"]
        total = totals.setdefault(currency, {"currency": currency, "count": 0, "net_amount": Decimal(0)})
        total["count"] += 1
        total["net_amount"] += Decimal(str(result["net_amount"]))
    return [{**totals[key], "net_amount": float(totals[key]["net_amount"])} for key in sorted(totals)]
