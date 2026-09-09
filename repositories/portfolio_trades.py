"""현물·현금·매매 내역을 동일 쓰기 트랜잭션에 저장한다."""

import hashlib
import json
from datetime import datetime, timezone

from domain.portfolio_trades import TradeConflict, TradeCreate, TradeError, TradeInput, calculate_trade
from repositories import portfolio
from repositories.db import get_db, read_snapshot, transaction


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


async def _state(google_sub: str, trade: TradeInput) -> tuple:
    db = await get_db()
    holding = await portfolio.get_portfolio_item(google_sub, trade.stock_code)
    cash = await portfolio.get_portfolio_item(google_sub, f"CASH_{trade.currency}")
    paired = await (await db.execute(
        "SELECT stock_code FROM user_portfolio WHERE google_sub = ? AND pair_long_code = ? ORDER BY stock_code",
        (google_sub, trade.stock_code),
    )).fetchall()
    if paired:
        raise TradeError("롱숏 페어의 종목은 개별 잔고 수정에서 관리해 주세요.")
    return holding, cash, _digest([holding, cash])


@read_snapshot()
async def preview_trade(google_sub: str, trade: TradeInput) -> dict:
    holding, cash, revision = await _state(google_sub, trade)
    return {**calculate_trade(trade, holding, cash), "revision": revision}


async def record_trade(google_sub: str, trade: TradeCreate) -> dict:
    request_id = str(trade.request_id)
    fingerprint = _digest(trade.model_dump(mode="json", exclude={"request_id"}))
    async with transaction() as db:
        existing = await (await db.execute(
            "SELECT fingerprint, result_json FROM portfolio_trades WHERE google_sub = ? AND request_id = ?",
            (google_sub, request_id),
        )).fetchone()
        if existing:
            if existing["fingerprint"] != fingerprint:
                raise TradeConflict("같은 요청 번호에 다른 매매 내용을 저장할 수 없습니다.")
            return {**json.loads(existing["result_json"]), "replayed": True}
        holding, cash, revision = await _state(google_sub, trade)
        if revision != trade.expected_revision:
            raise TradeConflict("잔고가 변경됐습니다. 미리보기를 다시 확인해 주세요.")
        result = calculate_trade(trade, holding, cash)
        now = datetime.now(timezone.utc).isoformat()
        if result["quantity_after"] == 0:
            await db.execute("DELETE FROM portfolio_tags WHERE google_sub = ? AND stock_code = ?", (google_sub, trade.stock_code))
            await db.execute("DELETE FROM user_portfolio WHERE google_sub = ? AND stock_code = ?", (google_sub, trade.stock_code))
        else:
            await portfolio.save_portfolio_item(
                google_sub, trade.stock_code, result["stock_name"], result["quantity_after"],
                result["avg_price_after"], trade.currency, avg_price_currency=result["avg_price_currency"],
            )
        if cash:
            await db.execute(
                "UPDATE user_portfolio SET quantity = ?, updated_at = ? WHERE google_sub = ? AND stock_code = ?",
                (result["cash_after"], now, google_sub, result["cash_code"]),
            )
        else:
            await portfolio.save_portfolio_item(
                google_sub, result["cash_code"], f"{trade.currency} 현금", result["cash_after"], 1,
                trade.currency, avg_price_currency=trade.currency,
            )
        result.update(request_id=request_id, created_at=now, replayed=False)
        await db.execute(
            "INSERT INTO portfolio_trades (google_sub, request_id, stock_code, fingerprint, result_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (google_sub, request_id, trade.stock_code, fingerprint, json.dumps(result, ensure_ascii=False), now),
        )
    return result


async def list_trades(google_sub: str, limit: int = 20) -> list[dict]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT result_json FROM portfolio_trades WHERE google_sub = ? ORDER BY id DESC LIMIT ?",
        (google_sub, limit),
    )).fetchall()
    return [json.loads(row["result_json"]) for row in rows]
