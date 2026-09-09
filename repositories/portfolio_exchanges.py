"""환전의 출금·입금·매매 원장을 한 트랜잭션으로 저장한다."""

import json
from datetime import datetime, timezone

from domain.portfolio_exchanges import ExchangeCreate, ExchangeInput, calculate_exchange
from domain.portfolio_trades import TradeConflict
from repositories import portfolio
from repositories.db import read_snapshot, transaction
from repositories.portfolio_trades import _digest


async def _state(user: str, exchange: ExchangeInput):
    source = await portfolio.get_portfolio_item(user, f"CASH_{exchange.from_currency}")
    target = await portfolio.get_portfolio_item(user, f"CASH_{exchange.to_currency}")
    return source, target, _digest([source, target])


@read_snapshot()
async def preview_exchange(user: str, exchange: ExchangeInput) -> dict:
    source, target, revision = await _state(user, exchange)
    return {**calculate_exchange(exchange, source, target), "revision": revision}


async def record_exchange(user: str, exchange: ExchangeCreate) -> dict:
    request_id = str(exchange.request_id)
    fingerprint = _digest(exchange.model_dump(mode="json", exclude={"request_id"}))
    async with transaction() as db:
        existing = await (await db.execute(
            "SELECT fingerprint,result_json FROM portfolio_trades WHERE google_sub=? AND request_id=?", (user, request_id),
        )).fetchone()
        if existing:
            if existing["fingerprint"] != fingerprint:
                raise TradeConflict("같은 요청 번호에 다른 거래 내용을 저장할 수 없습니다.")
            return {**json.loads(existing["result_json"]), "replayed": True}
        source, target, revision = await _state(user, exchange)
        if revision != exchange.expected_revision:
            raise TradeConflict("현금 잔고가 변경됐습니다. 환전 내용을 다시 확인해 주세요.")
        result = calculate_exchange(exchange, source, target)
        now = datetime.now(timezone.utc).isoformat()
        for cash, currency, balance in [(source, exchange.from_currency, result["from_after"]), (target, exchange.to_currency, result["to_after"])]:
            if cash:
                await db.execute("UPDATE user_portfolio SET quantity=?,updated_at=? WHERE google_sub=? AND stock_code=?",
                                 (balance, now, user, f"CASH_{currency}"))
            else:
                await portfolio.save_portfolio_item(user, f"CASH_{currency}", f"{currency} 현금", balance, 1, currency, avg_price_currency=currency)
        result.update(request_id=request_id, created_at=now, replayed=False)
        await db.execute(
            "INSERT INTO portfolio_trades (google_sub,request_id,stock_code,fingerprint,result_json,created_at) VALUES (?,?,?,?,?,?)",
            (user, request_id, result["stock_code"], fingerprint, json.dumps(result, ensure_ascii=False), now),
        )
    return result
