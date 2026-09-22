"""계좌별 표시 순서를 합산 순서 및 증권사 잔고와 분리해 보존한다."""

import json

from domain.portfolio_order import initial_order_key
from repositories import accounts
from repositories.db import get_db, transaction


async def _settings(user: str, aid: str):
    db = await get_db()
    row = await (await db.execute(
        "SELECT a.holding_order_json,l.product FROM portfolio_accounts a "
        "LEFT JOIN broker_account_links l ON l.account_id=a.account_id AND l.google_sub=a.google_sub "
        "WHERE a.google_sub=? AND a.account_id=?", (user, aid),
    )).fetchone()
    if row is None:
        raise accounts.AccountError("계좌를 찾을 수 없습니다.")
    return row


async def apply(user: str, aid: str, items: list[dict]) -> list[dict]:
    settings = await _settings(user, aid)
    raw = settings["holding_order_json"]
    if raw is None:
        # 기존 계좌의 순서는 명시적으로 저장/초기화하기 전까지 그대로 보인다.
        return items
    order = json.loads(raw)
    ranks = {code: rank for rank, code in enumerate(order)}
    # 이후 새 종목은 뒤에 추가한다. 기존 종목의 사용자 지정 순서는 유지한다.
    items.sort(key=lambda item: (ranks.get(item["stock_code"], len(ranks)),
                                initial_order_key(item["stock_code"], settings["product"])))
    for rank, item in enumerate(items):
        item["sort_order"] = rank
    return items


async def save(user: str, aid: str, codes: list[str]) -> None:
    from repositories import account_holdings
    async with transaction() as db:
        await _settings(user, aid)
        positions = await account_holdings.list_positions(user, aid)
        if len(codes) != len(positions) or set(codes) != {row["stock_code"] for row in positions}:
            raise accounts.AccountError("현재 계좌의 전체 종목 순서와 맞지 않습니다. 새로고침 후 다시 정렬해 주세요.")
        await db.execute(
            "UPDATE portfolio_accounts SET holding_order_json=? WHERE google_sub=? AND account_id=?",
            (json.dumps(codes, ensure_ascii=False), user, aid),
        )


async def reset(user: str, aid: str) -> list[str]:
    """최초 연동 또는 명시적 재정렬에만 기본 순서를 적용한다."""
    from repositories import account_holdings
    async with transaction():
        settings = await _settings(user, aid)
        positions = await account_holdings.list_positions(user, aid)
        codes = sorted((row["stock_code"] for row in positions),
                       key=lambda code: initial_order_key(code, settings["product"]))
        await save(user, aid, codes)
        return codes
