"""사용자 표시 설정을 계좌 잔고와 분리해 저장한다."""

from repositories import portfolio
from repositories.db import transaction


async def save(user: str, code: str, **metadata) -> dict | None:
    async with transaction() as db:
        row = await (await db.execute(
            "SELECT * FROM user_portfolio WHERE google_sub=? AND stock_code=?", (user, code),
        )).fetchone()
        if row is None:
            return None
        # 원장 쓰기 없이 현재 합산 잔고에 설정만 적용한다. 조회와 저장을
        # 같은 트랜잭션에 묶어 동시 동기화의 최신 수량을 되돌리지 않는다.
        return await portfolio._save_portfolio_projection(
            user, code, row["stock_name"], row["quantity"], row["avg_price"],
            currency=row["currency"], avg_price_currency=row["avg_price_currency"],
            **metadata,
        )
