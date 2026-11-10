"""공식 기초자산이 같은 선물 월물 사이에 사용자의 롱숏 연결을 이어준다."""

from domain.portfolio_codes import common_stock_code, is_preferred_stock
from repositories.db import transaction


async def remember(user: str, code: str, underlying: str | None, long_code: str | None) -> None:
    # NULL도 명시적인 연결 해제다. 해당 월물을 다음 동기화에서 다시 연결하지 않는다.
    async with transaction() as db:
        if not underlying:
            previous = await (await db.execute(
                "SELECT underlying_code FROM futures_pair_choices WHERE google_sub=? AND stock_code=?", (user, code),
            )).fetchone()
            # 마스터 장애 중 첫 해제도 종목별로 기억한다. 다음 정상 조회 때
            # capture가 기초자산을 보완하더라도 해제한 월물은 known에 남는다.
            underlying = previous["underlying_code"] if previous else ""
        await db.execute(
            "INSERT INTO futures_pair_choices VALUES (?,?,?,?) "
            "ON CONFLICT(google_sub,stock_code) DO UPDATE SET "
            "underlying_code=excluded.underlying_code,long_code=excluded.long_code",
            (user, code, underlying, long_code),
        )


async def capture(user: str, mapping: dict[str, str]) -> None:
    """기존 월물이 합산 잔고에서 삭제되기 전에 연결을 보존한다."""
    async with transaction() as db:
        rows = await (await db.execute(
            "SELECT stock_code,pair_long_code FROM user_portfolio "
            "WHERE google_sub=? AND stock_code LIKE 'KRFUT_%' AND pair_long_code IS NOT NULL",
            (user,),
        )).fetchall()
        for row in rows:
            underlying = mapping.get(row["stock_code"].removeprefix("KRFUT_"))
            if underlying:
                await remember(user, row["stock_code"], underlying, row["pair_long_code"])


async def apply(user: str, mapping: dict[str, str]) -> None:
    from repositories.portfolio import set_portfolio_pair

    async with transaction() as db:
        items = [dict(row) for row in await (await db.execute(
            "SELECT stock_code,quantity,pair_long_code FROM user_portfolio WHERE google_sub=?", (user,),
        )).fetchall()]
        by_code = {item["stock_code"]: item for item in items}
        choices = [dict(row) for row in await (await db.execute(
            "SELECT stock_code,underlying_code,long_code FROM futures_pair_choices WHERE google_sub=?", (user,),
        )).fetchall()]
        known = {row["stock_code"] for row in choices}
        configured = {row["underlying_code"] for row in choices if row["long_code"]}
        candidates: dict[str, set[str]] = {}
        for row in choices:
            long_code = row["long_code"]
            target = by_code.get(long_code)
            if target and target["quantity"] > 0 and not target["pair_long_code"]:
                candidates.setdefault(row["underlying_code"], set()).add(long_code)
        for item in items:
            code = item["stock_code"]
            if not code.startswith("KRFUT_") or item["quantity"] >= 0 or item["pair_long_code"] or code in known:
                continue
            underlying = mapping.get(code.removeprefix("KRFUT_"))
            targets = candidates.get(underlying, set())
            if underlying and underlying not in configured:
                # 첫 월물도 같은 발행사의 우선주 롱이 하나면 연결한다.
                targets = {row["stock_code"] for row in items
                           if row["quantity"] > 0 and not row["pair_long_code"]
                           and is_preferred_stock(row["stock_code"])
                           and common_stock_code(row["stock_code"]) == underlying}
            # 서로 다른 롱에 연결한 이력이 있으면 임의로 선택하지 않는다.
            if len(targets) == 1:
                long_code = next(iter(targets))
                await set_portfolio_pair(user, code, long_code)
                await remember(user, code, underlying, long_code)
