"""계좌별 잔고 원장과 기존 사용자 합산 테이블을 한 트랜잭션에서 유지한다."""

import functools
from contextvars import ContextVar
from datetime import datetime
from decimal import Decimal

from repositories import accounts
from repositories.db import get_db, transaction

_scope: ContextVar[tuple[str, str] | None] = ContextVar("holding_account", default=None)


async def initialize(db, user: str) -> None:
    done = await (await db.execute("SELECT 1 FROM account_holdings_initialized WHERE google_sub=?", (user,))).fetchone()
    if done:
        return
    await accounts.ensure_default_account(db, user)
    await db.execute("UPDATE portfolio_cashflows SET account_id=? WHERE google_sub=? AND account_id IS NULL",
                     (await accounts.get_default_account_id(user), user))
    await db.execute(
        "INSERT OR IGNORE INTO account_holdings "
        "SELECT google_sub,account_id,stock_code,stock_name,quantity,avg_price,"
        "COALESCE(avg_price_currency,'KRW'),COALESCE(currency,'KRW'),created_at,updated_at "
        "FROM user_portfolio WHERE google_sub=?", (user,),
    )
    await db.execute("INSERT INTO account_holdings_initialized VALUES (?)", (user,))


async def ensure(user: str) -> None:
    async with transaction() as db:
        await initialize(db, user)


async def require_account(user: str, account_id: str, *, writable=False) -> dict:
    account = await accounts.get_account(user, account_id)
    if not account:
        raise accounts.AccountError("계좌를 찾을 수 없습니다.")
    db = await get_db()
    linked = await (await db.execute("SELECT 1 FROM broker_account_links WHERE google_sub=? AND account_id=?", (user, account_id))).fetchone()
    if writable and linked:
        raise accounts.AccountError("NH 연동 계좌는 잔고 동기화로 갱신합니다. 수동 기록은 수동 계좌를 선택해 주세요.")
    return {**account, "linked": bool(linked)}


def current(user: str) -> str | None:
    value = _scope.get()
    return value[1] if value and value[0] == user else None


def account_operation(fn):
    """매매·환전·현금 수취의 읽기와 쓰기를 동일 계좌로 고정한다."""
    @functools.wraps(fn)
    async def wrapped(user, payload, *args, **kwargs):
        await ensure(user)
        aid = getattr(payload, "account_id", None) or await accounts.get_default_account_id(user)
        await require_account(user, aid, writable=True)
        token = _scope.set((user, aid))
        try:
            return await fn(user, payload, *args, **kwargs)
        finally:
            _scope.reset(token)
    return wrapped


async def list_positions(user: str, account_id: str | None = None) -> list[dict]:
    db = await get_db()
    sql = "SELECT h.*,a.name AS account_name FROM account_holdings h JOIN portfolio_accounts a ON a.account_id=h.account_id WHERE h.google_sub=?"
    args = [user]
    if account_id:
        await require_account(user, account_id)
        sql += " AND h.account_id=?"
        args.append(account_id)
    rows = await (await db.execute(sql + " ORDER BY a.sort_order,h.stock_code", args)).fetchall()
    return [dict(r) for r in rows]


async def get_position(user: str, code: str, account_id: str) -> dict | None:
    await require_account(user, account_id)
    db = await get_db()
    row = await (await db.execute(
        "SELECT h.*,p.group_name,p.pair_long_code FROM account_holdings h "
        "LEFT JOIN user_portfolio p ON p.google_sub=h.google_sub AND p.stock_code=h.stock_code "
        "WHERE h.google_sub=? AND h.account_id=? AND h.stock_code=?", (user, account_id, code),
    )).fetchone()
    return dict(row) if row else None


async def choose_account(user: str, code: str, account_id: str | None = None) -> str:
    aid = account_id or current(user)
    if not aid:
        rows = [r for r in await list_positions(user) if r["stock_code"] == code]
        if len(rows) > 1:
            raise accounts.AccountError("여러 계좌에 보유한 종목입니다. 변경할 계좌를 선택해 주세요.")
        aid = rows[0]["account_id"] if rows else await accounts.get_default_account_id(user)
    await require_account(user, aid, writable=True)
    return aid


async def rebuild(user: str, code: str, **metadata) -> dict | None:
    from repositories import portfolio
    rows = [r for r in await list_positions(user) if r["stock_code"] == code]
    if not rows:
        await portfolio._delete_portfolio_projection(user, code)
        return None
    qty = sum(Decimal(str(r["quantity"])) for r in rows)
    cost_currencies = {r["avg_price_currency"] for r in rows}
    mixed = len(cost_currencies) > 1
    cost = sum(Decimal(str(r["quantity"])) * Decimal(str(r["avg_price"])) for r in rows)
    avg = float(cost / qty) if qty and not mixed else 0
    row = rows[0]
    return await portfolio._save_portfolio_projection(
        user, code, metadata.pop("stock_name", row["stock_name"]), float(qty), avg,
        currency=row["currency"], avg_price_currency="KRW" if mixed else row["avg_price_currency"], **metadata,
    )


async def save(user, code, name, quantity, avg_price, currency="KRW", *, account_id=None, avg_price_currency=None, **metadata):
    async with transaction() as db:
        await initialize(db, user)
        aid = await choose_account(user, code, account_id)
        old = await get_position(user, code, aid)
        now = datetime.now().isoformat()
        cost_currency = avg_price_currency or (old or {}).get("avg_price_currency", "KRW")
        other = [r for r in await list_positions(user) if r["stock_code"] == code and r["account_id"] != aid]
        if any(r["currency"] != currency for r in other):
            raise accounts.AccountError("같은 종목의 거래 통화가 다른 계좌와 다릅니다. 종목코드와 통화를 확인해 주세요.")
        if any(r["quantity"] * quantity < 0 for r in other):
            raise accounts.AccountError("같은 종목의 매수·공매도 잔고는 서로 다른 종목코드로 관리해 주세요.")
        created = metadata.get("created_at") or (old or {}).get("created_at") or now
        await db.execute(
            "INSERT INTO account_holdings VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(google_sub,account_id,stock_code) DO UPDATE SET "
            "stock_name=excluded.stock_name,quantity=excluded.quantity,avg_price=excluded.avg_price,"
            "avg_price_currency=excluded.avg_price_currency,currency=excluded.currency,created_at=excluded.created_at,updated_at=excluded.updated_at",
            (user, aid, code, name, quantity, avg_price, cost_currency, currency, created, now),
        )
        projection = await rebuild(user, code, stock_name=name, **metadata)
        return {**projection, "account_id": aid, "quantity": quantity, "avg_price": avg_price, "avg_price_currency": cost_currency}


async def delete(user: str, code: str, account_id: str | None = None) -> bool:
    async with transaction() as db:
        await initialize(db, user)
        aid = await choose_account(user, code, account_id)
        result = await db.execute("DELETE FROM account_holdings WHERE google_sub=? AND account_id=? AND stock_code=?", (user, aid, code))
        await rebuild(user, code)
        return result.rowcount > 0


async def annotate(user: str, items: list[dict], account_id: str | None = None) -> list[dict]:
    positions = await list_positions(user, account_id)
    by_code = {}
    for position in positions:
        by_code.setdefault(position["stock_code"], []).append(position)
    output = []
    for item in items:
        rows = by_code.get(item["stock_code"], [])
        if account_id and not rows:
            continue
        if account_id:
            item.update(rows[0])
        item["account_positions"] = rows
        item["account_count"] = len(rows)
        output.append(item)
    return output
