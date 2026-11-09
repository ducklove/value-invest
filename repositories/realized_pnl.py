"""NH 실현손익 원장과 수집 상태, 세금 추정에 필요한 읽기.

증권사 응답의 정규화 사본이다. 잔고·현금·NAV 입출금을 바꾸지 않는다. 쓰기는 연결 소유자만 할 수 있고,
재조회는 group_key 단위 교체라 같은 응답을 다시 넣어도 행이 늘지 않는다.
"""

import json

from domain.broker_activity import stamp
from repositories import brokers
from repositories.broker_secrets import BrokerError
from repositories.db import get_db, transaction

DETAIL_PENDING = "detail_pending"
DETAIL_MISSING = "detail_missing"
_ROW_COLUMNS = ("source_key", "group_key", "market", "trade_date", "stock_code", "symbol", "stock_name", "nation_code",
                "currency", "sell_quantity", "sell_native", "realized_native", "sell_krw", "realized_krw")


async def _require_link(user: str, link: dict) -> None:
    """다른 사용자의 계좌이거나 수집 중 연결이 바뀌면 쓰지 않는다."""
    current = await brokers.get_link(user, link["account_id"])
    if current["account_fingerprint"] != link["account_fingerprint"]:
        raise BrokerError("수집 중 계좌 연결이 변경되어 손익 반영을 보류했습니다.")


async def _state(db, user: str, aid: str, year: int, market: str) -> dict | None:
    row = await (await db.execute(
        "SELECT * FROM broker_realized_pnl_state WHERE google_sub=? AND account_id=? AND year=? AND market=?",
        (user, aid, year, market))).fetchone()
    return dict(row) if row else None


async def state(user: str, aid: str, year: int, market: str) -> dict | None:
    return await _state(await get_db(), user, aid, year, market)


async def _group_keys(db, user: str, fingerprint: str, market: str, year: int) -> set[str]:
    rows = await (await db.execute(
        "SELECT DISTINCT group_key FROM broker_realized_pnl WHERE google_sub=? AND account_fingerprint=? AND market=? "
        "AND substr(trade_date,1,4)=?", (user, fingerprint, market, f"{year:04d}"))).fetchall()
    return {row["group_key"] for row in rows}


async def _delete_group(db, user: str, fingerprint: str, group_key: str) -> None:
    await db.execute("DELETE FROM broker_realized_pnl WHERE google_sub=? AND account_fingerprint=? AND group_key=?",
                     (user, fingerprint, group_key))


async def _insert(db, user: str, link: dict, entry: dict, now: str) -> None:
    columns = ("google_sub", "account_id", "account_fingerprint", *_ROW_COLUMNS, "issues_json", "data_json", "created_at")
    values = (user, link["account_id"], link["account_fingerprint"], *(entry.get(key) for key in _ROW_COLUMNS),
              json.dumps(entry.get("issues") or [], ensure_ascii=False), json.dumps(entry.get("data") or {}, ensure_ascii=False), now)
    await db.execute(f"INSERT INTO broker_realized_pnl ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", values)


async def apply_period(user: str, link: dict, year: int, market: str, groups: dict[str, dict], broker_total: float | None) -> int:
    """기간 합계 응답 전체를 반영한다. 사라진 묶음은 지우고 내용이 바뀐 묶음만 교체한다. 바뀐 묶음 수를 돌려준다."""
    fingerprint = link["account_fingerprint"]
    now = stamp()
    async with transaction() as db:
        await _require_link(user, link)
        previous = await _state(db, user, link["account_id"], year, market)
        known = json.loads(previous["digests_json"]) if previous and previous["account_fingerprint"] == fingerprint else {}
        existing = await _group_keys(db, user, fingerprint, market, year)
        for key in existing - groups.keys():
            await _delete_group(db, user, fingerprint, key)
        changed = 0
        for key, group in groups.items():
            if known.get(key) == group["digest"] and key in existing:
                continue
            await _delete_group(db, user, fingerprint, key)
            for entry in group["entries"]:
                await _insert(db, user, link, entry, now)
            changed += 1
        digests = json.dumps({key: group["digest"] for key, group in groups.items()}, sort_keys=True)
        await db.execute(
            "INSERT INTO broker_realized_pnl_state (account_id,google_sub,year,market,account_fingerprint,digests_json,broker_total,last_success_at,attempted_at) "
            "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,year,market) DO UPDATE SET account_fingerprint=excluded.account_fingerprint,"
            "digests_json=excluded.digests_json,broker_total=excluded.broker_total,last_success_at=excluded.last_success_at,attempted_at=excluded.attempted_at",
            (link["account_id"], user, year, market, fingerprint, digests, broker_total, now, now))
        return changed


async def pending_groups(user: str, link: dict, year: int, market: str) -> list[str]:
    """아직 합계 1행으로 남아 종목·일자 상세를 조회해야 하는 묶음."""
    db = await get_db()
    rows = await (await db.execute(
        "SELECT DISTINCT group_key FROM broker_realized_pnl WHERE google_sub=? AND account_fingerprint=? AND market=? "
        "AND substr(trade_date,1,4)=? AND issues_json LIKE ? ORDER BY group_key",
        (user, link["account_fingerprint"], market, f"{year:04d}", f'%"{DETAIL_PENDING}"%'))).fetchall()
    return [row["group_key"] for row in rows]


async def apply_detail(user: str, link: dict, group_key: str, entries: list[dict] | None) -> None:
    """합계와 맞는 상세로 묶음을 교체한다. 맞지 않으면(None) 합계 행을 유지하고 다시 조회하지 않게 표시한다."""
    fingerprint = link["account_fingerprint"]
    async with transaction() as db:
        await _require_link(user, link)
        rows = await (await db.execute(
            "SELECT id,issues_json FROM broker_realized_pnl WHERE google_sub=? AND account_fingerprint=? AND group_key=?",
            (user, fingerprint, group_key))).fetchall()
        if entries:
            await _delete_group(db, user, fingerprint, group_key)
            now = stamp()
            for entry in entries:
                await _insert(db, user, link, entry, now)
            return
        for row in rows:
            issues = [DETAIL_MISSING if issue == DETAIL_PENDING else issue for issue in json.loads(row["issues_json"])]
            await db.execute("UPDATE broker_realized_pnl SET issues_json=? WHERE id=?", (json.dumps(issues), row["id"]))


async def finish(user: str, link: dict, year: int, market: str) -> None:
    """한 구분의 수집을 끝까지 마쳤다. 연속 실패와 오류 문구를 지운다."""
    now = stamp()
    async with transaction() as db:
        await _require_link(user, link)
        await db.execute(
            "INSERT INTO broker_realized_pnl_state (account_id,google_sub,year,market,account_fingerprint,last_success_at,attempted_at) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(account_id,year,market) DO UPDATE SET account_fingerprint=excluded.account_fingerprint,"
            "last_success_at=excluded.last_success_at,attempted_at=excluded.attempted_at,failed_attempts=0,error=NULL",
            (link["account_id"], user, year, market, link["account_fingerprint"], now, now))


async def record_failure(user: str, link: dict, year: int, market: str, message: str) -> None:
    """이전 원장을 지우지 않고 실패만 남긴다. 연속 실패 횟수는 재조회 간격에 쓴다."""
    now = stamp()
    async with transaction() as db:
        await _require_link(user, link)
        await db.execute(
            "INSERT INTO broker_realized_pnl_state (account_id,google_sub,year,market,account_fingerprint,attempted_at,failed_attempts,error) "
            "VALUES (?,?,?,?,?,?,1,?) ON CONFLICT(account_id,year,market) DO UPDATE SET account_fingerprint=excluded.account_fingerprint,"
            "attempted_at=excluded.attempted_at,failed_attempts=failed_attempts+1,error=excluded.error",
            (link["account_id"], user, year, market, link["account_fingerprint"], now, message[:300]))


async def states(user: str) -> list[dict]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT account_id,year,market,broker_total,last_success_at,attempted_at,failed_attempts,error "
        "FROM broker_realized_pnl_state WHERE google_sub=?", (user,))).fetchall()
    return [dict(row) for row in rows]


async def rows(user: str) -> list[dict]:
    """세금 계산 대상 행. 일반 유형 계좌만 사용한다(ISA·연금·배우자 계좌는 과세 단위나 규칙이 다르다)."""
    db = await get_db()
    records = await (await db.execute(
        "SELECT r.*,a.name AS account_name FROM broker_realized_pnl r "
        "JOIN portfolio_accounts a ON a.account_id=r.account_id AND a.google_sub=r.google_sub "
        "WHERE r.google_sub=? AND a.type='general' ORDER BY r.trade_date DESC,r.id DESC", (user,))).fetchall()
    return [{**dict(row), "issues": json.loads(row["issues_json"])} for row in records]


async def account_coverage(user: str) -> list[dict]:
    """계좌별 유형·연결·보유 행 수. 계좌번호는 반환하지 않는다."""
    db = await get_db()
    records = await (await db.execute(
        "SELECT a.account_id,a.name,a.type,l.provider,l.environment,l.product,"
        "(SELECT COUNT(*) FROM account_holdings h WHERE h.google_sub=a.google_sub AND h.account_id=a.account_id AND h.quantity!=0) AS holdings "
        "FROM portfolio_accounts a LEFT JOIN broker_account_links l ON l.account_id=a.account_id AND l.google_sub=a.google_sub "
        "WHERE a.google_sub=? ORDER BY a.sort_order,a.account_id", (user,))).fetchall()
    return [dict(row) for row in records]


async def interest_rows(user: str, year: int) -> list[dict]:
    """사유 대조용 일별 합계의 원화 이자. 같은 NH 계좌를 다른 계좌 행으로 다시 연결해도 한 번만 센다."""
    db = await get_db()
    records = await (await db.execute(
        "SELECT MIN(account_id) AS account_id,date,kind,MAX(amount) AS amount FROM broker_cash_review_evidence "
        "WHERE google_sub=? AND currency='KRW' AND kind IN ('interest','mmw_interest') AND substr(date,1,4)=? "
        "GROUP BY fingerprint,booked_date,date,kind", (user, f"{year:04d}"))).fetchall()
    return [dict(row) for row in records]


async def ledger_interest_rows(user: str, year: int) -> list[dict]:
    """수입 원장의 이자 행. 일반 수입 가져오기가 보류 중이라 2026-11 운영에는 0건이다(docs/realized-tax.md)."""
    db = await get_db()
    records = await (await db.execute(
        "SELECT account_id,income_krw,data_json FROM broker_transactions WHERE google_sub=? AND kind='interest' "
        "AND substr(json_extract(data_json,'$.date'),1,4)=?", (user, f"{year:04d}"))).fetchall()
    return [{"account_id": row["account_id"], "income_krw": row["income_krw"], **json.loads(row["data_json"])} for row in records]


async def nh_positions(user: str) -> list[dict]:
    """NH 실계좌의 현재 보유분. 손실 실현 추정의 대상이며 수량이 없는 행은 제외한다."""
    db = await get_db()
    records = await (await db.execute(
        "SELECT h.account_id,a.name AS account_name,a.type AS account_type,h.stock_code,h.stock_name,h.quantity,h.avg_price,"
        "h.avg_price_currency,h.currency FROM account_holdings h "
        "JOIN broker_account_links l ON l.account_id=h.account_id AND l.google_sub=h.google_sub "
        "JOIN portfolio_accounts a ON a.account_id=h.account_id AND a.google_sub=h.google_sub "
        "WHERE h.google_sub=? AND l.provider='namuh' AND l.environment='live' AND h.quantity!=0 ORDER BY h.stock_code", (user,))).fetchall()
    return [dict(row) for row in records]


async def latest_stock_prices(user: str) -> list[dict]:
    """마지막 일별 정산의 원화 단가·환율. 라이브 시세 없이 같은 기준일로 평가한다."""
    db = await get_db()
    records = await (await db.execute(
        "SELECT date,stock_code,quantity,unit_price,currency,fx_rate,priced_from_fallback FROM portfolio_stock_snapshots "
        "WHERE google_sub=? AND date=(SELECT MAX(date) FROM portfolio_stock_snapshots WHERE google_sub=?)", (user, user))).fetchall()
    return [dict(row) for row in records]
