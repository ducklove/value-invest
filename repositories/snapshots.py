"""Portfolio NAV / snapshots / cashflows repository.

Daily + intraday snapshots, NAV/group-weight/constituent history, and atomic
cashflow transactions. NAV, cashflow markers and constituent snapshots share
the repositories.db.transaction boundary.
"""

from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import aiosqlite

from repositories.db import get_db, transaction

KST = ZoneInfo("Asia/Seoul")


async def _refresh_group_snapshots(db: aiosqlite.Connection, google_sub: str | None = None, snap_date: str | None = None):
    """Rebuild pre-aggregated group weights from per-stock snapshots.

    Group trend reads must stay cheap as history grows, so the expensive
    stock-level GROUP BY happens once at snapshot time (or one-time backfill),
    not on every chart request.
    """
    where = []
    params: list[str] = []
    if google_sub is not None:
        where.append("ps.google_sub = ?")
        params.append(google_sub)
    if snap_date is not None:
        where.append("ps.date = ?")
        params.append(snap_date)
    where_sql = "WHERE " + " AND ".join(where) if where else ""

    if google_sub is not None and snap_date is not None:
        await db.execute(
            "DELETE FROM portfolio_group_snapshots WHERE google_sub = ? AND date = ?",
            (google_sub, snap_date),
        )
    elif google_sub is not None:
        await db.execute("DELETE FROM portfolio_group_snapshots WHERE google_sub = ?", (google_sub,))
    else:
        await db.execute("DELETE FROM portfolio_group_snapshots")

    await db.execute(
        f"""
        WITH stock_rows AS (
            SELECT
                ps.google_sub,
                ps.date,
                COALESCE(ps.group_name, up.group_name, '기타') AS group_name,
                ps.stock_code,
                ps.market_value
            FROM portfolio_stock_snapshots ps
            LEFT JOIN user_portfolio up
              ON up.google_sub = ps.google_sub
             AND up.stock_code = ps.stock_code
            {where_sql}
        ),
        day_totals AS (
            SELECT google_sub, date, SUM(market_value) AS total_value
            FROM stock_rows
            GROUP BY google_sub, date
        ),
        group_rows AS (
            SELECT
                google_sub,
                date,
                group_name,
                SUM(market_value) AS market_value,
                COUNT(DISTINCT stock_code) AS stock_count
            FROM stock_rows
            GROUP BY google_sub, date, group_name
        )
        INSERT OR REPLACE INTO portfolio_group_snapshots
        (google_sub, date, group_name, market_value, stock_count, total_value, weight_pct)
        SELECT
            gr.google_sub,
            gr.date,
            gr.group_name,
            gr.market_value,
            gr.stock_count,
            dt.total_value AS total_value,
            CASE
                WHEN dt.total_value != 0
                THEN gr.market_value * 100.0 / dt.total_value
                ELSE NULL
            END AS weight_pct
        FROM group_rows gr
        JOIN day_totals dt
          ON dt.google_sub = gr.google_sub
         AND dt.date = gr.date
        """,
        tuple(params),
    )


async def _refresh_stock_weight_snapshots(db: aiosqlite.Connection, google_sub: str | None = None, snap_date: str | None = None):
    """Rebuild pre-aggregated per-stock weights for group drill-down charts."""
    where = []
    params: list[str] = []
    if google_sub is not None:
        where.append("ps.google_sub = ?")
        params.append(google_sub)
    if snap_date is not None:
        where.append("ps.date = ?")
        params.append(snap_date)
    where_sql = "WHERE " + " AND ".join(where) if where else ""

    if google_sub is not None and snap_date is not None:
        await db.execute(
            "DELETE FROM portfolio_stock_weight_snapshots WHERE google_sub = ? AND date = ?",
            (google_sub, snap_date),
        )
    elif google_sub is not None:
        await db.execute("DELETE FROM portfolio_stock_weight_snapshots WHERE google_sub = ?", (google_sub,))
    else:
        await db.execute("DELETE FROM portfolio_stock_weight_snapshots")

    await db.execute(
        f"""
        WITH stock_rows AS (
            SELECT
                ps.google_sub,
                ps.date,
                ps.stock_code,
                COALESCE(up.stock_name, ps.stock_code) AS stock_name,
                COALESCE(ps.group_name, up.group_name, '기타') AS group_name,
                ps.market_value
            FROM portfolio_stock_snapshots ps
            LEFT JOIN user_portfolio up
              ON up.google_sub = ps.google_sub
             AND up.stock_code = ps.stock_code
            {where_sql}
        ),
        day_totals AS (
            SELECT google_sub, date, SUM(market_value) AS total_value
            FROM stock_rows
            GROUP BY google_sub, date
        ),
        group_totals AS (
            SELECT google_sub, date, group_name, SUM(market_value) AS group_value
            FROM stock_rows
            GROUP BY google_sub, date, group_name
        )
        INSERT OR REPLACE INTO portfolio_stock_weight_snapshots
        (google_sub, date, group_name, stock_code, stock_name, market_value, group_value, total_value, group_weight_pct, portfolio_weight_pct)
        SELECT
            sr.google_sub,
            sr.date,
            sr.group_name,
            sr.stock_code,
            sr.stock_name,
            sr.market_value,
            gt.group_value,
            dt.total_value,
            CASE
                WHEN gt.group_value != 0
                THEN sr.market_value * 100.0 / gt.group_value
                ELSE NULL
            END AS group_weight_pct,
            CASE
                WHEN dt.total_value != 0
                THEN sr.market_value * 100.0 / dt.total_value
                ELSE NULL
            END AS portfolio_weight_pct
        FROM stock_rows sr
        JOIN group_totals gt
          ON gt.google_sub = sr.google_sub
         AND gt.date = sr.date
         AND gt.group_name = sr.group_name
        JOIN day_totals dt
          ON dt.google_sub = sr.google_sub
         AND dt.date = sr.date
        """,
        tuple(params),
    )


async def ensure_initial_snapshot_backfills(db: aiosqlite.Connection) -> None:
    """Run init_db's one-time aggregate snapshot rebuilds when target tables are empty."""
    cursor = await db.execute("SELECT COUNT(*) AS n FROM portfolio_group_snapshots")
    group_snapshot_count = (await cursor.fetchone())["n"]
    if group_snapshot_count == 0:
        await _refresh_group_snapshots(db)

    cursor = await db.execute("SELECT COUNT(*) AS n FROM portfolio_stock_weight_snapshots")
    stock_weight_snapshot_count = (await cursor.fetchone())["n"]
    if stock_weight_snapshot_count == 0:
        await _refresh_stock_weight_snapshots(db)


def _return_snapshot(row) -> dict | None:
    if row is None:
        return None
    result = dict(row)
    result["return_nav"] = result["nav"] * result.get("return_factor", 1)
    return result


async def get_latest_snapshot(google_sub: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, distribution_per_unit, return_factor FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date DESC LIMIT 1",
        (google_sub,),
    )
    row = await cursor.fetchone()
    return _return_snapshot(row)


async def get_snapshot_by_date(google_sub: str, snap_date: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw, distribution_per_unit, return_factor FROM portfolio_snapshots WHERE google_sub = ? AND date = ?",
        (google_sub, snap_date),
    )
    row = await cursor.fetchone()
    return _return_snapshot(row)


async def get_latest_snapshot_before_date(google_sub: str, snap_date: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw, distribution_per_unit, return_factor FROM portfolio_snapshots WHERE google_sub = ? AND date < ? ORDER BY date DESC LIMIT 1",
        (google_sub, snap_date),
    )
    row = await cursor.fetchone()
    return _return_snapshot(row)


async def save_snapshot(google_sub: str, date: str, total_value: float, total_invested: float, nav: float, total_units: float, fx_usdkrw: float | None = None, *, cashflow_cutoff_at: str | None = None, distribution_per_unit: float | None = None, return_factor: float | None = None):
    # 단문이지만 공유 커넥션 위의 맨 commit 은 다른 task 의 진행 중 쓰기를
    # 같이 커밋할 수 있어 transaction() 으로 통일한다 (이하 쓰기 헬퍼 동일).
    async with transaction() as db:
        previous = await get_snapshot_by_date(google_sub, date) or {}
        distribution_per_unit = previous.get("distribution_per_unit", 0) if distribution_per_unit is None else distribution_per_unit
        return_factor = previous.get("return_factor", 1) if return_factor is None else return_factor
        await db.execute(
            """INSERT OR REPLACE INTO portfolio_snapshots (google_sub, date, total_value, total_invested, nav, total_units, fx_usdkrw, cashflow_cutoff_at, distribution_per_unit, return_factor)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (google_sub, date, total_value, total_invested, nav, total_units, fx_usdkrw, cashflow_cutoff_at, distribution_per_unit, return_factor),
        )


async def get_month_end_snapshot(google_sub: str) -> dict | None:
    """Get the portfolio snapshot at the end of the previous month."""
    from datetime import date, timedelta
    month_end = date.today().replace(day=1) - timedelta(days=1)
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw, distribution_per_unit, return_factor FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (google_sub, month_end.isoformat()),
    )
    row = await cursor.fetchone()
    return _return_snapshot(row)


async def get_year_start_snapshot(google_sub: str) -> dict | None:
    """Get the last portfolio snapshot of the previous year (YTD base)."""
    from datetime import date
    year_end = date(date.today().year - 1, 12, 31).isoformat()
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw, distribution_per_unit, return_factor FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (google_sub, year_end),
    )
    row = await cursor.fetchone()
    return _return_snapshot(row)


async def get_snapshot_on_or_before(google_sub: str, snap_date: str) -> dict | None:
    """정산일 이하의 최신 스냅샷 — Today 카드의 기준선.

    ``get_latest_snapshot_before_date`` 는 date < 인 반면 이쪽은 date <= 다.
    20:00 정산 경계에서 당일 스냅샷이 이미 찍혔다면 그것이 기준이어야 한다.
    """
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw, distribution_per_unit, return_factor "
        "FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (google_sub, snap_date),
    )
    row = await cursor.fetchone()
    return _return_snapshot(row)


async def get_cashflows_created_after(google_sub: str, created_after: str) -> list[dict]:
    """기준 정산 이후 잔고에 반영된 거래. 명목 날짜보다 실제 정산 귀속이 우선한다.

    호출자는 기준일의 정산 마커를 넘긴다. 새 스냅샷은 잔고를 읽은 실제
    시점을 쓰고, 이전 스냅샷은 20:00 마커로 호환한다. 이미 기준 정산에
    반영된 거래는 생성 시각이 20시 이후여도 다시 보정하지 않는다.
    """
    db = await get_db()
    snapshot_date = created_after[:10]
    cursor = await db.execute(
        "SELECT cashflow_cutoff_at FROM portfolio_snapshots WHERE google_sub = ? AND date = ?",
        (google_sub, snapshot_date),
    )
    snap = await cursor.fetchone()
    cutoff = (snap["cashflow_cutoff_at"] if snap else None) or created_after
    cursor = await db.execute(
        "SELECT id, date, type, amount, nav_at_time, units_change, applied_snapshot_date, created_at "
        "FROM portfolio_cashflows WHERE google_sub = ? AND "
        "(applied_snapshot_date > ? OR (applied_snapshot_date IS NULL AND created_at > ?)) "
        "ORDER BY created_at ASC, id ASC",
        (google_sub, snapshot_date, cutoff),
    )
    rows = [dict(row) for row in await cursor.fetchall()]
    distributions = await get_distribution_flows(google_sub)
    rows.extend(row for row in distributions if (row["applied_snapshot_date"] and row["applied_snapshot_date"] > snapshot_date)
                or (not row["applied_snapshot_date"] and row["created_at"] > cutoff))
    return sorted(rows, key=lambda row: (row["created_at"], row["id"]))


async def get_nav_input_state(google_sub: str) -> tuple:
    """짧은 트랜잭션 안에서 읽어 평가 전후의 입력 일치를 검증한다."""
    db = await get_db()
    state = []
    for sql in (
        "SELECT * FROM user_portfolio WHERE google_sub = ? ORDER BY stock_code",
        "SELECT * FROM portfolio_cashflows WHERE google_sub = ? ORDER BY id",
        "SELECT * FROM portfolio_distributions WHERE google_sub = ? ORDER BY id",
        "SELECT * FROM portfolio_dividend_receipts WHERE google_sub = ? ORDER BY id",
        "SELECT * FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date DESC LIMIT 1",
    ):
        cursor = await db.execute(sql, (google_sub,))
        state.append(tuple(tuple(row) for row in await cursor.fetchall()))
    return tuple(state)


async def get_nav_history(google_sub: str) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, nav, total_value, total_invested, total_units, fx_usdkrw, distribution_per_unit, return_factor FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date ASC",
        (google_sub,),
    )
    return [_return_snapshot(row) for row in await cursor.fetchall()]


async def get_group_weight_history(google_sub: str) -> list[dict]:
    """Return pre-aggregated per-date portfolio group weights."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM portfolio_group_snapshots WHERE google_sub = ?",
        (google_sub,),
    )
    row = await cursor.fetchone()
    if not row or row["n"] == 0:
        # 재구축은 DELETE + INSERT 두 문장 — 중간 실패 시 집계 테이블이
        # 빈 채로 남지 않도록 명시 트랜잭션으로 묶는다.
        async with transaction() as txn:
            await _refresh_group_snapshots(txn, google_sub=google_sub)

    cursor = await db.execute(
        """
        SELECT date, group_name, market_value, stock_count, total_value, weight_pct
        FROM portfolio_group_snapshots
        WHERE google_sub = ?
        ORDER BY date ASC, market_value DESC
        """,
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_group_constituent_history(google_sub: str, group_name: str) -> list[dict]:
    """Return pre-aggregated stock weights within one portfolio group."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM portfolio_stock_weight_snapshots WHERE google_sub = ?",
        (google_sub,),
    )
    row = await cursor.fetchone()
    if not row or row["n"] == 0:
        # 위 get_group_weight_history 와 동일 — 원자적 재구축.
        async with transaction() as txn:
            await _refresh_stock_weight_snapshots(txn, google_sub=google_sub)

    cursor = await db.execute(
        """
        SELECT
            date,
            stock_code,
            stock_name,
            market_value,
            group_value,
            total_value,
            group_weight_pct AS weight_pct,
            portfolio_weight_pct
        FROM portfolio_stock_weight_snapshots
        WHERE google_sub = ?
          AND group_name = ?
        ORDER BY date ASC, market_value DESC
        """,
        (google_sub, group_name),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_tag_history(google_sub: str, tag: str) -> list[dict]:
    """Return daily value/portfolio-weight trend for the current tag membership."""
    tag_key = str(tag or "").strip().lstrip("#")
    if not tag_key:
        return []
    db = await get_db()
    cursor = await db.execute(
        """
        WITH tag_codes AS (
            SELECT DISTINCT stock_code
            FROM portfolio_tags
            WHERE google_sub = ?
              AND lower(trim(tag)) = lower(trim(?))
        ),
        tagged_rows AS (
            SELECT
                ps.date,
                ps.stock_code,
                ps.market_value
            FROM portfolio_stock_snapshots ps
            JOIN tag_codes tc
              ON tc.stock_code = ps.stock_code
            WHERE ps.google_sub = ?
        ),
        tag_totals AS (
            SELECT
                date,
                SUM(market_value) AS tag_value,
                COUNT(DISTINCT stock_code) AS stock_count
            FROM tagged_rows
            GROUP BY date
        ),
        day_totals AS (
            SELECT date, SUM(market_value) AS total_value
            FROM portfolio_stock_snapshots
            WHERE google_sub = ?
            GROUP BY date
        )
        SELECT
            tt.date,
            tt.tag_value,
            tt.stock_count,
            dt.total_value,
            ps.fx_usdkrw,
            CASE
                WHEN dt.total_value != 0
                THEN tt.tag_value * 100.0 / dt.total_value
                ELSE NULL
            END AS weight_pct
        FROM tag_totals tt
        JOIN day_totals dt
          ON dt.date = tt.date
        LEFT JOIN portfolio_snapshots ps
          ON ps.google_sub = ?
         AND ps.date = tt.date
        ORDER BY tt.date ASC
        """,
        (google_sub, tag_key, google_sub, google_sub, google_sub),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_distribution_flows(google_sub: str) -> list[dict]:
    """일반 입출금과 구분되는, 좌수 변동 없는 분배금 표시·손익 조정용 행."""
    db = await get_db()
    rows = await (await db.execute("SELECT * FROM portfolio_distributions WHERE google_sub=? ORDER BY id", (google_sub,))).fetchall()
    return [{"id": -row["id"], "date": row["date"], "type": "distribution", "amount": row["amount_krw"],
             "nav_at_time": None, "units_change": 0, "applied_snapshot_date": row["applied_snapshot_date"],
             "reversal_of_id": None, "cancelled_at": None, "memo": json.loads(row["result_json"])["memo"],
             "created_at": row["created_at"], "cash_code": f"CASH_{row['currency']}",
             "native_amount": row["amount"], "currency": row["currency"]} for row in rows]


async def get_pending_distributions(google_sub: str, snap_date: str) -> list[dict]:
    db = await get_db()
    rows = await (await db.execute(
        "SELECT id,amount_krw FROM portfolio_distributions WHERE google_sub=? AND date<=? AND applied_snapshot_date IS NULL",
        (google_sub, snap_date),
    )).fetchall()
    return [dict(row) for row in rows]


async def settle_dividend_receipts(google_sub: str, snap_date: str) -> None:
    """현금에 더한 배당 분류를 실제로 반영된 최초 NAV 정산일에 맞춘다."""
    async with transaction() as db:
        rows = await (await db.execute(
            "SELECT id,income_event_id FROM portfolio_dividend_receipts WHERE google_sub=? "
            "AND applied_snapshot_date IS NULL AND date(created_at,'+9 hours')<=?", (google_sub, snap_date),
        )).fetchall()
        for row in rows:
            await db.execute("UPDATE portfolio_income_events SET date=? WHERE id=? AND google_sub=?", (snap_date, row["income_event_id"], google_sub))
            await db.execute("UPDATE portfolio_dividend_receipts SET applied_snapshot_date=? WHERE id=?", (snap_date, row["id"]))


async def get_cashflows(google_sub: str) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT id, date, type, amount, nav_at_time, units_change, applied_snapshot_date, reversal_of_id, cancelled_at, memo, created_at FROM portfolio_cashflows WHERE google_sub = ? ORDER BY date DESC, created_at DESC",
        (google_sub,),
    )
    rows = [dict(row) for row in await cursor.fetchall()] + await get_distribution_flows(google_sub)
    return sorted(rows, key=lambda row: (row["date"], row["created_at"]), reverse=True)


class CashflowBalanceError(ValueError):
    def __init__(self, balance: float, amount: float):
        self.balance = balance
        self.amount = amount
        super().__init__(f"insufficient CASH_KRW balance: {balance} < {amount}")


class CashflowCancellationError(ValueError):
    pass


async def _sync_cash(db, google_sub: str, delta: float, now: str) -> None:
    cursor = await db.execute(
        "SELECT quantity FROM user_portfolio WHERE google_sub = ? AND stock_code = 'CASH_KRW'",
        (google_sub,),
    )
    cash = await cursor.fetchone()
    balance = float(cash["quantity"]) if cash else 0.0
    if balance + delta < 0:
        raise CashflowBalanceError(balance, -delta)
    if cash:
        await db.execute(
            "UPDATE user_portfolio SET quantity = ?, avg_price = 1.0, updated_at = ? "
            "WHERE google_sub = ? AND stock_code = 'CASH_KRW'",
            (balance + delta, now, google_sub),
        )
    elif delta > 0:
        await db.execute(
            "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, avg_price, quantity, currency, created_at, updated_at) "
            "VALUES (?, 'CASH_KRW', '원화', 1.0, ?, 'KRW', ?, ?)",
            (google_sub, delta, now, now),
        )


async def add_cashflow_and_sync_cash(
    google_sub: str, date: str, cf_type: str, amount: float,
    memo: str | None, nav_at_time: float | None, units_change: float | None,
) -> dict:
    now = datetime.now(KST).replace(tzinfo=None).isoformat()
    if date > now[:10]:
        raise CashflowCancellationError("미래 날짜의 입출금은 등록할 수 없습니다.")
    async with transaction() as db:
        await _sync_cash(db, google_sub, amount if cf_type == "deposit" else -amount, now)
        cursor = await db.execute(
            "INSERT INTO portfolio_cashflows (google_sub, date, type, amount, nav_at_time, units_change, memo, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (google_sub, date, cf_type, amount, nav_at_time, units_change, memo, now),
        )
        return {
            "id": cursor.lastrowid, "date": date, "type": cf_type, "amount": amount,
            "nav_at_time": nav_at_time, "units_change": units_change,
            "memo": memo, "created_at": now,
        }


async def delete_cashflow_and_sync_cash(google_sub: str, cf_id: int) -> bool:
    """미정산 거래는 제거하고, 정산된 거래는 오늘의 반대 거래로 취소한다.

    과거 평가액/좌수를 고치면 이후 수익률까지 변하므로 완료된 정산은
    보존한다. 취소 거래도 다음 정산까지 같은 미정산 보정을 받는다.
    """
    now = datetime.now(KST).replace(tzinfo=None).isoformat()
    async with transaction() as db:
        cursor = await db.execute(
            "SELECT * FROM portfolio_cashflows WHERE id = ? AND google_sub = ?", (cf_id, google_sub),
        )
        cf = await cursor.fetchone()
        if not cf:
            return False
        if cf["cancelled_at"]:
            return True  # 중복 요청이 잔고를 두 번 바꾸지 않는다.
        if cf["reversal_of_id"] is not None:
            raise CashflowCancellationError("취소 거래는 삭제할 수 없습니다. 새 입출금으로 정정해 주세요.")
        reverse_delta = -cf["amount"] if cf["type"] == "deposit" else cf["amount"]
        await _sync_cash(db, google_sub, reverse_delta, now)
        if cf["applied_snapshot_date"]:
            await db.execute(
                "INSERT INTO portfolio_cashflows (google_sub, date, type, amount, memo, created_at, reversal_of_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (google_sub, now[:10],
                 "withdrawal" if cf["type"] == "deposit" else "deposit",
                 cf["amount"], f"입출금 취소 (원거래 #{cf_id})", now, cf_id),
            )
            await db.execute("UPDATE portfolio_cashflows SET cancelled_at = ? WHERE id = ?", (now, cf_id))
        else:
            await db.execute("DELETE FROM portfolio_cashflows WHERE id = ? AND google_sub = ?", (cf_id, google_sub))
        return True


async def get_all_users_with_portfolio() -> list[str]:
    db = await get_db()
    cursor = await db.execute("SELECT DISTINCT google_sub FROM user_portfolio")
    return [row["google_sub"] for row in await cursor.fetchall()]


async def get_pending_cashflows(google_sub: str, date: str) -> list[dict]:
    """Cashflows not yet folded into any snapshot's total_units, up to `date`.

    date <= 정산일 이면 전부 대상이다 — 주말 입금(토·일 date, 월요일 정산),
    20:05 정산 이후 입력분(당일 date, 다음 정산), 소급 입력분이 모두 여기로
    들어온다. 과거에는 `date = 정산일` 정확 일치라 이들 유닛이 영구 유실돼
    다음 정산에서 NAV 가 입금액만큼 가짜 상승했다.
    """
    db = await get_db()
    cursor = await db.execute(
        "SELECT id, type, amount, units_change FROM portfolio_cashflows "
        "WHERE google_sub = ? AND date <= ? AND applied_snapshot_date IS NULL",
        (google_sub, date),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def save_stock_snapshots(google_sub: str, date: str, items: list[dict]):
    """일별 종목·그룹·비중을 NAV와 같은 트랜잭션에서 교체한다."""
    async with transaction() as db:
        await db.execute(
            "DELETE FROM portfolio_stock_snapshots WHERE google_sub = ? AND date = ?", (google_sub, date),
        )
        await db.executemany(
            """
            INSERT OR REPLACE INTO portfolio_stock_snapshots
            (google_sub, date, stock_code, market_value, group_name, quantity, unit_price, avg_price_krw, cost_basis, priced_from_fallback, currency, fx_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    google_sub,
                    date,
                    it["stock_code"],
                    it["market_value"],
                    it.get("group_name"),
                    it.get("quantity"),
                    it.get("unit_price"),
                    it.get("avg_price_krw"),
                    it.get("cost_basis"),
                    1 if it.get("priced_from_fallback") else 0,
                    it.get("currency"),
                    it.get("fx_rate"),
                )
                for it in items
            ],
        )
        await _refresh_group_snapshots(db, google_sub=google_sub, snap_date=date)
        await _refresh_stock_weight_snapshots(db, google_sub=google_sub, snap_date=date)


async def get_stock_snapshots_exact_date(google_sub: str, snap_date: str) -> list[dict]:
    """합계와 같은 날짜의 종목별 금액만 반환한다. 누락을 다른 날짜로 채우지 않는다."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code, market_value FROM portfolio_stock_snapshots WHERE google_sub = ? AND date = ?",
        (google_sub, snap_date),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_stock_snapshots_by_date(google_sub: str, date: str) -> list[dict]:
    """Get per-stock snapshots on or before a given date (latest available)."""
    db = await get_db()
    # Find the latest snapshot date on or before the target date
    cursor = await db.execute(
        "SELECT MAX(date) AS snap_date FROM portfolio_stock_snapshots WHERE google_sub = ? AND date <= ?",
        (google_sub, date),
    )
    row = await cursor.fetchone()
    snap_date = row["snap_date"] if row else None
    if not snap_date:
        return []
    cursor = await db.execute(
        "SELECT stock_code, market_value FROM portfolio_stock_snapshots WHERE google_sub = ? AND date = ?",
        (google_sub, snap_date),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def get_stock_snapshot_rows_on_or_before(google_sub: str, date: str) -> list[dict]:
    """Detailed per-stock snapshot rows on or before a date.

    Used by period reports where the report needs a stable historical
    allocation table, not just the stock_code -> market_value map. Current
    holdings are used only to fill display names/groups when the historical
    snapshot row does not already carry them.
    """
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT MAX(date) AS snap_date
        FROM portfolio_stock_snapshots
        WHERE google_sub = ? AND date <= ?
        """,
        (google_sub, date),
    )
    row = await cursor.fetchone()
    snap_date = row["snap_date"] if row else None
    if not snap_date:
        return []
    cursor = await db.execute(
        """
        SELECT
            ps.date,
            ps.stock_code,
            COALESCE(up.stock_name, ps.stock_code) AS stock_name,
            COALESCE(ps.group_name, up.group_name, '기타') AS group_name,
            ps.market_value,
            ps.quantity,
            ps.unit_price,
            ps.avg_price_krw,
            ps.cost_basis
        FROM portfolio_stock_snapshots ps
        LEFT JOIN user_portfolio up
          ON up.google_sub = ps.google_sub
         AND up.stock_code = ps.stock_code
        WHERE ps.google_sub = ?
          AND ps.date = ?
        ORDER BY ps.market_value DESC
        """,
        (google_sub, snap_date),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def get_stock_snapshots_before_date(google_sub: str, date: str) -> list[dict]:
    """Get per-stock snapshots strictly before a date (latest available)."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT MAX(date) AS snap_date FROM portfolio_stock_snapshots WHERE google_sub = ? AND date < ?",
        (google_sub, date),
    )
    row = await cursor.fetchone()
    snap_date = row["snap_date"] if row else None
    if not snap_date:
        return []
    cursor = await db.execute(
        "SELECT stock_code, market_value FROM portfolio_stock_snapshots WHERE google_sub = ? AND date = ?",
        (google_sub, snap_date),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def get_latest_stock_snapshot_rows(google_sub: str) -> list[dict]:
    """가장 최근 스냅샷 날짜의 종목별 평가액 행 (리밸런싱 현재 비중 기반).

    그룹/이름/수량은 user_portfolio 에서 보강한다 — 그룹 귀속 규칙
    (COALESCE(ps.group_name, up.group_name, '기타'))은 그룹 비중 차트를 만드는
    ``_refresh_group_snapshots`` 와 동일해, 리밸런싱 보고서의 그룹 비중이 UI 의
    그룹 비중과 같은 기준으로 계산된다. quantity 는 근사 주당가
    (market_value / quantity) 계산용 — 스냅샷 이후 수량이 바뀌면 근사치가 된다.
    """
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT
            ps.date,
            ps.stock_code,
            COALESCE(up.stock_name, ps.stock_code) AS stock_name,
            COALESCE(ps.group_name, up.group_name, '기타') AS group_name,
            ps.market_value,
            up.quantity
        FROM portfolio_stock_snapshots ps
        LEFT JOIN user_portfolio up
          ON up.google_sub = ps.google_sub
         AND up.stock_code = ps.stock_code
        WHERE ps.google_sub = ?
          AND ps.date = (
              SELECT MAX(date) FROM portfolio_stock_snapshots WHERE google_sub = ?
          )
        ORDER BY ps.market_value DESC
        """,
        (google_sub, google_sub),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def save_intraday_snapshot(google_sub: str, ts: str, total_value: float):
    async with transaction() as db:
        await db.execute(
            "INSERT OR REPLACE INTO portfolio_intraday (google_sub, ts, total_value) VALUES (?, ?, ?)",
            (google_sub, ts, total_value),
        )


async def get_intraday_snapshots(google_sub: str, date: str) -> list[dict]:
    """Get intraday snapshots for a given date (YYYY-MM-DD)."""
    return await get_intraday_snapshots_between(
        google_sub,
        date + "T00:00",
        date + "T99:99",
        include_start=True,
    )


async def get_intraday_snapshots_between(
    google_sub: str,
    start_ts: str,
    end_ts: str,
    *,
    include_start: bool = False,
) -> list[dict]:
    """Get intraday snapshots in a timestamp range.

    Timestamps are stored as local KST ISO minutes, so lexicographic range
    scans match chronological ordering.
    """
    db = await get_db()
    start_op = ">=" if include_start else ">"
    cursor = await db.execute(
        f"SELECT ts, total_value FROM portfolio_intraday WHERE google_sub = ? AND ts {start_op} ? AND ts < ? ORDER BY ts ASC",
        (google_sub, start_ts, end_ts),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def delete_old_intraday(days_to_keep: int = 7):
    """Remove intraday data older than N days."""
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days_to_keep)).isoformat()
    async with transaction() as db:
        await db.execute("DELETE FROM portfolio_intraday WHERE ts < ?", (cutoff + "T00:00",))
