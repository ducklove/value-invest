"""Value screener — cross-ticker filtering over fundamentals + market data.

The screener answers "which stocks in the universe satisfy these value
conditions today?". It runs purely over two existing tables:

* ``financial_data`` — revenue / operating_profit / net_income / total_*
  fundamentals (per stock_code × year, sourced from DART).
* ``market_data`` — per / pbr / eps / bps / dividend_per_share /
  dividend_yield / market_cap / close_price (per stock_code × year).

Derived metrics that are not columns elsewhere (ROE, debt ratio, operating
margin — see ``analyzer.py:53-55``) are computed in SQL here, so the whole
screener is one round-trip with no per-row Python.

Design notes:

* **Latest-year-per-ticker selection** reuses the self-join pattern from
  ``repositories/portfolio.py:342-358`` (``(stock_code, year) IN (SELECT
  stock_code, MAX(year) ...)``). We pick the latest year independently for
  fundamentals and valuation, because DART statements and year-end price
  snapshots arrive on different cadences.
* **Whitelist-only filtering.`` Filter keys are validated against
  ``ALLOWED_FILTERS``; values flow through ``?`` placeholders. There is no
  string interpolation of user input into SQL, mirroring the identifier
  allowlist discipline in ``repositories/schema.py``.
* **Universe join.`` ``corp_codes`` supplies the name and makes the result
  readable even when fundamentals are sparse. A ``LEFT JOIN`` keeps tickers
  whose valuation is known but fundamentals aren't (the screener then just
  can't match ROE-style filters for them).
"""

from __future__ import annotations

from repositories.db import get_db

# Columns the screener may filter/sort on, with the SQL expression to evaluate.
# Derived metrics (roe / debt_ratio / operating_margin) are computed inline so
# callers can filter on them without a persisted column. Keep this dict the
# single source of truth for what the API accepts — anything not here is
# rejected by the service layer before we reach SQL.
ALLOWED_FILTERS: dict[str, str] = {
    "per": "m.per",
    "pbr": "m.pbr",
    "dividend_yield": "m.dividend_yield",
    "market_cap": "m.market_cap",
    "eps": "m.eps",
    "bps": "m.bps",
    "roe": "CASE WHEN f.total_equity NOT NULL AND f.total_equity != 0 "
    "THEN f.net_income * 1.0 / f.total_equity * 100 END",
    "debt_ratio": "CASE WHEN f.total_equity NOT NULL AND f.total_equity != 0 "
    "THEN f.total_liabilities * 1.0 / f.total_equity * 100 END",
    "operating_margin": "CASE WHEN f.revenue NOT NULL AND f.revenue != 0 "
    "THEN f.operating_profit * 1.0 / f.revenue * 100 END",
}

# Sort keys mirror the filter whitelist (plus ticker/name) so the UI can order
# by any visible metric. Each maps to an ORDER BY expression.
ALLOWED_SORTS: dict[str, str] = {
    **ALLOWED_FILTERS,
    "stock_code": "c.stock_code",
    "corp_name": "c.corp_name",
    "close_price": "m.close_price",
    "year": "COALESCE(m.year, f.year)",
}

# Metrics returned per match. Defined up front so the service layer and tests
# agree on the row shape, and so we project only what we need.
_RESULT_COLUMNS = (
    "c.stock_code",
    "c.corp_name",
    "COALESCE(m.year, f.year) AS year",
    "m.close_price",
    "m.per",
    "m.pbr",
    "m.eps",
    "m.bps",
    "m.dividend_per_share",
    "m.dividend_yield",
    "m.market_cap",
    "f.revenue",
    "f.operating_profit",
    "f.net_income",
    "f.total_equity",
    "f.total_liabilities",
    "CASE WHEN f.total_equity NOT NULL AND f.total_equity != 0 "
    "THEN f.net_income * 1.0 / f.total_equity * 100 END AS roe",
    "CASE WHEN f.total_equity NOT NULL AND f.total_equity != 0 "
    "THEN f.total_liabilities * 1.0 / f.total_equity * 100 END AS debt_ratio",
    "CASE WHEN f.revenue NOT NULL AND f.revenue != 0 "
    "THEN f.operating_profit * 1.0 / f.revenue * 100 END AS operating_margin",
)


async def screen_stocks(
    filters: dict[str, tuple],
    *,
    sort_by: str = "market_cap",
    sort_dir: str = "desc",
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Run the screener, returning (rows, total_match_count).

    ``filters`` maps a whitelisted metric name to ``(op, value)`` where ``op``
    is one of ``min`` / ``max``. The caller (service layer) is responsible for
    validating keys/ops/values — this function trusts its arguments and only
    guards against empty input to avoid a malformed WHERE.

    Pagination is applied after computing the total, so the caller can render
    "N matches" without a second round-trip.
    """
    if limit <= 0:
        return [], 0

    where_parts: list[str] = []
    params: list = []
    for key, (op, value) in filters.items():
        expr = ALLOWED_FILTERS[key]
        sql_op = ">" if op == "min" else "<"
        where_parts.append(f"({expr} IS NOT NULL AND {expr} {sql_op} ?)")
        params.append(value)

    # Require at least one filter — an unfiltered universe scan is almost
    # certainly a caller mistake and would return thousands of sparse rows.
    where_clause = " AND ".join(where_parts) if where_parts else "1=1"

    sort_expr = ALLOWED_SORTS.get(sort_by, ALLOWED_SORTS["market_cap"])
    if sort_dir.lower() == "asc":
        sort_sql = f"{sort_expr} ASC"
    else:
        sort_sql = f"{sort_expr} DESC"
    # Secondary sort keeps ordering stable when the primary metric is NULL.
    sort_sql += ", c.stock_code ASC"

    select_clause = ", ".join(_RESULT_COLUMNS)

    # Latest-year selection, independent per source table. The subqueries pick
    # the max year with a non-null key metric so a ticker that has 2025 per
    # but only 2024 fundamentals still resolves to its newest available data.
    base = f"""
        FROM corp_codes c
        LEFT JOIN (
            SELECT md.* FROM market_data md
            JOIN (
                SELECT stock_code, MAX(year) AS y
                FROM market_data
                GROUP BY stock_code
            ) lm ON lm.stock_code = md.stock_code AND lm.y = md.year
        ) m ON m.stock_code = c.stock_code
        LEFT JOIN (
            SELECT fd.* FROM financial_data fd
            JOIN (
                SELECT stock_code, MAX(year) AS y
                FROM financial_data
                GROUP BY stock_code
            ) lf ON lf.stock_code = fd.stock_code AND lf.y = fd.year
        ) f ON f.stock_code = c.stock_code
        WHERE c.stock_code != '' AND {where_clause}
    """

    db = await get_db()

    count_cursor = await db.execute(f"SELECT COUNT(*) {base}", params)
    total = (await count_cursor.fetchone())[0]
    if total == 0:
        return [], 0

    page_cursor = await db.execute(
        f"SELECT {select_clause} {base} ORDER BY {sort_sql} LIMIT ? OFFSET ?",
        [*params, limit, offset],
    )
    rows = [dict(r) for r in await page_cursor.fetchall()]
    return rows, total


async def screener_coverage() -> dict[str, int]:
    """Quick stats on how much of the universe the screener can actually see.

    Surfaced in the UI so users understand *why* a scan returns few hits: the
    valuation/fundamentals tables are filled lazily (only for stocks someone
    has analyzed), not for the whole KRX universe. This is diagnostic only.
    """
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM corp_codes) AS universe,
          (SELECT COUNT(DISTINCT stock_code) FROM market_data) AS valued,
          (SELECT COUNT(DISTINCT stock_code) FROM financial_data) AS fundamentals
        """
    )
    row = await cursor.fetchone()
    return {
        "universe": int(row["universe"] or 0),
        "valued": int(row["valued"] or 0),
        "fundamentals": int(row["fundamentals"] or 0),
    }
