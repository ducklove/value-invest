"""Portfolio holdings read-side repository.

Stock-name resolution, the holdings list (get_portfolio), per-stock tags +
suggestions, target-metric / market-valuation rows, and preferred/trailing
dividends. Extracted verbatim from cache.py; cache.py re-exports these as
``cache.<fn>`` so routes/services are unchanged. The default-group helpers
(_resolve_default_group_name 등) and portfolio default backfills live here.
"""

from __future__ import annotations

from datetime import datetime

import aiosqlite

from repositories import accounts as accounts_repo
from repositories import db as db_module
from repositories.db import get_db, transaction
from services.portfolio.identifiers import is_korean_stock as _is_portfolio_korean_stock

_DEFAULT_GROUPS = [
    ("한국주식", 0, 1, "kr"),
    ("해외주식", 1, 1, "foreign"),
    ("기타", 2, 1, "etc"),
]

_SPECIAL_ASSETS_SET = {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH", "CRYPTO_USDT"}


def _is_special_or_cash(code: str) -> bool:
    return code in _SPECIAL_ASSETS_SET or code.startswith("CASH_")


def _default_type_for_code(stock_code: str) -> str:
    """Return the default_type key (kr/foreign/etc) for a stock code."""
    if _is_special_or_cash(stock_code):
        return "etc"
    if _is_portfolio_korean_stock(stock_code):
        return "kr"
    return "foreign"


async def _resolve_default_group_name(db: aiosqlite.Connection, google_sub: str, stock_code: str) -> str:
    """Look up the actual current group name for a default group type, even if renamed."""
    dtype = _default_type_for_code(stock_code)
    cursor = await db.execute(
        "SELECT group_name FROM portfolio_groups WHERE google_sub = ? AND default_type = ?",
        (google_sub, dtype),
    )
    row = await cursor.fetchone()
    if row:
        return row["group_name"]
    # Fallback: original name
    for name, _, _, dt in _DEFAULT_GROUPS:
        if dt == dtype:
            return name
    return "기타"


async def _ensure_default_groups(db: aiosqlite.Connection, google_sub: str):
    cursor = await db.execute(
        "SELECT COUNT(*) AS cnt FROM portfolio_groups WHERE google_sub = ? AND is_default = 1",
        (google_sub,),
    )
    row = await cursor.fetchone()
    if row["cnt"] >= len(_DEFAULT_GROUPS):
        return
    for name, order, is_default, dtype in _DEFAULT_GROUPS:
        await db.execute(
            "INSERT OR IGNORE INTO portfolio_groups (google_sub, group_name, sort_order, is_default, default_type) VALUES (?, ?, ?, ?, ?)",
            (google_sub, name, order, is_default, dtype),
        )


async def backfill_portfolio_defaults(db: aiosqlite.Connection) -> None:
    """Apply init_db's legacy portfolio group/account default backfills."""
    type_by_order = {0: "kr", 1: "foreign", 2: "etc"}
    for order, dtype in type_by_order.items():
        await db.execute(
            "UPDATE portfolio_groups "
            "SET default_type = ? "
            "WHERE is_default = 1 AND sort_order = ? AND default_type IS NULL",
            (dtype, order),
        )
    await db.execute(
        "UPDATE portfolio_groups SET default_type = 'kr' "
        "WHERE is_default = 1 AND default_type IS NULL AND group_name LIKE '%한국%'"
    )
    await db.execute(
        "UPDATE portfolio_groups SET default_type = 'foreign' "
        "WHERE is_default = 1 AND default_type IS NULL AND group_name LIKE '%해외%'"
    )
    await db.execute(
        "UPDATE portfolio_groups SET default_type = 'etc' "
        "WHERE is_default = 1 AND default_type IS NULL"
    )

    cursor = await db.execute("SELECT DISTINCT google_sub FROM user_portfolio")
    subs = [row["google_sub"] for row in await cursor.fetchall()]
    for sub in subs:
        await _ensure_default_groups(db, sub)
        await accounts_repo.ensure_default_account(db, sub)
        await db.execute(
            """
            UPDATE user_portfolio SET group_name = '기타'
            WHERE google_sub = ?
              AND group_name IS NULL
              AND stock_code IN ('KRX_GOLD', 'CRYPTO_BTC', 'CRYPTO_ETH', 'CRYPTO_USDT')
            """,
            (sub,),
        )
        await db.execute(
            """
            UPDATE user_portfolio SET group_name = '한국주식'
            WHERE google_sub = ?
              AND group_name IS NULL
              AND length(stock_code) = 6
              AND substr(stock_code, 1, 5) GLOB '[0-9][0-9][0-9][0-9][0-9]'
            """,
            (sub,),
        )
        await db.execute(
            """
            UPDATE user_portfolio SET group_name = '해외주식'
            WHERE google_sub = ? AND group_name IS NULL
            """,
            (sub,),
        )


async def get_portfolio(google_sub: str) -> list[dict]:
    db = await get_db()
    # created_at is surfaced so the UI can show '등록일자' and let the
    # user edit it. It was already stored on every insert but wasn't in
    # the SELECT list — the column existed server-side but was invisible.
    cursor = await db.execute(
        """
        SELECT stock_code, stock_name, quantity, avg_price,
               COALESCE(avg_price_currency, 'KRW') AS avg_price_currency,
               sort_order,
               COALESCE(currency, 'KRW') AS currency, group_name, benchmark_code,
               created_at, target_price,
               COALESCE(target_price_disabled, 0) AS target_price_disabled,
               NULLIF(target_price_formula, '') AS target_price_formula
        FROM user_portfolio
        WHERE google_sub = ?
        ORDER BY CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END, sort_order ASC, created_at ASC
        """,
        (google_sub,),
    )
    items = [dict(row) for row in await cursor.fetchall()]
    if not items:
        return []
    tag_rows = await get_portfolio_tags_for_user(google_sub)
    tags_by_code: dict[str, list[str]] = {}
    for row in tag_rows:
        tags_by_code.setdefault(row["stock_code"], []).append(row["tag"])
    for item in items:
        item["tags"] = tags_by_code.get(item["stock_code"], [])
    return items


async def get_portfolio_tags_for_user(google_sub: str) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT stock_code, tag, sort_order
        FROM portfolio_tags
        WHERE google_sub = ?
        ORDER BY stock_code ASC, sort_order ASC, tag ASC
        """,
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_portfolio_target_metrics(stock_codes: list[str]) -> dict[str, dict]:
    codes = [str(code or "").strip() for code in stock_codes if str(code or "").strip()]
    if not codes:
        return {}
    placeholders = ",".join("?" for _ in codes)
    db = await get_db()
    cursor = await db.execute(
        f"""
        SELECT stock_code, year, eps, bps, dividend_per_share
        FROM market_data
        WHERE stock_code IN ({placeholders})
        ORDER BY stock_code ASC, year DESC
        """,
        codes,
    )
    result: dict[str, dict] = {code: {"eps": None, "bps": None, "dps": None} for code in codes}
    for row in await cursor.fetchall():
        metrics = result.setdefault(row["stock_code"], {"eps": None, "bps": None, "dps": None})
        for key in ("eps", "bps", "dividend_per_share"):
            out_key = "dps" if key == "dividend_per_share" else key
            if metrics.get(out_key) is not None:
                continue
            value = row[key]
            if value is not None and value > 0:
                metrics[out_key] = value
    return result


async def get_latest_market_valuation(stock_code: str) -> dict:
    code = str(stock_code or "").strip()
    if not code:
        return {}
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT m.stock_code, m.year, m.close_price, m.per, m.pbr, m.eps, m.bps, m.market_cap,
               f.net_income, f.total_equity
        FROM market_data m
        LEFT JOIN financial_data f
          ON f.stock_code = m.stock_code AND f.year = m.year
        WHERE m.stock_code = ?
        ORDER BY m.year DESC
        """,
        (code,),
    )
    rows = [dict(row) for row in await cursor.fetchall()]
    result: dict = {"stock_code": code}
    for row in rows:
        if result.get("year") is None and any(
            row.get(key) is not None
            for key in ("per", "pbr", "eps", "bps", "net_income", "total_equity")
        ):
            result["year"] = row.get("year")
        for key in ("close_price", "per", "pbr", "eps", "bps", "market_cap", "net_income", "total_equity"):
            if result.get(key) is None and row.get(key) is not None:
                result[key] = row.get(key)
    return result if len(result) > 1 else {}


async def upsert_market_target_metrics(rows: list[dict]) -> int:
    values: list[tuple] = []
    for row in rows or []:
        try:
            stock_code = str(row.get("stock_code") or "").strip()
            year = int(row.get("year"))
        except (TypeError, ValueError):
            continue
        if not stock_code:
            continue
        values.append(
            (
                stock_code,
                year,
                row.get("close_price"),
                row.get("per"),
                row.get("pbr"),
                row.get("eps"),
                row.get("bps"),
                row.get("market_cap"),
            )
        )
    if not values:
        return 0

    db = await get_db()
    await db.executemany(
        """INSERT INTO market_data (stock_code, year, close_price, per, pbr, eps, bps, market_cap)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(stock_code, year) DO UPDATE SET
             close_price = COALESCE(excluded.close_price, market_data.close_price),
             per = COALESCE(excluded.per, market_data.per),
             pbr = COALESCE(excluded.pbr, market_data.pbr),
             eps = COALESCE(excluded.eps, market_data.eps),
             bps = COALESCE(excluded.bps, market_data.bps),
             market_cap = COALESCE(excluded.market_cap, market_data.market_cap)""",
        values,
    )
    await db.commit()
    return len(values)


async def get_portfolio_tags(google_sub: str, stock_code: str) -> list[str]:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT tag
        FROM portfolio_tags
        WHERE google_sub = ? AND stock_code = ?
        ORDER BY sort_order ASC, tag ASC
        """,
        (google_sub, stock_code),
    )
    return [row["tag"] for row in await cursor.fetchall()]


async def get_portfolio_tag_suggestions(google_sub: str, *, limit: int = 30) -> list[str]:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT tag, COUNT(*) AS usage_count, MIN(sort_order) AS first_order
        FROM portfolio_tags
        WHERE google_sub = ?
        GROUP BY tag
        ORDER BY usage_count DESC, first_order ASC, tag ASC
        LIMIT ?
        """,
        (google_sub, int(limit)),
    )
    return [row["tag"] for row in await cursor.fetchall()]


async def set_portfolio_tags(google_sub: str, stock_code: str, tags: list[str]) -> list[str]:
    now = datetime.now().isoformat()
    clean_tags: list[str] = []
    seen: set[str] = set()
    for raw_tag in tags:
        tag = " ".join(str(raw_tag or "").strip().lstrip("#").split())[:30]
        if not tag:
            continue
        key = tag.casefold()
        if key in seen:
            continue
        seen.add(key)
        clean_tags.append(tag)
        if len(clean_tags) >= 12:
            break
    # DELETE + INSERT 교체 패턴 — 중간 실패 시 태그가 전부 날아간 상태로
    # 남지 않도록 원자적으로.
    async with transaction() as db:
        await db.execute(
            "DELETE FROM portfolio_tags WHERE google_sub = ? AND stock_code = ?",
            (google_sub, stock_code),
        )
        await db.executemany(
            """
            INSERT INTO portfolio_tags (google_sub, stock_code, tag, sort_order, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (google_sub, stock_code, tag, idx, now)
                for idx, tag in enumerate(clean_tags)
            ],
        )
    return clean_tags


# Korean preferred-stock codes end with a suffix other than '0'. The
# market_data table typically only stores the common stock's dividend
# record, so the preferred stock gets no hit on an exact code match —
# hence the fallback to the common-stock row via _pref_to_common().
_KR_PREF_SUFFIXES = ("5", "7", "8", "9", "K", "L")


def _pref_to_common(code: str) -> str | None:
    """우선주 코드 → 해당 보통주 코드 (e.g. 005935 → 005930, 00088K → 000880).
    우선주가 아니면 None."""
    if len(code) != 6:
        return None
    if code[-1] in _KR_PREF_SUFFIXES:
        return code[:-1] + "0"
    return None


async def get_trailing_dividends(stock_codes: list[str]) -> dict[str, float]:
    """Latest positive `dividend_per_share` per stock across market_data.

    Used for the 배당액 column — the UI multiplies this by the user's
    quantity client-side so edits to quantity reflect instantly. We pull
    the most recent non-zero value because dividend_per_share occasionally
    arrives as 0 for the current year (pre-announcement) and that would
    hide the true trailing yield.

    Excluded: the current calendar year, to avoid the "0 until announced"
    trap the analysis page had (see stock_price.py dividend fallback).

    Preferred-stock fallback: the market_data pipeline (stock_price.py)
    indexes by common-stock code, so a 005935 holding will miss unless
    we also look up its common counterpart 005930. The actual dividend
    paid to preferred holders is usually slightly higher (typical 1%p
    dividend premium) than the common, but the common figure is the
    best automated approximation available until preferred-specific
    data is harvested separately. Without this fallback preferred-stock
    배당액 is silently 0 across the whole table.
    """
    if not stock_codes:
        return {}
    # Build expanded query set = user's codes ∪ their common-stock fallbacks.
    pref_to_common_map: dict[str, str] = {}
    for code in stock_codes:
        common = _pref_to_common(code)
        if common and common != code:
            pref_to_common_map[code] = common
    all_codes = list(set(stock_codes) | set(pref_to_common_map.values()))
    current_year = datetime.now().year
    placeholders = ",".join("?" for _ in all_codes)
    db = await get_db()
    # 0 도 유효한 '무배당 확정' 값으로 취급 — 이전엔 `> 0` 필터로 제외
    # 되어 UI 에서 '-' 로 표시됐는데, 사용자 요청대로 국내 배당 없는 종목
    # 은 '0' 이라 찍어야 정직함. 가장 최근 NOT NULL 연도를 고르므로
    # 과거에 배당이 있었더라도 최신 해에 정책이 바뀌어 0 이 됐으면 그
    # 값이 반영됨 (= 배당 중단 상태 정직 표시).
    cursor = await db.execute(
        f"""SELECT stock_code, dividend_per_share, year
            FROM market_data
            WHERE stock_code IN ({placeholders})
              AND dividend_per_share IS NOT NULL
              AND year < ?
              AND (stock_code, year) IN (
                SELECT stock_code, MAX(year)
                FROM market_data
                WHERE stock_code IN ({placeholders})
                  AND dividend_per_share IS NOT NULL
                  AND year < ?
                GROUP BY stock_code
              )""",
        (*all_codes, current_year, *all_codes, current_year),
    )
    direct_dps = {row["stock_code"]: float(row["dividend_per_share"]) for row in await cursor.fetchall()}

    # Preferred-stock override — a separately-curated Google Sheet carries
    # the authoritative per-year dividend for each preferred (Data!AI =
    # most recent year). When the sheet has an entry — ZERO INCLUDED —
    # that value wins over the common-stock fallback. The curator has
    # explicitly said "sheet 0 means dividend-of-zero, not unknown", so
    # we must not silently substitute the common stock's non-zero number.
    # Only NULL rows (missing) fall through to the common fallback.
    pref_codes = list(pref_to_common_map.keys())
    pref_overrides: dict[str, float] = {}
    if pref_codes:
        pref_placeholders = ",".join("?" for _ in pref_codes)
        cursor = await db.execute(
            f"""SELECT stock_code, dividend_per_share
                FROM preferred_dividends
                WHERE stock_code IN ({pref_placeholders})
                  AND dividend_per_share IS NOT NULL""",
            pref_codes,
        )
        pref_overrides = {row["stock_code"]: float(row["dividend_per_share"]) for row in await cursor.fetchall()}

    # foreign_dividends — yfinance-sourced + admin manual overrides.
    # Single lookup handles everything outside the KR market_data pipeline.
    # We query for all requested codes (not just preferred) because:
    #   - Overseas tickers (AAPL, GOOGL, ...) live here exclusively.
    #   - Admin may occasionally override any code via the manual API
    #     (naming calls this table 'foreign' but practically it's a
    #     generic override lane — market_data still wins when present).
    foreign_placeholders = ",".join("?" for _ in stock_codes)
    cursor = await db.execute(
        f"""SELECT stock_code, dps_krw
            FROM foreign_dividends
            WHERE stock_code IN ({foreign_placeholders})
              AND dps_krw IS NOT NULL""",
        stock_codes,
    )
    foreign_overrides = {row["stock_code"]: float(row["dps_krw"]) for row in await cursor.fetchall()}

    # Resolution order (most-specific first):
    #   (a) market_data 에 양수 dps — 한국 보통주의 authoritative 값
    #   (b) foreign_dividends — yfinance 해외 주식 / 관리자 manual override
    #       이 계층이 (a) 의 '0' 값보다 먼저 체크되는 것이 핵심: 채권
    #       ETF 같은 종목은 market_data 에 dps=0 으로 저장되지만 실제로
    #       분배금이 있으므로 관리자가 override 한 값이 있으면 그걸
    #       사용해야 함. market_data 양수 (a) 는 여전히 최상위라서 일반
    #       한국 주식의 자동 수집 값은 override 에 덮이지 않음.
    #   (c) market_data 의 0 값 — 배당 없는 종목 (ETF 등 포함, override
    #       미등록 시). '-' 가 아니라 '0' 으로 표시됨.
    #   (d) preferred_dividends (curated sheet, 0 은 유효 확정값)
    #   (e) 보통주 market_data fallback — 우선주에만 적용. 보통주가 0
    #       이라도 허용해서 '배당 중단' 상태 반영.
    out: dict[str, float] = {}
    for code in stock_codes:
        direct = direct_dps.get(code)
        if direct is not None and direct > 0:
            out[code] = direct
        elif code in foreign_overrides:
            out[code] = foreign_overrides[code]
        elif direct is not None:
            # market_data dps == 0 and no override
            out[code] = direct
        elif code in pref_overrides:
            out[code] = pref_overrides[code]
        elif code in pref_to_common_map and pref_to_common_map[code] in direct_dps:
            out[code] = direct_dps[pref_to_common_map[code]]
    return out


async def upsert_preferred_dividends(rows: list[dict]) -> int:
    """Bulk upsert from the Google Sheet fetcher.

    rows: [{
        "stock_code": "012205",
        "dividend_per_share": 0.0 | None,
        "source_name": "계양전기우",
        "common_code": "012200",
        "sheet_year": 2025,
    }, ...]

    Returns number of rows written. PK is stock_code so re-imports are
    idempotent — caller doesn't need to clear the table first. Rows with
    missing stock_code are skipped silently.
    """
    if not rows:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    db = await get_db()
    written = 0
    for r in rows:
        code = (r.get("stock_code") or "").strip()
        if not code:
            continue
        await db.execute(
            """INSERT INTO preferred_dividends
               (stock_code, dividend_per_share, source_name, common_code, sheet_year, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(stock_code) DO UPDATE SET
                   dividend_per_share = excluded.dividend_per_share,
                   source_name = excluded.source_name,
                   common_code = excluded.common_code,
                   sheet_year = excluded.sheet_year,
                   fetched_at = excluded.fetched_at""",
            (
                code,
                r.get("dividend_per_share"),
                r.get("source_name"),
                r.get("common_code"),
                r.get("sheet_year"),
                now,
            ),
        )
        written += 1
    await db.commit()
    return written


async def get_preferred_dividends_count() -> int:
    """Used by admin dashboard — how many preferred rows we have cached."""
    db = await get_db()
    cursor = await db.execute("SELECT COUNT(*) AS n FROM preferred_dividends")
    row = await cursor.fetchone()
    return int(row["n"]) if row else 0


async def list_preferred_dividends() -> list[dict]:
    """Return preferred-dividend sheet cache rows for admin coverage checks."""
    db = await get_db()
    cursor = await db.execute(
        """SELECT stock_code, dividend_per_share, source_name, common_code,
                  sheet_year, fetched_at
           FROM preferred_dividends
           ORDER BY source_name COLLATE NOCASE, stock_code ASC"""
    )
    return [dict(row) for row in await cursor.fetchall()]


# --- Portfolio holdings + groups CRUD (write side) ---


async def get_portfolio_item(google_sub: str, stock_code: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code, stock_name, quantity, avg_price, COALESCE(avg_price_currency, 'KRW') AS avg_price_currency, COALESCE(currency, 'KRW') AS currency, group_name FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def update_portfolio_quantity(google_sub: str, stock_code: str, new_quantity: int):
    db = await get_db()
    await db.execute(
        "UPDATE user_portfolio SET quantity = ? WHERE google_sub = ? AND stock_code = ?",
        (new_quantity, google_sub, stock_code),
    )
    await db.commit()


async def add_portfolio_item(
    google_sub: str, stock_code: str, stock_name: str, avg_price: float, quantity: int, currency: str = "KRW",
    avg_price_currency: str = "KRW",
):
    db = await get_db()
    now = datetime.now().isoformat()
    await db.execute(
        "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, avg_price, avg_price_currency, quantity, currency, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (google_sub, stock_code, stock_name, avg_price, avg_price_currency, quantity, currency, now),
    )
    await db.commit()


_TARGET_PRICE_UNCHANGED = object()
_TARGET_DISABLED_UNCHANGED = object()
_TARGET_FORMULA_UNCHANGED = object()


async def save_portfolio_item(
    google_sub: str, stock_code: str, stock_name: str, quantity: float, avg_price: float,
    currency: str = "KRW", group_name: str | None = None, benchmark_code: str | None = None,
    created_at: str | None = None,
    *,
    avg_price_currency: str | None = None,
    target_price=_TARGET_PRICE_UNCHANGED,
    target_price_disabled=_TARGET_DISABLED_UNCHANGED,
    target_price_formula=_TARGET_FORMULA_UNCHANGED,
) -> dict:
    """target_price 인자의 의미:
      - 인자 미전달 (sentinel) → 기존 값 그대로 유지 (수량/매입가만 편집할 때)
      - None              → 자동 계산으로 되돌림 (수동 override 해제)
      - float             → 명시적 수동 고정값

    target_price_disabled:
      - 미전달             → 기존 값 보존
      - True/1             → 자동 계산도 bypass, UI 에서 '-' 로 표시
      - False/0            → 자동 계산 활성화 (기본값)
    """
    target_price_provided = target_price is not _TARGET_PRICE_UNCHANGED
    target_formula_provided = target_price_formula is not _TARGET_FORMULA_UNCHANGED
    now = datetime.now().isoformat()
    # 기존 행 read → 보존 규칙 적용 → upsert 가 한 단위다. 다른 task 의
    # 동시 편집이 read 와 write 사이에 끼어들지 않도록 transaction() 으로
    # 묶는다 (BEGIN IMMEDIATE + 공유 커넥션 직렬화).
    async with transaction() as db:
        # Re-read existing row so we can preserve created_at on simple edits
        # (quantity / avg_price updates shouldn't reset the registration
        # date). Only overwrite created_at when the caller explicitly passes
        # one — that's how the UI's 등록일자 edit gets through.
        cursor = await db.execute(
            "SELECT sort_order, group_name, benchmark_code, created_at, COALESCE(avg_price_currency, 'KRW') AS avg_price_currency, target_price, COALESCE(target_price_disabled, 0) AS target_price_disabled, target_price_formula FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
            (google_sub, stock_code),
        )
        existing = await cursor.fetchone()
        sort_order = existing["sort_order"] if existing else None
        if group_name is None:
            if existing:
                group_name = existing["group_name"]
            else:
                group_name = await _resolve_default_group_name(db, google_sub, stock_code)
        if benchmark_code is None and existing:
            benchmark_code = existing["benchmark_code"]
        if avg_price_currency is None:
            avg_price_currency = existing["avg_price_currency"] if existing else "KRW"

        # Preserve existing created_at unless overridden; for brand-new rows
        # use `now`. This ordering means an explicit `created_at=None` on an
        # edit leaves the original date untouched, which matches the edit-
        # form contract (leaving the 등록일자 field blank = "no change").
        if created_at is None:
            created_at = existing["created_at"] if existing else now
        # target_price 미전달이면 기존 값 보존, 명시 None 이면 자동계산
        # 으로 되돌림, 숫자면 수동 override 저장.
        if target_price is _TARGET_PRICE_UNCHANGED:
            target_price = existing["target_price"] if existing else None
        if target_price_formula is _TARGET_FORMULA_UNCHANGED:
            target_price_formula = existing["target_price_formula"] if existing else None
        target_price_formula = str(target_price_formula or "").strip() or None
        if target_price_disabled is _TARGET_DISABLED_UNCHANGED:
            target_price_disabled = int(existing["target_price_disabled"]) if existing else 0
        else:
            target_price_disabled = 1 if target_price_disabled else 0
        # 명시 수동 값을 입력하면 disabled 플래그는 자동 해제 (사용자가
        # 목표가를 넣었다는 건 '표시하고 싶다' 는 의사). 수식이 있을 때의
        # target_price 는 "마지막으로 계산된 fallback 값" 이므로 수식과 함께
        # 보존한다. 이렇게 해야 BPS/EPS 외부 조회가 늦거나 실패해도 표가
        # 즉시 마지막 계산값을 표시할 수 있다.
        if target_price is not None:
            target_price_disabled = 0
            if target_price_provided and not target_formula_provided:
                target_price_formula = None
        if target_price_formula:
            target_price_disabled = 0
        if target_price_disabled == 1:
            target_price = None
            target_price_formula = None
        target_price_formula_db = target_price_formula or ""

        if sort_order is None and not existing:
            cursor = await db.execute(
                "SELECT MIN(sort_order) AS mn FROM user_portfolio WHERE google_sub = ? AND sort_order IS NOT NULL",
                (google_sub,),
            )
            row = await cursor.fetchone()
            min_order = row["mn"] if row and row["mn"] is not None else 0
            sort_order = min_order - 1

        await db.execute(
            """
            INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, avg_price_currency, sort_order, currency, group_name, benchmark_code, created_at, target_price, target_price_disabled, target_price_formula, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(google_sub, stock_code) DO UPDATE SET
                stock_name = excluded.stock_name,
                quantity = excluded.quantity,
                avg_price = excluded.avg_price,
                avg_price_currency = excluded.avg_price_currency,
                currency = excluded.currency,
                group_name = excluded.group_name,
                benchmark_code = excluded.benchmark_code,
                created_at = excluded.created_at,
                target_price = excluded.target_price,
                target_price_disabled = excluded.target_price_disabled,
                target_price_formula = excluded.target_price_formula,
                updated_at = excluded.updated_at
            """,
            (google_sub, stock_code, stock_name, quantity, avg_price, avg_price_currency, sort_order, currency, group_name, benchmark_code, created_at, target_price, target_price_disabled, target_price_formula_db, now),
        )
    return {
        "stock_code": stock_code, "stock_name": stock_name,
        "quantity": quantity, "avg_price": avg_price, "avg_price_currency": avg_price_currency, "currency": currency,
        "group_name": group_name, "benchmark_code": benchmark_code,
        "created_at": created_at,
        "target_price": target_price,
        "target_price_disabled": target_price_disabled,
        "target_price_formula": target_price_formula,
    }


async def clear_portfolio(google_sub: str):
    # 태그 삭제 + 보유 종목 삭제는 한 단위 — 부분 실패로 고아 태그가
    # 남지 않도록 원자적으로.
    async with transaction() as db:
        await db.execute("DELETE FROM portfolio_tags WHERE google_sub = ?", (google_sub,))
        await db.execute("DELETE FROM user_portfolio WHERE google_sub = ?", (google_sub,))


async def replace_portfolio(google_sub: str, items: list[dict]):
    """Atomic replace: delete all + insert new in one transaction."""
    async with aiosqlite.connect(db_module.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("BEGIN IMMEDIATE")
        try:
            now = datetime.now().isoformat()
            await db.execute("DELETE FROM portfolio_tags WHERE google_sub = ?", (google_sub,))
            await db.execute("DELETE FROM user_portfolio WHERE google_sub = ?", (google_sub,))
            for i, it in enumerate(items):
                group_name = await _resolve_default_group_name(db, google_sub, it["stock_code"])
                await db.execute(
                    """INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, avg_price_currency, sort_order, currency, group_name, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (google_sub, it["stock_code"], it["stock_name"], it["quantity"], it["avg_price"], it.get("avg_price_currency", "KRW"), i, it.get("currency", "KRW"), group_name, now, now),
                )
        except Exception:
            await db.rollback()
            raise
        await db.commit()


async def delete_portfolio_item(google_sub: str, stock_code: str) -> bool:
    # 태그 + 종목 행 삭제를 원자적으로 (부분 실패 시 고아 태그 방지).
    async with transaction() as db:
        await db.execute(
            "DELETE FROM portfolio_tags WHERE google_sub = ? AND stock_code = ?",
            (google_sub, stock_code),
        )
        cursor = await db.execute(
            "DELETE FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
            (google_sub, stock_code),
        )
    return cursor.rowcount > 0


async def update_portfolio_benchmark(google_sub: str, stock_code: str, benchmark_code: str | None):
    db = await get_db()
    cursor = await db.execute(
        "UPDATE user_portfolio SET benchmark_code = ?, updated_at = ? WHERE google_sub = ? AND stock_code = ?",
        (benchmark_code, datetime.now().isoformat(), google_sub, stock_code),
    )
    await db.commit()
    return cursor.rowcount > 0


async def save_portfolio_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
    await db.executemany(
        "UPDATE user_portfolio SET sort_order = ?, updated_at = ? WHERE google_sub = ? AND stock_code = ?",
        [
            (index, datetime.now().isoformat(), google_sub, code)
            for index, code in enumerate(ordered_stock_codes)
        ],
    )
    await db.commit()


async def get_portfolio_groups(google_sub: str) -> list[dict]:
    db = await get_db()
    await _ensure_default_groups(db, google_sub)
    await db.commit()
    cursor = await db.execute(
        "SELECT group_name, sort_order, is_default FROM portfolio_groups WHERE google_sub = ? ORDER BY sort_order ASC",
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def add_portfolio_group(google_sub: str, group_name: str) -> dict:
    db = await get_db()
    cursor = await db.execute(
        "SELECT MAX(sort_order) AS mx FROM portfolio_groups WHERE google_sub = ?",
        (google_sub,),
    )
    row = await cursor.fetchone()
    next_order = (row["mx"] or 0) + 1
    await db.execute(
        "INSERT INTO portfolio_groups (google_sub, group_name, sort_order, is_default) VALUES (?, ?, ?, 0)",
        (google_sub, group_name, next_order),
    )
    await db.commit()
    return {"group_name": group_name, "sort_order": next_order, "is_default": 0}


async def rename_portfolio_group(google_sub: str, old_name: str, new_name: str):
    # 그룹 테이블과 보유 종목의 group_name 을 함께 바꿔야 일관 — 원자적으로.
    async with transaction() as db:
        await db.execute(
            "UPDATE portfolio_groups SET group_name = ? WHERE google_sub = ? AND group_name = ?",
            (new_name, google_sub, old_name),
        )
        await db.execute(
            "UPDATE user_portfolio SET group_name = ? WHERE google_sub = ? AND group_name = ?",
            (new_name, google_sub, old_name),
        )


async def delete_portfolio_group(google_sub: str, group_name: str):
    # 소속 종목의 기본 그룹 이관 + 그룹 행 삭제가 한 단위 — 중간 실패 시
    # 존재하지 않는 그룹을 가리키는 종목이 남지 않도록 원자적으로.
    async with transaction() as db:
        cursor = await db.execute(
            "SELECT stock_code FROM user_portfolio WHERE google_sub = ? AND group_name = ?",
            (google_sub, group_name),
        )
        items = await cursor.fetchall()
        for item in items:
            default_grp = await _resolve_default_group_name(db, google_sub, item["stock_code"])
            await db.execute(
                "UPDATE user_portfolio SET group_name = ? WHERE google_sub = ? AND stock_code = ?",
                (default_grp, google_sub, item["stock_code"]),
            )
        await db.execute(
            "DELETE FROM portfolio_groups WHERE google_sub = ? AND group_name = ?",
            (google_sub, group_name),
        )


async def save_portfolio_groups_order(google_sub: str, group_names: list[str]):
    db = await get_db()
    await db.executemany(
        "UPDATE portfolio_groups SET sort_order = ? WHERE google_sub = ? AND group_name = ?",
        [(i, google_sub, name) for i, name in enumerate(group_names)],
    )
    await db.commit()
