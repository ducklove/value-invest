"""Portfolio holdings read-side repository.

Stock-name resolution, the holdings list (get_portfolio), per-stock tags +
suggestions, target-metric / market-valuation rows, and preferred/trailing
dividends. Extracted verbatim from cache.py; cache.py re-exports these as
``cache.<fn>`` so routes/services are unchanged. Cross-table lookups
(get_corp_name) go through the cache facade.
"""

from __future__ import annotations

from datetime import datetime

import cache


async def resolve_stock_name(stock_code: str) -> str | None:
    name = await cache.get_corp_name(stock_code)
    if name:
        return name
    return None


async def get_portfolio(google_sub: str) -> list[dict]:
    db = await cache.get_db()
    # created_at is surfaced so the UI can show '등록일자' and let the
    # user edit it. It was already stored on every insert but wasn't in
    # the SELECT list — the column existed server-side but was invisible.
    cursor = await db.execute(
        """
        SELECT stock_code, stock_name, quantity, avg_price, sort_order,
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
    db = await cache.get_db()
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
    db = await cache.get_db()
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
    db = await cache.get_db()
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

    db = await cache.get_db()
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
    db = await cache.get_db()
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
    db = await cache.get_db()
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
    db = await cache.get_db()
    now = datetime.now().isoformat()
    await db.execute(
        "DELETE FROM portfolio_tags WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
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
    await db.commit()
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
    db = await cache.get_db()
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
    db = await cache.get_db()
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
    db = await cache.get_db()
    cursor = await db.execute("SELECT COUNT(*) AS n FROM preferred_dividends")
    row = await cursor.fetchone()
    return int(row["n"]) if row else 0


async def list_preferred_dividends() -> list[dict]:
    """Return preferred-dividend sheet cache rows for admin coverage checks."""
    db = await cache.get_db()
    cursor = await db.execute(
        """SELECT stock_code, dividend_per_share, source_name, common_code,
                  sheet_year, fetched_at
           FROM preferred_dividends
           ORDER BY source_name COLLATE NOCASE, stock_code ASC"""
    )
    return [dict(row) for row in await cursor.fetchall()]
