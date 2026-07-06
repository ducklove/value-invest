"""DART corp_codes 참조 테이블 repository + 인메모리 조회 캐시.

cache.py 해체(Phase 2)로 이관. 상장사 코드표는 best-effort 시세/이름
캐시가 아니라 참조 데이터다 — 시작 시 DART 에서 갱신하고(core/lifespan),
벤치마크 라벨 렌더링 같은 빠른 UI 경로를 위해 전체 테이블을 메모리에
올려 둔다. 메모리 테이블은 커넥션 종료(repositories.bootstrap.close_db)
때 함께 리셋된다.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db, transaction

_corp_code_table: dict[str, dict[str, str]] | None = None

_CORP_SEARCH_ALIASES = {
    # DART stores the KCC parent company as the Korean legal name, so a plain
    # "KCC" search otherwise only finds KCC건설.
    "KCC": ["002380"],
}


def reset_memory_table() -> None:
    """인메모리 코드표 리셋 — DB 교체(테스트)나 셧다운 시 호출."""
    global _corp_code_table
    _corp_code_table = None


async def is_corp_codes_loaded() -> bool:
    db = await get_db()
    cursor = await db.execute("SELECT COUNT(*) FROM corp_codes")
    row = await cursor.fetchone()
    return row[0] > 0


async def corp_codes_need_refresh() -> bool:
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS total, SUM(CASE WHEN modify_date IS NOT NULL AND modify_date != '' THEN 1 ELSE 0 END) AS filled FROM corp_codes"
    )
    row = await cursor.fetchone()
    total = row["total"] or 0
    filled = row["filled"] or 0
    return total > 0 and filled == 0


async def save_corp_codes(codes: list[dict]):
    global _corp_code_table
    now = datetime.now().isoformat()
    async with transaction() as db:
        await db.executemany(
            "INSERT OR REPLACE INTO corp_codes (stock_code, corp_code, corp_name, modify_date, updated_at) VALUES (?, ?, ?, ?, ?)",
            [(c["stock_code"], c["corp_code"], c["corp_name"], c.get("modify_date"), now) for c in codes],
        )
    _corp_code_table = {
        str(c["stock_code"]): {
            "stock_code": str(c["stock_code"]),
            "corp_code": str(c.get("corp_code") or ""),
            "corp_name": str(c.get("corp_name") or ""),
        }
        for c in codes
        if c.get("stock_code") and c.get("corp_name")
    }


def _corp_search_alias_codes(query: str) -> list[str]:
    return _CORP_SEARCH_ALIASES.get((query or "").strip().upper(), [])


async def search_corp(query: str) -> list[dict]:
    query = (query or "").strip()
    db = await get_db()
    alias_rows = []
    for stock_code in _corp_search_alias_codes(query):
        cursor = await db.execute(
            "SELECT stock_code, corp_code, corp_name FROM corp_codes WHERE stock_code = ?",
            (stock_code,),
        )
        row = await cursor.fetchone()
        if row:
            alias_rows.append(dict(row))

    cursor = await db.execute(
        "SELECT stock_code, corp_code, corp_name FROM corp_codes "
        "WHERE corp_name LIKE ? OR stock_code LIKE ? "
        "ORDER BY "
        "CASE "
        "WHEN stock_code = ? THEN 0 "
        "WHEN corp_name = ? THEN 1 "
        "WHEN corp_name LIKE ? THEN 2 "
        "ELSE 3 END, "
        "COALESCE(modify_date, '') DESC, "
        "LENGTH(corp_name), stock_code "
        "LIMIT 20",
        (f"%{query}%", f"%{query}%", query, query, f"{query}%"),
    )
    rows = await cursor.fetchall()
    results = []
    seen_codes = set()
    exact_name_seen = set()
    for item in [*alias_rows, *(dict(row) for row in rows)]:
        if item["stock_code"] in seen_codes:
            continue
        seen_codes.add(item["stock_code"])
        if item["corp_name"] == query:
            if item["corp_name"] in exact_name_seen:
                continue
            exact_name_seen.add(item["corp_name"])
        results.append(item)
    return results


async def resolve_corp_search_query(query: str) -> dict | None:
    query = (query or "").strip()
    if not query:
        return None
    rows = await search_corp(query)
    if not rows:
        return None
    first = rows[0]
    normalized = query.upper()
    if (
        _corp_search_alias_codes(query)
        or first["stock_code"] == query
        or first["corp_name"] == query
        or first["corp_name"].upper() == normalized
    ):
        return first
    return None


async def get_corp_code(stock_code: str) -> str | None:
    row = (await load_corp_code_table()).get(str(stock_code or "").strip())
    return row["corp_code"] if row else None


async def get_corp_name(stock_code: str) -> str | None:
    row = (await load_corp_code_table()).get(str(stock_code or "").strip())
    return row["corp_name"] if row else None


async def resolve_stock_name(stock_code: str) -> str | None:
    name = await get_corp_name(stock_code)
    if name:
        return name
    return None


async def load_corp_code_table(*, force: bool = False) -> dict[str, dict[str, str]]:
    """Return the full internal listed-company code table.

    This is a reference table, not a best-effort quote/name cache. It is
    refreshed from DART at startup and then kept in memory for fast UI paths
    such as benchmark label rendering.
    """
    global _corp_code_table
    if _corp_code_table is not None and not force:
        return _corp_code_table
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code, corp_code, corp_name FROM corp_codes WHERE stock_code IS NOT NULL AND stock_code != ''"
    )
    _corp_code_table = {
        row["stock_code"]: {
            "stock_code": row["stock_code"],
            "corp_code": row["corp_code"],
            "corp_name": row["corp_name"],
        }
        for row in await cursor.fetchall()
    }
    return _corp_code_table
