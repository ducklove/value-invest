"""Wiki / research-report pipeline repository.

PDF cache, stock wiki entries (+ FTS search), Q&A history, pipeline stats and
target-stock selection. Extracted verbatim from cache.py; cache.py re-exports
these as ``cache.<fn>`` so wiki_ingestion and the wiki routes are unchanged.
"""

from __future__ import annotations

from repositories.db import get_db


async def get_pdf_cache_by_sha1(pdf_sha1: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM report_pdf_cache WHERE pdf_sha1 = ?", (pdf_sha1,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def save_pdf_cache_row(row: dict) -> None:
    """Upsert a row into report_pdf_cache. `row` must contain pdf_sha1."""
    db = await get_db()
    cols = [
        "pdf_sha1", "stock_code", "pdf_url", "file_path", "file_bytes",
        "parsed_text", "parse_status", "parse_error", "downloaded_at", "parsed_at",
    ]
    vals = [row.get(c) for c in cols]
    placeholders = ",".join("?" for _ in cols)
    set_clause = ",".join(f"{c}=excluded.{c}" for c in cols if c != "pdf_sha1")
    await db.execute(
        f"INSERT INTO report_pdf_cache ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(pdf_sha1) DO UPDATE SET {set_clause}",
        vals,
    )
    await db.commit()


async def pdf_is_already_summarized(stock_code: str, pdf_sha1: str) -> bool:
    """Returns True if a wiki entry for this PDF already exists."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT 1 FROM stock_wiki_entries WHERE stock_code = ? AND source_type = 'broker_report' AND source_ref = ? LIMIT 1",
        (stock_code, pdf_sha1),
    )
    return (await cursor.fetchone()) is not None


async def save_wiki_entry(entry: dict) -> int:
    """Insert a wiki entry. Returns the new row id. Enforces UNIQUE via
    ON CONFLICT — duplicate (stock, source_type, source_ref) replaces
    the prior summary so re-summarizing with a better model overwrites."""
    db = await get_db()
    cols = [
        "stock_code", "source_type", "source_ref", "report_date", "firm",
        "title", "recommendation", "target_price", "summary_md", "key_points_md",
        "model", "tokens_in", "tokens_out", "created_at",
    ]
    vals = [entry.get(c) for c in cols]
    placeholders = ",".join("?" for _ in cols)
    set_clause = ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("stock_code", "source_type", "source_ref"))
    cursor = await db.execute(
        f"INSERT INTO stock_wiki_entries ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(stock_code, source_type, source_ref) DO UPDATE SET {set_clause}",
        vals,
    )
    await db.commit()
    return cursor.lastrowid


async def get_wiki_entries(stock_code: str, limit: int = 20) -> list[dict]:
    db = await get_db()
    # LEFT JOIN report_pdf_cache so each entry carries the PDF URL it was
    # summarized from. Frontend uses this to attach summaries to matching
    # rows in the broker-report table (and keep the Q&A Retrieval layer
    # unaffected — only an extra string per row).
    cursor = await db.execute(
        """SELECT w.id, w.stock_code, w.source_type, w.source_ref, w.report_date,
                  w.firm, w.title, w.recommendation, w.target_price,
                  w.summary_md, w.key_points_md, w.model, w.created_at,
                  p.pdf_url AS pdf_url
           FROM stock_wiki_entries w
           LEFT JOIN report_pdf_cache p ON p.pdf_sha1 = w.source_ref
           WHERE w.stock_code = ?
           ORDER BY COALESCE(w.report_date, w.created_at) DESC
           LIMIT ?""",
        (stock_code, limit),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def search_wiki(stock_code: str, query: str, limit: int = 5) -> list[dict]:
    """FTS search scoped to one stock. Falls back to recency if FTS returns
    fewer than `limit` matches (e.g. question is too short for meaningful
    tokens, or FTS index is empty). Returned rows are the same shape as
    get_wiki_entries()."""
    db = await get_db()
    # FTS MATCH expects a sanitized query — strip characters FTS treats as
    # operators to avoid "fts5: syntax error near ..." on user input.
    sanitized = _sanitize_fts_query(query)
    rows: list[dict] = []
    if sanitized:
        cursor = await db.execute(
            """SELECT e.id, e.stock_code, e.source_type, e.source_ref, e.report_date,
                      e.firm, e.title, e.recommendation, e.target_price,
                      e.summary_md, e.key_points_md, e.model, e.created_at
               FROM stock_wiki_fts f
               JOIN stock_wiki_entries e ON e.id = f.rowid
               WHERE f.stock_wiki_fts MATCH ? AND e.stock_code = ?
               ORDER BY bm25(stock_wiki_fts) ASC
               LIMIT ?""",
            (sanitized, stock_code, limit),
        )
        rows = [dict(r) for r in await cursor.fetchall()]
    if len(rows) < limit:
        seen = {r["id"] for r in rows}
        extra = await get_wiki_entries(stock_code, limit * 2)
        for e in extra:
            if e["id"] in seen:
                continue
            rows.append(e)
            if len(rows) >= limit:
                break
    return rows[:limit]


def _sanitize_fts_query(q: str) -> str:
    """Strip FTS5 special chars so arbitrary user input doesn't raise.
    Kept simple: keep CJK, ASCII letters/digits, spaces; drop everything
    else. Collapse whitespace."""
    import re
    cleaned = re.sub(r"[^\w\s가-힯一-鿿]+", " ", q)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


async def qa_count_since(google_sub: str, since_iso: str) -> int:
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM stock_qa_history WHERE google_sub = ? AND created_at >= ?",
        (google_sub, since_iso),
    )
    row = await cursor.fetchone()
    return int(row["n"]) if row else 0


async def save_qa_entry(entry: dict) -> int:
    db = await get_db()
    cols = [
        "google_sub", "stock_code", "question", "answer_md", "source_ids",
        "model", "tokens_in", "tokens_out", "cost_usd", "created_at",
    ]
    vals = [entry.get(c) for c in cols]
    placeholders = ",".join("?" for _ in cols)
    cursor = await db.execute(
        f"INSERT INTO stock_qa_history ({','.join(cols)}) VALUES ({placeholders})",
        vals,
    )
    await db.commit()
    return cursor.lastrowid


async def get_wiki_stats() -> dict:
    """Aggregate counts for the wiki pipeline — cheap queries, safe to
    call on every page load. Returns {stocks_covered, total_entries,
    pdfs_cached, latest_entry_date}."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(DISTINCT stock_code) AS stocks, COUNT(*) AS entries, MAX(COALESCE(report_date, created_at)) AS latest FROM stock_wiki_entries",
    )
    row = await cursor.fetchone()
    cursor2 = await db.execute(
        "SELECT COUNT(*) AS n FROM report_pdf_cache WHERE parse_status = 'parsed'",
    )
    pdf_row = await cursor2.fetchone()
    return {
        "stocks_covered": int(row["stocks"] or 0) if row else 0,
        "total_entries": int(row["entries"] or 0) if row else 0,
        "pdfs_cached": int(pdf_row["n"] or 0) if pdf_row else 0,
        "latest_entry_date": (row["latest"] if row else None) or None,
    }


async def select_wiki_target_stocks(recent_days: int = 30) -> list[str]:
    """Pick which stocks the wiki ingestion pipeline should process.

    Union of four signals:
      (a) currently held in any user portfolio
      (b) starred by any user
      (c) per-user recent analysis (user_recent_analyses) within N days
      (d) any stock whose analysis snapshot (analysis_meta) was touched
          within N days

    (d) was added after a concrete miss: LG화학 had an analysis snapshot
    but no wiki entries, because user_recent_analyses bookkeeping had
    gone missing for that stock (the per-user path depends on a logged-in
    session being correctly threaded through /api/analyze — brittle).
    analysis_meta.analyzed_at is updated on every successful analysis run
    regardless of session state, so folding it in makes "user searched
    for this stock" the robust trigger the pipeline needed.
    """
    db = await get_db()
    cursor = await db.execute(
        f"""SELECT DISTINCT stock_code FROM (
            SELECT stock_code FROM user_portfolio
            UNION
            SELECT stock_code FROM user_stock_preferences WHERE is_starred = 1
            UNION
            SELECT stock_code FROM user_recent_analyses
              WHERE viewed_at >= datetime('now', '-{int(recent_days)} days')
            UNION
            SELECT stock_code FROM analysis_meta
              WHERE analyzed_at >= datetime('now', '-{int(recent_days)} days')
        )
        WHERE stock_code IS NOT NULL""",
    )
    return [r["stock_code"] for r in await cursor.fetchall()]
