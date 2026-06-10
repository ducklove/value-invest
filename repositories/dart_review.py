"""DART periodic-filing AI review repository.

Extracted verbatim from cache.py. cache.py re-exports these as ``cache.<fn>`` so
dart_report_review and routes/dart_review are unchanged.
"""

from __future__ import annotations

import json
from datetime import datetime

from repositories.db import get_db


async def get_dart_report_review(stock_code: str, rcept_no: str | None = None) -> dict | None:
    db = await get_db()
    if rcept_no:
        cursor = await db.execute(
            "SELECT * FROM dart_report_reviews WHERE stock_code = ? AND rcept_no = ?",
            (stock_code, rcept_no),
        )
    else:
        cursor = await db.execute(
            """
            SELECT * FROM dart_report_reviews
            WHERE stock_code = ?
            ORDER BY COALESCE(report_date, '') DESC, updated_at DESC, id DESC
            LIMIT 1
            """,
            (stock_code,),
        )
    row = await cursor.fetchone()
    if not row:
        return None
    out = dict(row)
    out["comparison_reports"] = json.loads(out.pop("comparison_reports_json") or "[]")
    out["review"] = json.loads(out.pop("review_json") or "{}")
    return out


async def save_dart_report_review(review: dict) -> dict:
    db = await get_db()
    now = datetime.now().isoformat()
    review_payload = review.get("review") or {}
    comparison_reports = review.get("comparison_reports") or []
    await db.execute(
        """
        INSERT INTO dart_report_reviews
            (stock_code, corp_code, corp_name, rcept_no, report_name, report_date,
             primary_doc_chars, comparison_reports_json, review_json, review_md,
             model, tokens_in, tokens_out, cost_usd, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, rcept_no) DO UPDATE SET
            corp_code = excluded.corp_code,
            corp_name = excluded.corp_name,
            report_name = excluded.report_name,
            report_date = excluded.report_date,
            primary_doc_chars = excluded.primary_doc_chars,
            comparison_reports_json = excluded.comparison_reports_json,
            review_json = excluded.review_json,
            review_md = excluded.review_md,
            model = excluded.model,
            tokens_in = excluded.tokens_in,
            tokens_out = excluded.tokens_out,
            cost_usd = excluded.cost_usd,
            updated_at = excluded.updated_at
        """,
        (
            review.get("stock_code"),
            review.get("corp_code"),
            review.get("corp_name"),
            review.get("rcept_no"),
            review.get("report_name"),
            review.get("report_date"),
            int(review.get("primary_doc_chars") or 0),
            json.dumps(comparison_reports, ensure_ascii=False),
            json.dumps(review_payload, ensure_ascii=False),
            review.get("review_md") or review_payload.get("summary_md") or "",
            review.get("model"),
            int(review.get("tokens_in") or 0),
            int(review.get("tokens_out") or 0),
            float(review.get("cost_usd") or 0),
            review.get("created_at") or now,
            now,
        ),
    )
    await db.commit()
    return await get_dart_report_review(review.get("stock_code"), review.get("rcept_no")) or review
