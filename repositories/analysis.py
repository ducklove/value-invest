"""Analysis meta + snapshot cache (per stock).

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime
import json

from cache_layer import CACHE_NS_LATEST_REPORT, CACHE_NS_REPORT_LIST
from repositories.db import get_db


async def save_analysis_meta(stock_code: str, corp_name: str):
    db = await get_db()
    cursor = await db.execute(
        "SELECT payload_json FROM analysis_meta WHERE stock_code = ?",
        (stock_code,),
    )
    row = await cursor.fetchone()
    await db.execute(
        "INSERT OR REPLACE INTO analysis_meta (stock_code, corp_name, analyzed_at, payload_json) VALUES (?, ?, ?, ?)",
        (
            stock_code,
            corp_name,
            datetime.now().isoformat(),
            row["payload_json"] if row else None,
        ),
    )
    await db.commit()


async def save_analysis_snapshot(stock_code: str, corp_name: str, payload: dict):
    db = await get_db()
    analyzed_at = payload.get("analyzed_at") or datetime.now().isoformat()
    snapshot = dict(payload)
    snapshot["analyzed_at"] = analyzed_at
    await db.execute(
        "INSERT OR REPLACE INTO analysis_meta (stock_code, corp_name, analyzed_at, payload_json) VALUES (?, ?, ?, ?)",
        (
            stock_code,
            corp_name,
            analyzed_at,
            json.dumps(snapshot, ensure_ascii=False),
        ),
    )
    await db.commit()


async def get_analysis_meta(stock_code: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM analysis_meta WHERE stock_code = ?", (stock_code,)
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_analysis_snapshot(stock_code: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code, corp_name, analyzed_at, payload_json FROM analysis_meta WHERE stock_code = ?",
        (stock_code,),
    )
    row = await cursor.fetchone()
    if not row or not row["payload_json"]:
        return None
    try:
        payload = json.loads(row["payload_json"])
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None

    payload.setdefault("stock_code", row["stock_code"])
    payload.setdefault("corp_name", row["corp_name"])
    payload.setdefault("analyzed_at", row["analyzed_at"])
    payload["cached"] = True
    return payload


async def delete_analysis(stock_code: str):
    db = await get_db()
    await db.execute("DELETE FROM financial_data WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM market_data WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM analysis_meta WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM latest_report_cache WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM report_list_cache WHERE stock_code = ?", (stock_code,))
    await db.execute(
        "DELETE FROM cache_values WHERE namespace IN (?, ?) AND key = ?",
        (CACHE_NS_LATEST_REPORT, CACHE_NS_REPORT_LIST, stock_code),
    )
    await db.execute("DELETE FROM dart_report_reviews WHERE stock_code = ?", (stock_code,))
    await db.commit()
