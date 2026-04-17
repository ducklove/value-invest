"""Per-stock wiki endpoints.

- GET /api/analysis/{stock_code}/wiki — public, lists summarized reports.
- POST /api/analysis/{stock_code}/ask — auth-required, RAG-style Q&A over
  the stock's wiki (implemented in Phase 3; declared in the same router
  so the mount point stays stable).
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Query

import cache

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/analysis/{stock_code}/wiki")
async def get_stock_wiki(stock_code: str, limit: int = Query(20, ge=1, le=100)):
    """Return wiki entries for a stock, most-recent first.

    Public: wiki is built from public broker reports, and exposing it is
    a read-only aggregation of data the user could fetch directly. The
    Q&A endpoint is the one that costs LLM tokens and requires auth."""
    entries = await cache.get_wiki_entries(stock_code, limit=limit)
    return {
        "stock_code": stock_code,
        "count": len(entries),
        "entries": entries,
    }
