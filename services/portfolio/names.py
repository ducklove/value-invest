"""Name/ticker resolution for portfolio codes.

Resolves what the UI should display for a raw user-entered code: cash and
special assets map to fixed labels, static foreign tickers short-circuit
discovery, Korean codes use the corp table (with Naver fallback inside
``foreign.resolve_name``), domestic search aliases (e.g. "KCC" → 002380) are
preferred over foreign lookups, and everything else goes through the
Reuters/Naver world-stock pipeline.

Moved from routes/portfolio.py (resolve_name handler); the route keeps a thin
HTTP delegator. ``foreign`` is called through the module object so tests can
patch ``services.portfolio.foreign.<fn>``.
"""

from __future__ import annotations

from services.portfolio import foreign
from services.portfolio.identifiers import (
    CASH_NAMES,
    SPECIAL_ASSETS,
    is_cash_asset,
    is_korean_stock,
    normalize_portfolio_code,
    static_foreign_ticker,
)


async def resolve_portfolio_name(code: str) -> dict:
    """Resolve ``code`` to its canonical stock_code / display name payload."""
    code = normalize_portfolio_code(code)
    if is_cash_asset(code):
        return {"stock_code": code, "stock_name": CASH_NAMES.get(code, code)}
    if code in SPECIAL_ASSETS:
        return {"stock_code": code, "stock_name": foreign._SPECIAL_ASSET_NAMES.get(code, code)}
    static = static_foreign_ticker(code)
    if static:
        return {
            "stock_code": static["ticker"],
            "stock_name": static["name"],
            "reuters_code": static["ticker"],
        }
    if is_korean_stock(code):
        name = await foreign.resolve_name(code)
        return {"stock_code": code, "stock_name": name}
    domestic_match = await foreign.resolve_domestic_code_alias(code)
    if domestic_match:
        return {
            "stock_code": domestic_match["stock_code"],
            "stock_name": domestic_match["corp_name"],
        }
    # Foreign: find reuters code
    reuters = await foreign.resolve_foreign_reuters(code)
    if reuters:
        d = await foreign.fetch_naver_world_stock(reuters)
        name = d.get("stockName") or d.get("stockNameEng") if d else None
        return {"stock_code": reuters, "stock_name": name, "reuters_code": reuters}
    return {"stock_code": code, "stock_name": None}
