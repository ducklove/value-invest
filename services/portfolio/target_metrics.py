from __future__ import annotations

import logging
import re

from services.portfolio.identifiers import common_stock_code, is_korean_stock, is_preferred_stock
from services.portfolio.time_windows import today_kst_date
from services.portfolio.valuation import fetch_valuation_basis_map

logger = logging.getLogger(__name__)

_TARGET_FINANCIAL_VARS_RE = re.compile(r"\b(BPS|EPS)\b")


def _formula_financial_vars(formula: str | None) -> set[str]:
    return set(_TARGET_FINANCIAL_VARS_RE.findall(str(formula or "")))


async def supplement_target_metrics(items: list[dict], target_metrics_map: dict[str, dict]) -> None:
    """Fill formula variables from the same valuation source used by insights.

    The portfolio table should not maintain a separate BPS/EPS cache for target
    prices. When a target formula references BPS or EPS, refresh those values
    through the shared valuation basis helper even if market_data already has
    a cached value. This keeps formula targets aligned with the investment
    insight modal after finance-pi corrections.
    """

    needed_vars: dict[str, set[str]] = {}
    for item in items:
        formula_vars = _formula_financial_vars(item.get("target_price_formula"))
        if not formula_vars:
            continue
        code = item.get("stock_code")
        candidates = [code]
        if code and is_korean_stock(code) and is_preferred_stock(code):
            candidates.append(common_stock_code(code))
        for candidate in candidates:
            if not candidate or not is_korean_stock(candidate):
                continue
            current = target_metrics_map.setdefault(candidate, {"eps": None, "bps": None, "dps": None})
            needed_vars.setdefault(candidate, set()).update(formula_vars)

    codes = list(needed_vars)
    if not codes:
        return

    today = today_kst_date()

    try:
        basis_map = await fetch_valuation_basis_map(codes, as_of=today)
    except Exception as exc:
        logger.warning("target metric valuation basis batch failed (%s): %s", ",".join(codes[:5]), exc)
        return

    for code, basis in basis_map.items():
        if not basis.get("applicable"):
            continue

        formula_vars = needed_vars.get(code, set())
        current = target_metrics_map.setdefault(code, {"eps": None, "bps": None, "dps": None})
        if "BPS" in formula_vars and basis.get("bps") is not None:
            current["bps"] = basis["bps"]
        if "EPS" in formula_vars and basis.get("eps") is not None:
            current["eps"] = basis["eps"]
