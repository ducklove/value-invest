"""Evaluate a 목표가(target-price) formula with *current* data.

The alert engine needs the same target the UI shows. For formula targets the
stored ``target_price`` column is only a save-time fallback (and is empty when
the formula uses live-only variables like 보유지분/본주가격), so it can diverge
badly from the live value — e.g. a 보유지분-based 삼성생명 target showed the
auto 매입가×1.30 instead of the real ~926,044.

This resolves the formula at evaluation time using the live quote / valuation /
dividend / holding-value sources (the same ones routes/portfolio.py uses at save
time), but HTTP-agnostically: it returns None when a required variable can't be
obtained instead of raising, so callers fall back to the stored/auto target.
"""

from __future__ import annotations

import asyncio
import logging

from repositories import portfolio as portfolio_repo
import integrations
from services import stock_quotes
from services.portfolio import runtime_quotes
from services.portfolio.identifiers import common_stock_code, is_korean_stock, is_preferred_stock
from services.portfolio.targets import evaluate_target_formula, extract_target_variables
from services.portfolio.time_windows import today_kst_date
from services.portfolio.valuation import fetch_valuation_basis


logger = logging.getLogger(__name__)


class TargetFormulaError(ValueError):
    """Save-time formula resolution failure that should surface to the user."""


def _num(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


async def _quote_price(stock_code: str) -> float | None:
    try:
        quote = await runtime_quotes.fetch_quote(stock_code)
    except Exception:
        return None
    return _num((quote or {}).get("price"))


_SAVE_QUOTE_TIMEOUT = 3.5


async def _save_time_quote_price(stock_code: str) -> float | None:
    """Quote with a hard timeout and cached-quote fallback (edit/save UX)."""
    try:
        quote = await asyncio.wait_for(
            runtime_quotes.fetch_quote(stock_code), timeout=_SAVE_QUOTE_TIMEOUT
        )
    except Exception:
        quote = stock_quotes.stock_to_quote(
            stock_quotes.get_stock_cached(stock_code, allow_stale=False)
        )
    return _num((quote or {}).get("price"))


async def _holding_value_per_share(stock_code: str, *, quote_price=None) -> float | None:
    """지주/보유지분 NAV per share — snapshot value or live subsidiary quotes."""
    fetch_price = quote_price or _quote_price
    meta = (integrations.build_public_integrations().get("holdingValue") or {}).get("meta") or {}
    item = meta.get(stock_code)
    if not isinstance(item, dict):
        return None
    snap = _num(item.get("holdingValuePerShare"))
    if snap is not None and snap > 0:
        return snap

    subsidiaries = item.get("subsidiaries") or []
    if not subsidiaries:
        return None

    async def _sub_value(sub: dict) -> float | None:
        code = str(sub.get("code") or "").strip()
        shares = _num(sub.get("sharesHeld"))
        if not code or shares is None:
            return None
        price = await fetch_price(code)
        return None if price is None else price * shares

    values = await asyncio.gather(*(_sub_value(s) for s in subsidiaries))
    if any(v is None for v in values):
        return None
    total_shares = _num(item.get("totalShares"))
    treasury = _num(item.get("treasuryShares")) or 0
    free_shares = (total_shares or 0) - treasury
    sub_total = sum(float(v) for v in values if v is not None)
    return sub_total / free_shares if free_shares > 0 and sub_total > 0 else None


async def resolve_formula_target(stock_code: str, formula: str, avg_price) -> float | None:
    """Evaluate ``formula`` for ``stock_code`` with current data. None if any
    required variable is unavailable."""
    variables = extract_target_variables(formula)
    if not variables:
        return None

    values: dict[str, float | None] = {}
    if "매입가" in variables:
        values["매입가"] = _num(avg_price)
    if "DPS" in variables:
        dps_map = await portfolio_repo.get_trailing_dividends([stock_code])
        values["DPS"] = _num(dps_map.get(stock_code))
    if variables & {"BPS", "EPS"}:
        source = (
            common_stock_code(stock_code)
            if is_korean_stock(stock_code) and is_preferred_stock(stock_code)
            else stock_code
        )
        try:
            basis = await fetch_valuation_basis(source, as_of=today_kst_date())
        except Exception as exc:
            logger.info("target valuation basis failed for %s: %s", source, exc)
            basis = {}
        if "BPS" in variables:
            values["BPS"] = _num(basis.get("bps"))
        if "EPS" in variables:
            values["EPS"] = _num(basis.get("eps"))
    if "보유지분" in variables:
        values["보유지분"] = await _holding_value_per_share(stock_code)
    if "본주가격" in variables:
        common = (
            common_stock_code(stock_code)
            if is_korean_stock(stock_code) and is_preferred_stock(stock_code)
            else ""
        )
        values["본주가격"] = await _quote_price(common) if common and common != stock_code else None

    if any(values.get(name) is None for name in variables):
        return None
    try:
        result = evaluate_target_formula(formula, values)
    except Exception:
        return None
    return result if result and result > 0 else None


async def resolve_formula_target_at_save(stock_code: str, formula: str, avg_price) -> float | None:
    """Resolve a formula to a saved fallback price at edit/save time.

    Moved from routes/portfolio.py (_resolve_target_formula_price); the route
    keeps a thin delegator that maps :class:`TargetFormulaError` to HTTP 400.

    BPS/EPS use the same valuation source as the investment insight modal and
    raise :class:`TargetFormulaError` when they cannot be fetched. Dynamic
    variables such as 보유지분 and 본주가격 are still recomputed on the client
    when quotes are available, so they intentionally do not block save
    (missing → return None and let the UI re-evaluate live).
    """
    variables = extract_target_variables(formula)
    if not variables:
        return None

    values: dict[str, float | None] = {}
    if "매입가" in variables:
        values["매입가"] = _num(avg_price)

    if "DPS" in variables:
        dps_map = await portfolio_repo.get_trailing_dividends([stock_code])
        values["DPS"] = _num(dps_map.get(stock_code))

    if variables & {"BPS", "EPS"}:
        source_code = (
            common_stock_code(stock_code)
            if is_korean_stock(stock_code) and is_preferred_stock(stock_code)
            else stock_code
        )
        basis = await fetch_valuation_basis(source_code, as_of=today_kst_date())
        if "BPS" in variables:
            values["BPS"] = _num(basis.get("bps"))
        if "EPS" in variables:
            values["EPS"] = _num(basis.get("eps"))

    if "보유지분" in variables:
        values["보유지분"] = await _holding_value_per_share(
            stock_code, quote_price=_save_time_quote_price
        )

    if "본주가격" in variables:
        common_code = (
            common_stock_code(stock_code)
            if is_korean_stock(stock_code) and is_preferred_stock(stock_code)
            else ""
        )
        values["본주가격"] = (
            await _save_time_quote_price(common_code)
            if common_code and common_code != stock_code
            else None
        )

    missing_financials = [name for name in ("BPS", "EPS") if name in variables and values.get(name) is None]
    if missing_financials:
        raise TargetFormulaError(f"{', '.join(missing_financials)} 값을 가져오지 못했습니다.")

    # 보유지분/본주가격은 저장 시점 fallback 을 채우되, quote 를 못 얻는
    # 경우에는 화면의 실시간 quote 도착 후 재평가에 맡긴다.
    if any(name not in values or values.get(name) is None for name in variables):
        return None

    try:
        return evaluate_target_formula(formula, values)
    except Exception as exc:
        raise TargetFormulaError(str(exc)) from exc
