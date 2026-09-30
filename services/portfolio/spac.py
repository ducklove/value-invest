"""SPAC Hunter inputs for the portfolio popup.

Current liquidation value follows spac-hunter/assets/valuation.js:
net simple interest within each contract, capitalized at rate changes/rollovers.
The future payout value is kept separate and comes directly from SPAC Hunter.

When SPAC Hunter publishes its own pipeline-computed value for the same day
(optional ``spacs[].currentLiquidationValue`` + ``currentLiquidationValueAsOf`` in
summary.json / data.json), that number wins; the Python port below is the fallback
for other days and for payloads that do not carry it yet.
"""

from __future__ import annotations

import calendar
import math
from datetime import date

import external_tools
from asset_insights import safe_float
from services.portfolio.identifiers import is_korean_stock
from services.portfolio.time_windows import today_kst_date


def is_spac(stock_code: str, name: str) -> bool:
    return is_korean_stock(stock_code) and ("스팩" in name or "SPAC" in name.upper())


def _date(value) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _add_months(day: date, months: int) -> date:
    year, month = divmod(day.year * 12 + day.month - 1 + months, 12)
    month += 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def current_liquidation_value(item: dict, as_of: date, assumptions: dict) -> float | None:
    """Accrued value as of a date, without pulling future rates/balances forward."""
    ipo = safe_float(item.get("ipoPrice"))
    basis = item.get("valuationBasis") or {}
    periods = sorted(
        (day, rate)
        for row in (item.get("escrowRatePeriods") or [])
        if isinstance(row, dict)
        and (day := _date(row.get("startDate"))) is not None
        and (rate := safe_float(row.get("ratePct"))) is not None and rate >= 0
    )
    start = _date(basis.get("trustStartDate") or (periods[0][0] if periods else item.get("listingDate")))
    if ipo is None or ipo <= 0 or start is None or as_of < start:
        return None
    if as_of == start:
        return ipo
    value = ipo
    anchor = basis.get("anchor") or {}
    anchor_date = _date(anchor.get("date"))
    anchor_value = safe_float(anchor.get("valuePerShare"))
    if anchor_date and start <= anchor_date <= as_of and anchor_value is not None and anchor_value > 0:
        start, value = anchor_date, anchor_value
    periods = [(day, rate) for day, rate in periods if day <= as_of]
    if not any(day <= start for day, _ in periods):
        return None

    def setting(key, default):
        value = basis.get(key)
        if value is None:
            value = assumptions.get(key)
        return safe_float(default if value is None else value)

    fee = setting("trustFeePct", 0.1)
    tax = setting("interestTaxPct", 15.4)
    rollover = safe_float(basis.get("rolloverMonths", 12))
    if (fee is None or fee < 0 or tax is None or not 0 <= tax <= 100
            or rollover is None or rollover < 1 or not rollover.is_integer()):
        return None
    boundaries = [start, *(day for day, _ in periods if start < day < as_of), as_of]
    for segment_start, segment_end in zip(boundaries, boundaries[1:]):
        rate = next(rate for day, rate in reversed(periods) if day <= segment_start)
        net_rate = max(0, (rate - fee) / 100) * (1 - tax / 100)
        term_start = segment_start
        while term_start < segment_end:
            term_end = min(_add_months(term_start, int(rollover)), segment_end)
            value *= 1 + net_rate * (term_end - term_start).days / 365
            term_start = term_end
    return value if math.isfinite(value) and value > 0 else None


def pipeline_liquidation_value(item: dict, as_of: date) -> float | None:
    """SPAC Hunter 파이프라인이 계산한 ``as_of`` 당일 청산가치(없거나 다른 날짜면 None)."""
    value = safe_float(item.get("currentLiquidationValue"))
    if value is None or not math.isfinite(value) or value <= 0:
        return None
    if _date(item.get("currentLiquidationValueAsOf")) != as_of:
        return None
    return value


async def fetch_spac_context(stock_code: str, name: str) -> dict | None:
    if not is_spac(stock_code, name):
        return None
    data = await external_tools.fetch_spac_data()
    item = next((row for row in data.get("spacs", [])
                 if isinstance(row, dict) and row.get("code") == stock_code.upper()), None)
    return {
        "applicable": True,
        "item": item or {},
        "assumptions": data.get("valuationAssumptions") or {},
        "updatedAt": data.get("lastUpdated"),
    }


def build_spac_insight(context: dict | None, quote: dict | None, *, as_of: date | None = None) -> dict | None:
    if context is None:
        return None
    as_of = as_of or today_kst_date()
    item = context["item"]
    price = safe_float((quote or {}).get("price"))
    if price is None or price <= 0:
        price = safe_float(item.get("currentPrice"))
    value = pipeline_liquidation_value(item, as_of)
    if value is None:
        value = current_liquidation_value(item, as_of, context["assumptions"])
    target = safe_float(item.get("liquidationValuePerShare"))
    payout = _date(item.get("payoutDate") or item.get("liquidationDate"))
    annualized = None
    if target is not None and target > 0 and price is not None and price > 0 and payout and payout > as_of:
        try:
            annualized = safe_float(((target / price) ** (365 / (payout - as_of).days) - 1) * 100)
        except OverflowError:
            pass
    discount = (value - price) / value * 100 if value and price is not None and price > 0 else None
    return {
        "applicable": True,
        "source": "SPAC Hunter",
        "updatedAt": context.get("updatedAt"),
        "asOf": as_of.isoformat(),
        "price": price,
        "currentLiquidationValue": round(value, 2) if value is not None else None,
        "liquidationDiscountPct": round(discount, 2) if discount is not None else None,
        "annualizedReturnPct": round(annualized, 2) if annualized is not None else None,
        "listingDate": item.get("listingDate"),
        "payoutDate": payout.isoformat() if payout else None,
    }
