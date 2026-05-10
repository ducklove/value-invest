from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import asset_insights
import cache
import close_price_client
from services.portfolio.identifiers import common_stock_code, is_korean_stock, is_preferred_stock
from services.portfolio.time_windows import today_kst_date


logger = logging.getLogger(__name__)


def metric_amount(metrics: dict, key: str) -> float | None:
    raw = metrics.get(key) if isinstance(metrics, dict) else None
    if isinstance(raw, dict):
        raw = raw.get("amount")
    return asset_insights.safe_float(raw)


def per_share_value(fundamental: dict, key: str) -> float | None:
    per_share = fundamental.get("per_share") if isinstance(fundamental, dict) else None
    entry = per_share.get(key) if isinstance(per_share, dict) else None
    if isinstance(entry, dict):
        return asset_insights.safe_float(entry.get("value"))
    return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _share_number(mapping: dict, *keys: str) -> float | None:
    for key in keys:
        value = asset_insights.safe_float(mapping.get(key))
        if value is not None:
            return value
    return None


def treasury_share_basis(fundamental: dict) -> dict:
    per_share = fundamental.get("per_share") if isinstance(fundamental, dict) else None
    if not isinstance(per_share, dict):
        return {}

    entries = []
    for key in ("bps", "eps_ttm", "eps_annual"):
        entry = per_share.get(key)
        if not isinstance(entry, dict):
            continue
        components = entry.get("components")
        if isinstance(components, list) and components:
            entries = [component for component in components if isinstance(component, dict)]
            break
        if _share_number(entry, "issued_shares", "istc_totqy") is not None:
            entries = [entry]
            break
    if not entries:
        return {}

    issued_total = 0.0
    treasury_total = 0.0
    outstanding_total = 0.0
    for entry in entries:
        issued = _share_number(entry, "issued_shares", "istc_totqy")
        treasury = _share_number(
            entry,
            "treasury_shares",
            "treasury_share_count",
            "treasury_stock_count",
            "treas_stock_co",
            "own_stock_count",
            "self_stock_count",
        )
        outstanding = _share_number(entry, "outstanding_shares", "distributed_shares", "distb_stock_co")
        if issued is None and outstanding is not None and treasury is not None:
            issued = outstanding + treasury
        if treasury is None and issued is not None and outstanding is not None:
            treasury = max(0.0, issued - outstanding)
        if issued is not None and issued > 0:
            issued_total += issued
        if treasury is not None and treasury > 0:
            treasury_total += treasury
        if outstanding is not None and outstanding > 0:
            outstanding_total += outstanding

    if issued_total <= 0:
        return {}
    return {
        "treasuryShareRatioPct": round(treasury_total / issued_total * 100, 2),
        "treasuryShares": round(treasury_total),
        "issuedShares": round(issued_total),
        "outstandingShares": round(outstanding_total) if outstanding_total > 0 else None,
    }


def needs_market_valuation_fallback(basis: dict) -> bool:
    if basis.get("source") != "internal_fundamentals":
        return True
    return any(basis.get(key) is None for key in ("eps", "bps", "netIncome", "equity"))


async def fetch_valuation_basis(
    stock_code: str,
    *,
    as_of: date | datetime | str | None = None,
    use_market_cache_fallback: bool = True,
) -> dict:
    code = (stock_code or "").strip()
    if not is_korean_stock(code):
        return {"applicable": False}

    source_code = common_stock_code(code) if is_preferred_stock(code) else code
    basis = {
        "applicable": True,
        "sourceCode": source_code,
        "source": "market_data",
    }

    fundamental: dict = {}
    try:
        fundamentals = await close_price_client.get_basic_fundamentals(source_code, as_of=as_of or today_kst_date())
        raw = fundamentals.get(source_code) if isinstance(fundamentals, dict) else None
        if isinstance(raw, dict):
            fundamental = raw
            basis["source"] = "internal_fundamentals"
            basis["fiscalYear"] = raw.get("fiscal_year")
            basis["asOf"] = raw.get("as_of") or raw.get("available_date")
    except Exception as exc:
        logger.info("portfolio valuation fundamentals unavailable (%s): %s", source_code, exc)

    metrics = fundamental.get("metrics") if isinstance(fundamental.get("metrics"), dict) else {}
    basis.update(
        {
            "eps": _first_present(per_share_value(fundamental, "eps_ttm"), per_share_value(fundamental, "eps_annual")),
            "bps": per_share_value(fundamental, "bps"),
            "netIncome": metric_amount(metrics, "net_income"),
            "equity": metric_amount(metrics, "equity"),
        }
    )
    basis.update(treasury_share_basis(fundamental))

    if not use_market_cache_fallback or not needs_market_valuation_fallback(basis):
        return basis

    cached = await cache.get_latest_market_valuation(source_code)
    if cached:
        basis.setdefault("fiscalYear", cached.get("year"))
        if not basis.get("asOf") and cached.get("year"):
            basis["asOf"] = str(cached.get("year"))
        for target_key, cached_key in (
            ("eps", "eps"),
            ("bps", "bps"),
            ("perFallback", "per"),
            ("pbrFallback", "pbr"),
            ("netIncome", "net_income"),
            ("equity", "total_equity"),
            ("closePrice", "close_price"),
        ):
            if basis.get(target_key) is None and cached.get(cached_key) is not None:
                basis[target_key] = cached.get(cached_key)
        if basis.get("source") == "market_data":
            basis["source"] = "market_data_cache"

    return basis
