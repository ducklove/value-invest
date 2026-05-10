from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any

import asset_insights
import cache
import close_price_client
from services.portfolio.identifiers import common_stock_code, is_korean_stock, is_preferred_stock
from services.portfolio.time_windows import today_kst_date


logger = logging.getLogger(__name__)

_VALUATION_CACHE_TTL_SECONDS = 300
_valuation_basis_cache: dict[tuple[str, str, bool], tuple[float, dict]] = {}


def _as_of_key(as_of: date | datetime | str | None) -> str:
    value = as_of or today_kst_date()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _copy_basis(basis: dict) -> dict:
    return dict(basis) if isinstance(basis, dict) else {}


def _cache_get(source_code: str, as_of_key: str, use_market_cache_fallback: bool) -> dict | None:
    key = (source_code, as_of_key, bool(use_market_cache_fallback))
    cached = _valuation_basis_cache.get(key)
    if not cached:
        return None
    ts, basis = cached
    if time.monotonic() - ts > _VALUATION_CACHE_TTL_SECONDS:
        _valuation_basis_cache.pop(key, None)
        return None
    return _copy_basis(basis)


def _cache_set(source_code: str, as_of_key: str, use_market_cache_fallback: bool, basis: dict) -> None:
    key = (source_code, as_of_key, bool(use_market_cache_fallback))
    _valuation_basis_cache[key] = (time.monotonic(), _copy_basis(basis))


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


def _base_basis(source_code: str) -> dict:
    return {
        "applicable": True,
        "sourceCode": source_code,
        "source": "market_data",
    }


def _basis_from_fundamental(source_code: str, raw: dict | None) -> dict:
    basis = _base_basis(source_code)
    fundamental = raw if isinstance(raw, dict) else {}
    if fundamental:
        basis["source"] = "internal_fundamentals"
        basis["fiscalYear"] = fundamental.get("fiscal_year")
        basis["asOf"] = fundamental.get("as_of") or fundamental.get("available_date")

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
    return basis


async def _apply_market_cache_fallback(source_code: str, basis: dict) -> dict:
    cached = await cache.get_latest_market_valuation(source_code)
    if not cached:
        return basis

    basis = _copy_basis(basis)
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


async def _persist_market_target_metrics(source_code: str, basis: dict) -> None:
    year = basis.get("fiscalYear")
    try:
        year = int(year)
    except (TypeError, ValueError):
        return
    row = {
        "stock_code": source_code,
        "year": year,
        "close_price": basis.get("closePrice"),
        "eps": basis.get("eps"),
        "bps": basis.get("bps"),
    }
    if row["eps"] is None and row["bps"] is None and row["close_price"] is None:
        return
    try:
        await cache.upsert_market_target_metrics([row])
    except Exception as exc:
        logger.info("market target metric cache write failed (%s): %s", source_code, exc)


async def fetch_valuation_basis(
    stock_code: str,
    *,
    as_of: date | datetime | str | None = None,
    use_market_cache_fallback: bool = True,
) -> dict:
    return (await fetch_valuation_basis_map([stock_code], as_of=as_of, use_market_cache_fallback=use_market_cache_fallback)).get(
        stock_code,
        {"applicable": False},
    )


async def fetch_valuation_basis_map(
    stock_codes: list[str] | tuple[str, ...],
    *,
    as_of: date | datetime | str | None = None,
    use_market_cache_fallback: bool = True,
) -> dict[str, dict]:
    as_of_text = _as_of_key(as_of)
    requested_codes = list(dict.fromkeys(str(code or "").strip() for code in stock_codes if str(code or "").strip()))
    result: dict[str, dict] = {}
    source_by_requested: dict[str, str] = {}
    source_codes: list[str] = []

    for code in requested_codes:
        if not is_korean_stock(code):
            result[code] = {"applicable": False}
            continue
        source_code = common_stock_code(code) if is_preferred_stock(code) else code
        source_by_requested[code] = source_code
        if source_code not in source_codes:
            source_codes.append(source_code)

    basis_by_source: dict[str, dict] = {}
    missing_sources: list[str] = []
    for source_code in source_codes:
        cached = _cache_get(source_code, as_of_text, use_market_cache_fallback)
        if cached is not None:
            basis_by_source[source_code] = cached
        else:
            missing_sources.append(source_code)

    fundamentals: dict[str, Any] = {}
    if missing_sources:
        try:
            fundamentals = await close_price_client.get_basic_fundamentals(missing_sources, as_of=as_of_text)
        except Exception as exc:
            logger.info("portfolio valuation fundamentals unavailable (%s): %s", ",".join(missing_sources[:5]), exc)

    for source_code in missing_sources:
        basis = _basis_from_fundamental(source_code, fundamentals.get(source_code) if isinstance(fundamentals, dict) else None)
        if use_market_cache_fallback and needs_market_valuation_fallback(basis):
            basis = await _apply_market_cache_fallback(source_code, basis)
        if basis.get("source") == "internal_fundamentals":
            await _persist_market_target_metrics(source_code, basis)
        basis_by_source[source_code] = basis
        _cache_set(source_code, as_of_text, use_market_cache_fallback, basis)

    for requested_code, source_code in source_by_requested.items():
        result[requested_code] = _copy_basis(basis_by_source.get(source_code) or _base_basis(source_code))
    return result
