from __future__ import annotations

import asyncio
import logging
import re

import cache
import close_price_client
import dart_client
from services.portfolio.identifiers import common_stock_code, is_korean_stock, is_preferred_stock
from services.portfolio.time_windows import today_kst_date


logger = logging.getLogger(__name__)

_TARGET_FINANCIAL_VARS_RE = re.compile(r"(?<![A-Za-z가-힣_])(BPS|EPS)(?![A-Za-z가-힣_])")
_TREASURY_SHARE_KEYS = (
    "treasury_shares",
    "treasury_share_count",
    "treasury_stock_count",
    "treas_stock_co",
    "own_stock_count",
    "self_stock_count",
)


def _positive_number(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _metric_amount(metrics: dict, key: str) -> float | None:
    raw = metrics.get(key) if isinstance(metrics, dict) else None
    if isinstance(raw, dict):
        raw = raw.get("amount")
    return _positive_number(raw)


def _per_share_entry(fundamental: dict, key: str) -> dict:
    per_share = fundamental.get("per_share") if isinstance(fundamental, dict) else None
    raw = per_share.get(key) if isinstance(per_share, dict) else None
    return raw if isinstance(raw, dict) else {}


def _per_share_value(fundamental: dict, key: str, share_status: dict | None = None) -> float | None:
    entry = _per_share_entry(fundamental, key)
    value = _positive_number(entry.get("value"))
    if value is None:
        return None
    if entry.get("treasury_shares_excluded") is True:
        return value

    distributed = _first_positive(share_status, ("distributed_shares", "distb_stock_co"))
    if distributed is None:
        return value

    numerator = _positive_number(entry.get("numerator_amount"))
    if numerator is not None:
        return round(numerator / distributed, 2)

    original_shares = _positive_number(entry.get("shares"))
    if original_shares is not None:
        return round(value * original_shares / distributed, 2)
    return value


def _metric_year(fundamental: dict, metrics: dict) -> int | None:
    candidates = []
    if isinstance(fundamental, dict):
        candidates.append(fundamental.get("fiscal_year"))
    if isinstance(metrics, dict):
        candidates.extend(item.get("fiscal_year") for item in metrics.values() if isinstance(item, dict))
    for value in candidates:
        try:
            if value:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_positive(mapping: dict | None, keys: tuple[str, ...]) -> float | None:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        value = _positive_number(mapping.get(key))
        if value is not None:
            return value
    return None


def _shares_excluding_treasury(
    market_row: dict,
    fundamental: dict,
    metrics: dict,
    share_status: dict | None = None,
) -> float | None:
    distributed = _first_positive(share_status, ("distributed_shares", "distb_stock_co"))
    if distributed is not None:
        return distributed

    listed = (
        _first_positive(share_status, ("issued_shares", "istc_totqy"))
        or _first_positive(market_row, ("listed_shares", "lstn_stcn"))
    )
    if listed is None:
        close = _positive_number(market_row.get("close"))
        market_cap = _positive_number(market_row.get("market_cap"))
        if close and market_cap:
            listed = market_cap / close
    if listed is None:
        return None

    treasury = (
        _first_positive(share_status, _TREASURY_SHARE_KEYS)
        or _first_positive(market_row, _TREASURY_SHARE_KEYS)
        or _first_positive(fundamental, _TREASURY_SHARE_KEYS)
        or _metric_amount(metrics, "treasury_shares")
        or _metric_amount(metrics, "treasury_share_count")
    )
    if treasury is not None and 0 <= treasury < listed:
        return listed - treasury
    return listed


def _formula_financial_vars(formula: str | None) -> set[str]:
    return set(_TARGET_FINANCIAL_VARS_RE.findall(str(formula or "")))


async def _fetch_target_share_statuses(codes: list[str], fundamentals: dict, fallback_year: int) -> dict[str, dict]:
    async def _fetch_one(code: str):
        fundamental = fundamentals.get(code) if isinstance(fundamentals, dict) else {}
        metrics = fundamental.get("metrics") if isinstance(fundamental, dict) and isinstance(fundamental.get("metrics"), dict) else {}
        year = _metric_year(fundamental if isinstance(fundamental, dict) else {}, metrics) or fallback_year
        try:
            corp_code = await cache.get_corp_code(code)
            if not corp_code:
                return code, {}
            return code, await dart_client.fetch_common_stock_share_status(corp_code, year)
        except Exception as exc:
            logger.warning("target metric DART share status failed (%s): %s", code, exc)
            return code, {}

    pairs = await asyncio.gather(*(_fetch_one(code) for code in codes))
    return {code: status for code, status in pairs if status}


async def supplement_target_metrics(items: list[dict], target_metrics_map: dict[str, dict]) -> None:
    needed: list[str] = []
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
            needed.append(candidate)

    codes = list(dict.fromkeys(needed))
    if not codes:
        return

    today = today_kst_date()
    try:
        fundamentals = await close_price_client.get_basic_fundamentals(codes, as_of=today)
    except Exception as exc:
        logger.warning("target metric internal API supplement failed (%s): %s", ",".join(codes[:5]), exc)
        return

    share_statuses = await _fetch_target_share_statuses(codes, fundamentals, today.year - 1)
    cache_rows: list[dict] = []
    for code in codes:
        fundamental = fundamentals.get(code) if isinstance(fundamentals, dict) else None
        if not isinstance(fundamental, dict):
            continue
        metrics = fundamental.get("metrics") if isinstance(fundamental.get("metrics"), dict) else {}
        share_status = share_statuses.get(code)
        bps = _per_share_value(fundamental, "bps", share_status)
        eps = _per_share_value(fundamental, "eps_ttm", share_status) or _per_share_value(fundamental, "eps_annual", share_status)
        if bps is None or eps is None:
            equity = _metric_amount(metrics, "equity")
            net_income = _metric_amount(metrics, "net_income")
            shares = _shares_excluding_treasury({}, fundamental, metrics, share_status)
            if shares is not None and shares > 0:
                if bps is None and equity is not None:
                    bps = round(equity / shares, 2)
                if eps is None and net_income is not None:
                    eps = round(net_income / shares, 2)
        if bps is None and eps is None:
            continue

        current = target_metrics_map.setdefault(code, {"eps": None, "bps": None, "dps": None})
        if bps is not None:
            current["bps"] = bps
        if eps is not None:
            current["eps"] = eps

        cache_rows.append(
            {
                "stock_code": code,
                "year": _metric_year(fundamental, metrics) or today.year - 1,
                "close_price": None,
                "market_cap": None,
                "eps": eps,
                "bps": bps,
                "per": None,
                "pbr": None,
            }
        )

    if cache_rows:
        await cache.upsert_market_target_metrics(cache_rows)
