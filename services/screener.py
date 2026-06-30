"""Value screener service — validation, normalization, caching.

Sits between ``routes/screener.py`` (HTTP) and ``repositories/screener.py``
(SQL). Responsibilities:

* Validate and normalize the filter spec coming from the API so the repo only
  ever sees trusted (key, op, value) tuples — the SQL builder trusts its args.
* Round numbers sensibly for display.
* Cache a page of results keyed by the filter spec so repeat navigations don't
  re-hit SQLite; short TTL because fundamentals move only quarterly.

The cache is keyed on a stable hash of (filters, sort, pagination), so two
identical requests share an entry but a changed filter bypasses it.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import cache
from core.errors import AppError
from repositories import screener as screener_repo

# Which metrics may be filtered, with a human label and sane bounds. Bounds
# exist to reject nonsense (e.g. P/E of 1e9) that would still run as SQL but
# produce a useless result. Kept intentionally loose — value screens vary.
FILTER_SPECS: dict[str, dict[str, Any]] = {
    "per": {"label": "P/E (배)", "min": -1000, "max": 1000},
    "pbr": {"label": "P/B (배)", "min": -100, "max": 100},
    "dividend_yield": {"label": "배당수익률 (%)", "min": -50, "max": 200},
    "market_cap": {"label": "시가총액 (억원)", "min": 0, "max": 1e8},
    "roe": {"label": "ROE (%)", "min": -500, "max": 1000},
    "debt_ratio": {"label": "부채비율 (%)", "min": -100, "max": 1e6},
    "operating_margin": {"label": "영업이익률 (%)", "min": -500, "max": 500},
    "eps": {"label": "EPS (원)", "min": -1e7, "max": 1e7},
    "bps": {"label": "BPS (원)", "min": -1e7, "max": 1e7},
}

ALLOWED_SORTS = tuple(screener_repo.ALLOWED_SORTS.keys())
DEFAULT_SORT = "market_cap"
DEFAULT_SORT_DIR = "desc"

# Short cache window — fundamentals are quarterly, but the user may re-run a
# screen after adjusting filters and should see fresh pagination counts fast.
_CACHE_TTL_SECONDS = 5 * 60
_CACHE_NAMESPACE = "screener.results"

MAX_LIMIT = 200


class ScreenerError(AppError):
    """User-facing screener input error (maps to HTTP 400 via AppError? — see note).

    AppError defaults to 500; we override status_code to 400 so bad filter
    input comes back as a client error, consistent with the routes' use of
    HTTPException(400) elsewhere.
    """

    status_code = 400
    default_detail = "스크리너 조건이 올바르지 않습니다."


def _coerce_number(key: str, op: str, raw: Any) -> float:
    """Validate one filter value, returning a clean float or raising."""
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ScreenerError(f"{key}의 {op} 값이 숫자가 아닙니다.") from exc
    spec = FILTER_SPECS[key]
    if value < spec["min"] or value > spec["max"]:
        raise ScreenerError(
            f"{key}의 {op} 값이 허용 범위({spec['min']}~{spec['max']})를 벗어났습니다."
        )
    return value


def normalize_filters(raw: dict[str, Any]) -> dict[str, tuple[str, float]]:
    """Turn the API payload into a trusted ``(op, value)`` map for the repo.

    Input shape (each optional, both directions independent)::

        {"per": {"min": 3, "max": 15}, "roe": {"min": 10}, ...}

    A metric with neither ``min`` nor ``max`` is dropped silently. Unknown
    metric names are rejected so the API surface stays explicit.
    """
    if not isinstance(raw, dict):
        raise ScreenerError("필터는 object 형태여야 합니다.")
    filters: dict[str, tuple[str, float]] = {}
    for key, bounds in raw.items():
        if key not in FILTER_SPECS:
            raise ScreenerError(f"알 수 없는 필터 항목입니다: {key}")
        if not isinstance(bounds, dict):
            raise ScreenerError(f"{key} 필터는 object(min/max) 형태여야 합니다.")
        for op in ("min", "max"):
            if op in bounds and bounds[op] is not None:
                filters[key] = (op, _coerce_number(key, op, bounds[op]))
    return filters


def _cache_key(
    filters: dict[str, tuple[str, float]],
    sort_by: str,
    sort_dir: str,
    limit: int,
    offset: int,
) -> str:
    # Sorted items make the key order-independent, so {"per":{"min":3}} and a
    # re-serialized equivalent share a cache entry.
    payload = json.dumps(
        {
            "filters": sorted(filters.items()),
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "limit": limit,
            "offset": offset,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _round_metric(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    return value


def _format_row(row: dict) -> dict:
    """Round float metrics for display; keep None as None."""
    float_keys = {
        "close_price",
        "per",
        "pbr",
        "eps",
        "bps",
        "dividend_per_share",
        "dividend_yield",
        "market_cap",
        "revenue",
        "operating_profit",
        "net_income",
        "total_equity",
        "total_liabilities",
        "roe",
        "debt_ratio",
        "operating_margin",
    }
    return {k: (_round_metric(v) if k in float_keys else v) for k, v in row.items()}


async def run_screen(
    filters_raw: dict[str, Any],
    *,
    sort_by: str = DEFAULT_SORT,
    sort_dir: str = DEFAULT_SORT_DIR,
    limit: int = 50,
    offset: int = 0,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Validate inputs, hit the repo (or cache), and shape the API response."""
    if sort_by not in ALLOWED_SORTS:
        raise ScreenerError(f"정렬 기준은 {', '.join(ALLOWED_SORTS)} 중 하나여야 합니다.")
    if sort_dir not in ("asc", "desc"):
        raise ScreenerError("정렬 방향은 asc 또는 desc 여야 합니다.")
    if limit <= 0 or limit > MAX_LIMIT:
        raise ScreenerError(f"limit 은 1~{MAX_LIMIT} 사이여야 합니다.")
    if offset < 0:
        raise ScreenerError("offset 은 0 이상이어야 합니다.")

    filters = normalize_filters(filters_raw)

    key = _cache_key(filters, sort_by, sort_dir, limit, offset)
    if use_cache:
        cached = await cache.get_cache_value_entry(_CACHE_NAMESPACE, key)
        if cached is not None:
            return cached.value

    rows, total = await screener_repo.screen_stocks(
        filters,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )

    result = {
        "filters": {k: {"op": v[0], "value": v[1]} for k, v in filters.items()},
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "limit": limit,
        "offset": offset,
        "total": total,
        "rows": [_format_row(r) for r in rows],
    }
    if use_cache:
        await cache.set_cache_value(_CACHE_NAMESPACE, key, result, ttl_seconds=_CACHE_TTL_SECONDS)
    return result


async def get_filter_specs() -> dict[str, Any]:
    """Static-ish description of available filters, for the UI to render controls."""
    coverage = await screener_repo.screener_coverage()
    return {
        "filters": FILTER_SPECS,
        "sorts": ALLOWED_SORTS,
        "default_sort": DEFAULT_SORT,
        "default_sort_dir": DEFAULT_SORT_DIR,
        "max_limit": MAX_LIMIT,
        "coverage": coverage,
    }
