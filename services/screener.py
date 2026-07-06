"""Value screener service — validation, snapshot fetch, in-memory filtering.

Sits between ``routes/screener.py`` (HTTP) and ``repositories/screener.py``
(in-memory filter). Responsibilities:

* Validate and normalize the filter spec coming from the API so the repo only
  ever sees trusted (key, op, value) tuples.
* Fetch the full-universe snapshot from finance-pi and cache *the snapshot*
  (not the filtered result) so repeated screens with different filters reuse
  one fetch — finance-pi is the single source of truth and we store nothing.
* Round numbers sensibly for display.

Two cache layers:
* ``screener.snapshot`` — the finance-pi snapshot itself (10 min TTL). This is
  the expensive call; filters run over the cached snapshot in memory.
* ``screener.results`` — the final paginated response (2 min TTL), keyed by
  (filters, sort, pagination), for instant re-navigation.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from close_price_client import ClosePriceClientError, get_screener_snapshot
from core.errors import AppError, ExternalServiceError
from repositories import cache_values
from repositories import screener as screener_repo

# Which metrics may be filtered, with a human label and sane bounds. Bounds
# exist to reject nonsense (e.g. P/E of 1e9) that would still run but produce
# a useless result. Kept intentionally loose — value screens vary.
# finance-pi snapshot fields: per, pbr, dividend_yield, market_cap, roe,
# debt_ratio, operating_margin. (eps/bps were dropped — the snapshot does not
# carry per-share values, only the computed multiples.)
FILTER_SPECS: dict[str, dict[str, Any]] = {
    "per": {"label": "P/E (배)", "min": -1000, "max": 1000},
    "pbr": {"label": "P/B (배)", "min": -100, "max": 100},
    "dividend_yield": {"label": "배당수익률 (%)", "min": -50, "max": 200},
    "market_cap": {"label": "시가총액 (억원)", "min": 0, "max": 1e8},
    "roe": {"label": "ROE (%)", "min": -500, "max": 1000},
    "debt_ratio": {"label": "부채비율 (%)", "min": -100, "max": 1e6},
    "operating_margin": {"label": "영업이익률 (%)", "min": -500, "max": 500},
}

ALLOWED_SORTS = tuple(screener_repo.ALLOWED_SORTS.keys())
DEFAULT_SORT = "market_cap"
DEFAULT_SORT_DIR = "desc"

# Snapshot cache — the expensive finance-pi call. 10 min keeps it fresh enough
# for intraday use without hammering finance-pi on every screen.
_SNAPSHOT_CACHE_TTL = 10 * 60
_SNAPSHOT_CACHE_NS = "screener.snapshot"
# Result cache — the final paginated response. 2 min for instant re-navigation.
_RESULT_CACHE_TTL = 2 * 60
_RESULT_CACHE_NS = "screener.results"

MAX_LIMIT = 200


class ScreenerError(AppError):
    """User-facing screener input error (maps to HTTP 400).

    AppError defaults to 500; we override status_code to 400 so bad filter
    input comes back as a client error.
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


def normalize_filters(raw: dict[str, Any]) -> dict[str, list[tuple[str, float]]]:
    """Turn the API payload into a trusted ``{metric: [(op, value), ...]}`` map.

    Input shape (each optional, both directions independent)::

        {"per": {"min": 3, "max": 15}, "roe": {"min": 10}, ...}

    A metric may carry both ``min`` and ``max`` simultaneously (range filter).
    A metric with neither is dropped silently. Unknown metric names are
    rejected so the API surface stays explicit.
    """
    if not isinstance(raw, dict):
        raise ScreenerError("필터는 object 형태여야 합니다.")
    filters: dict[str, list[tuple[str, float]]] = {}
    for key, bounds in raw.items():
        if key not in FILTER_SPECS:
            raise ScreenerError(f"알 수 없는 필터 항목입니다: {key}")
        if not isinstance(bounds, dict):
            raise ScreenerError(f"{key} 필터는 object(min/max) 형태여야 합니다.")
        for op in ("min", "max"):
            if op in bounds and bounds[op] is not None:
                filters.setdefault(key, []).append((op, _coerce_number(key, op, bounds[op])))
    return filters


def _result_cache_key(
    filters: dict[str, tuple[str, float]],
    sort_by: str,
    sort_dir: str,
    limit: int,
    offset: int,
) -> str:
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
        "close",
        "per",
        "pbr",
        "dividend_yield",
        "market_cap",
        "roe",
        "debt_ratio",
        "operating_margin",
        "revenue",
        "operating_profit",
        "net_income",
        "equity",
    }
    return {k: (_round_metric(v) if k in float_keys else v) for k, v in row.items()}


async def _get_snapshot() -> list[dict]:
    """Fetch (or reuse cached) finance-pi screener snapshot rows.

    finance-pi returns ``market_cap`` in KRW (원). The UI filter label and the
    display formatter both assume 억원 (100M KRW), so we normalize once here
    — the cached snapshot and every downstream filter/display see 억원.
    """
    cached = await cache_values.get_cache_value_entry(_SNAPSHOT_CACHE_NS, "latest")
    if cached is not None and isinstance(cached.value, list):
        return cached.value
    try:
        payload = await get_screener_snapshot()
    except ClosePriceClientError as exc:
        raise ExternalServiceError("스크리너 데이터를 불러오지 못했습니다.") from exc
    rows = payload.get("rows") or []
    for row in rows:
        mc = row.get("market_cap")
        if isinstance(mc, (int, float)) and mc != 0:
            # 원 → 억원. 0/None 은 그대로 둔다(데이터 없음).
            row["market_cap"] = round(mc / 1e8, 2)
    await cache_values.set_cache_value(_SNAPSHOT_CACHE_NS, "latest", rows, ttl_seconds=_SNAPSHOT_CACHE_TTL)
    return rows


async def run_screen(
    filters_raw: dict[str, Any],
    *,
    sort_by: str = DEFAULT_SORT,
    sort_dir: str = DEFAULT_SORT_DIR,
    limit: int = 50,
    offset: int = 0,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Validate inputs, fetch snapshot, filter in memory, shape the response."""
    if sort_by not in ALLOWED_SORTS:
        raise ScreenerError(f"정렬 기준은 {', '.join(ALLOWED_SORTS)} 중 하나여야 합니다.")
    if sort_dir not in ("asc", "desc"):
        raise ScreenerError("정렬 방향은 asc 또는 desc 여야 합니다.")
    if limit <= 0 or limit > MAX_LIMIT:
        raise ScreenerError(f"limit 은 1~{MAX_LIMIT} 사이여야 합니다.")
    if offset < 0:
        raise ScreenerError("offset 은 0 이상이어야 합니다.")

    filters = normalize_filters(filters_raw)
    if not filters:
        # 빈 필터 전수 스캔은 의도치 않은 호출(수천 건 행 반환)이므로 거부한다.
        raise ScreenerError("최소 한 개 이상의 필터 조건을 설정하세요.")

    key = _result_cache_key(filters, sort_by, sort_dir, limit, offset)
    if use_cache:
        cached = await cache_values.get_cache_value_entry(_RESULT_CACHE_NS, key)
        if cached is not None:
            return cached.value

    snapshot = await _get_snapshot()
    rows, total = screener_repo.screen_snapshot(
        snapshot,
        filters,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )

    result = {
        "filters": {
            k: [{"op": op, "value": val} for op, val in pairs]
            for k, pairs in filters.items()
        },
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "limit": limit,
        "offset": offset,
        "total": total,
        "rows": [_format_row(r) for r in rows],
    }
    if use_cache:
        await cache_values.set_cache_value(_RESULT_CACHE_NS, key, result, ttl_seconds=_RESULT_CACHE_TTL)
    return result


async def get_filter_specs() -> dict[str, Any]:
    """Available filters/sorts + current coverage, for the UI to render."""
    snapshot = await _get_snapshot()
    coverage = screener_repo.snapshot_coverage(snapshot)
    return {
        "filters": FILTER_SPECS,
        "sorts": ALLOWED_SORTS,
        "default_sort": DEFAULT_SORT,
        "default_sort_dir": DEFAULT_SORT_DIR,
        "max_limit": MAX_LIMIT,
        "coverage": coverage,
    }
