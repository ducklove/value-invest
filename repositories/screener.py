"""Value screener — in-memory filtering over a finance-pi universe snapshot.

The screener answers "which stocks in the universe satisfy these value
conditions today?". It no longer queries the local ``market_data`` /
``financial_data`` tables (which only cover stocks a user has analyzed).
Instead the service layer fetches a full KOSPI/KOSDAQ snapshot from
finance-pi (``/api/fundamentals/screener``) and this module filters/sorts/
paginates that snapshot in memory.

Design notes:

* **Whitelist-only filtering.** Filter keys are validated against
  ``ALLOWED_FILTERS``; the finance-pi snapshot row uses matching field names.
  The service layer validates values before calling here.
* **finance-pi field names** map 1:1 to the screener filters, except the
  historical ``stock_code``/``corp_name`` (now ``ticker``/``name``) and
  ``close_price`` (now ``close``). ``ALLOWED_SORTS`` keeps the sort keys the
  UI already uses and resolves them to snapshot field names.
* **No persistence.** Nothing is written to the local DB — the snapshot is
  fetched live and optionally cached by the service layer. This matches the
  project decision to keep universe valuation data in finance-pi only.
"""

from __future__ import annotations

# Metrics the screener may filter/sort on. The keys are the API filter names
# (matching services/screener.py FILTER_SPECS); the values are the
# corresponding field names in a finance-pi screener snapshot row.
ALLOWED_FILTERS: dict[str, str] = {
    "per": "per",
    "pbr": "pbr",
    "dividend_yield": "dividend_yield",
    "market_cap": "market_cap",
    "roe": "roe",
    "debt_ratio": "debt_ratio",
    "operating_margin": "operating_margin",
    # 히스토리 지표(services/screener 가 스냅샷 행에 붙인다) — 전년 대비 증가율(%).
    "revenue_growth": "revenue_growth",
    "op_profit_growth": "op_profit_growth",
    "dps_growth": "dps_growth",
}

# Sort keys. The UI sends these names; they resolve to snapshot fields.
# ``stock_code``/``corp_name``/``close_price`` are kept as aliases so the
# frontend contract does not change, even though the snapshot uses
# ``ticker``/``name``/``close``. 결과 표의 모든 열(종가 제외)이 헤더 클릭으로
# 정렬되므로 표에 나오는 히스토리 열도 여기 있다.
ALLOWED_SORTS: dict[str, str] = {
    **ALLOWED_FILTERS,
    "stock_code": "ticker",
    "corp_name": "name",
    "market": "market",
    "close_price": "close",
    "market_cap": "market_cap",
    "dps": "dps",
    "roe_change": "roe_change",
    "debt_ratio_change": "debt_ratio_change",
    "op_margin_change": "op_margin_change",
    "revenue_cagr_3y": "revenue_cagr_3y",
    "revenue_growth_years": "revenue_growth_years",
    "dps_growth_years": "dps_growth_years",
    "profit_years": "profit_years",
}

# 문자열로 비교하는 정렬 필드(나머지는 숫자).
_TEXT_SORT_FIELDS = frozenset({"ticker", "name", "market"})

# Snapshot row fields projected in screener results, with friendly aliases for
# the frontend (which still expects stock_code/corp_name/close_price from the
# original SQL-based implementation).
_ROW_FIELD_ALIASES = {
    "ticker": "stock_code",
    "name": "corp_name",
    "close": "close_price",
}


def _row_value(row: dict, key: str) -> float | None:
    """Read a numeric filter/sort field from a snapshot row, tolerating None."""
    field = ALLOWED_FILTERS.get(key, key)
    value = row.get(field)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _meets_filter(row: dict, key: str, op: str, target: float) -> bool:
    value = _row_value(row, key)
    if value is None:
        return False
    return value > target if op == "min" else value < target


def screen_snapshot(
    rows: list[dict],
    filters: dict[str, list[tuple]],
    *,
    sort_by: str = "market_cap",
    sort_dir: str = "desc",
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Filter/sort/paginate an in-memory finance-pi snapshot.

    ``filters`` maps a whitelisted metric name to a list of ``(op, value)``
    pairs. A metric may carry both ``min`` and ``max`` (range). All active
    constraints are AND-ed. The caller (service layer) validates keys/ops/
    values — this function trusts its arguments.

    Returns ``(page_rows, total_match_count)``. Each result row is a copy of
    the snapshot row with stock_code/corp_name/close_price aliases added so
    the frontend contract is unchanged.
    """
    if limit <= 0 or not rows:
        return [], 0

    # Apply filters (AND across all metrics, AND within a metric's min/max).
    matched = rows
    for key, pairs in filters.items():
        for op, value in pairs:
            field = ALLOWED_FILTERS.get(key, key)
            matched = [
                r for r in matched
                if r.get(field) is not None and _meets_filter(r, key, op, float(value))
            ]

    total = len(matched)
    if total == 0:
        return [], 0

    matched = sort_rows(matched, sort_by, sort_dir)

    page = matched[offset:offset + limit]
    result = []
    for row in page:
        out = dict(row)
        for src, dst in _ROW_FIELD_ALIASES.items():
            if src in out and dst not in out:
                out[dst] = out[src]
        result.append(out)
    return result, total


def sort_rows(rows: list[dict], sort_by: str, sort_dir: str) -> list[dict]:
    """정렬 — 값이 없는 행은 오름·내림 어느 방향이든 항상 맨 뒤.

    (예전 구현은 ``reverse=True`` 한 번으로 정렬해 내림차순에서 결측 행이 맨
    앞으로 왔다.) 값이 있는 행만 정렬하고 결측 행을 원래 순서로 뒤에 붙인다.
    """
    sort_field = ALLOWED_SORTS.get(sort_by, ALLOWED_SORTS["market_cap"])
    reverse = sort_dir.lower() != "asc"
    is_text = sort_field in _TEXT_SORT_FIELDS
    present: list[tuple[object, dict]] = []
    missing: list[dict] = []
    for row in rows:
        value = row.get(sort_field)
        if is_text:
            key: object = str(value).strip() if value is not None else ""
            if not key:
                missing.append(row)
                continue
        else:
            if value is None or isinstance(value, bool):
                missing.append(row)
                continue
            try:
                key = float(value)
            except (TypeError, ValueError):
                missing.append(row)
                continue
            if key != key:  # NaN
                missing.append(row)
                continue
        present.append((key, row))
    present.sort(key=lambda pair: pair[0], reverse=reverse)
    return [row for _, row in present] + missing


def snapshot_coverage(rows: list[dict]) -> dict[str, int]:
    """Coverage stats derived from a finance-pi snapshot.

    The snapshot already excludes ETFs/preferred/SPACs/REITs, so ``universe``
    is the snapshot size (the investable KOSPI/KOSDAQ set), and the per-metric
    counts show how many rows actually carry each value (e.g. PBR is null for
    tickers without equity data).
    """
    universe = len(rows)
    valued = sum(1 for r in rows if r.get("close") is not None)
    fundamentals = sum(1 for r in rows if r.get("equity") is not None)
    return {
        "universe": universe,
        "valued": valued,
        "fundamentals": fundamentals,
    }
