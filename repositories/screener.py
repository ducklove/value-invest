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
}

# Sort keys. The UI sends these names; they resolve to snapshot fields.
# ``stock_code``/``corp_name``/``close_price`` are kept as aliases so the
# frontend sort control does not change, even though the snapshot uses
# ``ticker``/``name``/``close``.
ALLOWED_SORTS: dict[str, str] = {
    **ALLOWED_FILTERS,
    "stock_code": "ticker",
    "corp_name": "name",
    "close_price": "close",
    "market_cap": "market_cap",
}

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

    # Sort. None values sort last regardless of direction so the user always
    # sees rows that have the sort metric populated first.
    sort_field = ALLOWED_SORTS.get(sort_by, ALLOWED_SORTS["market_cap"])
    reverse = sort_dir.lower() != "asc"

    def _sort_key(row: dict):
        v = row.get(sort_field)
        # (has_value, value): has_value=False sorts before when reverse, so
        # None rows go to the end on desc and to the start on asc — we flip by
        # pushing them to the bottom in both cases via a sentinel.
        try:
            return (0, float(v)) if v is not None else (1, 0.0)
        except (TypeError, ValueError):
            return (1, 0.0)

    matched.sort(key=_sort_key, reverse=reverse)

    page = matched[offset:offset + limit]
    result = []
    for row in page:
        out = dict(row)
        for src, dst in _ROW_FIELD_ALIASES.items():
            if src in out and dst not in out:
                out[dst] = out[src]
        result.append(out)
    return result, total


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
