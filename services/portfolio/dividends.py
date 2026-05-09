from __future__ import annotations

from services.portfolio.identifiers import (
    common_stock_code,
    is_korean_stock,
    is_preferred_stock,
    normalize_portfolio_code,
)


DIVIDEND_WARMUP_TTL = 6 * 60 * 60


def dividend_warmup_targets(code: str) -> list[str]:
    normalized = normalize_portfolio_code(code)
    if not is_korean_stock(normalized):
        return []
    if is_preferred_stock(normalized):
        common = common_stock_code(normalized)
        return [normalized, common] if common != normalized else [normalized]
    return [normalized]


def due_dividend_warmup_targets(
    codes: list[str],
    now: float,
    last_warmup: dict[str, float],
    *,
    running_codes: set[str] | None = None,
    ttl_seconds: float = DIVIDEND_WARMUP_TTL,
) -> list[str]:
    running_codes = running_codes or set()
    seen: set[str] = set()
    targets: list[str] = []
    for code in codes:
        for target in dividend_warmup_targets(code):
            if target not in seen:
                seen.add(target)
                targets.append(target)

    due: list[str] = []
    for code in targets:
        if code in running_codes:
            continue
        last = last_warmup.get(code, 0)
        if now - last > ttl_seconds:
            due.append(code)
    return due
