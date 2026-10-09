"""Pure subscription planning: priority, user fairness, deduplication and stability."""

from __future__ import annotations

from domain.portfolio_codes import normalize_portfolio_code

PRIORITIES = ("portfolio", "benchmark", "sidebar", "analysis")
MAX_CLIENT_CODES = 1000


def sanitize(requested: object) -> dict[str, list[str]]:
    result = {}
    seen = set()
    if not isinstance(requested, dict):
        return result
    for group in PRIORITIES:
        values = requested.get(group)
        if not isinstance(values, list):
            continue
        codes = []
        for raw in values[:MAX_CLIENT_CODES]:
            if not isinstance(raw, str) or len(raw) > 32:
                continue
            code = normalize_portfolio_code(raw)
            if code and code not in seen and len(seen) < MAX_CLIENT_CODES:
                seen.add(code)
                codes.append(code)
        result[group] = codes
    return result


def fair_codes(demands: dict[str | None, dict[str, list[str]]]) -> list[str]:
    """One turn per user; all anonymous clients share a single guest demand bucket."""
    result, seen = [], set()
    for group in PRIORITIES:
        lists = [demands[user].get(group, []) for user in sorted(demands, key=lambda user: user or "")]
        for index in range(max((len(codes) for codes in lists), default=0)):
            for codes in lists:
                if index < len(codes) and codes[index] not in seen:
                    seen.add(codes[index])
                    result.append(codes[index])
    return result


def allocate(codes: list[str], sources: list, previous: dict[str, str]) -> dict[str, str]:
    """Retain existing ownership where possible, prefer specialized sources for new codes."""
    remaining = {source.id: source.capacity for source in sources}
    by_id = {source.id: source for source in sources}
    assignments = {}
    for code in codes:
        old = by_id.get(previous.get(code))
        choices = ([old] if old else []) + [source for source in sources if source is not old]
        for source in choices:
            if remaining[source.id] > 0 and source.supports(code) and source.available(code):
                assignments[code] = source.id
                remaining[source.id] -= 1
                break
    return assignments
