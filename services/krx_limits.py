"""KRX 상한가/하한가 (daily price limit) computation from tick units.

The ±30% daily limit price must itself be a valid tick-aligned order price, so
상한가 = the largest valid tick ≤ base×1.30 and 하한가 = the smallest valid tick
≥ base×0.70 (the tick is the one applicable at the *limit* price level). This is
the exact rule, so a stock is only flagged at the limit when its price actually
equals the limit — not merely "near +30%".

Tick table: 2023-01-25 unified KOSPI/KOSDAQ 호가가격단위.
Caveats: KONEX (±15%), ETF/ETN, listing-day and 정리매매 securities use a
different band/limit and are not modeled here (they fall back to "not at limit").
"""

from __future__ import annotations

import math

# (가격 상한[미만], 호가단위) — price < ceiling 이면 해당 tick.
_TICK_BANDS: tuple[tuple[int, int], ...] = (
    (2_000, 1),
    (5_000, 5),
    (20_000, 10),
    (50_000, 50),
    (200_000, 100),
    (500_000, 500),
)
_TICK_TOP = 1_000  # 500,000원 이상


def krx_tick_size(price: float) -> int:
    """호가가격단위 at *price* (KOSPI/KOSDAQ, 2023 개편 기준)."""
    for ceiling, tick in _TICK_BANDS:
        if price < ceiling:
            return tick
    return _TICK_TOP


def _to_base(base_price) -> float | None:
    try:
        value = float(base_price)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def krx_upper_limit(base_price) -> float | None:
    """상한가 = base×1.30 을 그 가격대 호가단위로 내림."""
    base = _to_base(base_price)
    if base is None:
        return None
    raw = base * 1.30
    tick = krx_tick_size(raw)
    return math.floor(round(raw, 4) / tick) * tick


def krx_lower_limit(base_price) -> float | None:
    """하한가 = base×0.70 을 그 가격대 호가단위로 올림."""
    base = _to_base(base_price)
    if base is None:
        return None
    raw = base * 0.70
    tick = krx_tick_size(raw)
    return math.ceil(round(raw, 4) / tick) * tick
