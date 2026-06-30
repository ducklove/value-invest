"""Compatibility shim — 구현은 ``services.market.sessions`` 로 이관됨(ST-03).

기존 ``import market_sessions`` / ``market_sessions.open_markets`` 호출부가
깨지지 않게 동일한 심볼을 재수출한다. 새 코드는
``from services.market import sessions`` 를 직접 사용할 것.
루트 모듈은 모든 호출부가 이관된 뒤 별도 커밋에서 제거한다.
"""

from __future__ import annotations

from services.market.sessions import (  # noqa: F401 — re-export for compatibility
    KST,
    MARKETS,
    _in_window,
    _nth_weekday,
    open_markets,
    us_eastern_is_dst,
)
