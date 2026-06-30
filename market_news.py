"""Compatibility shim — 구현은 ``services.market.news`` 로 이관됨(ST-03).

기존 ``import market_news`` / ``market_news.fetch_market_news`` 호출부가
깨지지 않게 동일한 심볼을 재수출한다. 새 코드는
``from services.market import fetch_market_news`` 를 직접 사용할 것.
루트 모듈은 모든 호출부가 이관된 뒤 별도 커밋에서 제거한다.
"""

from __future__ import annotations

from services.market.news import (  # noqa: F401 — re-export for compatibility
    _NEWS_TTL,
    _SEM,
    _parse_news,
    fetch_market_news,
)
