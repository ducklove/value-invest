"""Market-data collection domain (ST-03 progressive migration).

루트 평면 모듈(market_sessions, market_news, market_movers 등)이 점진 이관되는
패키지. 각 모듈은 순수 도메인 로직만 담고, ``repositories``·``core`` 만
의존한다(순환 import 방지). 루트 모듈은 호환 재수출 레이어로 남아 기존
``import market_X`` 호출부를 깨지 않게 한다 — 후속 커밋에서 단계 제거.
"""

from services.market.news import (
    fetch_market_news,
)
from services.market.sessions import (
    KST,
    MARKETS,
    open_markets,
    us_eastern_is_dst,
)

__all__ = [
    "KST",
    "MARKETS",
    "fetch_market_news",
    "open_markets",
    "us_eastern_is_dst",
]
