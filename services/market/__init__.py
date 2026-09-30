"""Market-data collection domain (ST-03 progressive migration).

루트 평면 모듈(market_sessions, market_news, market_movers 등)이 이관된 패키지
(D-18). 호출부는 새 경로를 직접 import 하고, 루트 호환 shim 은 두지 않는다
(tests/test_legacy_root_modules.py 가 재도입을 막는다). 이 ``__init__`` 은 가벼운
심볼만 재수출한다 — 무거운 하위 모듈을 여기서 import 하면 순환 import 위험이 커진다.
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
