"""Tests for ST-03 — 루트 레거시 모듈 → services/market/ 패키지 이관.

이관 후 (1) 새 패키지 경로가 동작하고, (2) 순환 import 없이 로드되는지 검증한다.
루트 호환 shim(market_news·market_sessions)은 호출부 이전 후 삭제됐다 — 재도입
방지는 tests/test_legacy_root_modules.py 가 맡는다.
"""

from __future__ import annotations


def test_new_package_path_exposes_sessions_symbols():
    from services.market import sessions

    assert sessions.MARKETS == ("KR", "US", "JP", "HK", "CN")
    assert callable(sessions.open_markets)
    assert callable(sessions.us_eastern_is_dst)


def test_new_package_init_re_exports_sessions():
    from services.market import MARKETS, open_markets

    assert MARKETS == ("KR", "US", "JP", "HK", "CN")
    assert callable(open_markets)


def test_new_package_path_exposes_news():
    from services.market import news

    assert callable(news.fetch_market_news)


def test_no_circular_import_on_package_load():
    """services.market 패키지 로드가 순환 import 없이 완료된다.

    앞선 테스트들이 이미 import 에 성공했으므로, 여기서는 모듈이 캐시에
    있더라도 reload 하지 않고 그저 패키지의 __all__ 이 기대 심볼을 담는지만
    확인한다(순환 import 가 있었다면 위 테스트에서 ImportError 발생).
    """
    import services.market as pkg

    for name in ("open_markets", "fetch_market_news", "MARKETS", "KST"):
        assert name in pkg.__all__
