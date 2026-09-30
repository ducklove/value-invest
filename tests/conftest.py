"""pytest 공용 fixture.

unittest 클래스 기반 테스트는 tests/_harness.py 의 ``TempDbMixin`` 을
상속한다 (unittest 클래스는 fixture 주입 불가). function-style 테스트는
아래 ``temp_db`` fixture 로 같은 temp-DB 수명주기를 쓴다.
"""
import os
import sys
import tempfile

import pytest
from _harness import close_temp_db, open_temp_db

# 테스트는 외부 엔드포인트에 닿지 않는다. 운영 기본값(KIS 프록시,
# finance-pi 종가 API)을 그대로 두면, 해당 호스트로의 연결이 빠르게
# 거절되지 않고 블랙홀되는 환경(예: 프록시 차단 샌드박스)에서 mock이
# 누락된 테스트가 무기한 멈춘다. import 시점에 빠른 실패 주소로 고정해
# 어떤 환경에서도 동일하게 동작하게 한다. (conftest 는 테스트 모듈보다
# 먼저 import 되므로 모듈 import 시점에 env 를 읽는 클라이언트에도 적용)
os.environ.setdefault("KIS_PROXY_BASE_URL", "http://127.0.0.1:1")
os.environ.setdefault("CLOSE_PRICE_API_ENABLED", "0")
# 형제 summary.json(services/ecosystem/siblings.py) 단계는 기본 꺼 둔다 — 기존 테스트는
# 레거시 fetch(_get_json/_load_pair)만 패치한다. summary 경로 테스트는 켜고 쓴다.
os.environ.setdefault("ECOSYSTEM_SUMMARIES", "0")


@pytest.fixture
async def temp_db():
    """temp-DB 수명주기 fixture — 패치된 DB 경로를 yield 한다."""
    tmp = tempfile.TemporaryDirectory()
    db_path, db_patch = await open_temp_db(tmp)
    try:
        yield db_path
    finally:
        await close_temp_db(tmp, db_patch)


@pytest.fixture(autouse=True)
def _reset_short_lived_quote_caches():
    """모듈 전역 단기 캐시(벌크 시세 micro-cache, KIS 재무/배당 TTL)를 테스트마다 비운다.

    같은 종목코드를 다른 mock 값으로 조회하는 테스트끼리 캐시로 값이 새지 않게
    한다. 이미 import 된 모듈만 건드려 import 부작용(env 고정 순서)을 만들지 않는다.
    """
    stock_quotes = sys.modules.get("services.stock_quotes")
    if stock_quotes is not None:
        stock_quotes._bulk_micro_cache.clear()
        stock_quotes._bulk_inflight.clear()
    kis_proxy_client = sys.modules.get("kis_proxy_client")
    if kis_proxy_client is not None:
        kis_proxy_client.clear_response_cache()
    yield
