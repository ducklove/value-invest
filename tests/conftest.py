"""pytest 공용 fixture.

unittest 클래스 기반 테스트는 tests/_harness.py 의 ``TempDbMixin`` 을
상속한다 (unittest 클래스는 fixture 주입 불가). function-style 테스트는
아래 ``temp_db`` fixture 로 같은 temp-DB 수명주기를 쓴다.
"""
import os
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


@pytest.fixture
async def temp_db():
    """temp-DB 수명주기 fixture — 패치된 DB 경로를 yield 한다."""
    tmp = tempfile.TemporaryDirectory()
    db_path, db_patch = await open_temp_db(tmp)
    try:
        yield db_path
    finally:
        await close_temp_db(tmp, db_patch)
