"""Compatibility shim — 구현은 ``services.portfolio.nav_snapshot`` 로 이관됨(D-18).

deploy/repairs 의 1회성 복구 스크립트(마커 기반, 리팩터링하지 않음)가
``import snapshot_nav`` 후 모듈 전역(``_fx_usdkrw`` 등)을 읽고 쓰고,
``python3 snapshot_nav.py <date>`` 로 직접 실행한다. 그래서 재수출이 아니라
``sys.modules`` 에 정본 모듈 객체 자체를 등록한다 — 두 이름이 같은 객체다.
새 코드는 ``from services.portfolio import nav_snapshot`` 를 쓸 것.
"""

import sys

from services.portfolio import nav_snapshot as _impl

if __name__ == "__main__":
    _impl.main()
else:
    sys.modules[__name__] = _impl
