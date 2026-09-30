"""숫자 파서 단일 출처 (1단계).

``parse_number`` 는 허브에 흩어져 있던 "콤마 제거 + None/'' → None" 파서들과
정확히 같은 의미다. 값을 ``str()`` 로 바꾼 뒤 콤마를 지우고 양끝 공백을 걷어
``float`` 으로 읽는다. 읽을 수 없으면 None. NaN/inf 문자열은 그대로 통과한다.
bool 은 ``str(True)`` 가 숫자가 아니므로 None 이다.

의미가 다른 파서들(NaN 거부, 음수 거부, 예외 발생 등)은 2단계에서 옵션을
명시해 옮긴다.
"""

from __future__ import annotations

from typing import Any


def parse_number(value: Any, *, zero_as_none: bool = False) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if zero_as_none and number == 0:
        return None
    return number
