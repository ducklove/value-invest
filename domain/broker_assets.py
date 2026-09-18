"""증권사 조회 상품과 포트폴리오에 합산하는 원화 평가 항목."""

ACCOUNT_PRODUCTS = frozenset({"stocks", "gold", "krfuture", "gbfuture"})
FUTURES_VALUE_CODES = frozenset({"FUTURES_BASE_KRW", "FUTURES_PNL_KRW"})


def is_futures_value(code: str) -> bool:
    return code in FUTURES_VALUE_CODES
