"""사유 입력의 중요도 기준. 소액이라는 이유로 거래 종류를 추정하지 않는다."""

from decimal import Decimal

MINOR_FLOOR_KRW = Decimal("1000")
MINOR_CAP_KRW = Decimal("10000")
MINOR_CASH_RATIO = Decimal("0.0001")  # 예수금의 0.01%, 하루 절대 변동 합계에 적용


def minor_limit(currency: str, cash_basis: float | None, fx_rate: float | None = None) -> Decimal:
    rate = Decimal(1) if currency == "KRW" else Decimal(str(fx_rate or 0))
    if not rate.is_finite() or rate <= 0:
        return Decimal(0)  # 환율이 없으면 외화를 원화와 같은 액수로 취급하지 않는다.
    basis = abs(Decimal(str(cash_basis or 0)))
    limit = min(MINOR_CAP_KRW, max(MINOR_FLOOR_KRW, basis * rate * MINOR_CASH_RATIO))
    return limit / rate
