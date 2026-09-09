"""배당 분배금은 현금만 줄이고 펀드 좌수는 유지한다."""

from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, field_validator

from domain.portfolio_trades import TradeCurrency, TradeError, TradeNumber, money_unit


class DistributionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    currency: TradeCurrency = "KRW"
    amount: Annotated[TradeNumber, Field(gt=0)]
    fx_rate: Annotated[TradeNumber, Field(gt=0)] | None = None
    memo: Annotated[str, Field(max_length=500)] = ""

    @field_validator("amount", "fx_rate", mode="before")
    @classmethod
    def no_boolean(cls, value):
        if isinstance(value, bool):
            raise ValueError("금액과 환율은 숫자로 입력해 주세요.")
        return value


class DistributionCreate(DistributionInput):
    request_id: UUID
    expected_revision: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class DistributionResult(BaseModel):
    currency: TradeCurrency
    amount: FiniteFloat
    amount_krw: FiniteFloat
    fx_rate: FiniteFloat
    cash_before: FiniteFloat
    cash_after: FiniteFloat
    available_before: FiniteFloat
    available_after: FiniteFloat
    units_change: Literal[0] = 0
    memo: str


class DistributionPreview(DistributionResult):
    revision: str


class DistributionRecord(DistributionResult):
    request_id: str
    date: str
    created_at: str
    replayed: bool


def calculate_distribution(payload: DistributionInput, cash: dict | None, available: Decimal) -> dict:
    unit = money_unit(payload.currency)
    if payload.amount != payload.amount.quantize(unit):
        raise TradeError("분배금은 해당 통화의 최소 금액 단위로 입력해 주세요.")
    balance = Decimal(str((cash or {}).get("quantity", 0)))
    if not balance.is_finite() or (cash and cash.get("currency") != payload.currency):
        raise TradeError("출금할 현금의 잔고·통화를 확인해 주세요.")
    if payload.amount > available:
        raise TradeError("아직 분배하지 않은 세후 배당 누적액을 초과할 수 없습니다.")
    if payload.amount > balance:
        raise TradeError("분배금을 출금할 현금이 부족합니다.")
    after = balance - payload.amount
    if Decimal(str(float(after))) != after:
        raise TradeError("변경 후 현금의 저장 정밀도를 벗어났습니다.")
    fx = Decimal(1) if payload.currency == "KRW" else payload.fx_rate
    if fx is None:
        raise TradeError("분배 시점의 환율(1 외화당 KRW)을 입력해 주세요.")
    krw = (payload.amount * fx).quantize(Decimal(1), rounding=ROUND_HALF_UP)
    if not 0 < krw <= 1_000_000_000_000:
        raise TradeError("원화 환산 분배금이 지원 범위를 벗어났습니다.")
    return {"currency": payload.currency, "amount": float(payload.amount), "amount_krw": float(krw), "fx_rate": float(fx),
            "cash_before": float(balance), "cash_after": float(after), "available_before": float(available),
            "available_after": float(available - payload.amount), "units_change": 0, "memo": payload.memo}
