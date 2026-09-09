"""현금 통화 간 환전의 체결 금액과 양쪽 잔고를 계산한다."""

from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, field_validator

from domain.portfolio_trades import TradeCurrency, TradeError, TradeNumber, money_unit


class ExchangeInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    side: Literal["exchange"]
    from_currency: TradeCurrency
    to_currency: TradeCurrency
    amount: Annotated[TradeNumber, Field(gt=0)]
    rate: Annotated[TradeNumber, Field(gt=0)]
    rate_basis: Literal["to_per_from", "from_per_to"] = "to_per_from"
    fees: TradeNumber = Decimal(0)
    received_amount: Annotated[TradeNumber, Field(gt=0)] | None = None
    memo: Annotated[str, Field(max_length=500)] = ""

    @field_validator("from_currency", "to_currency", mode="before")
    @classmethod
    def uppercase(cls, value):
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("amount", "rate", "fees", "received_amount", mode="before")
    @classmethod
    def reject_boolean(cls, value):
        if isinstance(value, bool):
            raise ValueError("금액과 환율은 숫자로 입력해 주세요.")
        return value


class ExchangeCreate(ExchangeInput):
    request_id: UUID
    expected_revision: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ExchangeResult(BaseModel):
    side: Literal["exchange"]
    stock_code: str
    stock_name: str
    from_currency: TradeCurrency
    to_currency: TradeCurrency
    amount: FiniteFloat
    rate: FiniteFloat
    rate_basis: Literal["to_per_from", "from_per_to"]
    fees: FiniteFloat
    source_debit: FiniteFloat
    received_amount: FiniteFloat
    received_override: bool
    from_before: FiniteFloat
    from_after: FiniteFloat
    to_before: FiniteFloat
    to_after: FiniteFloat
    units_change: Literal[0] = 0
    memo: str


class ExchangePreview(ExchangeResult):
    revision: str


class ExchangeRecord(ExchangeResult):
    request_id: str
    created_at: str
    replayed: bool


def calculate_exchange(exchange: ExchangeInput, source: dict | None, target: dict | None) -> dict:
    if exchange.from_currency == exchange.to_currency:
        raise TradeError("서로 다른 통화를 선택해 주세요.")
    from_unit, to_unit = money_unit(exchange.from_currency), money_unit(exchange.to_currency)
    if exchange.amount != exchange.amount.quantize(from_unit) or exchange.fees != exchange.fees.quantize(from_unit):
        raise TradeError("환전 금액과 수수료는 보내는 통화의 최소 금액 단위로 입력해 주세요.")
    if exchange.received_amount is not None and exchange.received_amount != exchange.received_amount.quantize(to_unit):
        raise TradeError("실제 수령액은 받는 통화의 최소 금액 단위로 입력해 주세요.")
    balances = []
    for cash, currency in [(source, exchange.from_currency), (target, exchange.to_currency)]:
        balance = Decimal(str((cash or {}).get("quantity", 0)))
        if not balance.is_finite() or (cash and (cash.get("currency") != currency or cash.get("pair_long_code"))):
            raise TradeError("현금 항목의 잔고·통화 설정을 확인해 주세요.")
        balances.append(balance)
    debit = exchange.amount + exchange.fees
    if debit > balances[0]:
        raise TradeError(f"수수료를 포함한 {exchange.from_currency} 현금이 부족합니다.")
    received = exchange.received_amount
    if received is None:
        received = (exchange.amount * exchange.rate if exchange.rate_basis == "to_per_from" else exchange.amount / exchange.rate).quantize(to_unit, rounding=ROUND_HALF_UP)
    limit = Decimal("1000000000000000") if to_unit == 1 else Decimal("1000000000000")
    if not 0 < received <= limit:
        raise TradeError("수령액이 지원 범위를 벗어났습니다. 환율을 확인해 주세요.")
    after = [balances[0] - debit, balances[1] + received]
    for balance, currency in zip(after, [exchange.from_currency, exchange.to_currency], strict=True):
        limit = Decimal("1000000000000000") if money_unit(currency) == 1 else Decimal("1000000000000")
        if abs(balance) > limit or Decimal(str(float(balance))) != balance:
            raise TradeError("변경 후 현금 잔고가 저장 범위·정밀도를 벗어났습니다.")
    return {
        "side": "exchange", "stock_code": f"CASH_{exchange.from_currency}",
        "stock_name": f"{exchange.from_currency} → {exchange.to_currency}",
        "from_currency": exchange.from_currency, "to_currency": exchange.to_currency,
        "amount": float(exchange.amount), "rate": float(exchange.rate), "rate_basis": exchange.rate_basis,
        "fees": float(exchange.fees), "source_debit": float(debit), "received_amount": float(received),
        "received_override": exchange.received_amount is not None,
        "from_before": float(balances[0]), "from_after": float(after[0]),
        "to_before": float(balances[1]), "to_after": float(after[1]), "units_change": 0, "memo": exchange.memo,
    }
