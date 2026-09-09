"""현물 매매 입력과 잔고·매입가 계산. 외부 시세는 체결가로 대신하지 않는다."""

from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, field_validator

from core.errors import AppError
from domain.portfolio_codes import is_cash_asset, is_korean_stock, is_special_asset

TradeCurrency = Literal["KRW", "USD", "EUR", "JPY", "CNY", "HKD", "GBP", "AUD", "CAD", "CHF", "TWD", "VND"]
TradeNumber = Annotated[Decimal, Field(ge=0, le=1_000_000_000_000, decimal_places=8, allow_inf_nan=False)]
TaxRate = Annotated[Decimal, Field(ge=0, le=100, decimal_places=6, allow_inf_nan=False)]


def money_unit(currency: str) -> Decimal:
    return Decimal(1) if currency in {"KRW", "JPY", "VND"} else Decimal("0.01")


def withholding(gross: Decimal, rate: Decimal, currency: str) -> Decimal:
    return (gross * rate / 100).quantize(money_unit(currency), rounding=ROUND_DOWN)


class TradeError(AppError):
    status_code = 400


class TradeConflict(TradeError):
    status_code = 409


class TradeInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    stock_code: Annotated[str, Field(pattern=r"^[A-Z0-9][A-Z0-9._-]{0,23}$")]
    stock_name: Annotated[str, Field(min_length=1, max_length=80)]
    side: Literal["buy", "sell"]
    quantity: Annotated[Decimal, Field(gt=0, le=1_000_000_000, decimal_places=8, allow_inf_nan=False)]
    price: Annotated[TradeNumber, Field(gt=0)]
    fees: TradeNumber = Decimal(0)
    # 이전 클라이언트의 fees는 세금 포함 총비용이었다. 생략 시 추가 과세하지 않는다.
    tax_rate: TaxRate = Decimal(0)
    tax_amount: TradeNumber | None = None
    currency: TradeCurrency = "KRW"
    cost_fx_rate: Annotated[TradeNumber, Field(gt=0)] | None = None
    memo: Annotated[str, Field(max_length=500)] = ""

    @field_validator("stock_code", "currency", mode="before")
    @classmethod
    def uppercase(cls, value):
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("quantity", "price", "fees", "cost_fx_rate", "tax_rate", "tax_amount", mode="before")
    @classmethod
    def reject_boolean(cls, value):
        if isinstance(value, bool):
            raise ValueError("숫자에는 참/거짓을 사용할 수 없습니다.")
        return value


class TradeCreate(TradeInput):
    request_id: UUID
    expected_revision: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class TradeResult(BaseModel):
    stock_code: str
    stock_name: str
    side: Literal["buy", "sell"]
    currency: TradeCurrency
    cash_code: str
    quantity: FiniteFloat
    price: FiniteFloat
    fees: FiniteFloat
    commission: FiniteFloat | None = None
    tax_rate: FiniteFloat = 0
    tax_amount: FiniteFloat = 0
    gross_amount: FiniteFloat
    cash_change: FiniteFloat
    quantity_before: FiniteFloat
    quantity_after: FiniteFloat
    cash_before: FiniteFloat
    cash_after: FiniteFloat
    avg_price_before: FiniteFloat
    avg_price_after: FiniteFloat
    avg_price_currency: str
    cost_fx_rate: FiniteFloat | None
    memo: str


class TradePreview(TradeResult):
    revision: str


class TradeRecord(TradeResult):
    request_id: str
    created_at: str
    replayed: bool


def calculate_trade(trade: TradeInput, holding: dict | None, cash: dict | None) -> dict:
    code = trade.stock_code
    if is_cash_asset(code) or code.startswith(("FUT_", "SHORT_", "IDX_")):
        raise TradeError("현금·지수·선물·공매도는 현물 매매로 기록할 수 없습니다.")
    if (is_korean_stock(code) or is_special_asset(code)) and trade.currency != "KRW":
        raise TradeError("이 종목의 체결 통화는 KRW로 입력해 주세요.")
    holding = holding or {}
    quantity = Decimal(str(holding.get("quantity", 0)))
    avg_price = Decimal(str(holding.get("avg_price", 0)))
    balance = Decimal(str((cash or {}).get("quantity", 0)))
    if not all(n.is_finite() for n in (quantity, avg_price, balance)):
        raise TradeError("기존 잔고의 숫자를 확인해 주세요.")
    if quantity < 0 or holding.get("pair_long_code"):
        raise TradeError("롱숏 포지션은 개별 잔고 수정에서 관리해 주세요.")
    if holding and holding.get("currency", "KRW") != trade.currency:
        raise TradeError("체결 통화가 기존 종목 통화와 다릅니다.")
    if cash and cash.get("currency", "KRW") != trade.currency:
        raise TradeError("현금 항목의 통화 설정을 확인해 주세요.")
    unit = money_unit(trade.currency)
    money_limit = Decimal("1000000000000000") if unit == 1 else Decimal("1000000000000")
    if trade.fees != trade.fees.quantize(unit):
        raise TradeError("수수료·세금은 해당 통화의 최소 금액 단위로 입력해 주세요.")
    gross = (trade.quantity * trade.price).quantize(unit, rounding=ROUND_HALF_UP)
    if gross <= 0 or gross > money_limit:
        raise TradeError("체결 금액이 지원 범위를 벗어났습니다.")
    tax = trade.tax_amount if trade.tax_amount is not None else withholding(gross, trade.tax_rate, trade.currency)
    if tax != tax.quantize(unit):
        raise TradeError("세금은 해당 통화의 최소 금액 단위로 입력해 주세요.")
    fees = trade.fees + tax
    avg_currency = holding.get("avg_price_currency") or trade.currency
    if trade.side == "buy":
        change = -(gross + fees)
        after_quantity = quantity + trade.quantity
        if after_quantity > 1_000_000_000:
            raise TradeError("매수 후 보유 수량이 너무 큽니다.")
        if balance + change < 0:
            raise TradeError(f"{trade.currency} 현금이 부족합니다. 잔고를 먼저 확인해 주세요.")
        if quantity > 0 and avg_price <= 0:
            raise TradeError("추가 매수 전에 기존 평균 매입가를 입력해 주세요.")
        rate = Decimal(1)
        if quantity > 0 and avg_currency != trade.currency:
            if trade.cost_fx_rate is None:
                raise TradeError(f"평균 매입가 계산에 필요한 {trade.currency} → {avg_currency} 체결 환율을 입력해 주세요.")
            rate = trade.cost_fx_rate
        if quantity == 0:
            avg_currency = trade.currency
        after_avg = (quantity * avg_price - change * rate) / after_quantity
        if after_avg > 1_000_000_000_000:
            raise TradeError("계산된 평균 매입가가 너무 큽니다. 체결 환율을 확인해 주세요.")
        after_avg = after_avg.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
    else:
        if trade.quantity > quantity:
            raise TradeError("보유 수량보다 많이 매도할 수 없습니다.")
        if fees > gross:
            raise TradeError("수수료·세금이 매도 금액보다 큽니다.")
        after_quantity = quantity - trade.quantity
        after_avg = avg_price if after_quantity else Decimal(0)
        change = gross - fees
    after_cash = balance + change
    if abs(after_cash) > money_limit:
        raise TradeError("변경 후 현금 잔고가 지원 범위를 벗어났습니다.")
    # 기존 REAL 컬럼에 저장하면서 소수 수량이나 잔액을 잃는 거래는 차단한다.
    if Decimal(str(float(after_quantity))) != after_quantity or Decimal(str(float(after_cash))) != after_cash:
        raise TradeError("잔고의 저장 정밀도를 벗어났습니다. 입력 수량과 잔고를 확인해 주세요.")
    return {
        "stock_code": code, "stock_name": holding.get("stock_name") or trade.stock_name,
        "side": trade.side, "currency": trade.currency, "cash_code": f"CASH_{trade.currency}",
        "quantity": float(trade.quantity), "price": float(trade.price), "fees": float(fees),
        "commission": float(trade.fees), "tax_rate": float(trade.tax_rate), "tax_amount": float(tax),
        "gross_amount": float(gross), "cash_change": float(change),
        "quantity_before": float(quantity), "quantity_after": float(after_quantity),
        "cash_before": float(balance), "cash_after": float(after_cash),
        "avg_price_before": float(avg_price), "avg_price_after": float(after_avg),
        "avg_price_currency": avg_currency, "memo": trade.memo,
        "cost_fx_rate": float(trade.cost_fx_rate) if trade.cost_fx_rate else None,
    }
