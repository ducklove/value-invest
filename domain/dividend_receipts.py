"""배당금 수취 입력과 세후 현금 계산. 세율은 수정 가능한 입력 기본값이다."""

from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, field_validator

from domain.portfolio_codes import is_special_asset
from domain.portfolio_trades import TaxRate, TradeCurrency, TradeError, TradeNumber, money_unit, withholding

DividendCountry = Literal["KR", "US", "CN", "HK", "OTHER"]
DIVIDEND_TAX_RATES = {"KR": Decimal("15.4"), "US": Decimal("15"), "CN": Decimal("14.4"), "HK": Decimal("15.4"), "OTHER": Decimal(0)}


def receipt_today() -> date:
    return datetime.now(timezone(timedelta(hours=9))).date()


class DividendInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    stock_code: Annotated[str, Field(pattern=r"^[A-Z0-9][A-Z0-9._-]{0,23}$")]
    stock_name: Annotated[str, Field(min_length=1, max_length=80)]
    currency: TradeCurrency = "KRW"
    country: DividendCountry = "KR"
    received_date: date
    gross_amount: Annotated[TradeNumber, Field(gt=0)] | None = None
    quantity: Annotated[TradeNumber, Field(gt=0)] | None = None
    amount_per_share: Annotated[TradeNumber, Field(gt=0)] | None = None
    tax_rate: TaxRate | None = None
    tax_amount: TradeNumber | None = None
    fx_rate: Annotated[TradeNumber, Field(gt=0)] | None = None
    source_key: Annotated[str, Field(pattern=r"^[A-Z0-9][A-Z0-9._-]{0,23}:(estimated|ex_date):\d{4}-\d{2}-\d{2}$")] | None = None
    memo: Annotated[str, Field(max_length=500)] = ""

    @field_validator("stock_code", "currency", "country", mode="before")
    @classmethod
    def uppercase(cls, value):
        return value.strip().upper() if isinstance(value, str) else value

    @field_validator("gross_amount", "quantity", "amount_per_share", "tax_rate", "tax_amount", "fx_rate", mode="before")
    @classmethod
    def reject_boolean(cls, value):
        if isinstance(value, bool):
            raise ValueError("금액과 세율은 숫자로 입력해 주세요.")
        return value

    @field_validator("received_date")
    @classmethod
    def not_future(cls, value):
        if value > receipt_today():
            raise ValueError("실제 입금된 날짜를 입력해 주세요. 미래 배당은 수취할 수 없습니다.")
        return value


class DividendCreate(DividendInput):
    request_id: UUID
    expected_revision: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class DividendResult(BaseModel):
    stock_code: str
    stock_name: str
    currency: TradeCurrency
    country: DividendCountry
    received_date: str
    applied_date: str
    gross_amount: FiniteFloat
    quantity: FiniteFloat | None
    amount_per_share: FiniteFloat | None
    tax_rate: FiniteFloat
    tax_amount: FiniteFloat
    net_amount: FiniteFloat
    fx_rate: FiniteFloat
    amount_krw: FiniteFloat
    cash_code: str
    cash_before: FiniteFloat
    cash_after: FiniteFloat
    source_key: str | None
    memo: str


class DividendPreview(DividendResult):
    revision: str


class DividendRecord(DividendResult):
    request_id: str
    created_at: str
    income_event_id: int | None
    replayed: bool


def calculate_dividend(receipt: DividendInput, cash: dict | None) -> dict:
    if is_special_asset(receipt.stock_code) or receipt.stock_code.startswith(("FUT_", "SHORT_", "IDX_")):
        raise TradeError("배당을 지급한 주식·ETF 종목을 선택해 주세요.")
    if receipt.source_key and not receipt.source_key.startswith(receipt.stock_code + ":"):
        raise TradeError("선택한 배당 스케줄과 종목이 다릅니다.")
    unit = money_unit(receipt.currency)
    if receipt.gross_amount is not None:
        if receipt.quantity is not None or receipt.amount_per_share is not None:
            raise TradeError("세전 총액 또는 주당 배당금·수량 중 한 방식으로 입력해 주세요.")
        gross = receipt.gross_amount
        if gross != gross.quantize(unit):
            raise TradeError("세전 총액은 해당 통화의 최소 금액 단위로 입력해 주세요.")
    else:
        if receipt.quantity is None or receipt.amount_per_share is None:
            raise TradeError("주당 배당금과 배당 대상 수량을 입력해 주세요.")
        gross = (receipt.quantity * receipt.amount_per_share).quantize(unit, rounding=ROUND_HALF_UP)
    if not 0 < gross <= 1_000_000_000_000:
        raise TradeError("배당 금액이 지원 범위를 벗어났습니다.")
    rate = receipt.tax_rate if receipt.tax_rate is not None else DIVIDEND_TAX_RATES[receipt.country]
    tax = receipt.tax_amount if receipt.tax_amount is not None else withholding(gross, rate, receipt.currency)
    if tax != tax.quantize(unit) or tax > gross:
        raise TradeError("공제 금액의 통화 단위를 확인하고 세전 금액 이하로 입력해 주세요.")
    net = gross - tax
    fx_rate = Decimal(1) if receipt.currency == "KRW" else receipt.fx_rate
    if fx_rate is None:
        raise TradeError("배당 수익 분류에 사용할 수취 환율(1 외화당 KRW)을 입력해 주세요.")
    amount_krw = (net * fx_rate).quantize(Decimal(1), rounding=ROUND_HALF_UP)
    if amount_krw > 1_000_000_000_000:
        raise TradeError("원화 환산 금액이 너무 큽니다. 환율을 확인해 주세요.")
    cash = cash or {}
    before = Decimal(str(cash.get("quantity", 0)))
    if not before.is_finite() or cash.get("currency", receipt.currency) != receipt.currency:
        raise TradeError("기존 현금 잔고·통화를 확인해 주세요.")
    after = before + net
    limit = Decimal("1000000000000000") if unit == 1 else Decimal("1000000000000")
    if abs(after) > limit or Decimal(str(float(after))) != after:
        raise TradeError("변경 후 현금 잔고가 저장 범위·정밀도를 벗어났습니다.")
    return {
        "stock_code": receipt.stock_code, "stock_name": receipt.stock_name,
        "currency": receipt.currency, "country": receipt.country,
        "received_date": receipt.received_date.isoformat(), "applied_date": receipt_today().isoformat(),
        "gross_amount": float(gross), "quantity": float(receipt.quantity) if receipt.quantity is not None else None,
        "amount_per_share": float(receipt.amount_per_share) if receipt.amount_per_share is not None else None,
        "tax_rate": float(rate), "tax_amount": float(tax), "net_amount": float(net),
        "fx_rate": float(fx_rate), "amount_krw": float(amount_krw), "cash_code": f"CASH_{receipt.currency}",
        "cash_before": float(before), "cash_after": float(after), "source_key": receipt.source_key, "memo": receipt.memo,
    }
