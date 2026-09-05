"""저장 전에 적용하는 포트폴리오 입력 계약."""

from datetime import date
from typing import Annotated, Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field, StrictBool, ValidationError, field_validator

Quantity = Annotated[float, Field(allow_inf_nan=False, ge=-1_000_000_000, le=1_000_000_000)]
Price = Annotated[float, Field(allow_inf_nan=False, ge=0, le=1_000_000_000_000)]


class HoldingInput(BaseModel):
    quantity: Quantity
    avg_price: Price
    target_price: Price | None = None
    target_price_disabled: StrictBool = False
    created_at: date | None = None

    @field_validator("quantity", "avg_price", "target_price", mode="before")
    @classmethod
    def reject_boolean_number(cls, value):
        if isinstance(value, bool):
            raise ValueError("숫자 항목에는 참/거짓을 사용할 수 없습니다.")
        return value

    @field_validator("quantity")
    @classmethod
    def nonzero_quantity(cls, value):
        if value == 0:
            raise ValueError("수량은 0이 아닌 값이어야 합니다.")
        return value

    @field_validator("created_at", mode="before")
    @classmethod
    def calendar_date(cls, value):
        if value in (None, ""):
            return None
        if isinstance(value, date):
            return value
        if not isinstance(value, str) or len(value) != 10:
            raise ValueError("날짜는 YYYY-MM-DD 형식이어야 합니다.")
        return date.fromisoformat(value)

    @field_validator("target_price", mode="before")
    @classmethod
    def empty_target(cls, value):
        return None if isinstance(value, str) and not value.strip() else value


class CashflowInput(BaseModel):
    type: Literal["deposit", "withdrawal"]
    amount: Annotated[float, Field(allow_inf_nan=False, gt=0, le=1_000_000_000_000)]
    date: str = ""
    memo: Annotated[str, Field(max_length=500)] | None = None

    @field_validator("amount", mode="before")
    @classmethod
    def amount_is_number(cls, value):
        return HoldingInput.reject_boolean_number(value)

    @field_validator("amount")
    @classmethod
    def whole_won(cls, value):
        if not value.is_integer():
            raise ValueError("원화 입출금은 정수 금액으로 입력해 주세요.")
        return value

    @field_validator("date")
    @classmethod
    def valid_date(cls, value):
        HoldingInput.calendar_date(value)
        return value


def validate_input(model: type[BaseModel], payload: dict) -> dict:
    try:
        validated = model.model_validate(payload).model_dump(exclude_unset=True)
    except ValidationError as exc:
        fields = ", ".join(".".join(map(str, error["loc"])) for error in exc.errors())
        raise HTTPException(status_code=400, detail=f"입력값을 확인해 주세요: {fields}") from exc
    return {**payload, **validated}
