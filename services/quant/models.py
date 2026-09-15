"""연구 요청의 공개 계약. 지원하지 않는 전략·파라미터는 거절한다."""

from datetime import date, datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


def completed_date():
    return datetime.now(timezone(timedelta(hours=9))).date() - timedelta(days=1)


class ResearchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, validate_default=True)
    strategy: Literal["preferred_switch"] = "preferred_switch"
    common: str = Field(pattern=r"^[0-9]{6}$")
    preferred: str = Field(pattern=r"^[0-9A-Z]{6}$")
    start: date
    end: date
    capital: float = Field(default=10_000_000, ge=10_000, le=1_000_000_000)
    window: int = Field(default=126, ge=20, le=252)
    entry_z: float = Field(default=2, ge=0.5, le=5)
    exit_z: float = Field(default=0.5, ge=-2, le=3)
    max_holding: int = Field(default=40, ge=1, le=252)
    commission_bps: float = Field(default=5, ge=0, le=100)
    sell_tax_bps: float = Field(default=20, ge=0, le=100)
    slippage_bps: float = Field(default=10, ge=0, le=200)
    participation: float = Field(default=0.01, gt=0, le=0.05)

    @model_validator(mode="after")
    def dates_and_thresholds(self):
        if not self.start < self.end <= completed_date():
            raise ValueError("시작일 이후부터 어제까지의 완료된 기간을 선택해 주세요.")
        if (self.end - self.start).days > 2922:
            raise ValueError("연구 기간은 최대 8년입니다.")
        if self.common == self.preferred or self.exit_z >= self.entry_z:
            raise ValueError("종목과 진입·복귀 기준을 확인해 주세요.")
        return self


class RunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_key: str = Field(min_length=8, max_length=100, pattern=r"^[a-zA-Z0-9_-]+$")
    config: ResearchConfig
