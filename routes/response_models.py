"""핵심 조회 API의 응답 계약. 점진적 전환 중에도 기존 확장 필드는 보존한다."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat


class ExtensibleResponse(BaseModel):
    model_config = ConfigDict(extra="allow")


class QuoteResponse(ExtensibleResponse):
    # 배치 조회의 빈 객체는 해당 자산의 시세 없음이며 0원과 다르다.
    price: FiniteFloat | None = None
    change: FiniteFloat | None = None
    change_pct: FiniteFloat | None = None
    currency: str | None = None


class HoldingResponse(ExtensibleResponse):
    stock_code: str
    stock_name: str
    quantity: FiniteFloat
    avg_price: FiniteFloat
    avg_price_currency: str | None = None
    currency: str | None = None
    group_name: str | None = None
    benchmark_code: str | None = None
    trailing_dps: FiniteFloat | None = None
    tags: list[str] = Field(default_factory=list)
    quote: QuoteResponse | None = None


class NavPoint(BaseModel):
    date: str
    nav: FiniteFloat
    return_nav: FiniteFloat | None = None
    return_factor: FiniteFloat = 1
    distribution_per_unit: FiniteFloat = 0
    total_value: FiniteFloat
    total_invested: FiniteFloat
    total_units: FiniteFloat
    fx_usdkrw: FiniteFloat | None


class CashflowDelta(BaseModel):
    id: int
    type: Literal["deposit", "withdrawal", "distribution"]
    amount: FiniteFloat
    signed_amount: FiniteFloat
    nav_at_time: FiniteFloat | None
    units_change: FiniteFloat | None
    created_at: str
    applied_snapshot_date: str | None = None
    cash_code: str = "CASH_KRW"


class PreviousDayResponse(BaseModel):
    date: str | None
    total_value: FiniteFloat | None
    fx_usdkrw: FiniteFloat | None
    nav: FiniteFloat | None
    return_nav: FiniteFloat | None = None
    return_factor: FiniteFloat = 1
    stock_values: dict[str, FiniteFloat]
    today_net_cashflow: FiniteFloat
    today_cashflows_by_stock: dict[str, FiniteFloat]
    today_cashflows: list[CashflowDelta]


class PeriodStartResponse(ExtensibleResponse):
    date: str | None = None
    total_value: FiniteFloat | None = None
    nav: FiniteFloat | None = None
    total_invested: FiniteFloat | None = None
    total_units: FiniteFloat | None = None
    fx_usdkrw: FiniteFloat | None = None
    stock_values: dict[str, FiniteFloat]
    net_cashflow: FiniteFloat | None = None
    cashflows_by_stock: dict[str, FiniteFloat] | None = None


class IntradayPoint(BaseModel):
    ts: str
    total_value: FiniteFloat


class IncomeEvent(ExtensibleResponse):
    id: int
    date: str
    stock_code: str
    kind: Literal["dividend", "fee"]
    amount_krw: FiniteFloat


class AttributionComponent(BaseModel):
    key: Literal["external_flow", "distribution", "price", "fx", "combined", "dividend", "fee", "unclassified"]
    label: str
    amount: FiniteFloat


class AttributionStock(BaseModel):
    stock_code: str
    stock_name: str
    price: FiniteFloat
    fx: FiniteFloat
    combined: FiniteFloat
    intervals: int
    total: FiniteFloat


class AttributionCoverage(BaseModel):
    eligible_intervals: int
    examined_intervals: int
    issues: dict[str, int]


class AttributionAvailable(BaseModel):
    available: Literal[True]
    requested_start: str
    requested_end: str
    baseline_mode: Literal["before_period", "first_in_period"]
    baseline_date: str
    ending_date: str
    starting_value: FiniteFloat
    ending_value: FiniteFloat
    value_change: FiniteFloat
    investment_pnl: FiniteFloat
    components: list[AttributionComponent]
    reconciliation_error: FiniteFloat
    stocks: list[AttributionStock]
    coverage: AttributionCoverage
    income_events: list[IncomeEvent]
    notes: list[str]


class AttributionUnavailable(BaseModel):
    available: Literal[False]
    message: str
    income_events: list[IncomeEvent]


AttributionResponse = Annotated[AttributionAvailable | AttributionUnavailable, Field(discriminator="available")]


class ThesisCheck(ExtensibleResponse):
    status: Literal["breached", "due", "unknown", "monitoring", "manual", "archived", "changed"]
    value: FiniteFloat | None = None
    threshold: FiniteFloat | None = None
    year: int | None = None
    stale: bool | None = None
    due: bool | None = None
    event_id: int | None = None


class ThesisResponse(ExtensibleResponse):
    id: int
    stock_code: str
    thesis: str
    invalidation: str
    metric: str
    operator: str
    threshold: FiniteFloat | None
    check: ThesisCheck


class ThesesResponse(BaseModel):
    items: list[ThesisResponse]
    checked_at: str
    metrics: dict[str, str]
