"""사용자별 수익 분해와 투자 논거 관리 API."""

from datetime import date, timedelta
from typing import Annotated, Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field, StrictBool, field_validator, model_validator

from deps import get_current_user
from repositories import investment_insights as repo
from repositories import journal
from services.portfolio import attribution, theses
from services.portfolio.time_windows import today_kst_date

router = APIRouter()
Code = Annotated[str, Field(pattern=r"^[A-Z0-9._\-]{1,24}$")]


class ThesisInput(BaseModel):
    stock_code: Code
    thesis: Annotated[str, Field(min_length=1, max_length=2000)]
    invalidation: Annotated[str, Field(min_length=1, max_length=2000)]
    metric: Literal["manual", "revenue_growth_pct", "operating_margin_pct", "debt_ratio_pct"] = "manual"
    operator: Literal["lt", "lte", "gt", "gte"] = "lt"
    threshold: Annotated[float, Field(allow_inf_nan=False, ge=-1e9, le=1e9)] | None = None
    deadline: date | None = None
    evidence_url: Annotated[str, Field(max_length=2000)] | None = None
    journal_entry_id: Annotated[int, Field(gt=0)] | None = None
    archived: StrictBool = False

    @field_validator("threshold", mode="before")
    @classmethod
    def numeric_threshold(cls, value):
        if isinstance(value, bool):
            raise ValueError("기준값은 숫자로 입력해 주세요.")
        return value

    @field_validator("thesis", "invalidation")
    @classmethod
    def meaningful_text(cls, value):
        if not value.strip():
            raise ValueError("투자 논거와 반증 조건을 입력해 주세요.")
        return value.strip()

    @field_validator("evidence_url")
    @classmethod
    def safe_url(cls, value):
        if not value:
            return None
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("근거 링크는 http 또는 https 주소여야 합니다.")
        return value

    @model_validator(mode="after")
    def needs_threshold(self):
        if self.metric != "manual" and self.threshold is None:
            raise ValueError("자동 점검의 기준값을 입력해 주세요.")
        return self


class IncomeInput(BaseModel):
    date: date
    stock_code: Annotated[str, Field(pattern=r"^[A-Z0-9._\-]{0,24}$")] = ""
    kind: Literal["dividend", "fee"]
    amount_krw: Annotated[float, Field(gt=0, le=1e12, allow_inf_nan=False)]
    memo: Annotated[str, Field(max_length=500)] = ""

    @field_validator("date")
    @classmethod
    def received_date(cls, value):
        if value > today_kst_date():
            raise ValueError("실제 반영된 과거 날짜만 입력해 주세요.")
        return value

    @field_validator("amount_krw", mode="before")
    @classmethod
    def no_boolean(cls, value):
        if isinstance(value, bool):
            raise ValueError("금액은 숫자로 입력해 주세요.")
        return value


async def owner(request: Request) -> str:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "로그인이 필요합니다.")
    return user["google_sub"]


@router.get("/api/portfolio/attribution")
async def get_attribution(request: Request, start: date | None = Query(None), end: date | None = Query(None)):
    user = await owner(request)
    ending = end or today_kst_date()
    beginning = start or (ending-timedelta(days=30))
    try:
        return await attribution.build_attribution(user, beginning.isoformat(), ending.isoformat())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/api/portfolio/income-events", status_code=201)
async def add_income(request: Request, payload: IncomeInput):
    return {"id": await repo.add_income(await owner(request), payload.model_dump(mode="json"))}


@router.delete("/api/portfolio/income-events/{event_id}")
async def remove_income(request: Request, event_id: int):
    if not await repo.delete_income(await owner(request), event_id):
        raise HTTPException(404, "내역을 찾을 수 없습니다.")
    return {"ok": True}


@router.get("/api/portfolio/theses")
async def list_theses(request: Request):
    return await theses.review_theses(await owner(request))


async def store_thesis(user: str, payload: ThesisInput, thesis_id: int | None = None):
    if payload.journal_entry_id:
        entry = await journal.get_entry(user, payload.journal_entry_id)
        if not entry or entry["stock_code"] != payload.stock_code:
            raise HTTPException(404, "같은 종목의 본인 투자일지만 연결할 수 있습니다.")
    if thesis_id is None and len(await repo.list_theses(user)) >= 200:
        raise HTTPException(400, "투자 논거는 최대 200개까지 보관할 수 있습니다.")
    result = await repo.save_thesis(user, payload.model_dump(mode="json"), thesis_id)
    if result is None:
        raise HTTPException(404, "투자 논거를 찾을 수 없습니다.")
    return {"id": result}


@router.post("/api/portfolio/theses", status_code=201)
async def create_thesis(request: Request, payload: ThesisInput):
    return await store_thesis(await owner(request), payload)


@router.put("/api/portfolio/theses/{thesis_id}")
async def update_thesis(request: Request, thesis_id: int, payload: ThesisInput):
    return await store_thesis(await owner(request), payload, thesis_id)


@router.get("/api/portfolio/theses/{thesis_id}/history")
async def thesis_history(request: Request, thesis_id: int):
    user = await owner(request)
    if not any(item["id"] == thesis_id for item in await repo.list_theses(user)):
        raise HTTPException(404, "투자 논거를 찾을 수 없습니다.")
    return {"items": await repo.check_history(user, thesis_id)}
