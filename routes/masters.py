"""투자 대가 전략 HTTP API.

두 엔드포인트:

* ``GET /api/masters/strategies`` — 전략 카탈로그 전체(철학·원칙·장단점·
  기본 배분·리밸런싱 아이디어). UI 는 이 spec 으로 카드·비교표를 그린다.
* ``POST /api/masters/simulate`` — 지정한 대가 1명(strategy_id)과 사용자의
  위험 성향·투자 기간·선호 자산군(profile), 선택적 투자금액(amount)을 받아
  상품(국내 상장 ETF) 단위의 참고용 포트폴리오를 계산한다. 입력이 JSON
  객체라 POST 를 쓴다(스크리너 run 과 같은 이유).

인증은 선택(공개 교육 콘텐츠, 사용자별 비밀 없음) — ``routes/screener.py``
와 같은 방식으로 세션 워밍만 한다. 응답은 누구에게나 동일하다.

시뮬레이션 응답에는 항상 "투자 조언이 아닌 참고용" disclaimer 가 포함된다.
"""

from __future__ import annotations

from fastapi import APIRouter, Body, Request

from deps import get_current_user
from services import investment_masters

router = APIRouter(prefix="/api/masters", tags=["masters"])


@router.get("/strategies")
async def get_strategies(request: Request):
    await get_current_user(request)
    return investment_masters.get_catalog_payload()


@router.post("/simulate")
async def simulate(request: Request, payload: dict = Body(default={})):
    await get_current_user(request)
    # MastersError(400)는 AppError 핸들러가 상태코드로 매핑한다 — 여기서
    # HTTPException 으로 다시 감싸지 않는다(스크리너와 동일 정책).
    return await investment_masters.build_portfolio(payload if isinstance(payload, dict) else {})
