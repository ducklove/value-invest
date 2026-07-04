"""투자 대가 전략 HTTP API.

두 엔드포인트:

* ``GET /api/masters/strategies`` — 전략 카탈로그 전체(철학·원칙·장단점·
  기본 배분·리밸런싱 아이디어). UI 는 이 spec 으로 카드·비교표를 그린다.
* ``POST /api/masters/simulate`` — 지정한 대가 1명(strategy_id)과 사용자의
  위험 성향·투자 기간·선호 자산군(profile), 선택적 투자금액(amount)을 받아
  상품(국내 상장 ETF) 단위의 참고용 포트폴리오를 계산한다. 입력이 JSON
  객체라 POST 를 쓴다(스크리너 run 과 같은 이유).

* ``POST /api/masters/review`` — 지정한 대가 1명의 관점으로 사용자의 실제
  포트폴리오를 LLM 으로 진단한다(로그인 필수 + 레이트리밋 + AI 예산 가드).

카탈로그/시뮬레이션은 인증 선택(공개 교육 콘텐츠) — ``routes/screener.py``
와 같은 방식으로 세션 워밍만 한다. 응답은 누구에게나 동일하다.

모든 응답에는 "투자 조언이 아닌 참고용" disclaimer 가 포함된다.
"""

from __future__ import annotations

import os

from fastapi import APIRouter, Body, HTTPException, Request

from core.rate_limit import enforce_rate_limit
from deps import get_current_user
from services import ai_client, investment_masters
from services.portfolio import masters_review

router = APIRouter(prefix="/api/masters", tags=["masters"])

# LLM 호출이라 스크리너류보다 훨씬 빡빡하게 — 포트폴리오 AI 분석(6/10분)보다
# 약간 낮은 기본값. 예산 가드(enforce_budget_caps→429)는 ai_client 쪽에 별도.
MASTERS_REVIEW_BURST_LIMIT = int(os.environ.get("MASTERS_REVIEW_BURST_LIMIT", "4"))
MASTERS_REVIEW_BURST_WINDOW_SECONDS = int(os.environ.get("MASTERS_REVIEW_BURST_WINDOW_SECONDS", "600"))


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


@router.post("/review")
async def review(request: Request, payload: dict = Body(default={})):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    enforce_rate_limit(
        request,
        scope="masters_review",
        user=user,
        max_requests=MASTERS_REVIEW_BURST_LIMIT,
        window_seconds=MASTERS_REVIEW_BURST_WINDOW_SECONDS,
        detail="진단 요청이 너무 많습니다. 잠시 후 다시 시도해 주세요.",
    )
    try:
        return await masters_review.generate_review(payload if isinstance(payload, dict) else {}, user)
    except masters_review.EmptyPortfolioError:
        raise HTTPException(status_code=400, detail="진단할 포트폴리오가 없습니다. 먼저 보유 종목을 등록해 주세요.")
    except ai_client.MissingOpenRouterKeyError:
        raise HTTPException(status_code=500, detail="AI API 키가 설정되지 않았습니다.")
    except ai_client.OpenRouterError as exc:
        # BudgetExceededError(429)는 AppError 핸들러가 처리하므로 여기 오지 않는다.
        raise HTTPException(status_code=502, detail=f"AI 호출에 실패했습니다: {exc}")
