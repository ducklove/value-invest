"""공통 예외 계층과 HTTP 매핑 (재설계 계획 Phase 2-5).

외부 서비스·DB 실패를 의미 있는 타입으로 분류해, 라우트마다 흩어진 광역
``except Exception`` 과 맨몸 500 응답을 점진적으로 대체한다.

설계 노트:

- ``AppError`` 가 ``RuntimeError`` 를 상속하는 것은 의도된 호환 장치다.
  기존 외부 클라이언트 예외(``KISProxyError``, ``ClosePriceClientError``)가
  이 계층으로 재베이스되어도, 그 예외들을 ``except RuntimeError`` 로 잡던
  기존 호출부가 그대로 동작한다.
- 핸들러 등록은 ``register_exception_handlers(app)`` 한 곳에서 한다.
  ``AppError`` 베이스 하나만 등록하면 FastAPI 가 서브클래스까지 잡고,
  각 예외의 ``status_code`` / ``default_detail`` 클래스 속성으로 응답을
  만든다. 5xx 매핑은 기존 ``_RequestLatencyMiddleware`` 의 오류 기록
  대상에 자연히 포함된다.
- 라우트가 직접 ``HTTPException`` 으로 변환하던 경로는 그대로 둬도 된다.
  이 계층은 "아무도 잡지 않아 맨몸 500 이 되던" 실패에 의미 있는 상태
  코드와 한국어 detail 을 주는 안전망이다.
"""

from __future__ import annotations


class AppError(RuntimeError):
    """앱 정의 오류의 공통 베이스 — 잡히지 않으면 HTTP 응답으로 매핑된다."""

    status_code = 500
    default_detail = "서버 내부 오류가 발생했습니다."


class ExternalServiceError(AppError):
    """외부 서비스(KIS proxy, finance-pi, 스크레이프 소스 등) 호출 실패."""

    status_code = 502
    default_detail = "외부 서비스 호출에 실패했습니다."


class RateLimitError(ExternalServiceError):
    """업스트림 또는 자체 정책의 호출 한도 초과."""

    status_code = 429
    default_detail = "요청이 너무 많습니다. 잠시 후 다시 시도해 주세요."


class DBError(AppError):
    """데이터베이스 쓰기/트랜잭션 실패."""

    status_code = 500
    default_detail = "데이터베이스 작업에 실패했습니다."


def register_exception_handlers(app) -> None:
    """FastAPI 앱에 AppError 계층 → JSON 응답 매핑을 등록한다."""
    from fastapi import Request
    from fastapi.responses import JSONResponse

    async def _handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": str(exc) or exc.default_detail},
        )

    app.add_exception_handler(AppError, _handle_app_error)
