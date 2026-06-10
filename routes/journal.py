"""투자 일지 API (로드맵 신규 기능 ⑥).

매수/매도/메모 판단의 '이유'를 기록하고, 나중에 당시 가격 → 현재 가격
수익률과 함께 복기한다 — 가치투자 서비스의 차별화 포인트.

* GET    /api/portfolio/journal?stock_code=  — 목록(+현재가·작성 후 수익률)
* POST   /api/portfolio/journal              — 작성(서버가 당시 가격/목표가 스냅샷)
* PATCH  /api/portfolio/journal/{entry_id}   — note 만 수정
* DELETE /api/portfolio/journal/{entry_id}   — 삭제

작성 시점 스냅샷 정책:
* price_at_entry  — services.stock_quotes.get_quote_snapshot (60초 캐시 +
  dead-stock 캐시가 있는 공용 읽기 경로) best-effort. 실패하면 NULL.
* target_price_at_entry — user_portfolio.target_price 의 저장값. 이 컬럼은
  수동 override 또는 목표가 수식의 '마지막 계산값'이 저장 시점에 이미
  채워져 있어(routes/portfolio._resolve_target_formula_price) 수식 머신을
  다시 돌리지 않고 싸게 스냅샷한다. 미보유/미설정/비활성(target_price_
  disabled=1)이면 NULL.

세션 인증은 routes/portfolio_risk.py 와 같은 패턴(_require_user). 소유권은
저장소 쿼리의 google_sub 조건으로 강제 — 남의 항목 id 는 404 로 보인다.
"""

from __future__ import annotations

import asyncio
import logging
import re

from fastapi import APIRouter, HTTPException, Query, Request

from deps import get_current_user
from repositories import journal as journal_repo
from services import stock_quotes

logger = logging.getLogger(__name__)
router = APIRouter()

# 포트폴리오 코드 공간과 동일하게 영숫자 + . _ - (예: 005930, AAPL, 0005.HK,
# CASH_KRW). 대문자 정규화 후 검사한다.
_STOCK_CODE_RE = re.compile(r"^[A-Z0-9._\-]{1,24}$")
NOTE_MAX_LENGTH = 2000


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


def _validate_stock_code(raw) -> str:
    code = str(raw or "").strip().upper()
    if not _STOCK_CODE_RE.match(code):
        raise HTTPException(status_code=400, detail="stock_code 형식이 올바르지 않습니다.")
    return code


def _validate_note(raw) -> str:
    note = str(raw or "").strip()
    if not note:
        raise HTTPException(status_code=400, detail="note(판단 이유)를 입력해 주세요.")
    if len(note) > NOTE_MAX_LENGTH:
        raise HTTPException(
            status_code=400, detail=f"note 는 {NOTE_MAX_LENGTH}자 이하여야 합니다."
        )
    return note


def _validate_quantity(raw) -> float | None:
    if raw in (None, ""):
        return None
    try:
        quantity = float(raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="quantity 값이 올바르지 않습니다.")
    if not (quantity > 0):
        raise HTTPException(status_code=400, detail="quantity 는 0보다 커야 합니다.")
    return quantity


async def _capture_price(stock_code: str) -> float | None:
    """작성 시점 현재가 — 공용 시세 읽기 경로 best-effort, 실패하면 NULL."""
    try:
        quote = await stock_quotes.get_quote_snapshot(stock_code)
    except Exception:
        logger.warning("투자 일지 가격 스냅샷 실패: %s", stock_code, exc_info=True)
        return None
    price = quote.get("price") if isinstance(quote, dict) else None
    return float(price) if price not in (None, "") else None


async def _capture_target_price(google_sub: str, stock_code: str) -> float | None:
    """보유 행의 저장된 목표가(override 또는 수식 마지막 계산값) 스냅샷."""
    holding = await journal_repo.get_holding_snapshot(google_sub, stock_code)
    if not holding or holding.get("target_price_disabled"):
        return None
    target = holding.get("target_price")
    return float(target) if target not in (None, "") else None


async def _current_prices(stock_codes: set[str]) -> dict[str, float]:
    """목록 화면용 현재가 — 종목별 best-effort 병렬 조회(캐시 우선)."""
    codes = sorted(stock_codes)
    prices = await asyncio.gather(*(_capture_price(code) for code in codes))
    return {code: price for code, price in zip(codes, prices) if price is not None}


@router.get("/api/portfolio/journal")
async def list_journal(request: Request, stock_code: str | None = Query(None)):
    user = _require_user(await get_current_user(request))
    code = _validate_stock_code(stock_code) if stock_code else None

    entries = await journal_repo.list_entries(user["google_sub"], code)
    prices = await _current_prices({e["stock_code"] for e in entries})
    for entry in entries:
        current = prices.get(entry["stock_code"])
        entry["current_price"] = current
        entry_price = entry.get("price_at_entry")
        # 작성 후 수익률(%) — 당시 가격이 있을 때만 계산.
        entry["since_entry_return_pct"] = (
            round((current - entry_price) / entry_price * 100, 2)
            if current is not None and entry_price not in (None, 0)
            else None
        )
    return {"entries": entries}


@router.post("/api/portfolio/journal")
async def create_journal_entry(request: Request):
    user = _require_user(await get_current_user(request))
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="잘못된 요청 본문입니다.")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="잘못된 요청 본문입니다.")

    stock_code = _validate_stock_code(payload.get("stock_code"))
    entry_type = str(payload.get("entry_type") or "").strip().lower()
    if entry_type not in journal_repo.ENTRY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"entry_type 은 {', '.join(journal_repo.ENTRY_TYPES)} 중 하나여야 합니다.",
        )
    note = _validate_note(payload.get("note"))
    quantity = _validate_quantity(payload.get("quantity"))

    # 작성 시점 스냅샷 — 가격은 best-effort(NULL 허용), 목표가는 보유 행의
    # 저장값. 종목명은 보유 행 우선, 없으면 클라이언트가 보낸 표시명.
    price_at_entry = await _capture_price(stock_code)
    holding = await journal_repo.get_holding_snapshot(user["google_sub"], stock_code)
    target_price_at_entry = await _capture_target_price(user["google_sub"], stock_code)
    stock_name = (holding or {}).get("stock_name") or str(payload.get("stock_name") or "").strip() or None

    entry = await journal_repo.insert_entry(
        user["google_sub"],
        stock_code,
        entry_type,
        note,
        stock_name=stock_name,
        price_at_entry=price_at_entry,
        quantity=quantity,
        target_price_at_entry=target_price_at_entry,
    )
    return entry


@router.patch("/api/portfolio/journal/{entry_id}")
async def update_journal_note(request: Request, entry_id: int):
    user = _require_user(await get_current_user(request))
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="잘못된 요청 본문입니다.")
    note = _validate_note(payload.get("note") if isinstance(payload, dict) else None)

    # 소유권: google_sub 조건이 걸린 UPDATE — 남의 항목/없는 항목은 404.
    updated = await journal_repo.update_note(user["google_sub"], entry_id, note)
    if not updated:
        raise HTTPException(status_code=404, detail="일지 항목을 찾을 수 없습니다.")
    return await journal_repo.get_entry(user["google_sub"], entry_id)


@router.delete("/api/portfolio/journal/{entry_id}")
async def delete_journal_entry(request: Request, entry_id: int):
    user = _require_user(await get_current_user(request))
    deleted = await journal_repo.delete_entry(user["google_sub"], entry_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="일지 항목을 찾을 수 없습니다.")
    return {"ok": True}
