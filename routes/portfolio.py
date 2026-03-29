import asyncio
import logging

from fastapi import APIRouter, Body, HTTPException, Query, Request

import cache
import stock_price
from deps import RECENT_QUOTES_SEMAPHORE, get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


async def _enrich_with_quotes(items: list[dict]) -> list[dict]:
    async def fetch(item: dict) -> dict:
        enriched = dict(item)
        try:
            async with RECENT_QUOTES_SEMAPHORE:
                enriched["quote"] = await stock_price.fetch_quote_snapshot(item["stock_code"])
        except Exception as exc:
            logger.warning("포트폴리오 현재가 조회 실패(%s): %s", item.get("stock_code"), exc)
            enriched["quote"] = {}
        return enriched

    return await asyncio.gather(*(fetch(item) for item in items))


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


@router.get("/api/portfolio")
async def get_portfolio(request: Request):
    user = _require_user(await get_current_user(request))
    items = await cache.get_portfolio(user["google_sub"])
    return await _enrich_with_quotes(items)


@router.put("/api/portfolio/{stock_code}")
async def save_portfolio_item(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))

    stock_name = str(payload.get("stock_name") or "").strip()
    if not stock_name:
        resolved = await cache.resolve_stock_name(stock_code)
        if resolved:
            stock_name = resolved
        else:
            raise HTTPException(status_code=400, detail="종목명을 입력해 주세요.")

    quantity = payload.get("quantity")
    avg_price = payload.get("avg_price")
    if quantity is None or avg_price is None:
        raise HTTPException(status_code=400, detail="수량과 매입가를 입력해 주세요.")

    try:
        quantity = int(quantity)
        avg_price = float(avg_price)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="수량과 매입가는 숫자여야 합니다.")

    if quantity <= 0:
        raise HTTPException(status_code=400, detail="수량은 1 이상이어야 합니다.")
    if avg_price < 0:
        raise HTTPException(status_code=400, detail="매입가는 0 이상이어야 합니다.")

    result = await cache.save_portfolio_item(user["google_sub"], stock_code, stock_name, quantity, avg_price)
    return {"ok": True, **result}


@router.delete("/api/portfolio/{stock_code}")
async def delete_portfolio_item(stock_code: str, request: Request):
    user = _require_user(await get_current_user(request))
    await cache.delete_portfolio_item(user["google_sub"], stock_code)
    return {"ok": True}


@router.put("/api/portfolio/order")
async def save_portfolio_order(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    stock_codes = payload.get("stock_codes")
    if not isinstance(stock_codes, list) or not stock_codes:
        raise HTTPException(status_code=400, detail="정렬할 종목 목록이 필요합니다.")
    codes = [str(c).strip() for c in stock_codes if str(c).strip()]
    await cache.save_portfolio_order(user["google_sub"], codes)
    return {"ok": True}


@router.get("/api/portfolio/resolve-name")
async def resolve_name(code: str = Query(..., min_length=1)):
    name = await cache.resolve_stock_name(code.strip())
    return {"stock_code": code.strip(), "stock_name": name}
