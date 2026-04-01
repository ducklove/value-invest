from fastapi import APIRouter, Body, HTTPException, Query, Request

import cache
import stock_price
from deps import get_current_user

router = APIRouter()


@router.get("/api/market-summary")
async def market_summary(codes: str = ""):
    import market_indicators

    code_list = (
        [c.strip() for c in codes.split(",") if c.strip()]
        if codes
        else ["KOSPI", "KOSDAQ", "USD_KRW", "CMDT_GC", "NIGHT_FUTURES"]
    )
    results = await market_indicators.fetch_indicators(code_list)
    return results


@router.get("/api/market-indicators")
async def list_indicators():
    import market_indicators

    return market_indicators.CATALOG


@router.get("/api/search")
async def search(q: str = Query(..., min_length=1)):
    results = await cache.search_corp(q)
    return results


@router.get("/api/preferences/{stock_code}")
async def get_stock_preference(stock_code: str, request: Request):
    current_user = await get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    preference = await cache.get_user_stock_preference(current_user["google_sub"], stock_code)
    return {
        "stock_code": stock_code,
        "authenticated": True,
        "user_preference": preference,
    }


@router.put("/api/preferences/{stock_code}")
async def update_stock_preference(stock_code: str, request: Request, payload: dict = Body(...)):
    current_user = await get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    preference = await cache.save_user_stock_preference(
        current_user["google_sub"],
        stock_code,
        is_starred=payload.get("is_starred") if "is_starred" in payload else None,
        is_pinned=payload.get("is_pinned") if "is_pinned" in payload else None,
        note=payload.get("note") if "note" in payload else None,
    )
    return {
        "stock_code": stock_code,
        "authenticated": True,
        "user_preference": preference,
    }


@router.get("/api/quote/{stock_code}")
async def quote_snapshot(stock_code: str):
    corp_code = await cache.get_corp_code(stock_code)
    if not corp_code:
        raise HTTPException(status_code=404, detail="종목코드를 찾을 수 없습니다.")
    return await stock_price.fetch_quote_snapshot(stock_code)
