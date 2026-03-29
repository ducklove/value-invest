from fastapi import APIRouter, Body, HTTPException, Query, Request

import cache
from deps import attach_quote_snapshots, get_current_user

router = APIRouter()


@router.delete("/api/cache/{stock_code}")
async def delete_cache(stock_code: str, request: Request):
    current_user = await get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인 후 내 목록에서만 삭제할 수 있습니다.")
    await cache.delete_user_recent_analysis(current_user["google_sub"], stock_code)
    return {"ok": True, "scope": "user"}


@router.put("/api/cache/order")
async def update_cache_order(request: Request, payload: dict = Body(...)):
    current_user = await get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    stock_codes = payload.get("stock_codes")
    if not isinstance(stock_codes, list) or not stock_codes:
        raise HTTPException(status_code=400, detail="정렬할 종목 목록이 필요합니다.")

    normalized_codes = []
    seen_codes = set()
    for item in stock_codes:
        stock_code = str(item or "").strip()
        if not stock_code or stock_code in seen_codes:
            continue
        seen_codes.add(stock_code)
        normalized_codes.append(stock_code)

    if not normalized_codes:
        raise HTTPException(status_code=400, detail="정렬할 종목 목록이 비어 있습니다.")

    await cache.save_user_stock_order(current_user["google_sub"], normalized_codes)
    return {"ok": True, "stock_codes": normalized_codes}


@router.get("/api/cache/list")
async def cache_list(request: Request, include_quotes: bool = Query(False), tab: str = Query("recent")):
    current_user = await get_current_user(request)
    items = await cache.get_cached_analyses(
        limit=20,
        include_quotes=include_quotes,
        google_sub=current_user["google_sub"] if current_user else None,
        tab=tab,
    )
    if include_quotes:
        return await attach_quote_snapshots(items)
    return items
