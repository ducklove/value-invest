from fastapi import APIRouter, Body, HTTPException, Query, Request

import cache
from deps import get_current_user
from services import stock_quotes

router = APIRouter()


@router.get("/api/market-summary")
async def market_summary(codes: str = ""):
    import market_indicators

    code_list = (
        [c.strip()[:30] for c in codes.split(",") if c.strip()][:60]
        if codes
        else ["KOSPI", "KOSDAQ", "USD_KRW", "CMDT_GC", "NIGHT_FUTURES"]
    )
    results = await market_indicators.fetch_indicators(code_list)
    return results


@router.get("/api/market-indicators")
async def list_indicators():
    import market_indicators

    return market_indicators.CATALOG


@router.get("/api/market/movers")
async def get_market_movers(kind: str = "market_cap", market: str = "kospi", limit: int = 10):
    """Public 시장 랭킹 — 시총상위/거래상위/급상승/급하락 (Naver sise)."""
    import market_movers

    limit = max(1, min(int(limit), 30))
    items = await market_movers.fetch_market_movers(kind, market, limit)
    return {"kind": kind, "market": market, "items": items}


@router.get("/api/market/sectors")
async def get_market_sectors(limit: int = 12):
    """Public 업종별 등락 (Naver sise_group)."""
    import market_movers

    limit = max(1, min(int(limit), 40))
    items = await market_movers.fetch_sectors(limit)
    return {"sectors": items}


@router.get("/api/market/news")
async def get_market_news(limit: int = 8):
    """Public 주요 뉴스 (Naver finance mainnews)."""
    import market_news

    limit = max(1, min(int(limit), 20))
    items = await market_news.fetch_market_news(limit)
    return {"news": items}


@router.get("/api/market/investor-flows")
async def get_investor_flows():
    """Public 투자자별 매매동향 — 코스피·코스닥 개인/외국인/기관 순매수(최근 영업일)."""
    import market_movers

    flows = await market_movers.fetch_investor_flows()
    return {"flows": flows}


@router.get("/api/external/insights")
async def external_insights():
    """Public — 외부 분석 도구(지주사 NAV·우선주 괴리율·김치프리미엄) 요약."""
    import external_tools

    return await external_tools.fetch_external_insights()


@router.get("/api/external/stock/{code}")
async def external_stock_links(code: str):
    """Public — 종목분석 deep-link: 이 종목의 우선주 괴리율/지주사 NAV 정보."""
    import external_tools

    return await external_tools.fetch_stock_links(code)


@router.get("/api/settings/market-bar")
async def get_market_bar_setting(request: Request):
    import json
    user = await get_current_user(request)
    if not user:
        return {"codes": None}  # client uses localStorage
    raw = await cache.get_user_setting(user["google_sub"], "market_bar_codes")
    return {"codes": json.loads(raw) if raw else None}


@router.put("/api/settings/market-bar")
async def set_market_bar_setting(request: Request, payload: dict = Body(...)):
    import json
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    codes = payload.get("codes", [])
    if not isinstance(codes, list) or len(codes) > 10:
        raise HTTPException(status_code=400, detail="최대 10개까지 설정할 수 있습니다.")
    codes = [str(c).strip()[:30] for c in codes if isinstance(c, str) and str(c).strip()]
    await cache.set_user_setting(user["google_sub"], "market_bar_codes", json.dumps(codes))
    return {"ok": True}


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
    return await stock_quotes.get_quote_snapshot(stock_code)
