from fastapi import APIRouter, Body, HTTPException, Query, Request

from deps import get_current_user
from repositories import corp_codes
from repositories import user_settings as user_settings_repo
from repositories import user_stocks as user_stocks_repo
from services import stock_quotes

router = APIRouter()


@router.get("/api/market-summary")
async def market_summary(codes: str = ""):
    import market_indicators

    code_list = (
        [c.strip()[:30] for c in codes.split(",") if c.strip()][:160]
        if codes
        else ["KOSPI", "KOSDAQ", "USD_KRW", "CMDT_GC", "NIGHT_FUTURES"]
    )
    results = await market_indicators.fetch_indicators(code_list)
    return results


@router.get("/api/market/live")
async def market_live(codes: str = ""):
    """야간선물·Hyperliquid의 짧은 TTL REST 폴백을 제공한다."""
    import market_indicators

    code_list = [c.strip()[:30] for c in codes.split(",") if c.strip()][:10]
    return await market_indicators.fetch_indicators_live(code_list)


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


@router.get("/api/market/economic-calendar")
async def get_economic_calendar(
    request: Request,
    start: str = "",
    end: str = "",
    high: str = "",
    mid: str = "",
    low: str = "",
):
    """Public 경제캘린더 (zeroin/한경 피드). 기간 + 중요도별 국가 선택.

    high/mid/low: 각 중요도의 국가 선택. 'all'(모든 국가) 또는 국가코드 CSV(예:
    kr,us). 빈 값(쿼리에는 있으나 비어 있음)은 그 중요도 숨김. 셋 다 미지정이면
    기본값(상=전체, 중·하=한국)을 적용. start/end: YYYY-MM-DD, 미지정 시 오늘~+6일.
    """
    from datetime import date, datetime, timedelta

    import economic_calendar

    def _parse_date(s: str):
        try:
            return datetime.strptime(s.strip(), "%Y-%m-%d").date()
        except (ValueError, AttributeError):
            return None

    today = date.today()
    start_d = _parse_date(start) or today
    end_d = _parse_date(end) or (start_d + timedelta(days=6))
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    # 과도한 범위 방어(업스트림 부하·응답 크기) — 최대 62일.
    if (end_d - start_d).days > 62:
        end_d = start_d + timedelta(days=62)

    def _level_sel(raw: str):
        raw = (raw or "").strip()
        if not raw:
            return None
        if raw.lower() == "all":
            return "all"
        return [c.strip().lower() for c in raw.split(",") if c.strip()][:30]

    qp = request.query_params
    raw_by_level = {"high": high, "mid": mid, "low": low}
    selection: dict[str, object] = {}
    for level, raw in raw_by_level.items():
        if level not in qp:
            continue  # 미지정 — 기본값 처리 대상
        sel = _level_sel(raw)
        if sel is not None:  # 쿼리에 있으나 빈 값이면 비활성(생략)
            selection[level] = sel
    # 중요도 파라미터가 하나도 없으면(맨손 호출) 기본값 적용.
    if not any(level in qp for level in ("high", "mid", "low")):
        selection = {"high": "all", "mid": ["kr"], "low": ["kr"]}

    return await economic_calendar.fetch_calendar_by_level(
        start_date=start_d.isoformat(),
        end_date=end_d.isoformat(),
        selection=selection,
    )


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
    raw = await user_settings_repo.get_user_setting(user["google_sub"], "market_bar_codes")
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
    await user_settings_repo.set_user_setting(user["google_sub"], "market_bar_codes", json.dumps(codes))
    return {"ok": True}


@router.get("/api/search")
async def search(q: str = Query(..., min_length=1)):
    results = await corp_codes.search_corp(q)
    return results


@router.get("/api/preferences/{stock_code}")
async def get_stock_preference(stock_code: str, request: Request):
    current_user = await get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    preference = await user_stocks_repo.get_user_stock_preference(current_user["google_sub"], stock_code)
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

    preference = await user_stocks_repo.save_user_stock_preference(
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
    corp_code = await corp_codes.get_corp_code(stock_code)
    if not corp_code:
        raise HTTPException(status_code=404, detail="종목코드를 찾을 수 없습니다.")
    return await stock_quotes.get_quote_snapshot(stock_code)
