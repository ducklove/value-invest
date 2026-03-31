import re

import httpx
from fastapi import APIRouter, Body, HTTPException, Query, Request

import cache
import stock_price
from deps import get_current_user

router = APIRouter()


@router.get("/api/market-summary")
async def market_summary():
    async def fetch_index(code: str) -> dict:
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(
                    f"https://finance.naver.com/sise/sise_index.naver?code={code}",
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                value = re.search(r'id="now_value"[^>]*>([^<]+)', r.text)
                direction = re.search(r'class="quotient\s+(up|dn)"', r.text)
                d = direction.group(1) if direction else ""
                # Pattern: <span>21.59</span> -0.40%
                change_block = re.search(
                    r'change_value_and_rate"[^>]*><span>([^<]+)</span>\s*([-+]?[0-9.]+%)',
                    r.text,
                )
                change_val = change_block.group(1).strip() if change_block else None
                change_pct = change_block.group(2).strip() if change_block else None
                return {
                    "value": value.group(1).strip() if value else None,
                    "change": change_val,
                    "change_pct": change_pct,
                    "direction": "up" if d == "up" else "down" if d == "dn" else "",
                }
        except Exception:
            return {}

    async def fetch_fx() -> dict:
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get("https://finance.naver.com/marketindex/", headers={"User-Agent": "Mozilla/5.0"})
                value = re.search(r'class="value"[^>]*>([0-9,.]+)', r.text)
                change = re.search(r'class="change"[^>]*>([0-9,.]+)', r.text)
                d = re.search(r'class="head_info.*?class="(up|down)"', r.text, re.DOTALL)
                val_str = value.group(1).strip().replace(",", "") if value else None
                chg_str = change.group(1).strip().replace(",", "") if change else None
                change_pct = None
                if val_str and chg_str:
                    try:
                        v, c = float(val_str), float(chg_str)
                        prev = v - c if d and d.group(1) == "up" else v + c
                        if prev:
                            change_pct = f"{c / prev * 100:.2f}%"
                    except ValueError:
                        pass
                return {
                    "value": value.group(1).strip() if value else None,
                    "change": change.group(1).strip() if change else None,
                    "change_pct": change_pct,
                    "direction": d.group(1) if d else "",
                }
        except Exception:
            return {}

    async def fetch_night_futures() -> dict:
        """Fetch KOSPI200 night futures from esignal.co.kr via socket.io polling."""
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://esignal.co.kr/kospi200-futures-night/",
                    "Origin": "https://esignal.co.kr",
                }
                r1 = await c.get(
                    "https://esignal.co.kr/proxy/8888/socket.io/",
                    params={"EIO": "3", "transport": "polling"},
                    headers=headers,
                )
                m = re.search(r'"sid":"([^"]+)"', r1.text)
                if not m:
                    return {}
                sid = m.group(1)
                r2 = await c.get(
                    "https://esignal.co.kr/proxy/8888/socket.io/",
                    params={"EIO": "3", "transport": "polling", "sid": sid},
                    headers=headers,
                )
                import json as _json
                pm = re.search(r'\["populate","(\{.+?\})"\]', r2.text)
                if not pm:
                    return {}
                raw = pm.group(1).replace('\\"', '"')
                data = _json.loads(raw)
                val = float(data["value"])
                diff = float(data["value_diff"])
                prev = float(data["value_day"])
                pct = round(diff / prev * 100, 2) if prev else 0
                direction = "up" if diff > 0 else "down" if diff < 0 else ""
                return {
                    "value": f"{val:,.2f}",
                    "change": f"{abs(diff):.2f}",
                    "change_pct": f"{'-' if diff < 0 else ''}{abs(pct):.2f}%",
                    "direction": direction,
                }
        except Exception:
            return {}

    import asyncio
    kospi, kosdaq, fx, nf = await asyncio.gather(
        fetch_index("KOSPI"), fetch_index("KOSDAQ"), fetch_fx(), fetch_night_futures()
    )
    return {"kospi": kospi, "kosdaq": kosdaq, "usd_krw": fx, "night_futures": nf}


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
