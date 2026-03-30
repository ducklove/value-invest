import asyncio
import logging
import re
from functools import partial

import httpx
from fastapi import APIRouter, Body, HTTPException, Query, Request

import cache
import stock_price
from deps import RECENT_QUOTES_SEMAPHORE, get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


_SPECIAL_ASSETS = {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH"}


def _is_special_asset(code: str) -> bool:
    return code in _SPECIAL_ASSETS


def _is_korean_stock(code: str) -> bool:
    return len(code) == 6 and code[:5].isdigit()


async def _fetch_naver_stock_name(stock_code: str) -> str | None:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                f"https://finance.naver.com/item/main.naver?code={stock_code}",
                follow_redirects=True,
            )
            m = re.search(r"<title>\s*(.+?)\s*:\s*N", resp.text)
            return m.group(1).strip() if m else None
    except Exception:
        return None


_SPECIAL_ASSET_NAMES = {"KRX_GOLD": "KRX 금현물", "CRYPTO_BTC": "비트코인", "CRYPTO_ETH": "이더리움"}


async def _resolve_name(stock_code: str) -> str | None:
    if stock_code in _SPECIAL_ASSET_NAMES:
        return _SPECIAL_ASSET_NAMES[stock_code]
    if _is_korean_stock(stock_code):
        name = await cache.resolve_stock_name(stock_code)
        if name:
            return name
        return await _fetch_naver_stock_name(stock_code)
    return await _resolve_foreign_name(stock_code)


async def _fetch_naver_world_stock(reuters_code: str) -> dict | None:
    """Fetch foreign stock info from Naver world stock API."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                f"https://api.stock.naver.com/stock/{reuters_code}/basic",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code != 200:
                return None
            d = resp.json()
            if not d.get("stockName"):
                return None
            return d
    except Exception:
        return None


_EXCHANGE_SUFFIXES = (
    "", ".O", ".K", ".N", ".HM", ".HK", ".T", ".SS", ".SZ", ".L", ".AX",
    ".DE", ".F", ".PA", ".AS", ".MI", ".MC", ".SW", ".ST", ".CO", ".HE",
)

_YFINANCE_SUFFIXES = (
    "", ".DE", ".F", ".PA", ".AS", ".MI", ".MC", ".L", ".AX", ".T",
    ".HK", ".SS", ".SZ", ".SW", ".ST", ".CO",
)

_CURRENCY_MAP = {
    "USD": "USA", "EUR": "DEU", "GBP": "GBR", "JPY": "JPN",
    "HKD": "HKG", "CNY": "CHN", "AUD": "AUS", "CAD": "CAN",
    "CHF": "CHE", "SEK": "SWE", "DKK": "DNK", "NOK": "NOR",
    "TWD": "TWN", "VND": "VNM",
}


async def _resolve_foreign_name(ticker: str) -> str | None:
    """Try Naver first, then yfinance as fallback."""
    # If ticker already has a dot (e.g., EUN2.DE), try as-is first
    if "." in ticker:
        d = await _fetch_naver_world_stock(ticker)
        if d:
            return d.get("stockName") or d.get("stockNameEng")
    for suffix in _EXCHANGE_SUFFIXES:
        code = ticker + suffix if suffix else ticker
        d = await _fetch_naver_world_stock(code)
        if d:
            return d.get("stockName") or d.get("stockNameEng")
    # yfinance fallback
    return await _yfinance_resolve_name(ticker)


async def _yfinance_find_ticker(ticker: str) -> str | None:
    """Find a working yfinance ticker, trying various exchange suffixes."""
    try:
        import yfinance as yf
        loop = asyncio.get_event_loop()
        candidates = [ticker] if "." in ticker else [ticker + s for s in _YFINANCE_SUFFIXES]
        for candidate in candidates:
            try:
                t = await loop.run_in_executor(None, partial(yf.Ticker, candidate))
                info = await loop.run_in_executor(None, lambda: t.info)
                if info.get("shortName") or info.get("longName"):
                    return candidate
            except Exception:
                continue
    except Exception:
        pass
    return None


async def _yfinance_resolve_name(ticker: str) -> str | None:
    try:
        import yfinance as yf
        found = await _yfinance_find_ticker(ticker)
        if not found:
            return None
        loop = asyncio.get_event_loop()
        t = await loop.run_in_executor(None, partial(yf.Ticker, found))
        info = await loop.run_in_executor(None, lambda: t.info)
        return info.get("shortName") or info.get("longName")
    except Exception:
        return None


async def _resolve_foreign_reuters(ticker: str) -> str | None:
    """Find the full reuters code on Naver, or a working yfinance ticker."""
    if "." in ticker:
        d = await _fetch_naver_world_stock(ticker)
        if d:
            return d.get("reutersCode") or ticker
    for suffix in _EXCHANGE_SUFFIXES:
        code = ticker + suffix if suffix else ticker
        d = await _fetch_naver_world_stock(code)
        if d:
            return d.get("reutersCode") or code
    # yfinance fallback — find a working ticker with suffix
    found = await _yfinance_find_ticker(ticker)
    return found or ticker


async def _fetch_foreign_quote(reuters_code: str) -> dict:
    # Try Naver first
    d = await _fetch_naver_world_stock(reuters_code)
    if d and d.get("closePrice"):
        try:
            price_str = str(d["closePrice"]).replace(",", "")
            price = float(price_str)
            change_str = str(d.get("compareToPreviousClosePrice", "0")).replace(",", "")
            change = float(change_str)
            change_pct = float(d.get("fluctuationsRatio", 0))
            nation = d.get("nationType", "")
            price_krw = await _fx_to_krw(nation, price)
            change_krw = await _fx_to_krw(nation, change)
            return {
                "price": round(price_krw),
                "change": round(change_krw),
                "change_pct": change_pct,
                "nation": d.get("nationName", ""),
            }
        except Exception as exc:
            logger.warning("해외주식 시세 파싱 실패(%s): %s", reuters_code, exc)

    # yfinance fallback
    return await _yfinance_fetch_quote(reuters_code)


async def _yfinance_fetch_quote(ticker: str) -> dict:
    try:
        import yfinance as yf
        loop = asyncio.get_event_loop()
        t = await loop.run_in_executor(None, partial(yf.Ticker, ticker))
        fi = await loop.run_in_executor(None, lambda: t.fast_info)
        price = fi.last_price
        prev = fi.previous_close
        currency = (fi.currency or "USD").upper()
        change = round(price - prev, 4) if price and prev else 0
        change_pct = round(change / prev * 100, 2) if prev else None
        nation = _CURRENCY_MAP.get(currency, "USA")
        price_krw = await _fx_to_krw(nation, price)
        change_krw = await _fx_to_krw(nation, change)
        return {
            "price": round(price_krw),
            "change": round(change_krw),
            "change_pct": change_pct,
        }
    except Exception as exc:
        logger.warning("yfinance 시세 조회 실패(%s): %s", ticker, exc)
        return {}


import time as _time

async def _fetch_krx_gold_quote() -> dict:
    """Fetch KRX gold spot price from Naver Finance gold page."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                "https://finance.naver.com/marketindex/goldDailyQuote.naver",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            html = resp.content.decode("euc-kr", errors="ignore")
            rows = re.findall(
                r'<tr class="(?:up|down)">\s*<td class="date">([^<]+)</td>\s*<td class="num">([^<]+)',
                html,
            )
            if len(rows) >= 2:
                today_price = round(float(rows[0][1].replace(",", "")))
                prev_price = round(float(rows[1][1].replace(",", "")))
                change = today_price - prev_price
                change_pct = round(change / prev_price * 100, 2) if prev_price else 0
                return {"price": today_price, "change": change, "change_pct": change_pct}
            if rows:
                today_price = round(float(rows[0][1].replace(",", "")))
                return {"price": today_price, "change": 0, "change_pct": 0}
    except Exception as e:
        logger.warning("KRX gold quote fetch failed: %s", e)
    return {}


_CRYPTO_UPBIT_MAP = {"CRYPTO_BTC": "KRW-BTC", "CRYPTO_ETH": "KRW-ETH"}


async def _fetch_crypto_quote(stock_code: str) -> dict:
    """Fetch crypto price in KRW from Upbit API."""
    market = _CRYPTO_UPBIT_MAP.get(stock_code)
    if not market:
        return {}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                f"https://api.upbit.com/v1/ticker?markets={market}",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            data = resp.json()
            if data and isinstance(data, list):
                d = data[0]
                price = round(d["trade_price"])
                change = round(d["signed_change_price"])
                change_pct = round(d["signed_change_rate"] * 100, 2)
                return {"price": price, "change": change, "change_pct": change_pct}
    except Exception as e:
        logger.warning("Crypto quote fetch failed for %s: %s", stock_code, e)
    return {}


_quote_cache: dict[str, tuple[float, dict]] = {}
_QUOTE_CACHE_TTL = 60


_ticker_map: dict[str, str] = {}  # stock_code -> resolved ticker (e.g., A200 -> A200.AX)


async def _fetch_quote(stock_code: str) -> dict:
    now = _time.monotonic()
    cached = _quote_cache.get(stock_code)
    if cached and (now - cached[0]) < _QUOTE_CACHE_TTL:
        return cached[1]
    if stock_code == "KRX_GOLD":
        q = await _fetch_krx_gold_quote()
    elif stock_code in _CRYPTO_UPBIT_MAP:
        q = await _fetch_crypto_quote(stock_code)
    elif _is_korean_stock(stock_code):
        q = await stock_price.fetch_quote_snapshot(stock_code)
    else:
        # Use resolved ticker if available, otherwise try to resolve
        ticker = _ticker_map.get(stock_code, stock_code)
        q = await _fetch_foreign_quote(ticker)
        if not q and ticker == stock_code and "." not in stock_code:
            resolved = await _resolve_foreign_reuters(stock_code)
            if resolved and resolved != stock_code:
                _ticker_map[stock_code] = resolved
                q = await _fetch_foreign_quote(resolved)
    _quote_cache[stock_code] = (now, q)
    return q


async def _enrich_with_cached_quotes(items: list[dict]) -> list[dict]:
    """Attach only already-cached quotes (no network calls)."""
    now = _time.monotonic()
    result = []
    for item in items:
        enriched = dict(item)
        cached = _quote_cache.get(item["stock_code"])
        enriched["quote"] = cached[1] if cached and (now - cached[0]) < _QUOTE_CACHE_TTL else {}
        result.append(enriched)
    return result


QUOTE_RATE_INTERVAL = 0.22  # ~4.5 req/s, stays under 5/s limit

# --- FX rate cache ---
_fx_cache: dict[str, float] = {}
_fx_cache_ts: float = 0
_FX_CACHE_TTL = 300  # 5 minutes

_NATION_TO_CURRENCY = {
    "USA": "USD", "VNM": "VND", "JPN": "JPY", "CHN": "CNY",
    "HKG": "HKD", "GBR": "GBP", "TWN": "TWD", "AUS": "AUD",
    "CAN": "CAD", "CHE": "CHF", "DEU": "EUR", "FRA": "EUR",
    "NLD": "EUR", "ITA": "EUR", "ESP": "EUR",
}

_NATION_TO_FX = {
    "USA": "FX_USDKRW", "VNM": "FX_VNDKRW", "JPN": "FX_JPYKRW",
    "CHN": "FX_CNYKRW", "HKG": "FX_HKDKRW", "GBR": "FX_GBPKRW",
    "EUR": "FX_EURKRW", "DEU": "FX_EURKRW", "FRA": "FX_EURKRW",
    "TWN": "FX_TWDKRW", "AUS": "FX_AUDKRW", "CAN": "FX_CADKRW",
    "CHE": "FX_CHFKRW",
}
_FX_UNIT = {"FX_JPYKRW": 100, "FX_VNDKRW": 100}


async def _get_fx_rates() -> dict[str, float]:
    import time
    global _fx_cache, _fx_cache_ts
    if _fx_cache and (time.time() - _fx_cache_ts) < _FX_CACHE_TTL:
        return _fx_cache
    try:
        rates = {}
        async with httpx.AsyncClient(timeout=5) as c:
            for page in (1, 2):
                r = await c.get(
                    f"https://finance.naver.com/marketindex/exchangeList.naver?page={page}",
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                import re as _re
                rows = _re.findall(
                    r'marketindexCd=(\w+)"[^>]*>[^<]*</a>.*?<td class="sale">([^<]+)',
                    r.text, _re.DOTALL,
                )
                for code, val in rows:
                    try:
                        rates[code] = float(val.strip().replace(",", ""))
                    except ValueError:
                        pass
        if rates:
            _fx_cache = rates
            _fx_cache_ts = time.time()
    except Exception:
        pass
    return _fx_cache


async def _detect_currency(stock_code: str) -> str:
    d = await _fetch_naver_world_stock(stock_code)
    if d:
        nation = d.get("nationType", "")
        return _NATION_TO_CURRENCY.get(nation, "USD")
    # yfinance fallback
    try:
        import yfinance as yf
        loop = asyncio.get_event_loop()
        found = await _yfinance_find_ticker(stock_code)
        if found:
            t = await loop.run_in_executor(None, partial(yf.Ticker, found))
            fi = await loop.run_in_executor(None, lambda: t.fast_info)
            return (fi.currency or "USD").upper()
    except Exception:
        pass
    return "USD"


async def _fx_to_krw(nation: str, amount: float) -> float:
    """Convert foreign currency amount to KRW."""
    fx_code = _NATION_TO_FX.get(nation)
    if not fx_code:
        return amount  # unknown nation, assume already KRW-like
    rates = await _get_fx_rates()
    rate = rates.get(fx_code)
    if not rate:
        return amount
    unit = _FX_UNIT.get(fx_code, 1)
    return amount * rate / unit


@router.get("/api/portfolio/quotes")
async def stream_portfolio_quotes(request: Request):
    """Stream quote updates one by one with rate limiting."""
    user = _require_user(await get_current_user(request))
    items = await cache.get_portfolio(user["google_sub"])

    async def generate():
        import json as _json
        now = _time.monotonic()
        for item in items:
            code = item["stock_code"]
            cached = _quote_cache.get(code)
            was_cached = cached and (now - cached[0]) < _QUOTE_CACHE_TTL
            try:
                quote = await _fetch_quote(code)
            except Exception:
                quote = {}
            yield f"data: {_json.dumps({'stock_code': code, 'quote': quote}, ensure_ascii=False)}\n\n"
            if not was_cached:
                await asyncio.sleep(QUOTE_RATE_INTERVAL)
        yield "data: {\"done\": true}\n\n"

    from fastapi.responses import StreamingResponse
    return StreamingResponse(generate(), media_type="text/event-stream")


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


@router.get("/api/portfolio")
async def get_portfolio(request: Request):
    user = _require_user(await get_current_user(request))
    items = await cache.get_portfolio(user["google_sub"])
    return await _enrich_with_cached_quotes(items)


@router.put("/api/portfolio/{stock_code}")
async def save_portfolio_item(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))

    stock_name = str(payload.get("stock_name") or "").strip()
    if not stock_name:
        resolved = await _resolve_name(stock_code)
        if resolved:
            stock_name = resolved
        else:
            raise HTTPException(status_code=400, detail="종목명을 입력해 주세요.")

    quantity = payload.get("quantity")
    avg_price = payload.get("avg_price")
    if quantity is None or avg_price is None:
        raise HTTPException(status_code=400, detail="수량과 매입가를 입력해 주세요.")

    try:
        quantity = float(quantity)
        avg_price = float(avg_price)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="수량과 매입가는 숫자여야 합니다.")

    if quantity == 0:
        raise HTTPException(status_code=400, detail="수량은 0이 아닌 값이어야 합니다.")
    if avg_price < 0:
        raise HTTPException(status_code=400, detail="매입가는 0 이상이어야 합니다.")

    currency = str(payload.get("currency") or "").upper()
    if not currency:
        if _is_korean_stock(stock_code) or _is_special_asset(stock_code):
            currency = "KRW"
        else:
            currency = await _detect_currency(stock_code)
    result = await cache.save_portfolio_item(user["google_sub"], stock_code, stock_name, quantity, avg_price, currency)
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


@router.post("/api/portfolio/bulk")
async def bulk_import(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    mode = str(payload.get("mode", "add")).strip()
    rows = payload.get("items")
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="등록할 종목이 없습니다.")

    # Validate all rows first
    parsed = []
    errors = []
    for i, row in enumerate(rows):
        code = str(row.get("stock_code") or "").strip()
        if not code:
            errors.append(f"행 {i+1}: 종목코드가 비어 있습니다.")
            continue
        try:
            qty = float(row.get("quantity", 0))
            price = float(row.get("avg_price", 0))
        except (TypeError, ValueError):
            errors.append(f"행 {i+1} ({code}): 수량/매입가가 올바르지 않습니다.")
            continue
        if qty == 0:
            errors.append(f"행 {i+1} ({code}): 수량은 0이 아닌 값이어야 합니다.")
            continue
        parsed.append({"stock_code": code, "quantity": qty, "avg_price": price})

    if errors:
        raise HTTPException(status_code=400, detail="\n".join(errors))

    # Resolve names concurrently
    async def resolve(item):
        name = await _resolve_name(item["stock_code"])
        return {**item, "stock_name": name or item["stock_code"]}

    resolved = await asyncio.gather(*(resolve(p) for p in parsed))

    if mode == "replace":
        await cache.clear_portfolio(user["google_sub"])

    for item in resolved:
        code = item["stock_code"]
        currency = "KRW" if _is_korean_stock(code) or _is_special_asset(code) else await _detect_currency(code)
        await cache.save_portfolio_item(
            user["google_sub"], item["stock_code"], item["stock_name"], item["quantity"], item["avg_price"], currency,
        )

    return {"ok": True, "imported": len(resolved), "mode": mode}


@router.get("/api/portfolio/resolve-name")
async def resolve_name(code: str = Query(..., min_length=1)):
    code = code.strip()
    if _is_special_asset(code):
        return {"stock_code": code, "stock_name": _SPECIAL_ASSET_NAMES.get(code, code)}
    if _is_korean_stock(code):
        name = await _resolve_name(code)
        return {"stock_code": code, "stock_name": name}
    # Foreign: find reuters code
    reuters = await _resolve_foreign_reuters(code)
    if reuters:
        d = await _fetch_naver_world_stock(reuters)
        name = d.get("stockName") or d.get("stockNameEng") if d else None
        return {"stock_code": reuters, "stock_name": name, "reuters_code": reuters}
    return {"stock_code": code, "stock_name": None}
