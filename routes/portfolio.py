import asyncio
import logging
import re
from functools import partial

import httpx
from fastapi import APIRouter, Body, HTTPException, Query, Request

import cache
import kis_ws_manager
import stock_price
from deps import RECENT_QUOTES_SEMAPHORE, get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


_SPECIAL_ASSETS = {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH"}

_CASH_NAMES = {
    "CASH_KRW": "원화", "CASH_USD": "미국 달러", "CASH_EUR": "유로",
    "CASH_JPY": "일본 엔", "CASH_CNY": "중국 위안", "CASH_HKD": "홍콩 달러",
    "CASH_GBP": "영국 파운드", "CASH_AUD": "호주 달러", "CASH_CAD": "캐나다 달러",
    "CASH_CHF": "스위스 프랑", "CASH_TWD": "대만 달러", "CASH_VND": "베트남 동",
    "CASH_SEK": "스웨덴 크로나", "CASH_DKK": "덴마크 크로네", "CASH_NOK": "노르웨이 크로네",
}

_CASH_FX_CODE = {
    "CASH_USD": "FX_USDKRW", "CASH_EUR": "FX_EURKRW", "CASH_JPY": "FX_JPYKRW",
    "CASH_CNY": "FX_CNYKRW", "CASH_HKD": "FX_HKDKRW", "CASH_GBP": "FX_GBPKRW",
    "CASH_AUD": "FX_AUDKRW", "CASH_CAD": "FX_CADKRW", "CASH_CHF": "FX_CHFKRW",
    "CASH_TWD": "FX_TWDKRW", "CASH_VND": "FX_VNDKRW",
}


def _is_cash_asset(code: str) -> bool:
    return code.startswith("CASH_")


def _is_special_asset(code: str) -> bool:
    return code in _SPECIAL_ASSETS or _is_cash_asset(code)


def _is_korean_stock(code: str) -> bool:
    return len(code) == 6 and code[:5].isdigit()


def _is_preferred_stock(code: str) -> bool:
    if len(code) != 6 or not code[:5].isdigit():
        return False
    return not code[5].isdigit() or code[5] != '0'


def _common_stock_code(code: str) -> str:
    return code[:5] + '0'


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
    if stock_code in _CASH_NAMES:
        return _CASH_NAMES[stock_code]
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


async def _fetch_cash_quote(stock_code: str) -> dict:
    """Fetch cash quote: KRW=1, others=FX rate to KRW."""
    if stock_code == "CASH_KRW":
        return {"price": 1, "change": 0, "change_pct": 0}
    fx_code = _CASH_FX_CODE.get(stock_code)
    if not fx_code:
        return {}
    rates = await _get_fx_rates()
    rate = rates.get(fx_code)
    if not rate:
        return {}
    unit = _FX_UNIT.get(fx_code, 1)
    price = rate / unit
    return {"price": round(price, 2), "change": 0, "change_pct": 0}


_quote_cache: dict[str, tuple[float, dict]] = {}
_QUOTE_CACHE_TTL = 60


_ticker_map: dict[str, str] = {}  # stock_code -> resolved ticker (e.g., A200 -> A200.AX)


async def _fetch_quote(stock_code: str) -> dict:
    now = _time.monotonic()
    cached = _quote_cache.get(stock_code)
    if cached and (now - cached[0]) < _QUOTE_CACHE_TTL:
        return cached[1]
    if _is_cash_asset(stock_code):
        q = await _fetch_cash_quote(stock_code)
    elif stock_code == "KRX_GOLD":
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
    """Attach cached quotes — WebSocket cache preferred, then polling cache."""
    now = _time.monotonic()
    result = []
    for item in items:
        enriched = dict(item)
        code = item["stock_code"]
        ws_q = kis_ws_manager.get_cached_quote(code)
        if ws_q and ws_q.get("price") is not None:
            enriched["quote"] = {
                "date": ws_q.get("date", ""),
                "price": ws_q["price"],
                "previous_close": ws_q.get("previous_close"),
                "change": ws_q.get("change"),
                "change_pct": ws_q.get("change_pct"),
            }
        else:
            cached = _quote_cache.get(code)
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


# --- Benchmark ---

_market_type_cache: dict[str, str] = {}  # stock_code -> "KOSPI" | "KOSDAQ"


async def _detect_market_type(code: str) -> str:
    """Detect if a Korean stock is KOSPI or KOSDAQ via Naver Finance."""
    if code in _market_type_cache:
        return _market_type_cache[code]
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                f"https://finance.naver.com/item/main.naver?code={code}",
                headers={"User-Agent": "Mozilla/5.0"},
                follow_redirects=True,
            )
            if "코스닥" in resp.text[:5000]:
                _market_type_cache[code] = "KOSDAQ"
            else:
                _market_type_cache[code] = "KOSPI"
    except Exception:
        _market_type_cache[code] = "KOSPI"
    return _market_type_cache[code]


async def _prefetch_market_types(codes: list[str]):
    """Bulk-detect market types in parallel for codes not yet cached."""
    uncached = [c for c in codes if c not in _market_type_cache and _is_korean_stock(c) and not _is_preferred_stock(c)]
    if not uncached:
        return
    async def _detect(code):
        await _detect_market_type(code)
    await asyncio.gather(*[_detect(c) for c in uncached])


async def _resolve_default_benchmark(code: str) -> str:
    """Return the default benchmark code for a stock."""
    if _is_cash_asset(code):
        return "FX_USDKRW"
    if code in _SPECIAL_ASSETS:
        return "FX_USDKRW"
    if _is_korean_stock(code):
        if _is_preferred_stock(code):
            return _common_stock_code(code)
        mtype = await _detect_market_type(code)
        return "IDX_KOSPI" if mtype == "KOSPI" else "IDX_KOSDAQ"
    return "IDX_SP500"


_BENCHMARK_NAMES = {
    "IDX_KOSPI": "코스피",
    "IDX_KOSDAQ": "코스닥",
    "IDX_SP500": "S&P500",
    "FX_USDKRW": "USD/KRW",
}

_benchmark_name_cache: dict[str, str] = {}


async def _resolve_benchmark_name(code: str) -> str:
    """Resolve a benchmark code to a human-readable name."""
    if code in _BENCHMARK_NAMES:
        return _BENCHMARK_NAMES[code]
    if code in _benchmark_name_cache:
        return _benchmark_name_cache[code]
    # For codes with dots/slashes, try dash variant first (faster for yfinance)
    alt = code.replace(".", "-").replace("/", "-") if not _is_korean_stock(code) else None
    if alt and alt != code:
        name = await _yfinance_resolve_name(alt)
        if not name:
            name = await _resolve_name(code)
    else:
        name = await _resolve_name(code)
    result = name or code
    _benchmark_name_cache[code] = result
    return result

_benchmark_quote_cache: dict[str, tuple[float, dict]] = {}
_BENCHMARK_CACHE_TTL = 120  # 2 minutes


async def _fetch_index_quote(index_code: str) -> dict:
    """Fetch KOSPI or KOSDAQ index change_pct from Naver."""
    naver_code = "KOSPI" if index_code == "IDX_KOSPI" else "KOSDAQ"
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(
                f"https://finance.naver.com/sise/sise_index.naver?code={naver_code}",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            direction = re.search(r'class="quotient\s+(up|dn)"', r.text)
            d = direction.group(1) if direction else ""
            change_block = re.search(
                r'change_value_and_rate"[^>]*><span>([^<]+)</span>\s*([-+]?[0-9.]+%)',
                r.text,
            )
            if change_block:
                pct_str = change_block.group(2).replace("%", "")
                pct = float(pct_str)
                if d == "dn" and pct > 0:
                    pct = -pct
                return {"change_pct": pct}
    except Exception as e:
        logger.warning("Index quote fetch failed for %s: %s", index_code, e)
    return {}


async def _fetch_sp500_quote() -> dict:
    """Fetch S&P 500 change_pct via yfinance."""
    try:
        import yfinance as yf
        loop = asyncio.get_event_loop()
        t = await loop.run_in_executor(None, partial(yf.Ticker, "^GSPC"))
        fi = await loop.run_in_executor(None, lambda: t.fast_info)
        price = fi.last_price
        prev = fi.previous_close
        if price and prev:
            pct = round((price - prev) / prev * 100, 2)
            return {"change_pct": pct}
    except Exception as e:
        logger.warning("S&P500 quote fetch failed: %s", e)
    return {}


async def _fetch_fx_usdkrw_quote() -> dict:
    """Fetch USD/KRW change_pct."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(
                "https://finance.naver.com/marketindex/",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            value = re.search(r'class="value"[^>]*>([0-9,.]+)', r.text)
            change = re.search(r'class="change"[^>]*>([0-9,.]+)', r.text)
            d = re.search(r'class="head_info.*?class="(up|down)"', r.text, re.DOTALL)
            if value and change:
                v = float(value.group(1).replace(",", ""))
                c = float(change.group(1).replace(",", ""))
                prev = v - c if d and d.group(1) == "up" else v + c
                if prev:
                    pct = round(c / prev * 100, 2)
                    if d and d.group(1) == "down":
                        pct = -pct
                    return {"change_pct": pct}
    except Exception as e:
        logger.warning("FX USD/KRW quote fetch failed: %s", e)
    return {}


async def _fetch_benchmark_quote(benchmark_code: str) -> dict:
    """Fetch a benchmark quote (cached)."""
    now = _time.monotonic()
    cached = _benchmark_quote_cache.get(benchmark_code)
    if cached and (now - cached[0]) < _BENCHMARK_CACHE_TTL:
        return cached[1]

    if benchmark_code == "IDX_KOSPI" or benchmark_code == "IDX_KOSDAQ":
        q = await _fetch_index_quote(benchmark_code)
    elif benchmark_code == "IDX_SP500":
        q = await _fetch_sp500_quote()
    elif benchmark_code == "FX_USDKRW":
        q = await _fetch_fx_usdkrw_quote()
    else:
        # It's a stock code (e.g., common stock for preferred)
        # For codes with dots/slashes, try dash variant directly first (faster)
        alt = benchmark_code.replace(".", "-").replace("/", "-") if not _is_korean_stock(benchmark_code) else None
        if alt and alt != benchmark_code:
            stock_q = await _yfinance_fetch_quote(alt)
            if not stock_q or not stock_q.get("change_pct"):
                stock_q = await _fetch_quote(benchmark_code)
        else:
            stock_q = await _fetch_quote(benchmark_code)
        q = {"change_pct": stock_q.get("change_pct")} if stock_q else {}

    _benchmark_quote_cache[benchmark_code] = (now, q)
    return q


@router.get("/api/portfolio/groups")
async def get_groups(request: Request):
    user = _require_user(await get_current_user(request))
    return await cache.get_portfolio_groups(user["google_sub"])


@router.post("/api/portfolio/groups")
async def add_group(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    name = str(payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="그룹명을 입력해 주세요.")
    groups = await cache.get_portfolio_groups(user["google_sub"])
    if any(g["group_name"] == name for g in groups):
        raise HTTPException(status_code=400, detail="이미 존재하는 그룹명입니다.")
    result = await cache.add_portfolio_group(user["google_sub"], name)
    return {"ok": True, **result}


@router.put("/api/portfolio/groups/{group_name}")
async def rename_group(group_name: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    new_name = str(payload.get("new_name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="새 그룹명을 입력해 주세요.")
    groups = await cache.get_portfolio_groups(user["google_sub"])
    target = next((g for g in groups if g["group_name"] == group_name), None)
    if not target:
        raise HTTPException(status_code=404, detail="그룹을 찾을 수 없습니다.")
    if any(g["group_name"] == new_name for g in groups):
        raise HTTPException(status_code=400, detail="이미 존재하는 그룹명입니다.")
    await cache.rename_portfolio_group(user["google_sub"], group_name, new_name)
    return {"ok": True}


@router.delete("/api/portfolio/groups/{group_name}")
async def delete_group(group_name: str, request: Request):
    user = _require_user(await get_current_user(request))
    groups = await cache.get_portfolio_groups(user["google_sub"])
    target = next((g for g in groups if g["group_name"] == group_name), None)
    if not target:
        raise HTTPException(status_code=404, detail="그룹을 찾을 수 없습니다.")
    default_count = sum(1 for g in groups if g["is_default"])
    if target["is_default"] and default_count <= 3:
        raise HTTPException(status_code=400, detail="기본 그룹은 삭제할 수 없습니다.")
    await cache.delete_portfolio_group(user["google_sub"], group_name)
    return {"ok": True}


@router.put("/api/portfolio/groups-order")
async def save_groups_order(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    names = payload.get("group_names")
    if not isinstance(names, list) or not names:
        raise HTTPException(status_code=400, detail="그룹 목록이 필요합니다.")
    await cache.save_portfolio_groups_order(user["google_sub"], names)
    return {"ok": True}


@router.get("/api/portfolio/quotes")
async def stream_portfolio_quotes(request: Request):
    """Stream quote updates one by one with rate limiting."""
    user = _require_user(await get_current_user(request))
    items = await cache.get_portfolio(user["google_sub"])

    # Prefetch market types before streaming
    needs_resolve = [it for it in items if not it.get("benchmark_code")]
    if needs_resolve:
        await _prefetch_market_types([it["stock_code"] for it in needs_resolve])

    async def generate():
        import json as _json
        now = _time.monotonic()
        benchmark_codes = set()
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
            bc = item.get("benchmark_code") or await _resolve_default_benchmark(code)
            benchmark_codes.add(bc)
        # Fetch and stream benchmark quotes
        for bc in benchmark_codes:
            try:
                bq = await _fetch_benchmark_quote(bc)
                yield f"data: {_json.dumps({'benchmark_code': bc, 'benchmark_quote': bq}, ensure_ascii=False)}\n\n"
            except Exception:
                pass
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
    await cache.get_portfolio_groups(user["google_sub"])  # ensure default groups
    items = await cache.get_portfolio(user["google_sub"])
    # Prefetch market types in parallel, then resolve default benchmarks
    needs_resolve = [it for it in items if not it.get("benchmark_code")]
    if needs_resolve:
        await _prefetch_market_types([it["stock_code"] for it in needs_resolve])
        for item in needs_resolve:
            item["benchmark_code"] = await _resolve_default_benchmark(item["stock_code"])
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
        if _is_cash_asset(stock_code):
            currency = stock_code.replace("CASH_", "")
        elif _is_korean_stock(stock_code) or _is_special_asset(stock_code):
            currency = "KRW"
        else:
            currency = await _detect_currency(stock_code)
    group_name = str(payload.get("group_name") or "").strip() or None
    benchmark_code = str(payload.get("benchmark_code") or "").strip() or None
    result = await cache.save_portfolio_item(user["google_sub"], stock_code, stock_name, quantity, avg_price, currency, group_name, benchmark_code)
    return {"ok": True, **result}


@router.put("/api/portfolio/{stock_code}/benchmark")
async def update_benchmark(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    benchmark_code = str(payload.get("benchmark_code") or "").strip() or None
    await cache.update_portfolio_benchmark(user["google_sub"], stock_code, benchmark_code)
    # Return the effective benchmark and its quote
    effective = benchmark_code or await _resolve_default_benchmark(stock_code)
    bq = await _fetch_benchmark_quote(effective)
    name = await _resolve_benchmark_name(effective)
    return {"ok": True, "benchmark_code": benchmark_code, "effective_benchmark": effective, "benchmark_name": name, "benchmark_quote": bq}


@router.get("/api/portfolio/benchmark-quotes")
async def get_benchmark_quotes(request: Request):
    """Fetch all unique benchmark quotes for the user's portfolio."""
    user = _require_user(await get_current_user(request))
    items = await cache.get_portfolio(user["google_sub"])
    needs_resolve = [it for it in items if not it.get("benchmark_code")]
    if needs_resolve:
        await _prefetch_market_types([it["stock_code"] for it in needs_resolve])
    benchmark_codes = set()
    for item in items:
        bc = item.get("benchmark_code") or await _resolve_default_benchmark(item["stock_code"])
        benchmark_codes.add(bc)
    async def _fetch_one(bc):
        try:
            bq, name = await asyncio.gather(
                _fetch_benchmark_quote(bc), _resolve_benchmark_name(bc)
            )
            return bc, {**bq, "name": name}
        except Exception:
            return bc, {}

    pairs = await asyncio.gather(*[_fetch_one(bc) for bc in benchmark_codes])
    return {bc: data for bc, data in pairs}


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
    if _is_cash_asset(code):
        return {"stock_code": code, "stock_name": _CASH_NAMES.get(code, code)}
    if code in _SPECIAL_ASSETS:
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


# --- NAV / Snapshots / Cashflows ---

@router.get("/api/portfolio/nav-history")
async def get_nav_history(request: Request):
    user = _require_user(await get_current_user(request))
    return await cache.get_nav_history(user["google_sub"])


@router.get("/api/portfolio/cashflows")
async def get_cashflows(request: Request):
    user = _require_user(await get_current_user(request))
    return await cache.get_cashflows(user["google_sub"])


@router.post("/api/portfolio/cashflows")
async def add_cashflow(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    cf_type = str(payload.get("type") or "").strip()
    if cf_type not in ("deposit", "withdrawal"):
        raise HTTPException(status_code=400, detail="type은 deposit 또는 withdrawal이어야 합니다.")
    amount = payload.get("amount")
    if amount is None or float(amount) <= 0:
        raise HTTPException(status_code=400, detail="금액은 0보다 커야 합니다.")
    amount = float(amount)
    cf_date = str(payload.get("date") or "").strip()
    if not cf_date:
        from datetime import date
        cf_date = date.today().isoformat()
    memo = str(payload.get("memo") or "").strip() or None

    # Get latest NAV for units calculation
    latest = await cache.get_latest_snapshot(user["google_sub"])
    nav_at_time = latest["nav"] if latest else 1000.0
    units_change = amount / nav_at_time
    if cf_type == "withdrawal":
        units_change = -units_change

    result = await cache.add_cashflow(user["google_sub"], cf_date, cf_type, amount, memo, nav_at_time, units_change)
    return {"ok": True, **result}


@router.delete("/api/portfolio/cashflows/{cf_id}")
async def delete_cashflow(cf_id: int, request: Request):
    user = _require_user(await get_current_user(request))
    await cache.delete_cashflow(user["google_sub"], cf_id)
    return {"ok": True}


