import asyncio
import logging
import os
import re
import time
from functools import partial
from pathlib import Path

import httpx
from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

import cache
import kis_ws_manager
import market_indicators
import stock_price
from deps import RECENT_QUOTES_SEMAPHORE, get_current_user

_OPENROUTER_KEY = ""
_keys_file = Path(__file__).parent.parent / "keys.txt"
if _keys_file.exists():
    for line in _keys_file.read_text().splitlines():
        if line.startswith("OPENROUTER_API_KEY="):
            _OPENROUTER_KEY = line.split("=", 1)[1].strip()

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
        async with _NAVER_SEM:
            async with httpx.AsyncClient(timeout=_NAVER_HTTP_TIMEOUT) as client:
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
        async with _NAVER_SEM:
            async with httpx.AsyncClient(timeout=_NAVER_HTTP_TIMEOUT) as client:
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

# --- Concurrency bounds & deadlines for external calls ---
# Limits how many in-flight calls can hit each external dependency at once,
# so a slow upstream cannot pin every uvicorn worker thread.
_NAVER_SEM = asyncio.Semaphore(6)
_YF_SEM = asyncio.Semaphore(3)
_YF_CALL_TIMEOUT = 8.0
_NAVER_HTTP_TIMEOUT = httpx.Timeout(5.0, connect=3.0)

# Negative cache: tickers we already failed to resolve via yfinance — avoids
# re-running the 16-suffix loop on every quote refresh.
_failed_yf_tickers: set[str] = set()

# Negative cache for any code whose last quote fetch returned empty.
# Stops the per-poll storm against KIS proxy / Naver / yfinance for codes
# that are temporarily (or permanently) broken upstream. TTL-based so a
# stock that recovers will be retried after the window expires.
_DEAD_QUOTE_TTL = 300  # seconds
_dead_quote_cache: dict[str, float] = {}


def _is_dead(code: str) -> bool:
    ts = _dead_quote_cache.get(code)
    if not ts:
        return False
    if (time.monotonic() - ts) < _DEAD_QUOTE_TTL:
        return True
    _dead_quote_cache.pop(code, None)
    return False


def _mark_dead(code: str) -> None:
    _dead_quote_cache[code] = time.monotonic()


async def _yf_run(fn):
    """Run a synchronous yfinance call in the executor, bounded by a
    semaphore and a hard wall-clock deadline. Raises on timeout."""
    loop = asyncio.get_event_loop()
    async with _YF_SEM:
        return await asyncio.wait_for(
            loop.run_in_executor(None, fn), timeout=_YF_CALL_TIMEOUT
        )


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
    """Find a working yfinance ticker, trying various exchange suffixes.
    Bounded by _YF_SEM and a per-call timeout; results (positive and negative)
    are cached to avoid re-running the suffix loop on every quote refresh."""
    if ticker in _ticker_map:
        return _ticker_map[ticker]
    if ticker in _failed_yf_tickers:
        return None
    try:
        import yfinance as yf
        candidates = [ticker] if "." in ticker else [ticker + s for s in _YFINANCE_SUFFIXES]

        def _probe(cand):
            t = yf.Ticker(cand)
            info = t.info
            if info.get("shortName") or info.get("longName"):
                return cand
            return None

        for candidate in candidates:
            try:
                hit = await _yf_run(partial(_probe, candidate))
                if hit:
                    await _save_ticker(ticker, hit)
                    return hit
            except (asyncio.TimeoutError, Exception):
                continue
    except Exception:
        pass
    _failed_yf_tickers.add(ticker)
    return None


async def _yfinance_resolve_name(ticker: str) -> str | None:
    try:
        import yfinance as yf
        found = await _yfinance_find_ticker(ticker)
        if not found:
            return None

        def _name(c):
            info = yf.Ticker(c).info
            return info.get("shortName") or info.get("longName")

        return await _yf_run(partial(_name, found))
    except (asyncio.TimeoutError, Exception):
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
    if ticker in _failed_yf_tickers:
        return {}
    try:
        import yfinance as yf

        def _snap(c):
            t = yf.Ticker(c)
            fi = t.fast_info
            return fi.last_price, fi.previous_close, (fi.currency or "USD").upper()

        try:
            price, prev, currency = await _yf_run(partial(_snap, ticker))
        except asyncio.TimeoutError:
            logger.warning("yfinance 시세 타임아웃(%s)", ticker)
            return {}
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
        _failed_yf_tickers.add(ticker)
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

# Last-known quotes — never expires, survives cache TTL expiry.
# Used as a fallback so the UI can show *something* immediately after restart.
_last_known_quotes: dict[str, dict] = {}


_ticker_map: dict[str, str] = {}  # stock_code -> resolved ticker (e.g., A200 -> A200.AX)
_ticker_map_loaded = False


async def _ensure_ticker_map():
    """Load ticker_map from DB on first access."""
    global _ticker_map_loaded
    if _ticker_map_loaded:
        return
    try:
        saved = await cache.load_ticker_map()
        _ticker_map.update(saved)
        logger.info("Ticker map loaded: %d entries from DB", len(saved))
    except Exception as exc:
        logger.warning("Ticker map load failed: %s", exc)
    _ticker_map_loaded = True


async def _save_ticker(stock_code: str, resolved: str):
    """Save a resolved ticker to both memory and DB."""
    _ticker_map[stock_code] = resolved
    try:
        await cache.save_ticker(stock_code, resolved)
    except Exception as exc:
        logger.warning("Ticker map save failed (%s -> %s): %s", stock_code, resolved, exc)


async def _fetch_quote(stock_code: str) -> dict:
    now = _time.monotonic()
    cached = _quote_cache.get(stock_code)
    if cached and (now - cached[0]) < _QUOTE_CACHE_TTL:
        return cached[1]
    if _is_dead(stock_code):
        return {}
    if _is_cash_asset(stock_code):
        q = await _fetch_cash_quote(stock_code)
    elif stock_code == "KRX_GOLD":
        q = await _fetch_krx_gold_quote()
    elif stock_code in _CRYPTO_UPBIT_MAP:
        q = await _fetch_crypto_quote(stock_code)
    elif _is_korean_stock(stock_code):
        ws_q = kis_ws_manager.get_cached_quote(stock_code)
        if ws_q and ws_q.get("price") is not None:
            q = {
                "date": ws_q.get("date", ""),
                "price": ws_q["price"],
                "previous_close": ws_q.get("previous_close"),
                "change": ws_q.get("change"),
                "change_pct": ws_q.get("change_pct"),
            }
        else:
            q = await stock_price.fetch_quote_snapshot(stock_code)
    else:
        # Use resolved ticker if available, otherwise try to resolve
        await _ensure_ticker_map()
        ticker = _ticker_map.get(stock_code, stock_code)
        q = await _fetch_foreign_quote(ticker)
        if not q and ticker == stock_code and "." not in stock_code:
            resolved = await _resolve_foreign_reuters(stock_code)
            if resolved and resolved != stock_code:
                await _save_ticker(stock_code, resolved)
                q = await _fetch_foreign_quote(resolved)
    if q and q.get("price") is not None:
        _quote_cache[stock_code] = (now, q)
        _last_known_quotes[stock_code] = q
    else:
        _mark_dead(stock_code)
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
            if cached and (now - cached[0]) < _QUOTE_CACHE_TTL:
                enriched["quote"] = cached[1]
            else:
                # Fallback: last-known quote (stale but better than nothing)
                lk = _last_known_quotes.get(code)
                enriched["quote"] = dict(lk, _stale=True) if lk else {}
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
        found = await _yfinance_find_ticker(stock_code)
        if found:
            def _curr(c):
                return (yf.Ticker(c).fast_info.currency or "USD").upper()
            return await _yf_run(partial(_curr, found))
    except (asyncio.TimeoutError, Exception):
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
        async with _NAVER_SEM:
            async with httpx.AsyncClient(timeout=_NAVER_HTTP_TIMEOUT) as client:
                resp = await client.get(
                    f"https://finance.naver.com/item/main.naver?code={code}",
                    headers={"User-Agent": "Mozilla/5.0"},
                    follow_redirects=True,
                )
        if "코스닥" in resp.text[:30000]:
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


_BENCHMARK_TO_INDICATOR = {
    "IDX_KOSPI": "KOSPI",
    "IDX_KOSDAQ": "KOSDAQ",
    "IDX_SP500": "SPX",
    "FX_USDKRW": "USD_KRW",
}


def _indicator_to_change_pct(data: dict) -> float | None:
    """Convert market_indicators result {change_pct: '0.45%', direction: 'up'} → signed float."""
    if not data:
        return None
    raw = (data.get("change_pct") or "").strip().rstrip("%").replace(",", "")
    if not raw:
        return None
    try:
        pct = float(raw)
    except ValueError:
        return None
    if data.get("direction") == "down" and pct > 0:
        pct = -pct
    return pct


async def _fetch_benchmark_quote(benchmark_code: str) -> dict:
    """Fetch a benchmark quote (cached). Reuses market_indicators for shared sources."""
    now = _time.monotonic()
    cached = _benchmark_quote_cache.get(benchmark_code)
    if cached and (now - cached[0]) < _BENCHMARK_CACHE_TTL:
        return cached[1]

    indicator_code = _BENCHMARK_TO_INDICATOR.get(benchmark_code)
    if indicator_code:
        try:
            data = await market_indicators.fetch_indicators([indicator_code])
            pct = _indicator_to_change_pct(data.get(indicator_code) or {})
            q = {"change_pct": pct} if pct is not None else {}
        except Exception as e:
            logger.warning("Indicator-based benchmark fetch failed for %s: %s", benchmark_code, e)
            q = {}
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
    name = str(payload.get("name") or "").strip()[:50]
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
    new_name = str(payload.get("new_name") or "").strip()[:50]
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
    if not isinstance(names, list) or not names or len(names) > 50:
        raise HTTPException(status_code=400, detail="그룹 목록이 필요합니다.")
    names = [str(n).strip()[:50] for n in names if str(n).strip()]
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

        # Fire every quote fetch in parallel — backpressure is enforced by
        # the per-upstream semaphores (_NAVER_SEM, _YF_SEM, KIS proxy sem),
        # so we don't serialize here. Stream results as they arrive.
        async def _one_quote(code: str) -> tuple[str, dict]:
            try:
                return code, await _fetch_quote(code)
            except Exception:
                return code, {}

        quote_tasks = [asyncio.create_task(_one_quote(it["stock_code"])) for it in items]

        # Resolve benchmark codes in parallel too (some need _detect_market_type).
        async def _resolve_bc(item: dict) -> str:
            return item.get("benchmark_code") or await _resolve_default_benchmark(item["stock_code"])

        bc_task = asyncio.create_task(asyncio.gather(*[_resolve_bc(it) for it in items]))

        for fut in asyncio.as_completed(quote_tasks):
            code, quote = await fut
            yield f"data: {_json.dumps({'stock_code': code, 'quote': quote}, ensure_ascii=False)}\n\n"

        try:
            benchmark_codes = set(await bc_task)
        except Exception:
            benchmark_codes = set()

        # Benchmark quotes in parallel as well.
        async def _one_bench(bc: str) -> tuple[str, dict]:
            try:
                return bc, await _fetch_benchmark_quote(bc)
            except Exception:
                return bc, {}

        bench_tasks = [asyncio.create_task(_one_bench(bc)) for bc in benchmark_codes]
        for fut in asyncio.as_completed(bench_tasks):
            bc, bq = await fut
            yield f"data: {_json.dumps({'benchmark_code': bc, 'benchmark_quote': bq}, ensure_ascii=False)}\n\n"

        yield "data: {\"done\": true}\n\n"

    from fastapi.responses import StreamingResponse
    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/api/asset-quote/{stock_code}")
async def asset_quote(stock_code: str):
    """Fetch quote for any asset type (Korean stock, cash, gold, crypto, foreign)."""
    try:
        q = await _fetch_quote(stock_code)
        if not q:
            raise HTTPException(status_code=404, detail="시세를 가져올 수 없습니다.")
        return q
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=404, detail="시세를 가져올 수 없습니다.")


_NON_QUOTABLE_PREFIXES = ("IDX_", "FX_")

@router.post("/api/asset-quotes")
async def asset_quotes_batch(payload: dict = Body(...)):
    """Fetch quotes for multiple codes in one request."""
    codes = payload.get("codes", [])
    if not isinstance(codes, list) or len(codes) > 100:
        raise HTTPException(status_code=400, detail="최대 100개까지 조회 가능합니다.")
    codes = list({str(c).strip() for c in codes if str(c).strip()})

    async def _fetch_one(code):
        if code.startswith(_NON_QUOTABLE_PREFIXES):
            return code, {}
        try:
            q = await _fetch_quote(code)
            return code, q or {}
        except Exception:
            return code, {}

    results = await asyncio.gather(*[_fetch_one(c) for c in codes])
    return {code: quote for code, quote in results}


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
    if abs(quantity) > 1_000_000_000:
        raise HTTPException(status_code=400, detail="수량이 너무 큽니다.")
    if avg_price < 0:
        raise HTTPException(status_code=400, detail="매입가는 0 이상이어야 합니다.")
    if avg_price > 1_000_000_000_000:
        raise HTTPException(status_code=400, detail="매입가가 너무 큽니다.")

    currency = str(payload.get("currency") or "").upper()
    if not currency:
        if _is_cash_asset(stock_code):
            currency = stock_code.replace("CASH_", "")
        elif _is_korean_stock(stock_code) or _is_special_asset(stock_code):
            currency = "KRW"
        else:
            currency = await _detect_currency(stock_code)
    group_name = str(payload.get("group_name") or "").strip() or None
    if group_name:
        groups = await cache.get_portfolio_groups(user["google_sub"])
        if not any(g["group_name"] == group_name for g in groups):
            raise HTTPException(status_code=400, detail=f"존재하지 않는 그룹명입니다: {group_name}")
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

    # Resolve currencies
    for item in resolved:
        code = item["stock_code"]
        item["currency"] = "KRW" if _is_korean_stock(code) or _is_special_asset(code) else await _detect_currency(code)

    if mode == "replace":
        await cache.replace_portfolio(user["google_sub"], resolved)
    else:
        for item in resolved:
            await cache.save_portfolio_item(
                user["google_sub"], item["stock_code"], item["stock_name"], item["quantity"], item["avg_price"], item["currency"],
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

@router.get("/api/portfolio/prev-day-snapshot")
async def get_prev_day_snapshot(request: Request):
    user = _require_user(await get_current_user(request))
    from datetime import date, timedelta
    today = date.today()
    yesterday = (today - timedelta(days=1)).isoformat()
    db = await cache.get_db()
    # Previous day's closing snapshot
    cursor = await db.execute(
        "SELECT date, total_value, fx_usdkrw, nav FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (user["google_sub"], yesterday),
    )
    snap_row = await cursor.fetchone()
    total_value = snap_row["total_value"] if snap_row else None
    fx_usdkrw = snap_row["fx_usdkrw"] if snap_row else None
    prev_nav = snap_row["nav"] if snap_row else None
    # Per-stock snapshots
    stock_snapshots = await cache.get_stock_snapshots_by_date(user["google_sub"], yesterday)
    stock_values = {s["stock_code"]: s["market_value"] for s in stock_snapshots}
    # Net cashflow not yet reflected in snapshot. Use created_at > snapshot
    # date 22:00 (snapshot runs at 22:00) to catch cashflows entered after
    # the snapshot was taken, regardless of their nominal date.
    snap_date = snap_row["date"] if snap_row else None
    if snap_date:
        created_after = f"{snap_date}T22:00:00"
    else:
        created_after = today.isoformat()
    cursor2 = await db.execute(
        "SELECT type, amount FROM portfolio_cashflows WHERE google_sub = ? AND created_at > ?",
        (user["google_sub"], created_after),
    )
    today_net_cashflow = 0.0
    for row in await cursor2.fetchall():
        if row["type"] == "deposit":
            today_net_cashflow += row["amount"]
        elif row["type"] == "withdrawal":
            today_net_cashflow -= row["amount"]
    return {
        "total_value": total_value,
        "fx_usdkrw": fx_usdkrw,
        "nav": prev_nav,
        "stock_values": stock_values,
        "today_net_cashflow": today_net_cashflow,
    }


@router.get("/api/portfolio/month-end-value")
async def get_month_end_value(request: Request):
    user = _require_user(await get_current_user(request))
    from datetime import date, timedelta
    month_end = (date.today().replace(day=1) - timedelta(days=1)).isoformat()
    snapshot = await cache.get_month_end_snapshot(user["google_sub"])
    stock_snapshots = await cache.get_stock_snapshots_by_date(user["google_sub"], month_end)
    result = dict(snapshot) if snapshot else {}
    result["stock_values"] = {s["stock_code"]: s["market_value"] for s in stock_snapshots}
    return result


@router.get("/api/portfolio/year-start-value")
async def get_year_start_value(request: Request):
    user = _require_user(await get_current_user(request))
    snapshot = await cache.get_year_start_snapshot(user["google_sub"])
    result = dict(snapshot) if snapshot else {}
    if snapshot and snapshot.get("date"):
        stock_snapshots = await cache.get_stock_snapshots_by_date(user["google_sub"], snapshot["date"])
        result["stock_values"] = {s["stock_code"]: s["market_value"] for s in stock_snapshots}
    else:
        result["stock_values"] = {}
    return result


@router.get("/api/portfolio/nav-history")
async def get_nav_history(request: Request):
    user = _require_user(await get_current_user(request))
    return await cache.get_nav_history(user["google_sub"])


@router.get("/api/portfolio/intraday")
async def get_intraday(request: Request):
    user = _require_user(await get_current_user(request))
    from datetime import date, timedelta
    today = date.today()
    points = await cache.get_intraday_snapshots(user["google_sub"], today.isoformat())
    # Prepend previous day's closing snapshot as baseline (ts="00:00")
    yesterday = (today - timedelta(days=1)).isoformat()
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT total_value FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (user["google_sub"], yesterday),
    )
    row = await cursor.fetchone()
    if row and row["total_value"]:
        points = [{"ts": today.isoformat() + "T00:00", "total_value": row["total_value"]}] + points
    return points


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

    # For withdrawals, check CASH_KRW balance
    google_sub = user["google_sub"]
    if cf_type == "withdrawal":
        cash_item = await cache.get_portfolio_item(google_sub, "CASH_KRW")
        cash_balance = (cash_item["quantity"] * cash_item["avg_price"]) if cash_item else 0
        if cash_balance < amount:
            raise HTTPException(
                status_code=400,
                detail=f"원화 잔액이 부족합니다. (잔액: {cash_balance:,.0f}원, 출금액: {amount:,.0f}원)",
            )

    # Get latest NAV for units calculation
    latest = await cache.get_latest_snapshot(google_sub)
    nav_at_time = latest["nav"] if latest else 1000.0
    units_change = amount / nav_at_time
    if cf_type == "withdrawal":
        units_change = -units_change

    result = await cache.add_cashflow(google_sub, cf_date, cf_type, amount, memo, nav_at_time, units_change)

    # Sync CASH_KRW in portfolio
    cash_item = await cache.get_portfolio_item(google_sub, "CASH_KRW")
    if cash_item:
        delta = amount if cf_type == "deposit" else -amount
        new_qty = max(0, cash_item["quantity"] + int(delta))
        await cache.update_portfolio_quantity(google_sub, "CASH_KRW", new_qty)
    elif cf_type == "deposit":
        await cache.add_portfolio_item(google_sub, "CASH_KRW", "원화", 1.0, int(amount), "KRW")

    return {"ok": True, **result}


@router.delete("/api/portfolio/cashflows/{cf_id}")
async def delete_cashflow(cf_id: int, request: Request):
    user = _require_user(await get_current_user(request))
    google_sub = user["google_sub"]

    # Get cashflow info before deleting to reverse CASH_KRW
    cf = await cache.get_cashflow(google_sub, cf_id)
    await cache.delete_cashflow(google_sub, cf_id)

    if cf:
        cash_item = await cache.get_portfolio_item(google_sub, "CASH_KRW")
        if cash_item:
            # Reverse: deposit was +, so undo is -; withdrawal was -, so undo is +
            reverse_delta = -cf["amount"] if cf["type"] == "deposit" else cf["amount"]
            new_qty = max(0, cash_item["quantity"] + int(reverse_delta))
            await cache.update_portfolio_quantity(google_sub, "CASH_KRW", new_qty)

    return {"ok": True}


# ---------------------------------------------------------------------------
# AI Portfolio Analysis (OpenRouter)
# ---------------------------------------------------------------------------

_AI_MODEL = "google/gemma-4-26b-a4b-it:free"


@router.post("/api/portfolio/ai-analysis")
async def ai_portfolio_analysis(request: Request):
    user = _require_user(await get_current_user(request))
    if not _OPENROUTER_KEY:
        raise HTTPException(status_code=500, detail="AI API 키가 설정되지 않았습니다.")

    google_sub = user["google_sub"]
    items = await cache.get_portfolio(google_sub=google_sub)
    if not items:
        raise HTTPException(status_code=400, detail="포트폴리오가 비어 있습니다.")

    enriched = await _enrich_with_cached_quotes(items)

    # Build holdings summary
    holdings_lines = []
    total_value = 0
    for item in enriched:
        q = item.get("quote", {})
        price = q.get("price")
        qty = item.get("quantity", 0)
        avg = item.get("avg_price", 0)
        mv = price * qty if price and qty else None
        ret = ((price - avg) / avg * 100) if price and avg and avg > 0 else None
        chg = q.get("change_pct")
        name = item.get("stock_name", item["stock_code"])
        line = f"- {name} ({item['stock_code']}): 수량={qty}, 매입가={avg:,.0f}"
        if price:
            line += f", 현재가={price:,.0f}"
        if ret is not None:
            line += f", 수익률={ret:+.1f}%"
        if chg is not None:
            line += f", 일간={chg:+.2f}%"
        if mv:
            line += f", 평가={mv:,.0f}원"
            total_value += mv
        holdings_lines.append(line)

    # NAV / performance
    from datetime import date as _date
    nav_history = await cache.get_nav_history(google_sub)
    perf_lines = []
    if nav_history:
        latest = nav_history[-1]
        first = nav_history[0]
        perf_lines.append(f"NAV: {latest['nav']:.2f} ({first['date']}~{latest['date']})")
        if len(nav_history) > 252:
            yoy = (latest['nav'] / nav_history[-252]['nav'] - 1) * 100
            perf_lines.append(f"YoY: {yoy:+.2f}%")
        days = (_date.fromisoformat(latest['date']) - _date.fromisoformat(first['date'])).days
        if days > 365:
            cagr = ((latest['nav'] / first['nav']) ** (365 / days) - 1) * 100
            perf_lines.append(f"CAGR: {cagr:+.2f}%")

    # Market summary
    try:
        market = await market_indicators.fetch_indicators(["KOSPI", "KOSDAQ", "USD_KRW", "SPX", "US10Y", "OIL_CL"])
        market_lines = [f"- {k}: {v.get('value','')} ({v.get('direction','')}{v.get('change_pct','')})" for k, v in market.items()]
    except Exception:
        market_lines = ["시장 데이터를 가져올 수 없습니다."]

    prompt = f"""당신은 한국 주식 시장 전문 투자 자문가입니다. 아래 포트폴리오를 분석해 주세요.

## 보유 종목 (총 평가: {total_value:,.0f}원)
{chr(10).join(holdings_lines)}

## 성과
{chr(10).join(perf_lines) if perf_lines else "N/A"}

## 시장 현황
{chr(10).join(market_lines)}

분석 항목:
1. 포트폴리오 구성 평가 (분산도, 섹터 편중)
2. 주요 종목 밸류에이션과 리스크
3. 시장 상황 고려 단기 전망
4. 리밸런싱/비중 조절 제안

한국어로 간결하게 답변해 주세요."""

    import json as _json

    async def _stream():
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {_OPENROUTER_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _AI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 2000,
                    "stream": True,
                },
                timeout=60.0,
            )
            input_tokens = 0
            output_tokens = 0
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                payload = line[6:]
                if payload.strip() == "[DONE]":
                    break
                try:
                    chunk = _json.loads(payload)
                    delta = chunk.get("choices", [{}])[0].get("delta", {})
                    content = delta.get("content", "")
                    if content:
                        output_tokens += 1
                        yield f"data: {_json.dumps({'content': content})}\n\n"
                    usage = chunk.get("usage")
                    if usage:
                        input_tokens = usage.get("prompt_tokens", input_tokens)
                        output_tokens = usage.get("completion_tokens", output_tokens)
                except Exception:
                    continue
            yield f"data: {_json.dumps({'done': True, 'input_tokens': input_tokens, 'output_tokens': output_tokens})}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")
