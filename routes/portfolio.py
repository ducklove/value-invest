import asyncio
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

import ai_config
import asset_insights
import cache
import close_price_client
import integrations
import dart_client
import kis_proxy_client
import kis_ws_manager
import market_indicators
import stock_price
from deps import RECENT_QUOTES_SEMAPHORE, get_current_user
from services.portfolio.identifiers import (
    CASH_FX_CODE as _CASH_FX_CODE,
    CASH_NAMES as _CASH_NAMES,
    SPECIAL_ASSETS as _SPECIAL_ASSETS,
    common_stock_code as _common_stock_code,
    is_cash_asset as _is_cash_asset,
    is_korean_stock as _is_korean_stock,
    is_preferred_stock as _is_preferred_stock,
    is_special_asset as _is_special_asset,
    normalize_portfolio_code as _normalize_portfolio_code,
    static_foreign_ticker as _static_foreign_ticker,
)
from services.portfolio.quotes import PortfolioQuoteCache, quote_from_ws as _quote_from_ws
from services.portfolio.targets import (
    evaluate_target_formula as _evaluate_target_formula,
    extract_target_variables as _extract_target_variables,
    parse_target_input as _parse_target_input,
)
from services.portfolio.target_metrics import supplement_target_metrics as _supplement_target_metrics
from services.portfolio.benchmarks import (
    BENCHMARK_ENDPOINT_ITEM_TIMEOUT as _BENCHMARK_ENDPOINT_ITEM_TIMEOUT,
    BENCHMARK_FETCH_TIMEOUT as _BENCHMARK_FETCH_TIMEOUT,
    BENCHMARK_TO_INDICATOR as _BENCHMARK_TO_INDICATOR,
    BENCHMARK_YF_TICKER as _BENCHMARK_YF_TICKER,
    BenchmarkQuoteCache,
    benchmark_name as _benchmark_name,
    benchmark_name_fast as _benchmark_name_fast,
    default_benchmark_for_code as _default_benchmark_for_code,
    fast_default_benchmark_for_code as _fast_default_benchmark_for_code,
    indicator_to_change_pct as _indicator_to_change_pct,
)
from services.portfolio.dividends import (
    due_dividend_warmup_targets as _due_dividend_warmup_targets,
)
from services.portfolio.time_windows import (
    intraday_axis_baseline_ts as _intraday_axis_baseline_ts,
    is_after_settlement_marker as _is_after_settlement_marker,
    portfolio_today_baseline_date as _portfolio_today_baseline_date,
    today_kst_date as _today_kst_date,
)
from services.portfolio.valuation import fetch_valuation_basis as _fetch_common_valuation_basis

_OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "")
_keys_file = Path(__file__).parent.parent / "keys.txt"
if _keys_file.exists():
    for line in _keys_file.read_text().splitlines():
        if line.startswith("OPENROUTER_API_KEY="):
            _OPENROUTER_KEY = line.split("=", 1)[1].strip()

logger = logging.getLogger(__name__)
router = APIRouter()


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
    stock_code = _normalize_portfolio_code(stock_code)
    if stock_code in _SPECIAL_ASSET_NAMES:
        return _SPECIAL_ASSET_NAMES[stock_code]
    if stock_code in _CASH_NAMES:
        return _CASH_NAMES[stock_code]
    static = _static_foreign_ticker(stock_code)
    if static:
        return static["name"]
    if _is_korean_stock(stock_code):
        name = await cache.resolve_stock_name(stock_code)
        if name:
            return name
        return await _fetch_naver_stock_name(stock_code)
    domestic_match = await _resolve_domestic_code_alias(stock_code)
    if domestic_match:
        return domestic_match["corp_name"]
    return await _resolve_foreign_name(stock_code)


async def _resolve_domestic_code_alias(stock_code: str) -> dict | None:
    stock_code = _normalize_portfolio_code(stock_code)
    if (
        not stock_code
        or _is_special_asset(stock_code)
        or _is_korean_stock(stock_code)
        or _static_foreign_ticker(stock_code)
    ):
        return None
    return await cache.resolve_corp_search_query(stock_code)


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
_YAHOO_HTTP_TIMEOUT = httpx.Timeout(6.0, connect=3.0)
_YAHOO_SEM = asyncio.Semaphore(4)
_INSIGHT_QUOTE_TIMEOUT = 8.5
_STATIC_FOREIGN_QUOTE_TIMEOUT = 3.0

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
    if _static_foreign_ticker(code):
        return False
    ts = _dead_quote_cache.get(code)
    if not ts:
        return False
    if (time.monotonic() - ts) < _DEAD_QUOTE_TTL:
        return True
    _dead_quote_cache.pop(code, None)
    return False


def _mark_dead(code: str) -> None:
    if _static_foreign_ticker(code):
        return
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
    """Try yfinance first, then Naver as fallback."""
    static = _static_foreign_ticker(ticker)
    if static:
        return static["name"]
    name = await _yfinance_resolve_name(ticker)
    if name:
        return name
    # Naver fallback
    upper = ticker.upper()
    if "." in upper:
        d = await _fetch_naver_world_stock(upper)
        if d:
            return d.get("stockName") or d.get("stockNameEng")
    for suffix in _EXCHANGE_SUFFIXES:
        code = upper + suffix if suffix else upper
        d = await _fetch_naver_world_stock(code)
        if d:
            return d.get("stockName") or d.get("stockNameEng")
    return None


async def _yfinance_find_ticker(ticker: str) -> str | None:
    """Find a working yfinance ticker, trying various exchange suffixes.
    Bounded by _YF_SEM and a per-call timeout; results (positive and negative)
    are cached to avoid re-running the suffix loop on every quote refresh."""
    static = _static_foreign_ticker(ticker)
    if static:
        return static["ticker"]
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
    static = _static_foreign_ticker(ticker)
    if static:
        return static["name"]
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
    """Find a working yfinance ticker, or fall back to Naver reuters code."""
    # yfinance first — more reliable for foreign stocks
    static = _static_foreign_ticker(ticker)
    if static:
        return static["ticker"]
    found = await _yfinance_find_ticker(ticker)
    if found:
        return found
    # Naver fallback
    upper = ticker.upper()
    if "." in upper:
        d = await _fetch_naver_world_stock(upper)
        if d:
            return d.get("reutersCode") or upper
    for suffix in _EXCHANGE_SUFFIXES:
        code = upper + suffix if suffix else upper
        d = await _fetch_naver_world_stock(code)
        if d:
            return d.get("reutersCode") or code
    return ticker


def _guess_kis_exchanges(ticker: str) -> list[str]:
    """Guess KIS exchange codes from ticker suffix."""
    upper = ticker.upper()
    if upper.endswith(".HK"):
        return ["HKS"]
    if upper.endswith((".T",)):
        return ["TSE"]
    if upper.endswith((".SS",)):
        return ["SHS"]
    if upper.endswith((".SZ",)):
        return ["SZS"]
    # Suffixes that indicate non-KIS markets (AUS, Germany, etc.)
    if upper.endswith((".AX", ".DE", ".F", ".PA", ".AS", ".MI", ".MC", ".SW", ".ST", ".CO", ".HE", ".L", ".HM")):
        return []
    # US-listed: try AMS (NYSE Arca), NAS, NYS
    return ["AMS", "NAS", "NYS"]


async def _kis_fetch_foreign_quote(ticker: str) -> dict:
    """Try fetching quote from KIS overseas API. Returns {} on failure."""
    exchanges = _guess_kis_exchanges(ticker)
    if not exchanges:
        return {}
    # Strip exchange suffix for KIS symbol
    symbol = ticker.split(".")[0].upper()
    for excd in exchanges:
        try:
            data = await kis_proxy_client.get_overseas_quote(symbol, excd)
            s = data.get("summary", {})
            price = s.get("price")
            if price is not None:
                nation = {"NAS": "USA", "NYS": "USA", "AMS": "USA", "HKS": "HKG", "TSE": "JPN", "SHS": "CHN", "SZS": "CHN"}.get(excd, "USA")
                price_krw = await _fx_to_krw(nation, price)
                change = s.get("change") or 0
                change_krw = await _fx_to_krw(nation, change)
                return {
                    "price": round(price_krw),
                    "change": round(change_krw),
                    "change_pct": s.get("change_pct"),
                }
        except Exception as exc:
            logger.debug("KIS overseas quote failed (%s/%s): %s", excd, symbol, exc)
    return {}


async def _fetch_foreign_quote(reuters_code: str) -> dict:
    static = _static_foreign_ticker(reuters_code)
    if static:
        try:
            q = await asyncio.wait_for(
                _yfinance_fetch_quote_fast(static["ticker"]),
                timeout=_STATIC_FOREIGN_QUOTE_TIMEOUT,
            )
        except Exception as exc:
            logger.warning(
                "static foreign quote fast path failed (%s): %s",
                static["ticker"],
                exc,
            )
            q = {}
        if q and q.get("price") is not None:
            return q
        return {}

    # 1. KIS proxy — fastest and most reliable for US/HK stocks
    q = await _kis_fetch_foreign_quote(reuters_code)
    if q and q.get("price") is not None:
        return q

    # 2. yfinance fallback
    q = await _yfinance_fetch_quote(reuters_code)
    if q and q.get("price") is not None:
        return q

    # 3. Naver fallback
    upper_code = reuters_code.upper()
    d = await _fetch_naver_world_stock(upper_code)
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

    return {}


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


def _infer_yf_currency(ticker: str) -> str:
    ticker = (ticker or "").upper()
    if ticker.endswith(".T"):
        return "JPY"
    if ticker.endswith(".HK"):
        return "HKD"
    if ticker.endswith(".SS") or ticker.endswith(".SZ"):
        return "CNY"
    if ticker.endswith(".L"):
        return "GBP"
    if ticker.endswith(".AX"):
        return "AUD"
    if ticker.endswith(".TO"):
        return "CAD"
    if ticker.endswith((".DE", ".F", ".PA", ".AS", ".MI", ".MC")):
        return "EUR"
    return "USD"


async def _fetch_yahoo_chart(ticker: str, *, range_: str = "1y", interval: str = "1d") -> dict:
    ticker = (ticker or "").strip()
    if not ticker:
        return {"rows": [], "currency": None, "meta": {}}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker, safe='')}"
    try:
        async with _YAHOO_SEM:
            async with httpx.AsyncClient(timeout=_YAHOO_HTTP_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(
                    url,
                    params={"range": range_, "interval": interval, "includePrePost": "false"},
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                resp.raise_for_status()
        result = (((resp.json() or {}).get("chart") or {}).get("result") or [None])[0]
        if not result:
            return {"rows": [], "currency": None, "meta": {}}
        meta = result.get("meta") or {}
        timestamps = result.get("timestamp") or []
        quote_data = (((result.get("indicators") or {}).get("quote") or [{}])[0] or {})
        closes = quote_data.get("close") or []
        rows = []
        for ts, close in zip(timestamps, closes):
            try:
                if close is None:
                    continue
                rows.append({
                    "date": datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat(),
                    "close": round(float(close), 6),
                })
            except Exception:
                continue
        return {
            "rows": rows,
            "currency": (meta.get("currency") or _infer_yf_currency(ticker)).upper(),
            "meta": meta,
        }
    except Exception as exc:
        logger.warning("Yahoo chart fetch failed (%s): %s", ticker, exc)
        return {"rows": [], "currency": None, "meta": {}}


async def _yfinance_fetch_quote_fast(ticker: str) -> dict:
    if ticker in _failed_yf_tickers:
        return {}

    try:
        payload = await asyncio.wait_for(_fetch_yahoo_chart(ticker, range_="5d"), timeout=7.0)
        values = [row["close"] for row in payload.get("rows") or [] if row.get("close") is not None]
        if not values:
            return {}
        meta = payload.get("meta") or {}
        price = float(meta.get("regularMarketPrice") or values[-1])
        prev = float(meta.get("chartPreviousClose") or (values[-2] if len(values) >= 2 else values[-1]))
        if price is None:
            return {}
        change = round(price - prev, 4) if prev else 0
        change_pct = round(change / prev * 100, 2) if prev else None
        currency = (payload.get("currency") or _infer_yf_currency(ticker)).upper()
        fx_rate = await _insight_fx_rate(currency)
        price_krw = price * fx_rate
        change_krw = change * fx_rate
        return {
            "price": round(price_krw),
            "change": round(change_krw),
            "change_pct": change_pct,
        }
    except Exception as exc:
        logger.warning("fast yfinance quote failed (%s): %s", ticker, exc)
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


_fx_daily_cache: dict[str, tuple[float, dict]] = {}  # fx_code -> (ts, {price, change, change_pct})
_FX_DAILY_CACHE_TTL = 300


async def _fetch_fx_daily_change(fx_code: str) -> dict:
    """Fetch today's FX rate + change vs. previous business day from Naver.

    Uses the per-currency daily-quote page, same pattern as KRX gold.
    Returns {} on failure; caller can fall back to a plain rate lookup.
    """
    import time
    cached = _fx_daily_cache.get(fx_code)
    if cached and (time.time() - cached[0]) < _FX_DAILY_CACHE_TTL:
        return cached[1]
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                f"https://finance.naver.com/marketindex/exchangeDailyQuote.naver?marketindexCd={fx_code}",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            html = resp.content.decode("euc-kr", errors="ignore")
            rows = re.findall(
                r'<tr class="(?:up|down)">\s*<td class="date">[^<]+</td>\s*<td class="num">([\d,\.]+)</td>',
                html,
            )
            if len(rows) >= 2:
                price = float(rows[0].replace(",", ""))
                prev = float(rows[1].replace(",", ""))
                change = price - prev
                change_pct = round(change / prev * 100, 2) if prev else 0.0
                result = {"price": price, "change": change, "change_pct": change_pct}
                _fx_daily_cache[fx_code] = (time.time(), result)
                return result
            if rows:
                # Only one row available (first listing day?) — no delta.
                price = float(rows[0].replace(",", ""))
                result = {"price": price, "change": 0.0, "change_pct": 0.0}
                _fx_daily_cache[fx_code] = (time.time(), result)
                return result
    except Exception as e:
        logger.warning("FX daily fetch failed for %s: %s", fx_code, e)
    return {}


async def _fetch_cash_quote(stock_code: str) -> dict:
    """Fetch cash quote: KRW=1, others=FX rate to KRW with daily change."""
    if stock_code == "CASH_KRW":
        return {"price": 1, "change": 0, "change_pct": 0}
    fx_code = _CASH_FX_CODE.get(stock_code)
    if not fx_code:
        return {}
    unit = _FX_UNIT.get(fx_code, 1)
    # Prefer the per-currency daily-quote scrape — gives us change vs prev close.
    daily = await _fetch_fx_daily_change(fx_code)
    if daily.get("price"):
        price = daily["price"] / unit
        change = daily["change"] / unit
        return {
            "price": round(price, 2),
            "change": round(change, 4),
            "change_pct": daily["change_pct"],
        }
    # Fallback: exchangeList scrape — current rate only, no change.
    rates = await _get_fx_rates()
    rate = rates.get(fx_code)
    if not rate:
        return {}
    return {"price": round(rate / unit, 2), "change": 0, "change_pct": 0}


_quote_cache = PortfolioQuoteCache()


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
    cached = _quote_cache.get_fresh(stock_code)
    if cached:
        return cached
    if _is_dead(stock_code):
        return _quote_cache.get_fallback(stock_code)
    if _is_cash_asset(stock_code):
        q = await _fetch_cash_quote(stock_code)
    elif stock_code == "KRX_GOLD":
        q = await _fetch_krx_gold_quote()
    elif stock_code in _CRYPTO_UPBIT_MAP:
        q = await _fetch_crypto_quote(stock_code)
    elif _is_korean_stock(stock_code):
        q = _quote_from_ws(kis_ws_manager.get_cached_quote(stock_code))
        if not q:
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
    if not _quote_cache.remember(stock_code, q):
        _mark_dead(stock_code)
        fallback = _quote_cache.get_fallback(stock_code)
        if fallback:
            return fallback
    return q


def _cached_quote_for_code(code: str) -> dict:
    ws_quote = _quote_from_ws(kis_ws_manager.get_cached_quote(code))
    if ws_quote:
        return ws_quote
    return _quote_cache.get_cached(code)


async def _enrich_with_cached_quotes(items: list[dict]) -> list[dict]:
    """Attach cached quotes — WebSocket cache preferred, then polling cache."""
    result = []
    for item in items:
        enriched = dict(item)
        enriched["quote"] = _cached_quote_for_code(item["stock_code"])
        result.append(enriched)
    return result


async def _fill_snapshot_quotes(google_sub: str, items: list[dict]) -> None:
    if not items:
        return
    latest = await cache.get_latest_snapshot(google_sub)
    snap_date = latest.get("date") if latest else None
    if not snap_date:
        return
    rows = await cache.get_stock_snapshots_by_date(google_sub, snap_date)
    values = {row["stock_code"]: row["market_value"] for row in rows}
    for item in items:
        quote = item.get("quote") or {}
        if quote.get("price") is not None:
            continue
        value = values.get(item.get("stock_code"))
        qty = item.get("quantity")
        try:
            if value is None or qty is None or float(qty) == 0:
                continue
            item["quote"] = {
                "date": snap_date,
                "price": round(float(value) / float(qty), 4),
                "change": 0,
                "change_pct": None,
                "_stale": True,
            }
        except (TypeError, ValueError, ZeroDivisionError):
            continue


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
    static = _static_foreign_ticker(stock_code)
    if static:
        return static["currency"]
    # yfinance first
    try:
        import yfinance as yf
        found = await _yfinance_find_ticker(stock_code)
        if found:
            def _curr(c):
                return (yf.Ticker(c).fast_info.currency or "USD").upper()
            return await _yf_run(partial(_curr, found))
    except (asyncio.TimeoutError, Exception):
        pass
    # Naver fallback
    d = await _fetch_naver_world_stock(stock_code.upper())
    if d:
        nation = d.get("nationType", "")
        return _NATION_TO_CURRENCY.get(nation, "USD")
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
    market_type = await _detect_market_type(code) if _is_korean_stock(code) and not _is_preferred_stock(code) else None
    return _default_benchmark_for_code(code, market_type=market_type)


def _resolve_default_benchmark_fast(code: str) -> str:
    """Cheap benchmark fallback for first-paint portfolio loading."""
    return _fast_default_benchmark_for_code(code, cached_market_type=_market_type_cache.get(code))

_benchmark_name_cache: dict[str, str] = {}


async def _resolve_benchmark_name(code: str) -> str:
    """Resolve a benchmark code to a human-readable name."""
    builtin_name = _benchmark_name(code)
    if builtin_name:
        return builtin_name
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

_benchmark_quote_cache = BenchmarkQuoteCache()


def _cached_benchmark_quote(benchmark_code: str, *, allow_stale: bool = True) -> dict | None:
    return _benchmark_quote_cache.get(benchmark_code, allow_stale=allow_stale)


def _resolve_benchmark_name_fast(code: str, items: list[dict] | None = None) -> str:
    return _benchmark_name_fast(code, items, _benchmark_name_cache)


async def _fetch_benchmark_quote(benchmark_code: str) -> dict:
    """Fetch a benchmark quote (cached). Reuses market_indicators for shared sources."""
    cached = _cached_benchmark_quote(benchmark_code, allow_stale=False)
    if cached is not None:
        return cached

    indicator_code = _BENCHMARK_TO_INDICATOR.get(benchmark_code)
    if indicator_code:
        try:
            data = await asyncio.wait_for(
                market_indicators.fetch_indicators([indicator_code]),
                timeout=_BENCHMARK_FETCH_TIMEOUT,
            )
            pct = _indicator_to_change_pct(data.get(indicator_code) or {})
            q = {"change_pct": pct} if pct is not None else {}
        except Exception as e:
            logger.warning("Indicator-based benchmark fetch failed for %s: %s", benchmark_code, e)
            return _cached_benchmark_quote(benchmark_code, allow_stale=True) or {}
    elif benchmark_code.startswith("FX_"):
        daily = await _fetch_fx_daily_change(benchmark_code)
        q = {"change_pct": daily.get("change_pct")} if daily and daily.get("change_pct") is not None else {}
    else:
        # It's a stock code (e.g., common stock for preferred)
        # For codes with dots/slashes, try dash variant directly first (faster)
        preset_ticker = _BENCHMARK_YF_TICKER.get(benchmark_code)
        if preset_ticker:
            stock_q = await _yfinance_fetch_quote_fast(preset_ticker)
        else:
            alt = _yfinance_direct_ticker(benchmark_code) if not _is_korean_stock(benchmark_code) else None
            stock_q = None
        if not preset_ticker and alt and alt != benchmark_code:
            stock_q = await _yfinance_fetch_quote_fast(alt)
            if not stock_q or not stock_q.get("change_pct"):
                stock_q = await _fetch_quote(benchmark_code)
        elif not preset_ticker:
            stock_q = await _fetch_quote(benchmark_code)
        q = {"change_pct": stock_q.get("change_pct")} if stock_q else {}

    _benchmark_quote_cache.set(benchmark_code, q)
    return q


_ASSET_HISTORY_CACHE_TTL = 15 * 60
_asset_history_cache: dict[str, tuple[float, dict]] = {}
_insight_fx_cache: dict[str, tuple[float, float]] = {}
_insight_item_warm_cache: dict[str, float] = {}
_insight_common_warm_ts: float = 0.0
_insight_warmup_task: asyncio.Task | None = None
_INSIGHT_WARMUP_TTL = 15 * 60
_dividend_warmup_last: dict[str, float] = {}
_dividend_warmup_tasks: dict[str, asyncio.Task] = {}


async def _refresh_domestic_dividend_from_dart(code: str) -> int:
    corp_code = await cache.get_corp_code(code)
    if not corp_code:
        return 0
    current_year = datetime.now().year
    dividends = await dart_client.fetch_dividend_per_share_by_year(
        corp_code,
        start_year=max(current_year - 3, dart_client.DART_ANNUAL_DATA_START_YEAR),
        end_year=current_year - 1,
    )
    return await cache.upsert_market_dividends(code, dividends)


async def _warm_market_data_for_dividend(code: str) -> None:
    code = _normalize_portfolio_code(code)
    try:
        updated = await _refresh_domestic_dividend_from_dart(code)
        if updated:
            logger.info("Portfolio DART dividend warmup completed (%s, %d rows)", code, updated)
            return
        latest_dividend_years = await cache.get_latest_dividend_years([code])
        if latest_dividend_years.get(code, 0) >= datetime.now().year - 1:
            return
        fin_data = await cache.get_financial_data(code)
        corp_code = await cache.get_corp_code(code)
        refreshed = await stock_price.fetch_market_data(code, fin_data, corp_code=corp_code)
        if refreshed:
            await cache.save_market_data(code, refreshed)
            logger.info("Portfolio dividend market-data warmup completed (%s, %d rows)", code, len(refreshed))
    except Exception as exc:
        logger.warning("Portfolio dividend market-data warmup failed (%s): %s", code, exc)
    finally:
        _dividend_warmup_tasks.pop(code, None)


def _consume_dividend_warmup_result(code: str, task: asyncio.Task) -> None:
    _dividend_warmup_tasks.pop(code, None)
    try:
        task.exception()
    except asyncio.CancelledError:
        pass


def _running_dividend_warmup_codes() -> set[str]:
    return {
        code
        for code, task in _dividend_warmup_tasks.items()
        if task and not task.done()
    }


def _start_dividend_warmup_task(code: str, now: float) -> asyncio.Task | None:
    _dividend_warmup_last[code] = now
    try:
        async def _delayed_warmup():
            await asyncio.sleep(10)
            await _warm_market_data_for_dividend(code)

        task = asyncio.create_task(_delayed_warmup())
    except RuntimeError:
        return None
    _dividend_warmup_tasks[code] = task
    task.add_done_callback(lambda t, c=code: _consume_dividend_warmup_result(c, t))
    return task


def _schedule_portfolio_dividend_warmup(codes: list[str]) -> None:
    now = time.monotonic()
    due = _due_dividend_warmup_targets(codes, now, _dividend_warmup_last, running_codes=_running_dividend_warmup_codes())
    for code in due:
        _start_dividend_warmup_task(code, now)


async def _warm_portfolio_dividends_for_response(codes: list[str], timeout: float = 2.5) -> None:
    now = time.monotonic()
    due = _due_dividend_warmup_targets(codes, now, _dividend_warmup_last, running_codes=_running_dividend_warmup_codes())
    tasks = [
        task
        for code in due
        if (task := _start_dividend_warmup_task(code, now)) is not None
    ]
    if not tasks:
        return
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    for task in done:
        try:
            task.exception()
        except asyncio.CancelledError:
            pass
    if pending:
        logger.info("Portfolio dividend warmup still running in background (%d pending)", len(pending))

_INSIGHT_FX_TICKER = {
    "USD": "KRW=X",
    "EUR": "EURKRW=X",
    "JPY": "JPYKRW=X",
    "CNY": "CNYKRW=X",
    "HKD": "HKDKRW=X",
    "GBP": "GBPKRW=X",
    "AUD": "AUDKRW=X",
    "CAD": "CADKRW=X",
    "CHF": "CHFKRW=X",
}

_BENCHMARK_YF_TICKER = {
    "IDX_KOSPI": "^KS11",
    "IDX_KOSDAQ": "^KQ11",
    "IDX_SP500": "^GSPC",
    "GOLD": "GC=F",
    "AGG": "AGG",
    "FX_USDKRW": "KRW=X",
    "FX_EURKRW": "EURKRW=X",
    "FX_JPYKRW": "JPYKRW=X",
    "FX_CNYKRW": "CNYKRW=X",
}

_LOCAL_BENCHMARK_INDEX_SERIES = {
    "IDX_KOSPI": "KOSPI",
    "IDX_KOSDAQ": "KOSDAQ",
    "IDX_SP500": "SP500",
}
_LOCAL_BENCHMARK_COMMODITIES = {
    "GOLD": "gold",
}


def _yfinance_direct_ticker(code: str) -> str:
    ticker = (code or "").strip()
    if "/" in ticker:
        ticker = ticker.replace("/", "-")
    if "." in ticker:
        prefix, suffix = ticker.rsplit(".", 1)
        # Yahoo uses BRK-B/BF-B for US class shares, but keeps exchange
        # suffixes such as 7203.T unchanged.
        if len(suffix) == 1 and prefix.replace(".", "").isalpha():
            ticker = f"{prefix}-{suffix}"
    return ticker


async def _insight_fx_rate(currency: str | None) -> float:
    currency = (currency or "KRW").upper()
    if currency == "KRW":
        return 1.0
    now = _time.monotonic()
    cached = _insight_fx_cache.get(currency)
    if cached and (now - cached[0]) < 300:
        return cached[1]
    ticker = _INSIGHT_FX_TICKER.get(currency)
    if not ticker:
        return 1.0
    payload = await asyncio.wait_for(_fetch_yahoo_chart(ticker, range_="5d"), timeout=7.0)
    rows = payload.get("rows") or []
    meta = payload.get("meta") or {}
    rate = asset_insights.safe_float(meta.get("regularMarketPrice"))
    if rate is None and rows:
        rate = asset_insights.safe_float(rows[-1].get("close"))
    if rate is None or rate <= 0:
        return 1.0
    _insight_fx_cache[currency] = (now, rate)
    return rate


async def _resolve_insight_benchmark(item: dict) -> str:
    code = item["stock_code"]
    manual = item.get("benchmark_code")
    if manual:
        return manual
    if code == "KRX_GOLD":
        return "GOLD"
    if code in {"CRYPTO_BTC", "CRYPTO_ETH"}:
        return "IDX_SP500"
    if _is_cash_asset(code):
        return _CASH_FX_CODE.get(code, "FX_USDKRW")
    profile = asset_insights.classify_asset(code, item.get("stock_name") or "", item.get("currency") or "")
    if profile.get("assetClass") == "bond_etf":
        return "AGG"
    return await _resolve_default_benchmark(code)


async def _download_yfinance_history(ticker: str, period: str = "1y") -> dict:
    ticker = (ticker or "").strip()
    if not ticker:
        return {"rows": [], "currency": None}
    key = f"{ticker}:{period}"
    now = _time.monotonic()
    cached = _asset_history_cache.get(key)
    if cached and (now - cached[0]) < _ASSET_HISTORY_CACHE_TTL:
        return cached[1]

    try:
        payload = await asyncio.wait_for(_fetch_yahoo_chart(ticker, range_=period), timeout=7.0)
    except Exception as exc:
        logger.warning("asset insight history fetch failed (%s): %s", ticker, exc)
        payload = {"rows": [], "currency": None}
    result = {
        "rows": payload.get("rows") or [],
        "currency": payload.get("currency") or _infer_yf_currency(ticker),
    }
    _asset_history_cache[key] = (now, result)
    return result


async def _download_korean_history(code: str, period_days: int = 370) -> dict:
    code = (code or "").strip()
    if not _is_korean_stock(code):
        return {"rows": [], "currency": None}
    key = f"KIS:{code}:{period_days}"
    now = _time.monotonic()
    cached = _asset_history_cache.get(key)
    if cached and (now - cached[0]) < _ASSET_HISTORY_CACHE_TTL:
        return cached[1]

    end_date = date.today()
    start_date = end_date - timedelta(days=period_days)
    if code.isdigit():
        try:
            local_rows = await asyncio.wait_for(
                close_price_client.get_daily_closes(code, since=start_date, until=end_date),
                timeout=3.0,
            )
        except Exception as exc:
            logger.info("Local Korean asset insight history unavailable (%s): %s", code, exc)
            local_rows = []
        if local_rows:
            result = {
                "rows": [
                    {"date": row["date"], "close": round(float(row["close"]), 6)}
                    for row in local_rows
                    if row.get("date") and row.get("close") is not None
                ],
                "currency": "KRW",
            }
            _asset_history_cache[key] = (_time.monotonic(), result)
            return result

    try:
        payload = await asyncio.wait_for(
            kis_proxy_client.get_history(
                code,
                start_date=start_date,
                end_date=end_date,
                period="D",
                adjusted=True,
            ),
            timeout=8.0,
        )
    except Exception as exc:
        logger.warning("Korean asset insight history fetch failed (%s): %s", code, exc)
        return {"rows": [], "currency": "KRW"}

    items = payload.get("items") if isinstance(payload, dict) else []
    if not items and isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list) and value:
                items = value
                break
    rows = []
    for item in stock_price._sorted_history_items(items):
        trade_date = stock_price._parse_date(
            stock_price._get_first(item, "stck_bsop_date", "date", "trade_date", "business_date")
        )
        close = stock_price._safe_float(
            stock_price._get_first(item, "stck_clpr", "close_price", "close"),
            zero_as_none=False,
        )
        if trade_date and close is not None:
            rows.append({"date": trade_date.isoformat(), "close": round(float(close), 6)})

    result = {"rows": rows, "currency": "KRW"}
    _asset_history_cache[key] = (now, result)
    return result


async def _download_local_benchmark_history(benchmark_code: str, period_days: int = 370) -> list[dict]:
    series_id = _LOCAL_BENCHMARK_INDEX_SERIES.get(benchmark_code)
    commodity = _LOCAL_BENCHMARK_COMMODITIES.get(benchmark_code)
    if not series_id and not commodity:
        return []

    key = f"LOCAL_BENCH:{benchmark_code}:{period_days}"
    now = _time.monotonic()
    cached = _asset_history_cache.get(key)
    if cached and (now - cached[0]) < _ASSET_HISTORY_CACHE_TTL:
        return cached[1].get("rows") or []

    end_date = date.today()
    start_date = end_date - timedelta(days=period_days)
    try:
        if series_id:
            rows = await asyncio.wait_for(
                close_price_client.get_macro_index(series_id, since=start_date, until=end_date),
                timeout=2.0,
            )
        else:
            rows = await asyncio.wait_for(
                close_price_client.get_macro_commodity(commodity, since=start_date, until=end_date),
                timeout=2.0,
            )
    except Exception as exc:
        logger.info("Local benchmark insight history unavailable (%s): %s", benchmark_code, exc)
        rows = []

    normalized = [
        {"date": row["date"], "close": round(float(row["close"]), 6)}
        for row in rows
        if row.get("date") and row.get("close") is not None
    ]
    if normalized:
        _asset_history_cache[key] = (now, {"rows": normalized, "currency": "KRW" if series_id else "USD"})
    return normalized


async def _asset_history_for_insight(code: str, item: dict) -> dict:
    special = asset_insights.yfinance_ticker_for_special_asset(code)
    if special:
        return await _download_yfinance_history(special)
    if _is_cash_asset(code):
        return {"rows": [], "currency": code.replace("CASH_", "")}
    if _is_korean_stock(code):
        return await _download_korean_history(code)
    static = _static_foreign_ticker(code)
    if static:
        return await _download_yfinance_history(static["ticker"])
    await _ensure_ticker_map()
    ticker = _ticker_map.get(code) or _yfinance_direct_ticker(code)
    return await _download_yfinance_history(ticker)


async def _benchmark_history_for_insight(benchmark_code: str | None) -> list[dict]:
    if not benchmark_code:
        return []
    if _is_korean_stock(benchmark_code):
        payload = await _download_korean_history(benchmark_code)
        return payload.get("rows") or []
    local_rows = await _download_local_benchmark_history(benchmark_code)
    if local_rows:
        return local_rows
    ticker = _BENCHMARK_YF_TICKER.get(benchmark_code)
    if not ticker:
        ticker = _yfinance_direct_ticker(benchmark_code)
    if not ticker:
        return []
    payload = await _download_yfinance_history(ticker)
    return payload.get("rows") or []


async def _fetch_quote_for_insight(stock_code: str) -> dict:
    if _is_cash_asset(stock_code) or stock_code == "KRX_GOLD" or stock_code in _CRYPTO_UPBIT_MAP or _is_korean_stock(stock_code):
        return await _fetch_quote(stock_code)

    cached = _quote_cache.get_fresh(stock_code)
    if cached:
        return cached
    stale = _quote_cache.get_fallback(stock_code)

    await _ensure_ticker_map()
    static = _static_foreign_ticker(stock_code)
    ticker = (
        (static["ticker"] if static else None)
        or _ticker_map.get(stock_code)
        or _yfinance_direct_ticker(stock_code)
    )
    try:
        q = await asyncio.wait_for(_yfinance_fetch_quote_fast(ticker), timeout=_INSIGHT_QUOTE_TIMEOUT)
    except Exception as exc:
        logger.warning("asset insight quote fetch failed (%s): %s", stock_code, exc)
        return stale
    if _quote_cache.remember(stock_code, q):
        return q
    return stale


def _macro_codes_for_asset(profile: dict, currency: str | None) -> list[str]:
    asset_class = profile.get("assetClass")
    currency = (currency or "").upper()
    codes = ["USD_KRW"]
    if asset_class in {"foreign_stock", "foreign_etf"}:
        codes.extend(["SPX", "IXIC", "US10Y"])
    elif asset_class == "korean_stock":
        codes.extend(["KOSPI", "KOSDAQ", "KR3Y"])
    elif asset_class == "bond_etf":
        codes.extend(["US10Y", "KR3Y"])
    elif asset_class == "gold":
        codes.extend(["CMDT_GC", "US10Y"])
    elif asset_class == "crypto":
        codes.extend(["SPX", "US10Y"])
    elif asset_class == "cash":
        codes.extend(["US10Y", "KR3Y"])
    if currency == "EUR":
        codes.append("EUR_KRW")
    elif currency == "JPY":
        codes.append("JPY_KRW")
    elif currency == "CNY":
        codes.append("CNY_KRW")
    return list(dict.fromkeys(codes))


def _format_macro(indicators: dict) -> list[dict]:
    result = []
    for code, data in indicators.items():
        if not data:
            continue
        catalog = market_indicators.CATALOG.get(code, {})
        result.append({
            "code": code,
            "label": catalog.get("label", code),
            "category": catalog.get("category", ""),
            "value": data.get("value") or "",
            "change": data.get("change") or "",
            "changePct": data.get("change_pct") or "",
            "direction": data.get("direction") or "",
        })
    return result


async def _fetch_insight_indicators(codes: list[str]) -> dict:
    try:
        return await asyncio.wait_for(market_indicators.fetch_indicators(codes), timeout=5.0)
    except Exception as exc:
        logger.warning("asset insight indicator fetch failed: %s", exc)
        return {}


async def warm_asset_insight_common(initial_delay_seconds: float = 0.0) -> None:
    """Warm shared dependencies that made the first asset-insight click slow."""
    global _insight_common_warm_ts
    if initial_delay_seconds > 0:
        await asyncio.sleep(initial_delay_seconds)
    now = _time.monotonic()
    if _insight_common_warm_ts and (now - _insight_common_warm_ts) < _INSIGHT_WARMUP_TTL:
        return
    try:
        await asyncio.gather(
            _insight_fx_rate("USD"),
            _download_yfinance_history("^KS11"),
            _download_yfinance_history("^KQ11"),
            _download_yfinance_history("^GSPC"),
            _download_yfinance_history("AGG"),
            _download_yfinance_history("GC=F"),
            _fetch_benchmark_quote("IDX_SP500"),
            _fetch_benchmark_quote("IDX_KOSPI"),
            _fetch_benchmark_quote("IDX_KOSDAQ"),
            _fetch_insight_indicators(["USD_KRW", "KOSPI", "KOSDAQ", "SPX", "IXIC", "US10Y", "KR3Y", "CMDT_GC"]),
            return_exceptions=True,
        )
        _insight_common_warm_ts = _time.monotonic()
        logger.info("Portfolio asset insight common warmup completed")
    except Exception as exc:
        logger.warning("Portfolio asset insight common warmup failed: %s", exc)


def _is_asset_insight_candidate(code: str) -> bool:
    return bool((code or "").strip())


async def _warm_asset_insight_item(item: dict) -> None:
    code = item.get("stock_code") or ""
    if not _is_asset_insight_candidate(code):
        return
    now = _time.monotonic()
    last = _insight_item_warm_cache.get(code)
    if last and (now - last) < _INSIGHT_WARMUP_TTL:
        return

    effective_benchmark = await _resolve_insight_benchmark(item)
    profile = {
        "code": code,
        "name": item.get("stock_name") or code,
        "currency": item.get("currency") or "",
        **asset_insights.classify_asset(code, item.get("stock_name") or "", item.get("currency") or ""),
    }
    await asyncio.gather(
        _fetch_quote_for_insight(code),
        _asset_history_for_insight(code, item),
        _benchmark_history_for_insight(effective_benchmark),
        _fetch_insight_indicators(_macro_codes_for_asset(profile, item.get("currency"))),
        return_exceptions=True,
    )
    _insight_item_warm_cache[code] = _time.monotonic()


async def warm_asset_insights_for_items(items: list[dict]) -> None:
    await warm_asset_insight_common()
    limit = int(os.environ.get("PORTFOLIO_INSIGHT_WARMUP_LIMIT", "4"))
    candidates = [it for it in items if _is_asset_insight_candidate(it.get("stock_code") or "")]
    if limit > 0:
        candidates = candidates[:limit]
    if not candidates:
        return
    await asyncio.gather(*[_warm_asset_insight_item(it) for it in candidates], return_exceptions=True)


def _schedule_asset_insight_warmup(items: list[dict]) -> None:
    global _insight_warmup_task
    if os.environ.get("PORTFOLIO_INSIGHT_WARMUP", "0") != "1":
        return
    if _insight_warmup_task and not _insight_warmup_task.done():
        return
    try:
        _insight_warmup_task = asyncio.create_task(warm_asset_insights_for_items(items))
        _insight_warmup_task.add_done_callback(lambda task: task.exception())
    except RuntimeError:
        return


def _gold_gap_for_asset(code: str) -> dict | None:
    config = integrations.build_public_integrations().get("goldGap", {})
    asset_key = (config.get("assetByPortfolioCode") or {}).get(code)
    if not asset_key:
        return None
    asset = (config.get("assets") or {}).get(asset_key) or {}
    return {
        "asset": asset_key,
        "label": asset.get("label") or asset_key,
        "latestGapPct": asset.get("latestGapPct"),
        "latestDate": asset.get("latestDate"),
        "thresholdPct": asset.get("thresholdPct"),
        "updatedAt": config.get("updatedAt"),
        "url": f"{config.get('baseUrl', '').rstrip('/')}/?asset={asset_key}" if config.get("baseUrl") else "",
    }


def _holding_context_for_asset(code: str) -> dict | None:
    config = integrations.build_public_integrations().get("holdingValue") or {}
    meta = (config.get("meta") or {}).get(code)
    if not isinstance(meta, dict):
        return None
    subsidiaries = meta.get("subsidiaries") or []
    base_url = str(config.get("baseUrl") or "").rstrip("/")
    return {
        "applicable": True,
        "code": code,
        "baseUrl": base_url,
        "url": f"{base_url}/?code={quote(code)}" if base_url else "",
        "subsidiaryCount": len(subsidiaries),
        "meta": meta,
    }


async def _fetch_insight_valuation_basis(stock_code: str) -> dict:
    return await _fetch_common_valuation_basis(stock_code, as_of=_today_kst_date())


def _build_insight_valuation(quote: dict | None, basis: dict) -> dict:
    if not basis.get("applicable"):
        return {"applicable": False}
    price = asset_insights.safe_float((quote or {}).get("price")) or asset_insights.safe_float(basis.get("closePrice"))
    calculated = asset_insights.calculate_valuation_metrics(
        price=price,
        eps=basis.get("eps"),
        bps=basis.get("bps"),
        net_income=basis.get("netIncome"),
        equity=basis.get("equity"),
        per=basis.get("perFallback"),
        pbr=basis.get("pbrFallback"),
    )
    return {
        "applicable": True,
        **calculated,
        "source": basis.get("source"),
        "sourceCode": basis.get("sourceCode"),
        "fiscalYear": basis.get("fiscalYear"),
        "asOf": basis.get("asOf"),
        "treasuryShareRatioPct": basis.get("treasuryShareRatioPct"),
        "treasuryShares": basis.get("treasuryShares"),
        "issuedShares": basis.get("issuedShares"),
        "outstandingShares": basis.get("outstandingShares"),
    }


def _normalize_portfolio_tags(raw_tags) -> list[str]:
    if raw_tags is None:
        return []
    if isinstance(raw_tags, str):
        parts = re.split(r"[,#\n]+", raw_tags)
    elif isinstance(raw_tags, list):
        parts = raw_tags
    else:
        raise HTTPException(status_code=400, detail="tags는 문자열 또는 배열이어야 합니다.")

    tags: list[str] = []
    seen: set[str] = set()
    for raw in parts:
        tag = re.sub(r"\s+", " ", str(raw or "").strip().lstrip("#"))[:30]
        if not tag:
            continue
        key = tag.casefold()
        if key in seen:
            continue
        seen.add(key)
        tags.append(tag)
        if len(tags) >= 12:
            break
    return tags


@router.get("/api/portfolio/asset-insight/{stock_code}")
async def asset_insight(stock_code: str, request: Request):
    user = _require_user(await get_current_user(request))
    stock_code = stock_code.strip()
    items = await cache.get_portfolio(user["google_sub"])
    item = next((it for it in items if it["stock_code"] == stock_code), None)
    if not item:
        raise HTTPException(status_code=404, detail="포트폴리오에 없는 종목입니다.")

    quote_task = asyncio.create_task(_fetch_quote_for_insight(stock_code))
    asset_history_task = asyncio.create_task(_asset_history_for_insight(stock_code, item))
    effective_benchmark = await _resolve_insight_benchmark(item)

    profile = {
        "code": stock_code,
        "name": item.get("stock_name") or stock_code,
        "currency": item.get("currency") or "",
        "benchmarkCode": effective_benchmark,
        **asset_insights.classify_asset(stock_code, item.get("stock_name") or "", item.get("currency") or ""),
    }
    indicator_task = asyncio.create_task(_fetch_insight_indicators(_macro_codes_for_asset(profile, item.get("currency"))))
    valuation_task = asyncio.create_task(_fetch_insight_valuation_basis(stock_code))

    quote, history_payload, benchmark_quote, benchmark_name, benchmark_rows, indicators, valuation_basis = await asyncio.gather(
        quote_task,
        asset_history_task,
        _fetch_benchmark_quote(effective_benchmark),
        _resolve_benchmark_name(effective_benchmark),
        _benchmark_history_for_insight(effective_benchmark),
        indicator_task,
        valuation_task,
    )
    profile["benchmarkName"] = benchmark_name

    metrics = asset_insights.calculate_history_metrics(history_payload.get("rows") or [])
    benchmark_metrics = asset_insights.calculate_history_metrics(benchmark_rows)
    benchmark_returns = benchmark_metrics.get("returns") or {}
    relative = asset_insights.relative_returns(metrics.get("returns") or {}, benchmark_returns)
    position = asset_insights.calculate_position(item, quote)
    valuation = _build_insight_valuation(quote, valuation_basis)
    gold_gap = _gold_gap_for_asset(stock_code)
    holding = _holding_context_for_asset(stock_code)
    tags_task = asyncio.create_task(cache.get_portfolio_tags(user["google_sub"], stock_code))
    tag_suggestions_task = asyncio.create_task(cache.get_portfolio_tag_suggestions(user["google_sub"]))

    benchmark = {
        "code": effective_benchmark,
        "name": benchmark_name,
        "dayChangePct": benchmark_quote.get("change_pct") if benchmark_quote else None,
        "returns": benchmark_returns,
        "relativeReturns": relative,
    }
    return {
        "profile": profile,
        "position": position,
        "quote": quote or {},
        "valuation": valuation,
        "metrics": metrics,
        "benchmark": benchmark,
        "macro": _format_macro(indicators),
        "goldGap": gold_gap,
        "holding": holding,
        "tags": await tags_task,
        "tagSuggestions": await tag_suggestions_task,
        "history": (history_payload.get("rows") or [])[-80:],
        "dataQuality": {
            "historyCurrency": history_payload.get("currency"),
            "historyPoints": metrics.get("historyPoints", 0),
            "benchmarkPoints": benchmark_metrics.get("historyPoints", 0),
        },
        "signals": asset_insights.build_signals(profile, position, metrics, benchmark, gold_gap),
    }


@router.put("/api/portfolio/{stock_code}/tags")
async def update_portfolio_tags(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    stock_code = stock_code.strip()
    item = await cache.get_portfolio_item(user["google_sub"], stock_code)
    if not item:
        raise HTTPException(status_code=404, detail="포트폴리오에 없는 종목입니다.")
    tags = _normalize_portfolio_tags((payload or {}).get("tags"))
    saved = await cache.set_portfolio_tags(user["google_sub"], stock_code, tags)
    return {
        "ok": True,
        "stock_code": stock_code,
        "tags": saved,
        "tagSuggestions": await cache.get_portfolio_tag_suggestions(user["google_sub"]),
    }


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
_ASSET_QUOTES_BATCH_TIMEOUT = 4.5
_ASSET_QUOTES_ITEM_TIMEOUT = 4.2
_ASSET_QUOTES_CONCURRENCY = 16

@router.post("/api/asset-quotes")
async def asset_quotes_batch(payload: dict = Body(...)):
    """Fetch quotes for multiple codes in one request."""
    codes = payload.get("codes", [])
    if not isinstance(codes, list) or len(codes) > 100:
        raise HTTPException(status_code=400, detail="최대 100개까지 조회 가능합니다.")
    codes = list({str(c).strip() for c in codes if str(c).strip()})
    fresh = bool(payload.get("fresh", True))
    if not fresh:
        return {code: _cached_quote_for_code(code) for code in codes}

    # 순차 호출 — 화면은 이미 localStorage snapshot + 서버 cache 로
    # 즉시 떠 있으므로 (loadPortfolio 의 _pfRestoreSnapshot + GET /api/
    # portfolio 의 _enrich_with_cached_quotes), 여기서 하는 건 '뒤에서
    # 조용히 fresh 로 교체' 에 해당. upstream API 에 동시에 때리지 않고
    # 한 종목씩 순차로 처리해 rate limit·외부 서버 부하에 친화적.
    sem = asyncio.Semaphore(_ASSET_QUOTES_CONCURRENCY)

    async def _fetch_one(code):
        if code.startswith(_NON_QUOTABLE_PREFIXES):
            return code, {}
        try:
            async with sem:
                q = await asyncio.wait_for(_fetch_quote(code), timeout=_ASSET_QUOTES_ITEM_TIMEOUT)
            return code, q or {}
        except asyncio.CancelledError:
            return code, {}
        except (asyncio.TimeoutError, Exception):
            return code, {}

    tasks = [asyncio.create_task(_fetch_one(code)) for code in codes]
    done, pending = await asyncio.wait(tasks, timeout=_ASSET_QUOTES_BATCH_TIMEOUT)
    for task in pending:
        task.cancel()
    results = {code: {} for code in codes}
    for task in done:
        if task.cancelled():
            continue
        try:
            code, quote = task.result()
            results[code] = quote
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    return results


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


@router.get("/api/portfolio")
async def get_portfolio(request: Request):
    started = time.perf_counter()
    user = _require_user(await get_current_user(request))
    await cache.get_portfolio_groups(user["google_sub"])  # ensure default groups
    items = await cache.get_portfolio(user["google_sub"])
    needs_resolve = [it for it in items if not it.get("benchmark_code")]
    for item in needs_resolve:
        item["benchmark_code"] = _resolve_default_benchmark_fast(item["stock_code"])
    # Annotate with trailing dividend per share so the UI can show a
    # "배당액" column (= trailing_dps × quantity). Multiplying on the
    # client keeps the number fresh while the user edits quantity in
    # the inline edit row.
    codes = [it["stock_code"] for it in items]
    _schedule_portfolio_dividend_warmup(codes)
    metric_codes = list(dict.fromkeys(
        codes + [
            _common_stock_code(code)
            for code in codes
            if _is_korean_stock(code) and _is_preferred_stock(code)
        ]
    ))
    dps_map, target_metrics_map = await asyncio.gather(
        cache.get_trailing_dividends(codes),
        cache.get_portfolio_target_metrics(metric_codes),
    )
    await _supplement_target_metrics(items, target_metrics_map)
    for it in items:
        code = it["stock_code"]
        metrics = dict(target_metrics_map.get(code) or {})
        if _is_korean_stock(code) and _is_preferred_stock(code):
            common_metrics = target_metrics_map.get(_common_stock_code(code)) or {}
            for key in ("eps", "bps", "dps"):
                if metrics.get(key) is None and common_metrics.get(key) is not None:
                    metrics[key] = common_metrics[key]
        trailing_dps = dps_map.get(code)
        it["trailing_dps"] = trailing_dps
        if trailing_dps is not None:
            metrics["dps"] = trailing_dps
        it["target_metrics"] = metrics
    enriched = await _enrich_with_cached_quotes(items)
    await _fill_snapshot_quotes(user["google_sub"], enriched)
    _schedule_asset_insight_warmup(enriched)
    elapsed_ms = (time.perf_counter() - started) * 1000
    if elapsed_ms > 1000:
        logger.warning("portfolio list slow: %.0fms items=%d user=%s", elapsed_ms, len(enriched), user.get("email") or user.get("google_sub"))
    return enriched


def _target_number_or_none(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


async def _quote_price_for_target_formula(stock_code: str) -> float | None:
    try:
        quote = await asyncio.wait_for(_fetch_quote(stock_code), timeout=3.5)
    except Exception:
        quote = _cached_quote_for_code(stock_code)
    return _target_number_or_none((quote or {}).get("price"))


async def _holding_value_for_target_formula(stock_code: str) -> float | None:
    meta = (integrations.build_public_integrations().get("holdingValue") or {}).get("meta") or {}
    item = meta.get(stock_code)
    if not isinstance(item, dict):
        return None

    subsidiaries = item.get("subsidiaries") or []
    if not subsidiaries:
        return None

    async def _sub_value(sub: dict) -> float | None:
        code = str(sub.get("code") or "").strip()
        shares = _target_number_or_none(sub.get("sharesHeld"))
        if not code or shares is None:
            return None
        price = await _quote_price_for_target_formula(code)
        if price is None:
            return None
        return price * shares

    values = await asyncio.gather(*(_sub_value(sub) for sub in subsidiaries))
    if any(value is None for value in values):
        return None

    total_shares = _target_number_or_none(item.get("totalShares"))
    treasury_shares = _target_number_or_none(item.get("treasuryShares")) or 0
    free_shares = (total_shares or 0) - treasury_shares
    sub_total = sum(float(value) for value in values if value is not None)
    return sub_total / free_shares if free_shares > 0 and sub_total > 0 else None


async def _resolve_target_formula_price(stock_code: str, formula: str, avg_price: float) -> float | None:
    """Resolve a formula to a saved fallback price at edit/save time.

    BPS/EPS use the same valuation source as the investment insight modal.
    Dynamic variables such as 보유지분 and 본주가격 are still recomputed on the
    client when quotes are available, so they intentionally do not block save.
    """
    variables = _extract_target_variables(formula)
    if not variables:
        return None

    values: dict[str, float | None] = {}
    if "매입가" in variables:
        values["매입가"] = _target_number_or_none(avg_price)

    if "DPS" in variables:
        dps_map = await cache.get_trailing_dividends([stock_code])
        values["DPS"] = _target_number_or_none(dps_map.get(stock_code))

    if variables & {"BPS", "EPS"}:
        source_code = _common_stock_code(stock_code) if _is_korean_stock(stock_code) and _is_preferred_stock(stock_code) else stock_code
        basis = await _fetch_common_valuation_basis(source_code, as_of=_today_kst_date())
        if "BPS" in variables:
            values["BPS"] = _target_number_or_none(basis.get("bps"))
        if "EPS" in variables:
            values["EPS"] = _target_number_or_none(basis.get("eps"))

    if "보유지분" in variables:
        values["보유지분"] = await _holding_value_for_target_formula(stock_code)

    if "본주가격" in variables:
        common_code = _common_stock_code(stock_code) if _is_korean_stock(stock_code) and _is_preferred_stock(stock_code) else ""
        values["본주가격"] = await _quote_price_for_target_formula(common_code) if common_code and common_code != stock_code else None

    missing_financials = [name for name in ("BPS", "EPS") if name in variables and values.get(name) is None]
    if missing_financials:
        raise HTTPException(status_code=400, detail=f"{', '.join(missing_financials)} 값을 가져오지 못했습니다.")

    # 보유지분/본주가격은 저장 시점 fallback 을 채우되, quote 를 못 얻는
    # 경우에는 화면의 실시간 quote 도착 후 재평가에 맡긴다.
    if any(name not in values or values.get(name) is None for name in variables):
        return None

    try:
        return _evaluate_target_formula(formula, values)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/api/portfolio/{stock_code}")
async def save_portfolio_item(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    stock_code = _normalize_portfolio_code(stock_code)

    stock_name = str(payload.get("stock_name") or "").strip()
    domestic_alias = await _resolve_domestic_code_alias(stock_code)
    if domestic_alias:
        stock_code = domestic_alias["stock_code"]
        if not stock_name:
            stock_name = domestic_alias["corp_name"]

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
    # 등록일자 — accept "YYYY-MM-DD" from the UI edit form. Store as full
    # ISO so other columns (created_at DESC ordering) keep working, but
    # parse strictly so a malformed string can't overwrite the field
    # with garbage. None / empty string → leave existing value alone
    # (handled by cache.save_portfolio_item default semantics).
    created_at_raw = str(payload.get("created_at") or "").strip()
    created_at: str | None = None
    if created_at_raw:
        try:
            from datetime import date as _date
            parsed = _date.fromisoformat(created_at_raw[:10])
            # Reconstruct as ISO datetime at 00:00 so downstream code that
            # expects datetime.fromisoformat still works.
            created_at = parsed.isoformat() + "T00:00:00"
        except ValueError:
            raise HTTPException(status_code=400, detail="등록일자는 YYYY-MM-DD 형식이어야 합니다.")

    # 목표가 (수동 override). target_price_formula 은 숫자 문자열 또는
    # BPS/EPS/DPS/보유지분/본주가격/매입가 기반 수식을 받는다. 빈 값은
    # 자동 계산으로 복귀한다. 기존 target_price payload 도 호환 유지.
    target_price_kwarg: dict = {}
    if "target_price_formula" in payload:
        try:
            parsed_target = _parse_target_input(payload.get("target_price_formula"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        target_price_kwarg["target_price"] = parsed_target.price
        target_price_kwarg["target_price_formula"] = parsed_target.formula
        target_price_kwarg["target_price_disabled"] = False
        if parsed_target.formula:
            target_price_kwarg["target_price"] = await _resolve_target_formula_price(
                stock_code,
                parsed_target.formula,
                avg_price,
            )
    elif "target_price" in payload:
        raw_tp = payload.get("target_price")
        if raw_tp is None or (isinstance(raw_tp, str) and raw_tp.strip() == ""):
            target_price_kwarg["target_price"] = None
            target_price_kwarg["target_price_formula"] = None
        else:
            try:
                tp_val = float(raw_tp)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="목표가는 숫자여야 합니다.")
            if tp_val < 0:
                raise HTTPException(status_code=400, detail="목표가는 0 이상이어야 합니다.")
            target_price_kwarg["target_price"] = tp_val
            target_price_kwarg["target_price_formula"] = None
    # target_price_disabled — True 면 자동 계산도 bypass, UI 는 '-'.
    if "target_price_disabled" in payload:
        raw_d = payload.get("target_price_disabled")
        target_price_kwarg["target_price_disabled"] = bool(raw_d)
        if bool(raw_d):
            target_price_kwarg["target_price_formula"] = None

    result = await cache.save_portfolio_item(
        user["google_sub"], stock_code, stock_name, quantity, avg_price,
        currency, group_name, benchmark_code, created_at,
        **target_price_kwarg,
    )

    # 신규 해외 종목이면 yfinance 배당을 백그라운드로 fetch. 기존 동일
    # 코드에 대해 이미 foreign_dividends row 가 있으면 (auto/manual 무관)
    # 재 fetch 안 함 — 관리자 수동 override 보호 + 불필요한 yfinance 호출
    # 방지. fire-and-forget 이므로 PUT 응답 지연에 영향 없음. 실패 시
    # 로그만 남기고 포트폴리오 저장 자체는 성공 유지.
    try:
        if (not _is_korean_stock(stock_code)
                and not _is_cash_asset(stock_code)
                and stock_code not in _SPECIAL_ASSETS):
            existing_div = await cache.get_foreign_dividend(stock_code)
            if existing_div is None:
                async def _bg_fetch_dividend(code: str):
                    try:
                        import foreign_dividends
                        await foreign_dividends.refresh_foreign_dividends([code])
                    except Exception as exc:
                        logger.warning("auto foreign dividend fetch failed (%s): %s", code, exc)
                task = asyncio.create_task(_bg_fetch_dividend(stock_code))
                # Attach done callback so "Task exception was never retrieved"
                # doesn't pollute logs — the inner try/except already swallows.
                task.add_done_callback(lambda t: t.exception())
    except Exception as exc:
        logger.warning("foreign dividend dispatch guard failed (%s): %s", stock_code, exc)

    _schedule_portfolio_dividend_warmup([stock_code])
    return {"ok": True, **result}


@router.put("/api/portfolio/{stock_code}/benchmark")
async def update_benchmark(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    stock_code = _normalize_portfolio_code(stock_code)
    benchmark_code = _normalize_portfolio_code(str(payload.get("benchmark_code") or "")) or None
    updated = await cache.update_portfolio_benchmark(user["google_sub"], stock_code, benchmark_code)
    if not updated:
        raise HTTPException(status_code=404, detail="포트폴리오 종목을 찾을 수 없습니다.")
    # Return the effective benchmark and its quote without blocking the edit UI
    # on slow upstream index/FX sources.
    effective = benchmark_code or _resolve_default_benchmark_fast(stock_code)
    try:
        bq = await asyncio.wait_for(_fetch_benchmark_quote(effective), timeout=_BENCHMARK_ENDPOINT_ITEM_TIMEOUT)
    except Exception:
        bq = _cached_benchmark_quote(effective, allow_stale=True) or {}
    name = _resolve_benchmark_name_fast(effective)
    return {"ok": True, "benchmark_code": benchmark_code, "effective_benchmark": effective, "benchmark_name": name, "benchmark_quote": bq}


@router.get("/api/portfolio/benchmark-quotes")
async def get_benchmark_quotes(request: Request):
    """Fetch all unique benchmark quotes for the user's portfolio."""
    user = _require_user(await get_current_user(request))
    items = await cache.get_portfolio(user["google_sub"])
    benchmark_codes = set()
    for item in items:
        bc = item.get("benchmark_code") or _resolve_default_benchmark_fast(item["stock_code"])
        if bc:
            benchmark_codes.add(bc)
    async def _fetch_one(bc):
        name = _resolve_benchmark_name_fast(bc, items)
        try:
            bq = await asyncio.wait_for(
                _fetch_benchmark_quote(bc),
                timeout=_BENCHMARK_ENDPOINT_ITEM_TIMEOUT,
            )
            return bc, {**bq, "name": name}
        except Exception:
            bq = _cached_benchmark_quote(bc, allow_stale=True) or {}
            return bc, {**bq, "name": name}

    codes = list(benchmark_codes)
    pairs = await asyncio.gather(*[_fetch_one(bc) for bc in codes], return_exceptions=True)
    response = {}
    for bc, pair in zip(codes, pairs):
        if isinstance(pair, BaseException):
            name = _resolve_benchmark_name_fast(bc, items)
            bq = _cached_benchmark_quote(bc, allow_stale=True) or {}
            response[bc] = {**bq, "name": name}
            continue
        code, data = pair
        response[code] = data
    return response


@router.delete("/api/portfolio/{stock_code}")
async def delete_portfolio_item(stock_code: str, request: Request):
    user = _require_user(await get_current_user(request))
    stock_code = _normalize_portfolio_code(stock_code)
    deleted = await cache.delete_portfolio_item(user["google_sub"], stock_code)
    if not deleted:
        raise HTTPException(status_code=404, detail="포트폴리오에 없는 종목입니다.")
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
        code = _normalize_portfolio_code(str(row.get("stock_code") or ""))
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
    _schedule_portfolio_dividend_warmup([item["stock_code"] for item in resolved])

    return {"ok": True, "imported": len(resolved), "mode": mode}


@router.get("/api/portfolio/resolve-name")
async def resolve_name(code: str = Query(..., min_length=1)):
    code = _normalize_portfolio_code(code)
    if _is_cash_asset(code):
        return {"stock_code": code, "stock_name": _CASH_NAMES.get(code, code)}
    if code in _SPECIAL_ASSETS:
        return {"stock_code": code, "stock_name": _SPECIAL_ASSET_NAMES.get(code, code)}
    static = _static_foreign_ticker(code)
    if static:
        return {
            "stock_code": static["ticker"],
            "stock_name": static["name"],
            "reuters_code": static["ticker"],
        }
    if _is_korean_stock(code):
        name = await _resolve_name(code)
        return {"stock_code": code, "stock_name": name}
    domestic_match = await _resolve_domestic_code_alias(code)
    if domestic_match:
        return {
            "stock_code": domestic_match["stock_code"],
            "stock_name": domestic_match["corp_name"],
        }
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
    baseline_date = _portfolio_today_baseline_date()
    db = await cache.get_db()
    # Latest 22:00 settlement snapshot for the active Today window.
    cursor = await db.execute(
        "SELECT date, total_value, fx_usdkrw, nav FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (user["google_sub"], baseline_date),
    )
    snap_row = await cursor.fetchone()
    total_value = snap_row["total_value"] if snap_row else None
    fx_usdkrw = snap_row["fx_usdkrw"] if snap_row else None
    prev_nav = snap_row["nav"] if snap_row else None
    # Per-stock snapshots
    stock_snapshots = await cache.get_stock_snapshots_by_date(user["google_sub"], baseline_date)
    stock_values = {s["stock_code"]: s["market_value"] for s in stock_snapshots}
    # Net cashflow not yet reflected in snapshot. Use created_at > snapshot
    # date 22:00 (snapshot runs at 22:00) to catch cashflows entered after
    # the snapshot was taken, regardless of their nominal date.
    snap_date = snap_row["date"] if snap_row else None
    if snap_date:
        created_after = f"{snap_date}T22:00:00"
    else:
        created_after = baseline_date
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
        # date is the baseline the UI's Today card compares against. Was
        # missing from the response, which made the frontend's baseline
        # label silently fall back to "기준 없음" while the numerical
        # value was being computed against nav/total_value anyway —
        # label and value disagreed.
        "date": snap_date,
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


@router.get("/api/portfolio/group-weight-history")
async def get_group_weight_history(request: Request):
    user = _require_user(await get_current_user(request))
    return await cache.get_group_weight_history(user["google_sub"])


@router.get("/api/portfolio/group-constituent-history")
async def get_group_constituent_history(request: Request, group: str = Query(..., min_length=1)):
    user = _require_user(await get_current_user(request))
    return await cache.get_group_constituent_history(user["google_sub"], group.strip())


@router.get("/api/portfolio/benchmark-history")
async def get_benchmark_history(code: str = Query(...), start: str = Query(...)):
    """Return daily close prices for a benchmark index, served from the
    local `benchmark_daily` table. Performs a one-shot lazy backfill
    against yfinance the first time a code is requested, or when the
    requested `start` predates what we currently have.

    After that, the nightly `snapshot_nav.update_benchmark_today()` hook
    keeps the table fresh so normal requests hit SQLite only (~ms) and
    are immune to yfinance outages.
    """
    import asyncio
    import logging

    import benchmark_history

    code_up = code.upper()
    if code_up not in benchmark_history.YF_TICKER:
        raise HTTPException(status_code=400, detail=f"Unknown benchmark: {code}")

    logger = logging.getLogger(__name__)

    # Lazy backfill — no-op if DB already covers `start` or further back.
    # Failures here are swallowed (logged) inside backfill_benchmark; we
    # still try to serve whatever rows we have rather than 502-ing.
    try:
        await asyncio.wait_for(benchmark_history.backfill_benchmark(code_up, start), timeout=10)
    except asyncio.TimeoutError:
        logger.warning("Benchmark backfill timed out (%s start=%s); serving cached rows only", code_up, start)

    rows = await cache.get_benchmark_rows(code_up, start=start)
    return rows


@router.get("/api/portfolio/intraday")
async def get_intraday(request: Request):
    user = _require_user(await get_current_user(request))
    today = _today_kst_date()
    baseline_date = _portfolio_today_baseline_date()
    points = await cache.get_intraday_snapshots(user["google_sub"], today.isoformat())
    if baseline_date == today.isoformat():
        # Once the 22:00 settlement exists, the new Today window starts at
        # that snapshot. Same-day intraday points before 22:00 belong to the
        # completed window and must not leak into the reset sparkline.
        points = [p for p in points if _is_after_settlement_marker(p.get("ts"), baseline_date)]
    # Prepend active 22:00 settlement snapshot as the zero baseline. The
    # frontend treats T00:00 as x=0 on the 22→22 sparkline axis.
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT total_value FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (user["google_sub"], baseline_date),
    )
    row = await cursor.fetchone()
    if row and row["total_value"]:
        points = [{"ts": _intraday_axis_baseline_ts(today), "total_value": row["total_value"]}] + points
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
    try:
        amount = float(payload.get("amount"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="금액은 숫자여야 합니다.")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="금액은 0보다 커야 합니다.")
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

_AI_DEFAULT_MODEL = os.getenv("AI_DEFAULT_MODEL", "qwen/qwen3.6-plus")
_AI_FAST_MODEL = os.getenv("AI_FAST_MODEL", os.getenv("WIKI_QA_MODEL", "google/gemma-4-31b-it"))
_AI_PREMIUM_MODEL = os.getenv("AI_PREMIUM_MODEL", _AI_DEFAULT_MODEL)

_AI_SYSTEM_PROMPT = """당신은 한국/해외 자산을 함께 보는 투자 리서치 어시스턴트입니다.
규칙:
- 제공된 포트폴리오, 시장지표, 리서치 요약에 근거해 답하세요.
- 알 수 없는 사실은 추정이라고 분명히 말하고, 없는 데이터를 꾸며내지 마세요.
- 투자 조언은 단정 대신 조건부 시나리오와 리스크로 표현하세요.
- 결론에는 실행 우선순위와 확인해야 할 데이터 공백을 포함하세요."""


def _ai_model_profiles() -> dict[str, str]:
    return {
        "fast": _AI_FAST_MODEL,
        "balanced": _AI_DEFAULT_MODEL,
        "premium": _AI_PREMIUM_MODEL,
    }


async def _ai_model_profiles_async() -> dict[str, str]:
    return await ai_config.model_profiles()


async def _resolve_ai_model(payload: dict, user: dict) -> tuple[str, str]:
    profile = str(payload.get("profile") or payload.get("mode") or "balanced").strip().lower()
    profiles = await _ai_model_profiles_async()
    if profile not in profiles:
        profile = "balanced"
    model = profiles[profile]
    req_model = str(payload.get("model") or "").strip()
    if req_model and user.get("is_admin"):
        model = req_model
        profile = "custom"
    return model, profile


def _fmt_krw_ai(v: float) -> str:
    """Format KRW for AI prompt: 조/억 units with 4 significant digits."""
    av = abs(v)
    if av >= 1e12:  # 조
        jo = v / 1e12
        # 4 sig figs: e.g. 93.56조, 1.234조, 123.4조
        if av >= 1e15:
            return f"{jo:,.0f}조"
        elif av >= 1e14:
            return f"{jo:,.1f}조"
        elif av >= 1e13:
            return f"{jo:,.2f}조"
        else:
            return f"{jo:,.3f}조"
    elif av >= 1e8:  # 억
        eok = v / 1e8
        if av >= 1e11:
            return f"{eok:,.0f}억"
        elif av >= 1e10:
            return f"{eok:,.1f}억"
        elif av >= 1e9:
            return f"{eok:,.2f}억"
        else:
            return f"{eok:,.3f}억"
    else:
        return f"{v:,.0f}원"


@router.get("/api/portfolio/ai-models")
async def ai_model_list(request: Request):
    """Return available OpenRouter models (for admin model picker)."""
    user = _require_user(await get_current_user(request))
    profiles = await _ai_model_profiles_async()
    openrouter_key = await ai_config.get_openrouter_key()
    if not openrouter_key:
        return {"models": [], "default": profiles["balanced"], "profiles": profiles}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get("https://openrouter.ai/api/v1/models")
            data = resp.json().get("data", [])
            models = []
            for m in data:
                p = m.get("pricing", {})
                models.append({
                    "id": m["id"],
                    "name": m.get("name", m["id"]),
                    "prompt_price": float(p.get("prompt", 0)) * 1e6,
                    "completion_price": float(p.get("completion", 0)) * 1e6,
                    "context": m.get("context_length", 0),
                })
            models.sort(key=lambda x: x["id"])
            return {"models": models, "default": profiles["balanced"], "profiles": profiles}
    except Exception as exc:
        logger.warning("Failed to fetch OpenRouter models: %s", exc)
        return {"models": [], "default": profiles["balanced"], "profiles": profiles}


@router.post("/api/portfolio/ai-analysis")
async def ai_portfolio_analysis(request: Request, payload: dict = Body(default={})):
    user = _require_user(await get_current_user(request))
    openrouter_key = await ai_config.get_openrouter_key()
    if not openrouter_key:
        raise HTTPException(status_code=500, detail="AI API 키가 설정되지 않았습니다.")

    model, model_profile = await _resolve_ai_model(payload, user)
    started_at = time.perf_counter()

    # Optional user inquiry/question to include in the prompt
    user_query = (payload.get("query") or "").strip()
    # Hard cap to avoid runaway prompt growth; anything sensible fits easily.
    if len(user_query) > 2000:
        user_query = user_query[:2000]

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
        line = f"- {name} ({item['stock_code']}): 수량={qty}, 매입가={_fmt_krw_ai(avg)}"
        if price:
            line += f", 현재가={_fmt_krw_ai(price)}"
        if ret is not None:
            line += f", 수익률={ret:+.1f}%"
        if chg is not None:
            line += f", 일간={chg:+.2f}%"
        if mv:
            line += f", 평가={_fmt_krw_ai(mv)}"
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

    # Per-holding wiki snippets. Cap at the 10 largest positions so the
    # prompt stays bounded; each stock gets at most 2 recent wiki entries
    # with just the key_points (not the full summary).
    wiki_lines: list[str] = []
    wiki_used_count = 0
    try:
        ranked = sorted(
            (i for i in enriched if (i.get("quote", {}) or {}).get("price") and i.get("quantity")),
            key=lambda i: (i["quote"]["price"] or 0) * (i.get("quantity") or 0),
            reverse=True,
        )[:10]
        for item in ranked:
            code = item["stock_code"]
            name = item.get("stock_name") or code
            entries = await cache.get_wiki_entries(code, limit=2)
            if not entries:
                continue
            wiki_lines.append(f"### {name} ({code})")
            for e in entries:
                date_s = e.get("report_date") or (e.get("created_at") or "")[:10]
                firm = e.get("firm") or ""
                rec = (e.get("recommendation") or "").strip()
                tp = e.get("target_price")
                tp_s = f"TP={int(tp):,}" if tp else ""
                head = f"- [{firm}, {date_s}"
                if rec: head += f", {rec}"
                if tp_s: head += f", {tp_s}"
                head += "]"
                key = (e.get("key_points_md") or "").strip()
                # Fold bullets into one line (< 300 chars) so the full
                # prompt stays readable and compact.
                flat = " ".join(line.lstrip("- \t") for line in key.splitlines() if line.strip())
                if flat:
                    head += f" {flat[:300]}"
                wiki_lines.append(head)
                wiki_used_count += 1
    except Exception as _wiki_exc:
        logger.warning("portfolio AI wiki injection failed: %s", _wiki_exc)

    query_section = f"""

## 사용자 질문/요청
{user_query}

위 질문/요청을 우선적으로 고려하여 답변해 주세요.""" if user_query else ""

    wiki_section = (
        f"\n\n## 종목별 리서치 요약 (최근 증권사 리포트)\n{chr(10).join(wiki_lines)}"
        if wiki_lines else ""
    )

    prompt = f"""아래 포트폴리오를 분석해 주세요.

## 보유 종목 (총 평가: {_fmt_krw_ai(total_value)})
{chr(10).join(holdings_lines)}

## 성과
{chr(10).join(perf_lines) if perf_lines else "N/A"}

## 시장 현황
{chr(10).join(market_lines)}{wiki_section}{query_section}

분석 항목:
1. 포트폴리오 구성 평가 (분산도, 섹터 편중)
2. 주요 종목 밸류에이션과 리스크 — 증권사 의견을 근거로 인용 가능하면 인용
3. 시장 상황 고려 단기/중기 시나리오
4. 리밸런싱/비중 조절 제안과 우선순위
5. 추가로 확인해야 할 데이터 공백

한국어로 간결하게 마크다운 형식으로 답변해 주세요."""

    import json as _json

    async def _stream():
        # Use client.stream() (context manager) rather than client.post() —
        # the latter buffers the entire response body even with stream=True
        # in the JSON payload, defeating the purpose and inflating latency
        # until the model finishes. With stream() the first token reaches
        # the browser as soon as OpenRouter emits it.
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=None)) as client:
            async with client.stream(
                "POST",
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {openrouter_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": _AI_SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 2000,
                    "stream": True,
                },
            ) as resp:
                if resp.status_code != 200:
                    # Need to consume body before httpx exposes it.
                    body = await resp.aread()
                    try:
                        err = _json.loads(body)
                        msg = err.get("error", {}).get("message", f"HTTP {resp.status_code}")
                    except Exception:
                        msg = f"HTTP {resp.status_code}"
                    yield f"data: {_json.dumps({'content': f'API 오류: {msg}'})}\n\n"
                    await ai_config.record_usage(
                        google_sub=google_sub,
                        feature="portfolio_analysis",
                        model=model,
                        model_profile=model_profile,
                        ok=False,
                        error=msg,
                        latency_ms=int((time.perf_counter() - started_at) * 1000),
                    )
                    yield f"data: {_json.dumps({'done': True, 'input_tokens': 0, 'output_tokens': 0, 'model': model, 'model_profile': model_profile, 'cost': 0, 'wiki_used': wiki_used_count})}\n\n"
                    return

                input_tokens = 0
                output_tokens = 0
                cost = 0
                async for line in resp.aiter_lines():
                    # If the browser closed the tab, stop consuming upstream
                    # tokens — OpenRouter bills per-token and a forgotten
                    # request could run to the full max_tokens budget.
                    if await request.is_disconnected():
                        logger.info("AI analysis: client disconnected, aborting upstream")
                        return
                    if not line.startswith("data: "):
                        continue
                    payload = line[6:]
                    if payload.strip() == "[DONE]":
                        break
                    try:
                        chunk = _json.loads(payload)
                        if "error" in chunk:
                            yield f"data: {_json.dumps({'content': chunk['error'].get('message', 'Unknown error')})}\n\n"
                            break
                        delta = chunk.get("choices", [{}])[0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            output_tokens += 1
                            yield f"data: {_json.dumps({'content': content})}\n\n"
                        usage = chunk.get("usage")
                        if usage:
                            input_tokens = usage.get("prompt_tokens", input_tokens)
                            output_tokens = usage.get("completion_tokens", output_tokens)
                            cost = usage.get("cost", cost) or cost
                    except Exception:
                        continue
                await ai_config.record_usage(
                    google_sub=google_sub,
                    feature="portfolio_analysis",
                    model=model,
                    model_profile=model_profile,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cost_usd=float(cost or 0),
                    latency_ms=int((time.perf_counter() - started_at) * 1000),
                    ok=True,
                )
                yield f"data: {_json.dumps({'done': True, 'input_tokens': input_tokens, 'output_tokens': output_tokens, 'model': model, 'model_profile': model_profile, 'cost': cost, 'wiki_used': wiki_used_count})}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")
