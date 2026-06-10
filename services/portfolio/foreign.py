"""Foreign-stock quotes and name/ticker resolution.

Extracted verbatim from ``routes/portfolio.py`` so the foreign-quote +
name-resolution cluster lives in one cohesive module instead of being
interleaved with HTTP handlers in a ~2,700-line file. Behavior, caches and
concurrency bounds are preserved exactly from the original implementation;
only the function names were promoted from ``_private`` to module-public.

Dependency direction: this module imports ``cache``, ``cache_layer``,
``kis_proxy_client``, ``httpx``, ``yfinance`` (lazily) and the sibling
``services.portfolio`` modules (``fx``, ``currencies``, ``identifiers``).
It must never import ``routes`` — the router depends on this module, not the
other way around.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from functools import partial
from urllib.parse import quote

import httpx

import cache
from repositories import ticker_map as ticker_map_repo
import kis_proxy_client
from cache_layer import MemoryTTLCache
from services.portfolio import currencies
from services.portfolio import fx
from services.portfolio.identifiers import (
    CASH_NAMES as _CASH_NAMES,
    is_korean_stock as _is_korean_stock,
    is_special_asset as _is_special_asset,
    normalize_portfolio_code as _normalize_portfolio_code,
    static_foreign_ticker as _static_foreign_ticker,
)

logger = logging.getLogger(__name__)


_SPECIAL_ASSET_NAMES = {"KRX_GOLD": "KRX 금현물", "CRYPTO_BTC": "비트코인", "CRYPTO_ETH": "이더리움", "CRYPTO_USDT": "테더"}

_EXCHANGE_SUFFIXES = (
    "", ".O", ".K", ".N", ".HM", ".HK", ".T", ".SS", ".SZ", ".L", ".AX",
    ".DE", ".F", ".PA", ".AS", ".MI", ".MC", ".SW", ".ST", ".CO", ".HE",
)

_YFINANCE_SUFFIXES = (
    "", ".DE", ".F", ".PA", ".AS", ".MI", ".MC", ".L", ".AX", ".T",
    ".HK", ".SS", ".SZ", ".SW", ".ST", ".CO",
)

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

# Negative cache: tickers we just failed to fetch/resolve via yfinance —
# avoids re-running the 16-suffix probe loop on every quote refresh. TTL'd
# (not a permanent set) so a transient Yahoo error or rate-limit can no longer
# block a ticker until the next server restart; it self-heals after the TTL.
_FAILED_YF_TTL = 300
_failed_yf_cache = MemoryTTLCache("portfolio.failed_yf", _FAILED_YF_TTL)


_ticker_map: dict[str, str] = {}  # stock_code -> resolved ticker (e.g., A200 -> A200.AX)
_ticker_map_loaded = False


async def fetch_naver_stock_name(stock_code: str) -> str | None:
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


async def resolve_name(stock_code: str) -> str | None:
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
        return await fetch_naver_stock_name(stock_code)
    domestic_match = await resolve_domestic_code_alias(stock_code)
    if domestic_match:
        return domestic_match["corp_name"]
    return await resolve_foreign_name(stock_code)


async def resolve_domestic_code_alias(stock_code: str) -> dict | None:
    stock_code = _normalize_portfolio_code(stock_code)
    if (
        not stock_code
        or _is_special_asset(stock_code)
        or _is_korean_stock(stock_code)
        or _static_foreign_ticker(stock_code)
    ):
        return None
    return await cache.resolve_corp_search_query(stock_code)


async def fetch_naver_world_stock(reuters_code: str) -> dict | None:
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


def yf_marked_failed(ticker: str) -> bool:
    return bool(_failed_yf_cache.get(ticker))


def yf_mark_failed(ticker: str) -> None:
    _failed_yf_cache.set(ticker, True)


async def yf_run(fn):
    """Run a synchronous yfinance call in the executor, bounded by a
    semaphore and a hard wall-clock deadline. Raises on timeout."""
    loop = asyncio.get_event_loop()
    async with _YF_SEM:
        return await asyncio.wait_for(
            loop.run_in_executor(None, fn), timeout=_YF_CALL_TIMEOUT
        )


async def resolve_foreign_name(ticker: str) -> str | None:
    """Try yfinance first, then Naver as fallback."""
    static = _static_foreign_ticker(ticker)
    if static:
        return static["name"]
    name = await yfinance_resolve_name(ticker)
    if name:
        return name
    # Naver fallback
    upper = ticker.upper()
    if "." in upper:
        d = await fetch_naver_world_stock(upper)
        if d:
            return d.get("stockName") or d.get("stockNameEng")
    for suffix in _EXCHANGE_SUFFIXES:
        code = upper + suffix if suffix else upper
        d = await fetch_naver_world_stock(code)
        if d:
            return d.get("stockName") or d.get("stockNameEng")
    return None


async def yfinance_find_ticker(ticker: str) -> str | None:
    """Find a working yfinance ticker, trying various exchange suffixes.
    Bounded by _YF_SEM and a per-call timeout; results (positive and negative)
    are cached to avoid re-running the suffix loop on every quote refresh."""
    static = _static_foreign_ticker(ticker)
    if static:
        return static["ticker"]
    if ticker in _ticker_map:
        return _ticker_map[ticker]
    if yf_marked_failed(ticker):
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
                hit = await yf_run(partial(_probe, candidate))
                if hit:
                    await save_ticker(ticker, hit)
                    return hit
            except (asyncio.TimeoutError, Exception):
                continue
    except Exception:
        pass
    yf_mark_failed(ticker)
    return None


async def yfinance_resolve_name(ticker: str) -> str | None:
    static = _static_foreign_ticker(ticker)
    if static:
        return static["name"]
    try:
        import yfinance as yf
        found = await yfinance_find_ticker(ticker)
        if not found:
            return None

        def _name(c):
            info = yf.Ticker(c).info
            return info.get("shortName") or info.get("longName")

        return await yf_run(partial(_name, found))
    except (asyncio.TimeoutError, Exception):
        return None


async def resolve_foreign_reuters(ticker: str) -> str | None:
    """Find a working yfinance ticker, or fall back to Naver reuters code."""
    # yfinance first — more reliable for foreign stocks
    static = _static_foreign_ticker(ticker)
    if static:
        return static["ticker"]
    found = await yfinance_find_ticker(ticker)
    if found:
        return found
    # Naver fallback
    upper = ticker.upper()
    if "." in upper:
        d = await fetch_naver_world_stock(upper)
        if d:
            return d.get("reutersCode") or upper
    for suffix in _EXCHANGE_SUFFIXES:
        code = upper + suffix if suffix else upper
        d = await fetch_naver_world_stock(code)
        if d:
            return d.get("reutersCode") or code
    return ticker


def guess_kis_exchanges(ticker: str) -> list[str]:
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


async def kis_fetch_foreign_quote(ticker: str) -> dict:
    """Try fetching quote from KIS overseas API. Returns {} on failure."""
    exchanges = guess_kis_exchanges(ticker)
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
                price_krw = await fx.fx_to_krw(nation, price)
                change = s.get("change") or 0
                change_krw = await fx.fx_to_krw(nation, change)
                return {
                    "price": round(price_krw),
                    "change": round(change_krw),
                    "change_pct": s.get("change_pct"),
                }
        except Exception as exc:
            logger.debug("KIS overseas quote failed (%s/%s): %s", excd, symbol, exc)
    return {}


async def fetch_foreign_quote(reuters_code: str) -> dict:
    static = _static_foreign_ticker(reuters_code)
    if static:
        try:
            q = await asyncio.wait_for(
                yfinance_fetch_quote_fast(static["ticker"]),
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
    q = await kis_fetch_foreign_quote(reuters_code)
    if q and q.get("price") is not None:
        return q

    # 2. yfinance via the Yahoo chart API (httpx + browser UA) — far more
    #    reliable from a server IP than fast_info, which Yahoo rate-limits.
    #    This is the same path static ETFs use; without it a non-static
    #    foreign ticker (e.g. AAA.AX on the ASX, which KIS does not serve)
    #    had only fast_info and could fail repeatedly, leaving its value blank.
    q = await yfinance_fetch_quote_fast(reuters_code)
    if q and q.get("price") is not None:
        return q

    # 2b. fast_info fallback
    q = await yfinance_fetch_quote(reuters_code)
    if q and q.get("price") is not None:
        return q

    # 3. Naver fallback
    upper_code = reuters_code.upper()
    d = await fetch_naver_world_stock(upper_code)
    if d and d.get("closePrice"):
        try:
            price_str = str(d["closePrice"]).replace(",", "")
            price = float(price_str)
            change_str = str(d.get("compareToPreviousClosePrice", "0")).replace(",", "")
            change = float(change_str)
            change_pct = float(d.get("fluctuationsRatio", 0))
            nation = d.get("nationType", "")
            price_krw = await fx.fx_to_krw(nation, price)
            change_krw = await fx.fx_to_krw(nation, change)
            return {
                "price": round(price_krw),
                "change": round(change_krw),
                "change_pct": change_pct,
                "nation": d.get("nationName", ""),
            }
        except Exception as exc:
            logger.warning("해외주식 시세 파싱 실패(%s): %s", reuters_code, exc)

    return {}


async def yfinance_fetch_quote(ticker: str) -> dict:
    if yf_marked_failed(ticker):
        return {}
    try:
        import yfinance as yf

        def _snap(c):
            t = yf.Ticker(c)
            fi = t.fast_info
            return fi.last_price, fi.previous_close, (fi.currency or "USD").upper()

        try:
            price, prev, currency = await yf_run(partial(_snap, ticker))
        except asyncio.TimeoutError:
            logger.warning("yfinance 시세 타임아웃(%s)", ticker)
            return {}
        change = round(price - prev, 4) if price and prev else 0
        change_pct = round(change / prev * 100, 2) if prev else None
        nation = currencies.CURRENCY_TO_NATION.get(currency, "USA")
        price_krw = await fx.fx_to_krw(nation, price)
        change_krw = await fx.fx_to_krw(nation, change)
        return {
            "price": round(price_krw),
            "change": round(change_krw),
            "change_pct": change_pct,
        }
    except Exception as exc:
        logger.warning("yfinance 시세 조회 실패(%s): %s", ticker, exc)
        yf_mark_failed(ticker)
        return {}


def infer_yf_currency(ticker: str) -> str:
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


def yfinance_direct_ticker(code: str) -> str:
    """Normalize a portfolio code into a direct yfinance ticker.

    Yahoo uses dash class shares (BRK-B / BF-B) but keeps exchange suffixes such
    as ``7203.T`` unchanged. Slashes become dashes.
    """
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


async def fetch_yahoo_chart(ticker: str, *, range_: str = "1y", interval: str = "1d") -> dict:
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
            "currency": (meta.get("currency") or infer_yf_currency(ticker)).upper(),
            "meta": meta,
        }
    except Exception as exc:
        logger.warning("Yahoo chart fetch failed (%s): %s", ticker, exc)
        return {"rows": [], "currency": None, "meta": {}}


async def yfinance_fetch_quote_fast(ticker: str) -> dict:
    # No negative-cache gate here: the chart API is a single cheap, reliable
    # call, so it must not be skipped just because the unreliable fast_info
    # path marked this ticker as failed (that is exactly when we want it).
    try:
        payload = await asyncio.wait_for(fetch_yahoo_chart(ticker, range_="5d"), timeout=7.0)
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
        currency = (payload.get("currency") or infer_yf_currency(ticker)).upper()
        fx_rate = await fx.fx_rate_for_currency(currency)
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


async def ensure_ticker_map():
    """Load ticker_map from DB on first access."""
    global _ticker_map_loaded
    if _ticker_map_loaded:
        return
    try:
        saved = await ticker_map_repo.load_ticker_map()
        _ticker_map.update(saved)
        logger.info("Ticker map loaded: %d entries from DB", len(saved))
    except Exception as exc:
        logger.warning("Ticker map load failed: %s", exc)
    _ticker_map_loaded = True


async def save_ticker(stock_code: str, resolved: str):
    """Save a resolved ticker to both memory and DB."""
    _ticker_map[stock_code] = resolved
    try:
        await ticker_map_repo.save_ticker(stock_code, resolved)
    except Exception as exc:
        logger.warning("Ticker map save failed (%s -> %s): %s", stock_code, resolved, exc)


async def detect_currency(stock_code: str) -> str:
    static = _static_foreign_ticker(stock_code)
    if static:
        return static["currency"]
    # yfinance first
    try:
        import yfinance as yf
        found = await yfinance_find_ticker(stock_code)
        if found:
            def _curr(c):
                return (yf.Ticker(c).fast_info.currency or "USD").upper()
            return await yf_run(partial(_curr, found))
    except (asyncio.TimeoutError, Exception):
        pass
    # Naver fallback
    d = await fetch_naver_world_stock(stock_code.upper())
    if d:
        nation = d.get("nationType", "")
        return currencies.NATION_TO_CURRENCY.get(nation, "USD")
    return "USD"
