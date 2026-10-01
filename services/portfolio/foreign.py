"""Foreign-stock quotes and name/ticker resolution.

Extracted verbatim from ``routes/portfolio.py`` so the foreign-quote +
name-resolution cluster lives in one cohesive module instead of being
interleaved with HTTP handlers in a ~2,700-line file. Behavior, caches and
concurrency bounds are preserved exactly from the original implementation;
only the function names were promoted from ``_private`` to module-public.

Dependency direction: this module imports ``cache``, ``cache_layer``,
``services.market.sources.kis_proxy``, ``httpx``, ``yfinance`` (lazily) and the sibling
``services.portfolio`` modules (``fx``, ``currencies``, ``identifiers``).
It must never import ``routes`` — the router depends on this module, not the
other way around.
"""

from __future__ import annotations

import asyncio
import logging
import re
from functools import partial

import httpx

from cache_layer import MemoryTTLCache
from core.http import get_http_client
from domain.portfolio_codes import is_hong_kong_rmb_counter
from repositories import corp_codes
from repositories import ticker_map as ticker_map_repo
from services.market.sources import kis_proxy as kis_proxy_client
from services.market.sources import yahoo, yfinance_runner
from services.portfolio import currencies, fx
from services.portfolio.identifiers import (
    CASH_NAMES as _CASH_NAMES,
)
from services.portfolio.identifiers import (
    is_korean_stock as _is_korean_stock,
)
from services.portfolio.identifiers import (
    is_special_asset as _is_special_asset,
)
from services.portfolio.identifiers import (
    normalize_portfolio_code as _normalize_portfolio_code,
)
from services.portfolio.identifiers import (
    static_foreign_ticker as _static_foreign_ticker,
)
from services.portfolio.identifiers import (
    yahoo_symbol as _yahoo_symbol,
)

logger = logging.getLogger(__name__)

# Backward-compatible module export; the rule itself lives with the other
# currency mappings so history and quote paths cannot drift apart.
infer_yf_currency = currencies.infer_yf_currency


_SPECIAL_ASSET_NAMES = {"KRX_GOLD": "KRX 금현물", "CMA_RP_KRW": "CMA 원화RP", "CRYPTO_BTC": "비트코인", "CRYPTO_ETH": "이더리움", "CRYPTO_USDT": "테더"}

_EXCHANGE_SUFFIXES = (
    "", ".O", ".K", ".N", ".HM", ".HN", ".HK", ".T", ".SS", ".SZ", ".L", ".AX",
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
_YF_CALL_TIMEOUT = 8.0
_NAVER_HTTP_TIMEOUT = httpx.Timeout(5.0, connect=3.0)
_YAHOO_SEARCH_TIMEOUT = httpx.Timeout(4.0, connect=2.0)
# Yahoo chart·search 는 services.market.sources.yahoo 의 호스트 단위 한도를 공유한다.
_INSIGHT_QUOTE_TIMEOUT = 8.5
_STATIC_FOREIGN_QUOTE_TIMEOUT = 3.0
_KIS_FOREIGN_QUOTE_TIMEOUT = 4.0
_FOREIGN_SEARCH_QUOTE_TYPES = {"EQUITY", "ETF", "MUTUALFUND"}

# Negative cache: tickers we just failed to fetch/resolve via yfinance —
# avoids re-running the 16-suffix probe loop on every quote refresh. TTL'd
# (not a permanent set) so a transient Yahoo error or rate-limit can no longer
# block a ticker until the next server restart; it self-heals after the TTL.
_FAILED_YF_TTL = 300
_failed_yf_cache = MemoryTTLCache("portfolio.failed_yf", _FAILED_YF_TTL, evict_expired_after=0)


_ticker_map: dict[str, str] = {}  # stock_code -> resolved ticker (e.g., A200 -> A200.AX)
_ticker_map_loaded = False


def _is_pseudo_code(code: str | None) -> bool:
    """외부 종목 조회 대상이 아닌 허브 가상 코드(현금·금·RP·코인·선물 평가)인가.

    이런 코드는 어느 거래소에도 없으므로 해외 종목 조회 진입점마다 이걸로 즉시
    끊는다 — 예전에는 CASH_CNY 하나가 접미사 탐색으로 네이버 22회·yfinance 16회·
    Yahoo chart 조회를 알림 패스마다 반복했다."""
    return _is_special_asset(code)


async def fetch_naver_stock_name(stock_code: str) -> str | None:
    try:
        async with _NAVER_SEM:
            client = await get_http_client("naver")
            resp = await client.get(
                f"https://finance.naver.com/item/main.naver?code={stock_code}",
                timeout=_NAVER_HTTP_TIMEOUT,
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
    if _is_pseudo_code(stock_code):
        return None
    static = _static_foreign_ticker(stock_code)
    if static:
        return static["name"]
    if _is_korean_stock(stock_code):
        name = await corp_codes.resolve_stock_name(stock_code)
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
    return await corp_codes.resolve_corp_search_query(stock_code)


async def fetch_naver_world_stock(reuters_code: str) -> dict | None:
    """Fetch foreign stock info from Naver world stock API."""
    if _is_pseudo_code(reuters_code):
        return None
    try:
        async with _NAVER_SEM:
            client = await get_http_client("naver")
            resp = await client.get(
                f"https://api.stock.naver.com/stock/{reuters_code}/basic",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=_NAVER_HTTP_TIMEOUT,
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
    """Run a synchronous yfinance call on the shared yfinance runner (dedicated
    pool + concurrency limit) with a hard wall-clock deadline. Raises on timeout."""
    return await yfinance_runner.run(fn, timeout=_YF_CALL_TIMEOUT)


async def resolve_foreign_name(ticker: str) -> str | None:
    """Try yfinance first, then Naver as fallback."""
    if _is_pseudo_code(ticker):
        return None
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
    Bounded by the yfinance runner and a per-call timeout; results (positive and negative)
    are cached to avoid re-running the suffix loop on every quote refresh."""
    if _is_pseudo_code(ticker):
        return None
    static = _static_foreign_ticker(ticker)
    if static:
        return static["ticker"]
    if ticker in _ticker_map:
        return _ticker_map[ticker]
    if yf_marked_failed(ticker):
        return None
    try:
        import yfinance as yf
        candidates = _yfinance_candidates(ticker)

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


def _yfinance_candidates(ticker: str) -> list[str]:
    ticker = (ticker or "").strip()
    if not ticker:
        return []
    if "." not in ticker:
        return [ticker + suffix for suffix in _YFINANCE_SUFFIXES]
    # Yahoo 표기(GOOGL.O → GOOGL, BRK.B → BRK-B)를 먼저, 원래 표기는 그다음에.
    return list(dict.fromkeys([_yahoo_symbol(ticker), ticker]))


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
    if _is_pseudo_code(ticker):
        return ticker
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
    # 베트남(.HM/.HN 네이버, .VN Yahoo)도 여기다 — 미국 거래소로 흘리면 같은 심볼의
    # 미국 종목(VNM.VN → NYSE Arca VNM ETF)을 시세로 잡는다.
    if upper.endswith((".AX", ".DE", ".F", ".PA", ".AS", ".MI", ".MC", ".SW", ".ST", ".CO", ".HE", ".L",
                       ".HM", ".HN", ".VN")):
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
            data = await asyncio.wait_for(
                kis_proxy_client.get_overseas_quote(symbol, excd),
                timeout=_KIS_FOREIGN_QUOTE_TIMEOUT,
            )
            s = data.get("summary", {})
            price = s.get("price")
            if price is not None:
                nation = {"NAS": "USA", "NYS": "USA", "AMS": "USA", "HKS": "HKG", "TSE": "JPN", "SHS": "CHN", "SZS": "CHN"}.get(excd, "USA")
                if is_hong_kong_rmb_counter(ticker):
                    nation = "CHN"
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
    if _is_pseudo_code(reuters_code):
        return {}
    # 베트남 거래소 식별자는 Naver 형식이다. Yahoo의 미국 종목으로 재해석하지 않는다.
    if reuters_code.upper().endswith((".HM", ".HN")):
        return await fetch_naver_foreign_quote(reuters_code)
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
    return await fetch_naver_foreign_quote(reuters_code)


async def fetch_naver_foreign_quote(reuters_code: str) -> dict:
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
            if is_hong_kong_rmb_counter(reuters_code):
                nation = "CHN"
            price_krw = await fx.fx_to_krw(nation, price)
            change_krw = await fx.fx_to_krw(nation, change)
            return {
                "price": round(price_krw),
                "change": round(change_krw),
                "change_pct": change_pct,
                "nation": d.get("nationName", ""),
            }
        except (TypeError, ValueError, OverflowError, fx.FXUnavailableError) as exc:
            logger.warning("해외주식 시세 파싱 실패(%s): %s", reuters_code, exc)

    return {}


async def yfinance_fetch_quote(ticker: str) -> dict:
    if _is_pseudo_code(ticker) or yf_marked_failed(ticker):
        return {}
    try:
        import yfinance as yf

        def _snap(c):
            t = yf.Ticker(c)
            fi = t.fast_info
            return fi.last_price, fi.previous_close, (fi.currency or "USD").upper()

        try:
            price, prev, currency = await yf_run(partial(_snap, _yahoo_symbol(ticker)))
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


def yfinance_direct_ticker(code: str) -> str:
    """Normalize a portfolio code into a direct yfinance ticker.

    ``domain.portfolio_codes.yahoo_symbol`` 위임 — Reuters 미국 거래소 접미사
    제거(GOOGL.O → GOOGL), .HM → .VN, 클래스 주식 대시(BRK.B → BRK-B), Yahoo
    거래소 접미사(7203.T, BP.L)는 유지.
    """
    return _yahoo_symbol(code)


def _looks_like_direct_foreign_ticker(query: str) -> bool:
    raw = (query or "").strip()
    if not raw or len(raw) > 24 or any(ch.isspace() for ch in raw):
        return False
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9./-]*", raw):
        return False
    return raw == raw.upper() or any(ch in raw for ch in ".-/")


def _foreign_search_fallback(query: str) -> dict | None:
    if not _looks_like_direct_foreign_ticker(query):
        return None
    ticker = yfinance_direct_ticker(_normalize_portfolio_code(query))
    if not ticker:
        return None
    return {
        "stock_code": ticker,
        "ticker": ticker,
        "stock_name": ticker,
        "exchange": "",
        "quote_type": "TICKER",
        "currency": infer_yf_currency(ticker),
        "source": "direct",
    }


def _normalize_yahoo_search_quote(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None
    symbol = _normalize_portfolio_code(raw.get("symbol"))
    if not symbol or len(symbol) > 30:
        return None
    if symbol.startswith("^") or symbol.endswith("=X"):
        return None
    quote_type = _normalize_portfolio_code(raw.get("quoteType") or raw.get("typeDisp"))
    if quote_type and quote_type not in _FOREIGN_SEARCH_QUOTE_TYPES:
        return None
    name = (
        str(raw.get("shortname") or "").strip()
        or str(raw.get("longname") or "").strip()
        or str(raw.get("name") or "").strip()
        or symbol
    )
    exchange = (
        str(raw.get("exchDisp") or "").strip()
        or str(raw.get("fullExchangeName") or "").strip()
        or str(raw.get("exchange") or "").strip()
    )
    currency = _normalize_portfolio_code(raw.get("currency")) or infer_yf_currency(symbol)
    return {
        "stock_code": symbol,
        "ticker": symbol,
        "stock_name": name,
        "exchange": exchange,
        "quote_type": quote_type or "EQUITY",
        "currency": currency,
        "source": "yahoo",
    }


async def search_foreign_tickers(query: str, *, limit: int = 8) -> list[dict]:
    """Fast Yahoo-style ticker suggestions for the portfolio add box.

    The UI should not ask users for Reuters suffixes such as ``.O``. Yahoo
    symbols (AAPL, BRK-B, 7203.T) are what our chart path already understands,
    so suggestions return those directly and fall back to a direct ticker only
    when the user clearly typed one.
    """
    raw = str(query or "").strip()
    if not raw:
        return []

    results: list[dict] = []
    static = _static_foreign_ticker(raw)
    if static:
        results.append({
            "stock_code": static["ticker"],
            "ticker": static["ticker"],
            "stock_name": static["name"],
            "exchange": "",
            "quote_type": "EQUITY",
            "currency": static["currency"],
            "source": "static",
        })

    try:
        async with yahoo.host_limit():
            client = await get_http_client("yahoo")
            resp = await client.get(
                "https://query1.finance.yahoo.com/v1/finance/search",
                params={
                    "q": raw,
                    "quotesCount": max(12, limit * 3),
                    "newsCount": 0,
                    "enableFuzzyQuery": "true",
                },
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=_YAHOO_SEARCH_TIMEOUT,
            )
            resp.raise_for_status()
        for quote_row in (resp.json() or {}).get("quotes") or []:
            item = _normalize_yahoo_search_quote(quote_row)
            if item:
                results.append(item)
    except Exception as exc:
        logger.debug("Yahoo ticker search failed (%s): %s", raw, exc)

    fallback = _foreign_search_fallback(raw)
    if fallback:
        results.append(fallback)

    deduped: list[dict] = []
    seen: set[str] = set()
    for item in results:
        code = item.get("stock_code")
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


async def fetch_yahoo_chart(ticker: str, *, range_: str = "1y", interval: str = "1d") -> dict:
    """``{rows: [{date, session_date, close}], currency, meta}`` — 공용 Yahoo
    chart provider 위임. 테스트가 이 모듈 이름을 patch 하므로 래퍼로 남긴다.
    허브 코드(GOOGL.O, FUEVFVND.HM)는 Yahoo 심볼로 바꿔 요청한다."""
    return await yahoo.fetch_close_series(
        _yahoo_symbol(ticker), range_=range_, interval=interval, currency_fallback=infer_yf_currency,
    )


async def yfinance_fetch_quote_fast(ticker: str) -> dict:
    # No negative-cache gate here: the chart API is a single cheap, reliable
    # call, so it must not be skipped just because the unreliable fast_info
    # path marked this ticker as failed (that is exactly when we want it).
    if _is_pseudo_code(ticker):
        return {}
    try:
        payload = await asyncio.wait_for(fetch_yahoo_chart(ticker, range_="5d"), timeout=7.0)
        values = [row["close"] for row in payload.get("rows") or [] if row.get("close") is not None]
        if not values:
            return {}
        meta = payload.get("meta") or {}
        price = float(meta.get("regularMarketPrice") or values[-1])
        # For multi-day chart ranges Yahoo's chartPreviousClose is the close
        # before the requested range, not the previous trading day's close.
        prev = float(values[-2] if len(values) >= 2 else meta.get("chartPreviousClose") or values[-1])
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
    if _is_korean_stock(stock_code):
        return "KRW"
    if _is_pseudo_code(stock_code):
        code = _normalize_portfolio_code(stock_code)
        return code.removeprefix("CASH_") if code.startswith("CASH_") else "KRW"
    if is_hong_kong_rmb_counter(stock_code):
        return "CNY"
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
