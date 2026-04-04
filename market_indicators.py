"""
Market indicators module — catalog of 20 indicators with fetch functions.

Public API:
    CATALOG  — dict of all indicators {code: {label, category}}
    fetch_indicators(codes) — fetch multiple indicators in parallel
"""

import asyncio
import json as _json
import re

import httpx

# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

CATALOG: dict[str, dict] = {
    # 국내 지수
    "KOSPI":    {"label": "KOSPI",    "category": "국내 지수"},
    "KOSDAQ":   {"label": "KOSDAQ",   "category": "국내 지수"},
    "KOSPI200": {"label": "KOSPI200", "category": "국내 지수"},
    # 해외 지수
    "SPX":  {"label": "S&P 500",  "category": "해외 지수"},
    "IXIC": {"label": "NASDAQ",   "category": "해외 지수"},
    "DJI":  {"label": "다우존스", "category": "해외 지수"},
    "NI225": {"label": "닛케이225", "category": "해외 지수"},
    "HSI":  {"label": "항셍",     "category": "해외 지수"},
    "SHC":  {"label": "상해종합", "category": "해외 지수"},
    # 원자재
    "CMDT_GC": {"label": "금",       "category": "원자재"},
    "CMDT_SI": {"label": "은",       "category": "원자재"},
    "OIL_CL":  {"label": "WTI유",    "category": "원자재"},
    "OIL_BRT": {"label": "브렌트유", "category": "원자재"},
    # 환율
    "USD_KRW": {"label": "달러/원", "category": "환율"},
    "EUR_KRW": {"label": "유로/원", "category": "환율"},
    "JPY_KRW": {"label": "엔/원",  "category": "환율"},
    "CNY_KRW": {"label": "위안/원", "category": "환율"},
    # 채권
    "KR3Y":   {"label": "한국3년물", "category": "채권"},
    "US10Y":  {"label": "미국10년물", "category": "채권"},
    # 야간선물
    "NIGHT_FUTURES": {"label": "야간선물", "category": "야간선물"},
}

_EMPTY = {"value": "", "change": "", "change_pct": "", "direction": ""}

_HEADERS = {"User-Agent": "Mozilla/5.0"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _decode_naver_spans(html_fragment: str) -> str:
    """Decode spans like <span class="no5">5</span><span class="jum">.</span> into '5.'"""
    result = []
    for m in re.finditer(r'class="(no\d|jum|shim)"[^>]*>([^<]*)', html_fragment):
        cls, text = m.group(1), m.group(2)
        if cls == "jum":
            result.append(".")
        elif cls == "shim":
            result.append(",")
        elif cls.startswith("no"):
            result.append(text.strip())
    return "".join(result)


def _fmt(val: float, decimals: int = 2) -> str:
    """Format number with commas and decimal places."""
    return f"{val:,.{decimals}f}"


def _calc_change_pct(value_str: str, change_str: str, direction: str) -> str:
    """Calculate change_pct from value and change."""
    try:
        v = float(value_str.replace(",", ""))
        c = float(change_str.replace(",", ""))
        prev = v - c if direction == "up" else v + c
        if prev:
            return f"{c / prev * 100:.2f}%"
    except (ValueError, ZeroDivisionError):
        pass
    return ""


# ---------------------------------------------------------------------------
# Korean index fetchers (KOSPI, KOSDAQ, KOSPI200)
# ---------------------------------------------------------------------------

_KR_INDEX_CODES = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ", "KOSPI200": "KPI200"}


async def _fetch_kr_index(client: httpx.AsyncClient, naver_code: str) -> dict:
    try:
        r = await client.get(
            f"https://finance.naver.com/sise/sise_index.naver?code={naver_code}",
            headers=_HEADERS,
        )
        html = r.content.decode("euc-kr", errors="ignore")

        # Value: try direct text first (KOSPI/KOSDAQ), then nested <strong> (KPI200)
        value_m = re.search(r'id="now_value"[^>]*>([^<]+)', html)
        value_str = value_m.group(1).strip() if value_m and value_m.group(1).strip() else ""
        if not value_str:
            value_m2 = re.search(r'id="now_value"[^>]*>.*?<strong[^>]*>([^<]+)', html, re.DOTALL)
            value_str = value_m2.group(1).strip() if value_m2 else ""

        # Direction: try quotient class (KOSPI/KOSDAQ)
        direction_m = re.search(r'class="quotient\s+(up|dn)"', html)
        d = direction_m.group(1) if direction_m else ""

        # Change: try change_value_and_rate (KOSPI/KOSDAQ)
        change_block = re.search(
            r'change_value_and_rate"[^>]*><span>([^<]+)</span>\s*([-+]?[0-9.]+%)',
            html,
        )
        if change_block:
            change_val = change_block.group(1).strip()
            change_pct = change_block.group(2).strip().lstrip("+-")
        else:
            # Fallback: separate change_value and change_rate elements (KPI200)
            chg_m = re.search(r'id="change_value"[^>]*>.*?>([\d,.]+)', html, re.DOTALL)
            change_val = chg_m.group(1).strip() if chg_m else ""
            rate_m = re.search(r'id="change_rate"[^>]*>.*?>([\d,.]+%)', html, re.DOTALL)
            change_pct = rate_m.group(1).strip().lstrip("+-") if rate_m else ""
            # Direction from change_value class (p11_red=up, p11_blue=down)
            if not d:
                dir_m2 = re.search(r'id="change_value"[^>]*>.*?class="[^"]*p11_(red|blue)', html, re.DOTALL)
                if dir_m2:
                    d = "up" if dir_m2.group(1) == "red" else "dn"

        return {
            "value": value_str,
            "change": change_val,
            "change_pct": change_pct,
            "direction": "up" if d == "up" else "down" if d == "dn" else "",
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# Foreign index fetchers
# ---------------------------------------------------------------------------

_FOREIGN_SYMBOLS = {
    "SPX": "SPI@SPX",
    "IXIC": "NAS@IXIC",
    "DJI": "DJI@DJI",
    "NI225": "NII@NI225",
    "HSI": "HSI@HSI",
    "SHC": "SHS@SHC",
}


async def _fetch_foreign_index(client: httpx.AsyncClient, symbol: str) -> dict:
    try:
        r = await client.get(
            f"https://finance.naver.com/world/sise.naver?symbol={symbol}",
            headers=_HEADERS,
        )
        html = r.content.decode("euc-kr", errors="ignore")

        # Value: class="no_today"
        today_m = re.search(r'class="no_today".*?<em[^>]*>(.*?)</em>', html, re.DOTALL)
        value_str = _decode_naver_spans(today_m.group(1)) if today_m else ""

        # Change and direction: class="no_exday"
        exday_m = re.search(r'class="no_exday".*?</dl>', html, re.DOTALL)
        change_str = ""
        change_pct = ""
        direction = ""
        if exday_m:
            block = exday_m.group(0)
            # Direction from class="no_(up|down)" on em tags
            dir_m = re.search(r'class="no_(up|down)"', block)
            direction = dir_m.group(1) if dir_m else ""
            # Find all em tags within the block
            ems = re.findall(r'<em[^>]*>(.*?)</em>', block, re.DOTALL)
            if len(ems) >= 1:
                change_str = _decode_naver_spans(ems[0])
            if len(ems) >= 2:
                pct_raw = _decode_naver_spans(ems[1])
                change_pct = pct_raw.strip().strip("%").strip()
                if change_pct:
                    change_pct = change_pct + "%"

        # Format value with commas and 2 decimals
        try:
            val_num = float(value_str.replace(",", ""))
            value_str = _fmt(val_num)
        except (ValueError, AttributeError):
            pass

        # Format change
        try:
            chg_num = float(change_str.replace(",", ""))
            change_str = _fmt(chg_num)
        except (ValueError, AttributeError):
            pass

        return {
            "value": value_str,
            "change": change_str,
            "change_pct": change_pct,
            "direction": direction,
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# Marketindex page (gold, WTI, exchange rates, KR bonds)
# ---------------------------------------------------------------------------

_MARKETINDEX_COMMODITY_MAP = {
}

_MARKETINDEX_FX_MAP = {
    "USD_KRW": "head usd",
    "EUR_KRW": "head eur",
    "JPY_KRW": "head jpy",
    "CNY_KRW": "head cny",
}


def _parse_marketindex_block(html: str, head_class: str) -> dict:
    """Parse a head block from the marketindex page (commodities or FX)."""
    pattern = rf'class="{re.escape(head_class)}".*?</a>'
    m = re.search(pattern, html, re.DOTALL)
    if not m:
        return dict(_EMPTY)
    block = m.group(0)
    val_m = re.search(r'class="value">([0-9,.]+)', block)
    chg_m = re.search(r'class="change">\s*([0-9,.]+)', block)
    dir_m = re.search(r'class="head_info\s+point_(up|dn|down)"', block)
    value_str = val_m.group(1) if val_m else ""
    change_str = chg_m.group(1) if chg_m else ""
    d = dir_m.group(1) if dir_m else ""
    direction = "up" if d == "up" else "down" if d in ("dn", "down") else ""

    # Format value
    try:
        val_num = float(value_str.replace(",", ""))
        value_str = _fmt(val_num)
    except (ValueError, AttributeError):
        pass

    # Format change
    try:
        chg_num = float(change_str.replace(",", ""))
        change_str = _fmt(chg_num)
    except (ValueError, AttributeError):
        pass

    change_pct = _calc_change_pct(value_str, change_str, direction)
    return {
        "value": value_str,
        "change": change_str,
        "change_pct": change_pct,
        "direction": direction,
    }


def _parse_kr_bond(html: str) -> dict:
    """Parse Korean 3Y bond rate from marketindex page interest rate table."""
    # Find the row with IRR_GOVT03Y
    m = re.search(
        r'marketindexCd=IRR_GOVT03Y.*?</tr>',
        html,
        re.DOTALL,
    )
    if not m:
        return dict(_EMPTY)
    row_block = m.group(0)
    # Find the enclosing <tr> to get direction from class
    # Search backwards from the match to find <tr class="...">
    pos = html.find("marketindexCd=IRR_GOVT03Y")
    if pos < 0:
        return dict(_EMPTY)
    # Search backwards for the <tr tag
    tr_start = html.rfind("<tr", 0, pos)
    if tr_start < 0:
        return dict(_EMPTY)
    tr_tag = html[tr_start:pos]
    dir_m = re.search(r'class="(up|down|same)"', tr_tag)
    direction = dir_m.group(1) if dir_m else ""
    if direction == "same":
        direction = ""

    # Parse td values: first <td> is rate, second <td> has change
    tds = re.findall(r'<td[^>]*>(.*?)</td>', row_block, re.DOTALL)
    value_str = ""
    change_str = ""
    if len(tds) >= 1:
        value_str = re.sub(r'<[^>]+>', '', tds[0]).strip()
    if len(tds) >= 2:
        change_str = re.sub(r'<[^>]+>', '', tds[1]).strip()

    # Bond rates are small numbers (e.g. 2.65) — format with 2 decimals
    try:
        val_num = float(value_str.replace(",", ""))
        value_str = f"{val_num:.2f}"
    except (ValueError, AttributeError):
        pass

    try:
        chg_num = float(change_str.replace(",", ""))
        change_str = f"{chg_num:.2f}"
    except (ValueError, AttributeError):
        pass

    change_pct = _calc_change_pct(value_str, change_str, direction)
    return {
        "value": value_str,
        "change": change_str,
        "change_pct": change_pct,
        "direction": direction,
    }


async def _fetch_marketindex_page(client: httpx.AsyncClient) -> str:
    """Fetch the marketindex page once, return decoded HTML."""
    r = await client.get(
        "https://finance.naver.com/marketindex/",
        headers=_HEADERS,
    )
    return r.content.decode("euc-kr", errors="ignore")


# ---------------------------------------------------------------------------
# worldDailyQuote fetchers (silver, brent)
# ---------------------------------------------------------------------------

_WORLD_DAILY_CODES = {
    "CMDT_SI": "CMDT_SI",
    "OIL_BRT": "OIL_BRT",
}


async def _fetch_world_daily_quote(client: httpx.AsyncClient, market_code: str) -> dict:
    try:
        r = await client.get(
            f"https://finance.naver.com/marketindex/worldDailyQuote.naver?marketindexCd={market_code}&fdtc=2",
            headers=_HEADERS,
        )
        html = r.content.decode("euc-kr", errors="ignore")
        # First data row: <tr class="(up|down)">
        row_m = re.search(r'<tr\s+class="(up|down|same)"[^>]*>(.*?)</tr>', html, re.DOTALL)
        if not row_m:
            return dict(_EMPTY)
        direction = row_m.group(1)
        if direction == "same":
            direction = ""
        row_html = row_m.group(2)
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
        # tds: [date, value, change, change_pct]
        if len(tds) < 4:
            return dict(_EMPTY)
        value_str = re.sub(r'<[^>]+>', '', tds[1]).strip()
        change_str = re.sub(r'<[^>]+>', '', tds[2]).strip()
        pct_str = re.sub(r'<[^>]+>', '', tds[3]).strip()
        # Strip sign from pct
        pct_str = pct_str.lstrip("+-")

        # Format value
        try:
            val_num = float(value_str.replace(",", ""))
            value_str = _fmt(val_num)
        except (ValueError, AttributeError):
            pass

        # Format change
        try:
            chg_num = float(change_str.replace(",", ""))
            change_str = _fmt(chg_num)
        except (ValueError, AttributeError):
            pass

        return {
            "value": value_str,
            "change": change_str,
            "change_pct": pct_str,
            "direction": direction,
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# US 10Y bond (interestDailyQuote)
# ---------------------------------------------------------------------------


async def _fetch_us10y(client: httpx.AsyncClient) -> dict:
    try:
        r = await client.get(
            "https://finance.naver.com/marketindex/interestDailyQuote.naver?marketindexCd=IRR_US10Y",
            headers=_HEADERS,
        )
        html = r.content.decode("euc-kr", errors="ignore")
        row_m = re.search(r'<tr\s+class="(up|down|same)"[^>]*>(.*?)</tr>', html, re.DOTALL)
        if not row_m:
            return dict(_EMPTY)
        direction = row_m.group(1)
        if direction == "same":
            direction = ""
        row_html = row_m.group(2)
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
        if len(tds) < 4:
            return dict(_EMPTY)
        value_str = re.sub(r'<[^>]+>', '', tds[1]).strip()
        change_str = re.sub(r'<[^>]+>', '', tds[2]).strip()
        pct_str = re.sub(r'<[^>]+>', '', tds[3]).strip()
        pct_str = pct_str.lstrip("+-")

        # Bond rates: format with 2 decimals, no commas
        try:
            val_num = float(value_str.replace(",", ""))
            value_str = f"{val_num:.2f}"
        except (ValueError, AttributeError):
            pass

        try:
            chg_num = float(change_str.replace(",", ""))
            change_str = f"{chg_num:.2f}"
        except (ValueError, AttributeError):
            pass

        return {
            "value": value_str,
            "change": change_str,
            "change_pct": pct_str,
            "direction": direction,
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# Gold futures (Yahoo Finance — near-24h coverage)
# ---------------------------------------------------------------------------


async def _fetch_yahoo_commodity(client: httpx.AsyncClient, symbol: str) -> dict:
    """Fetch commodity futures from Yahoo Finance (e.g., CL=F)."""
    try:
        r = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={"interval": "1d", "range": "5d"},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
        )
        if r.status_code != 200:
            return dict(_EMPTY)
        data = _json.loads(r.text)
        meta = data["chart"]["result"][0]["meta"]
        price = meta["regularMarketPrice"]
        # Use last OHLC close as prev (chartPreviousClose can be stale)
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        valid_closes = [c for c in closes if c is not None]
        prev = valid_closes[-1] if valid_closes else meta["chartPreviousClose"]
        diff = price - prev
        pct = abs(diff) / prev * 100 if prev else 0
        direction = "up" if diff > 0 else "down" if diff < 0 else ""
        return {
            "value": _fmt(price),
            "change": _fmt(abs(diff)),
            "change_pct": f"{pct:.2f}%",
            "direction": direction,
        }
    except Exception:
        return dict(_EMPTY)


async def _fetch_gold_live(client: httpx.AsyncClient) -> dict:
    """Fetch live XAU spot from gold-api.com, prev close from DB."""
    import cache as _cache
    from datetime import date as _date

    try:
        r = await client.get(
            "https://api.gold-api.com/price/XAU/USD",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if r.status_code != 200:
            return dict(_EMPTY)
        data = _json.loads(r.text)
        price = data.get("price")
        if not price:
            return dict(_EMPTY)

        # Load prev close from DB (written by snapshot_gold_close.py at 22:00)
        prev = None
        try:
            stored = await _cache.get_user_setting("__system__", "gold_prev_close")
            if stored:
                prev = float(stored)
        except Exception:
            pass

        if not prev:
            # No stored prev close — show price only, no change
            return {"value": _fmt(price), "change": "", "change_pct": "", "direction": ""}

        diff = price - prev
        pct = abs(diff) / prev * 100 if prev else 0
        direction = "up" if diff > 0 else "down" if diff < 0 else ""
        return {
            "value": _fmt(price),
            "change": _fmt(abs(diff)),
            "change_pct": f"{pct:.2f}%",
            "direction": direction,
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# Night futures (esignal.co.kr socket.io)
# ---------------------------------------------------------------------------


async def _fetch_night_futures(client: httpx.AsyncClient) -> dict:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://esignal.co.kr/kospi200-futures-night/",
            "Origin": "https://esignal.co.kr",
        }
        r1 = await client.get(
            "https://esignal.co.kr/proxy/8888/socket.io/",
            params={"EIO": "3", "transport": "polling"},
            headers=headers,
        )
        m = re.search(r'"sid":"([^"]+)"', r1.text)
        if not m:
            return dict(_EMPTY)
        sid = m.group(1)
        r2 = await client.get(
            "https://esignal.co.kr/proxy/8888/socket.io/",
            params={"EIO": "3", "transport": "polling", "sid": sid},
            headers=headers,
        )
        pm = re.search(r'\["populate","(\{.+?\})"\]', r2.text)
        if not pm:
            return dict(_EMPTY)
        raw = pm.group(1).replace('\\"', '"')
        data = _json.loads(raw)
        val = float(data["value"])
        diff = float(data["value_diff"])
        prev = float(data["value_day"])
        pct = round(abs(diff) / prev * 100, 2) if prev else 0
        direction = "up" if diff > 0 else "down" if diff < 0 else ""
        return {
            "value": _fmt(val),
            "change": _fmt(abs(diff)),
            "change_pct": f"{pct:.2f}%",
            "direction": direction,
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def fetch_indicators(codes: list[str]) -> dict[str, dict]:
    """Fetch multiple indicators in parallel. Returns {code: result_dict}."""
    results: dict[str, dict] = {}
    if not codes:
        return results

    # Group codes by source to minimize HTTP requests
    kr_indices = []       # need individual fetches
    foreign_indices = []  # need individual fetches
    marketindex_items = []  # share one page fetch
    world_daily_items = []  # need individual fetches
    us10y_needed = False
    night_futures_needed = False
    gold_needed = False
    wti_needed = False

    for code in codes:
        if code == "CMDT_GC":
            gold_needed = True
        elif code == "OIL_CL":
            wti_needed = True
        elif code in _KR_INDEX_CODES:
            kr_indices.append(code)
        elif code in _FOREIGN_SYMBOLS:
            foreign_indices.append(code)
        elif code in _MARKETINDEX_COMMODITY_MAP or code in _MARKETINDEX_FX_MAP or code == "KR3Y":
            marketindex_items.append(code)
        elif code in _WORLD_DAILY_CODES:
            world_daily_items.append(code)
        elif code == "US10Y":
            us10y_needed = True
        elif code == "NIGHT_FUTURES":
            night_futures_needed = True

    need_marketindex_page = len(marketindex_items) > 0

    async with httpx.AsyncClient(timeout=5) as client:
        tasks = []
        task_keys = []

        # Korean indices — each needs a separate fetch
        for code in kr_indices:
            naver_code = _KR_INDEX_CODES[code]
            tasks.append(_fetch_kr_index(client, naver_code))
            task_keys.append(("kr", code))

        # Foreign indices — each needs a separate fetch
        for code in foreign_indices:
            symbol = _FOREIGN_SYMBOLS[code]
            tasks.append(_fetch_foreign_index(client, symbol))
            task_keys.append(("foreign", code))

        # Marketindex page — fetch once
        if need_marketindex_page:
            tasks.append(_fetch_marketindex_page(client))
            task_keys.append(("marketindex_page", None))

        # worldDailyQuote — each needs a separate fetch
        for code in world_daily_items:
            market_code = _WORLD_DAILY_CODES[code]
            tasks.append(_fetch_world_daily_quote(client, market_code))
            task_keys.append(("world_daily", code))

        # US 10Y bond
        if us10y_needed:
            tasks.append(_fetch_us10y(client))
            task_keys.append(("us10y", None))

        # Commodities
        if gold_needed:
            tasks.append(_fetch_gold_live(client))
            task_keys.append(("gold", None))
        if wti_needed:
            tasks.append(_fetch_yahoo_commodity(client, "CL=F"))
            task_keys.append(("wti", None))

        # Night futures
        if night_futures_needed:
            tasks.append(_fetch_night_futures(client))
            task_keys.append(("night_futures", None))

        # Run all in parallel
        fetched = await asyncio.gather(*tasks, return_exceptions=True)

        marketindex_html = None

        for (kind, code), result in zip(task_keys, fetched):
            if isinstance(result, Exception):
                result = dict(_EMPTY)

            if kind == "kr":
                results[code] = result
            elif kind == "foreign":
                results[code] = result
            elif kind == "marketindex_page":
                marketindex_html = result
            elif kind == "world_daily":
                results[code] = result
            elif kind == "gold":
                results["CMDT_GC"] = result
            elif kind == "wti":
                results["OIL_CL"] = result
            elif kind == "us10y":
                results["US10Y"] = result
            elif kind == "night_futures":
                results["NIGHT_FUTURES"] = result

        # Parse marketindex page items
        if marketindex_html:
            for code in marketindex_items:
                try:
                    if code in _MARKETINDEX_COMMODITY_MAP:
                        results[code] = _parse_marketindex_block(
                            marketindex_html, _MARKETINDEX_COMMODITY_MAP[code]
                        )
                    elif code in _MARKETINDEX_FX_MAP:
                        results[code] = _parse_marketindex_block(
                            marketindex_html, _MARKETINDEX_FX_MAP[code]
                        )
                    elif code == "KR3Y":
                        results[code] = _parse_kr_bond(marketindex_html)
                except Exception:
                    results[code] = dict(_EMPTY)

    # Fill in any missing codes with empty
    for code in codes:
        if code not in results:
            results[code] = dict(_EMPTY)

    return results
