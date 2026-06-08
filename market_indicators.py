"""
Market indicators module — catalog of 20 indicators with fetch functions.

Public API:
    CATALOG  — dict of all indicators {code: {label, category}}
    fetch_indicators(codes) — fetch multiple indicators in parallel
"""

import asyncio
import json as _json
import os
import re
from datetime import datetime, timedelta

import httpx

from cache_layer import MemoryTTLCache

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
    "AUD_KRW": {"label": "호주달러/원", "category": "환율"},
    "VND_KRW": {"label": "베트남동/원", "category": "환율"},
    "USD_IDX": {"label": "달러지수", "category": "환율"},
    # 국채 — country(KR/US/기타)·maturity(년, overnight=0)로 프론트가 yield curve·국가비교 구성
    "US_SOFR": {"label": "미국 SOFR", "category": "국채", "country": "US", "maturity": 0},
    "US3M":   {"label": "미국3개월", "category": "국채", "country": "US", "maturity": 0.25},
    "US6M":   {"label": "미국6개월", "category": "국채", "country": "US", "maturity": 0.5},
    "US1Y":   {"label": "미국1년물", "category": "국채", "country": "US", "maturity": 1},
    "US2Y":   {"label": "미국2년물", "category": "국채", "country": "US", "maturity": 2},
    "US3Y":   {"label": "미국3년물", "category": "국채", "country": "US", "maturity": 3},
    "US5Y":   {"label": "미국5년물", "category": "국채", "country": "US", "maturity": 5},
    "US10Y":  {"label": "미국10년물", "category": "국채", "country": "US", "maturity": 10},
    "US20Y":  {"label": "미국20년물", "category": "국채", "country": "US", "maturity": 20},
    "US30Y":  {"label": "미국30년물", "category": "국채", "country": "US", "maturity": 30},
    "KOFR":     {"label": "KOFR",     "category": "국채", "country": "KR", "maturity": 0},
    "KR_CD91":  {"label": "한국 CD(91일)", "category": "국채", "country": "KR", "maturity": 0.25},
    "KR_KORIBOR6M": {"label": "한국 KORIBOR(6개월)", "category": "국채", "country": "KR", "maturity": 0.5},
    "KR_MSB1Y": {"label": "통안채1년", "category": "국채", "country": "KR", "maturity": 1},
    "KR2Y":   {"label": "한국2년물", "category": "국채", "country": "KR", "maturity": 2},
    "KR3Y":   {"label": "한국3년물", "category": "국채", "country": "KR", "maturity": 3},
    "KR5Y":   {"label": "한국5년물", "category": "국채", "country": "KR", "maturity": 5},
    "KR10Y":  {"label": "한국10년물", "category": "국채", "country": "KR", "maturity": 10},
    "KR20Y":  {"label": "한국20년물", "category": "국채", "country": "KR", "maturity": 20},
    "KR30Y":  {"label": "한국30년물", "category": "국채", "country": "KR", "maturity": 30},
    "JP6M":   {"label": "일본6개월", "category": "국채", "country": "JP", "maturity": 0.5},
    "JP2Y":   {"label": "일본2년물", "category": "국채", "country": "JP", "maturity": 2},
    "JP3Y":   {"label": "일본3년물", "category": "국채", "country": "JP", "maturity": 3},
    "JP5Y":   {"label": "일본5년물", "category": "국채", "country": "JP", "maturity": 5},
    "JP10Y":  {"label": "일본10년물", "category": "국채", "country": "JP", "maturity": 10},
    "JP20Y":  {"label": "일본20년물", "category": "국채", "country": "JP", "maturity": 20},
    "JP30Y":  {"label": "일본30년물", "category": "국채", "country": "JP", "maturity": 30},
    "DE10Y":  {"label": "독일10년물", "category": "국채", "country": "DE", "maturity": 10},
    "FR10Y":  {"label": "프랑스10년물", "category": "국채", "country": "FR", "maturity": 10},
    "GB10Y":  {"label": "영국10년물", "category": "국채", "country": "GB", "maturity": 10},
    "AU10Y":  {"label": "호주10년물", "category": "국채", "country": "AU", "maturity": 10},
    "CN10Y":  {"label": "중국10년물", "category": "국채", "country": "CN", "maturity": 10},
    # 야간선물
    "NIGHT_FUTURES": {"label": "야간선물", "category": "야간선물"},
    # 바이낸스 USDⓈ-M 선물 (토큰화 주식 무기한) — USDT 가격·24h 등락
    "BNB_EWY":     {"label": "EWY",        "category": "바이낸스"},
    "BNB_SAMSUNG": {"label": "삼성전자",   "category": "바이낸스"},
    "BNB_SKHYNIX": {"label": "SK하이닉스", "category": "바이낸스"},
    "BNB_HYUNDAI": {"label": "현대차",     "category": "바이낸스"},
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


def _indicator_has_value(data: dict | None) -> bool:
    if not data:
        return False
    value = data.get("value")
    return value is not None and str(value).strip() != ""


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
            # Fallback: KPI200 layout. The change magnitude lives in a <span>
            # inside id="change_value" and the signed rate in a <strong> inside
            # id="change_rate". Anchoring on those exact tags avoids grabbing
            # stray digits (e.g. an <img width="7">) or an unrelated "0.00%".
            chg_m = re.search(
                r'id="change_value".*?<span[^>]*>\s*([\d,.]+)\s*<', html, re.DOTALL
            )
            change_val = chg_m.group(1).strip() if chg_m else ""
            rate_m = re.search(
                r'id="change_rate".*?<strong[^>]*>\s*([+-]?[\d,.]+)\s*%', html, re.DOTALL
            )
            rate_raw = rate_m.group(1).strip() if rate_m else ""
            # A rate is only trustworthy alongside a parsed magnitude; otherwise
            # leave it blank so downstream omits the index instead of asserting 0%.
            change_pct = rate_raw.lstrip("+-") + "%" if (rate_raw and change_val) else ""
            if not d:
                if rate_raw.startswith("-"):
                    d = "dn"
                elif re.search(r'id="change_value".*?ico_down\.gif', html, re.DOTALL):
                    d = "dn"
                elif re.search(r'id="change_value".*?ico_up\.gif', html, re.DOTALL):
                    d = "up"
                elif rate_raw and float(rate_raw) > 0:
                    d = "up"

        direction = "up" if d == "up" else "down" if d == "dn" else ""

        # Reconcile: recompute the rate from value + magnitude and prefer it when
        # the scraped rate is missing or inconsistent. This is the structural fix
        # for the "변동 없이 0.00%" bug — a fabricated/stray rate can no longer
        # survive a cross-check against the actual value and change.
        if value_str and change_val and direction:
            recomputed = _calc_change_pct(value_str, change_val, direction)
            if recomputed:
                scraped = float(change_pct.rstrip("%")) if change_pct.rstrip("%").replace(".", "").isdigit() else None
                if scraped is None or abs(scraped - float(recomputed.rstrip("%"))) > 0.1:
                    change_pct = recomputed
        elif not change_val:
            # No reliable magnitude → never emit a change/rate at all.
            change_val = ""
            change_pct = ""

        return {
            "value": value_str,
            "change": change_val,
            "change_pct": change_pct,
            "direction": direction,
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
# Binance USDⓈ-M Futures (tokenized stock perps) — 한 번의 배치 요청
# ---------------------------------------------------------------------------
_BINANCE_MAP = {
    "BNB_EWY":     "EWYUSDT",
    "BNB_SAMSUNG": "SAMSUNGUSDT",
    "BNB_SKHYNIX": "SKHYNIXUSDT",
    "BNB_HYUNDAI": "HYUNDAIUSDT",
}


async def _fetch_binance_tickers(client: httpx.AsyncClient, codes: list[str]) -> dict:
    """바이낸스 USDⓈ-M 선물 24h 티커(배치). {code: {value, change, change_pct, direction}}.

    값은 USDT 가격, change 는 절대 변동(부호 제외, 프론트가 direction 으로 부호 표시).
    실패·결측은 _EMPTY 로 안전 폴백한다.
    """
    out: dict[str, dict] = {}
    symbols = [_BINANCE_MAP[c] for c in codes if c in _BINANCE_MAP]
    if not symbols:
        return out
    try:
        r = await client.get(
            "https://fapi.binance.com/fapi/v1/ticker/24hr",
            params={"symbols": _json.dumps(symbols, separators=(",", ":"))},
            headers=_HEADERS,
        )
        rows = r.json()
        by_symbol = {
            row.get("symbol"): row for row in rows if isinstance(row, dict)
        } if isinstance(rows, list) else {}
        for code in codes:
            row = by_symbol.get(_BINANCE_MAP.get(code))
            if not row:
                out[code] = dict(_EMPTY)
                continue
            try:
                last = float(row.get("lastPrice"))
                chg = float(row.get("priceChange"))
                pct = float(row.get("priceChangePercent"))
            except (TypeError, ValueError):
                out[code] = dict(_EMPTY)
                continue
            direction = "up" if pct > 0 else ("down" if pct < 0 else "")
            out[code] = {
                "value": _fmt(last),
                "change": _fmt(abs(chg)),
                "change_pct": f"{abs(pct):.2f}%",
                "direction": direction,
            }
    except Exception:
        for code in codes:
            out.setdefault(code, dict(_EMPTY))
    return out


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
    "USD_IDX": "head usd_idx",  # 달러지수 — 메인 페이지 head 블록 구조가 환율과 동일
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
    """Fetch US 10Y Treasury yield from Yahoo Finance (^TNX)."""
    try:
        r = await client.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5ETNX",
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
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        valid_closes = [c for c in closes if c is not None]
        prev = valid_closes[-2] if len(valid_closes) >= 2 else meta.get("chartPreviousClose", price)
        diff = price - prev
        pct = abs(diff) / prev * 100 if prev else 0
        direction = "up" if diff > 0 else "down" if diff < 0 else ""
        return {
            "value": f"{price:.2f}",
            "change": f"{abs(diff):.2f}",
            "change_pct": f"{pct:.2f}%",
            "direction": direction,
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# US overnight RFR (SOFR — Federal Reserve Bank of New York public API)
# ---------------------------------------------------------------------------


async def _fetch_sofr(client: httpx.AsyncClient) -> dict:
    """Fetch SOFR (US 익일물 RFR, KOFR 대응) from the NY Fed public rates API.

    ECOS KOFR 와 같은 표시 정밀도(.2f)·직전 영업일 대비 방식으로 맞춘다.
    """
    try:
        r = await client.get(
            "https://markets.newyorkfed.org/api/rates/secured/sofr/last/2.json",
            headers={"User-Agent": "value-invest/1.0", "Accept": "application/json"},
        )
        if r.status_code != 200:
            return dict(_EMPTY)
        refs = (_json.loads(r.text).get("refRates") or [])
        pts = [
            (x.get("effectiveDate"), x.get("percentRate"))
            for x in refs if x.get("percentRate") is not None and x.get("effectiveDate")
        ]
        if not pts:
            return dict(_EMPTY)
        pts.sort(key=lambda x: x[0], reverse=True)  # 최신 영업일 우선
        price = float(pts[0][1])
        prev = float(pts[1][1]) if len(pts) >= 2 else price
        diff = round(price - prev, 2)
        if diff == 0:
            return {"value": f"{price:.2f}", "change": "", "change_pct": "", "direction": ""}
        change_pct = f"{abs(diff) / prev * 100:.2f}%" if prev else ""
        return {
            "value": f"{price:.2f}",
            "change": f"{abs(diff):.2f}",
            "change_pct": change_pct,
            "direction": "up" if diff > 0 else "down",
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# FX daily quote (AUD, VND — not on the marketindex front-page head blocks)
# ---------------------------------------------------------------------------

_FX_DAILY_MAP = {
    "AUD_KRW": "FX_AUDKRW",
    "VND_KRW": "FX_VNDKRW",  # 네이버 표기와 동일하게 100동당 원으로 인용
}


async def _fetch_fx_daily(client: httpx.AsyncClient, fx_code: str) -> dict:
    """Fetch an FX rate + daily change from Naver's exchangeDailyQuote page.

    Used for currencies absent from the marketindex front-page head blocks
    (AUD, VND). The first two rows are today's and the previous business day's
    rates; the daily change is derived from them.
    """
    try:
        r = await client.get(
            f"https://finance.naver.com/marketindex/exchangeDailyQuote.naver?marketindexCd={fx_code}",
            headers=_HEADERS,
        )
        html = r.content.decode("euc-kr", errors="ignore")
        rows = re.findall(
            r'<tr class="(?:up|down)">\s*<td class="date">[^<]+</td>\s*<td class="num">([\d,\.]+)</td>',
            html,
        )
        if not rows:
            return dict(_EMPTY)
        price = float(rows[0].replace(",", ""))
        prev = float(rows[1].replace(",", "")) if len(rows) >= 2 else price
        diff = price - prev
        direction = "up" if diff > 0 else "down" if diff < 0 else ""
        change_pct = f"{abs(diff) / prev * 100:.2f}%" if prev else ""
        return {
            "value": _fmt(price),
            "change": _fmt(abs(diff)),
            "change_pct": change_pct,
            "direction": direction,
        }
    except Exception:
        return dict(_EMPTY)


# ---------------------------------------------------------------------------
# Government bond yields (CNBC quote API — one request covers all symbols)
# ---------------------------------------------------------------------------

# 내부 코드 → CNBC 심볼. US10Y(Yahoo ^TNX)·KR3Y(Naver)는 기존 경로를 유지하고,
# 신규 만기/국가만 CNBC 한 번의 묶음 요청으로 가져온다.
_CNBC_BOND_MAP = {
    "US3M":  "US3M",
    "US1Y":  "US1Y",
    "US2Y":  "US2Y",
    "US6M":  "US6M",
    "US3Y":  "US3Y",
    "US5Y":  "US5Y",
    "US20Y": "US20Y",
    "US30Y": "US30Y",
    "KR5Y":  "KR5Y-KR",
    "KR10Y": "KR10Y-KR",
    "JP6M":  "JP6M-JP",
    "JP2Y":  "JP2Y-JP",
    "JP3Y":  "JP3Y-JP",
    "JP5Y":  "JP5Y-JP",
    "JP10Y": "JP10Y-JP",
    "JP20Y": "JP20Y-JP",
    "JP30Y": "JP30Y-JP",
    "DE10Y": "DE10Y-DE",
    "FR10Y": "FR10Y-FR",
    "GB10Y": "UK10Y-GB",
    "AU10Y": "AU10Y-AU",
    "CN10Y": "CN10Y-CN",
}


def _parse_cnbc_quote(q: dict) -> dict:
    """Map one CNBC FormattedQuote into the indicator result shape."""
    last_raw = str(q.get("last") or "").strip().rstrip("%").strip()
    try:
        value = float(last_raw.replace(",", ""))
    except ValueError:
        return dict(_EMPTY)
    value_str = f"{value:.2f}"

    changetype = str(q.get("changetype") or "").upper()
    chg_raw = str(q.get("change") or "").strip()
    # 'UNCH' = unchanged; treat as flat with no delta.
    if chg_raw in ("", "UNCH", "NA") or changetype == "UNCH":
        return {"value": value_str, "change": "", "change_pct": "", "direction": ""}
    try:
        chg = float(chg_raw.replace(",", "").replace("+", ""))
    except ValueError:
        return {"value": value_str, "change": "", "change_pct": "", "direction": ""}

    direction = (
        "up" if changetype == "UP"
        else "down" if changetype == "DOWN"
        else "up" if chg > 0 else "down" if chg < 0 else ""
    )

    # CNBC's own change_pct for yields is unreliable; derive it from yesterday's
    # close to match the US10Y/KR3Y convention (|Δyield| / prev_yield).
    change_pct = ""
    prev_raw = str(q.get("previous_day_closing") or "").rstrip("%").strip()
    try:
        prev = float(prev_raw.replace(",", ""))
        if prev:
            change_pct = f"{abs(chg) / prev * 100:.2f}%"
    except ValueError:
        pass

    return {
        "value": value_str,
        "change": f"{abs(chg):.2f}",
        "change_pct": change_pct,
        "direction": direction,
    }


async def _fetch_cnbc_bonds(client: httpx.AsyncClient, codes: list[str]) -> dict[str, dict]:
    """Fetch several government-bond yields from CNBC in one batched request."""
    out = {code: dict(_EMPTY) for code in codes}
    sym_to_code = {_CNBC_BOND_MAP[c]: c for c in codes if c in _CNBC_BOND_MAP}
    if not sym_to_code:
        return out
    try:
        r = await client.get(
            "https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol",
            params={
                "symbols": "|".join(sym_to_code.keys()),
                "requestMethod": "itv",
                "noform": "1",
                "partnerId": "2",
                "fund": "1",
                "exthrs": "1",
                "output": "json",
            },
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        )
        if r.status_code != 200:
            return out
        data = _json.loads(r.text)
        quotes = data.get("FormattedQuoteResult", {}).get("FormattedQuote") or []
        for q in quotes:
            code = sym_to_code.get(q.get("symbol"))
            if code:
                out[code] = _parse_cnbc_quote(q)
    except Exception:
        pass
    return out


# ---------------------------------------------------------------------------
# Korean bond yields (Bank of Korea ECOS — 국고채 전 만기·통안채·KOFR)
# ---------------------------------------------------------------------------

# CNBC 에 없는 한국물(전 만기 국고채·통안채·KOFR)을 한국은행 ECOS 시장금리(일별)
# 통계표 817Y002 에서 가져온다. 기존 KR3Y(Naver)·KR5Y/KR10Y(CNBC)는 실시간성이
# 좋아 그대로 두고, ECOS 가 유일 소스인 항목만 여기서 채운다.
# ECOS 일별 금리는 직전 영업일 기준으로 공표(당일 장중값 아님)된다.
_ECOS_STAT = "817Y002"
_ECOS_BOND_MAP = {
    "KOFR":     "010901000",  # KOFR(공시RFR)
    "KR_CD91":  "010502000",  # CD(91일) — 미국 3M(US3M)에 대응하는 한국 단기금리
    "KR_KORIBOR6M": "010151000",  # KORIBOR(6개월) — 한국 6개월 단기금리(국고채 6M 부재)
    "KR_MSB1Y": "010400001",  # 통안증권(1년)
    "KR2Y":     "010195000",  # 국고채(2년)
    "KR20Y":    "010220000",  # 국고채(20년)
    "KR30Y":    "010230000",  # 국고채(30년)
}


async def _fetch_ecos_bonds(client: httpx.AsyncClient, codes: list[str]) -> dict[str, dict]:
    """Fetch Korean bond yields from the BOK ECOS API (one request, all items).

    Returns {internal_code: result_dict}. Empty results if ECOS_API_KEY is unset.
    """
    out = {code: dict(_EMPTY) for code in codes}
    api_key = os.getenv("ECOS_API_KEY", "").strip()
    item_to_code = {_ECOS_BOND_MAP[c]: c for c in codes if c in _ECOS_BOND_MAP}
    if not api_key or not item_to_code:
        return out
    try:
        end = datetime.now()
        start = end - timedelta(days=14)  # 최근 2영업일 확보(공휴일·공표지연 여유)
        url = (
            f"https://ecos.bok.or.kr/api/StatisticSearch/{api_key}/json/kr/1/700/"
            f"{_ECOS_STAT}/D/{start:%Y%m%d}/{end:%Y%m%d}"
        )
        r = await client.get(url, headers={"User-Agent": "value-invest/1.0"})
        if r.status_code != 200:
            return out
        rows = (_json.loads(r.text).get("StatisticSearch") or {}).get("row") or []
        # 항목코드별 (날짜, 값) 시계열 수집 — 필요한 항목만.
        series: dict[str, list] = {}
        for row in rows:
            ic = row.get("ITEM_CODE1")
            if ic not in item_to_code:
                continue
            val, t = row.get("DATA_VALUE"), row.get("TIME")
            if val in (None, "") or not t:
                continue
            try:
                series.setdefault(ic, []).append((t, float(val)))
            except (ValueError, TypeError):
                continue
        for ic, pts in series.items():
            pts.sort(key=lambda x: x[0])
            price = pts[-1][1]
            prev = pts[-2][1] if len(pts) >= 2 else price
            diff = round(price - prev, 2)  # 표시 정밀도(.2f) 이하 변동은 '변동 없음'
            if diff == 0:
                out[item_to_code[ic]] = {
                    "value": f"{price:.2f}", "change": "", "change_pct": "", "direction": "",
                }
                continue
            change_pct = f"{abs(diff) / prev * 100:.2f}%" if prev else ""
            out[item_to_code[ic]] = {
                "value": f"{price:.2f}",
                "change": f"{abs(diff):.2f}",
                "change_pct": change_pct,
                "direction": "up" if diff > 0 else "down",
            }
    except Exception:
        pass
    return out


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
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        valid_closes = [c for c in closes if c is not None]
        prev = valid_closes[-2] if len(valid_closes) >= 2 else meta.get("chartPreviousClose", price)
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
    """Fetch a fast gold futures daily quote for dashboard/benchmark use."""
    # The spot API can hang past its nominal timeout in some network states.
    # For the dashboard/benchmark UI, a fast daily futures move is safer than
    # blocking the whole market summary while waiting for a marginally fresher
    # spot quote.
    return await _fetch_yahoo_commodity(client, "GC=F")


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
        # EIO=4 (Socket.IO v4) handshake
        r1 = await client.get(
            "https://esignal.co.kr/proxy/8888/socket.io/",
            params={"EIO": "4", "transport": "polling"},
            headers=headers,
        )
        m = re.search(r'"sid":"([^"]+)"', r1.text)
        if not m:
            return dict(_EMPTY)
        sid = m.group(1)
        # Send namespace connect packet
        await client.post(
            "https://esignal.co.kr/proxy/8888/socket.io/",
            params={"EIO": "4", "transport": "polling", "sid": sid},
            headers=headers,
            content="40",
        )
        # Poll for data
        r3 = await client.get(
            "https://esignal.co.kr/proxy/8888/socket.io/",
            params={"EIO": "4", "transport": "polling", "sid": sid},
            headers=headers,
        )
        pm = re.search(r'\["populate","(\{.+?\})"\]', r3.text)
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


# Module-level cache so AI analysis / market-bar polling / admin page
# don't each re-scrape Naver on every call. Keyed by the sorted codes tuple
# so different code sets don't collide.
_INDICATORS_TTL = 60  # seconds — market bar ticks every 60s anyway
_indicators_cache = MemoryTTLCache("market_indicators.batch", _INDICATORS_TTL)
_indicator_item_cache = MemoryTTLCache("market_indicators.item", _INDICATORS_TTL)


async def fetch_indicators(codes: list[str]) -> dict[str, dict]:
    """Fetch multiple indicators in parallel. Returns {code: result_dict}."""
    results: dict[str, dict] = {}
    if not codes:
        return results
    requested_codes = list(dict.fromkeys(codes))
    key = tuple(sorted(requested_codes))
    cached = _indicators_cache.get(key)
    if cached:
        return dict(cached)

    fetch_codes: list[str] = []
    stale_results: dict[str, dict] = {}
    for code in requested_codes:
        item_cached = _indicator_item_cache.get_entry(code, allow_stale=True)
        if item_cached and item_cached.fresh:
            results[code] = dict(item_cached.value)
        else:
            if item_cached and _indicator_has_value(item_cached.value):
                stale_results[code] = dict(item_cached.value)
            fetch_codes.append(code)

    if not fetch_codes:
        _indicators_cache.set(key, dict(results))
        return dict(results)

    # Group codes by source to minimize HTTP requests
    kr_indices = []       # need individual fetches
    foreign_indices = []  # need individual fetches
    marketindex_items = []  # share one page fetch
    world_daily_items = []  # need individual fetches
    fx_daily_items = []   # AUD/VND — exchangeDailyQuote, one request each
    cnbc_bond_items = []  # government bonds — one batched CNBC request
    ecos_bond_items = []  # Korean bonds — one batched ECOS request
    us10y_needed = False
    sofr_needed = False
    night_futures_needed = False
    gold_needed = False
    wti_needed = False
    binance_items = []  # 바이낸스 선물 — one batched request

    for code in fetch_codes:
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
        elif code in _FX_DAILY_MAP:
            fx_daily_items.append(code)
        elif code in _CNBC_BOND_MAP:
            cnbc_bond_items.append(code)
        elif code in _ECOS_BOND_MAP:
            ecos_bond_items.append(code)
        elif code in _WORLD_DAILY_CODES:
            world_daily_items.append(code)
        elif code == "US10Y":
            us10y_needed = True
        elif code == "US_SOFR":
            sofr_needed = True
        elif code == "NIGHT_FUTURES":
            night_futures_needed = True
        elif code in _BINANCE_MAP:
            binance_items.append(code)

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

        # FX daily quote (AUD, VND) — each needs a separate fetch
        for code in fx_daily_items:
            tasks.append(_fetch_fx_daily(client, _FX_DAILY_MAP[code]))
            task_keys.append(("fx_daily", code))

        # Government bonds (CNBC) — one batched request covers all symbols
        if cnbc_bond_items:
            tasks.append(_fetch_cnbc_bonds(client, cnbc_bond_items))
            task_keys.append(("cnbc_bonds", None))

        # Korean bonds (ECOS) — one batched request covers all items
        if ecos_bond_items:
            tasks.append(_fetch_ecos_bonds(client, ecos_bond_items))
            task_keys.append(("ecos_bonds", None))

        # US 10Y bond
        if us10y_needed:
            tasks.append(_fetch_us10y(client))
            task_keys.append(("us10y", None))

        # US overnight RFR (SOFR)
        if sofr_needed:
            tasks.append(_fetch_sofr(client))
            task_keys.append(("sofr", None))

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

        # Binance USDⓈ-M futures — one batched request covers all symbols
        if binance_items:
            tasks.append(_fetch_binance_tickers(client, binance_items))
            task_keys.append(("binance", None))

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
            elif kind == "fx_daily":
                results[code] = result
            elif kind == "cnbc_bonds":
                # result is {internal_code: data}; copy only requested codes so
                # an unexpected shape can't overwrite unrelated entries.
                if isinstance(result, dict):
                    for c in cnbc_bond_items:
                        if c in result:
                            results[c] = result[c]
            elif kind == "ecos_bonds":
                if isinstance(result, dict):
                    for c in ecos_bond_items:
                        if c in result:
                            results[c] = result[c]
            elif kind == "gold":
                results["CMDT_GC"] = result
            elif kind == "wti":
                results["OIL_CL"] = result
            elif kind == "us10y":
                results["US10Y"] = result
            elif kind == "sofr":
                results["US_SOFR"] = result
            elif kind == "night_futures":
                results["NIGHT_FUTURES"] = result
            elif kind == "binance":
                # result is {code: data}; copy only requested codes.
                if isinstance(result, dict):
                    for c in binance_items:
                        if c in result:
                            results[c] = result[c]

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
    for code in fetch_codes:
        current = results.get(code) or dict(_EMPTY)
        if not _indicator_has_value(current) and code in stale_results:
            current = {**stale_results[code], "_stale": True}
        results[code] = current

    for code in fetch_codes:
        _indicator_item_cache.set(code, dict(results.get(code) or _EMPTY))

    final_results = {code: dict(results.get(code) or _EMPTY) for code in requested_codes}
    _indicators_cache.set(key, dict(final_results))
    return final_results


# ---------------------------------------------------------------------------
# 박스 단위 라이브 갱신 — 야간선물·바이낸스처럼 빠르게 변하는 지표만 짧은 TTL 로.
# ---------------------------------------------------------------------------
_LIVE_TTL = 8  # seconds — 클라이언트 10초 폴링보다 짧게(동시·연속 폴링 합치기)
_live_cache = MemoryTTLCache("market_indicators.live", _LIVE_TTL)
_LIVE_CODES = {"NIGHT_FUTURES", *_BINANCE_MAP}


async def fetch_indicators_live(codes: list[str]) -> dict[str, dict]:
    """야간선물·바이낸스만 짧은 TTL 로 조회한다(박스 단위 갱신용).

    60초 batch/item 캐시를 거치지 않고 해당 fetcher 를 직접 호출하되, 8초
    캐시로 동시/연속 폴링을 합쳐 외부 부하를 막는다. 지원하지 않는 코드는 무시.
    """
    wanted = [c for c in dict.fromkeys(codes) if c in _LIVE_CODES]
    if not wanted:
        return {}
    key = tuple(sorted(wanted))
    cached = _live_cache.get(key)
    if cached:
        return dict(cached)

    results: dict[str, dict] = {}
    binance_items = [c for c in wanted if c in _BINANCE_MAP]
    async with httpx.AsyncClient(timeout=5) as client:
        tasks = []
        task_kinds = []
        if binance_items:
            tasks.append(_fetch_binance_tickers(client, binance_items))
            task_kinds.append("binance")
        if "NIGHT_FUTURES" in wanted:
            tasks.append(_fetch_night_futures(client))
            task_kinds.append("night")
        fetched = await asyncio.gather(*tasks, return_exceptions=True)
        for kind, res in zip(task_kinds, fetched):
            if isinstance(res, Exception):
                continue
            if kind == "binance" and isinstance(res, dict):
                results.update(res)
            elif kind == "night":
                results["NIGHT_FUTURES"] = res

    for c in wanted:
        results.setdefault(c, dict(_EMPTY))
    _live_cache.set(key, dict(results))
    return dict(results)
