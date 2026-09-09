"""종목별 배당 날짜 수집. 지급일과 권리일을 섞지 않고 원본 의미를 보존한다."""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

import kis_proxy_client
from core.http import get_http_client
from repositories.cache_values import get_cache_value_entry, set_cache_value
from repositories.ticker_map import load_ticker_map
from services.portfolio.identifiers import is_korean_stock, static_foreign_ticker

logger = logging.getLogger(__name__)
NAMESPACE = "dividend.schedule.v1"
TTL = 6 * 3600
RETRY_TTL = 300
BATCH_TIMEOUT = 15
AGNC_URL = "https://investors.agnc.com/stock-information/dividend-history"
SCHWAB_BASE = "https://www.schwabassetmanagement.com/products/"
# 발행사 상품군. 주기 자체는 상품별 지급 이력에서 판단한다.
SCHWAB_TICKERS = frozenset("SCHP SCHZ SCHO SCHR SCHQ SCHI SCHJ SCHB SCHX SCHG SCHV SCHA SCHM SCHD SCHH SCHF SCHE SCHC SCHY SCMB SCYB".split())


def number(value) -> float | None:
    try:
        result = float(str(value).replace(",", "").replace("$", "").strip())
        return result if math.isfinite(result) and result >= 0 else None
    except (TypeError, ValueError):
        return None


def parse_day(value) -> str | None:
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_official_html(html: str, ticker: str) -> list[dict]:
    """열 제목으로 매핑하여 숨김 열·모바일 표에도 날짜 의미를 보존한다."""
    soup = BeautifulSoup(html, "html.parser")
    is_agnc = ticker == "AGNC"
    required = ["Ex-Dividend Date", "Record Date", "Payment Date", "Dividend Per Share"] if is_agnc else ["Ex-Date", "Record Date", "Payable Date", "Total Distribution"]
    events = {}
    for table in soup.select("table"):
        headers = [cell.get_text(" ", strip=True) for cell in table.select("thead th")]
        if not all(key in headers for key in required):
            continue
        for row in table.select("tbody tr"):
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"], recursive=False)]
            if len(cells) != len(headers):
                continue
            values = dict(zip(headers, cells))
            ex, record, pay = [parse_day(values[key]) for key in required[:3]]
            amount = number(values[required[3]])
            if not pay or amount is None:
                continue
            events[(ex, pay)] = {
                "ex_date": ex, "record_date": record, "pay_date": pay,
                "declaration_date": parse_day(values.get("Declaration Date")),
                "amount_per_share": amount, "currency": "USD",
                "source": "AGNC 공시" if is_agnc else "Schwab 공시",
                "source_url": AGNC_URL if is_agnc else SCHWAB_BASE + ticker.lower(),
            }
    if not events:
        raise ValueError("공식 배당 표를 확인할 수 없습니다")
    return list(events.values())


def parse_kis_dividends(payload: dict, code: str) -> list[dict]:
    """KIS 예탁원정보: record_date는 기준일, divi_pay_dt는 현금 지급일."""
    rows = payload.get("items")
    if not isinstance(rows, list):
        raise ValueError("예탁원 배당 일정 응답이 없습니다")
    events = {}
    for row in rows:
        if row.get("sht_cd") and row["sht_cd"] != code:
            continue
        record = parse_day(row.get("record_date"))
        if not record:
            continue
        events[record] = {"record_date": record, "ex_date": None, "pay_date": parse_day(row.get("divi_pay_dt")),
                          "amount_per_share": number(row.get("per_sto_divi_amt")), "currency": "KRW",
                          "source": "KIS·예탁원 배당 일정", "source_url": "https://apiportal.koreainvestment.com/"}
    return list(events.values())


def parse_yahoo_chart(payload: dict, ticker: str) -> list[dict]:
    results = payload.get("chart", {}).get("result") or []
    if not results:
        raise ValueError("배당 이력 응답이 없습니다")
    result = results[0]
    currency = result.get("meta", {}).get("currency")
    if not currency:
        raise ValueError("배당 통화를 확인할 수 없습니다")
    # Yahoo의 dividends 타임스탬프는 배당락일이다. 지급일로 사용하지 않는다.
    offset = float(result.get("meta", {}).get("gmtoffset") or 0)
    zone = timezone(timedelta(seconds=offset))
    events = []
    for row in (result.get("events", {}).get("dividends") or {}).values():
        amount = number(row.get("amount"))
        stamp = number(row.get("date"))
        if amount is None or stamp is None:
            continue
        events.append({
            "ex_date": datetime.fromtimestamp(stamp, zone).date().isoformat(),
            "record_date": None, "pay_date": None, "declaration_date": None,
            "amount_per_share": amount, "currency": currency.upper(),
            "source": "Yahoo 배당락 이력",
            "source_url": f"https://finance.yahoo.com/quote/{quote(ticker, safe='')}/history/?filter=div",
        })
    return events


async def fetch_history(ticker: str) -> dict:
    client = await get_http_client("dividend_schedule")
    headers = {"User-Agent": "Mozilla/5.0"}
    official = ticker == "AGNC" or ticker in SCHWAB_TICKERS
    domestic_events = None
    if ticker.endswith((".KS", ".KQ")) and is_korean_stock(ticker[:-3]):
        try:
            async with asyncio.timeout(4):
                payload = await kis_proxy_client.get_dividends(
                    ticker[:-3], start_date=date.today() - timedelta(days=800),
                    end_date=date.today() + timedelta(days=365),
                )
            domestic_events = parse_kis_dividends(payload, ticker[:-3]) or None
        except (TimeoutError, kis_proxy_client.KISProxyError, httpx.HTTPError, ValueError, TypeError) as exc:
            logger.info("예탁원 일정 조회 보류 (%s): %s", ticker, type(exc).__name__)
    if domestic_events is not None:
        events = domestic_events
        official = True
    elif official:
        url = AGNC_URL if ticker == "AGNC" else SCHWAB_BASE + ticker.lower()
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        events = await asyncio.to_thread(parse_official_html, response.text, ticker)
    else:
        response = await client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker, safe='')}",
            params={"range": "2y", "interval": "1d", "events": "div"}, headers=headers,
        )
        if response.status_code == 404 and ticker.endswith(".KS"):
            ticker = ticker[:-3] + ".KQ"
            response = await client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker, safe='')}",
                params={"range": "2y", "interval": "1d", "events": "div"}, headers=headers,
            )
        response.raise_for_status()
        events = parse_yahoo_chart(response.json(), ticker)
    cutoff = (date.today() - timedelta(days=800)).isoformat()
    events = [event for event in events if (event.get("pay_date") or event.get("ex_date") or event.get("record_date") or "") >= cutoff]
    return {
        "events": events, "official": official,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": "fresh",
    }


async def _history(ticker: str, semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        cached = await get_cache_value_entry(NAMESPACE, ticker, allow_stale=True)
        if cached and not cached.stale:
            return cached.value
        try:
            result = await fetch_history(ticker)
            if cached and cached.value.get("official") and not result.get("official"):
                raise ValueError("공식 지급일 자료 갱신 보류")
        except (httpx.HTTPError, ValueError, TypeError, KeyError, OverflowError) as exc:
            logger.warning("배당 일정 조회 실패 (%s): %s", ticker, type(exc).__name__)
            result = {**(cached.value if cached else {"events": [], "fetched_at": None}),
                      "status": "stale" if cached and cached.value.get("events") else "unavailable"}
            await set_cache_value(NAMESPACE, ticker, result, ttl_seconds=RETRY_TTL)
            return result
        await set_cache_value(NAMESPACE, ticker, result, ttl_seconds=TTL)
        return result


async def get_histories(codes: list[str]) -> dict[str, dict]:
    """전체 첫 조회도 15초로 제한. 실패한 종목은 기존 캐시와 상태를 반환한다."""
    ticker_map = await load_ticker_map()
    tickers = {}
    for code in codes:
        static = static_foreign_ticker(code) or {}
        ticker = ticker_map.get(code) or static.get("ticker") or (code + ".KS" if is_korean_stock(code) else code)
        for suffix in (".OQ", ".O", ".K", ".PK"):
            if ticker.endswith(suffix):
                ticker = ticker[:-len(suffix)]
                break
        tickers[code] = ticker
    semaphore = asyncio.Semaphore(6)
    tasks = {ticker: asyncio.create_task(_history(ticker, semaphore)) for ticker in set(tickers.values())}
    if not tasks:
        return {}
    try:
        await asyncio.wait(tasks.values(), timeout=BATCH_TIMEOUT)
    finally:
        for task in tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
    results = {}
    for code, ticker in tickers.items():
        task = tasks[ticker]
        if task.cancelled():
            cached = await get_cache_value_entry(NAMESPACE, ticker, allow_stale=True)
            results[code] = {**(cached.value if cached else {"events": [], "fetched_at": None}), "status": "stale" if cached else "unavailable"}
        else:
            results[code] = task.result()
    return results
