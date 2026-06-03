"""Quote fetchers for special (non-equity) portfolio assets.

KRX gold spot (Naver) and crypto (Upbit). Extracted from
routes/portfolio.py; self-contained (only httpx) and behavior-preserving.
"""

from __future__ import annotations

import logging
import re

import httpx

logger = logging.getLogger(__name__)

# Special-asset code → Upbit market.
CRYPTO_UPBIT_MAP: dict[str, str] = {
    "CRYPTO_BTC": "KRW-BTC",
    "CRYPTO_ETH": "KRW-ETH",
    "CRYPTO_USDT": "KRW-USDT",
}


def is_crypto_asset(stock_code: str) -> bool:
    return stock_code in CRYPTO_UPBIT_MAP


async def fetch_krx_gold_quote() -> dict:
    """Fetch KRX gold spot price from the Naver Finance gold page."""
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


async def fetch_crypto_quote(stock_code: str) -> dict:
    """Fetch a crypto price in KRW from the Upbit API."""
    market = CRYPTO_UPBIT_MAP.get(stock_code)
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
