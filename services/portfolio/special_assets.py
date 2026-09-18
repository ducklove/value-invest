"""Quote fetchers for special (non-equity) portfolio assets.

KRX gold spot (Naver) and crypto (Upbit). Extracted from
routes/portfolio.py; self-contained and behavior-preserving.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime

import httpx

from core.http import get_http_client

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
    """KRX 1kg 금현물의 원/g 시세. NH 체결 미수신 시 사용하는 보조 경로."""
    try:
        client = await get_http_client("naver")
        resp = await client.get(
            "https://api.stock.naver.com/marketindex/metals/M04020000",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5,
        )
        resp.raise_for_status()
        row = resp.json()
        if not isinstance(row, dict):
            raise ValueError("금 시세 응답 형식 오류")
        exchange = row.get("stockExchangeType")
        if (row.get("reutersCode") != "M04020000" or row.get("unit") != "원/g"
                or not isinstance(exchange, dict) or exchange.get("code") != "KRX"):
            raise ValueError("금 종목·거래소·단위 불일치")
        price = float(str(row["closePrice"]).replace(",", ""))
        change = float(str(row["fluctuations"]).replace(",", ""))
        sign = {"1": 1, "2": 1, "3": 0, "4": -1, "5": -1}[row["fluctuationsType"]["code"]]
        if not math.isfinite(price) or not math.isfinite(change) or (sign == 0 and change != 0):
            raise ValueError("금 시세 숫자 오류")
        change = abs(change) * sign
        previous = price - change
        if price <= 0 or previous <= 0:
            raise ValueError("금 시세는 양수여야 합니다")
        at = datetime.fromisoformat(row["localTradedAt"])
        if at.utcoffset() is None:
            raise ValueError("금 시세 기준시각 누락")
        return {"price": price, "change": change, "change_pct": round(change / previous * 100, 2),
                "source": "naver_json", "market": "KRX", "currency": "KRW", "unit": "원/g",
                "date": at.date().isoformat(), "as_of": at.isoformat()}
    except (httpx.HTTPError, UnicodeError, ValueError, TypeError, KeyError) as e:
        logger.warning("KRX gold quote fetch failed: %s", e)
    return {}


async def fetch_crypto_quote(stock_code: str) -> dict:
    """Fetch a crypto price in KRW from the Upbit API."""
    market = CRYPTO_UPBIT_MAP.get(stock_code)
    if not market:
        return {}
    try:
        client = await get_http_client("upbit")
        resp = await client.get(
            f"https://api.upbit.com/v1/ticker?markets={market}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5,
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
