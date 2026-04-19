"""Fetch trailing-annual dividend per share for overseas tickers via yfinance.

Scope limited to foreign stocks because:
  * Korean stocks are covered by the market_data pipeline (stock_price.py)
  * Preferred stocks have their own curated sheet (preferred_dividends.py)
  * Cash / gold / crypto obviously have no dividend concept

We write the trailingAnnualDividendRate (last 12 months actual payouts in
native currency), convert to KRW using the same FX path the quote
fetchers use, and upsert into `foreign_dividends` with source='yfinance'.
Rows that an admin has manually overridden (source='manual') are
preserved — see cache.upsert_foreign_dividends_auto for the guard clause.

Like the preferred-dividends module, this one has NO background loop —
admin triggers it on demand. yfinance responses for dividend fields are
typically stable over a reporting period, and the stock list is small
(a handful of overseas holdings per user), so manual refresh is fine.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

import cache

logger = logging.getLogger(__name__)


def _is_korean_code(code: str) -> bool:
    """Same rule used in routes/portfolio._is_korean_stock — 6-digit
    numeric code = KOSPI/KOSDAQ. We duplicate it here (rather than
    importing) to avoid pulling routes.portfolio into this module and
    creating an import cycle via its indirect deps."""
    return len(code) == 6 and code[:5].isdigit()


def _is_cash_or_special(code: str) -> bool:
    if code.startswith("CASH_"):
        return True
    return code in {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH"}


async def select_foreign_target_codes() -> list[str]:
    """Codes we should fetch dividends for = every distinct stock_code
    across all users' portfolios that's NOT a Korean common stock,
    preferred stock, cash asset, or crypto/gold. Callers typically
    pass this straight into fetch_many()."""
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT DISTINCT stock_code FROM user_portfolio WHERE stock_code IS NOT NULL"
    )
    codes = [row["stock_code"] for row in await cursor.fetchall()]
    return [
        c for c in codes
        if not _is_korean_code(c) and not _is_cash_or_special(c)
    ]


def _fetch_one_sync(ticker: str) -> dict | None:
    """Blocking yfinance call. Returns {dps_native, currency} or None
    if the ticker doesn't resolve.

    Resolution order for the per-share dividend figure:
      (1) trailingAnnualDividendRate > 0  — 가장 신뢰. 실측 지난 12개월.
      (2) dividendRate                    — forward 추정치.
      (3) yield × regularMarketPrice      — 채권 ETF 등 trailing 이 0 이나
          미제공인 케이스. 83199.HK (CSOP China 5-Year Treasury Bond ETF)
          같은 종목은 trailing=0 인데 yield=3.45% 로 실제 분배금 ~3.6 CNY
          을 정확히 표현. yield × price 가 trailing 실측과 거의 일치한다
          는 걸 샘플로 확인.
      (4) 그래도 없으면 0.0 — '배당 없음' 확정으로 저장.
    """
    import yfinance as yf
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as exc:
        logger.warning("yfinance info failed (%s): %s", ticker, exc)
        return None

    dps: float = 0.0

    def _to_float(v) -> float | None:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    trailing = _to_float(info.get("trailingAnnualDividendRate"))
    if trailing is not None and trailing > 0:
        dps = trailing
    else:
        forward = _to_float(info.get("dividendRate"))
        if forward is not None and forward > 0:
            dps = forward
        else:
            # 채권 ETF 등 ── yield × price 로 역산.
            y = _to_float(info.get("yield"))
            price = (_to_float(info.get("regularMarketPrice"))
                     or _to_float(info.get("currentPrice"))
                     or _to_float(info.get("previousClose")))
            if y is not None and y > 0 and price is not None and price > 0:
                dps = round(y * price, 4)

    currency = (info.get("currency") or "USD").upper()
    return {"dps_native": dps, "currency": currency}


async def _fetch_one(ticker: str) -> dict | None:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_one_sync, ticker)


async def refresh_foreign_dividends(
    stock_codes: list[str] | None = None,
) -> dict:
    """Full pipeline. Caller can pass an explicit list; if None we pull
    the full portfolio-wide target set.

    Returns a stats dict. Never raises — a yfinance outage on one
    ticker doesn't abort the rest, and upstream failures are reported
    rather than thrown.
    """
    started = datetime.now()
    if stock_codes is None:
        try:
            stock_codes = await select_foreign_target_codes()
        except Exception as exc:
            logger.warning("foreign_dividends: target selection failed: %s", exc)
            return {"ok": False, "error": str(exc)[:300], "rows_written": 0,
                    "total_attempted": 0}

    # Resolve each portfolio code to the yfinance ticker (ticker_map
    # handles forms like BRK.B vs BRK-B, suffixed HK tickers, etc).
    # We import routes.portfolio lazily to avoid a startup-time import
    # cycle — foreign_dividends gets imported early from cache / admin,
    # while routes.portfolio pulls in plenty of other modules.
    from routes.portfolio import _ticker_map, _ensure_ticker_map, _fx_to_krw
    await _ensure_ticker_map()

    rows: list[dict] = []
    failures: list[str] = []
    # 현지 통화 → '국적' 코드. _fx_to_krw 가 nation 기반이라 mapping 필요.
    # KIS 데이터에 맞춰 USA/HKG/JPN/CHN/VNM/TWN 정도만 실제 쓰임. 기타는
    # USA 로 fallback 하면 yfinance 의 `currency` 기준으로 일단 USD 환산
    # 되어 의도와 어긋나지만 희귀 케이스 (유럽 주식 등) 는 수동 override
    # 권장.
    _CUR_TO_NATION = {
        "USD": "USA", "HKD": "HKG", "JPY": "JPN", "CNY": "CHN", "CNH": "CHN",
        "VND": "VNM", "TWD": "TWN", "EUR": "EUR", "GBP": "GBR", "AUD": "AUS",
        "CAD": "CAN", "CHF": "CHE",
    }

    for code in stock_codes:
        ticker = _ticker_map.get(code, code)
        try:
            info = await _fetch_one(ticker)
        except Exception as exc:
            logger.warning("foreign_dividends: fetch_one crashed (%s): %s", code, exc)
            failures.append(code)
            continue
        if info is None:
            failures.append(code)
            continue
        dps_native = float(info["dps_native"])
        currency = info["currency"]
        nation = _CUR_TO_NATION.get(currency, "USA")
        try:
            dps_krw = await _fx_to_krw(nation, dps_native)
        except Exception as exc:
            logger.warning("foreign_dividends: fx to KRW failed (%s %s): %s", code, currency, exc)
            failures.append(code)
            continue
        rows.append({
            "stock_code": code,
            "dps_native": dps_native,
            "currency": currency,
            "dps_krw": round(float(dps_krw), 2),
        })

    try:
        written = await cache.upsert_foreign_dividends_auto(rows)
    except Exception as exc:
        logger.exception("foreign_dividends: upsert failed")
        return {"ok": False, "error": f"upsert: {exc}", "rows_written": 0,
                "total_attempted": len(stock_codes)}

    elapsed = (datetime.now() - started).total_seconds()
    logger.info(
        "foreign_dividends: %d/%d rows refreshed in %.2fs (%d failures)",
        written, len(stock_codes), elapsed, len(failures),
    )
    return {
        "ok": True,
        "rows_written": written,
        "total_attempted": len(stock_codes),
        "failures": failures,
        "elapsed_seconds": round(elapsed, 2),
    }


__all__ = [
    "select_foreign_target_codes",
    "refresh_foreign_dividends",
]
