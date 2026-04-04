"""Backfill NPS snapshots from 2026-01-02 using current holdings + historical prices.

Assumes current ownership_pct for all dates (approximation).
Uses yfinance for historical close prices.
"""

import asyncio
import logging
from datetime import date, timedelta

import yfinance as yf

import cache
from nps_scraper import fetch_nps_holdings, resolve_stock_codes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_NAV = 1000.0


def _get_trading_days(start: str, end: str) -> list[str]:
    """Get KRX trading days between start and end (inclusive) using yfinance."""
    # Use KOSPI index as proxy for trading calendar
    ticker = yf.Ticker("^KS11")
    hist = ticker.history(start=start, end=end, interval="1d")
    return [d.strftime("%Y-%m-%d") for d in hist.index]


def _fetch_historical_prices(stock_codes: list[str], start: str, end: str) -> dict[str, dict[str, float]]:
    """Fetch historical close prices for all stock codes.
    Returns {stock_code: {date: close_price, ...}, ...}
    """
    result = {}
    total = len(stock_codes)
    for i, code in enumerate(stock_codes):
        if not code:
            continue
        yf_code = code + ".KS"  # KRX stocks
        try:
            ticker = yf.Ticker(yf_code)
            hist = ticker.history(start=start, end=end, interval="1d")
            if hist.empty:
                # Try KOSDAQ suffix
                yf_code = code + ".KQ"
                ticker = yf.Ticker(yf_code)
                hist = ticker.history(start=start, end=end, interval="1d")
            if not hist.empty:
                result[code] = {d.strftime("%Y-%m-%d"): row["Close"] for d, row in hist.iterrows()}
        except Exception as e:
            logger.warning("Failed to fetch history for %s: %s", code, e)
        if (i + 1) % 20 == 0:
            logger.info("Fetched %d/%d stock histories", i + 1, total)
    return result


async def backfill():
    await cache.init_db()

    start_date = "2026-01-02"
    end_date = (date.today() - timedelta(days=1)).isoformat()

    logger.info("Backfilling NPS from %s to %s", start_date, end_date)

    # 1. Get current holdings
    holdings = fetch_nps_holdings()
    holdings = await resolve_stock_codes(holdings)
    valid = [h for h in holdings if h.get("stock_code")]
    logger.info("Holdings: %d total, %d with stock codes", len(holdings), len(valid))

    # 2. Fetch all historical prices at once
    stock_codes = list({h["stock_code"] for h in valid})
    logger.info("Fetching historical prices for %d stocks...", len(stock_codes))
    price_history = _fetch_historical_prices(stock_codes, start_date, end_date)
    logger.info("Got price history for %d stocks", len(price_history))

    # 3. Get trading days
    trading_days = _get_trading_days(start_date, end_date)
    logger.info("Trading days: %d", len(trading_days))

    # 4. Build daily snapshots
    total_units = None
    prev_prices = {}  # stock_code -> previous day's close

    for day_idx, snap_date in enumerate(trading_days):
        day_holdings = []
        total_value = 0.0

        for h in valid:
            code = h["stock_code"]
            prices = price_history.get(code, {})
            price = prices.get(snap_date)
            if price is None:
                # Use previous known price
                price = prev_prices.get(code)
            if price is None:
                continue

            prev_price = prev_prices.get(code)
            change_pct = ((price - prev_price) / prev_price * 100) if prev_price and prev_price > 0 else None

            mv = h["shares"] * price
            total_value += mv
            day_holdings.append({
                "stock_code": code,
                "stock_name": h["name"],
                "shares": h["shares"],
                "ownership_pct": h["ownership_pct"],
                "price": round(price),
                "market_value": round(mv),
                "change_pct": round(change_pct, 2) if change_pct is not None else None,
            })
            prev_prices[code] = price

        if total_value == 0:
            continue

        # NAV calculation
        if total_units is None:
            total_units = total_value / BASE_NAV
            nav = BASE_NAV
        else:
            nav = total_value / total_units

        await cache.save_nps_holdings(snap_date, day_holdings)
        await cache.save_nps_snapshot(snap_date, total_value, nav, len(day_holdings))

        if (day_idx + 1) % 10 == 0:
            logger.info("  Day %d/%d: %s value=%.0f nav=%.2f holdings=%d",
                        day_idx + 1, len(trading_days), snap_date, total_value, nav, len(day_holdings))

    logger.info("Backfill complete: %d trading days", len(trading_days))

    # 5. Generate HTML for the latest day only
    from snapshot_nps import run_nps_snapshot
    logger.info("Generating HTML for latest snapshot...")
    await run_nps_snapshot(trading_days[-1] if trading_days else None)

    await cache.close_db()


if __name__ == "__main__":
    asyncio.run(backfill())
