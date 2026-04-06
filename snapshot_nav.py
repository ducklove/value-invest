"""Daily portfolio snapshot + NAV calculation. Run via cron at 22:00 KST."""

import asyncio
import logging
from datetime import date

import cache

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_NAV = 1000.0


async def _fetch_total_value(google_sub: str) -> tuple[float, float, list[dict]]:
    """Return (total_market_value, total_invested, per_stock_values) for a user's portfolio."""
    from routes.portfolio import _fetch_quote

    items = await cache.get_portfolio(google_sub)
    total_value = 0.0
    total_invested = 0.0
    per_stock = []
    for item in items:
        qty = item["quantity"]
        avg_price = item["avg_price"]
        total_invested += qty * avg_price
        try:
            quote = await _fetch_quote(item["stock_code"])
            price = quote.get("price") if quote else None
            if price is not None:
                mv = qty * price
            else:
                mv = qty * avg_price  # fallback to cost basis
        except Exception as e:
            logger.warning("Quote fetch failed for %s: %s", item["stock_code"], e)
            mv = qty * avg_price
        total_value += mv
        per_stock.append({"stock_code": item["stock_code"], "market_value": mv})
        await asyncio.sleep(0.25)  # rate limit
    return total_value, total_invested, per_stock


async def take_snapshot(google_sub: str, snap_date: str):
    """Take a daily snapshot and compute NAV for one user."""
    total_value, total_invested, per_stock = await _fetch_total_value(google_sub)
    if total_value == 0:
        logger.info("Skipping %s: portfolio value is 0", google_sub)
        return

    prev = await cache.get_latest_snapshot(google_sub)

    if prev is None:
        # First snapshot ever
        nav = BASE_NAV
        total_units = total_value / BASE_NAV
    else:
        nav = prev["nav"]
        total_units = prev["total_units"]

    # Apply cashflows for this date: adjust units before computing NAV
    cashflows = await cache.get_pending_cashflows(google_sub, snap_date)
    for cf in cashflows:
        if cf["units_change"] is not None:
            # Already applied (e.g., re-run)
            continue
        amt = cf["amount"]
        if nav > 0:
            units_delta = amt / nav
            if cf["type"] == "withdrawal":
                units_delta = -units_delta
            total_units += units_delta
            # Update cashflow record with nav and units
            db = await cache.get_db()
            await db.execute(
                "UPDATE portfolio_cashflows SET nav_at_time = ?, units_change = ? WHERE id = ?",
                (nav, units_delta, cf["id"]),
            )
            await db.commit()

    # Compute new NAV
    if total_units > 0:
        nav = total_value / total_units
    else:
        nav = BASE_NAV
        total_units = total_value / BASE_NAV if total_value > 0 else 0

    await cache.save_snapshot(google_sub, snap_date, total_value, total_invested, nav, total_units, _fx_usdkrw)
    if per_stock:
        await cache.save_stock_snapshots(google_sub, snap_date, per_stock)
    logger.info("Snapshot saved: %s date=%s value=%.0f nav=%.2f units=%.2f stocks=%d fx=%.1f", google_sub[:8], snap_date, total_value, nav, total_units, len(per_stock), _fx_usdkrw or 0)


async def _save_gold_close():
    """Save current XAU spot price as prev close for tomorrow's market bar."""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.gold-api.com/price/XAU/USD", headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                price = r.json().get("price")
                if price:
                    await cache.set_user_setting("__system__", "gold_prev_close", str(price))
                    logger.info("Gold prev close saved: %.2f", price)
    except Exception as e:
        logger.warning("Failed to save gold close: %s", e)


_fx_usdkrw: float | None = None


async def _fetch_fx_usdkrw():
    """Fetch current USD/KRW rate."""
    global _fx_usdkrw
    try:
        from routes.portfolio import _fetch_cash_quote
        q = await _fetch_cash_quote("CASH_USD")
        if q and q.get("price"):
            _fx_usdkrw = q["price"]
            logger.info("FX USD/KRW: %.2f", _fx_usdkrw)
    except Exception as e:
        logger.warning("Failed to fetch FX rate: %s", e)


async def run_all_snapshots(snap_date: str | None = None):
    """Take snapshots for all users with portfolio items."""
    await cache.init_db()
    if snap_date is None:
        snap_date = date.today().isoformat()
    if date.fromisoformat(snap_date).weekday() >= 5:
        logger.info("Snapshot skipped: %s is a weekend", snap_date)
        await cache.close_db()
        return
    await _fetch_fx_usdkrw()
    users = await cache.get_all_users_with_portfolio()
    logger.info("Taking snapshots for %d users on %s", len(users), snap_date)
    for google_sub in users:
        try:
            await take_snapshot(google_sub, snap_date)
        except Exception as e:
            logger.error("Snapshot failed for %s: %s", google_sub[:8], e)
    await _save_gold_close()
    await cache.close_db()


if __name__ == "__main__":
    import sys
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(run_all_snapshots(target_date))
