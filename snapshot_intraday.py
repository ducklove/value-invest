"""Intraday portfolio snapshot. Run via cron every 30 minutes."""

import asyncio
import logging
from datetime import datetime

import cache

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def _fetch_total_value(google_sub: str) -> float:
    from routes.portfolio import _fetch_quote

    items = await cache.get_portfolio(google_sub)
    total = 0.0
    for item in items:
        qty = item["quantity"]
        avg_price = item["avg_price"]
        try:
            quote = await _fetch_quote(item["stock_code"])
            price = quote.get("price") if quote else None
            total += qty * (price if price is not None else avg_price)
        except Exception:
            total += qty * avg_price
        await asyncio.sleep(0.15)
    return total


async def run(manage_db: bool = True):
    if manage_db:
        await cache.init_db()
    await cache.delete_old_intraday(days_to_keep=7)
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M")
    users = await cache.get_all_users_with_portfolio()
    logger.info("Intraday snapshot for %d users at %s", len(users), ts)
    for google_sub in users:
        try:
            total_value = await _fetch_total_value(google_sub)
            if total_value > 0:
                await cache.save_intraday_snapshot(google_sub, ts, total_value)
                logger.info("  %s: %.0f", google_sub[:8], total_value)
        except Exception as e:
            logger.error("  %s failed: %s", google_sub[:8], e)
    if manage_db:
        await cache.close_db()


if __name__ == "__main__":
    asyncio.run(run())
