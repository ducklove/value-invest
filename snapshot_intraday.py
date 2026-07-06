"""Intraday portfolio snapshot. Run via systemd timer every 10 minutes (08:00-20:00 KST)."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from repositories import bootstrap
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from services.portfolio import runtime_quotes as portfolio_quotes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


class IntradaySnapshotIncomplete(RuntimeError):
    """Raised when a user portfolio cannot be valued without distorting NAV."""


def _today_kst() -> str:
    return datetime.now(KST).date().isoformat()


def _quote_price(quote: dict | None) -> float | None:
    if not quote or quote.get("_stale") is True:
        return None
    value = quote.get("price")
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def _fetch_total_value(google_sub: str, snap_date: str | None = None) -> float:
    snap_date = snap_date or _today_kst()
    items = await portfolio_repo.get_portfolio(google_sub)
    prev_stock_values = {
        row["stock_code"]: float(row["market_value"])
        for row in await snapshots_repo.get_stock_snapshots_by_date(google_sub, snap_date)
        if row.get("stock_code") and row.get("market_value") is not None
    }
    total = 0.0
    missing: list[str] = []
    fallback_count = 0
    for item in items:
        code = item["stock_code"]
        qty = float(item["quantity"])
        is_korean = portfolio_quotes.is_korean_stock(code)
        try:
            if is_korean:
                quote = await portfolio_quotes.fetch_quote(code, force_refresh=True, use_ws_cache=False)
            else:
                quote = await portfolio_quotes.fetch_quote(code)
            price = _quote_price(quote)
        except Exception as exc:
            logger.warning("Intraday quote fetch failed for %s: %s", code, exc)
            price = None

        if price is not None:
            total += qty * price
        elif code in prev_stock_values and not is_korean:
            total += prev_stock_values[code]
            fallback_count += 1
            logger.warning("Intraday quote unavailable for %s, using latest stock snapshot", code)
        else:
            missing.append(code)
        await asyncio.sleep(0.15)
    if missing:
        raise IntradaySnapshotIncomplete(
            "missing intraday quotes without stock snapshot fallback: " + ", ".join(missing[:8])
        )
    if fallback_count:
        logger.warning("Intraday snapshot used stock-snapshot fallback for %d holdings", fallback_count)
    return total


async def run(manage_db: bool = True):
    if manage_db:
        await bootstrap.init_db()
    await snapshots_repo.delete_old_intraday(days_to_keep=7)
    ts = datetime.now(KST).strftime("%Y-%m-%dT%H:%M")
    users = await snapshots_repo.get_all_users_with_portfolio()
    logger.info("Intraday snapshot for %d users at %s", len(users), ts)
    ok = 0
    failed: list[str] = []
    for google_sub in users:
        try:
            total_value = await _fetch_total_value(google_sub)
            if total_value > 0:
                await snapshots_repo.save_intraday_snapshot(google_sub, ts, total_value)
                logger.info("  %s: %.0f", google_sub[:8], total_value)
                ok += 1
        except Exception as e:
            logger.error("  %s failed: %s", google_sub[:8], e)
            failed.append(google_sub[:8])
    # Dashboard signal — "last intraday tick" with user counts. wait=True
    # because this script closes its own DB handle right after.
    try:
        import observability
        await observability.record_event(
            "snapshot_intraday",
            "tick_ok" if not failed else "tick_partial",
            level="info" if not failed else "warning",
            details={"ts": ts, "users_total": len(users), "users_ok": ok, "users_failed": failed},
            wait=True,
        )
    except Exception:
        pass
    if manage_db:
        await bootstrap.close_db()


if __name__ == "__main__":
    asyncio.run(run())
