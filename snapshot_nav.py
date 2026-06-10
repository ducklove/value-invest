"""Daily portfolio snapshot + NAV calculation. Run via cron at 22:00 KST."""

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone

import cache  # init_db/close_db(스키마·연결 수명)는 아직 cache 소유
import close_price_client
import kis_proxy_client
from repositories import db as db_repo
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from repositories import user_settings as user_settings_repo
from services.portfolio import runtime_quotes as portfolio_quotes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_NAV = 1000.0
KST = timezone(timedelta(hours=9))


class SnapshotIncomplete(RuntimeError):
    """Raised when NAV cannot be valued without using a non-market fallback."""


def _today_kst() -> date:
    return datetime.now(KST).date()


def _safe_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _item_date(item: dict) -> str:
    text = str(
        item.get("date")
        or item.get("trade_date")
        or item.get("business_date")
        or item.get("stck_bsop_date")
        or ""
    ).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10].replace(".", "-")


def _item_close(item: dict) -> float | None:
    return _safe_float(item.get("close") or item.get("close_price") or item.get("stck_clpr"))


async def _fetch_historical_korean_quote(stock_code: str, snap_date: str) -> dict:
    target = date.fromisoformat(snap_date)
    try:
        rows = await close_price_client.get_daily_prices(
            stock_code,
            since=target,
            until=target,
            fields=("close", "trading_value"),
        )
        for row in rows:
            if row.get("date") == snap_date and row.get("close") is not None:
                return {
                    "date": snap_date,
                    "price": row["close"],
                    "change": 0,
                    "change_pct": None,
                    "trade_value": row.get("trading_value"),
                }
    except Exception as exc:
        logger.warning("Historical close API failed for %s %s: %s", stock_code, snap_date, exc)

    try:
        payload = await kis_proxy_client.get_history(
            stock_code,
            start_date=target,
            end_date=target,
            period="D",
            adjusted=True,
        )
        for item in payload.get("items") or []:
            if _item_date(item) == snap_date:
                close = _item_close(item)
                if close is not None:
                    return {
                        "date": snap_date,
                        "price": close,
                        "change": 0,
                        "change_pct": None,
                    }
    except Exception as exc:
        logger.warning("KIS historical close failed for %s %s: %s", stock_code, snap_date, exc)

    return {}


async def _fetch_total_value(google_sub: str, snap_date: str) -> tuple[float, float, list[dict]]:
    """Return (total_market_value, total_invested, per_stock_values) for a user's portfolio."""
    items = await portfolio_repo.get_portfolio(google_sub)
    # Load previous per-stock snapshot for fallback (avoids avg_price distortion)
    prev_stock_map = {}
    prev_stocks = await snapshots_repo.get_stock_snapshots_before_date(google_sub, snap_date)
    for ps in prev_stocks:
        prev_stock_map[ps["stock_code"]] = ps["market_value"]

    total_value = 0.0
    total_invested = 0.0
    per_stock = []
    missing: list[str] = []
    for item in items:
        qty = item["quantity"]
        avg_price = item["avg_price"]
        total_invested += qty * avg_price
        try:
            if portfolio_quotes.is_korean_stock(item["stock_code"]):
                if date.fromisoformat(snap_date) < _today_kst():
                    quote = await _fetch_historical_korean_quote(item["stock_code"], snap_date)
                else:
                    quote = await portfolio_quotes.fetch_quote(
                        item["stock_code"],
                        force_refresh=True,
                        use_ws_cache=False,
                    )
            else:
                quote = await portfolio_quotes.fetch_quote(item["stock_code"])
            price = None if not quote or quote.get("_stale") is True else _safe_float(quote.get("price"))
            if price is not None:
                mv = qty * price
            elif item["stock_code"] in prev_stock_map:
                mv = prev_stock_map[item["stock_code"]]
                logger.warning("Quote unavailable for %s, using previous stock snapshot %.0f", item["stock_code"], mv)
            else:
                missing.append(item["stock_code"])
                continue
        except Exception as e:
            logger.warning("Quote fetch failed for %s: %s", item["stock_code"], e)
            if item["stock_code"] in prev_stock_map:
                mv = prev_stock_map[item["stock_code"]]
            else:
                missing.append(item["stock_code"])
                continue
        total_value += mv
        per_stock.append({
            "stock_code": item["stock_code"],
            "market_value": mv,
            "group_name": item.get("group_name"),
        })
        await asyncio.sleep(0.25)  # rate limit
    if missing:
        raise SnapshotIncomplete(
            "missing daily quotes without stock snapshot fallback: " + ", ".join(missing[:8])
        )
    return total_value, total_invested, per_stock


async def take_snapshot(google_sub: str, snap_date: str):
    """Take a daily snapshot and compute NAV for one user."""
    total_value, total_invested, per_stock = await _fetch_total_value(google_sub, snap_date)
    if total_value == 0:
        logger.info("Skipping %s: portfolio value is 0", google_sub)
        return

    existing = await snapshots_repo.get_snapshot_by_date(google_sub, snap_date)
    prev = existing or await snapshots_repo.get_latest_snapshot_before_date(google_sub, snap_date)

    if prev is None:
        # First snapshot ever
        nav = BASE_NAV
        total_units = total_value / BASE_NAV
    else:
        nav = prev["nav"]
        total_units = prev["total_units"]

    # On rerun, preserve the already-materialized units for that date. If this
    # is the first snapshot for snap_date, apply same-day cashflows once.
    if existing is None:
        cashflows = await snapshots_repo.get_pending_cashflows(google_sub, snap_date)
        for cf in cashflows:
            if cf["units_change"] is not None:
                # Already applied (e.g., imported data or a previous run).
                total_units += cf["units_change"]
                continue
            amt = cf["amount"]
            if nav > 0:
                units_delta = amt / nav
                if cf["type"] == "withdrawal":
                    units_delta = -units_delta
                total_units += units_delta
                # Update cashflow record with nav and units
                db = await db_repo.get_db()
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

    await snapshots_repo.save_snapshot(google_sub, snap_date, total_value, total_invested, nav, total_units, _fx_usdkrw)
    if per_stock:
        await snapshots_repo.save_stock_snapshots(google_sub, snap_date, per_stock)
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
                    await user_settings_repo.set_user_setting("__system__", "gold_prev_close", str(price))
                    logger.info("Gold prev close saved: %.2f", price)
    except Exception as e:
        logger.warning("Failed to save gold close: %s", e)


async def _update_benchmark_history():
    """Append yesterday/today closes to benchmark_daily for the NAV chart
    overlays (KOSPI / SP500 / GOLD). Best-effort — a yfinance outage just
    delays one row; must not block the snapshot itself."""
    try:
        import benchmark_history
        written = await benchmark_history.update_benchmark_today()
        logger.info("Benchmark history increment: %s", written)
    except Exception as e:
        logger.warning("Benchmark history increment failed: %s", e)


_fx_usdkrw: float | None = None


async def _fetch_fx_usdkrw():
    """Fetch current USD/KRW rate."""
    global _fx_usdkrw
    try:
        q = await portfolio_quotes.fetch_cash_quote("CASH_USD")
        if q and q.get("price"):
            _fx_usdkrw = q["price"]
            logger.info("FX USD/KRW: %.2f", _fx_usdkrw)
    except Exception as e:
        logger.warning("Failed to fetch FX rate: %s", e)


async def run_all_snapshots(snap_date: str | None = None, manage_db: bool = True):
    """Take snapshots for all users with portfolio items.

    When invoked inside the web process (via /api/internal/snapshot/nav)
    the caller already owns cache's DB lifecycle — pass manage_db=False
    so we don't close the shared aiosqlite connection out from under it.
    """
    if manage_db:
        await cache.init_db()
    if snap_date is None:
        snap_date = _today_kst().isoformat()
    if date.fromisoformat(snap_date).weekday() >= 5:
        logger.info("Snapshot skipped: %s is a weekend", snap_date)
        import observability
        await observability.record_event(
            "snapshot_nav", "skipped_weekend",
            level="info", details={"date": snap_date}, wait=True,
        )
        if manage_db:
            await cache.close_db()
        return
    await _fetch_fx_usdkrw()
    users = await snapshots_repo.get_all_users_with_portfolio()
    logger.info("Taking snapshots for %d users on %s", len(users), snap_date)
    success_count = 0
    failed_users: list[str] = []
    for google_sub in users:
        try:
            await take_snapshot(google_sub, snap_date)
            success_count += 1
        except Exception as e:
            logger.error("Snapshot failed for %s: %s", google_sub[:8], e)
            failed_users.append(google_sub[:8])
    await _save_gold_close()
    await _update_benchmark_history()
    # Record tick outcome for the dashboard. `wait=True` because this is
    # a batch script that's about to close the DB handle — can't detach.
    import observability
    await observability.record_event(
        "snapshot_nav",
        "tick_ok" if not failed_users else "tick_partial",
        level="info" if not failed_users else "warning",
        details={
            "date": snap_date,
            "users_total": len(users),
            "users_ok": success_count,
            "users_failed": failed_users,
            "fx_usdkrw": _fx_usdkrw,
        },
        wait=True,
    )
    if manage_db:
        await cache.close_db()


if __name__ == "__main__":
    import sys
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(run_all_snapshots(target_date))
