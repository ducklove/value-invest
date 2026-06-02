"""Portfolio alert evaluation with edge-triggered de-duplication.

A rule fires once when its condition flips from unmet → met (and the rule is
``armed``). After firing it disarms; it re-arms when the condition is no longer
met. This stops every evaluation tick from re-sending the same alert while a
threshold stays crossed, yet lets it fire again on the next genuine crossing.

Data sources are reused verbatim so alerts agree with what the UI shows:
* per-stock price → ``services.portfolio.runtime_quotes.fetch_quote``
* portfolio NAV   → ``snapshot_intraday._fetch_total_value`` (same intraday sum)
* prev-close NAV  → latest ``portfolio_snapshots`` row (the Today baseline)
"""

from __future__ import annotations

import asyncio
import logging

import cache
from services.notifications import channels
from services.portfolio import runtime_quotes
from services.portfolio.time_windows import portfolio_today_baseline_date


logger = logging.getLogger(__name__)

STOCK_ALERT_TYPES = frozenset({"price_above", "price_below"})
PORTFOLIO_ALERT_TYPES = frozenset(
    {"nav_above", "nav_below", "daily_change_above", "daily_change_below"}
)
ALL_ALERT_TYPES = STOCK_ALERT_TYPES | PORTFOLIO_ALERT_TYPES


def _condition_met(alert_type: str, metric: float, threshold: float) -> bool:
    if alert_type.endswith("_above"):
        return metric >= threshold
    if alert_type.endswith("_below"):
        return metric <= threshold
    return False


def _fmt_num(value: float | None) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(number - round(number)) < 1e-9:
        return f"{round(number):,}"
    return f"{number:,.2f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}%"


async def _safe_quote_price(code: str) -> float | None:
    try:
        is_korean = runtime_quotes.is_korean_stock(code)
        if is_korean:
            quote = await runtime_quotes.fetch_quote(code, force_refresh=True, use_ws_cache=False)
        else:
            quote = await runtime_quotes.fetch_quote(code)
    except Exception as exc:
        logger.warning("alert quote fetch failed for %s: %s", code, exc)
        return None
    if not quote or quote.get("_stale") is True:
        return None
    price = quote.get("price")
    if price in (None, ""):
        return None
    try:
        return float(price)
    except (TypeError, ValueError):
        return None


async def _portfolio_nav(google_sub: str) -> float | None:
    """Canonical intraday NAV — reuse the snapshot summation so the alert NAV
    matches the stored snapshots exactly. Returns None if it can't be valued."""
    import snapshot_intraday
    try:
        total = await snapshot_intraday._fetch_total_value(google_sub)
    except Exception as exc:
        logger.info("alert NAV unavailable for %s: %s", google_sub[:8], exc)
        return None
    return total if total and total > 0 else None


async def _prev_close_nav(google_sub: str) -> float | None:
    baseline = portfolio_today_baseline_date()
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT nav, total_value FROM portfolio_snapshots"
        " WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (google_sub, baseline),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    value = row["nav"] if row["nav"] is not None else row["total_value"]
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _format_message(rule: dict, name: str, metric: float) -> str:
    scope = rule["scope"]
    alert_type = rule["alert_type"]
    threshold = rule["threshold"]
    note = (rule.get("note") or "").strip()
    suffix = f"\n📝 {note}" if note else ""

    if scope == "stock":
        direction = "이상" if alert_type.endswith("_above") else "이하"
        return (
            f"🔔 [{name}] 지정가 알림\n"
            f"현재가 {_fmt_num(metric)} (지정가 {_fmt_num(threshold)} {direction})"
            f"{suffix}"
        )
    if alert_type in ("nav_above", "nav_below"):
        direction = "이상" if alert_type.endswith("_above") else "이하"
        return (
            f"🔔 포트폴리오 총평가액 알림\n"
            f"현재 {_fmt_num(metric)}원 (기준 {_fmt_num(threshold)}원 {direction})"
            f"{suffix}"
        )
    # daily change %
    direction = "이상" if alert_type.endswith("_above") else "이하"
    return (
        f"🔔 포트폴리오 일간 등락률 알림\n"
        f"현재 {_fmt_pct(metric)} (기준 {_fmt_pct(threshold)} {direction})"
        f"{suffix}"
    )


async def evaluate_user(google_sub: str) -> int:
    """Evaluate all enabled rules for one user. Returns alerts sent."""
    rules = await cache.list_portfolio_alerts(google_sub, enabled_only=True)
    if not rules:
        return 0
    if not await channels.has_active_channel(google_sub):
        return 0

    # Resolve held-stock names once (for nicer messages).
    name_map: dict[str, str] = {}
    try:
        for item in await cache.get_portfolio(google_sub):
            name_map[item["stock_code"]] = item.get("stock_name") or item["stock_code"]
    except Exception:
        pass

    stock_codes = {
        r["stock_code"] for r in rules if r["scope"] == "stock" and r["stock_code"]
    }
    needs_nav = any(
        r["scope"] == "portfolio" and r["alert_type"] in ("nav_above", "nav_below")
        for r in rules
    )
    needs_daily = any(
        r["scope"] == "portfolio"
        and r["alert_type"] in ("daily_change_above", "daily_change_below")
        for r in rules
    )

    quote_prices: dict[str, float | None] = {}
    for code in stock_codes:
        quote_prices[code] = await _safe_quote_price(code)
        await asyncio.sleep(0.1)

    nav: float | None = None
    daily_change_pct: float | None = None
    if needs_nav or needs_daily:
        nav = await _portfolio_nav(google_sub)
    if needs_daily and nav is not None:
        prev = await _prev_close_nav(google_sub)
        if prev:
            daily_change_pct = (nav - prev) / prev * 100.0

    sent = 0
    for rule in rules:
        scope = rule["scope"]
        alert_type = rule["alert_type"]
        if scope == "stock":
            metric = quote_prices.get(rule["stock_code"])
        elif alert_type in ("nav_above", "nav_below"):
            metric = nav
        else:
            metric = daily_change_pct

        if metric is None:
            continue  # missing data — leave edge state untouched

        condition = _condition_met(alert_type, metric, rule["threshold"])
        armed = bool(rule["armed"])

        if condition and armed:
            name = name_map.get(rule.get("stock_code") or "", rule.get("stock_code") or "")
            text = _format_message(rule, name, metric)
            await channels.dispatch(google_sub, text)
            await cache.set_portfolio_alert_state(
                rule["id"], armed=False, last_value=metric, triggered=True
            )
            sent += 1
        elif not condition and not armed:
            await cache.set_portfolio_alert_state(
                rule["id"], armed=True, last_value=metric, triggered=False
            )
        else:
            await cache.set_portfolio_alert_state(
                rule["id"], armed=armed, last_value=metric, triggered=False
            )
    return sent


async def evaluate_all() -> dict:
    """One evaluation pass over every user that has portfolio holdings."""
    users = await cache.get_all_users_with_portfolio()
    total_sent = 0
    evaluated = 0
    for google_sub in users:
        try:
            total_sent += await evaluate_user(google_sub)
            evaluated += 1
        except Exception as exc:
            logger.warning("alert evaluation failed for %s: %s", google_sub[:8], exc)
    return {"users": len(users), "evaluated": evaluated, "sent": total_sent}


async def run_alert_loop(stop_event: asyncio.Event, *, interval_seconds: float, initial_delay_seconds: float = 30.0) -> None:
    """Periodically evaluate alerts until ``stop_event`` is set."""
    if interval_seconds <= 0:
        logger.info("alert loop disabled (NOTIFY_ALERT_INTERVAL_S<=0)")
        return
    from services.notifications import telegram
    if not telegram.is_configured():
        logger.info("alert loop disabled (no notification channel configured)")
        return
    logger.info("alert loop starting (interval=%.0fs)", interval_seconds)
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=initial_delay_seconds)
        return  # stopped during initial delay
    except asyncio.TimeoutError:
        pass
    while not stop_event.is_set():
        try:
            result = await evaluate_all()
            if result.get("sent"):
                logger.info("alert loop sent %d notifications", result["sent"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("alert loop pass failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass
    logger.info("alert loop stopped")
