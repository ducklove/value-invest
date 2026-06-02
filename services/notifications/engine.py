"""Portfolio alert evaluation with edge-triggered de-duplication.

A rule fires once when its condition flips from unmet → met (and the rule is
``armed``). After firing it disarms; it re-arms when the condition is no longer
met. This stops every evaluation tick from re-sending the same alert while a
threshold stays crossed, yet lets it fire again on the next genuine crossing.

Rule shapes:
* per-stock      — ``price_above`` / ``price_below`` (one rule per holding).
* portfolio-wide — ``nav_above`` / ``nav_below`` and ``daily_change_above`` /
  ``daily_change_below`` (whole-portfolio NAV / NAV daily change).
* blanket        — ``target_reached`` (every holding's own 목표가) and
  ``daily_change_abs`` (every holding moving ±n% intraday). One rule covers all
  holdings; per-holding edge state lives in ``state_json`` so each holding fires
  once independently.

Data sources are reused so alerts agree with what the UI shows:
* per-stock price / daily change → ``runtime_quotes.fetch_quote``
* effective 목표가 → ``target_price`` column, else 우선주 본주가, else 매입가×1.3
* portfolio NAV → ``snapshot_intraday._fetch_total_value``
* prev-close NAV → latest ``portfolio_snapshots`` row (the Today baseline)
"""

from __future__ import annotations

import asyncio
import json
import logging

import cache
from services.notifications import channels
from services.portfolio import runtime_quotes
from services.portfolio.identifiers import common_stock_code, is_preferred_stock
from services.portfolio.time_windows import portfolio_today_baseline_date


logger = logging.getLogger(__name__)

PRICE_TYPES = frozenset({"price_above", "price_below"})            # scope=stock
NAV_TYPES = frozenset({"nav_above", "nav_below"})                  # scope=portfolio
PORTFOLIO_DAILY_TYPES = frozenset({"daily_change_above", "daily_change_below"})  # scope=portfolio
TARGET_TYPES = frozenset({"target_reached"})                      # blanket (all holdings)
DAILY_ABS_TYPES = frozenset({"daily_change_abs"})                 # blanket (all holdings, ±n%)
BLANKET_TYPES = TARGET_TYPES | DAILY_ABS_TYPES
ALL_ALERT_TYPES = PRICE_TYPES | NAV_TYPES | PORTFOLIO_DAILY_TYPES | BLANKET_TYPES


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


def _to_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def _safe_quote(code: str) -> dict:
    """Fetch a quote dict ({price, change_pct, ...}) or {} on any failure."""
    try:
        if runtime_quotes.is_korean_stock(code):
            quote = await runtime_quotes.fetch_quote(code, force_refresh=True, use_ws_cache=False)
        else:
            quote = await runtime_quotes.fetch_quote(code)
    except Exception as exc:
        logger.warning("alert quote fetch failed for %s: %s", code, exc)
        return {}
    if not quote or quote.get("_stale") is True:
        return {}
    return quote


def _quote_price(quote: dict) -> float | None:
    return _to_float((quote or {}).get("price"))


def _quote_change_pct(quote: dict) -> float | None:
    return _to_float((quote or {}).get("change_pct"))


def _effective_target(item: dict, common_price: float | None) -> float | None:
    """Resolve the 목표가 the UI would show for the common cases.

    Order: explicit/formula-fallback `target_price` → 우선주 본주가 → 매입가×1.3.
    Returns None when the user disabled the target or no basis is available.
    지주사 NAV-기반 자동 목표가는 서버에서 재현하지 않고 매입가×1.3로 폴백한다.
    """
    if item.get("target_price_disabled"):
        return None
    saved = _to_float(item.get("target_price"))
    if saved is not None and saved > 0:
        return saved
    code = item.get("stock_code") or ""
    if is_preferred_stock(code) and common_price and common_price > 0:
        return common_price
    avg = _to_float(item.get("avg_price"))
    return avg * 1.3 if avg and avg > 0 else None


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
    value = _to_float(value)
    return value if value and value > 0 else None


def _note_suffix(rule: dict) -> str:
    note = (rule.get("note") or "").strip()
    return f"\n📝 {note}" if note else ""


def _format_portfolio_message(rule: dict, metric: float) -> str:
    alert_type = rule["alert_type"]
    threshold = rule["threshold"]
    direction = "이상" if alert_type.endswith("_above") else "이하"
    if alert_type in NAV_TYPES:
        return (
            f"🔔 포트폴리오 총평가액 알림\n"
            f"현재 {_fmt_num(metric)}원 (기준 {_fmt_num(threshold)}원 {direction})"
            f"{_note_suffix(rule)}"
        )
    return (
        f"🔔 포트폴리오 일간 등락률 알림\n"
        f"현재 {_fmt_pct(metric)} (기준 {_fmt_pct(threshold)} {direction})"
        f"{_note_suffix(rule)}"
    )


def _format_price_message(rule: dict, name: str, metric: float) -> str:
    direction = "이상" if rule["alert_type"].endswith("_above") else "이하"
    return (
        f"🔔 [{name}] 지정가 알림\n"
        f"현재가 {_fmt_num(metric)} (지정가 {_fmt_num(rule['threshold'])} {direction})"
        f"{_note_suffix(rule)}"
    )


async def _eval_blanket(google_sub: str, rule: dict, items_by_code: dict, quote_map: dict) -> int:
    """Evaluate a blanket rule across every holding with per-holding edge state."""
    try:
        state = json.loads(rule.get("state_json") or "{}")
    except (TypeError, ValueError):
        state = {}
    if not isinstance(state, dict):
        state = {}

    alert_type = rule["alert_type"]
    threshold = rule["threshold"]
    sent = 0
    changed = False

    for code, item in items_by_code.items():
        quote = quote_map.get(code, {})
        price = None
        target = None
        chg = None
        if alert_type in TARGET_TYPES:
            price = _quote_price(quote)
            if price is None:
                continue
            common_price = _quote_price(quote_map.get(common_stock_code(code), {}))
            target = _effective_target(item, common_price)
            if target is None:
                continue
            condition = price >= target
        else:  # daily_change_abs
            chg = _quote_change_pct(quote)
            if chg is None:
                continue
            condition = abs(chg) >= threshold

        armed = state.get(code, True)
        if condition and armed:
            name = item.get("stock_name") or code
            if alert_type in TARGET_TYPES:
                text = f"🎯 [{name}] 목표가 달성\n현재가 {_fmt_num(price)} (목표가 {_fmt_num(target)})"
            else:
                text = f"🔔 [{name}] 일간 등락률 알림\n현재 {_fmt_pct(chg)} (기준 ±{_fmt_num(threshold)}%)"
            text += _note_suffix(rule)
            await channels.dispatch(google_sub, text)
            state[code] = False
            changed = True
            sent += 1
        elif not condition and armed is False:
            state[code] = True
            changed = True

    if changed:
        await cache.set_portfolio_alert_state_json(rule["id"], json.dumps(state))
    return sent


async def evaluate_user(google_sub: str) -> int:
    """Evaluate all enabled rules for one user. Returns alerts sent."""
    rules = await cache.list_portfolio_alerts(google_sub, enabled_only=True)
    if not rules:
        return 0
    if not await channels.has_active_channel(google_sub):
        return 0

    items_by_code: dict[str, dict] = {}
    try:
        for item in await cache.get_portfolio(google_sub):
            items_by_code[item["stock_code"]] = item
    except Exception:
        pass

    def _name(code: str | None) -> str:
        item = items_by_code.get(code or "")
        return (item.get("stock_name") if item else None) or code or ""

    # --- which quotes do we need? ---
    price_codes = {r["stock_code"] for r in rules if r["alert_type"] in PRICE_TYPES and r.get("stock_code")}
    has_blanket = any(r["alert_type"] in BLANKET_TYPES for r in rules)
    needed: set[str] = set(price_codes)
    if has_blanket:
        needed |= set(items_by_code)
    # 우선주 자동 목표가는 본주가를 추가로 조회해야 한다.
    if any(r["alert_type"] in TARGET_TYPES for r in rules):
        for code, item in items_by_code.items():
            if item.get("target_price") in (None, "") and not item.get("target_price_disabled") and is_preferred_stock(code):
                needed.add(common_stock_code(code))

    quote_map: dict[str, dict] = {}
    for code in needed:
        quote_map[code] = await _safe_quote(code)
        await asyncio.sleep(0.1)

    needs_nav = any(r["alert_type"] in NAV_TYPES for r in rules)
    needs_pf_daily = any(r["alert_type"] in PORTFOLIO_DAILY_TYPES for r in rules)
    nav: float | None = None
    pf_daily_pct: float | None = None
    if needs_nav or needs_pf_daily:
        nav = await _portfolio_nav(google_sub)
    if needs_pf_daily and nav is not None:
        prev = await _prev_close_nav(google_sub)
        if prev:
            pf_daily_pct = (nav - prev) / prev * 100.0

    sent = 0
    for rule in rules:
        alert_type = rule["alert_type"]

        if alert_type in BLANKET_TYPES:
            sent += await _eval_blanket(google_sub, rule, items_by_code, quote_map)
            continue

        # single-metric rules (price / nav / portfolio daily) use the `armed` flag
        if alert_type in PRICE_TYPES:
            metric = _quote_price(quote_map.get(rule.get("stock_code") or "", {}))
        elif alert_type in NAV_TYPES:
            metric = nav
        else:  # portfolio daily change
            metric = pf_daily_pct

        if metric is None:
            continue
        condition = _condition_met(alert_type, metric, rule["threshold"])
        armed = bool(rule["armed"])
        if condition and armed:
            if alert_type in PRICE_TYPES:
                text = _format_price_message(rule, _name(rule.get("stock_code")), metric)
            else:
                text = _format_portfolio_message(rule, metric)
            await channels.dispatch(google_sub, text)
            await cache.set_portfolio_alert_state(rule["id"], armed=False, last_value=metric, triggered=True)
            sent += 1
        elif not condition and not armed:
            await cache.set_portfolio_alert_state(rule["id"], armed=True, last_value=metric, triggered=False)
        else:
            await cache.set_portfolio_alert_state(rule["id"], armed=armed, last_value=metric, triggered=False)
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
    # Credentials are per-user, so we don't gate on any env token — users with
    # a registered channel are picked up by evaluate_user; others are skipped.
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
