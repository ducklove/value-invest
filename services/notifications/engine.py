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
from services.krx_limits import krx_lower_limit, krx_upper_limit
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
LIMIT_TYPES = frozenset({"limit_reached"})                        # blanket (all holdings, 상/하한가)
BLANKET_TYPES = TARGET_TYPES | DAILY_ABS_TYPES | LIMIT_TYPES
ALL_ALERT_TYPES = PRICE_TYPES | NAV_TYPES | PORTFOLIO_DAILY_TYPES | BLANKET_TYPES


def _condition_met(alert_type: str, metric: float, threshold: float) -> bool:
    if alert_type.endswith("_above"):
        return metric >= threshold
    if alert_type.endswith("_below"):
        return metric <= threshold
    return False


def _fmt_num(value: float | None) -> str:
    """원/가격 표시: 한국 주식·금액은 정수 원이므로 소숫점 없이 반올림."""
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{round(number):,}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}%"


def _fmt_thresh(value: float | None) -> str:
    """임계 % 표시: 불필요한 소숫점 0 제거 (5 → '5', 2.5 → '2.5')."""
    if value is None:
        return "-"
    try:
        return f"{float(value):g}"
    except (TypeError, ValueError):
        return str(value)


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


async def _prev_close_value(google_sub: str) -> tuple[float | None, str | None]:
    """전일 결산 '총평가액'과 그 결산 날짜.

    일간 등락률은 오늘 총평가액(_portfolio_nav)과 같은 스케일인 total_value 와
    비교해야 한다. 과거에 per-unit `nav`(기준가, ~천원대)와 비교해 수억 %가 나오던
    버그를 막는다.
    """
    baseline = portfolio_today_baseline_date()
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, total_value, nav FROM portfolio_snapshots"
        " WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (google_sub, baseline),
    )
    row = await cursor.fetchone()
    if not row:
        return None, None
    value = _to_float(row["total_value"])
    if value is None:
        value = _to_float(row["nav"])
    return (value if value and value > 0 else None), row["date"]


async def _net_cashflow_since_settlement(google_sub: str, snap_date: str | None) -> float:
    """결산(22:00) 이후 입금(+)·출금(-) 순합. Today 카드와 동일한 보정으로,
    오늘 들어온/나간 현금을 수익률에서 제외한다."""
    if not snap_date:
        return 0.0
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT type, amount FROM portfolio_cashflows"
        " WHERE google_sub = ? AND created_at > ?",
        (google_sub, f"{snap_date}T22:00:00"),
    )
    net = 0.0
    for row in await cursor.fetchall():
        amount = _to_float(row["amount"]) or 0.0
        if row["type"] == "deposit":
            net += amount
        elif row["type"] == "withdrawal":
            net -= amount
    return net


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
        name = item.get("stock_name") or code
        condition = False
        text = None

        if alert_type in TARGET_TYPES:
            price = _quote_price(quote)
            if price is None:
                continue
            common_price = _quote_price(quote_map.get(common_stock_code(code), {}))
            target = _effective_target(item, common_price)
            if target is None:
                continue
            condition = price >= target
            if condition:
                text = f"🎯 [{name}] 목표가 달성\n현재가 {_fmt_num(price)} (목표가 {_fmt_num(target)})"
        elif alert_type in LIMIT_TYPES:
            # 상/하한가는 국내 주식만. 전일 기준가(previous_close)에 호가단위를
            # 적용한 정확한 상한가/하한가와 현재가를 비교한다(근사 ±30% 아님).
            if not runtime_quotes.is_korean_stock(code):
                continue
            price = _quote_price(quote)
            base = _to_float(quote.get("previous_close"))
            if price is None or base is None:
                continue
            upper = krx_upper_limit(base)
            lower = krx_lower_limit(base)
            if upper is not None and price >= upper:
                condition = True
                text = f"🔼 [{name}] 상한가 도달\n현재가 {_fmt_num(price)} (상한가 {_fmt_num(upper)})"
            elif lower is not None and price <= lower:
                condition = True
                text = f"🔽 [{name}] 하한가 도달\n현재가 {_fmt_num(price)} (하한가 {_fmt_num(lower)})"
        else:  # daily_change_abs
            chg = _quote_change_pct(quote)
            if chg is None:
                continue
            condition = abs(chg) >= threshold
            if condition:
                text = f"🔔 [{name}] 일간 등락률 알림\n현재 {_fmt_pct(chg)} (기준 ±{_fmt_thresh(threshold)}%)"

        armed = state.get(code, True)
        if condition and armed:
            await channels.dispatch(google_sub, (text or "") + _note_suffix(rule))
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
    nav: float | None = None  # 오늘 총평가액 (총평가액 알림 + 일간등락 분자)
    pf_daily_pct: float | None = None
    if needs_nav or needs_pf_daily:
        nav = await _portfolio_nav(google_sub)
    if needs_pf_daily and nav is not None:
        prev_total, prev_date = await _prev_close_value(google_sub)
        if prev_total:
            # 오늘 들어온/나간 현금은 수익률에서 제외 (Today 카드와 동일).
            net_cf = await _net_cashflow_since_settlement(google_sub, prev_date)
            pf_daily_pct = (nav - net_cf - prev_total) / prev_total * 100.0

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


# --- Economic calendar event result alerts ---------------------------------

_CALENDAR_STALE_DAYS = 30


def _calendar_num(value) -> float | None:
    """Extract a comparable number from a calendar value ('3.2%','$24.86B','115K')."""
    cleaned = "".join(c for c in str(value or "") if c.isdigit() or c in ".-")
    if cleaned in ("", "-", ".", "-."):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _calendar_surprise(actual, forecast) -> str:
    a = _calendar_num(actual)
    f = _calendar_num(forecast)
    if a is None or f is None:
        return ""
    if a > f:
        return "📈 예상 상회"
    if a < f:
        return "📉 예상 하회"
    return "= 예상 부합"


def _format_calendar_message(sub: dict, ev: dict) -> str:
    flag = (ev.get("flag") or "").strip()
    cname = (ev.get("country_name") or sub.get("country_name") or "").strip()
    name = (ev.get("event") or sub.get("event") or "").strip()
    actual = (ev.get("actual") or "").strip()
    forecast = (ev.get("forecast") or sub.get("forecast") or "").strip()
    previous = (ev.get("previous") or sub.get("previous") or "").strip()

    head = " ".join(p for p in (flag, cname) if p)
    line2 = f"{head} · {name}" if head else name
    detail = f"실제 {actual or '-'}"
    extras = []
    if forecast:
        extras.append(f"예상 {forecast}")
    if previous:
        extras.append(f"이전 {previous}")
    if extras:
        detail += f" ({', '.join(extras)})"

    lines = ["📅 경제지표 결과 발표", line2, detail]
    surprise = _calendar_surprise(actual, forecast)
    if surprise:
        lines.append(surprise)
    return "\n".join(lines)


async def evaluate_calendar_all() -> dict:
    """Fire 'result released' alerts for subscribed calendar events.

    One shared calendar fetch covers all pending subscriptions: we union their
    countries over the [oldest pending date .. today] window, index the result
    by zeroin ``index_id``, then notify each user whose subscribed event now has
    an ``actual`` value. Edge-triggered via the ``fired`` flag (one send each).
    """
    import economic_calendar
    from collections import defaultdict
    from datetime import date, timedelta

    pending = await cache.list_pending_calendar_subscriptions()
    if not pending:
        return {"subs": 0, "sent": 0}

    today = date.today().isoformat()
    stale_cutoff = (date.today() - timedelta(days=_CALENDAR_STALE_DAYS)).isoformat()
    # A future event has no result yet; only dates up to today can fire.
    candidates = [s for s in pending if (s.get("event_date") or "") <= today]

    events_by_id: dict[str, dict] = {}
    dates = [s["event_date"] for s in candidates if s.get("event_date") and s["event_date"] >= stale_cutoff]
    if dates:
        countries = sorted({s.get("country") for s in candidates if s.get("country")})
        try:
            data = await economic_calendar.fetch_economic_calendar(
                start_date=min(dates),
                end_date=today,
                countries=countries or None,
                importance=["high", "mid", "low"],
            )
            events_by_id = {
                e["index_id"]: e for e in data.get("events", []) if e.get("index_id")
            }
        except Exception as exc:
            logger.warning("calendar alert fetch failed: %s", exc)

    by_user: dict[str, list] = defaultdict(list)
    for sub in candidates:
        by_user[sub["google_sub"]].append(sub)

    sent = 0
    for google_sub, subs in by_user.items():
        try:
            if not await channels.has_active_channel(google_sub):
                continue
            for sub in subs:
                ev = events_by_id.get(sub.get("event_id"))
                if not ev or not (ev.get("actual") or "").strip():
                    continue
                await channels.dispatch(google_sub, _format_calendar_message(sub, ev))
                await cache.mark_calendar_subscription_fired(sub["id"])
                sent += 1
        except Exception as exc:
            logger.warning("calendar alert eval failed for %s: %s", google_sub[:8], exc)

    try:
        await cache.delete_stale_calendar_subscriptions(stale_cutoff)
    except Exception:
        pass
    return {"subs": len(pending), "sent": sent}


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
            cal = await evaluate_calendar_all()
            if cal.get("sent"):
                logger.info("calendar alert loop sent %d notifications", cal["sent"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("calendar alert pass failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass
    logger.info("alert loop stopped")
