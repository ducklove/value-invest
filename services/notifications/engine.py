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
from datetime import datetime

import cache
from repositories import notifications as notifications_repo
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from services.krx_limits import krx_lower_limit, krx_upper_limit
from services.notifications import channels
from services.portfolio import foreign
from services.portfolio import runtime_quotes
from services.portfolio.identifiers import common_stock_code, is_preferred_stock
from services.portfolio.target_resolver import resolve_formula_target
from services.portfolio.time_windows import portfolio_today_baseline_date, settlement_marker_seconds


logger = logging.getLogger(__name__)
_evaluate_all_lock = asyncio.Lock()

PRICE_TYPES = frozenset({"price_above", "price_below"})            # scope=stock
NAV_TYPES = frozenset({"nav_above", "nav_below"})                  # scope=portfolio
PORTFOLIO_DAILY_TYPES = frozenset({"daily_change_above", "daily_change_below"})  # scope=portfolio
TARGET_TYPES = frozenset({"target_reached"})                      # blanket (all holdings)
DAILY_ABS_TYPES = frozenset({"daily_change_abs"})                 # blanket (all holdings, ±n%)
LIMIT_TYPES = frozenset({"limit_reached"})                        # blanket (all holdings, 상/하한가)
# blanket 중 시세가 필요한 것(전 종목 quote)과 외부 feed(공시/리포트)를 구분.
BLANKET_QUOTE_TYPES = TARGET_TYPES | DAILY_ABS_TYPES | LIMIT_TYPES
BLANKET_FEED_TYPES = frozenset({"disclosure_new_all", "report_new_all"})  # 보유 전 종목 신규 공시/리포트
BLANKET_TYPES = BLANKET_QUOTE_TYPES | BLANKET_FEED_TYPES
# 개별 종목(분석 화면) 알림 — 보유 여부 무관, scope='stock'.
STOCK_DAILY_ABS_TYPES = frozenset({"stock_daily_abs"})            # 개별 종목 일간 등락률 ±n%
STOCK_FEED_TYPES = frozenset({"disclosure_new", "report_new"})    # 신규 공시 / 신규 리포트
# 리밸런싱 드리프트 — scope=portfolio, 임계값은 목표별 tolerance(rebalance_targets)
# 가 대신하므로 rule.threshold 는 쓰지 않는다(0). 목표별 엣지 상태는 state_json.
REBALANCE_TYPES = frozenset({"rebalance_drift"})
ALL_ALERT_TYPES = (
    PRICE_TYPES | NAV_TYPES | PORTFOLIO_DAILY_TYPES | BLANKET_TYPES
    | STOCK_DAILY_ABS_TYPES | STOCK_FEED_TYPES | REBALANCE_TYPES
)


def _condition_met(alert_type: str, metric: float, threshold: float) -> bool:
    if alert_type.endswith("_above"):
        return metric >= threshold
    if alert_type.endswith("_below"):
        return metric <= threshold
    return False


def _fmt_num(value: float | None) -> str:
    """가격/금액 표시: 정수 단위는 소숫점 없이 반올림하고 통화 suffix는 붙이지 않는다."""
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


def _daily_abs_dedupe_key(alert_type: str, code: str, threshold: float | None) -> str:
    return f"alert:{alert_type}:{code}:{_fmt_thresh(threshold)}"


def _to_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _quote_change_pct(quote: dict) -> float | None:
    return _to_float((quote or {}).get("change_pct"))


async def _regular_daily_quote(code: str) -> dict:
    """Regular-session daily quote for foreign stock daily-change alerts."""
    try:
        await foreign.ensure_ticker_map()
        ticker = foreign._ticker_map.get(code) or foreign.yfinance_direct_ticker(code)
        quote = await foreign.yfinance_fetch_quote_fast(ticker)
        if not quote:
            resolved = await foreign.resolve_foreign_reuters(code)
            if resolved and resolved != ticker:
                await foreign.save_ticker(code, resolved)
                quote = await foreign.yfinance_fetch_quote_fast(resolved)
        return quote or {}
    except Exception as exc:
        logger.info("regular daily quote failed for %s: %s", code, exc)
        return {}


async def _safe_quote(code: str, *, regular_daily_change: bool = False) -> dict:
    """Fetch a quote dict ({price, change_pct, ...}) or {} on any failure."""
    try:
        if runtime_quotes.is_korean_stock(code):
            quote = await runtime_quotes.fetch_quote(code, force_refresh=True, use_ws_cache=False)
        else:
            quote = await runtime_quotes.fetch_quote(
                code,
                force_refresh=regular_daily_change,
                use_ws_cache=not regular_daily_change,
            )
    except Exception as exc:
        logger.warning("alert quote fetch failed for %s: %s", code, exc)
        return {}
    if not quote or quote.get("_stale") is True:
        quote = {}
    if regular_daily_change and not runtime_quotes.is_korean_stock(code):
        daily_quote = await _regular_daily_quote(code)
        daily_change_pct = _quote_change_pct(daily_quote)
        if daily_change_pct is not None:
            quote = dict(quote or daily_quote)
            quote["change_pct"] = daily_change_pct
    if not quote or quote.get("_stale") is True:
        return {}
    return quote


def _quote_price(quote: dict) -> float | None:
    return _to_float((quote or {}).get("price"))


async def _effective_target(item: dict, common_price: float | None) -> float | None:
    """Resolve the 목표가 the UI shows.

    Order: 수식이면 라이브 데이터로 직접 평가(보유지분·BPS 등) → 저장된 숫자 목표가
    → 우선주 본주가 → 매입가×1.3. 비활성(×)이면 None.
    수식 평가가 핵심: 저장된 target_price 폴백은 stale 하거나(보유지분 수식은) 비어
    있어 화면값(예: 삼성생명 ~926,044)과 크게 달라질 수 있다.
    """
    if item.get("target_price_disabled"):
        return None
    formula = (item.get("target_price_formula") or "").strip()
    if formula:
        resolved = await resolve_formula_target(item["stock_code"], formula, item.get("avg_price"))
        if resolved is not None:
            return resolved
        # 라이브 변수(보유지분 등)를 못 얻으면 저장 폴백/자동값으로.
    saved = _to_float(item.get("target_price"))
    if saved is not None and saved > 0:
        return saved
    code = item.get("stock_code") or ""
    if is_preferred_stock(code):
        return common_price if common_price and common_price > 0 else None
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
    """결산(20:00) 이후 입금(+)·출금(-) 순합. Today 카드와 동일한 보정으로,
    오늘 들어온/나간 현금을 수익률에서 제외한다."""
    if not snap_date:
        return 0.0
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT type, amount FROM portfolio_cashflows"
        " WHERE google_sub = ? AND created_at > ?",
        (google_sub, settlement_marker_seconds(snap_date)),
    )
    net = 0.0
    for row in await cursor.fetchall():
        amount = _to_float(row["amount"]) or 0.0
        if row["type"] == "deposit":
            net += amount
        elif row["type"] == "withdrawal":
            net -= amount
    return net


def _today_str() -> str:
    # last_triggered_at / state_json 의 발송시각도 datetime.now() 로 저장되므로
    # 같은 시계(서버 로컬=KST)로 비교해 TZ 어긋남을 피한다.
    return datetime.now().strftime("%Y-%m-%d")


def _fired_today(last_triggered_at) -> bool:
    """동일 알림이 오늘 이미 발송됐는지 — 하루 1회 상한용.

    조건이 임계값 근처에서 오르내리거나 평가기가 둘 돌아 같은 알림이 하루에
    여러 번 가던 문제를 막는다. 다음 날이 되면 last_triggered_at 이 어제라 다시
    1회 발송 가능.
    """
    return bool(last_triggered_at) and str(last_triggered_at)[:10] == _today_str()


def _note_suffix(rule: dict) -> str:
    note = (rule.get("note") or "").strip()
    return f"\n📝 {note}" if note else ""


def _emphasize(text: str) -> str:
    """중요 알림: 강조 헤더로 감싸 메신저에서 더 눈에 띄게 한다.

    텔레그램·카카오 모두 plain text 로 보내므로(마크다운 미적용) 굵게 대신
    이모지·구분선으로 강조한다. 카카오 200자 제한을 감안해 헤더는 짧게 둔다.
    """
    return f"🚨🚨 중요 알림 🚨🚨\n━━━━━━━━━━\n{text}"


def _format_portfolio_message(rule: dict, metric: float) -> str:
    alert_type = rule["alert_type"]
    threshold = rule["threshold"]
    direction = "이상" if alert_type.endswith("_above") else "이하"
    if alert_type in NAV_TYPES:
        return (
            f"🔔 포트폴리오 총평가액 알림\n"
            f"현재 {_fmt_num(metric)} (기준 {_fmt_num(threshold)} {direction})"
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


def _format_stock_daily_message(rule: dict, name: str, metric: float) -> str:
    return (
        f"🔔 [{name}] 일간 등락률 알림\n"
        f"현재 {_fmt_pct(metric)} (기준 ±{_fmt_thresh(rule['threshold'])}%)"
        f"{_note_suffix(rule)}"
    )


def _format_disclosure_message(rule: dict, name: str, item: dict) -> str:
    rcept = str(item.get("rcept_no") or "").strip()
    url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept}" if rcept else ""
    lines = [f"📑 [{name}] 새 공시", str(item.get("report_nm") or "").strip()]
    if url:
        lines.append(url)
    return "\n".join(p for p in lines if p) + _note_suffix(rule)


def _format_report_message(rule: dict, name: str, item: dict) -> str:
    firm = str(item.get("firm") or "").strip()
    title = str(item.get("title") or "").strip()
    head = " · ".join(p for p in (firm, title) if p)
    lines = [f"📈 [{name}] 새 리포트"]
    if head:
        lines.append(head)
    extras = []
    rec = str(item.get("recommendation") or "").strip()
    tp = str(item.get("target_price") or "").strip()
    if rec:
        extras.append(rec)
    if tp:
        extras.append(f"목표가 {tp}")
    if extras:
        lines.append(" / ".join(extras))
    return "\n".join(lines) + _note_suffix(rule)


def _format_rebalance_message(rule: dict, breached_items: list[dict]) -> str:
    """리밸런싱 알림 본문 — 이탈 항목을 '이름 현재% (목표 n%, ±x.x%p)' 줄로 나열."""
    lines = ["⚖️ 리밸런싱 알림 — 목표 비중 이탈"]
    for item in breached_items:
        label = str(item.get("label") or item.get("key") or "")
        if item.get("scope") == "group":
            label += " 그룹"
        current = item.get("current_weight_pct") or 0.0
        drift = item.get("drift_pct") or 0.0
        lines.append(
            f"{label} {current:.1f}% (목표 {_fmt_thresh(item.get('target_weight_pct'))}%, {drift:+.1f}%p)"
        )
    return "\n".join(lines) + _note_suffix(rule)


async def _eval_rebalance(google_sub: str, rule: dict) -> int:
    """리밸런싱 드리프트 규칙 평가 — 목표별 엣지 트리거.

    services.portfolio.rebalance 의 보고서(최근 일별 스냅샷 기준 — 시세 조회
    없음)를 읽어 |드리프트| > tolerance 인 목표를 찾는다. 목표별 상태는
    blanket 규칙과 같은 형식으로 state_json 에 저장한다:
    ``{"<scope>:<key>": {"armed": bool, "fired": "YYYY-MM-DD"|None}}``.
    발화하면 disarm, 허용 오차 안으로 복귀하면 re-arm, 같은 날은 1회만
    (fired 날짜 상한) — 기존 규칙들의 엣지 시멘틱과 동일. 새로 발화하는
    목표들은 한 메시지로 묶어 보낸다.
    """
    from services.portfolio import rebalance as rebalance_service

    try:
        report = await rebalance_service.compute_rebalance(google_sub)
    except Exception as exc:
        logger.warning("rebalance report failed for %s: %s", google_sub[:8], exc)
        return 0

    try:
        state = json.loads(rule.get("state_json") or "{}")
    except (TypeError, ValueError):
        state = {}
    if not isinstance(state, dict):
        state = {}

    today_str = _today_str()
    to_send: list[dict] = []
    changed = False
    for item in report.get("items", []):
        state_key = f"{item.get('scope')}:{item.get('key')}"
        entry = state.get(state_key)
        if isinstance(entry, dict):
            armed = bool(entry.get("armed", True))
            fired = entry.get("fired")
        else:
            armed = True
            fired = None
        if item.get("breached") and armed and fired != today_str:
            to_send.append(item)
            state[state_key] = {"armed": False, "fired": today_str}
            changed = True
        elif not item.get("breached") and not armed:
            state[state_key] = {"armed": True, "fired": fired}
            changed = True

    sent = 0
    if to_send:
        message = _format_rebalance_message(rule, to_send)
        if rule.get("important"):
            message = _emphasize(message)
        await channels.dispatch(google_sub, message)
        sent = 1
    if changed:
        await notifications_repo.set_portfolio_alert_state_json(rule["id"], json.dumps(state))
    return sent


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
    today_str = _today_str()
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
            target = await _effective_target(item, common_price)
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

        # per-holding 상태: {"armed": bool, "fired": "YYYY-MM-DD"|None}.
        # 레거시 형식(bool)도 수용. fired 가 오늘이면 재발송하지 않음(하루 1회).
        entry = state.get(code)
        if isinstance(entry, dict):
            armed = bool(entry.get("armed", True))
            fired = entry.get("fired")
        else:
            armed = entry if isinstance(entry, bool) else True
            fired = None

        if condition and armed and fired != today_str:
            message = (text or "") + _note_suffix(rule)
            if rule.get("important"):
                message = _emphasize(message)
            dedupe_key = (
                _daily_abs_dedupe_key(alert_type, code, threshold)
                if alert_type in DAILY_ABS_TYPES
                else None
            )
            if dedupe_key:
                await channels.dispatch(google_sub, message, dedupe_key=dedupe_key)
            else:
                await channels.dispatch(google_sub, message)
            state[code] = {"armed": False, "fired": today_str}
            changed = True
            sent += 1
        elif not condition and not armed:
            state[code] = {"armed": True, "fired": fired}
            changed = True

    if changed:
        await notifications_repo.set_portfolio_alert_state_json(rule["id"], json.dumps(state))
    return sent


# --- 신규 공시 / 리포트 (개별 종목 feed 알림) -------------------------------
#
# baseline(마지막으로 관측한 최신 식별자)을 state_json 에 저장한다. 구독 직후
# baseline 이 없으면 현재 최신을 저장만 하고 미발송(과거 항목 폭탄 방지). 이후
# 최신이 baseline 과 달라지면 1회 발송하고 baseline 을 갱신한다. armed/하루1회
# 상한은 쓰지 않는다(갱신 자체가 dedup).
#
# 외부 API 비용은 두 단계로 줄인다: (1) 같은 평가 패스에서 같은 종목을 여러
# 사용자가 구독해도 ``feed_cache`` 로 1회만 조회, (2) 패스 간에는 모듈 전역
# TTL 캐시로 과도한 재조회를 막는다.
_FEED_TTL_SECONDS = 600.0
_disc_cache: dict[str, tuple[float, dict | None]] = {}
_rep_cache: dict[str, tuple[float, dict | None]] = {}


def _is_excluded_disclosure(report_nm: str) -> bool:
    """발행 관련 저신호 공시(증권발행실적보고서·증권신고서·투자설명서 등) 제외."""
    from market_daily import (
        _SECURITIES_LOW_SIGNAL_DISCLOSURE_KEYWORDS,
        _compact_disclosure_text,
        _matches_disclosure_keyword,
    )
    compact = _compact_disclosure_text(report_nm)
    return _matches_disclosure_keyword(compact, _SECURITIES_LOW_SIGNAL_DISCLOSURE_KEYWORDS)


async def _fetch_latest_disclosure(code: str) -> dict | None:
    import dart_client
    try:
        corp_code = await cache.get_corp_code(code)
        if not corp_code:
            return None
        items = await dart_client.fetch_recent_disclosures(corp_code)
    except Exception as exc:
        logger.info("disclosure fetch failed for %s: %s", code, exc)
        return None
    for item in items or []:
        if _is_excluded_disclosure(str(item.get("report_nm") or "")):
            continue
        return item  # 최신순이므로 첫 비저신호 공시가 최신
    return None


async def _fetch_latest_report_sig(code: str) -> dict | None:
    import report_client
    try:
        report = await report_client.fetch_latest_report(code)
    except Exception as exc:
        logger.info("report fetch failed for %s: %s", code, exc)
        return None
    if not report:
        return None
    sig = "|".join(str(report.get(k) or "") for k in ("date", "title", "firm", "pdf_url"))
    return {
        "sig": sig,
        "firm": report.get("firm"),
        "title": report.get("title"),
        "target_price": report.get("target_price"),
        "recommendation": report.get("recommendation"),
    }


async def _cached_feed(kind: str, code: str, feed_cache: dict | None):
    """공시('disc')/리포트('rep') 최신값을 per-pass + 모듈 TTL 캐시로 조회."""
    import time as _time

    pass_key = (kind, code)
    if feed_cache is not None and pass_key in feed_cache:
        return feed_cache[pass_key]
    store = _disc_cache if kind == "disc" else _rep_cache
    now = _time.time()
    hit = store.get(code)
    if hit and now - hit[0] < _FEED_TTL_SECONDS:
        result = hit[1]
    else:
        result = await (_fetch_latest_disclosure(code) if kind == "disc" else _fetch_latest_report_sig(code))
        store[code] = (now, result)
    if feed_cache is not None:
        feed_cache[pass_key] = result
    return result


async def _eval_stock_feed(google_sub: str, rule: dict, name: str, feed_cache: dict | None) -> int:
    code = rule.get("stock_code") or ""
    if not code:
        return 0
    alert_type = rule["alert_type"]
    try:
        state = json.loads(rule.get("state_json") or "{}")
    except (TypeError, ValueError):
        state = {}
    if not isinstance(state, dict):
        state = {}
    baseline = state.get("baseline")

    if alert_type == "disclosure_new":
        latest = await _cached_feed("disc", code, feed_cache)
        if not latest:
            return 0
        ident = str(latest.get("rcept_no") or "")
        text = _format_disclosure_message(rule, name, latest)
    else:  # report_new
        latest = await _cached_feed("rep", code, feed_cache)
        if not latest:
            return 0
        ident = str(latest.get("sig") or "")
        text = _format_report_message(rule, name, latest)

    if not ident:
        return 0
    if baseline is None or ident == baseline:
        # 첫 관측(또는 변화 없음): 기준선만 저장/유지, 발송 안 함.
        if baseline != ident:
            await notifications_repo.set_portfolio_alert_state_json(rule["id"], json.dumps({"baseline": ident}))
        return 0
    if rule.get("important"):
        text = _emphasize(text)
    await channels.dispatch(google_sub, text)
    await notifications_repo.set_portfolio_alert_state_json(rule["id"], json.dumps({"baseline": ident}))
    return 1


async def _eval_blanket_feed(
    google_sub: str, rule: dict, items_by_code: dict, override_codes: set, feed_cache: dict | None
) -> int:
    """보유 전 종목 신규 공시/리포트(blanket). per-holding baseline 을 state_json
    ``{"<code>": "<ident>"}`` 에 저장한다.

    개별(분석 화면) 규칙이 있는 종목(override_codes)은 스킵 — 그 종목은 개별 설정이
    우선한다. 첫 관측은 baseline 만 저장(미발송), 이후 식별자가 바뀌면 1회 발송.
    """
    try:
        state = json.loads(rule.get("state_json") or "{}")
    except (TypeError, ValueError):
        state = {}
    if not isinstance(state, dict):
        state = {}

    kind = "disc" if rule["alert_type"] == "disclosure_new_all" else "rep"
    sent = 0
    changed = False
    for code, item in items_by_code.items():
        if code in override_codes:
            continue  # 개별 설정(켜짐/꺼짐)이 전체에 우선 → blanket 평가 제외
        latest = await _cached_feed(kind, code, feed_cache)
        if not latest:
            continue
        raw = latest.get("rcept_no") if kind == "disc" else latest.get("sig")
        ident = str(raw or "")
        if not ident:
            continue
        baseline = state.get(code)
        if baseline is None:
            state[code] = ident  # 첫 관측: 기준선만, 미발송
            changed = True
            continue
        if ident == baseline:
            continue
        name = item.get("stock_name") or code
        text = _format_disclosure_message(rule, name, latest) if kind == "disc" else _format_report_message(rule, name, latest)
        if rule.get("important"):
            text = _emphasize(text)
        await channels.dispatch(google_sub, text)
        state[code] = ident
        changed = True
        sent += 1

    if changed:
        await notifications_repo.set_portfolio_alert_state_json(rule["id"], json.dumps(state))
    return sent


async def evaluate_user(google_sub: str, *, feed_cache: dict | None = None) -> int:
    """Evaluate all enabled rules for one user. Returns alerts sent."""
    rules = await notifications_repo.list_portfolio_alerts(google_sub, enabled_only=True)
    if not rules:
        return 0
    if not await channels.has_active_channel(google_sub):
        return 0

    items_by_code: dict[str, dict] = {}
    try:
        for item in await portfolio_repo.get_portfolio(google_sub):
            items_by_code[item["stock_code"]] = item
    except Exception:
        pass

    async def _name(code: str | None) -> str:
        item = items_by_code.get(code or "")
        nm = item.get("stock_name") if item else None
        if nm:
            return nm
        try:  # 비보유 종목(분석 화면 알림)은 corp_codes 표에서 이름 보강
            return (await cache.get_corp_name(code)) or code or ""
        except Exception:
            return code or ""

    # --- which quotes do we need? ---
    # price + 개별 종목 일간등락률 규칙의 종목 시세가 필요하다.
    metric_codes = {
        r["stock_code"] for r in rules
        if r["alert_type"] in (PRICE_TYPES | STOCK_DAILY_ABS_TYPES) and r.get("stock_code")
    }
    daily_metric_codes = {
        r["stock_code"] for r in rules
        if r["alert_type"] in STOCK_DAILY_ABS_TYPES and r.get("stock_code")
    }
    has_quote_blanket = any(r["alert_type"] in BLANKET_QUOTE_TYPES for r in rules)
    has_daily_blanket = any(r["alert_type"] in DAILY_ABS_TYPES for r in rules)
    needed: set[str] = set(metric_codes)
    if has_quote_blanket:
        needed |= set(items_by_code)
    if has_daily_blanket:
        daily_metric_codes |= set(items_by_code)
    # 전체 신규 공시/리포트(blanket feed)가 있으면, 개별 규칙이 걸린 종목은 그 종목의
    # 개별 설정(켜짐/꺼짐)이 우선하므로 blanket 평가에서 제외한다(enabled 무관 조회).
    override_disc: set[str] = set()
    override_rep: set[str] = set()
    if any(r["alert_type"] in BLANKET_FEED_TYPES for r in rules):
        for r in await notifications_repo.list_portfolio_alerts(google_sub):
            code = r.get("stock_code")
            if not code:
                continue
            if r.get("alert_type") == "disclosure_new":
                override_disc.add(code)
            elif r.get("alert_type") == "report_new":
                override_rep.add(code)
    # 우선주 자동 목표가는 본주가를 추가로 조회해야 한다.
    if any(r["alert_type"] in TARGET_TYPES for r in rules):
        for code, item in items_by_code.items():
            if item.get("target_price") in (None, "") and not item.get("target_price_disabled") and is_preferred_stock(code):
                needed.add(common_stock_code(code))

    quote_map: dict[str, dict] = {}
    for code in needed:
        if code in daily_metric_codes:
            quote_map[code] = await _safe_quote(code, regular_daily_change=True)
        else:
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

        if alert_type in BLANKET_FEED_TYPES:
            override = override_disc if alert_type == "disclosure_new_all" else override_rep
            sent += await _eval_blanket_feed(google_sub, rule, items_by_code, override, feed_cache)
            continue

        if alert_type in BLANKET_TYPES:  # quote blanket (목표가/일간등락/상하한가)
            sent += await _eval_blanket(google_sub, rule, items_by_code, quote_map)
            continue

        if alert_type in STOCK_FEED_TYPES:
            sent += await _eval_stock_feed(google_sub, rule, await _name(rule.get("stock_code")), feed_cache)
            continue

        if alert_type in REBALANCE_TYPES:  # 리밸런싱 드리프트 (스냅샷 기반, 시세 불필요)
            sent += await _eval_rebalance(google_sub, rule)
            continue

        # single-metric rules (price / 개별 일간등락 / nav / portfolio daily): armed 플래그
        if alert_type in PRICE_TYPES:
            metric = _quote_price(quote_map.get(rule.get("stock_code") or "", {}))
        elif alert_type in STOCK_DAILY_ABS_TYPES:
            metric = _quote_change_pct(quote_map.get(rule.get("stock_code") or "", {}))
        elif alert_type in NAV_TYPES:
            metric = nav
        else:  # portfolio daily change
            metric = pf_daily_pct

        if metric is None:
            continue
        if alert_type in STOCK_DAILY_ABS_TYPES:
            condition = abs(metric) >= rule["threshold"]
        else:
            condition = _condition_met(alert_type, metric, rule["threshold"])
        armed = bool(rule["armed"])
        if condition and armed and not _fired_today(rule.get("last_triggered_at")):
            if alert_type in PRICE_TYPES:
                text = _format_price_message(rule, await _name(rule.get("stock_code")), metric)
            elif alert_type in STOCK_DAILY_ABS_TYPES:
                text = _format_stock_daily_message(rule, await _name(rule.get("stock_code")), metric)
            else:
                text = _format_portfolio_message(rule, metric)
            if rule.get("important"):
                text = _emphasize(text)
            dedupe_key = (
                _daily_abs_dedupe_key(alert_type, rule.get("stock_code") or "", rule["threshold"])
                if alert_type in STOCK_DAILY_ABS_TYPES
                else None
            )
            if dedupe_key:
                await channels.dispatch(google_sub, text, dedupe_key=dedupe_key)
            else:
                await channels.dispatch(google_sub, text)
            await notifications_repo.set_portfolio_alert_state(rule["id"], armed=False, last_value=metric, triggered=True)
            sent += 1
        elif not condition and not armed:
            await notifications_repo.set_portfolio_alert_state(rule["id"], armed=True, last_value=metric, triggered=False)
        else:
            await notifications_repo.set_portfolio_alert_state(rule["id"], armed=armed, last_value=metric, triggered=False)
    return sent


async def evaluate_all() -> dict:
    """One evaluation pass over every user with portfolio holdings or alert rules.

    개별 종목(분석 화면) 알림은 보유 종목이 아닐 수 있으므로, 보유 사용자에 더해
    알림 규칙이 하나라도 있는 사용자도 평가 대상에 포함한다. 같은 공시/리포트
    종목을 여러 사용자가 구독해도 외부 API 를 한 번만 치도록 feed_cache 를 공유한다.
    """
    if _evaluate_all_lock.locked():
        logger.info("alert evaluation skipped: previous pass still running")
        return {"users": 0, "evaluated": 0, "sent": 0, "skipped": "already_running"}
    async with _evaluate_all_lock:
        users = set(await snapshots_repo.get_all_users_with_portfolio())
        try:
            users |= set(await notifications_repo.get_all_users_with_alerts())
        except Exception:
            pass
        feed_cache: dict = {}
        total_sent = 0
        evaluated = 0
        for google_sub in users:
            try:
                total_sent += await evaluate_user(google_sub, feed_cache=feed_cache)
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

    pending = await notifications_repo.list_pending_calendar_subscriptions()
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
                await notifications_repo.mark_calendar_subscription_fired(sub["id"])
                sent += 1
        except Exception as exc:
            logger.warning("calendar alert eval failed for %s: %s", google_sub[:8], exc)

    try:
        await notifications_repo.delete_stale_calendar_subscriptions(stale_cutoff)
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
