"""시장 지표 장애를 화면 방문과 무관하게 감지하고 복구까지 추적한다."""

import asyncio
import logging
import sqlite3
from datetime import datetime, timezone

import httpx

import observability
from repositories import cache_values, users
from services.market import naver_indicators
from services.notifications import channels

logger = logging.getLogger(__name__)
NAMESPACE = "market_indicator_health"
KEY = "status"
CODES = sorted(naver_indicators.CODES | {"CMDT_GC", "OIL_CL"})
ALERT_AFTER_SECONDS = 600


def _time(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def problem(row: dict, now: datetime) -> str | None:
    if not row.get("value") or row.get("_stale"):
        return "수집 실패 · 이전 값" if row.get("value") else "시세 누락"
    if row.get("_degraded"):
        return "주 공급원 실패 · 대체 시세 사용"
    # 거래 중이라고 명시한 시세만 검사한다. 휴장·장 마감 시세를 오류로 세지 않는다.
    if row.get("market_status") == "OPEN":
        stamp = _time(row.get("as_of"))
        delay = float(row.get("delay_minutes") or 0)
        if stamp is None or (now - stamp).total_seconds() > (delay + 30) * 60:
            return "거래 중 시세 기준 시각 지연"
    return None


async def _notify(text: str, key: str) -> int:
    sent = 0
    for user in await users.get_all_users():
        if user.get("is_admin"):
            sent += await channels.dispatch(user["google_sub"], text, dedupe_key=key)
    return sent


async def check_once(*, now: datetime | None = None) -> dict:
    import market_indicators

    now = now or datetime.now(timezone.utc)
    previous_entry = await cache_values.get_cache_value_entry(NAMESPACE, KEY, allow_stale=True)
    previous = previous_entry.value if previous_entry else {}
    try:
        quotes = await asyncio.wait_for(market_indicators.fetch_indicators(CODES), timeout=50)
    except (httpx.HTTPError, TimeoutError):
        quotes = {}
    issues = {}
    for code in CODES:
        row = quotes.get(code) or {}
        reason = problem(row, now)
        if reason:
            first = previous.get("issues", {}).get(code, {}).get("since") or now.isoformat()
            issues[code] = {"reason": reason, "since": first, "as_of": row.get("as_of"), "source": row.get("source")}
    state = {"checked_at": now.isoformat(), "issues": issues, "notified": previous.get("notified", False),
             "incident": previous.get("incident") or now.isoformat()}
    status = "warn" if issues else "ok"
    overdue = [code for code, issue in issues.items() if (now - (_time(issue["since"]) or now)).total_seconds() >= ALERT_AFTER_SECONDS]
    if overdue:
        status = "error"
        if not state["notified"]:
            labels = ", ".join(market_indicators.CATALOG[c]["label"] for c in overdue)
            sent = await _notify(f"시장 지표 갱신 장애가 10분 이상 지속됩니다: {labels}. 자동 재시도 중이며 사이드바의 기준 시각·지연 표시를 확인해 주세요.", f"market-indicators:failure:{state['incident']}")
            state["notified"] = sent > 0
    elif not issues:
        if state["notified"]:
            sent = await _notify("시장 지표 수집이 정상 복구되었습니다.", f"market-indicators:recovered:{state['incident']}")
            state["notified"] = not bool(sent)
        if not state["notified"]:
            state["incident"] = None
    state["status"] = status
    if set(issues) != set(previous.get("issues", {})) or status != previous.get("status"):
        await observability.record_event("market_indicators", "health_changed", level="error" if status == "error" else "warning" if issues else "info", details=state, wait=True)
    await cache_values.set_cache_value(NAMESPACE, KEY, state)
    return state


async def summary(*, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    entry = await cache_values.get_cache_value_entry(NAMESPACE, KEY, allow_stale=True)
    state = entry.value if entry else {}
    checked = _time(state.get("checked_at"))
    if not checked or (now - checked).total_seconds() > 300:
        return {"check": "market_indicators", "status": "warn", "detail": "시장 지표 감시 결과 없음 또는 5분 이상 미갱신", "value": None}
    return {"check": "market_indicators", "status": state["status"],
            "detail": "정상" if not state["issues"] else "; ".join(f"{c}: {v['reason']}" for c, v in state["issues"].items()), "value": len(state["issues"])}


async def run_loop(stop: asyncio.Event):
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=60)
        except TimeoutError:
            try:
                await check_once()
            except (sqlite3.Error, httpx.HTTPError, OSError, ValueError, TypeError, TimeoutError):
                logger.exception("시장 지표 감시 실패")
                await observability.record_event("market_indicators", "monitor_failed", level="error")
