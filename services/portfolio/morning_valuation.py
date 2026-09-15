"""07:00 KST 평가를 보존한다. 정규 결산/NAV 이력과 독립적인 브리핑 입력이다."""

from __future__ import annotations

import asyncio
import logging
import math

from core.errors import AppError
from repositories import cache_values, portfolio, snapshots, user_settings
from repositories.db import read_snapshot
from services.portfolio import runtime_quotes, time_windows

NAMESPACE = "morning_valuation"
_capture_lock = asyncio.Lock()
logger = logging.getLogger(__name__)


def is_overseas_group(name: str | None) -> bool:
    text = str(name or "").strip().lower()
    return any(token in text for token in ("해외", "미국", "글로벌", "foreign", "overseas", "global")) or text in {
        "us", "usa", "international", "intl",
    }


def _number(value) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _group_performance(rows: list[dict], previous: list[dict], total: float | None) -> list[dict]:
    groups = sorted({r["group_name"] for r in rows + previous if is_overseas_group(r.get("group_name"))})
    result = []
    for name in groups:
        current = [r for r in rows if r.get("group_name") == name]
        before = [r for r in previous if r.get("group_name") == name]
        prev_by_code = {r["stock_code"]: r for r in before}
        missing = [r["stock_name"] for r in current if r["market_value"] is None]
        value = None if missing else sum(r["market_value"] for r in current)
        prev_value = sum(float(r["market_value"]) for r in before) if before else None
        comparable = bool(before) and {r["stock_code"] for r in current} == set(prev_by_code)
        contributions = []
        for row in current:
            prev = prev_by_code.get(row["stock_code"], {})
            quantity = _number(prev.get("quantity"))
            if (row["market_value"] is None or quantity is None
                    or not math.isclose(row["quantity"], quantity, abs_tol=1e-9, rel_tol=0)):
                comparable = False
                continue
            old_value = float(prev["market_value"])
            change = row["market_value"] - old_value
            contributions.append({
                "stock_code": row["stock_code"], "stock_name": row["stock_name"],
                "change_krw": change,
                "change_pct": change / abs(old_value) * 100 if old_value else None,
            })
        change = value - prev_value if comparable and value is not None and prev_value is not None else None
        result.append({
            "group_name": name, "market_value": value, "prev_value": prev_value,
            "change_krw": change,
            "change_pct": change / prev_value * 100 if change is not None and prev_value and prev_value > 0 else None,
            "weight_pct": value / total * 100 if value is not None and total and total > 0 else None,
            "stock_count": len(current), "missing": missing,
            "comparison_unavailable": not comparable,
            "up_count": sum(r["change_krw"] > 0 for r in contributions),
            "down_count": sum(r["change_krw"] < 0 for r in contributions),
            "flat_count": sum(r["change_krw"] == 0 for r in contributions),
            "top": sorted([r for r in contributions if r["change_krw"] > 0], key=lambda r: -r["change_krw"])[:3],
            "bottom": sorted([r for r in contributions if r["change_krw"] < 0], key=lambda r: r["change_krw"])[:3],
        })
    return sorted(result, key=lambda g: abs(g.get("market_value") or 0), reverse=True)


async def load(google_sub: str, day: str) -> dict | None:
    entry = await cache_values.get_cache_value_entry(NAMESPACE, google_sub)
    if entry and entry.value.get("date") == day:
        return entry.value
    return None


async def capture(google_sub: str) -> dict:
    """하루 첫 성공 캡처를 유지한다. 지연 실행을 07:00 값으로 소급하지 않는다."""
    async with _capture_lock:
        now = time_windows.now_kst()
        target = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if now < target:
            raise AppError("07:00 이전에는 모닝 평가를 저장할 수 없습니다.")
        day = now.date().isoformat()
        saved = await load(google_sub, day)
        if saved and saved.get("total_value") is not None and not saved.get("missing"):
            return saved

        # 수량·비교 결산·현금흐름은 같은 DB 읽기 시점에 고정한다.
        async with read_snapshot():
            holdings = await portfolio.get_portfolio(google_sub)
            holdings = [row for row in holdings if float(row["quantity"]) != 0]
            prev = await snapshots.get_latest_snapshot_before_date(google_sub, day)
            previous = await snapshots.get_stock_snapshot_rows_on_or_before(google_sub, prev["date"]) if prev else []
            previous = [r for r in previous if r["date"] == prev["date"]]
            flows = await snapshots.get_cashflows_created_after(
                google_sub, time_windows.settlement_marker_seconds(prev["date"]),
            ) if prev else []

        semaphore = asyncio.Semaphore(4)

        async def quote(code: str) -> dict:
            async with semaphore:
                return await runtime_quotes.fetch_quote(code, force_refresh=True, use_ws_cache=False)

        quotes = await asyncio.gather(*(quote(r["stock_code"]) for r in holdings), return_exceptions=True)
        rows = []
        for holding, fetched in zip(holdings, quotes):
            q = fetched if isinstance(fetched, dict) else {}
            price = _number(q.get("price")) if not q.get("_stale") else None
            if price is not None and price <= 0:
                price = None
            quantity = float(holding["quantity"])
            rows.append({
                "stock_code": holding["stock_code"], "stock_name": holding.get("stock_name") or holding["stock_code"],
                "group_name": holding.get("group_name") or "기타", "quantity": quantity,
                "unit_price": price, "market_value": quantity * price if price is not None else None,
                "quote_as_of": q.get("as_of") or q.get("date"), "fetched_at": q.get("fetched_at"),
            })
        missing = [r["stock_name"] for r in rows if r["market_value"] is None]
        total = sum(r["market_value"] for r in rows) if rows and not missing else None
        cutoff = now.replace(tzinfo=None).isoformat()
        net = sum(float(r["amount"]) * (1 if r["type"] == "deposit" else -1)
                  for r in flows if r["type"] in {"deposit", "withdrawal", "distribution"} and r["created_at"] <= cutoff)
        prev_value = _number((prev or {}).get("total_value"))
        change = total - net - prev_value if total is not None and prev_value else None
        finished = time_windows.now_kst()
        # 정상 캡처도 순차 HTTP 응답의 실제 수집 구간을 함께 보존한다.
        scheduled = now < target.replace(minute=2) and finished < target.replace(minute=10)
        as_of = target if scheduled else now
        result = {
            "date": day, "target_at": target.isoformat(timespec="seconds"),
            "as_of": as_of.isoformat(timespec="seconds"), "captured_at": now.isoformat(timespec="seconds"),
            "completed_at": finished.isoformat(timespec="seconds"), "source": "scheduled" if scheduled else "late",
            "total_value": total, "prev_value": prev_value, "prev_date": (prev or {}).get("date"),
            "change_krw": change, "change_pct": change / prev_value * 100 if change is not None and prev_value > 0 else None,
            "net_cashflow": net, "missing": missing, "holdings": rows,
            "overseas_groups": _group_performance(rows, previous, total),
        }
        await cache_values.set_cache_value(NAMESPACE, google_sub, result)
        return result


async def capture_enabled() -> dict:
    users = await user_settings.get_users_with_setting("daily_briefing_enabled", "true")
    results = await asyncio.gather(*(capture(user) for user in users), return_exceptions=True)
    failed = sum(isinstance(r, BaseException) or bool(r.get("missing")) or r.get("total_value") is None for r in results)
    for user, result in zip(users, results):
        if isinstance(result, BaseException):
            logger.error("morning valuation failed user=%s: %s", user[:8], result)
        elif result.get("missing"):
            logger.warning("morning valuation missing user=%s: %s", user[:8], result["missing"])
    import observability

    await observability.record_event(
        "morning_valuation", "capture_partial" if failed else "capture_ok",
        level="warning" if failed else "info", details={"users": len(users), "failed": failed}, wait=True,
    )
    if failed:
        raise AppError(f"모닝 평가 {failed}명 수집 실패 또는 시세 누락")
    return {"users": len(users), "captured": len(users)}
