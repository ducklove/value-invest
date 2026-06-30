"""Daily portfolio action board.

The board turns existing data into a short review queue:

* rebalance breaches from services.portfolio.rebalance
* linked-dashboard signals from external_tools
* user review state from repositories.action_reviews

The generated actions are intentionally stateless; only the review status is
persisted so stale external data cannot leave old action bodies in the DB.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import external_tools
from repositories import action_reviews as reviews_repo
from repositories import portfolio as portfolio_repo
from services.portfolio import rebalance as rebalance_service

KST = timezone(timedelta(hours=9))

SEVERITY_RANK = {"high": 0, "watch": 1, "info": 2}


def _now_kst() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _action(
    key: str,
    category: str,
    severity: str,
    title: str,
    detail: str,
    *,
    source: str,
    stock_code: str | None = None,
    stock_name: str | None = None,
    url: str | None = None,
    metric=None,
    meta: dict | None = None,
) -> dict:
    return {
        "key": key,
        "category": category,
        "severity": severity if severity in SEVERITY_RANK else "info",
        "title": title,
        "detail": detail,
        "source": source,
        "stock_code": stock_code,
        "stock_name": stock_name,
        "url": url,
        "metric": metric,
        "status": "open",
        "review": None,
        "meta": meta or {},
    }


def _rebalance_actions(report: dict) -> list[dict]:
    actions: list[dict] = []
    for item in report.get("items") or []:
        if not item.get("breached"):
            continue
        scope = str(item.get("scope") or "")
        key = str(item.get("key") or "")
        action = item.get("action")
        amount = item.get("action_amount")
        title = f"{item.get('label') or key} 비중 이탈"
        drift = item.get("drift_pct")
        target_pct = item.get("target_weight_pct")
        target_text = f"{float(target_pct):g}%" if target_pct is not None else "목표 비중"
        drift_text = f"{drift:+.1f}%p" if isinstance(drift, (int, float)) else "허용 범위 초과"
        if action and amount:
            detail = f"목표 {target_text} 대비 {drift_text}. {action} 약 {float(amount):,.0f}원 제안."
        else:
            detail = f"목표 {target_text} 대비 {drift_text}."
        actions.append(_action(
            f"rebalance:{scope}:{key}",
            "rebalance",
            "high",
            title,
            detail,
            source="리밸런싱",
            stock_code=key if scope == "stock" else None,
            stock_name=item.get("label") if scope == "stock" else None,
            url="#pfRebalanceWrap",
            metric=drift,
            meta={"scope": scope, **item},
        ))
    return actions


def _signal_actions(holdings: list[dict], signals_by_code: dict[str, list[dict]]) -> list[dict]:
    by_code = {
        str(item.get("stock_code") or "").strip().upper(): item
        for item in holdings
        if item.get("stock_code")
    }
    actions: list[dict] = []
    for code, signals in signals_by_code.items():
        holding = by_code.get(str(code or "").upper(), {})
        stock_name = holding.get("stock_name") or code
        for signal in signals or []:
            kind = str(signal.get("kind") or "external")
            title = str(signal.get("title") or f"{stock_name} 연결 신호")
            detail = str(signal.get("detail") or "연결 프로젝트에서 확인할 신호가 있습니다.")
            actions.append(_action(
                f"signal:{kind}:{code}",
                "linked_signal",
                str(signal.get("severity") or "info"),
                title,
                detail,
                source=str(signal.get("short_label") or "연결"),
                stock_code=code,
                stock_name=stock_name,
                url=signal.get("url"),
                metric=signal.get("metric"),
                meta={"kind": kind, "signal": signal},
            ))
    return actions


def _merge_review_state(actions: list[dict], reviews: dict[str, dict]) -> list[dict]:
    merged: list[dict] = []
    for action in actions:
        review = reviews.get(action["key"])
        if review:
            action = {**action, "status": review.get("status") or "open", "review": review}
        merged.append(action)
    return merged


def _sort_actions(actions: list[dict]) -> list[dict]:
    return sorted(
        actions,
        key=lambda item: (
            0 if item.get("status") == "open" else 1,
            SEVERITY_RANK.get(item.get("severity"), 9),
            item.get("category") != "rebalance",
            str(item.get("stock_name") or item.get("title") or ""),
        ),
    )


async def build_action_board(google_sub: str) -> dict:
    holdings = await portfolio_repo.get_portfolio(google_sub)
    codes = [str(item.get("stock_code") or "").strip().upper() for item in holdings if item.get("stock_code")]

    rebalance_report = await rebalance_service.compute_rebalance(google_sub)
    signals_by_code = await external_tools.fetch_portfolio_signals(codes)

    actions = _rebalance_actions(rebalance_report) + _signal_actions(holdings, signals_by_code)
    reviews = await reviews_repo.list_reviews(google_sub, [item["key"] for item in actions])
    actions = _sort_actions(_merge_review_state(actions, reviews))

    open_actions = [item for item in actions if item.get("status") == "open"]
    linked_signals = [
        {**signal, "stock_code": code}
        for code, signals in signals_by_code.items()
        for signal in (signals or [])
    ]
    return {
        "generated_at": _now_kst(),
        "as_of": rebalance_report.get("as_of"),
        "summary": {
            "action_count": len(actions),
            "open_count": len(open_actions),
            "rebalance_breaches": int(rebalance_report.get("breached_count") or 0),
            "signal_count": len(linked_signals),
            "resolved_count": len(actions) - len(open_actions),
        },
        "actions": actions,
        "queue": open_actions,
        "signals": linked_signals,
    }
