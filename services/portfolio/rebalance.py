"""리밸런싱 도우미 — 목표 비중 대비 드리프트 계산 (로드맵 신규 기능 ①).

현재 비중 산정 기준(v1): 가장 최근 일별 종목 스냅샷(portfolio_stock_snapshots)
의 market_value 합계. 이는 그룹 비중 차트(portfolio_group_snapshots)와 동일한
기반 — 현금(CASH_*)을 포함한 전 보유 자산의 KRW 평가액 합을 분모로 쓰는
포트폴리오 UI 의 '비중' 컬럼과 같은 기준이다. 라이브 시세 합산은 보유 종목 수에
비례해 수 초씩 걸리고(레이트 리밋) 알림 엔진에서도 부담이라, v1 은 스냅샷을
쓴다. 따라서 보고서는 마지막 스냅샷 시점(as_of) 기준이며 장중 변동은 다음
스냅샷에 반영된다 — 응답의 ``as_of`` 로 신선도를 드러낸다.

계산(compute_drift_report)은 순수 함수로 분리해 데이터 적재 없이 테스트한다.
"""

from __future__ import annotations

import logging

from repositories import rebalance_targets as targets_repo
from repositories import snapshots as snapshots_repo

logger = logging.getLogger(__name__)

SCOPE_STOCK = "stock"
SCOPE_GROUP = "group"
VALID_SCOPES = (SCOPE_STOCK, SCOPE_GROUP)


def _to_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _current_value(target: dict, holdings: list[dict]) -> float:
    """목표(scope+key)에 해당하는 현재 평가액 — 미보유면 0."""
    key = target["target_key"]
    if target["scope"] == SCOPE_GROUP:
        return sum(
            float(h.get("market_value") or 0.0)
            for h in holdings
            if (h.get("group_name") or "기타") == key
        )
    return sum(
        float(h.get("market_value") or 0.0)
        for h in holdings
        if h.get("stock_code") == key
    )


def _approx_price(target: dict, holdings: list[dict]) -> float | None:
    """종목 목표의 근사 주당가 — 스냅샷 평가액 / 보유 수량 (현금·미보유는 None)."""
    if target["scope"] != SCOPE_STOCK:
        return None
    for h in holdings:
        if h.get("stock_code") != target["target_key"]:
            continue
        mv = _to_float(h.get("market_value"))
        qty = _to_float(h.get("quantity"))
        if mv is None or not qty:
            return None
        price = mv / qty
        return price if price > 0 else None
    return None


def _label(target: dict, holdings: list[dict]) -> str:
    """표시명 — 종목은 보유 행의 이름, 그룹/미보유는 키 그대로."""
    if target["scope"] == SCOPE_STOCK:
        for h in holdings:
            if h.get("stock_code") == target["target_key"]:
                return str(h.get("stock_name") or target["target_key"])
    return str(target["target_key"])


def compute_drift_report(
    targets: list[dict],
    holdings: list[dict],
    *,
    as_of: str | None = None,
) -> dict:
    """목표 비중 대비 드리프트 보고서 (순수 계산).

    targets: rebalance_targets 행 [{scope, target_key, target_weight_pct, tolerance_pct}].
    holdings: 최근 스냅샷 행 [{stock_code, stock_name, group_name, market_value, quantity}].

    항목별로 current_weight_pct / drift_pct(현재−목표) / breached(|drift| >
    tolerance, 경계값은 미돌파) / 조정 금액(action_amount = 총평가액 × |drift|/100,
    초과면 매도·부족이면 매수) / 종목이고 가격이 있으면 근사 주식 수를 낸다.
    총평가액이 0(스냅샷 없음)이면 비중·제안 없이 목표만 돌려준다.
    """
    total_value = sum(float(h.get("market_value") or 0.0) for h in holdings)
    items: list[dict] = []
    breached_count = 0

    for target in targets:
        target_pct = float(target["target_weight_pct"])
        tolerance = float(target.get("tolerance_pct") or targets_repo.DEFAULT_TOLERANCE_PCT)
        item: dict = {
            "scope": target["scope"],
            "key": target["target_key"],
            "label": _label(target, holdings),
            "target_weight_pct": target_pct,
            "tolerance_pct": tolerance,
            "current_weight_pct": None,
            "current_value": None,
            "drift_pct": None,
            "breached": False,
            "action": None,
            "action_amount": None,
            "approx_shares": None,
            "approx_price": None,
        }
        if total_value > 0:
            current_value = _current_value(target, holdings)
            current_pct = current_value / total_value * 100.0
            drift = current_pct - target_pct
            item["current_value"] = round(current_value, 2)
            item["current_weight_pct"] = round(current_pct, 4)
            item["drift_pct"] = round(drift, 4)
            item["breached"] = abs(drift) > tolerance
            # 조정 제안: 총평가액 × 드리프트/100 — 초과(+드리프트)는 매도,
            # 부족(−드리프트)은 매수. 1원 미만은 제안 생략(노이즈).
            amount = total_value * abs(drift) / 100.0
            if amount >= 1.0:
                item["action"] = "매도" if drift > 0 else "매수"
                item["action_amount"] = round(amount)
                price = _approx_price(target, holdings)
                if price:
                    item["approx_price"] = round(price, 4)
                    shares = round(amount / price)
                    item["approx_shares"] = shares if shares > 0 else None
        if item["breached"]:
            breached_count += 1
        items.append(item)

    return {
        "as_of": as_of,
        "total_value": round(total_value, 2),
        "items": items,
        "breached_count": breached_count,
    }


async def compute_rebalance(google_sub: str) -> dict:
    """사용자의 목표 + 최근 스냅샷을 읽어 드리프트 보고서를 만든다."""
    targets = await targets_repo.list_targets(google_sub)
    holdings = await snapshots_repo.get_latest_stock_snapshot_rows(google_sub)
    as_of = holdings[0]["date"] if holdings else None
    return compute_drift_report(targets, holdings, as_of=as_of)
