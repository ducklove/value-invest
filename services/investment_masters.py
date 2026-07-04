"""투자 대가 전략 카탈로그 + 참고용 자산배분 시뮬레이션.

'도구' 탭의 "투자 대가의 전략" 화면이 쓰는 서비스 계층.

- 전략 데이터는 코드가 아니라 ``data/investment_masters.json`` 에 산다.
  새 대가/전략 추가 = JSON 에 항목 하나 추가 (코드 수정 불필요).
  스키마는 ``_validate_catalog`` 가 로드 시점에 검증하므로, 오타·비중 합
  오류가 배포 전에 (테스트에서) 터진다.
- ``personalize`` 는 사용자의 위험 성향·투자 기간·선호 자산군을 받아
  전략별 예시 배분을 결정론적으로 조정한다. 예측·추천이 아니라
  "대가의 배분을 내 성향으로 옮기면 이런 모양"을 보여주는 교육용 계산.

모든 응답에는 카탈로그의 disclaimer(투자 조언 아님)가 포함된다.
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from core.config import PROJECT_ROOT
from core.errors import AppError

CATALOG_PATH = PROJECT_ROOT / "data" / "investment_masters.json"

# 비중 합 검증 허용 오차 (부동소수 대비)
_WEIGHT_SUM_TOLERANCE = 0.01


class MastersError(AppError):
    """전략 시뮬레이션 입력 오류 — 클라이언트 잘못이므로 400."""

    status_code = 400
    default_detail = "전략 시뮬레이션 요청이 올바르지 않습니다."


class MastersCatalogError(AppError):
    """카탈로그 데이터 자체의 결함 — 배포 데이터 문제이므로 500."""

    status_code = 500
    default_detail = "전략 카탈로그 데이터에 문제가 있습니다."


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise MastersCatalogError(message)


def _validate_catalog(raw: dict[str, Any]) -> dict[str, Any]:
    """카탈로그 스키마 검증. 실패 메시지는 어떤 항목이 문제인지 특정한다."""
    _require(isinstance(raw.get("disclaimer"), str) and raw["disclaimer"].strip() != "", "disclaimer 가 비어 있습니다")
    groups = raw.get("asset_groups")
    _require(isinstance(groups, dict) and len(groups) > 0, "asset_groups 가 비어 있습니다")
    assets = raw.get("asset_classes")
    _require(isinstance(assets, dict) and len(assets) > 0, "asset_classes 가 비어 있습니다")
    for asset_id, spec in assets.items():
        _require(isinstance(spec.get("label"), str), f"asset_classes.{asset_id}.label 누락")
        _require(spec.get("group") in groups, f"asset_classes.{asset_id}.group '{spec.get('group')}' 미정의")

    options = raw.get("profile_options", {})
    risk_ids = {o["id"] for o in options.get("risk", [])}
    horizon_ids = {o["id"] for o in options.get("horizon", [])}
    _require(len(risk_ids) > 0, "profile_options.risk 가 비어 있습니다")
    _require(len(horizon_ids) > 0, "profile_options.horizon 가 비어 있습니다")

    pers = raw.get("personalization", {})
    for key in ("risk_equity_shift_pt", "horizon_equity_shift_pt"):
        _require(isinstance(pers.get(key), dict), f"personalization.{key} 누락")
    _require(set(pers["risk_equity_shift_pt"]) == risk_ids, "risk_equity_shift_pt 키가 risk 옵션과 다릅니다")
    _require(set(pers["horizon_equity_shift_pt"]) == horizon_ids, "horizon_equity_shift_pt 키가 horizon 옵션과 다릅니다")

    strategies = raw.get("strategies")
    _require(isinstance(strategies, list) and len(strategies) > 0, "strategies 가 비어 있습니다")
    seen_ids: set[str] = set()
    for s in strategies:
        sid = s.get("id")
        _require(isinstance(sid, str) and sid != "", "strategy.id 누락")
        _require(sid not in seen_ids, f"strategy id 중복: {sid}")
        seen_ids.add(sid)
        for field in ("master", "title", "tagline", "summary", "allocation_basis"):
            _require(isinstance(s.get(field), str) and s[field].strip() != "", f"{sid}.{field} 누락")
        for field in ("principles", "pros", "cons", "references"):
            _require(isinstance(s.get(field), list) and len(s[field]) > 0, f"{sid}.{field} 누락")
        fit = s.get("fit", {})
        _require(set(fit.get("risk", [])) <= risk_ids and len(fit.get("risk", [])) > 0, f"{sid}.fit.risk 값 오류")
        _require(set(fit.get("horizon", [])) <= horizon_ids and len(fit.get("horizon", [])) > 0, f"{sid}.fit.horizon 값 오류")
        for field in ("risk_level", "effort_level"):
            _require(isinstance(s.get(field), int) and 1 <= s[field] <= 5, f"{sid}.{field} 는 1~5 정수여야 합니다")
        reb = s.get("rebalancing", {})
        _require(isinstance(reb.get("frequency"), str), f"{sid}.rebalancing.frequency 누락")
        _require(isinstance(reb.get("ideas"), list) and len(reb["ideas"]) > 0, f"{sid}.rebalancing.ideas 누락")

        allocation = s.get("base_allocation")
        _require(isinstance(allocation, list) and len(allocation) > 0, f"{sid}.base_allocation 누락")
        total = 0.0
        alloc_assets: set[str] = set()
        for row in allocation:
            asset = row.get("asset")
            _require(asset in assets, f"{sid} 배분의 자산 '{asset}' 이 asset_classes 에 없습니다")
            _require(asset not in alloc_assets, f"{sid} 배분에 자산 '{asset}' 중복")
            alloc_assets.add(asset)
            weight = row.get("weight")
            _require(isinstance(weight, (int, float)) and weight > 0, f"{sid}.{asset}.weight 는 양수여야 합니다")
            total += float(weight)
        _require(abs(total - 100.0) < _WEIGHT_SUM_TOLERANCE, f"{sid} 배분 합계가 100이 아닙니다: {total}")
    return raw


@lru_cache(maxsize=1)
def load_catalog() -> dict[str, Any]:
    raw = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    return _validate_catalog(raw)


def get_catalog_payload() -> dict[str, Any]:
    """GET /api/masters/strategies 응답 — 카탈로그 전체 (읽기 전용 사본)."""
    catalog = load_catalog()
    return {
        "version": catalog.get("version", 1),
        "disclaimer": catalog["disclaimer"],
        "asset_groups": catalog["asset_groups"],
        "asset_classes": catalog["asset_classes"],
        "profile_options": catalog["profile_options"],
        "strategies": catalog["strategies"],
    }


def _asset_group(catalog: dict[str, Any], asset: str) -> str:
    return catalog["asset_classes"][asset]["group"]


def _parse_profile(profile: dict[str, Any], catalog: dict[str, Any]) -> tuple[str, str, set[str]]:
    if not isinstance(profile, dict):
        raise MastersError("profile 객체가 필요합니다.")
    options = catalog["profile_options"]
    risk = profile.get("risk")
    if risk not in {o["id"] for o in options["risk"]}:
        raise MastersError(f"risk 값이 올바르지 않습니다: {risk!r}")
    horizon = profile.get("horizon")
    if horizon not in {o["id"] for o in options["horizon"]}:
        raise MastersError(f"horizon 값이 올바르지 않습니다: {horizon!r}")

    all_groups = set(catalog["asset_groups"])
    raw_groups = profile.get("asset_groups")
    if raw_groups is None:
        preferred = all_groups
    else:
        if not isinstance(raw_groups, list) or not raw_groups:
            raise MastersError("asset_groups 는 비어 있지 않은 목록이어야 합니다.")
        preferred = set()
        for g in raw_groups:
            if g not in all_groups:
                raise MastersError(f"알 수 없는 자산군입니다: {g!r}")
            preferred.add(g)
    return risk, horizon, preferred


def _round_weights(weights: dict[str, float]) -> dict[str, float]:
    """0.1 단위 최대잉여법 반올림 — 합계 100.0 을 정확히 보존한다."""
    scaled = {a: w * 10 for a, w in weights.items()}
    floored = {a: int(v) for a, v in scaled.items()}
    remainder = round(1000 - sum(floored.values()))
    # 소수부 큰 순서(동률이면 자산 id 순)로 1(=0.1%p)씩 배분 — 결정론 보장.
    order = sorted(scaled, key=lambda a: (-(scaled[a] - floored[a]), a))
    for i in range(remainder):
        floored[order[i % len(order)]] += 1
    return {a: v / 10 for a, v in floored.items()}


def _scale_group(
    weights: dict[str, float],
    members: list[str],
    delta: float,
) -> None:
    """members 자산들에 delta(%p)를 기존 비중 비례로 가감한다 (in-place)."""
    total = sum(weights[a] for a in members)
    if total <= 0:
        return
    for a in members:
        weights[a] += delta * (weights[a] / total)


def _personalize_strategy(
    strategy: dict[str, Any],
    risk: str,
    horizon: str,
    preferred: set[str],
    catalog: dict[str, Any],
) -> dict[str, Any]:
    assets_meta = catalog["asset_classes"]
    pers = catalog["personalization"]
    adjustments: list[str] = []

    weights = {row["asset"]: float(row["weight"]) for row in strategy["base_allocation"]}
    notes = {row["asset"]: row.get("note", "") for row in strategy["base_allocation"]}

    # 1) 선호 자산군 반영 — 제외 자산의 비중을 남은 자산에 비례 배분.
    excluded = [a for a in weights if _asset_group(catalog, a) not in preferred]
    if excluded:
        excluded_weight = sum(weights[a] for a in excluded)
        remaining = [a for a in weights if a not in excluded]
        if not remaining:
            return _strategy_result(
                strategy, risk, horizon, preferred, catalog, allocation=None, adjustments=[],
                note="선택한 자산군만으로는 이 전략의 배분을 구성할 수 없습니다. 자산군 선택을 넓혀 보세요.",
            )
        excluded_labels = ", ".join(assets_meta[a]["label"] for a in excluded)
        for a in excluded:
            del weights[a]
        _scale_group(weights, list(weights), excluded_weight)
        adjustments.append(f"선호 자산군에서 제외한 {excluded_labels} 비중 {excluded_weight:g}%p 를 남은 자산에 비례 배분했습니다.")

    # 2) 위험 성향·투자 기간 반영 — 주식군 ↔ 방어군(채권·현금성) 사이의 이동.
    shift = float(pers["risk_equity_shift_pt"][risk]) + float(pers["horizon_equity_shift_pt"][horizon])
    shift = max(float(pers["shift_min_pt"]), min(float(pers["shift_max_pt"]), shift))
    equity_assets = [a for a in weights if _asset_group(catalog, a) == "equity"]
    defensive_assets = [a for a in weights if _asset_group(catalog, a) in ("bond", "cash")]
    if shift < 0 and equity_assets and defensive_assets:
        moved = min(-shift, sum(weights[a] for a in equity_assets))
        if moved > 0:
            _scale_group(weights, equity_assets, -moved)
            _scale_group(weights, defensive_assets, moved)
            adjustments.append(f"성향·기간에 맞춰 주식 비중 {moved:g}%p 를 채권·현금성으로 옮겼습니다.")
    elif shift > 0 and equity_assets and defensive_assets:
        floor = float(pers["defensive_floor_pt"])
        available = max(0.0, sum(weights[a] for a in defensive_assets) - floor)
        moved = min(shift, available)
        if moved > 0:
            _scale_group(weights, defensive_assets, -moved)
            _scale_group(weights, equity_assets, moved)
            adjustments.append(f"성향·기간에 맞춰 채권·현금성 비중 {moved:g}%p 를 주식으로 옮겼습니다.")

    weights = {a: w for a, w in weights.items() if w > 1e-9}
    rounded = _round_weights(weights)
    allocation = [
        {
            "asset": a,
            "label": assets_meta[a]["label"],
            "group": _asset_group(catalog, a),
            "weight": rounded[a],
            "note": notes.get(a, ""),
        }
        for a in sorted(rounded, key=lambda a: -rounded[a])
    ]
    return _strategy_result(strategy, risk, horizon, preferred, catalog, allocation=allocation, adjustments=adjustments)


def _fit_score_and_reasons(
    strategy: dict[str, Any],
    risk: str,
    horizon: str,
    preferred: set[str],
    catalog: dict[str, Any],
) -> tuple[int, list[str]]:
    fit = strategy["fit"]
    options = catalog["profile_options"]
    risk_label = next(o["label"] for o in options["risk"] if o["id"] == risk)
    horizon_label = next(o["label"] for o in options["horizon"] if o["id"] == horizon)

    score = 0.0
    reasons: list[str] = []
    if risk in fit["risk"]:
        score += 35
        reasons.append(f"위험 성향({risk_label})이 이 전략의 결에 맞습니다.")
    else:
        score += 10
        reasons.append(f"위험 성향({risk_label})과는 다소 결이 다른 전략입니다.")
    if horizon in fit["horizon"]:
        score += 35
        reasons.append(f"투자 기간({horizon_label})이 이 전략이 전제하는 기간과 맞습니다.")
    else:
        score += 10
        reasons.append(f"투자 기간({horizon_label})은 이 전략이 전제하는 기간과 다릅니다.")

    covered = sum(
        float(row["weight"])
        for row in strategy["base_allocation"]
        if _asset_group(catalog, row["asset"]) in preferred
    )
    score += 30 * (covered / 100.0)
    if covered >= 100 - _WEIGHT_SUM_TOLERANCE:
        reasons.append("기본 배분의 모든 자산군이 선호 자산군 안에 있습니다.")
    else:
        reasons.append(f"기본 배분 중 {covered:g}% 만 선호 자산군에 속합니다.")
    return round(score), reasons


def _strategy_result(
    strategy: dict[str, Any],
    risk: str,
    horizon: str,
    preferred: set[str],
    catalog: dict[str, Any],
    *,
    allocation: list[dict[str, Any]] | None,
    adjustments: list[str],
    note: str | None = None,
) -> dict[str, Any]:
    score, reasons = _fit_score_and_reasons(strategy, risk, horizon, preferred, catalog)
    return {
        "strategy_id": strategy["id"],
        "master": strategy["master"],
        "title": strategy["title"],
        "fit_score": score,
        "fit_reasons": reasons,
        "allocation": allocation,
        "adjustments": adjustments,
        "note": note,
        "rebalancing": strategy["rebalancing"],
    }


def personalize(profile: dict[str, Any]) -> dict[str, Any]:
    """POST /api/masters/simulate — 성향 기반 전략별 참고용 배분."""
    catalog = load_catalog()
    risk, horizon, preferred = _parse_profile(profile, catalog)
    results = [
        _personalize_strategy(s, risk, horizon, preferred, catalog)
        for s in catalog["strategies"]
    ]
    results.sort(key=lambda r: (-r["fit_score"], r["strategy_id"]))
    return {
        "disclaimer": catalog["disclaimer"],
        "profile": {"risk": risk, "horizon": horizon, "asset_groups": sorted(preferred)},
        "results": results,
    }
