"""투자 대가 관점의 포트폴리오 진단 (masters review).

'도구 > 투자 대가의 전략'의 세 번째 단계: 카탈로그(data/investment_masters.json)의
철학 요약과 사용자의 실제 보유 포트폴리오를 한 프롬프트로 묶어, 지정한 대가
1명의 관점에서 진단 리포트(마크다운)를 생성한다.

- LLM 호출은 ``services.ai_client.post_chat_completion`` 공용 경로를 쓴다 —
  키 확인, 일일 예산 가드(enforce_budget_caps → 429), 사용량 원장 기록까지
  거기서 처리된다. 모델은 ai_config MODEL_FEATURES["masters_review"] 로
  관리자 화면에서 교체 가능.
- 결정론 파트(자산군 근사 비중, 상위 집중도, 대가 예시 배분과의 갭)는 LLM
  없이 계산되어 응답에 함께 실린다 — 프롬프트의 근거이자 UI 시각화의
  단일 소스. 자산군 분류는 카탈로그의 asset_class_patterns(이름 부분일치)
  휴리스틱이라 "근사"임을 응답·프롬프트 양쪽에 명시한다.

의존 방향: repositories/portfolio, services.ai_client, services.investment_masters,
sibling quote_service. routes 를 import 하지 않는다.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import ai_config
from repositories import portfolio as portfolio_repo
from services import ai_client, investment_masters
from services.portfolio import quote_service
from services.portfolio.ai_analysis import fmt_krw

logger = logging.getLogger(__name__)

REVIEW_MAX_TOKENS = int(os.getenv("MASTERS_REVIEW_MAX_TOKENS", "3500"))
REVIEW_TIMEOUT_S = float(os.getenv("MASTERS_REVIEW_TIMEOUT_S", "120"))
# 프롬프트에 넣는 보유 종목 상한 — 초과분은 비중 하위부터 잘라 요약 한 줄로.
HOLDINGS_LIMIT = int(os.getenv("MASTERS_REVIEW_HOLDINGS_LIMIT", "40"))


class EmptyPortfolioError(RuntimeError):
    """보유 종목이 없어 진단할 수 없음 (route 가 400 으로 매핑)."""


def classify_holding(name: str, code: str, currency: str | None = None) -> str:
    """보유 종목 → 자산군(asset class) 휴리스틱 분류.

    카탈로그 asset_class_patterns 를 위에서부터 이름 부분일치로 적용하고,
    해외 통화/알파벳 시작 티커는 글로벌 주식, 나머지는 fallback(국내 주식).
    """
    catalog = investment_masters.load_catalog()
    text = (name or "").upper()
    for rule in catalog.get("asset_class_patterns", []):
        if rule["match"].upper() in text:
            return rule["asset"]
    if currency and str(currency).upper() not in ("KRW", ""):
        return "equity_global"
    norm_code = (code or "").strip().upper()
    if norm_code and not norm_code[:1].isdigit():
        return "equity_global"
    return catalog.get("asset_class_fallback", "equity_kr")


def portfolio_breakdown(enriched: list[dict]) -> dict[str, Any]:
    """평가액 기반 비중·집중도·자산군 근사 비중. 시세 없는 종목은 따로 표기."""
    catalog = investment_masters.load_catalog()
    assets_meta = catalog["asset_classes"]
    valued: list[dict[str, Any]] = []
    unpriced: list[str] = []
    total_value = 0.0
    for item in enriched:
        quote = item.get("quote") or {}
        price = quote.get("price")
        qty = item.get("quantity") or 0
        name = item.get("stock_name") or item.get("stock_code", "")
        if not price or not qty:
            unpriced.append(name)
            continue
        mv = float(price) * float(qty)
        total_value += mv
        valued.append({
            "name": name,
            "code": item.get("stock_code", ""),
            "value": mv,
            "asset": classify_holding(name, item.get("stock_code", ""), item.get("currency")),
        })
    valued.sort(key=lambda r: -r["value"])
    asset_totals: dict[str, float] = {}
    for row in valued:
        row["weight"] = round(row["value"] / total_value * 100, 1) if total_value else 0.0
        asset_totals[row["asset"]] = asset_totals.get(row["asset"], 0.0) + row["value"]
    asset_weights = [
        {
            "asset": asset,
            "label": assets_meta[asset]["label"],
            "group": assets_meta[asset]["group"],
            "weight": round(value / total_value * 100, 1) if total_value else 0.0,
        }
        for asset, value in sorted(asset_totals.items(), key=lambda kv: -kv[1])
    ]
    top3 = round(sum(r["weight"] for r in valued[:3]), 1)
    return {
        "total_value": total_value,
        "holdings_count": len(valued),
        "top3_weight": top3,
        "holdings": [
            {"name": r["name"], "code": r["code"], "weight": r["weight"], "asset": r["asset"]}
            for r in valued
        ],
        "asset_weights": asset_weights,
        "unpriced": unpriced,
    }


def allocation_gap(breakdown: dict[str, Any], strategy: dict[str, Any]) -> list[dict[str, Any]]:
    """내 자산군 근사 비중 vs 대가 예시 배분(base_allocation) 갭 표."""
    catalog = investment_masters.load_catalog()
    assets_meta = catalog["asset_classes"]
    mine = {row["asset"]: row["weight"] for row in breakdown["asset_weights"]}
    target = {row["asset"]: float(row["weight"]) for row in strategy["base_allocation"]}
    rows = []
    for asset in sorted(set(mine) | set(target), key=lambda a: -(target.get(a, 0.0))):
        mine_w = mine.get(asset, 0.0)
        target_w = target.get(asset, 0.0)
        rows.append({
            "asset": asset,
            "label": assets_meta[asset]["label"],
            "mine": mine_w,
            "target": target_w,
            "diff": round(mine_w - target_w, 1),
        })
    return rows


def build_review_prompt(
    strategy: dict[str, Any],
    breakdown: dict[str, Any],
    gap: list[dict[str, Any]],
) -> str:
    master = strategy["master"]
    principles = "\n".join(f"- {p}" for p in strategy["principles"])
    base_alloc = ", ".join(
        f"{row['asset']} {row['weight']}%" for row in strategy["base_allocation"]
    )
    holdings = breakdown["holdings"][:HOLDINGS_LIMIT]
    holdings_lines = "\n".join(
        f"- {h['name']} ({h['code']}): 비중 {h['weight']}% [{h['asset']}]" for h in holdings
    )
    omitted = breakdown["holdings_count"] - len(holdings)
    if omitted > 0:
        holdings_lines += f"\n- (비중 하위 {omitted}개 종목 생략)"
    unpriced_line = (
        f"\n- 시세 미확보로 비중 계산에서 빠진 종목: {', '.join(breakdown['unpriced'][:10])}"
        if breakdown["unpriced"] else ""
    )
    gap_lines = "\n".join(
        f"- {row['label']}: 내 비중 {row['mine']}% vs 예시 배분 {row['target']}% (차이 {row['diff']:+g}%p)"
        for row in gap
    )
    return f"""당신은 {master}의 공개된 투자 철학·저서·발언을 깊이 이해한 분석가입니다.
아래 사용자 포트폴리오를 {master}의 관점을 빌려 진단해 주세요.

## {master} — {strategy['title']} 철학 요약
{principles}
예시 자산배분: {base_alloc}
적합한 투자자: {strategy['fit']['description']}

## 사용자 포트폴리오 (총 평가 {fmt_krw(breakdown['total_value'])}, {breakdown['holdings_count']}종목, 상위3 집중도 {breakdown['top3_weight']}%)
{holdings_lines}{unpriced_line}

## 자산군 근사 비중 vs {master} 예시 배분 (종목명 휴리스틱 분류 — 근사치)
{gap_lines}

작성 규칙:
- "{master}라면 ~라고 볼 것입니다" 식의 관점 차용임을 유지하세요. 실존 인물의 발언을 창작하는 것이므로 단정적 인용은 금지합니다.
- 특정 종목의 매수/매도를 단정하지 마세요. 점검 관점과 질문으로 표현하세요.
- 제공된 데이터에 근거하고, 모르는 정보(종목의 사업 내용 등)는 추정임을 밝히세요.
- 한국어 마크다운으로만, HTML 태그 없이 답하세요.

답변 형식:
- ## 총평: 3개 이내 bullet — 이 포트폴리오를 {master} 철학의 눈으로 본 첫인상
- ## 철학에 부합하는 점: 구체적 종목/비중을 근거로
- ## 철학과 어긋나는 점: 구체적 종목/비중을 근거로
- ## {master}라면 던질 질문: 사용자가 스스로 점검할 질문 3~5개
- ## 점검 아이디어: 매매 지시가 아닌, 확인해 볼 데이터와 재배분 관점 (참고용임을 한 줄 명시)

각 섹션은 짧게, 필요하면 마크다운 표를 사용하세요."""


async def generate_review(payload: dict[str, Any], user: dict[str, Any]) -> dict[str, Any]:
    """POST /api/masters/review — 대가 1명의 관점으로 내 포트폴리오 진단."""
    catalog = investment_masters.load_catalog()
    strategy_id = payload.get("strategy_id")
    strategy = next((s for s in catalog["strategies"] if s["id"] == strategy_id), None)
    if strategy is None:
        raise investment_masters.MastersError(f"strategy_id 가 올바르지 않습니다: {strategy_id!r}")

    items = await portfolio_repo.get_portfolio(google_sub=user["google_sub"])
    if not items:
        raise EmptyPortfolioError("portfolio is empty")
    enriched = await quote_service.enrich_with_cached_quotes(items)
    breakdown = portfolio_breakdown(enriched)
    if not breakdown["holdings"]:
        raise EmptyPortfolioError("no priced holdings")
    gap = allocation_gap(breakdown, strategy)

    prompt = build_review_prompt(strategy, breakdown, gap)
    model = await ai_config.get_model_for_feature("masters_review")
    request_payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "당신은 투자 대가들의 공개된 철학을 바탕으로 포트폴리오를 교육 목적으로 진단하는 리서치 어시스턴트입니다. 투자 조언이 아닌 관점 제시임을 지키세요.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.4,
        "max_tokens": REVIEW_MAX_TOKENS,
        **ai_config.openrouter_reasoning_controls(model),
    }
    result = await ai_client.post_chat_completion(
        feature="masters_review",
        payload=request_payload,
        google_sub=user["google_sub"],
        model=model,
        model_profile="masters_review",
        timeout=REVIEW_TIMEOUT_S,
        ok_if_content=True,
    )
    return {
        "disclaimer": catalog["disclaimer"],
        "strategy": {"id": strategy["id"], "master": strategy["master"], "title": strategy["title"]},
        "markdown": result["content"],
        "breakdown": {
            "total_value": breakdown["total_value"],
            "holdings_count": breakdown["holdings_count"],
            "top3_weight": breakdown["top3_weight"],
            "asset_weights": breakdown["asset_weights"],
            "unpriced": breakdown["unpriced"],
        },
        "gap": gap,
        "model": result["model"],
        "input_tokens": result["input_tokens"],
        "output_tokens": result["output_tokens"],
        "cost_usd": result["cost_usd"],
        "truncated": result["finish_reason"] == "length",
    }
