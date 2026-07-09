"""AI portfolio analysis (OpenRouter) — domain logic behind the AI endpoints.

Extracted from ``routes/portfolio.py``. The HTTP/SSE shell stays in the route:
auth, ``StreamingResponse`` construction, the ``data: <json>\\n\\n`` framing and
the typed-error → HTTPException mapping. This module owns the domain side:
model selection, prompt assembly (holdings summary, NAV performance, market
indicators, per-holding wiki research), the streaming OpenRouter call, AI
usage-ledger writes and the final done-event payload.

Seam: ``prepare_analysis`` resolves model/key and assembles the prompt up
front (so empty-portfolio / missing-key still surface as HTTP errors before
streaming starts, exactly as before); ``stream_analysis`` then yields plain
dict events (``{"content": ...}`` deltas and one ``{"done": True, ...}``
summary) which the route serializes to SSE lines verbatim.

Dependency direction: this module depends on shared data modules
(``ai_config``, ``cache``, ``market_indicators``) and sibling services
(``services.ai_client``, ``services.portfolio.quote_service``). It must not
import ``routes``.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, AsyncIterator, Awaitable, Callable

import httpx

import ai_config
import market_indicators
from core.http import get_http_client
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from repositories import wiki as wiki_repo
from services import ai_client
from services.market.formatting import format_indicator_change as _market_summary_change
from services.portfolio import quote_service

logger = logging.getLogger(__name__)

PORTFOLIO_AI_DEFAULT_MODEL = "google/gemini-3.5-flash"
AI_DEFAULT_MODEL = os.getenv("AI_DEFAULT_MODEL", PORTFOLIO_AI_DEFAULT_MODEL)
AI_FAST_MODEL = os.getenv("AI_FAST_MODEL", PORTFOLIO_AI_DEFAULT_MODEL)
AI_PREMIUM_MODEL = os.getenv("AI_PREMIUM_MODEL", AI_DEFAULT_MODEL)
AI_MAX_TOKENS = int(os.getenv("PORTFOLIO_AI_MAX_TOKENS", "8000"))
AI_REASONING_EFFORT = os.getenv("PORTFOLIO_AI_REASONING_EFFORT", "low").strip().lower()
WIKI_HOLDING_LIMIT = int(os.getenv("PORTFOLIO_AI_WIKI_HOLDING_LIMIT", "15"))
WIKI_ENTRY_LIMIT = int(os.getenv("PORTFOLIO_AI_WIKI_ENTRY_LIMIT", "3"))
WIKI_KEYPOINT_CHARS = int(os.getenv("PORTFOLIO_AI_WIKI_KEYPOINT_CHARS", "600"))
TRUNCATION_NOTICE = (
    "\n\n> 응답이 모델 출력 토큰 한도에 도달해 끝부분이 잘렸을 수 있습니다. "
    "질문 범위를 좁히거나 다시 실행해 주세요.\n"
)

# Hard cap to avoid runaway prompt growth; anything sensible fits easily.
USER_QUERY_MAX_CHARS = 4000

SYSTEM_PROMPT = """당신은 한국/해외 자산을 함께 보는 투자 리서치 어시스턴트입니다.
규칙:
- 제공된 포트폴리오, 시장지표, 리서치 요약에 근거해 답하세요.
- 알 수 없는 사실은 추정이라고 분명히 말하고, 없는 데이터를 꾸며내지 마세요.
- 투자 조언은 단정 대신 조건부 시나리오와 리스크로 표현하세요.
- 결론에는 실행 우선순위와 확인해야 할 데이터 공백을 포함하세요."""


class MissingAPIKeyError(RuntimeError):
    """No OpenRouter API key is configured (route maps this to HTTP 500)."""


class EmptyPortfolioError(RuntimeError):
    """The user has no holdings to analyze (route maps this to HTTP 400)."""


# ---------------------------------------------------------------------------
# Model selection
# ---------------------------------------------------------------------------


def env_model_profiles() -> dict[str, str]:
    """Env-only profile map (legacy fallback; runtime uses ``model_profiles``)."""
    return {
        "fast": AI_FAST_MODEL,
        "balanced": AI_DEFAULT_MODEL,
        "premium": AI_PREMIUM_MODEL,
    }


async def model_profiles() -> dict[str, str]:
    return await ai_config.model_profiles()


async def resolve_model(payload: dict, user: dict) -> tuple[str, str]:
    profile = str(payload.get("profile") or payload.get("mode") or "balanced").strip().lower()
    profiles = await model_profiles()
    if profile not in profiles:
        profile = "balanced"
    model = profiles[profile]
    req_model = str(payload.get("model") or "").strip()
    if req_model and user.get("is_admin"):
        model = req_model
        profile = "custom"
    return model, profile


async def list_models() -> dict:
    """Available OpenRouter models + profile map (for the admin model picker)."""
    profiles = await model_profiles()
    openrouter_key = await ai_config.get_openrouter_key()
    if not openrouter_key:
        return {"models": [], "default": profiles["balanced"], "profiles": profiles}
    try:
        client = await get_http_client("openrouter")
        resp = await client.get("https://openrouter.ai/api/v1/models", timeout=10.0)
        data = resp.json().get("data", [])
        models = []
        for m in data:
            p = m.get("pricing", {})
            models.append({
                "id": m["id"],
                "name": m.get("name", m["id"]),
                "prompt_price": float(p.get("prompt", 0)) * 1e6,
                "completion_price": float(p.get("completion", 0)) * 1e6,
                "context": m.get("context_length", 0),
            })
        models.sort(key=lambda x: x["id"])
        return {"models": models, "default": profiles["balanced"], "profiles": profiles}
    except Exception as exc:
        logger.warning("Failed to fetch OpenRouter models: %s", exc)
        return {"models": [], "default": profiles["balanced"], "profiles": profiles}


# ---------------------------------------------------------------------------
# Prompt assembly
# ---------------------------------------------------------------------------


def fmt_krw(v: float) -> str:
    """Format KRW for AI prompt: 조/억 units with 4 significant digits."""
    av = abs(v)
    if av >= 1e12:  # 조
        jo = v / 1e12
        # 4 sig figs: e.g. 93.56조, 1.234조, 123.4조
        if av >= 1e15:
            return f"{jo:,.0f}조"
        elif av >= 1e14:
            return f"{jo:,.1f}조"
        elif av >= 1e13:
            return f"{jo:,.2f}조"
        else:
            return f"{jo:,.3f}조"
    elif av >= 1e8:  # 억
        eok = v / 1e8
        if av >= 1e11:
            return f"{eok:,.0f}억"
        elif av >= 1e10:
            return f"{eok:,.1f}억"
        elif av >= 1e9:
            return f"{eok:,.2f}억"
        else:
            return f"{eok:,.3f}억"
    else:
        return f"{v:,.0f}원"


def holdings_summary(enriched: list[dict]) -> tuple[list[str], float]:
    """Per-holding prompt lines + total market value (KRW)."""
    holdings_lines = []
    total_value = 0
    for item in enriched:
        q = item.get("quote", {})
        price = q.get("price")
        qty = item.get("quantity", 0)
        avg = item.get("avg_price", 0)
        mv = price * qty if price and qty else None
        ret = ((price - avg) / avg * 100) if price and avg and avg > 0 else None
        chg = q.get("change_pct")
        name = item.get("stock_name", item["stock_code"])
        line = f"- {name} ({item['stock_code']}): 수량={qty}, 매입가={fmt_krw(avg)}"
        if price:
            line += f", 현재가={fmt_krw(price)}"
        if ret is not None:
            line += f", 수익률={ret:+.1f}%"
        if chg is not None:
            line += f", 일간={chg:+.2f}%"
        if mv:
            line += f", 평가={fmt_krw(mv)}"
            total_value += mv
        holdings_lines.append(line)
    return holdings_lines, total_value


def performance_lines(nav_history: list[dict]) -> list[str]:
    """NAV / YoY / CAGR prompt lines from the NAV history."""
    perf_lines = []
    if nav_history:
        latest = nav_history[-1]
        first = nav_history[0]
        perf_lines.append(f"NAV: {latest['nav']:.2f} ({first['date']}~{latest['date']})")
        if len(nav_history) > 252:
            yoy = (latest['nav'] / nav_history[-252]['nav'] - 1) * 100
            perf_lines.append(f"YoY: {yoy:+.2f}%")
        days = (date.fromisoformat(latest['date']) - date.fromisoformat(first['date'])).days
        if days > 365:
            cagr = ((latest['nav'] / first['nav']) ** (365 / days) - 1) * 100
            perf_lines.append(f"CAGR: {cagr:+.2f}%")
    return perf_lines


MARKET_SUMMARY_LABELS = {
    "USD_KRW": "환율",
    "US10Y": "미국10년물",
    "OIL_CL": "유가",
}


def _market_summary_label(code: str, item: dict) -> str:
    return MARKET_SUMMARY_LABELS.get(code) or item.get("label") or code


async def market_summary_lines() -> list[str]:
    try:
        market = await market_indicators.fetch_indicators(["KOSPI", "KOSDAQ", "USD_KRW", "SPX", "US10Y", "OIL_CL"])
        lines = []
        for code, item in market.items():
            value = item.get("value", "")
            if value in (None, ""):
                continue
            change = _market_summary_change(item)
            suffix = f" ({change})" if change else ""
            lines.append(f"- {_market_summary_label(code, item)}: {value}{suffix}")
        return lines
    except Exception:
        return ["시장 데이터를 가져올 수 없습니다."]


async def wiki_research_lines(enriched: list[dict]) -> tuple[list[str], int]:
    """Per-holding wiki snippets. Keep this bounded, but use enough context
    for the model to compare major positions instead of reacting only to
    the largest few holdings."""
    wiki_lines: list[str] = []
    wiki_used_count = 0
    try:
        ranked = sorted(
            (i for i in enriched if (i.get("quote", {}) or {}).get("price") and i.get("quantity")),
            key=lambda i: (i["quote"]["price"] or 0) * (i.get("quantity") or 0),
            reverse=True,
        )[:WIKI_HOLDING_LIMIT]
        for item in ranked:
            code = item["stock_code"]
            name = item.get("stock_name") or code
            entries = await wiki_repo.get_wiki_entries(code, limit=WIKI_ENTRY_LIMIT)
            if not entries:
                continue
            wiki_lines.append(f"### {name} ({code})")
            for e in entries:
                date_s = e.get("report_date") or (e.get("created_at") or "")[:10]
                firm = e.get("firm") or ""
                rec = (e.get("recommendation") or "").strip()
                tp = e.get("target_price")
                tp_s = f"TP={int(tp):,}" if tp else ""
                head = f"- [{firm}, {date_s}"
                if rec:
                    head += f", {rec}"
                if tp_s:
                    head += f", {tp_s}"
                head += "]"
                key = (e.get("key_points_md") or "").strip()
                # Fold bullets into one line (< 300 chars) so the full
                # prompt stays readable and compact.
                flat = " ".join(line.lstrip("- \t") for line in key.splitlines() if line.strip())
                if flat:
                    head += f" {flat[:WIKI_KEYPOINT_CHARS]}"
                wiki_lines.append(head)
                wiki_used_count += 1
    except Exception as _wiki_exc:
        logger.warning("portfolio AI wiki injection failed: %s", _wiki_exc)
    return wiki_lines, wiki_used_count


def build_prompt(
    *,
    holdings_lines: list[str],
    total_value: float,
    perf_lines: list[str],
    market_lines: list[str],
    wiki_lines: list[str],
    user_query: str,
) -> str:
    query_section = f"""

## 사용자 질문/요청
{user_query}

위 질문/요청을 우선적으로 고려하여 답변해 주세요.""" if user_query else ""

    wiki_section = (
        f"\n\n## 종목별 리서치 요약 (최근 증권사 리포트)\n{chr(10).join(wiki_lines)}"
        if wiki_lines else ""
    )

    return f"""아래 포트폴리오를 분석해 주세요.

## 보유 종목 (총 평가: {fmt_krw(total_value)})
{chr(10).join(holdings_lines)}

## 성과
{chr(10).join(perf_lines) if perf_lines else "N/A"}

## 시장 현황
{chr(10).join(market_lines)}{wiki_section}{query_section}

분석 항목:
1. 포트폴리오 구성 평가 (분산도, 섹터 편중)
2. 주요 종목 밸류에이션과 리스크 — 증권사 의견을 근거로 인용 가능하면 인용
3. 시장 상황 고려 단기/중기 시나리오
4. 리밸런싱/비중 조절 제안과 우선순위
5. 추가로 확인해야 할 데이터 공백

답변 형식:
- ## 핵심 판단: 3개 이내 bullet
- ## 포트폴리오 점검: 편중, 수익률, 종목별 근거
- ## 판단 근거: 데이터에서 실제로 확인한 근거와 추정의 구분
- ## 리스크와 촉매: 단기/중기 시나리오
- ## 실행 우선순위: 우선순위가 높은 조치부터
- ## 추가 확인 데이터: 부족한 데이터와 확인 방법

각 섹션은 짧게 유지하되, 가독성이 좋아지는 경우 마크다운 표, 비교 도표, ASCII 막대그래프를 적극 활용하세요.
내부 추론 과정은 노출하지 말고, 최종 판단의 근거 요약만 보여 주세요.
HTML 태그는 쓰지 말고 한국어 마크다운으로만 답변해 주세요."""


# ---------------------------------------------------------------------------
# Analysis orchestration (prepare → stream)
# ---------------------------------------------------------------------------


@dataclass
class AnalysisContext:
    """Everything ``stream_analysis`` needs, resolved before streaming starts."""

    model: str
    model_profile: str
    prompt: str
    wiki_used_count: int
    openrouter_key: str
    google_sub: str
    started_at: float


async def prepare_analysis(payload: dict, user: dict) -> AnalysisContext:
    """Resolve key/model and assemble the prompt for one analysis request.

    Raises ``MissingAPIKeyError`` / ``EmptyPortfolioError`` so these still
    surface as plain HTTP errors before any SSE bytes are sent.
    """
    openrouter_key = await ai_config.get_openrouter_key()
    if not openrouter_key:
        raise MissingAPIKeyError("OpenRouter API key is not configured")

    model, model_profile = await resolve_model(payload, user)
    started_at = time.perf_counter()

    # Optional user inquiry/question to include in the prompt
    user_query = (payload.get("query") or "").strip()
    if len(user_query) > USER_QUERY_MAX_CHARS:
        user_query = user_query[:USER_QUERY_MAX_CHARS]

    google_sub = user["google_sub"]
    items = await portfolio_repo.get_portfolio(google_sub=google_sub)
    if not items:
        raise EmptyPortfolioError("portfolio is empty")

    enriched = await quote_service.enrich_with_cached_quotes(items)
    holdings_lines, total_value = holdings_summary(enriched)
    perf_lines = performance_lines(await snapshots_repo.get_nav_history(google_sub))
    market_lines = await market_summary_lines()
    wiki_lines, wiki_used_count = await wiki_research_lines(enriched)

    prompt = build_prompt(
        holdings_lines=holdings_lines,
        total_value=total_value,
        perf_lines=perf_lines,
        market_lines=market_lines,
        wiki_lines=wiki_lines,
        user_query=user_query,
    )
    return AnalysisContext(
        model=model,
        model_profile=model_profile,
        prompt=prompt,
        wiki_used_count=wiki_used_count,
        openrouter_key=openrouter_key,
        google_sub=google_sub,
        started_at=started_at,
    )


def _done_event(
    ctx: AnalysisContext,
    *,
    input_tokens,
    output_tokens,
    cost,
    finish_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "done": True,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "model": ctx.model,
        "model_profile": ctx.model_profile,
        "cost": cost,
        "wiki_used": ctx.wiki_used_count,
        "reasoning_effort": AI_REASONING_EFFORT,
        "context_holdings": WIKI_HOLDING_LIMIT,
        "context_reports_per_holding": WIKI_ENTRY_LIMIT,
        "finish_reason": finish_reason,
        "truncated": finish_reason == "length",
        "max_tokens": AI_MAX_TOKENS,
    }


async def stream_analysis(
    ctx: AnalysisContext,
    *,
    is_disconnected: Callable[[], Awaitable[bool]],
) -> AsyncIterator[dict[str, Any]]:
    """Stream one analysis as domain events: ``{"content": ...}`` deltas,
    then a final ``{"done": True, ...}`` summary. Usage-ledger writes happen
    here; the route only adapts each yielded dict to an SSE line.

    ``is_disconnected`` is polled between upstream chunks so a closed browser
    tab stops the (per-token billed) OpenRouter request early.
    """
    # Use client.stream() (context manager) rather than client.post() —
    # the latter buffers the entire response body even with stream=True
    # in the JSON payload, defeating the purpose and inflating latency
    # until the model finishes. With stream() the first token reaches
    # the browser as soon as OpenRouter emits it.
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=None)) as client:
        request_payload = {
            "model": ctx.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": ctx.prompt},
            ],
            "max_tokens": AI_MAX_TOKENS,
            "stream": True,
            **ai_config.openrouter_reasoning_controls(ctx.model, effort=AI_REASONING_EFFORT),
        }
        async with ai_client.stream_chat_completion(
            client,
            request_payload,
            openrouter_key=ctx.openrouter_key,
        ) as resp:
            if resp.status_code != 200:
                # Need to consume body before httpx exposes it.
                body = await resp.aread()
                try:
                    err = json.loads(body)
                    msg = err.get("error", {}).get("message", f"HTTP {resp.status_code}")
                except Exception:
                    msg = f"HTTP {resp.status_code}"
                yield {"content": f"API 오류: {msg}"}
                await ai_config.record_usage(
                    google_sub=ctx.google_sub,
                    feature="portfolio_analysis",
                    model=ctx.model,
                    model_profile=ctx.model_profile,
                    ok=False,
                    error=msg,
                    latency_ms=int((time.perf_counter() - ctx.started_at) * 1000),
                )
                yield _done_event(ctx, input_tokens=0, output_tokens=0, cost=0)
                return

            input_tokens = 0
            output_tokens = 0
            cost = 0
            finish_reason = None
            async for line in resp.aiter_lines():
                # If the browser closed the tab, stop consuming upstream
                # tokens — OpenRouter bills per-token and a forgotten
                # request could run to the full max_tokens budget.
                if await is_disconnected():
                    logger.info("AI analysis: client disconnected, aborting upstream")
                    return
                if not line.startswith("data: "):
                    continue
                payload = line[6:]
                if payload.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(payload)
                    if "error" in chunk:
                        yield {"content": chunk["error"].get("message", "Unknown error")}
                        break
                    choice = (chunk.get("choices") or [{}])[0] or {}
                    chunk_finish_reason = choice.get("finish_reason")
                    if chunk_finish_reason:
                        finish_reason = chunk_finish_reason
                    delta = choice.get("delta", {})
                    content = delta.get("content", "")
                    if content:
                        output_tokens += 1
                        yield {"content": content}
                    usage = chunk.get("usage")
                    if usage:
                        input_tokens = usage.get("prompt_tokens", input_tokens)
                        output_tokens = usage.get("completion_tokens", output_tokens)
                        cost = usage.get("cost", cost) or cost
                except Exception:
                    continue
            truncated = finish_reason == "length"
            if truncated:
                yield {"content": TRUNCATION_NOTICE}
            await ai_config.record_usage(
                google_sub=ctx.google_sub,
                feature="portfolio_analysis",
                model=ctx.model,
                model_profile=ctx.model_profile,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost_usd=float(cost or 0),
                latency_ms=int((time.perf_counter() - ctx.started_at) * 1000),
                ok=not truncated,
                error="finish_reason_length" if truncated else None,
            )
            yield _done_event(
                ctx,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost=cost,
                finish_reason=finish_reason,
            )
