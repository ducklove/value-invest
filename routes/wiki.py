"""Per-stock wiki endpoints.

- GET /api/analysis/{stock_code}/wiki — public, lists summarized reports.
- POST /api/analysis/{stock_code}/ask — auth-required, streamed RAG Q&A
  over the stock's wiki + basic indicators. Rate-limited per-user.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta

import httpx
from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

import cache
from deps import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


# Per-user daily quota — prevents runaway cost and accidental loops.
QA_DAILY_LIMIT = int(os.environ.get("WIKI_QA_DAILY_LIMIT", "20"))
# Max question length; reasonable investor questions fit easily in 1k chars.
QA_MAX_QUESTION_CHARS = 1000
# Number of wiki entries retrieved as context for each question.
QA_CONTEXT_ENTRIES = 5
# Hard cap on key_points length per entry in the prompt — keeps total
# prompt bounded regardless of how verbose any single summary is.
QA_CTX_PER_ENTRY_CHARS = 800


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


def _today_kst_iso() -> str:
    """Return the start-of-day ISO timestamp in KST. We don't carry a
    tzinfo through saves, but the server is KST so naive-now already
    matches the stored `created_at` string format."""
    # Anchor at midnight local time.
    now = datetime.now()
    return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


@router.get("/api/analysis/{stock_code}/wiki")
async def get_stock_wiki(stock_code: str, limit: int = Query(20, ge=1, le=100)):
    """Return wiki entries for a stock, most-recent first. Public read."""
    entries = await cache.get_wiki_entries(stock_code, limit=limit)
    return {
        "stock_code": stock_code,
        "count": len(entries),
        "entries": entries,
    }


@router.get("/api/wiki/stats")
async def get_wiki_stats():
    """Cheap aggregate — how many stocks / reports the wiki covers.
    Public read, safe to call from the page header on every load."""
    return await cache.get_wiki_stats()


def _build_qa_context(entries: list[dict]) -> str:
    """Render retrieved wiki entries into a compact markdown block for
    inclusion in the prompt. Each entry is tagged with its id so the
    model can cite; we also return short metadata for link rendering."""
    lines: list[str] = []
    for e in entries:
        firm = e.get("firm") or ""
        date_s = e.get("report_date") or (e.get("created_at") or "")[:10]
        rec = e.get("recommendation") or ""
        tp = e.get("target_price")
        tp_s = f"TP={int(tp):,}" if tp else ""
        header = f"- [{date_s}, {firm}] {e.get('title','')}"
        if rec or tp_s:
            header += f" ({rec} {tp_s})".strip()
        lines.append(header)
        key = (e.get("key_points_md") or e.get("summary_md") or "").strip()
        if key:
            # Indent body under the header bullet so the LLM treats it
            # as one chunk. Trim to prevent one long entry from dominating.
            body = key[:QA_CTX_PER_ENTRY_CHARS]
            for row in body.splitlines():
                lines.append(f"  {row}")
    return "\n".join(lines).strip()


async def _load_stock_summary(stock_code: str) -> str:
    """Pull the last 3 years of finance + market rows to give the model
    baseline numbers. Keeps the prompt grounded when wiki is thin."""
    from analyzer import analyze as _analyze
    db = await cache.get_db()
    fin_cur = await db.execute(
        """SELECT year, revenue, operating_profit, net_income,
                  total_equity, total_assets, total_liabilities, report_date
           FROM financial_data WHERE stock_code = ?
           ORDER BY year DESC LIMIT 3""",
        (stock_code,),
    )
    fin_rows = [dict(r) for r in await fin_cur.fetchall()]
    mkt_cur = await db.execute(
        """SELECT year, close_price, per, pbr, eps, bps,
                  dividend_per_share, dividend_yield, market_cap
           FROM market_data WHERE stock_code = ?
           ORDER BY year DESC LIMIT 3""",
        (stock_code,),
    )
    mkt_rows = [dict(r) for r in await mkt_cur.fetchall()]
    corp_name = await cache.get_corp_name(stock_code) or ""
    lines: list[str] = [f"{corp_name} ({stock_code})"]
    if fin_rows:
        lines.append("재무 (최근 3년, 단위=원):")
        for r in fin_rows:
            lines.append(
                f"- {r.get('year')}: 매출 {r.get('revenue')} / 영업 {r.get('operating_profit')} / 순이익 {r.get('net_income')}"
            )
    if mkt_rows:
        lines.append("밸류에이션 (최근 3년):")
        for r in mkt_rows:
            lines.append(
                f"- {r.get('year')}: PER {r.get('per')} / PBR {r.get('pbr')} / EPS {r.get('eps')} / 시총 {r.get('market_cap')}"
            )
    return "\n".join(lines)


QA_SYSTEM = "당신은 한국 주식 애널리스트입니다. 아래 맥락만으로 답하고, 근거가 부족하면 모른다고 답하세요. 각 주장 뒤 괄호로 (증권사, 발행일) 인용하세요."


@router.post("/api/analysis/{stock_code}/ask")
async def ask_stock(
    stock_code: str,
    request: Request,
    payload: dict = Body(default={}),
):
    """Streamed retrieval-grounded Q&A over a stock's wiki."""
    user = _require_user(await get_current_user(request))
    google_sub = user["google_sub"]

    question = (payload or {}).get("question") if isinstance(payload, dict) else None
    question = (question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="질문을 입력해 주세요.")
    if len(question) > QA_MAX_QUESTION_CHARS:
        question = question[:QA_MAX_QUESTION_CHARS]

    # Rate limit: 20/day per user (ENV-overridable).
    used = await cache.qa_count_since(google_sub, _today_kst_iso())
    if used >= QA_DAILY_LIMIT:
        raise HTTPException(
            status_code=429,
            detail=f"오늘의 질문 한도({QA_DAILY_LIMIT}회)를 모두 사용했습니다.",
        )

    # Optional admin model override via payload.
    req_model = payload.get("model") if isinstance(payload, dict) else None
    # Lazy import to keep wiki route import-light and avoid cycles.
    from routes import portfolio as pf_mod
    if not pf_mod._OPENROUTER_KEY:
        raise HTTPException(status_code=500, detail="AI API 키가 설정되지 않았습니다.")
    # Resolution order: admin payload override → WIKI_QA_MODEL env →
    # _AI_DEFAULT_MODEL (the reasoning model used by portfolio analysis).
    # Q&A is latency-sensitive for users, so a non-reasoning instruct
    # model (e.g. Gemma 4 31B) usually fits better than a thinking model.
    model = os.environ.get("WIKI_QA_MODEL") or pf_mod._AI_DEFAULT_MODEL
    if req_model and user.get("is_admin"):
        model = req_model

    # Retrieval: top-K by FTS, fall back to recency.
    entries = await cache.search_wiki(stock_code, question, limit=QA_CONTEXT_ENTRIES)
    source_ids = [e["id"] for e in entries]
    wiki_block = _build_qa_context(entries) if entries else "(요약된 리포트 없음)"
    stock_block = await _load_stock_summary(stock_code)

    prompt = (
        f"## 종목\n{stock_block}\n\n"
        f"## 최근 리포트 요약\n{wiki_block}\n\n"
        f"## 질문\n{question}\n\n"
        "답변은 한국어 마크다운. 3-6문단. 각 주장 뒤 괄호로 (증권사, 발행일) 형식으로 인용."
    )

    async def _stream():
        import json as _json
        answer_parts: list[str] = []
        input_tokens = 0
        output_tokens = 0
        cost = 0.0
        used_model = model
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=None)) as client:
            try:
                async with client.stream(
                    "POST",
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {pf_mod._OPENROUTER_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": QA_SYSTEM},
                            {"role": "user", "content": prompt},
                        ],
                        "max_tokens": 1500,
                        "stream": True,
                    },
                ) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        try:
                            err = _json.loads(body)
                            msg = err.get("error", {}).get("message", f"HTTP {resp.status_code}")
                        except Exception:
                            msg = f"HTTP {resp.status_code}"
                        yield f"data: {_json.dumps({'content': f'API 오류: {msg}'})}\n\n"
                        yield f"data: {_json.dumps({'done': True, 'sources': source_ids, 'model': model, 'input_tokens': 0, 'output_tokens': 0, 'cost': 0})}\n\n"
                        return

                    async for line in resp.aiter_lines():
                        if await request.is_disconnected():
                            logger.info("ask_stock: client disconnected")
                            return
                        if not line.startswith("data: "):
                            continue
                        data_s = line[6:]
                        if data_s.strip() == "[DONE]":
                            break
                        try:
                            chunk = _json.loads(data_s)
                        except Exception:
                            continue
                        if "error" in chunk:
                            yield f"data: {_json.dumps({'content': chunk['error'].get('message', 'Unknown error')})}\n\n"
                            break
                        delta = (chunk.get("choices") or [{}])[0].get("delta", {})
                        content = delta.get("content") or ""
                        if content:
                            answer_parts.append(content)
                            output_tokens += 1
                            yield f"data: {_json.dumps({'content': content})}\n\n"
                        usage = chunk.get("usage")
                        if usage:
                            input_tokens = usage.get("prompt_tokens", input_tokens)
                            output_tokens = usage.get("completion_tokens", output_tokens)
                            cost = usage.get("cost", cost) or cost
                            used_model = chunk.get("model") or used_model
            except Exception as exc:
                logger.exception("ask_stock stream error")
                yield f"data: {_json.dumps({'content': f'오류: {exc}'})}\n\n"
                yield f"data: {_json.dumps({'done': True, 'sources': source_ids, 'model': used_model, 'input_tokens': 0, 'output_tokens': 0, 'cost': 0})}\n\n"
                return

        answer = "".join(answer_parts).strip()
        # Persist for audit + rate limit accounting, even if empty.
        try:
            await cache.save_qa_entry({
                "google_sub": google_sub,
                "stock_code": stock_code,
                "question": question,
                "answer_md": answer,
                "source_ids": json.dumps(source_ids),
                "model": used_model,
                "tokens_in": input_tokens,
                "tokens_out": output_tokens,
                "cost_usd": float(cost) if cost else None,
                "created_at": datetime.now().isoformat(),
            })
        except Exception:
            logger.exception("qa history save failed")
        yield f"data: {_json.dumps({'done': True, 'sources': source_ids, 'model': used_model, 'input_tokens': input_tokens, 'output_tokens': output_tokens, 'cost': cost})}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")
