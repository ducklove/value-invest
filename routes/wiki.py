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
# Hard cap on key_points length per entry in the prompt — keeps total
# prompt bounded regardless of how verbose any single summary is. The
# actual number of entries retrieved is sized by _classify_question().
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


def _fmt_krw(v) -> str:
    """Format KRW with 조/억 when large. None-safe."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "N/A"
    av = abs(v)
    if av >= 1e12:
        return f"{v / 1e12:,.2f}조"
    if av >= 1e8:
        return f"{v / 1e8:,.1f}억"
    return f"{v:,.0f}"


def _yoy_pct(series: list[dict], key: str) -> str:
    """Given rows sorted DESC by year, compute the most recent YoY %."""
    if len(series) < 2:
        return ""
    latest, prev = series[0], series[1]
    try:
        a = float(latest.get(key))
        b = float(prev.get(key))
        if b == 0:
            return ""
        pct = (a - b) / abs(b) * 100
        return f" (YoY {pct:+.1f}%)"
    except (TypeError, ValueError):
        return ""


async def _load_stock_summary(stock_code: str) -> str:
    """Pull a comprehensive, current snapshot for prompt grounding.

    Uses the analysis DB rows plus a live quote fetch. Kept small enough
    (~600 tokens for a typical stock) so it plays nicely with the wiki
    context block."""
    db = await cache.get_db()

    # 5-year history (was 3 — extra two years help the model reason about
    # trends and cyclicality without exploding prompt size).
    fin_cur = await db.execute(
        """SELECT year, revenue, operating_profit, net_income,
                  total_equity, total_assets, total_liabilities
           FROM financial_data WHERE stock_code = ?
           ORDER BY year DESC LIMIT 5""",
        (stock_code,),
    )
    fin_rows = [dict(r) for r in await fin_cur.fetchall()]
    mkt_cur = await db.execute(
        """SELECT year, close_price, per, pbr, eps, bps,
                  dividend_per_share, dividend_yield, market_cap
           FROM market_data WHERE stock_code = ?
           ORDER BY year DESC LIMIT 5""",
        (stock_code,),
    )
    mkt_rows = [dict(r) for r in await mkt_cur.fetchall()]
    corp_name = await cache.get_corp_name(stock_code) or ""

    # Live quote — uses the warm quote cache populated by portfolio/WS.
    live_line = ""
    try:
        from routes.portfolio import _fetch_quote
        q = await _fetch_quote(stock_code)
        if q and q.get("price"):
            price = q.get("price")
            chg_pct = q.get("change_pct")
            chg = q.get("change")
            bits = [f"현재가 {price:,.0f}원"]
            if chg_pct is not None:
                bits.append(f"일간 {chg_pct:+.2f}%")
            if chg is not None:
                bits.append(f"{chg:+,.0f}원")
            live_line = " / ".join(bits)
    except Exception:
        pass

    lines: list[str] = [f"{corp_name} ({stock_code})"]
    if live_line:
        lines.append(f"실시간: {live_line}")

    if fin_rows:
        lines.append(f"재무 (최근 {len(fin_rows)}년, 매출/영업이익/순이익, ROE):")
        for i, r in enumerate(fin_rows):
            # Basic derived metrics.
            rev = r.get("revenue")
            op = r.get("operating_profit")
            ni = r.get("net_income")
            eq = r.get("total_equity")
            roe = (ni / eq * 100) if ni and eq and eq > 0 else None
            op_margin = (op / rev * 100) if op is not None and rev and rev > 0 else None
            yoy = _yoy_pct(fin_rows, "revenue") if i == 0 else ""
            lines.append(
                f"- {r.get('year')}: 매출 {_fmt_krw(rev)}{yoy} / "
                f"영업 {_fmt_krw(op)} (영업이익률 {op_margin:.1f}%)" if op_margin is not None
                else f"- {r.get('year')}: 매출 {_fmt_krw(rev)}{yoy} / 영업 {_fmt_krw(op)}"
            )
            tail = f"  순이익 {_fmt_krw(ni)}"
            if roe is not None:
                tail += f" / ROE {roe:.1f}%"
            lines.append(tail)

    if mkt_rows:
        lines.append(f"밸류에이션 (최근 {len(mkt_rows)}년, 연말 기준):")
        for r in mkt_rows:
            per = r.get("per")
            pbr = r.get("pbr")
            eps = r.get("eps")
            dy = r.get("dividend_yield")
            mc = r.get("market_cap")
            parts = [f"{r.get('year')}:"]
            if per is not None: parts.append(f"PER {per:.1f}")
            if pbr is not None: parts.append(f"PBR {pbr:.2f}")
            if eps is not None: parts.append(f"EPS {eps:,.0f}")
            if dy is not None: parts.append(f"배당수익률 {dy:.2f}%")
            if mc: parts.append(f"시총 {_fmt_krw(mc)}")
            lines.append("- " + " / ".join(parts))

    return "\n".join(lines)


async def _load_macro_context() -> str:
    """Current market backdrop. Uses market_indicators which already has
    a 60s module-level cache so this is cheap."""
    try:
        import market_indicators
        data = await market_indicators.fetch_indicators([
            "KOSPI", "KOSDAQ", "USD_KRW", "US10Y", "SPX",
        ])
    except Exception:
        return ""
    if not data:
        return ""
    lines: list[str] = ["시장 지표 (현재):"]
    for code, v in data.items():
        if not v:
            continue
        val = v.get("value")
        direction = v.get("direction", "")
        chg = v.get("change_pct")
        tail = f" ({direction}{chg})" if chg else ""
        lines.append(f"- {code}: {val}{tail}")
    return "\n".join(lines) if len(lines) > 1 else ""


# Naver news scrape for recent headlines. Cheap fallback for "뉴스에
# 뭐 나왔어?" / "최근 어떻게 움직였어?" type questions.
_news_cache: dict[str, tuple[float, list[dict]]] = {}
_NEWS_CACHE_TTL = 600  # 10 min


async def _fetch_recent_news(stock_code: str, limit: int = 6) -> list[dict]:
    """Scrape Naver finance news list for the stock code.
    Returns list of {date, title, outlet}. Best-effort; returns [] on
    any failure."""
    import time as _time
    cached = _news_cache.get(stock_code)
    if cached and (_time.monotonic() - cached[0]) < _NEWS_CACHE_TTL:
        return cached[1][:limit]
    try:
        import httpx
        import re as _re
        url = f"https://finance.naver.com/item/news_news.naver?code={stock_code}&page=1"
        async with httpx.AsyncClient(timeout=8.0, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://finance.naver.com/",
        }) as client:
            resp = await client.get(url)
        html = resp.content.decode("euc-kr", errors="ignore")
        # Rows look like: <td class="title"><a ...>제목</a></td>
        # <td class="info">언론사</td><td class="date">2026.04.17 09:30</td>
        # Naver changes layouts occasionally; fall back to empty on mismatch.
        rows = _re.findall(
            r'<td class="title">.*?<a[^>]*>(.+?)</a>.*?'
            r'<td class="info">(.+?)</td>.*?<td class="date">([^<]+)</td>',
            html, _re.DOTALL,
        )
        news = []
        for title, outlet, date in rows[:limit]:
            # Strip remaining tags/entities.
            clean = lambda s: _re.sub(r"\s+", " ", _re.sub(r"<[^>]+>", "", s)).strip()
            news.append({
                "title": clean(title),
                "outlet": clean(outlet),
                "date": clean(date),
            })
        _news_cache[stock_code] = (_time.monotonic(), news)
        return news
    except Exception:
        return []


def _format_news_block(news: list[dict]) -> str:
    if not news:
        return ""
    lines = ["최근 뉴스 제목 (네이버 금융, 최신순):"]
    for n in news:
        lines.append(f"- [{n.get('date','')}, {n.get('outlet','')}] {n.get('title','')}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tier 0: rule-based shortcuts — answer simple factual questions directly
# from the DB / live quote, skipping the LLM entirely.
#
# Why: "현재가 얼마?" doesn't need retrieval, doesn't need reasoning, and
# doesn't need to count toward the daily LLM budget. Zero-latency
# deterministic answers beat a 2-second LLM round-trip for the subset of
# questions that reduce to a single number in our DB.
# ---------------------------------------------------------------------------


async def _try_shortcut(stock_code: str, question: str) -> str | None:
    """Return a formatted answer string if the question maps to a known
    single-field lookup; otherwise None so the caller falls through to
    the LLM path."""
    import re as _re
    q = question.strip().lower()
    # Strip punctuation / whitespace to make matching resilient to "PER은?"
    # vs "PER 몇 배?" etc.
    q_norm = _re.sub(r"[\s\?\!\.\,\~]+", "", q)

    def matches(*needles: str) -> bool:
        return any(n in q_norm for n in needles)

    # Pre-fetch common data lazily (only when we're about to match).
    corp_name = await cache.get_corp_name(stock_code) or stock_code
    db = await cache.get_db()
    mkt_cur = await db.execute(
        """SELECT year, close_price, per, pbr, eps, bps,
                  dividend_per_share, dividend_yield, market_cap
           FROM market_data WHERE stock_code = ?
           ORDER BY year DESC LIMIT 1""",
        (stock_code,),
    )
    mkt_row = await mkt_cur.fetchone()
    mkt = dict(mkt_row) if mkt_row else {}

    # Live quote via warm cache; may be None if market isn't trading yet
    # or the quote provider failed — in that case skip quote-dependent
    # shortcuts and let the LLM handle it with whatever context we have.
    quote = {}
    try:
        from routes.portfolio import _fetch_quote
        quote = await _fetch_quote(stock_code) or {}
    except Exception:
        pass

    price = quote.get("price")
    chg_pct = quote.get("change_pct")
    chg = quote.get("change")

    # --- Current price / quote ---
    if matches("현재가", "주가얼마", "주가는", "시세", "지금얼마") and price is not None:
        bits = [f"**{corp_name}** 현재가는 **{price:,.0f}원**입니다."]
        if chg_pct is not None:
            sign = "상승" if chg_pct > 0 else ("하락" if chg_pct < 0 else "보합")
            delta = f" ({chg:+,.0f}원)" if chg is not None else ""
            bits.append(f"오늘 {chg_pct:+.2f}%{delta} {sign} 중이에요.")
        return " ".join(bits)

    # --- Market cap ---
    if matches("시가총액", "시총"):
        mc = mkt.get("market_cap")
        if mc:
            return f"**{corp_name}**의 최근 연말 기준 시가총액은 **{_fmt_krw(mc)}원**이에요. (가장 최근 연간 마감 기준)"

    # --- PER / PBR / EPS / BPS ---
    if matches("per몇", "per은", "per이"):
        per = mkt.get("per")
        if per is not None:
            return f"**{corp_name}**의 최근 연말 기준 **PER**은 **{per:.1f}배**입니다."

    if matches("pbr몇", "pbr은", "pbr이"):
        pbr = mkt.get("pbr")
        if pbr is not None:
            return f"**{corp_name}**의 최근 연말 기준 **PBR**은 **{pbr:.2f}배**입니다."

    if matches("eps얼마", "eps는", "주당순이익"):
        eps = mkt.get("eps")
        if eps is not None:
            return f"**{corp_name}**의 최근 **EPS(주당순이익)**는 **{eps:,.0f}원**입니다."

    if matches("bps얼마", "bps는", "주당순자산"):
        bps = mkt.get("bps")
        if bps is not None:
            return f"**{corp_name}**의 최근 **BPS(주당순자산)**는 **{bps:,.0f}원**입니다."

    # --- Dividend ---
    if matches("배당수익률", "배당률"):
        dy = mkt.get("dividend_yield")
        if dy is not None:
            return f"**{corp_name}**의 최근 **배당수익률**은 **{dy:.2f}%**입니다. (연말 기준)"
    if matches("주당배당금", "배당금얼마"):
        dps = mkt.get("dividend_per_share")
        if dps is not None:
            return f"**{corp_name}**의 최근 **주당배당금**은 **{dps:,.0f}원**입니다."

    return None


# ---------------------------------------------------------------------------
# Tier 1: adaptive context — sizing data blocks by question complexity.
# ---------------------------------------------------------------------------

# Words that signal the user wants deep analysis. Presence of any of these
# (or a long question body) → give the model full context.
_DEEP_KEYWORDS = (
    "분석", "전망", "전망은", "밸류에이션", "dcf", "리스크", "평가",
    "추천", "의견", "어때", "어떻게봐", "매수", "매도", "투자",
    "왜", "이유", "비교",
)
# Words that signal a news-centric question. If absent and question is
# shallow, we skip the news scrape.
_NEWS_KEYWORDS = ("뉴스", "이슈", "소식", "최근", "발표")
# Words that signal broader market context matters.
_MACRO_KEYWORDS = ("시장", "증시", "금리", "환율", "매크로", "경기", "업황")


def _classify_question(question: str) -> dict:
    """Return a small dict describing the retrieval budget for a question.

    {wiki_limit: int, include_macro: bool, include_news: bool, is_deep: bool}.
    """
    q = question.lower()
    length = len(question)
    is_deep = length > 100 or any(kw in q for kw in _DEEP_KEYWORDS)
    wants_news = any(kw in q for kw in _NEWS_KEYWORDS)
    wants_macro = any(kw in q for kw in _MACRO_KEYWORDS)
    return {
        "is_deep": is_deep,
        "wiki_limit": 8 if is_deep else 3,
        "include_news": wants_news or is_deep,
        "include_macro": wants_macro or is_deep,
    }


QA_SYSTEM = """당신은 한국 주식 애널리스트입니다. 아래 제공된 맥락을 우선 근거로 사용하되,
필요하면 일반적인 금융 지식과 시장 상식을 활용해 유연하게 답하세요.

답변 가이드:
- 증권사 리포트 요약에 근거가 있으면 (증권사, 발행일) 형식으로 인용하세요.
- 재무·밸류에이션 숫자는 표로 제공된 값을 그대로 인용하세요.
- 일반 지식이나 시장 상식은 "일반적으로…" 같은 표현으로 명시하고 과도한 단정은 피하세요.
- 최근 뉴스 제목은 방향만 참고하되, 제목만으로 단정 지어 판단하지 마세요.
- 맥락과 일반 지식을 결합할 때는 두 관점을 구분해서 보여주세요.
- 근거가 충분하지 않은 예측이나 가격 타겟은 제시하지 마세요.
"""


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

    # --- Tier 0: rule-based shortcut ---
    # Try to answer trivially factual questions (current price, PER, etc.)
    # directly from the DB / warm quote cache. Shortcuts don't count
    # toward the daily LLM budget and return immediately without a
    # round-trip. If no pattern matches we fall through to the full
    # LLM path below.
    shortcut_answer = await _try_shortcut(stock_code, question)
    if shortcut_answer is not None:
        import json as _json

        async def _stream_shortcut():
            # Deliver the answer in a single SSE chunk so the existing
            # frontend (which expects data: {content:...} + data: {done:true})
            # doesn't need any changes.
            yield f"data: {_json.dumps({'content': shortcut_answer})}\n\n"
            yield f"data: {_json.dumps({'done': True, 'sources': [], 'model': 'shortcut', 'input_tokens': 0, 'output_tokens': 0, 'cost': 0})}\n\n"

        return StreamingResponse(_stream_shortcut(), media_type="text/event-stream")

    # --- Tier 1: classify question to size down context for simple asks ---
    plan = _classify_question(question)

    # Rate limit: 20/day per user (ENV-overridable). Only applies to
    # actual LLM calls; shortcuts are free.
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

    # Retrieval sized by question complexity. Shallow questions skip
    # big wiki chunks; deep ones get TOP-8 with FTS + recency fallback.
    entries = await cache.search_wiki(stock_code, question, limit=plan["wiki_limit"])
    source_ids = [e["id"] for e in entries]
    wiki_block = _build_qa_context(entries) if entries else "(요약된 리포트 없음)"
    stock_block = await _load_stock_summary(stock_code)

    # Macro / news only when the question signals they're relevant, or
    # on deep questions. Saves ~600-1200 tokens for the common shallow
    # query and avoids a Naver HTTP round-trip on the hot path.
    macro_block = await _load_macro_context() if plan["include_macro"] else ""
    if plan["include_news"]:
        news_items = await _fetch_recent_news(stock_code, limit=6)
        news_block = _format_news_block(news_items)
    else:
        news_block = ""

    prompt_sections = [f"## 종목\n{stock_block}"]
    if macro_block:
        prompt_sections.append(f"## 시장 상황\n{macro_block}")
    if news_block:
        prompt_sections.append(f"## 최근 뉴스\n{news_block}")
    prompt_sections.append(f"## 증권사 리포트 요약\n{wiki_block}")
    prompt_sections.append(
        f"## 질문\n{question}\n\n"
        "답변은 한국어 마크다운, 3-6문단. 섹션 헤더 없이 자연스럽게 서술하세요. "
        "숫자를 인용할 때는 단위(원·%·배)를 명확히 쓰고, 증권사 리포트를 인용할 때는 "
        "(증권사, 발행일) 형식으로 표기하세요. 일반적인 금융 지식도 필요하면 활용하되, "
        "그때는 '일반적으로' 같은 표현으로 구분해 주세요."
    )
    prompt = "\n\n".join(prompt_sections)

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
