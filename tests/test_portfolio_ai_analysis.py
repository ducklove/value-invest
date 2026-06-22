"""Unit tests for services/portfolio/ai_analysis.py.

The AI portfolio-analysis domain logic (model selection, prompt assembly, the
streaming OpenRouter call and usage-ledger writes) was extracted from
routes/portfolio.py; these tests cover the pieces that previously had no
direct coverage. The end-to-end SSE behavior (route + service together) is
exercised by tests/test_portfolio_ai_with_wiki.py.
"""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from services.portfolio import ai_analysis

_PROFILES = {"fast": "m-fast", "balanced": "m-bal", "premium": "m-prem"}


# ---------------------------------------------------------------------------
# Model selection
# ---------------------------------------------------------------------------


async def test_resolve_model_profile_selection():
    with patch.object(ai_analysis, "model_profiles", new=AsyncMock(return_value=dict(_PROFILES))):
        assert await ai_analysis.resolve_model({}, {}) == ("m-bal", "balanced")
        assert await ai_analysis.resolve_model({"profile": "fast"}, {}) == ("m-fast", "fast")
        assert await ai_analysis.resolve_model({"mode": "premium"}, {}) == ("m-prem", "premium")
        # Profile is trimmed/lowered before lookup.
        assert await ai_analysis.resolve_model({"profile": " PREMIUM "}, {}) == ("m-prem", "premium")
        # Unknown profile falls back to balanced.
        assert await ai_analysis.resolve_model({"profile": "turbo"}, {}) == ("m-bal", "balanced")


async def test_resolve_model_custom_model_is_admin_only():
    with patch.object(ai_analysis, "model_profiles", new=AsyncMock(return_value=dict(_PROFILES))):
        assert await ai_analysis.resolve_model(
            {"model": "vendor/custom"}, {"is_admin": True}
        ) == ("vendor/custom", "custom")
        # Non-admins cannot override the profile model.
        assert await ai_analysis.resolve_model(
            {"model": "vendor/custom"}, {"is_admin": False}
        ) == ("m-bal", "balanced")
        # Admin with empty model string keeps the profile model.
        assert await ai_analysis.resolve_model(
            {"model": "  ", "profile": "fast"}, {"is_admin": True}
        ) == ("m-fast", "fast")


def test_env_model_profiles_has_all_profiles():
    profiles = ai_analysis.env_model_profiles()
    assert set(profiles) == {"fast", "balanced", "premium"}
    assert all(profiles.values())


async def test_list_models_without_key_returns_profiles_only():
    with patch.object(ai_analysis, "model_profiles", new=AsyncMock(return_value=dict(_PROFILES))), \
         patch.object(ai_analysis.ai_config, "get_openrouter_key", new=AsyncMock(return_value="")):
        assert await ai_analysis.list_models() == {
            "models": [],
            "default": "m-bal",
            "profiles": _PROFILES,
        }


# ---------------------------------------------------------------------------
# Prompt assembly
# ---------------------------------------------------------------------------


def test_fmt_krw_units_and_significant_digits():
    assert ai_analysis.fmt_krw(0) == "0원"
    assert ai_analysis.fmt_krw(50000) == "50,000원"
    assert ai_analysis.fmt_krw(5e8) == "5.000억"
    assert ai_analysis.fmt_krw(1.5e10) == "150.0억"
    assert ai_analysis.fmt_krw(2e11) == "2,000억"
    assert ai_analysis.fmt_krw(3e12) == "3.000조"
    assert ai_analysis.fmt_krw(2.5e13) == "25.00조"
    assert ai_analysis.fmt_krw(4e14) == "400.0조"
    assert ai_analysis.fmt_krw(7e15) == "7,000조"
    assert ai_analysis.fmt_krw(-5e8) == "-5.000억"


def test_holdings_summary_full_quote():
    lines, total = ai_analysis.holdings_summary([{
        "stock_code": "005930",
        "stock_name": "삼성전자",
        "quantity": 10,
        "avg_price": 70000,
        "quote": {"price": 80000, "change_pct": 1.5},
    }])
    assert lines == [
        "- 삼성전자 (005930): 수량=10, 매입가=70,000원, 현재가=80,000원, "
        "수익률=+14.3%, 일간=+1.50%, 평가=800,000원"
    ]
    assert total == 800000


def test_holdings_summary_missing_quote_keeps_line_without_value():
    lines, total = ai_analysis.holdings_summary([{
        "stock_code": "000001",
        "stock_name": "테스트",
        "quantity": 5,
        "avg_price": 1000,
        "quote": {},
    }])
    assert lines == ["- 테스트 (000001): 수량=5, 매입가=1,000원"]
    assert total == 0


async def test_market_summary_lines_use_readable_labels_and_direction_marks():
    payload = {
        "USD_KRW": {"value": "1,380.50", "change_pct": "0.12%", "direction": "down"},
        "US10Y": {"value": "4.25", "change_pct": "0.03%", "direction": "up"},
        "OIL_CL": {"value": "75.20", "change_pct": "+1.10%", "direction": ""},
    }
    with patch.object(ai_analysis.market_indicators, "fetch_indicators", new=AsyncMock(return_value=payload)):
        lines = await ai_analysis.market_summary_lines()

    assert lines == [
        "- 환율: 1,380.50 (▼0.12%)",
        "- 미국10년물: 4.25 (▲0.03%)",
        "- 유가: 75.20 (+1.10%)",
    ]


def test_performance_lines_short_history_nav_only():
    assert ai_analysis.performance_lines([]) == []
    lines = ai_analysis.performance_lines([
        {"date": "2026-01-01", "nav": 1000.0},
        {"date": "2026-03-01", "nav": 1100.0},
    ])
    assert lines == ["NAV: 1100.00 (2026-01-01~2026-03-01)"]


def test_performance_lines_long_history_adds_yoy_and_cagr():
    from datetime import date, timedelta
    start = date(2025, 1, 1)
    history = [
        {"date": (start + timedelta(days=i)).isoformat(), "nav": 1000.0 + i}
        for i in range(400)
    ]
    lines = ai_analysis.performance_lines(history)
    assert len(lines) == 3
    assert lines[0].startswith("NAV: 1399.00 (2025-01-01~")
    assert lines[1].startswith("YoY: +")
    assert lines[2].startswith("CAGR: +")


def test_build_prompt_sections_toggle():
    base = dict(
        holdings_lines=["- 삼성전자 (005930): 수량=10"],
        total_value=8e8,
        perf_lines=[],
        market_lines=["- KOSPI: 3000"],
        wiki_lines=[],
        user_query="",
    )
    prompt = ai_analysis.build_prompt(**base)
    assert "## 보유 종목 (총 평가: 8.000억)" in prompt
    assert "- 삼성전자 (005930): 수량=10" in prompt
    assert "## 성과\nN/A" in prompt
    assert "- KOSPI: 3000" in prompt
    assert "종목별 리서치 요약" not in prompt
    assert "사용자 질문/요청" not in prompt
    assert "HTML 태그는 쓰지 말고" in prompt

    prompt = ai_analysis.build_prompt(**{
        **base,
        "perf_lines": ["NAV: 1100.00"],
        "wiki_lines": ["### 삼성전자 (005930)", "- [삼성증권, 2026-03-10] HBM"],
        "user_query": "반도체 비중 어때?",
    })
    assert "## 성과\nNAV: 1100.00" in prompt
    assert "## 종목별 리서치 요약 (최근 증권사 리포트)\n### 삼성전자 (005930)" in prompt
    assert "## 사용자 질문/요청\n반도체 비중 어때?" in prompt


# ---------------------------------------------------------------------------
# prepare_analysis
# ---------------------------------------------------------------------------


async def test_prepare_analysis_requires_api_key():
    with patch.object(ai_analysis.ai_config, "get_openrouter_key", new=AsyncMock(return_value="")):
        with pytest.raises(ai_analysis.MissingAPIKeyError):
            await ai_analysis.prepare_analysis({}, {"google_sub": "u1"})


async def test_prepare_analysis_rejects_empty_portfolio():
    with patch.object(ai_analysis.ai_config, "get_openrouter_key", new=AsyncMock(return_value="k")), \
         patch.object(ai_analysis, "resolve_model", new=AsyncMock(return_value=("m", "balanced"))), \
         patch.object(portfolio_repo, "get_portfolio", new=AsyncMock(return_value=[])):
        with pytest.raises(ai_analysis.EmptyPortfolioError):
            await ai_analysis.prepare_analysis({}, {"google_sub": "u1"})


async def test_prepare_analysis_builds_context_and_clamps_query():
    items = [{"stock_code": "005930", "stock_name": "삼성전자", "quantity": 10, "avg_price": 70000}]
    enriched = [{**items[0], "quote": {"price": 80000, "change_pct": 1.5}}]
    long_query = "ㅁ" * 5000
    with patch.object(ai_analysis.ai_config, "get_openrouter_key", new=AsyncMock(return_value="k")), \
         patch.object(ai_analysis, "resolve_model", new=AsyncMock(return_value=("m-bal", "balanced"))), \
         patch.object(portfolio_repo, "get_portfolio", new=AsyncMock(return_value=items)), \
         patch.object(ai_analysis.quote_service, "enrich_with_cached_quotes", new=AsyncMock(return_value=enriched)), \
         patch.object(snapshots_repo, "get_nav_history", new=AsyncMock(return_value=[])), \
         patch.object(ai_analysis, "market_summary_lines", new=AsyncMock(return_value=["- KOSPI: 3000"])), \
         patch.object(ai_analysis, "wiki_research_lines", new=AsyncMock(return_value=(["### 삼성전자 (005930)"], 1))):
        ctx = await ai_analysis.prepare_analysis({"query": long_query}, {"google_sub": "u1"})
    assert (ctx.model, ctx.model_profile) == ("m-bal", "balanced")
    assert ctx.google_sub == "u1"
    assert ctx.openrouter_key == "k"
    assert ctx.wiki_used_count == 1
    assert "삼성전자 (005930)" in ctx.prompt
    # User query is hard-capped at USER_QUERY_MAX_CHARS.
    assert "ㅁ" * ai_analysis.USER_QUERY_MAX_CHARS in ctx.prompt
    assert "ㅁ" * (ai_analysis.USER_QUERY_MAX_CHARS + 1) not in ctx.prompt


# ---------------------------------------------------------------------------
# stream_analysis
# ---------------------------------------------------------------------------


def _ctx() -> ai_analysis.AnalysisContext:
    return ai_analysis.AnalysisContext(
        model="m-bal",
        model_profile="balanced",
        prompt="프롬프트",
        wiki_used_count=2,
        openrouter_key="k",
        google_sub="u1",
        started_at=0.0,
    )


def _fake_stream(resp):
    @asynccontextmanager
    async def fake(client, payload, *, openrouter_key=None):
        fake.captured_payload = payload
        yield resp
    return fake


class _FakeResp:
    def __init__(self, status_code: int, lines: list[str] | None = None, body: bytes = b""):
        self.status_code = status_code
        self._lines = lines or []
        self._body = body

    async def aread(self):
        return self._body

    async def aiter_lines(self):
        for line in self._lines:
            yield line


async def test_stream_analysis_success_events_and_usage():
    resp = _FakeResp(200, lines=[
        'data: {"choices":[{"delta":{"content":"안녕"}}]}',
        "",
        'data: {"choices":[{"delta":{}}],"usage":{"prompt_tokens":100,"completion_tokens":20,"cost":0.01}}',
        "data: [DONE]",
    ])
    fake = _fake_stream(resp)
    record = AsyncMock()
    with patch.object(ai_analysis.ai_client, "stream_chat_completion", fake), \
         patch.object(ai_analysis.ai_config, "record_usage", record):
        events = [e async for e in ai_analysis.stream_analysis(
            _ctx(), is_disconnected=AsyncMock(return_value=False),
        )]

    assert events[0] == {"content": "안녕"}
    done = events[-1]
    assert done["done"] is True
    assert (done["input_tokens"], done["output_tokens"], done["cost"]) == (100, 20, 0.01)
    assert done["model"] == "m-bal"
    assert done["model_profile"] == "balanced"
    assert done["truncated"] is False
    assert done["finish_reason"] is None
    assert done["max_tokens"] == ai_analysis.AI_MAX_TOKENS
    assert done["wiki_used"] == 2
    assert done["context_holdings"] == ai_analysis.WIKI_HOLDING_LIMIT
    assert done["context_reports_per_holding"] == ai_analysis.WIKI_ENTRY_LIMIT
    record.assert_awaited_once()
    kwargs = record.await_args.kwargs
    assert kwargs["ok"] is True
    assert kwargs["feature"] == "portfolio_analysis"
    assert (kwargs["input_tokens"], kwargs["output_tokens"], kwargs["cost_usd"]) == (100, 20, 0.01)
    # The upstream request is assembled from the context + module config.
    payload = fake.captured_payload
    assert payload["model"] == "m-bal"
    assert payload["stream"] is True
    assert payload["max_tokens"] == ai_analysis.AI_MAX_TOKENS
    assert payload["messages"][0] == {"role": "system", "content": ai_analysis.SYSTEM_PROMPT}
    assert payload["messages"][1] == {"role": "user", "content": "프롬프트"}


async def test_stream_analysis_marks_length_finish_as_truncated():
    resp = _FakeResp(200, lines=[
        'data: {"choices":[{"delta":{"content":"본문"}}]}',
        'data: {"choices":[{"delta":{},"finish_reason":"length"}],"usage":{"prompt_tokens":100,"completion_tokens":8000,"cost":0.02}}',
        "data: [DONE]",
    ])
    record = AsyncMock()
    with patch.object(ai_analysis.ai_client, "stream_chat_completion", _fake_stream(resp)), \
         patch.object(ai_analysis.ai_config, "record_usage", record):
        events = [e async for e in ai_analysis.stream_analysis(
            _ctx(), is_disconnected=AsyncMock(return_value=False),
        )]

    assert events[0] == {"content": "본문"}
    assert events[1] == {"content": ai_analysis.TRUNCATION_NOTICE}
    done = events[-1]
    assert done["done"] is True
    assert done["truncated"] is True
    assert done["finish_reason"] == "length"
    assert done["max_tokens"] == ai_analysis.AI_MAX_TOKENS
    kwargs = record.await_args.kwargs
    assert kwargs["ok"] is False
    assert kwargs["error"] == "finish_reason_length"


async def test_stream_analysis_http_error_yields_error_and_done():
    resp = _FakeResp(400, body=b'{"error": {"message": "boom"}}')
    record = AsyncMock()
    with patch.object(ai_analysis.ai_client, "stream_chat_completion", _fake_stream(resp)), \
         patch.object(ai_analysis.ai_config, "record_usage", record):
        events = [e async for e in ai_analysis.stream_analysis(
            _ctx(), is_disconnected=AsyncMock(return_value=False),
        )]

    assert events[0] == {"content": "API 오류: boom"}
    done = events[1]
    assert done["done"] is True
    assert (done["input_tokens"], done["output_tokens"], done["cost"]) == (0, 0, 0)
    record.assert_awaited_once()
    kwargs = record.await_args.kwargs
    assert kwargs["ok"] is False
    assert kwargs["error"] == "boom"


async def test_stream_analysis_stops_on_client_disconnect():
    resp = _FakeResp(200, lines=['data: {"choices":[{"delta":{"content":"안녕"}}]}'])
    record = AsyncMock()
    with patch.object(ai_analysis.ai_client, "stream_chat_completion", _fake_stream(resp)), \
         patch.object(ai_analysis.ai_config, "record_usage", record):
        events = [e async for e in ai_analysis.stream_analysis(
            _ctx(), is_disconnected=AsyncMock(return_value=True),
        )]

    # Aborted before any event was emitted and without a usage write.
    assert events == []
    record.assert_not_awaited()
