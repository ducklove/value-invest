"""AI review pipeline for the latest DART periodic filing."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import warnings
import zipfile
from datetime import date, datetime, timedelta
from html import unescape
from typing import Any

import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

import ai_config
import dart_client
import observability
from cache_layer import MemoryTTLCache
from core.http import get_http_client
from repositories import corp_codes
from repositories import dart_review as dart_review_repo
from repositories import financial as financial_repo
from repositories import wiki as wiki_repo
from services import ai_client

logger = logging.getLogger(__name__)

DART_VIEWER_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
PERIODIC_REPORT_KEYWORDS = ("분기보고서", "반기보고서", "사업보고서")
PRIMARY_TEXT_CHARS = int(os.getenv("DART_REVIEW_PRIMARY_CHARS", "42000"))
COMPARISON_TEXT_CHARS = int(os.getenv("DART_REVIEW_COMPARISON_CHARS", "9000"))
COMPARISON_REPORT_COUNT = int(os.getenv("DART_REVIEW_COMPARISON_REPORTS", "2"))
REPORT_LOOKBACK_DAYS = int(os.getenv("DART_REVIEW_LOOKBACK_DAYS", "1100"))
DART_FILINGS_CACHE_TTL_S = int(os.getenv("DART_REVIEW_FILINGS_CACHE_TTL_S", "600"))
DART_REVIEW_TARGET_LIMIT = int(os.getenv("DART_REVIEW_TARGET_LIMIT", "12"))
# Output budget for the review JSON. The summary_md (markdown tables + bars),
# cards, watch_items and comparison_notes routinely exceed a small cap; when the
# model is cut off mid-JSON the parser falls back to dumping the raw blob into
# summary_md, which the UI then renders as garbled markdown. A generous default
# plus a larger one-shot retry keeps that failure mode rare.
DART_REVIEW_MAX_TOKENS = int(os.getenv("DART_REVIEW_MAX_TOKENS", "4096"))
DART_REVIEW_MAX_TOKENS_RETRY = int(os.getenv("DART_REVIEW_MAX_TOKENS_RETRY", "8000"))

_filings_cache = MemoryTTLCache("dart_report_review.filings", DART_FILINGS_CACHE_TTL_S)
_pipeline_lock = asyncio.Lock()


class DartReportReviewError(RuntimeError):
    """Recoverable user-facing error for report review generation."""


def _dart_key() -> str:
    dart_client.load_api_key()
    return dart_client.API_KEY


def _iso_date(value: str | None) -> str:
    raw = (value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def _report_kind(report_name: str) -> str:
    for keyword in PERIODIC_REPORT_KEYWORDS:
        if keyword in (report_name or ""):
            return keyword
    return "정기보고서"


def _period_from_name(report_name: str) -> str:
    match = re.search(r"\((\d{4})\.(\d{2})\)", report_name or "")
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    match = re.search(r"\((\d{4})\)", report_name or "")
    if match:
        return match.group(1)
    return ""


def _normalize_filing(item: dict[str, Any]) -> dict[str, Any]:
    rcept_no = str(item.get("rcept_no") or "").strip()
    report_name = str(item.get("report_nm") or "").strip()
    return {
        "rcept_no": rcept_no,
        "report_name": report_name,
        "report_date": _iso_date(item.get("rcept_dt")),
        "kind": _report_kind(report_name),
        "period": _period_from_name(report_name),
        "corp_name": item.get("corp_name") or "",
        "viewer_url": DART_VIEWER_URL.format(rcept_no=rcept_no) if rcept_no else "",
    }


async def fetch_periodic_filings(corp_code: str, *, limit: int = 8) -> list[dict[str, Any]]:
    """Return recent quarterly/half/annual filings from OpenDART."""
    cache_key = corp_code.strip()
    cached = _filings_cache.get_entry(cache_key)
    if cached is not None:
        return [dict(item) for item in cached.value[:limit]]

    api_key = _dart_key()
    if not api_key:
        raise DartReportReviewError("OPENDART_API_KEY가 설정되어 있지 않습니다.")

    end = date.today()
    start = end - timedelta(days=REPORT_LOOKBACK_DAYS)
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bgn_de": start.strftime("%Y%m%d"),
        "end_de": end.strftime("%Y%m%d"),
        "last_reprt_at": "Y",
        "pblntf_ty": "A",
        "sort": "date",
        "sort_mth": "desc",
        "page_no": "1",
        "page_count": "100",
    }
    client = await get_http_client("dart")
    resp = await client.get(f"{dart_client.BASE_URL}/list.json", params=params, timeout=20.0)
    resp.raise_for_status()
    data = resp.json()
    status = str(data.get("status") or "")
    if status != "000":
        if status in {"013", "014"}:
            _filings_cache.set(cache_key, [])
            return []
        raise DartReportReviewError(data.get("message") or f"DART 공시검색 실패({status})")

    filings: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in data.get("list") or []:
        report_name = str(item.get("report_nm") or "")
        if not any(keyword in report_name for keyword in PERIODIC_REPORT_KEYWORDS):
            continue
        filing = _normalize_filing(item)
        if not filing["rcept_no"] or filing["rcept_no"] in seen:
            continue
        seen.add(filing["rcept_no"])
        filings.append(filing)
        if len(filings) >= limit:
            break
    _filings_cache.set(cache_key, [dict(item) for item in filings])
    return filings


def _decode_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _clean_report_text(text: str) -> str:
    if not text:
        return ""
    text = unescape(text)
    if "<" in text and ">" in text:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
            soup = BeautifulSoup(text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text("\n")
    text = text.replace("\xa0", " ").replace("\u3000", " ")
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line:
            continue
        if len(line) <= 2 and re.fullmatch(r"[\W_]+", line):
            continue
        lines.append(line)
    return "\n".join(lines)


def _zip_document_text(content: bytes) -> str:
    texts: list[str] = []
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = [
            name for name in zf.namelist()
            if name.lower().endswith((".xml", ".html", ".htm", ".txt"))
        ]
        for name in names:
            try:
                texts.append(_clean_report_text(_decode_bytes(zf.read(name))))
            except Exception as exc:
                logger.warning("DART document member parse failed: %s (%s)", name, exc)
    return "\n\n".join(part for part in texts if part)


def _dart_error_message(content: bytes) -> str:
    text = _decode_bytes(content)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(text, "html.parser")
    message = soup.find("message")
    status = soup.find("status")
    if message:
        return message.get_text(" ", strip=True)
    if status:
        return f"DART document error {status.get_text(' ', strip=True)}"
    return "DART 원문 파일을 해석하지 못했습니다."


async def fetch_document_text(rcept_no: str) -> str:
    api_key = _dart_key()
    if not api_key:
        raise DartReportReviewError("OPENDART_API_KEY가 설정되어 있지 않습니다.")
    client = await get_http_client("dart")
    resp = await client.get(
        f"{dart_client.BASE_URL}/document.xml",
        params={"crtfc_key": api_key, "rcept_no": rcept_no},
        timeout=45.0,
    )
    resp.raise_for_status()
    content = resp.content
    if not zipfile.is_zipfile(io.BytesIO(content)):
        raise DartReportReviewError(_dart_error_message(content))
    text = _zip_document_text(content)
    if len(text) < 500:
        raise DartReportReviewError("DART 원문 텍스트가 너무 짧아 리뷰를 생성할 수 없습니다.")
    return text


def _focus_snippets(text: str, *, limit: int) -> str:
    keywords = (
        "사업의 내용", "사업의 개요", "주요 제품", "매출", "수주",
        "재무에 관한 사항", "연결재무제표", "현금흐름", "차입", "재고",
        "위험관리", "우발", "소송", "특수관계자", "배당",
    )
    snippets: list[str] = []
    used_ranges: list[tuple[int, int]] = []
    for keyword in keywords:
        pos = text.find(keyword)
        if pos < 0:
            continue
        start = max(0, pos - 400)
        end = min(len(text), pos + 1800)
        if any(start <= prev_end and end >= prev_start for prev_start, prev_end in used_ranges):
            continue
        used_ranges.append((start, end))
        snippets.append(text[start:end])
        if sum(len(s) for s in snippets) >= limit:
            break
    if not snippets:
        return text[:limit]
    return "\n\n---\n\n".join(snippets)[:limit]


async def _financial_context(stock_code: str) -> str:
    fin_rows = await financial_repo.get_financial_data(stock_code)
    mkt_rows = await financial_repo.get_market_data(stock_code)
    if not fin_rows and not mkt_rows:
        return ""

    def fmt_num(value: Any) -> str:
        try:
            v = float(value)
        except (TypeError, ValueError):
            return "-"
        if abs(v) >= 1e12:
            return f"{v / 1e12:.2f}조"
        if abs(v) >= 1e8:
            return f"{v / 1e8:.1f}억"
        return f"{v:,.0f}"

    lines = ["기존 분석 DB의 보조 재무/밸류에이션 맥락:"]
    for row in sorted(fin_rows, key=lambda r: r.get("year") or 0, reverse=True)[:5]:
        lines.append(
            "- {year}: 매출 {revenue} / 영업이익 {op} / 순이익 {ni} / 자본 {equity}".format(
                year=row.get("year"),
                revenue=fmt_num(row.get("revenue")),
                op=fmt_num(row.get("operating_profit")),
                ni=fmt_num(row.get("net_income")),
                equity=fmt_num(row.get("total_equity")),
            )
        )
    if mkt_rows:
        latest = sorted(mkt_rows, key=lambda r: r.get("year") or 0, reverse=True)[0]
        lines.append(
            "- 최근 시장지표: PER {per}, PBR {pbr}, 배당수익률 {dy}, 시총 {mc}".format(
                per=latest.get("per") if latest.get("per") is not None else "-",
                pbr=latest.get("pbr") if latest.get("pbr") is not None else "-",
                dy=latest.get("dividend_yield") if latest.get("dividend_yield") is not None else "-",
                mc=fmt_num(latest.get("market_cap")),
            )
        )
    return "\n".join(lines)


def _build_prompt(
    *,
    stock_code: str,
    corp_name: str,
    primary: dict[str, Any],
    primary_text: str,
    comparisons: list[dict[str, Any]],
    financial_context: str,
) -> str:
    comparison_blocks: list[str] = []
    for item in comparisons:
        comparison_blocks.append(
            "### 비교 보고서: {name} ({date})\n{snippet}".format(
                name=item["report_name"],
                date=item.get("report_date") or "",
                snippet=item.get("snippet") or "",
            )
        )
    comparison_text = "\n\n".join(comparison_blocks) or "비교 보고서 텍스트 없음"

    return f"""
아래 DART 정기보고서를 투자자가 빠르게 읽을 수 있는 형태로 재구성해 주세요.

대상: {corp_name} ({stock_code})
주 보고서: {primary['report_name']} / 접수일 {primary.get('report_date') or ''}
원문 링크: {primary.get('viewer_url') or ''}

요구사항:
- 주 보고서가 핵심 재료입니다. 비교 보고서는 변화/반복 리스크를 확인하는 보조 근거로만 쓰세요.
- 근거가 없는 추정은 금지하고, 불확실한 경우 "보고서에서 명확히 확인되지 않음"이라고 쓰세요.
- 투자 판단에 유용한 순서로 재구성하세요: 한눈 요약, 실적/수익성, 재무안정성/현금흐름, 사업부/제품, 리스크, 다음 체크포인트.
- 신호등 카드와 체크리스트, 표처럼 웹에서 바로 렌더링하기 좋은 형태로 만드세요.
- 핵심 지표의 직전 대비 변화는 summary_md에 텍스트 막대(█ 등)로 그리지 말고, 반드시
  별도의 metric_trends 배열로만 제공하세요. 화면에서 막대 그래프로 렌더링됩니다.
- 반드시 한국어로 답하세요.
- 아래 JSON만 반환하세요. JSON 밖 설명/마크다운 코드펜스는 쓰지 마세요.

반환 JSON 스키마:
{{
  "summary_md": "마크다운 본문. 표와 산문 위주. 텍스트 막대 그래프는 넣지 마세요.",
  "cards": [
    {{"label": "핵심 항목", "value": "짧은 값", "tone": "good|watch|bad|neutral", "detail": "한 줄 해석"}}
  ],
  "metric_trends": [
    {{"label": "영업이익률", "unit": "%", "note": "한 줄 코멘트(선택)",
      "before": {{"label": "2025 연간", "value": 13.1}},
      "after": {{"label": "2026 1분기", "value": 42.8}}}}
  ],
  "watch_items": ["확인해야 할 항목"],
  "comparison_notes": ["전/이전 보고서 대비 확인 사항"],
  "source_limits": "사용한 원문 범위와 한계"
}}

metric_trends 작성 규칙:
- 투자 판단에 중요한 지표 2~5개의 직전 대비 변화만 담으세요(영업이익률·매출·핵심 사업부 비중 등).
- value 는 단위 기호 없이 숫자만 쓰고, 단위는 unit 에 따로 쓰세요(예: "%", "조", "억").
- before/after 의 label 에는 비교 시점을 쓰세요(예: "2025 연간", "2026 1분기").

{financial_context}

## 비교 보고서 발췌
{comparison_text}

## 주 보고서 원문 발췌
{primary_text[:PRIMARY_TEXT_CHARS]}
""".strip()


def _strip_json_fence(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"```$", "", text).strip()
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start:end + 1]
    return text


def _coerce_number(value: Any) -> float | None:
    """LLM이 숫자 또는 '13.1%'/'333.6조' 같은 문자열로 줄 수 있어 첫 수치를 뽑는다."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        m = re.search(r"-?\d[\d,]*(?:\.\d+)?", value)
        if m:
            try:
                return float(m.group(0).replace(",", ""))
            except ValueError:
                return None
    return None


def _normalize_metric_trends(raw: Any) -> list[dict[str, Any]]:
    """metric_trends를 {label, unit, note, before:{label,value}, after:{label,value}} 로 정규화.

    value 가 None/문자열이어도 숫자를 뽑고, before/after 둘 다 값이 없으면 버린다.
    """
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        before = item.get("before") if isinstance(item.get("before"), dict) else {}
        after = item.get("after") if isinstance(item.get("after"), dict) else {}
        bv = _coerce_number(before.get("value"))
        av = _coerce_number(after.get("value"))
        if bv is None and av is None:
            continue
        out.append({
            "label": str(item.get("label") or "")[:40],
            "unit": str(item.get("unit") or "")[:8],
            "note": str(item.get("note") or "")[:80],
            "before": {"label": str(before.get("label") or "")[:24], "value": bv},
            "after": {"label": str(after.get("label") or "")[:24], "value": av},
        })
        if len(out) >= 8:
            break
    return out


def _normalize_review(raw_text: str) -> dict[str, Any]:
    try:
        parsed = json.loads(_strip_json_fence(raw_text))
    except Exception:
        parsed = {"summary_md": (raw_text or "").strip()}
    if not isinstance(parsed, dict):
        parsed = {"summary_md": str(parsed)}
    parsed.setdefault("summary_md", "")
    parsed.setdefault("cards", [])
    parsed.setdefault("watch_items", [])
    parsed.setdefault("comparison_notes", [])
    parsed.setdefault("source_limits", "")
    if not isinstance(parsed["cards"], list):
        parsed["cards"] = []
    parsed["metric_trends"] = _normalize_metric_trends(parsed.get("metric_trends"))
    return parsed


def _looks_like_raw_json_blob(text: str | None) -> bool:
    r"""True when a JSON payload was dumped verbatim into a markdown field.

    This is the truncated/unparseable-output failure mode: when ``json.loads``
    fails, ``_normalize_review`` falls back to putting the entire raw LLM text
    into ``summary_md``. Rendering that as markdown shows the user raw
    ``{"summary_md": ...}`` with literal ``\n`` and broken tables, so such a
    payload must be treated as broken rather than displayed or cached as a real
    review. Normal markdown starts with a heading or prose, never ``{``.
    """
    head = (text or "").lstrip()
    if not head.startswith("{"):
        return False
    return '"summary_md"' in head[:400]


def _finish_reason_from_response(data: dict[str, Any] | None) -> str | None:
    try:
        return ((data or {}).get("choices") or [{}])[0].get("finish_reason")
    except Exception:
        return None


def _review_payload_is_broken(payload: dict[str, Any], *, finish_reason: str | None = None) -> bool:
    """A freshly generated review is broken when the model output was cut off
    (``finish_reason == "length"``) or the JSON could not be parsed and the raw
    blob landed in ``summary_md``. Such payloads must never be cached."""
    if finish_reason == "length":
        return True
    summary = payload.get("summary_md") if isinstance(payload, dict) else None
    return _looks_like_raw_json_blob(summary)


def _cached_review_is_broken(cached: dict[str, Any] | None) -> bool:
    """Detect a previously cached review poisoned by the truncation bug, so the
    read path can fall back to ``missing`` and let the background pipeline
    regenerate it instead of serving raw JSON to the UI."""
    if not isinstance(cached, dict):
        return False
    review = cached.get("review")
    summary = review.get("summary_md") if isinstance(review, dict) else None
    return _looks_like_raw_json_blob(summary or cached.get("review_md"))


async def _call_openrouter(
    prompt: str,
    *,
    google_sub: str | None,
    max_tokens: int = DART_REVIEW_MAX_TOKENS,
) -> tuple[dict[str, Any], dict[str, Any]]:
    model = await ai_config.get_model_for_feature("dart_report_review")
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "당신은 한국 상장사 DART 정기보고서를 읽는 보수적인 투자 분석가입니다. "
                    "보고서 원문에 근거한 요약과 리스크 체크리스트만 제공합니다."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    try:
        result = await ai_client.post_chat_completion(
            feature="dart_report_review",
            payload=payload,
            google_sub=google_sub,
            model=model,
            model_profile="dart_report_review",
            timeout=httpx.Timeout(120.0, read=120.0),
            retry_without_reasoning=False,
        )
    except ai_client.MissingOpenRouterKeyError as exc:
        raise DartReportReviewError("OpenRouter API key is not configured.") from exc
    except ai_client.OpenRouterError as exc:
        raise DartReportReviewError(f"OpenRouter call failed: {exc}") from exc

    return _normalize_review(result["content"]), {
        "model": result["model"],
        "tokens_in": result["input_tokens"],
        "tokens_out": result["output_tokens"],
        "cost_usd": result["cost_usd"],
        "finish_reason": _finish_reason_from_response(result.get("data")),
    }


async def latest_review_status(stock_code: str) -> dict[str, Any]:
    corp_code = await corp_codes.get_corp_code(stock_code)
    if not corp_code:
        raise DartReportReviewError("종목코드를 찾을 수 없습니다.")
    corp_name = await corp_codes.get_corp_name(stock_code) or stock_code
    filings = await fetch_periodic_filings(corp_code)
    if not filings:
        return {
            "stock_code": stock_code,
            "corp_code": corp_code,
            "corp_name": corp_name,
            "status": "no_report",
            "can_generate": False,
            "latest_report": None,
            "review": None,
        }
    latest = filings[0]
    cached = await dart_review_repo.get_dart_report_review(stock_code, latest["rcept_no"])
    if cached and not _cached_review_is_broken(cached):
        cached["cached"] = True
        return {
            "stock_code": stock_code,
            "corp_code": corp_code,
            "corp_name": corp_name,
            "status": "ready",
            "can_generate": True,
            "latest_report": latest,
            "review": cached,
            "available_reports": filings,
        }
    # A poisoned (truncated-JSON) cache entry would otherwise be served as
    # "ready" forever and skipped by the background pipeline. Fall through to
    # "missing" so the UI shows the waiting state and the entry gets regenerated.
    stale = await dart_review_repo.get_dart_report_review(stock_code)
    if _cached_review_is_broken(stale):
        stale = None
    return {
        "stock_code": stock_code,
        "corp_code": corp_code,
        "corp_name": corp_name,
        "status": "missing",
        "can_generate": True,
        "latest_report": latest,
        "previous_review": stale,
        "available_reports": filings,
    }


async def generate_review(stock_code: str, *, google_sub: str | None, force: bool = False) -> dict[str, Any]:
    corp_code = await corp_codes.get_corp_code(stock_code)
    if not corp_code:
        raise DartReportReviewError("종목코드를 찾을 수 없습니다.")
    corp_name = await corp_codes.get_corp_name(stock_code) or stock_code

    filings = await fetch_periodic_filings(corp_code)
    if not filings:
        raise DartReportReviewError("최근 DART 정기보고서를 찾지 못했습니다.")
    primary = filings[0]
    if not force:
        cached = await dart_review_repo.get_dart_report_review(stock_code, primary["rcept_no"])
        if cached:
            cached["cached"] = True
            return cached

    primary_text = await fetch_document_text(primary["rcept_no"])
    comparison_items: list[dict[str, Any]] = []
    for filing in filings[1:1 + max(0, COMPARISON_REPORT_COUNT)]:
        try:
            text = await fetch_document_text(filing["rcept_no"])
            snippet = _focus_snippets(text, limit=COMPARISON_TEXT_CHARS)
            comparison_items.append({**filing, "doc_chars": len(text), "snippet": snippet})
        except Exception as exc:
            logger.warning("comparison DART report skipped %s: %s", filing["rcept_no"], exc)
            comparison_items.append({**filing, "doc_chars": 0, "snippet": "", "error": str(exc)})

    prompt = _build_prompt(
        stock_code=stock_code,
        corp_name=corp_name,
        primary=primary,
        primary_text=_focus_snippets(primary_text, limit=PRIMARY_TEXT_CHARS),
        comparisons=comparison_items,
        financial_context=await _financial_context(stock_code),
    )
    review_payload, usage = await _call_openrouter(prompt, google_sub=google_sub)
    if _review_payload_is_broken(review_payload, finish_reason=usage.get("finish_reason")):
        logger.warning(
            "DART review truncated/unparseable for %s (finish_reason=%s, tokens_out=%s); "
            "retrying with a larger token budget",
            stock_code,
            usage.get("finish_reason"),
            usage.get("tokens_out"),
        )
        review_payload, usage = await _call_openrouter(
            prompt, google_sub=google_sub, max_tokens=DART_REVIEW_MAX_TOKENS_RETRY
        )
        if _review_payload_is_broken(review_payload, finish_reason=usage.get("finish_reason")):
            # Never cache a truncated/garbled payload — that is exactly what made
            # the broken review stick until a manual regeneration.
            raise DartReportReviewError(
                "AI 응답이 잘려 정상적인 리뷰를 만들지 못했습니다. 잠시 후 다시 시도해 주세요."
            )

    review = {
        "stock_code": stock_code,
        "corp_code": corp_code,
        "corp_name": corp_name,
        "rcept_no": primary["rcept_no"],
        "report_name": primary["report_name"],
        "report_date": primary.get("report_date"),
        "primary_doc_chars": len(primary_text),
        "comparison_reports": [
            {
                key: item.get(key)
                for key in ("rcept_no", "report_name", "report_date", "kind", "period", "viewer_url", "doc_chars", "error")
            }
            for item in comparison_items
        ],
        "review": review_payload,
        "review_md": review_payload.get("summary_md") or "",
        "model": usage["model"],
        "tokens_in": usage["tokens_in"],
        "tokens_out": usage["tokens_out"],
        "cost_usd": usage["cost_usd"],
        "created_at": datetime.now().isoformat(),
    }
    saved = await dart_review_repo.save_dart_report_review(review)
    saved["cached"] = False
    return saved


def _reason_from_error(exc: Exception) -> str:
    message = str(exc)
    lowered = message.lower()
    if "opendart_api_key" in lowered or "openrouter api key" in lowered:
        return "config_missing"
    if "429" in lowered or "rate" in lowered:
        return "rate_limited"
    if "timeout" in lowered or "timed out" in lowered:
        return "timeout"
    if "dart" in lowered or "openrouter" in lowered or "http" in lowered:
        return "upstream_error"
    return "review_error"


async def review_stock(
    stock_code: str,
    *,
    force: bool = False,
    allow_generate: bool = True,
) -> dict[str, Any]:
    """Ensure the latest periodic filing has a cached AI review."""
    code = str(stock_code or "").strip()
    if not code:
        return {"stock_code": code, "status": "skipped", "reason": "invalid_stock_code"}

    corp_code = await corp_codes.get_corp_code(code)
    if not corp_code:
        return {"stock_code": code, "status": "skipped", "reason": "no_corp_code"}

    status = await latest_review_status(code)
    latest = status.get("latest_report") or {}
    if status.get("status") == "ready" and not force:
        return {
            "stock_code": code,
            "status": "skipped",
            "reason": "already_ready",
            "rcept_no": latest.get("rcept_no"),
        }
    if status.get("status") == "no_report":
        return {"stock_code": code, "status": "skipped", "reason": "no_report"}
    if not allow_generate:
        return {
            "stock_code": code,
            "status": "skipped",
            "reason": "target_limit_reached",
            "rcept_no": latest.get("rcept_no"),
        }

    review = await generate_review(code, google_sub=None, force=force)
    return {
        "stock_code": code,
        "status": "generated",
        "rcept_no": review.get("rcept_no"),
        "report_date": review.get("report_date"),
        "model": review.get("model"),
    }


def _bump_counter(bucket: dict[str, int], key: str | None, amount: int = 1) -> None:
    name = key or "unknown"
    bucket[name] = bucket.get(name, 0) + amount


async def run_pipeline(
    stock_codes: list[str] | None = None,
    *,
    target_limit: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Pre-generate missing DART filing reviews for active stocks.

    target_limit caps new LLM generations per run, not how many targets are
    checked. That lets later stocks progress after earlier ones are cached.
    """
    if _pipeline_lock.locked():
        return {
            "stocks_total": 0,
            "stocks_processed": 0,
            "generated": 0,
            "skipped": 1,
            "failed": 0,
            "skipped_by_reason": {"already_running": 1},
            "failed_by_reason": {},
            "per_stock": [],
            "already_running": True,
        }

    async with _pipeline_lock:
        if stock_codes is None:
            stock_codes = await wiki_repo.select_wiki_target_stocks()
        codes = [str(code).strip() for code in stock_codes if str(code or "").strip()]
        codes = list(dict.fromkeys(codes))
        try:
            generation_limit = DART_REVIEW_TARGET_LIMIT if target_limit is None else int(target_limit)
        except (TypeError, ValueError):
            generation_limit = DART_REVIEW_TARGET_LIMIT

        overall: dict[str, Any] = {
            "stocks_total": len(codes),
            "stocks_processed": 0,
            "generated": 0,
            "skipped": 0,
            "failed": 0,
            "skipped_by_reason": {},
            "failed_by_reason": {},
            "per_stock": [],
        }

        for code in codes:
            allow_generate = force or generation_limit <= 0 or overall["generated"] < generation_limit
            try:
                result = await review_stock(code, force=force, allow_generate=allow_generate)
            except Exception as exc:
                logger.exception("DART review pre-generation failed for %s: %s", code, exc)
                reason = _reason_from_error(exc)
                result = {
                    "stock_code": code,
                    "status": "failed",
                    "reason": reason,
                    "error": str(exc)[:300],
                }

            overall["stocks_processed"] += 1
            status = result.get("status")
            if status == "generated":
                overall["generated"] += 1
            elif status == "failed":
                overall["failed"] += 1
                _bump_counter(overall["failed_by_reason"], result.get("reason"))
            else:
                overall["skipped"] += 1
                _bump_counter(overall["skipped_by_reason"], result.get("reason"))

            overall["per_stock"].append(result)
            logger.info("DART review pre-generation %s: %s", code, result)

        return overall


async def run_background_loop(
    stop_event: asyncio.Event,
    *,
    interval_seconds: float,
    target_limit: int = DART_REVIEW_TARGET_LIMIT,
    initial_delay_seconds: float = 90.0,
) -> None:
    """Continuously pre-generate recent filing reviews until stopped."""
    if initial_delay_seconds > 0:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=initial_delay_seconds)
            return
        except asyncio.TimeoutError:
            pass

    while not stop_event.is_set():
        try:
            stats = await run_pipeline(target_limit=target_limit)
            failed = int(stats.get("failed") or 0)
            already_running = bool(stats.get("already_running"))
            if already_running:
                level = "info"
                kind = "tick_skipped"
            elif failed:
                level = "warning"
                kind = "tick_partial"
            else:
                level = "info"
                kind = "tick_ok"
            await observability.record_event(
                "dart_report_review",
                kind,
                level=level,
                details={
                    "stocks_processed": stats.get("stocks_processed", 0),
                    "generated": stats.get("generated", 0),
                    "skipped": stats.get("skipped", 0),
                    "failed": failed,
                    "skipped_by_reason": stats.get("skipped_by_reason", {}),
                    "failed_by_reason": stats.get("failed_by_reason", {}),
                    "target_limit": target_limit,
                },
            )
            logger.info(
                "DART review tick: stocks=%d generated=%d skipped=%d failed=%d",
                stats.get("stocks_processed", 0),
                stats.get("generated", 0),
                stats.get("skipped", 0),
                failed,
            )
        except Exception as exc:
            logger.exception("DART review background loop iteration crashed: %s", exc)
            await observability.record_event(
                "dart_report_review",
                "tick_crashed",
                level="error",
                details={"error": str(exc)[:500]},
            )

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            return
        except asyncio.TimeoutError:
            continue
