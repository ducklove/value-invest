"""AI review pipeline for the latest DART periodic filing."""

from __future__ import annotations

import io
import json
import logging
import os
import re
import time
import warnings
import zipfile
from datetime import date, datetime, timedelta
from html import unescape
from typing import Any

import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

import ai_config
import cache
import dart_client


logger = logging.getLogger(__name__)

DART_VIEWER_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
PERIODIC_REPORT_KEYWORDS = ("분기보고서", "반기보고서", "사업보고서")
PRIMARY_TEXT_CHARS = int(os.getenv("DART_REVIEW_PRIMARY_CHARS", "42000"))
COMPARISON_TEXT_CHARS = int(os.getenv("DART_REVIEW_COMPARISON_CHARS", "9000"))
COMPARISON_REPORT_COUNT = int(os.getenv("DART_REVIEW_COMPARISON_REPORTS", "2"))
REPORT_LOOKBACK_DAYS = int(os.getenv("DART_REVIEW_LOOKBACK_DAYS", "1100"))
DART_FILINGS_CACHE_TTL_S = int(os.getenv("DART_REVIEW_FILINGS_CACHE_TTL_S", "600"))

_filings_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}


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
    cached = _filings_cache.get(cache_key)
    if cached and time.monotonic() - cached[0] < DART_FILINGS_CACHE_TTL_S:
        return [dict(item) for item in cached[1][:limit]]

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
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(f"{dart_client.BASE_URL}/list.json", params=params)
    resp.raise_for_status()
    data = resp.json()
    status = str(data.get("status") or "")
    if status != "000":
        if status in {"013", "014"}:
            _filings_cache[cache_key] = (time.monotonic(), [])
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
    _filings_cache[cache_key] = (time.monotonic(), [dict(item) for item in filings])
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
    async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
        resp = await client.get(
            f"{dart_client.BASE_URL}/document.xml",
            params={"crtfc_key": api_key, "rcept_no": rcept_no},
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
    fin_rows = await cache.get_financial_data(stock_code)
    mkt_rows = await cache.get_market_data(stock_code)
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
- 그래픽화는 텍스트 막대, 신호등 카드, 체크리스트처럼 웹에서 바로 렌더링하기 좋은 형태로 만드세요.
- 반드시 한국어로 답하세요.
- 아래 JSON만 반환하세요. JSON 밖 설명/마크다운 코드펜스는 쓰지 마세요.

반환 JSON 스키마:
{{
  "summary_md": "마크다운 본문. 표와 짧은 막대 그래프를 포함.",
  "cards": [
    {{"label": "핵심 항목", "value": "짧은 값", "tone": "good|watch|bad|neutral", "detail": "한 줄 해석"}}
  ],
  "watch_items": ["확인해야 할 항목"],
  "comparison_notes": ["전/이전 보고서 대비 확인 사항"],
  "source_limits": "사용한 원문 범위와 한계"
}}

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


def _normalize_review(raw_text: str) -> dict[str, Any]:
    try:
        parsed = json.loads(_strip_json_fence(raw_text))
    except Exception:
        parsed = {"summary_md": raw_text.strip()}
    if not isinstance(parsed, dict):
        parsed = {"summary_md": str(parsed)}
    parsed.setdefault("summary_md", "")
    parsed.setdefault("cards", [])
    parsed.setdefault("watch_items", [])
    parsed.setdefault("comparison_notes", [])
    parsed.setdefault("source_limits", "")
    if not isinstance(parsed["cards"], list):
        parsed["cards"] = []
    return parsed


async def _call_openrouter(prompt: str, *, google_sub: str | None) -> tuple[dict[str, Any], dict[str, Any]]:
    openrouter_key = await ai_config.get_openrouter_key()
    if not openrouter_key:
        raise DartReportReviewError("OpenRouter API key가 설정되어 있지 않습니다.")

    model = await ai_config.get_model_for_feature("dart_report_review")
    started = time.monotonic()
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
        "max_tokens": 2800,
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, read=120.0)) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {openrouter_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        if resp.status_code != 200:
            msg = resp.text[:500]
            raise DartReportReviewError(f"OpenRouter 호출 실패: HTTP {resp.status_code} {msg}")
        data = resp.json()
        choice = (data.get("choices") or [{}])[0]
        raw_text = (choice.get("message") or {}).get("content") or ""
        usage = data.get("usage") or {}
        input_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
        output_tokens = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
        cost = float(usage.get("cost") or usage.get("total_cost") or 0)
        latency_ms = int((time.monotonic() - started) * 1000)
        await ai_config.record_usage(
            google_sub=google_sub,
            feature="dart_report_review",
            model=data.get("model") or model,
            model_profile="dart_report_review",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            latency_ms=latency_ms,
            ok=True,
        )
        return _normalize_review(raw_text), {
            "model": data.get("model") or model,
            "tokens_in": input_tokens,
            "tokens_out": output_tokens,
            "cost_usd": cost,
        }
    except Exception as exc:
        if isinstance(exc, DartReportReviewError):
            error = str(exc)
        else:
            error = repr(exc)
        await ai_config.record_usage(
            google_sub=google_sub,
            feature="dart_report_review",
            model=model,
            model_profile="dart_report_review",
            ok=False,
            error=error,
            latency_ms=int((time.monotonic() - started) * 1000),
        )
        raise


async def latest_review_status(stock_code: str) -> dict[str, Any]:
    corp_code = await cache.get_corp_code(stock_code)
    if not corp_code:
        raise DartReportReviewError("종목코드를 찾을 수 없습니다.")
    corp_name = await cache.get_corp_name(stock_code) or stock_code
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
    cached = await cache.get_dart_report_review(stock_code, latest["rcept_no"])
    if cached:
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
    stale = await cache.get_dart_report_review(stock_code)
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
    corp_code = await cache.get_corp_code(stock_code)
    if not corp_code:
        raise DartReportReviewError("종목코드를 찾을 수 없습니다.")
    corp_name = await cache.get_corp_name(stock_code) or stock_code

    filings = await fetch_periodic_filings(corp_code)
    if not filings:
        raise DartReportReviewError("최근 DART 정기보고서를 찾지 못했습니다.")
    primary = filings[0]
    if not force:
        cached = await cache.get_dart_report_review(stock_code, primary["rcept_no"])
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
    saved = await cache.save_dart_report_review(review)
    saved["cached"] = False
    return saved
