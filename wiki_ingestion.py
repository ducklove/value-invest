"""Background ingestion of broker research reports into the per-stock wiki.

Pipeline per report:
    1. download the PDF (URL whitelisted to stock.pstatic.net/stock-research)
    2. save bytes under data/pdf_cache/<sha1>.pdf, compute sha1
    3. extract text via pypdf
    4. call OpenRouter to produce a structured markdown summary
    5. persist to report_pdf_cache and stock_wiki_entries

Each step is idempotent and its result is cached — re-running is safe and
cheap. A run that crashes mid-way resumes from the same point on the next
invocation.

The public entry points are:
    - ingest_stock(stock_code, limit=...)
    - run_pipeline(stock_codes=None, per_stock_limit=...)
    - summarize_report(meta, text, model=None)  — also used by tests
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

import cache
import report_client
from routes.reports import _is_allowed_report_pdf_url

logger = logging.getLogger(__name__)


APP_DIR = Path(__file__).parent
PDF_CACHE_DIR = APP_DIR / "data" / "pdf_cache"

# Truncate parsed PDF text before sending to the LLM. Most broker reports
# are 5-15 pages; 12k chars ≈ 3k tokens for Korean — fits every model with
# room for the prompt scaffolding.
MAX_PDF_CHARS = 12000
# Maximum reports to summarize per stock per ingestion run. Prevents a
# backfill from hammering OpenRouter in one go.
DEFAULT_PER_STOCK_LIMIT = 10
# Parallelism for LLM calls — OpenRouter defaults can handle a few at once
# and each call is a few seconds. Keep modest so a pipeline run doesn't
# starve user-facing AI analysis.
_LLM_SEMAPHORE = asyncio.Semaphore(2)
_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(4)

# Pull OpenRouter config lazily from routes.portfolio so admin overrides
# and env-based keys stay the single source of truth.
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


def _get_openrouter_key() -> str:
    """Read the OpenRouter key the same way routes.portfolio does — via
    module-level global set from keys.txt at startup. Import is local to
    avoid a circular dep at module load."""
    from routes import portfolio as pf_mod
    return pf_mod._OPENROUTER_KEY or os.environ.get("OPENROUTER_API_KEY", "")


def _get_default_model() -> str:
    from routes import portfolio as pf_mod
    # Allow admins to pin a cheaper model for bulk summarization via env.
    return os.environ.get("WIKI_MODEL") or pf_mod._AI_DEFAULT_MODEL


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _sha1_hex(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def _ensure_cache_dir() -> None:
    PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)


async def download_pdf(url: str, timeout: float = 30.0) -> bytes:
    """Fetch the PDF bytes. Honors the same whitelist as routes.reports to
    avoid being used as an SSRF proxy."""
    if not _is_allowed_report_pdf_url(url):
        raise ValueError(f"URL not in allowed list: {url}")
    async with _DOWNLOAD_SEMAPHORE:
        async with httpx.AsyncClient(
            timeout=timeout,
            headers=report_client.HEADERS,
            follow_redirects=True,
        ) as client:
            resp = await client.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for {url}")
    # Sanity: real PDFs start with '%PDF-'. A redirect to an HTML error
    # page would pass the status check but not this.
    if not resp.content[:5].startswith(b"%PDF-"):
        raise RuntimeError(f"Not a PDF (first bytes: {resp.content[:16]!r})")
    return resp.content


def parse_pdf_bytes(data: bytes) -> str:
    """Extract text from PDF bytes. Returns a best-effort string; empty
    string for image-only PDFs (pypdf doesn't OCR)."""
    import io
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    chunks: list[str] = []
    for page in reader.pages:
        try:
            chunks.append(page.extract_text() or "")
        except Exception as exc:  # pragma: no cover — malformed PDFs
            logger.warning("pypdf extract failed on a page: %s", exc)
    text = "\n".join(chunks)
    # Collapse runs of whitespace; broker PDFs have lots of column breaks.
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ---------------------------------------------------------------------------
# LLM summarization
# ---------------------------------------------------------------------------

SUMMARY_SYSTEM = "당신은 한국 주식 증권사 리포트 요약 전문가입니다."

SUMMARY_PROMPT_TEMPLATE = """아래 증권사 리포트를 요약해 주세요.

## 메타
- 종목: {name} ({code})
- 증권사: {firm}
- 발행일: {report_date}
- 투자의견: {recommendation}
- 목표가: {target_price}

## 본문 (일부)
{body}

아래 3개 섹션을 한국어 마크다운으로 출력하세요. 다른 섹션이나 서두는
추가하지 마세요. 숫자와 전망은 본문에 근거해 구체적으로 인용하세요.

### 핵심 요약
투자 아이디어 / 주요 근거 / 핵심 리스크. 2-4문단.

### 주요 포인트
- 불릿 5-8개. 숫자, 성장률, 전망치, 주요 이벤트 중심.

### 밸류에이션 / 목표가 논거
목표가 산출 방법, 비교 그룹, 상방·하방 시나리오. 1-2문단.
"""


def build_summary_prompt(meta: dict, text: str) -> str:
    body = (text or "")[:MAX_PDF_CHARS]
    if not body.strip():
        body = "(본문 텍스트가 비어 있음 — PDF에서 추출 실패)"
    return SUMMARY_PROMPT_TEMPLATE.format(
        name=meta.get("stock_name") or "",
        code=meta.get("stock_code") or "",
        firm=meta.get("firm") or meta.get("firm_short") or "",
        report_date=meta.get("report_date") or meta.get("date") or "",
        recommendation=meta.get("recommendation") or "",
        target_price=meta.get("target_price") or "",
        body=body,
    )


def _split_summary_sections(md: str) -> tuple[str, str]:
    """Split the model's output into (summary_md, key_points_md).

    summary_md is the whole response; key_points_md is just the '주요 포인트'
    section (bulleted list). We keep both so the UI can show full summary
    on demand but embed just the bullets into portfolio-AI prompts."""
    summary = md.strip()
    key = ""
    # Match a section header followed by content up to the next ### or EOF.
    m = re.search(r"###\s*주요\s*포인트\s*\n(.*?)(?=\n###|\Z)", summary, re.DOTALL)
    if m:
        key = m.group(1).strip()
    return summary, key


async def summarize_report(meta: dict, text: str, model: str | None = None) -> dict:
    """Call OpenRouter for a structured markdown summary.

    Returns {summary_md, key_points_md, tokens_in, tokens_out, model}.
    Raises on upstream failure — callers catch and mark parse_failed."""
    key = _get_openrouter_key()
    if not key:
        raise RuntimeError("OpenRouter key not configured")
    model = model or _get_default_model()

    prompt = build_summary_prompt(meta, text)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SUMMARY_SYSTEM},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 1200,
        # Bulk; no stream needed, we're not showing tokens to the user.
        "stream": False,
    }
    async with _LLM_SEMAPHORE:
        async with httpx.AsyncClient(timeout=90.0) as client:
            resp = await client.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
    if resp.status_code != 200:
        raise RuntimeError(f"OpenRouter {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"OpenRouter response malformed: {data}") from exc
    usage = data.get("usage") or {}
    summary_md, key_points_md = _split_summary_sections(content)
    return {
        "summary_md": summary_md,
        "key_points_md": key_points_md,
        "tokens_in": usage.get("prompt_tokens"),
        "tokens_out": usage.get("completion_tokens"),
        "model": data.get("model") or model,
    }


# ---------------------------------------------------------------------------
# Per-report ingestion
# ---------------------------------------------------------------------------

def _parse_target_price(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


async def ingest_pdf_for_report(
    stock_code: str,
    report_meta: dict,
    model: str | None = None,
) -> dict:
    """Process one report: download+parse if needed, summarize if not
    already in wiki. Returns a short status dict for logging.

    report_meta is the dict produced by report_client.fetch_reports() —
    must contain pdf_url, date, firm, title, etc. stock_name is optional
    and filled via corp_codes lookup if absent."""
    pdf_url = (report_meta.get("pdf_url") or "").strip()
    if not pdf_url or not _is_allowed_report_pdf_url(pdf_url):
        return {"skipped": "no_pdf_url"}

    _ensure_cache_dir()

    # Step 1: download (or reuse disk cache by URL — we'll re-key by sha1 once bytes arrive).
    # We don't know sha1 until after download; that's OK, the bytes are tiny
    # compared to the LLM call cost we're gating.
    try:
        pdf_bytes = await download_pdf(pdf_url)
    except Exception as exc:
        # Record the failure so we don't keep retrying every run.
        tmp_sha = hashlib.sha1(pdf_url.encode()).hexdigest()
        await cache.save_pdf_cache_row({
            "pdf_sha1": tmp_sha,
            "stock_code": stock_code,
            "pdf_url": pdf_url,
            "file_path": None,
            "file_bytes": None,
            "parsed_text": None,
            "parse_status": "download_failed",
            "parse_error": str(exc)[:500],
            "downloaded_at": None,
            "parsed_at": None,
        })
        logger.warning("download failed %s: %s", pdf_url, exc)
        return {"failed": "download", "error": str(exc)}

    pdf_sha1 = _sha1_hex(pdf_bytes)

    # Step 2: already summarized for this stock? Skip LLM call.
    if await cache.pdf_is_already_summarized(stock_code, pdf_sha1):
        return {"skipped": "already_summarized", "pdf_sha1": pdf_sha1}

    # Persist the PDF bytes (cheap insurance for later re-parse / audit).
    file_path = PDF_CACHE_DIR / f"{pdf_sha1}.pdf"
    if not file_path.exists():
        file_path.write_bytes(pdf_bytes)

    # Step 3: parse
    try:
        text = parse_pdf_bytes(pdf_bytes)
        parse_status = "parsed" if text.strip() else "parse_failed"
        parse_error = None if text.strip() else "empty text extraction (image-only PDF?)"
    except Exception as exc:
        text = ""
        parse_status = "parse_failed"
        parse_error = str(exc)[:500]

    now = datetime.now().isoformat()
    # Store relative path when inside APP_DIR so backups & deploys stay
    # portable; otherwise (tests, one-off tools) keep absolute.
    try:
        stored_path = str(file_path.relative_to(APP_DIR))
    except ValueError:
        stored_path = str(file_path)
    await cache.save_pdf_cache_row({
        "pdf_sha1": pdf_sha1,
        "stock_code": stock_code,
        "pdf_url": pdf_url,
        "file_path": stored_path,
        "file_bytes": len(pdf_bytes),
        "parsed_text": text or None,
        "parse_status": parse_status,
        "parse_error": parse_error,
        "downloaded_at": now,
        "parsed_at": now if parse_status == "parsed" else None,
    })

    if parse_status != "parsed":
        return {"failed": "parse", "pdf_sha1": pdf_sha1, "error": parse_error}

    # Step 4: summarize
    try:
        summary = await summarize_report(report_meta, text, model=model)
    except Exception as exc:
        logger.warning("summarize failed %s: %s", pdf_sha1, exc)
        return {"failed": "summarize", "pdf_sha1": pdf_sha1, "error": str(exc)[:300]}

    # Step 5: persist wiki entry
    await cache.save_wiki_entry({
        "stock_code": stock_code,
        "source_type": "broker_report",
        "source_ref": pdf_sha1,
        "report_date": report_meta.get("date"),
        "firm": report_meta.get("firm") or report_meta.get("firm_short"),
        "title": report_meta.get("title"),
        "recommendation": report_meta.get("recommendation"),
        "target_price": _parse_target_price(report_meta.get("target_price")),
        "summary_md": summary["summary_md"],
        "key_points_md": summary["key_points_md"],
        "model": summary.get("model"),
        "tokens_in": summary.get("tokens_in"),
        "tokens_out": summary.get("tokens_out"),
        "created_at": now,
    })
    return {
        "ok": True,
        "pdf_sha1": pdf_sha1,
        "tokens_in": summary.get("tokens_in"),
        "tokens_out": summary.get("tokens_out"),
    }


# ---------------------------------------------------------------------------
# Per-stock / pipeline drivers
# ---------------------------------------------------------------------------

async def ingest_stock(
    stock_code: str,
    limit: int = DEFAULT_PER_STOCK_LIMIT,
    model: str | None = None,
) -> dict:
    """Fetch the latest `limit` reports for a stock and run each through
    the pipeline. Summaries already present are skipped cheaply.

    Returns {stock_code, processed, summarized, skipped, failed, details}.
    """
    # Use the cached report list when available (fast, respects TTL),
    # fall back to a live scrape if empty.
    cached = await cache.get_report_list(stock_code, ttl_minutes=24 * 60)
    reports: list[dict]
    if cached and cached.get("reports"):
        reports = cached["reports"]
    else:
        try:
            reports = await report_client.fetch_reports(stock_code)
            if reports:
                await cache.save_report_list(stock_code, reports)
        except Exception as exc:
            logger.warning("fetch_reports failed for %s: %s", stock_code, exc)
            reports = []

    reports = [r for r in reports if r.get("pdf_url")][:limit]
    # Attach corp_name so the summary prompt can include it.
    try:
        corp_name = await cache.get_corp_name(stock_code) or ""
    except Exception:
        corp_name = ""
    for r in reports:
        r.setdefault("stock_code", stock_code)
        r.setdefault("stock_name", corp_name)
        r.setdefault("report_date", r.get("date"))

    stats = {
        "stock_code": stock_code,
        "processed": 0,
        "summarized": 0,
        "skipped": 0,
        "failed": 0,
        "details": [],
    }
    for r in reports:
        stats["processed"] += 1
        try:
            result = await ingest_pdf_for_report(stock_code, r, model=model)
        except Exception as exc:
            logger.exception("ingest_pdf_for_report crashed: %s", exc)
            result = {"failed": "exception", "error": str(exc)[:300]}
        if result.get("ok"):
            stats["summarized"] += 1
        elif result.get("skipped"):
            stats["skipped"] += 1
        else:
            stats["failed"] += 1
        stats["details"].append(result)
    return stats


async def run_pipeline(
    stock_codes: list[str] | None = None,
    per_stock_limit: int = DEFAULT_PER_STOCK_LIMIT,
    model: str | None = None,
) -> dict:
    """Top-level: iterate target stocks and ingest.

    If `stock_codes` is None, uses cache.select_wiki_target_stocks() —
    portfolio + starred + recently-viewed."""
    if stock_codes is None:
        stock_codes = await cache.select_wiki_target_stocks()
    stock_codes = list(dict.fromkeys(stock_codes))  # dedupe, preserve order

    overall = {
        "stocks_total": len(stock_codes),
        "stocks_processed": 0,
        "summarized": 0,
        "skipped": 0,
        "failed": 0,
        "per_stock": [],
    }
    for code in stock_codes:
        try:
            s = await ingest_stock(code, limit=per_stock_limit, model=model)
        except Exception as exc:
            logger.exception("ingest_stock crashed for %s: %s", code, exc)
            s = {"stock_code": code, "error": str(exc)[:300]}
        overall["stocks_processed"] += 1
        overall["summarized"] += s.get("summarized", 0)
        overall["skipped"] += s.get("skipped", 0)
        overall["failed"] += s.get("failed", 0)
        overall["per_stock"].append({
            "stock_code": code,
            "summarized": s.get("summarized", 0),
            "skipped": s.get("skipped", 0),
            "failed": s.get("failed", 0),
        })
        logger.info("wiki ingest %s: %s", code, json.dumps(s.get("per_stock") or s, default=str)[:200])
    return overall
