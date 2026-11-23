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
import observability
from cache_layer import MemoryTTLCache
from core.http import get_http_client
from repositories import cache_values, corp_codes
from repositories import dart_review as dart_review_repo
from repositories import financial as financial_repo
from repositories import wiki as wiki_repo
from services import ai_client
from services.dart import client as dart_client

logger = logging.getLogger(__name__)

DART_VIEWER_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
PERIODIC_REPORT_KEYWORDS = ("분기보고서", "반기보고서", "사업보고서")
PRIMARY_TEXT_CHARS = int(os.getenv("DART_REVIEW_PRIMARY_CHARS", "42000"))
COMPARISON_TEXT_CHARS = int(os.getenv("DART_REVIEW_COMPARISON_CHARS", "9000"))
COMPARISON_REPORT_COUNT = int(os.getenv("DART_REVIEW_COMPARISON_REPORTS", "2"))
REPORT_LOOKBACK_DAYS = int(os.getenv("DART_REVIEW_LOOKBACK_DAYS", "1100"))
DART_FILINGS_CACHE_TTL_S = int(os.getenv("DART_REVIEW_FILINGS_CACHE_TTL_S", "600"))
DART_REVIEW_TARGET_LIMIT = int(os.getenv("DART_REVIEW_TARGET_LIMIT", "12"))
# Output budget for the review. The answer is JSON Lines blocks (see
# BLOCKS_FORMAT below): a cut-off answer keeps its complete blocks and is only
# retried, with the larger budget, when too little of it survived. The older
# single-object JSON lost everything when cut off, which is why the budget is
# generous.
#
# Hidden reasoning tokens are billed as output and count against this cap. A
# model whose reasoning is mandatory (Gemini 4 Hydrogen) spent the whole 4096 /
# 8000 budget thinking and returned a cut-off answer every time, so the budget
# leaves room for reasoning on top of the ~3.5k-token JSON. It is only a cap:
# a response that finishes earlier costs what it used.
DART_REVIEW_MAX_TOKENS = int(os.getenv("DART_REVIEW_MAX_TOKENS", "10000"))
DART_REVIEW_MAX_TOKENS_RETRY = int(os.getenv("DART_REVIEW_MAX_TOKENS_RETRY", "20000"))
DART_REVIEW_REASONING_EFFORT = os.getenv("DART_REVIEW_REASONING_EFFORT", "low")
# A filing whose review came back cut off is not cached, so every pipeline run
# used to pay for the same failing calls again. Failed output is now remembered
# per filing: the background pipeline waits out a cooldown between attempts and
# gives up after a few. A manual request from the UI is never blocked.
DART_REVIEW_MAX_FAILED_ATTEMPTS = int(os.getenv("DART_REVIEW_MAX_FAILED_ATTEMPTS", "2"))
DART_REVIEW_FAILURE_COOLDOWN_S = float(os.getenv("DART_REVIEW_FAILURE_COOLDOWN_S", str(20 * 3600)))
CACHE_NS_REVIEW_FAILURE = "dart_review.output_failure"

# Review output format. The model answers in JSON Lines, one block per line,
# from a small fixed catalog (the A2UI idea: the model composes the page from
# components the client already knows how to draw, and each one is validated on
# its own). A cut-off answer loses only its unfinished last line instead of the
# whole review, and tables arrive as data rather than model-written markdown.
BLOCKS_FORMAT = "blocks/v1"
CONTENT_BLOCK_TYPES = ("text", "bullets", "table", "checklist", "changes")
CARD_TONES = ("good", "watch", "bad", "neutral")
MAX_BLOCKS = 60
MAX_CARDS = 8
MAX_TRENDS = 8
MAX_TABLE_COLUMNS = 6
MAX_TABLE_ROWS = 15
MAX_LIST_ITEMS = 10
# A cut-off answer is still kept when it got at least this far; anything less
# counts as a failed review and is retried like before.
MIN_USABLE_CARDS = 1
MIN_USABLE_CONTENT_BLOCKS = 2
CHECKLIST_TITLE = "다음 체크포인트"
CHANGES_TITLE = "이전 보고서 대비 변화"


def _block_example(block: dict[str, Any]) -> str:
    return json.dumps(block, ensure_ascii=False)


BLOCK_EXAMPLE_CARD = _block_example(
    {"type": "card", "label": "부채비율", "value": "45.0%", "tone": "good", "detail": "전기말 52.0%에서 개선"}
)
BLOCK_EXAMPLE_TREND = _block_example({
    "type": "trend", "label": "영업이익률", "unit": "%", "note": "한 줄 코멘트(선택)",
    "before": {"label": "2025 연간", "value": 13.1}, "after": {"label": "2026 1분기", "value": 42.8},
})
BLOCK_EXAMPLE_TEXT = _block_example({"type": "text", "md": "한눈 요약 2~3문장. **핵심 수치**는 굵게."})
BLOCK_EXAMPLE_SECTION = _block_example({"type": "section", "title": "실적·수익성"})
BLOCK_EXAMPLE_TABLE = _block_example({
    "type": "table", "columns": ["구분", "직전 기간", "이번 기간", "비고"],
    "rows": [["매출액", "1조 2,000억 원", "1조 3,500억 원", "12.5% 증가"]], "note": "표 아래 한 줄 설명(선택)",
})
BLOCK_EXAMPLE_BULLETS = _block_example({"type": "bullets", "items": ["근거가 있는 항목 1", "항목 2"]})
BLOCK_EXAMPLE_CHECKLIST = _block_example({"type": "checklist", "items": ["다음 보고서에서 확인할 것"]})
BLOCK_EXAMPLE_CHANGES = _block_example({"type": "changes", "items": ["이전 보고서 대비 달라진 점"]})
BLOCK_EXAMPLE_LIMITS = _block_example({"type": "limits", "text": "사용한 원문 범위와 한계"})

_filings_cache = MemoryTTLCache("dart_report_review.filings", DART_FILINGS_CACHE_TTL_S, evict_expired_after=0)
_pipeline_lock = asyncio.Lock()


class DartReportReviewError(RuntimeError):
    """Recoverable user-facing error for report review generation."""


class DartReviewOutputError(DartReportReviewError):
    """The model was called (and billed) but its answer was cut off or unparseable."""


def _dart_key() -> str:
    return dart_client.api_key()


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
    # list.json 은 dart_client 단일 클라이언트(쿼리 캐시·쿼터 가드)를 지난다.
    try:
        items = await dart_client.fetch_filing_list(
            corp_code,
            start.strftime("%Y%m%d"),
            end.strftime("%Y%m%d"),
            last_reprt_at="Y",
            pblntf_ty="A",
            sort="date",
            sort_mth="desc",
            page_no=1,
            page_count=100,
            timeout=20.0,
        )
    except dart_client.DartListError as exc:
        if exc.dart_status is None:
            raise  # HTTP/파싱 오류 — 예전 raise_for_status 처럼 그대로 전파
        if exc.dart_status == "014":
            _filings_cache.set(cache_key, [])
            return []
        raise DartReportReviewError(exc.message or f"DART 공시검색 실패({exc.dart_status})") from exc
    if not items:
        _filings_cache.set(cache_key, [])
        return []

    filings: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
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
- 같은 내용을 여러 블록에 반복하지 마세요.
- 반드시 한국어로 답하세요.

출력 형식 — JSON Lines:
- 한 줄에 JSON 객체(블록) 하나씩 출력하세요. 배열이나 바깥 객체로 감싸지 말고,
  코드펜스·설명 문장도 쓰지 마세요. 객체 안에서 줄을 바꾸지 마세요.
- 블록은 아래 순서대로 출력하세요. 응답이 중간에 잘려도 앞쪽 블록은 그대로 화면에 쓰입니다.
- 문자열 안의 마크다운은 **굵게** 강조만 쓰세요. 제목·목록·표는 마크다운이 아니라
  해당 블록으로 만드세요. 텍스트 막대(█ 등)는 쓰지 마세요.

블록 종류와 순서:
1. card 3~6개 — 투자 판단에 가장 중요한 항목을 신호등 카드로. tone 은 good|watch|bad|neutral.
{BLOCK_EXAMPLE_CARD}
2. trend 2~5개 — 핵심 지표(영업이익률·매출·핵심 사업부 비중 등)의 직전 대비 변화. 화면에서 막대 그래프로 그려집니다.
   value 는 단위 없이 숫자만, 단위는 unit 에("%", "조", "억"). before/after 의 label 에는 비교 시점("2025 연간", "2026 1분기").
{BLOCK_EXAMPLE_TREND}
3. text 1개 — 한눈 요약 2~3문장.
{BLOCK_EXAMPLE_TEXT}
4. 다음 네 섹션을 이 순서로: 실적·수익성 / 재무안정성·현금흐름 / 사업부·제품 / 리스크.
   각 섹션은 section 블록 뒤에 text·bullets·table 블록 1~3개. 수치 비교는 table 로 쓰세요.
   table 의 첫 열은 항목명, 열 2~6개, 행 12개 이하, 셀은 문자열. note 는 표 아래 한 줄 설명(선택).
{BLOCK_EXAMPLE_SECTION}
{BLOCK_EXAMPLE_TABLE}
{BLOCK_EXAMPLE_BULLETS}
5. checklist 1개 — 다음 보고서·공시에서 확인할 체크포인트.
{BLOCK_EXAMPLE_CHECKLIST}
6. changes 1개 — 비교 보고서 대비 달라진 점과 반복되는 리스크.
{BLOCK_EXAMPLE_CHANGES}
7. limits 1개 — 사용한 원문 범위와 한계.
{BLOCK_EXAMPLE_LIMITS}

{financial_context}

## 비교 보고서 발췌
{comparison_text}

## 주 보고서 원문 발췌
{primary_text[:PRIMARY_TEXT_CHARS]}
""".strip()


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
        trend = _normalize_trend(item)
        if trend is None:
            continue
        out.append(trend)
        if len(out) >= MAX_TRENDS:
            break
    return out


def _normalize_trend(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    before = item.get("before") if isinstance(item.get("before"), dict) else {}
    after = item.get("after") if isinstance(item.get("after"), dict) else {}
    bv = _coerce_number(before.get("value"))
    av = _coerce_number(after.get("value"))
    if bv is None and av is None:
        return None
    return {
        "label": str(item.get("label") or "")[:40],
        "unit": str(item.get("unit") or "")[:8],
        "note": str(item.get("note") or "")[:80],
        "before": {"label": str(before.get("label") or "")[:24], "value": bv},
        "after": {"label": str(after.get("label") or "")[:24], "value": av},
    }


_BLOCK_BAR_CHARS = re.compile(r"[▀-▟]+")


def _clean_text(value: Any, limit: int) -> str:
    """One display string from model output: no block-bar glyphs, capped length."""
    if isinstance(value, bool) or value is None:
        return ""
    if isinstance(value, (int, float)):
        value = f"{value:,}" if isinstance(value, int) else str(value)
    if not isinstance(value, str):
        return ""
    text = _BLOCK_BAR_CHARS.sub("", value).strip()
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    return text


def _clean_items(value: Any, *, limit: int = 300) -> list[str]:
    if not isinstance(value, list):
        return []
    items = [_clean_text(item, limit) for item in value]
    return [item for item in items if item][:MAX_LIST_ITEMS]


def _normalize_block(raw: Any) -> dict[str, Any] | None:
    """Validate one model block against the catalog; None drops it."""
    if not isinstance(raw, dict):
        return None
    kind = raw.get("type")
    if kind == "card":
        label = _clean_text(raw.get("label"), 40)
        value = _clean_text(raw.get("value"), 40)
        if not (label or value):
            return None
        tone = str(raw.get("tone") or "neutral").strip().lower()
        return {
            "type": "card",
            "label": label,
            "value": value,
            "tone": tone if tone in CARD_TONES else "neutral",
            "detail": _clean_text(raw.get("detail"), 120),
        }
    if kind == "trend":
        trend = _normalize_trend(raw)
        return {"type": "trend", **trend} if trend else None
    if kind == "section":
        title = _clean_text(raw.get("title"), 40)
        return {"type": "section", "title": title} if title else None
    if kind == "text":
        md = _clean_text(raw.get("md") if raw.get("md") is not None else raw.get("text"), 1500)
        return {"type": "text", "md": md} if md else None
    if kind in ("bullets", "checklist", "changes"):
        items = _clean_items(raw.get("items"))
        if not items:
            return None
        block: dict[str, Any] = {"type": kind, "items": items}
        title = _clean_text(raw.get("title"), 40)
        if title and kind != "bullets":
            block["title"] = title
        return block
    if kind == "table":
        columns = [_clean_text(col, 40) for col in raw.get("columns") or []][:MAX_TABLE_COLUMNS]
        rows_raw = raw.get("rows") if isinstance(raw.get("rows"), list) else []
        if len(columns) < 2 or not rows_raw:
            return None
        width = len(columns)
        rows: list[list[str]] = []
        for row in rows_raw:
            if not isinstance(row, list):
                continue
            cells = [_clean_text(cell, 80) for cell in row[:width]]
            cells += [""] * (width - len(cells))
            if any(cells):
                rows.append(cells)
            if len(rows) >= MAX_TABLE_ROWS:
                break
        if not rows:
            return None
        block = {"type": "table", "columns": columns, "rows": rows}
        note = _clean_text(raw.get("note"), 160)
        if note:
            block["note"] = note
        return block
    if kind == "limits":
        text = _clean_text(raw.get("text"), 400)
        return {"type": "limits", "text": text} if text else None
    return None


def _scan_json_objects(text: str) -> list[Any]:
    """Every top-level JSON object in ``text``, in order.

    Tolerates the shapes models actually produce instead of clean JSON Lines:
    prose or code fences around the objects, an array wrapper, several objects
    on one line, pretty-printed objects. An object that does not parse (the
    cut-off last line) is skipped and the scan resumes at the next ``{``, so a
    truncated wrapper still yields the complete blocks nested inside it.
    """
    decoder = json.JSONDecoder()
    objects: list[Any] = []
    pos = text.find("{")
    while pos >= 0:
        try:
            obj, end = decoder.raw_decode(text, pos)
        except ValueError:
            pos = text.find("{", pos + 1)
            continue
        objects.append(obj)
        pos = text.find("{", end)
    return objects


def _blocks_to_markdown(blocks: list[dict[str, Any]]) -> str:
    """Markdown rendering of the document blocks, for ``review_md`` (wiki Q&A
    grounding) and any reader that predates the block format."""
    parts: list[str] = []
    for block in blocks:
        kind = block.get("type")
        if kind == "section":
            parts.append(f"## {block['title']}")
        elif kind == "text":
            parts.append(block["md"])
        elif kind == "bullets":
            parts.append("\n".join(f"- {item}" for item in block["items"]))
        elif kind in ("checklist", "changes"):
            title = block.get("title") or (CHECKLIST_TITLE if kind == "checklist" else CHANGES_TITLE)
            parts.append(f"## {title}\n" + "\n".join(f"- {item}" for item in block["items"]))
        elif kind == "table":
            def row_md(cells: list[str]) -> str:
                return "| " + " | ".join(cell.replace("|", "/") for cell in cells) + " |"
            lines = [row_md(block["columns"]), "|" + "---|" * len(block["columns"])]
            lines += [row_md(row) for row in block["rows"]]
            if block.get("note"):
                lines.append(f"\n{block['note']}")
            parts.append("\n".join(lines))
    return "\n\n".join(parts).strip()


def _blocks_usable(blocks: Any) -> bool:
    if not isinstance(blocks, list):
        return False
    cards = sum(1 for block in blocks if isinstance(block, dict) and block.get("type") == "card")
    content = sum(
        1 for block in blocks if isinstance(block, dict) and block.get("type") in CONTENT_BLOCK_TYPES
    )
    return cards >= MIN_USABLE_CARDS and content >= MIN_USABLE_CONTENT_BLOCKS


def _review_from_blocks(raw_blocks: list[Any]) -> dict[str, Any]:
    blocks: list[dict[str, Any]] = []
    dropped = 0
    counts: dict[str, int] = {}
    caps = {"card": MAX_CARDS, "trend": MAX_TRENDS}
    for raw in raw_blocks:
        block = _normalize_block(raw)
        if block is None or len(blocks) >= MAX_BLOCKS:
            dropped += 1
            continue
        kind = block["type"]
        if counts.get(kind, 0) >= caps.get(kind, MAX_BLOCKS):
            dropped += 1
            continue
        counts[kind] = counts.get(kind, 0) + 1
        blocks.append(block)

    def items_of(kind: str) -> list[str]:
        return [item for block in blocks if block["type"] == kind for item in block["items"]]

    def strip_type(block: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in block.items() if key != "type"}

    # cards / metric_trends / watch_items / comparison_notes / source_limits /
    # summary_md keep their old meaning, so the card strip, the trend chart,
    # wiki Q&A grounding and cached-review checks read both formats alike.
    return {
        "format": BLOCKS_FORMAT,
        "blocks": blocks,
        "dropped_blocks": dropped,
        "truncated": False,
        "cards": [strip_type(block) for block in blocks if block["type"] == "card"],
        "metric_trends": [strip_type(block) for block in blocks if block["type"] == "trend"],
        "watch_items": items_of("checklist"),
        "comparison_notes": items_of("changes"),
        "source_limits": " ".join(block["text"] for block in blocks if block["type"] == "limits"),
        "summary_md": _blocks_to_markdown(blocks),
    }


def _normalize_review(raw_text: str) -> dict[str, Any]:
    objects = _scan_json_objects(raw_text or "")
    raw_blocks: list[Any] = []
    legacy: dict[str, Any] | None = None
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        if isinstance(obj.get("type"), str):
            raw_blocks.append(obj)
        elif isinstance(obj.get("blocks"), list):
            raw_blocks.extend(obj["blocks"])
        elif legacy is None and any(key in obj for key in ("summary_md", "cards", "metric_trends")):
            legacy = obj
    if raw_blocks or (objects and legacy is None):
        # JSON came back but none of it is a usable block → an empty review,
        # which _review_payload_is_broken rejects instead of showing raw JSON.
        return _review_from_blocks(raw_blocks)
    # The older single-object schema, or an answer with no JSON at all (shown
    # as markdown, as before).
    parsed = legacy if legacy is not None else {"summary_md": (raw_text or "").strip()}
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
    """A freshly generated review is broken when it cannot be shown. Such
    payloads must never be cached.

    Block format: broken only when too few valid blocks came back. A cut-off
    answer that already holds cards and some body is kept (marked
    ``truncated``) rather than paid for again.
    Older single-object format: broken when the output was cut off
    (``finish_reason == "length"``) or the JSON could not be parsed and the raw
    blob landed in ``summary_md``."""
    if isinstance(payload, dict) and payload.get("format") == BLOCKS_FORMAT:
        return not _blocks_usable(payload.get("blocks"))
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
        **ai_config.openrouter_reasoning_controls(model, effort=DART_REVIEW_REASONING_EFFORT),
    }
    try:
        result = await ai_client.post_chat_completion(
            feature="dart_report_review",
            payload=payload,
            google_sub=google_sub,
            model=model,
            model_profile="dart_report_review",
            timeout=httpx.Timeout(120.0, read=120.0),
        )
    except ai_client.MissingOpenRouterKeyError as exc:
        raise DartReportReviewError("OpenRouter API key is not configured.") from exc
    except ai_client.OpenRouterError as exc:
        raise DartReportReviewError(f"OpenRouter call failed: {exc}") from exc

    finish_reason = _finish_reason_from_response(result.get("data"))
    review = _normalize_review(result["content"])
    if review.get("format") == BLOCKS_FORMAT and finish_reason == "length":
        review["truncated"] = True
    return review, {
        "model": result["model"],
        "tokens_in": result["input_tokens"],
        "tokens_out": result["output_tokens"],
        "cost_usd": result["cost_usd"],
        "finish_reason": finish_reason,
    }


def _failure_key(stock_code: str, rcept_no: str) -> str:
    return f"{stock_code}:{rcept_no}"


async def _output_failure(stock_code: str, rcept_no: str | None) -> dict[str, Any] | None:
    if not rcept_no:
        return None
    entry = await cache_values.get_cache_value_entry(
        CACHE_NS_REVIEW_FAILURE, _failure_key(stock_code, rcept_no), allow_stale=True
    )
    return entry.value if entry and isinstance(entry.value, dict) else None


async def _record_output_failure(stock_code: str, rcept_no: str, usage: dict[str, Any]) -> None:
    previous = await _output_failure(stock_code, rcept_no) or {}
    await cache_values.set_cache_value(
        CACHE_NS_REVIEW_FAILURE,
        _failure_key(stock_code, rcept_no),
        {
            "attempts": int(previous.get("attempts") or 0) + 1,
            "last_failed_at": datetime.now().isoformat(timespec="seconds"),
            "model": usage.get("model"),
            "finish_reason": usage.get("finish_reason"),
            "tokens_out": usage.get("tokens_out"),
        },
    )


def _failure_backoff_reason(failure: dict[str, Any] | None, *, now: datetime | None = None) -> str | None:
    """Why the background pipeline must not retry this filing yet, or None."""
    if not failure:
        return None
    if int(failure.get("attempts") or 0) >= DART_REVIEW_MAX_FAILED_ATTEMPTS:
        return "output_failed_max_attempts"
    try:
        last = datetime.fromisoformat(str(failure.get("last_failed_at")))
    except ValueError:
        return None
    if ((now or datetime.now()) - last).total_seconds() < DART_REVIEW_FAILURE_COOLDOWN_S:
        return "output_failed_cooldown"
    return None


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
            # the broken review stick until a manual regeneration. Remember the
            # failure instead, so the pipeline stops paying for it every run.
            await _record_output_failure(stock_code, primary["rcept_no"], usage)
            raise DartReviewOutputError(
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
    await cache_values.delete_cache_value(CACHE_NS_REVIEW_FAILURE, _failure_key(stock_code, primary["rcept_no"]))
    saved["cached"] = False
    return saved


def _reason_from_error(exc: Exception) -> str:
    if isinstance(exc, DartReviewOutputError):
        return "output_truncated"
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
    if not force:
        failure = await _output_failure(code, latest.get("rcept_no"))
        backoff = _failure_backoff_reason(failure)
        if backoff:
            return {
                "stock_code": code,
                "status": "skipped",
                "reason": backoff,
                "rcept_no": latest.get("rcept_no"),
                "attempts": int((failure or {}).get("attempts") or 0),
            }
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
