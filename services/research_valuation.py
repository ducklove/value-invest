"""Conservative fiscal-year EPS/BPS forecasts from published broker PDFs.

Only explicit annual estimate columns (2026E/2026F) and per-share rows are
accepted. Multiples quoted in a report, target-price assumptions, consensus
rows without an estimate marker, and ambiguous PDF table layouts are not
used. The caller divides the *current* quote by these inputs; the report's
historical share price is deliberately absent from this API.
"""

from __future__ import annotations

import asyncio
import io
import logging
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

import report_client
from cache_layer import MemoryTTLCache, cached_fetch
from core.http import get_http_client
from repositories import cache_values
from services.report_url_policy import is_allowed_report_pdf_url

logger = logging.getLogger(__name__)

_MAX_REPORTS = 3
_MAX_REPORT_AGE_DAYS = 180
_MAX_PDF_BYTES = 10 * 1024 * 1024
_MAX_PDF_PAGES = 12
_LOAD_TIMEOUT_SECONDS = 12.0
_CACHE_TTL = 24 * 60 * 60
_NO_DATA_TTL = 60 * 60
_cache = MemoryTTLCache("research.forward_valuation", _CACHE_TTL, evict_expired_after=_CACHE_TTL)
_KST = ZoneInfo("Asia/Seoul")

# Requiring four-digit years and an explicit E/F marker prevents quarterly,
# historical and 12M target-valuation assumptions from being called forward.
_YEAR = re.compile(r"(?<![\w/])((?:19|20)\d{2})\s*(?:\(([AEF])\)|([AEF]))?(?![\w/])", re.I)
_QUARTER = re.compile(r"\b(?:[1-4]Q\s*\d{2,4}|(?:19|20)\d{2}[/.-](?:03|06|09|12)|\d{2}[1-4]Q)\b", re.I)
_METRIC = re.compile(r"(?<!\S)(EPS|BPS)(?:\s*\(\s*원\s*\))?(?=\s|$)", re.I)
_VALUE = re.compile(r"(?:\(?-?\d[\d,]*(?:\.\d+)?\)?|[-−]|N/?A|N/?M)(?=\s|$)", re.I)
_WON_UNIT = re.compile(r"\([^)]*(?<![가-힣A-Za-z])원(?![가-힣A-Za-z])[^)]*\)")
_ISSUER_CODE = re.compile(r"(?<![\w,])([0-9]{5}[0-9A-Z])(?:\.(?:KS|KQ))?(?![\w,])")
_FINANCIAL_LABEL = re.compile(
    r"^(?:EPS|BPS|DPS|CFPS|SPS|PER|PBR|ROE|ROA|목표주가|현재주가|주가|주식수|시가총액|"
    r"매출액?|영업이익|당기순이익|순이익|이익|손익|자산|부채|자본|현금|배당|금액|거래)(?=\s|:|\()",
    re.I,
)


def _today() -> date:
    return datetime.now(_KST).date()


def _annual_header(line: str) -> list[tuple[int, bool]] | None:
    """Return an unambiguous consecutive annual header, including twins."""
    if _QUARTER.search(line):
        return None
    matches = list(_YEAR.finditer(line))
    if len(matches) < 2:
        return None
    # Between year cells there may only be table separators/whitespace.
    if any(line[left.end():right.start()].strip(" \t|") for left, right in zip(matches, matches[1:])):
        return None
    if line[matches[-1].end():].strip(" \t|"):
        return None
    columns = [(int(m.group(1)), bool((m.group(2) or m.group(3) or "").upper() in {"E", "F"})) for m in matches]
    # Two financial tables side by side repeat the identical year header.
    for count in range(2, min(len(columns), 8) + 1):
        first = columns[:count]
        if len(columns) % count == 0 and columns == first * (len(columns) // count):
            columns = first
            break
    years = [year for year, _ in columns]
    if len(years) > 8 or any(b != a + 1 for a, b in zip(years, years[1:])):
        return None
    return columns


def _metric_values(line: str, match: re.Match, count: int) -> list[float | None] | None:
    tail = line[match.end():].strip()
    values: list[float | None] = []
    for _ in range(count):
        cell = _VALUE.match(tail)
        if cell is None:
            return None
        raw = cell.group(0).replace(",", "")
        if raw.upper() in {"-", "−", "NA", "N/A", "NM", "N/M"}:
            values.append(None)
        else:
            if raw.startswith("("):
                if not raw.endswith(")"):
                    return None
                raw = "-" + raw[1:-1]
            try:
                values.append(float(raw))
            except ValueError:
                return None
        tail = tail[cell.end():].strip()
    # Extra numbers imply a different column count; do not silently shift.
    if tail and _VALUE.match(tail):
        return None
    return values


def extract_forward_inputs(text: str, *, today: date | None = None) -> dict | None:
    """Extract explicit KRW per-share estimates, or return no data.

    A missing/negative EPS is retained as missing: loss-making companies may
    still expose forward PBR. Never infer EPS/BPS from rounded PER/PBR.
    """
    current_year = (today or _today()).year
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in (text or "").splitlines()]
    columns: list[tuple[int, bool]] | None = None
    header_index = -1
    growth_section = False
    estimates: dict[int, dict[str, list[float]]] = {}
    for index, line in enumerate(lines):
        header = _annual_header(line)
        if header is not None:
            columns, header_index = header, index
            growth_section = False
            continue
        if _QUARTER.search(line) or index - header_index > 65:
            columns = None
        if columns is None:
            continue
        if re.search(r"성장성\s*\(", line):
            growth_section = True
        elif re.search(r"(?:주당지표|투자지표|수익성|안정성|활동성|밸류에이션)\s*\(", line):
            growth_section = False
        for match in _METRIC.finditer(line):
            metric = match.group(1).lower()
            if re.search(r"(?:수정|조정|희석|계속사업|adjusted|diluted)\s*$", line[:match.start()], re.I):
                continue
            # Bare EPS under a growth-rate section is not a per-share value.
            # A won-unit label must appear in this table or on this row.
            context = " ".join(lines[max(0, header_index - 2, index - 5):index + 1])
            if not _WON_UNIT.search(context):
                continue
            if growth_section:
                continue
            values = _metric_values(line, match, len(columns))
            if values is None:
                continue
            for (year, forecast), value in zip(columns, values):
                if forecast and current_year <= year <= current_year + 1 and value is not None and abs(value) < 100_000_000:
                    estimates.setdefault(year, {}).setdefault(metric, []).append(value)
    for year in sorted(estimates):
        values = estimates[year]
        # Repeated tables may agree; conflicting EPS/BPS rows are ambiguous.
        eps = values.get("eps", [])
        bps = values.get("bps", [])
        result = {
            "fiscal_year": year,
            "period": f"{year}E",
            "eps": eps[0] if eps and len(set(eps)) == 1 and eps[0] > 0 else None,
            "bps": bps[0] if bps and len(set(bps)) == 1 and bps[0] > 0 else None,
        }
        if result["eps"] is not None or result["bps"] is not None:
            return result
    return None


def _pdf_text(data: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    return "\n".join((page.extract_text() or "") for page in reader.pages[:_MAX_PDF_PAGES])


def _issuer_codes(text: str) -> set[str]:
    """Recognize issuer headings without treating six-digit prices as codes.

    There is no reliable issuer binding for tables inside sector reports.
    Reject them when another company heading/code occurs anywhere in the
    extracted pages, even if the requested issuer appears on the cover.
    """
    codes: set[str] = set()
    for raw in text.splitlines():
        line = raw.strip()
        matches = list(_ISSUER_CODE.finditer(line))
        if not matches or len(line) > 100 or _FINANCIAL_LABEL.search(line):
            continue
        remainder = _ISSUER_CODE.sub("", line)
        # A company heading contains its name and punctuation, not other
        # numeric data cells. A stand-alone '(005930)' cover heading is OK.
        if re.fullmatch(r"[가-힣A-Za-z\s()[\]_.·&:/-]*", remainder):
            codes.update(match.group(1) for match in matches)
    return codes


async def _report_inputs(report: dict, today: date, stock_code: str) -> dict | None:
    url = report.get("pdf_url", "")
    if not is_allowed_report_pdf_url(url):
        return None
    try:
        posted = date.fromisoformat(report.get("date", ""))
    except (TypeError, ValueError):
        return None
    if not 0 <= (today - posted).days <= _MAX_REPORT_AGE_DAYS:
        return None
    try:
        client = await get_http_client("report")
        response = await client.get(url, headers=report_client.HEADERS, timeout=5.0, follow_redirects=False)
        if response.status_code != 200 or len(response.content) > _MAX_PDF_BYTES or not response.content.startswith(b"%PDF-"):
            return None
        text = await asyncio.to_thread(_pdf_text, response.content)
        # A sector/multi-company attachment or a mismatched report-list cache
        # must not supply another share class's per-share estimates.
        if _issuer_codes(text) != {stock_code}:
            return None
        inputs = extract_forward_inputs(text, today=today)
    except Exception as exc:
        logger.debug("Forward valuation report unavailable: %s", type(exc).__name__)
        return None
    if inputs is None:
        return None
    return {
        **inputs,
        "basis": "broker_report_fiscal_year",
        "currency": "KRW",
        "source": report.get("firm", ""),
        "source_url": report.get("source_url", ""),
        "pdf_url": url,
        "as_of": posted.isoformat(),
        "as_of_basis": "report_posted_date",
        "report_title": report.get("title", ""),
    }


async def _load(stock_code: str, today: date) -> dict | None:
    # Reuse the report-list cache populated by the analysis report tab when
    # available; a valuation request never refreshes the entire report archive.
    try:
        cached = await cache_values.get_report_list(stock_code, ttl_minutes=24 * 60)
    except Exception:
        cached = None
    reports = (cached or {}).get("reports")
    if not reports:
        reports = await report_client.fetch_reports(stock_code, max_pages=1, per_page=_MAX_REPORTS)
    recent = sorted((r for r in reports if isinstance(r, dict)), key=lambda r: r.get("date", ""), reverse=True)[:_MAX_REPORTS]
    results = await asyncio.gather(*(_report_inputs(report, today, stock_code) for report in recent))
    return next((result for result in results if result is not None), None)


async def fetch_forward_valuation(stock_code: str) -> dict | None:
    """Return dated annual EPS/BPS forecasts from a recent broker report.

    Quotes are intentionally not cached with forecasts. Unsupported reports
    and upstream failures return None and never block ordinary valuations.
    """
    if not re.fullmatch(r"[0-9A-Z]{6}", stock_code or ""):
        return None
    today = _today()

    async def load() -> dict | None:
        try:
            return await asyncio.wait_for(_load(stock_code, today), timeout=_LOAD_TIMEOUT_SECONDS)
        except Exception as exc:
            logger.debug("Forward valuation unavailable for %s: %s", stock_code, type(exc).__name__)
            return None

    # The calendar year is part of the cache key so Dec estimates cannot
    # appear as forward figures after New Year, even during the 24h TTL.
    return await cached_fetch(_cache, f"{stock_code}:{today.year}", load, ttl=lambda value: _CACHE_TTL if value else _NO_DATA_TTL)
