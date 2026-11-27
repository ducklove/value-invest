"""공시 → 목표가 재가격(Pro-forma) & 주가 반응 시차 계산기 (기능id: reprice).

설계 원칙 ("틀린 숫자보다 미계산이 낫다"):
1. ``주식소각결정``:
   - finance-pi ``per_share`` 의 분모(발행주식수 vs 유통주식수)와 보통주 단독 구성이
     코드·데이터로 확정될 때만 계산한다.
   - 분모가 유통주식수이고 기취득/보유 자기주식 소각(``treasuryShares >= 소각수``)이면
     이미 분모에서 차감돼 있으므로 ``no_impact`` 로 처리한다.
   - 분모를 확정할 수 없거나 종류주식 혼합·신규취득 예정이면 ``uncalculable`` 로 두되,
     소각 주식 수·발행주식총수 대비 %·소각예정금액·소각예정일·주가 반응 등 사실은 표시한다.
2. ``현금ㆍ현물배당결정``:
   - ``배당구분`` 이 명시적으로 ``결산배당`` 일 때만 연간 DPS Pro-forma 및 푸시 줄을 계산한다.
3. 시점 신선도·정정 공시:
   - ``rcept_dt`` 가 최근 7일 이내이고 소각예정일/배당기준일이 아직 도래하지 않았을 때만
     Pro-forma·목표가 Δ 를 계산하고, 지났으면 ``stale``("이미 반영됐을 수 있음 — 주가 반응만")로 표시한다.
   - 후속 ``[기재정정]`` 이 존재하면 원본 공시는 ``정정됨`` 으로 표시하고 정정본만 계산한다.
4. 예산·보안·격리:
   - 비로그인 요청은 캐시된 원문 결과만 읽고 ``document.xml`` 을 새로 내려받지 않는다.
   - ``parse_status == "ok"`` 만 SQLite ``cache_values``(``v2``)에 영속 캐시하고 실패는 메모리 단기 캐시(1시간)만 둔다.
   - ZIP 엔트리 수(50개)·압축 해제 크기(20MB) 상한을 강제하고, 모든 파싱·계산 오류는 본 알림/응답을 막지 않게 격리한다.
"""

from __future__ import annotations

import asyncio
import io
import logging
import math
import re
import sqlite3
import warnings
import zipfile
import zlib
from datetime import date, timedelta
from html import unescape
from typing import Any

import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from core.cache import MemoryTTLCache, cached_fetch
from core.errors import AppError, DBError
from domain.timeutil import today_kst
from repositories import cache_values, corp_codes
from repositories import portfolio as portfolio_repo
from services.dart import client as dart_client
from services.market.sources import close_price as close_price_client
from services.portfolio import fx as portfolio_fx
from services.portfolio import runtime_quotes
from services.portfolio.identifiers import common_stock_code, is_korean_stock, is_preferred_stock
from services.portfolio.target_resolver import holding_value_meta, holding_value_per_share
from services.portfolio.targets import evaluate_target_formula, extract_target_variables
from services.portfolio.valuation import fetch_valuation_basis

logger = logging.getLogger(__name__)

DART_VIEWER_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
CACHE_NS_REPRICE_DOC = "dart.reprice_doc.v2"
DOC_DB_TTL_SECONDS = 90 * 86400.0
DOC_MEM_TTL_SECONDS = 24 * 3600.0
DOC_FAILURE_TTL_SECONDS = 3600.0
FILING_LIST_TTL_SECONDS = 600.0
MAX_REPRICE_DOCS = 8
PRO_FORMA_FRESH_DAYS = 7
MAX_ZIP_ENTRIES = 50
MAX_ZIP_UNCOMPRESSED_BYTES = 20 * 1024 * 1024
GAP_VERDICT_THRESHOLD_PCT_P = 2.0
NOTIFICATION_REPRICE_TIMEOUT_S = 4.0

STOCK_RETIREMENT = "stock_retirement"
CASH_DIVIDEND = "cash_dividend"
DISCLOSURE_LABELS = {
    STOCK_RETIREMENT: "주식소각결정",
    CASH_DIVIDEND: "현금ㆍ현물배당결정",
}

PRICE_REACTION_BASIS_LABEL = "접수일 직전 거래일 종가 기준"
STALE_REASON = "이미 반영됐을 수 있음 — 주가 반응만"
UNCALCULABLE_TREASURY_REASON = "기취득 자사주 소각 — 주당가치 분모에 이미 반영됐을 수 있어 계산하지 않음"

_STOCK_CODE_RE = re.compile(r"^[0-9][0-9A-Z]{5}$")
_FOOTNOTE_RE = re.compile(r"\((?:주|註)\s*\d+\)|\[(?:주|註)\s*\d+\]|\*\d+")
_NUM_TOKEN_RE = re.compile(r"[+-]?\d[\d,]*(?:\.\d+)?")
_EMPTY_CELL_VALUES = frozenset({
    "", "-", "―", "—", "–", "해당없음", "해당사항없음", "미해당", "없음", "미발행", "미정", "n/a", "na",
})
_EXCLUDED_TITLE_HINTS = (
    "자회사의주요경영사항",
    "종속회사의주요경영사항",
    "자회사",
    "종속회사",
    "철회",
    "취소",
    "조회공시",
    "미확정",
    "예고",
    "안내공시",
)
_EXISTING_TREASURY_HINTS = (
    "기취득",
    "기보유",
    "보유자기주식",
    "보유중인자기주식",
    "보유자사주",
    "배당가능이익범위내취득한자기주식",
)
_NEW_ACQUISITION_HINTS = (
    "장내직접취득",
    "장내취득",
    "장내매수",
    "신규취득",
    "향후취득",
    "신탁계약에의한취득",
    "신탁계약체결",
)

_doc_mem_cache = MemoryTTLCache("dart.reprice_doc.v2", DOC_MEM_TTL_SECONDS, evict_expired_after=DOC_MEM_TTL_SECONDS)
_doc_sem = asyncio.Semaphore(2)

_DART_ERRORS = (dart_client.DartListError, httpx.HTTPError, TimeoutError, OSError)
_DB_CACHE_ERRORS = (DBError, sqlite3.Error, OSError, ValueError, TypeError)
_REPRICE_DATA_ERRORS = (
    AppError,
    httpx.HTTPError,
    TimeoutError,
    sqlite3.Error,
    OSError,
    ValueError,
    TypeError,
    KeyError,
)
_ZIP_OPEN_ERRORS = (
    zipfile.BadZipFile,
    zlib.error,
    EOFError,
    RuntimeError,
    NotImplementedError,
    OSError,
    ValueError,
)
_XML_PARSE_ERRORS = (
    IndexError,
    AttributeError,
    RecursionError,
    ValueError,
    TypeError,
    KeyError,
)


class _ZipLimitExceededError(ValueError):
    """DART 원문 ZIP 엔트리 수 또는 압축 해제 용량 상한 초과."""


def is_supported_stock_code(code: str) -> bool:
    return bool(_STOCK_CODE_RE.match(str(code or "").strip().upper()))


def _compact_label(text: str | None) -> str:
    cleaned = unescape(str(text or ""))
    cleaned = re.sub(r"^\s*(?:\d+|[가-하]|[①-⑳])[\.\)]\s*", "", cleaned)
    cleaned = re.sub(r"\s+", "", cleaned)
    for ch in ("ㆍ", "·", "・", "•", ":", "(", ")", "[", "]", "/"):
        cleaned = cleaned.replace(ch, "")
    return cleaned.lower()


def is_amendment_disclosure(report_nm: str | None) -> bool:
    compact = re.sub(r"\s+", "", unescape(str(report_nm or "")))
    return any(tag in compact for tag in ("[기재정정]", "[정정]", "[첨부정정]", "기재정정"))


def classify_reprice_disclosure(report_nm: str | None) -> str | None:
    """보고서명이 재가격 대상 정형 공시 2종인지 판별한다."""
    raw = unescape(str(report_nm or "")).strip()
    if not raw:
        return None
    compact = re.sub(r"[\sㆍ·・•]+", "", raw)
    if any(hint in compact for hint in _EXCLUDED_TITLE_HINTS):
        return None
    if "주식소각결정" in compact:
        return STOCK_RETIREMENT
    if "현금현물배당결정" in compact or "현금배당결정" in compact:
        return CASH_DIVIDEND
    return None


def mark_superseded_disclosures(filings: list[dict[str, Any]]) -> dict[str, str]:
    """같은 공시 유형에서 후속 ``[기재정정]`` 이 있으면 이전 공시 ``rcept_no → 정정본 rcept_no`` 맵을 반환한다."""
    ordered = sorted(
        (f for f in filings if isinstance(f, dict) and f.get("rcept_no")),
        key=lambda f: (
            _iso_rcept_dt(f.get("rcept_dt")),
            str(f.get("rcept_no") or "").strip(),
        ),
        reverse=True,
    )
    active_amendment_by_type: dict[str, str] = {}
    superseded_by: dict[str, str] = {}
    for item in ordered:
        rcept_no = str(item.get("rcept_no") or "").strip()
        report_nm = str(item.get("report_nm") or "").strip()
        disc_type = item.get("disclosure_type") or classify_reprice_disclosure(report_nm)
        if not rcept_no or not disc_type:
            continue
        amended = is_amendment_disclosure(report_nm)
        newer_amendment = active_amendment_by_type.get(disc_type)
        if newer_amendment:
            superseded_by[rcept_no] = newer_amendment
            if not amended:
                # 해당 정정 체인의 최초 원본까지 도달했으므로 체인을 닫는다.
                active_amendment_by_type.pop(disc_type, None)
        elif amended:
            active_amendment_by_type[disc_type] = rcept_no
    return superseded_by


def _iso_rcept_dt(raw: str | None) -> str:
    text = str(raw or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return ""


def _decode_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def extract_xml_documents_from_zip(
    content: bytes,
    rcept_no: str = "",
    *,
    max_entries: int | None = None,
    max_uncompressed_bytes: int | None = None,
) -> list[str]:
    """``document.xml`` ZIP 에서 엔트리 수·해제 용량 상한을 검증하며 XML/HTML 본문을 추출한다."""
    entry_limit = MAX_ZIP_ENTRIES if max_entries is None else max_entries
    byte_limit = MAX_ZIP_UNCOMPRESSED_BYTES if max_uncompressed_bytes is None else max_uncompressed_bytes
    if len(content) > byte_limit:
        raise _ZipLimitExceededError("원문 압축 파일 크기가 상한을 초과합니다.")
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        infos = zf.infolist()
        if len(infos) > entry_limit:
            raise _ZipLimitExceededError("원문 압축 내 엔트리 수가 상한을 초과합니다.")
        declared_total = sum(max(0, int(info.file_size or 0)) for info in infos)
        if declared_total > byte_limit:
            raise _ZipLimitExceededError("원문 압축 해제 예정 용량이 상한을 초과합니다.")

        names = [
            info.filename for info in infos
            if not info.is_dir() and info.filename.lower().endswith((".xml", ".html", ".htm"))
        ]
        prefix = str(rcept_no or "").strip()
        names.sort(key=lambda n: (0 if prefix and n.startswith(prefix) else 1, len(n), n))

        docs: list[str] = []
        total_read = 0
        for name in names:
            remaining = byte_limit - total_read
            with zf.open(name) as member:
                chunk = member.read(remaining + 1)
            total_read += len(chunk)
            if total_read > byte_limit:
                raise _ZipLimitExceededError("원문 압축 해제 실제 용량이 상한을 초과합니다.")
            docs.append(_decode_bytes(chunk))
        return docs


def _cell_text(cell: Any) -> str:
    return re.sub(r"\s+", " ", unescape(cell.get_text(" ", strip=True))).strip()


def _safe_span(raw: Any) -> int:
    try:
        val = int(str(raw or "1").strip())
    except ValueError:
        return 1
    return max(1, min(val, 24))


def _expand_table_grid(table: Any) -> list[list[str]]:
    """HTML/DART-XML 표의 ``rowspan``·``colspan`` 을 2차원 문자열 격자로 펼친다."""
    trs = [tr for tr in table.find_all("tr") if tr.find_parent("table") == table]
    if not trs:
        return []
    grid: list[list[str | None]] = []
    for r_idx, tr in enumerate(trs):
        while len(grid) <= r_idx:
            grid.append([])
        cells = [
            c for c in tr.find_all(["td", "th", "tu", "te"])
            if c.find_parent("tr") == tr
        ]
        c_idx = 0
        row = grid[r_idx]
        for cell in cells:
            while c_idx < len(row) and row[c_idx] is not None:
                c_idx += 1
            text = _cell_text(cell)
            rspan = _safe_span(cell.get("rowspan"))
            cspan = _safe_span(cell.get("colspan"))
            for dr in range(rspan):
                target_r = r_idx + dr
                while len(grid) <= target_r:
                    grid.append([])
                target_row = grid[target_r]
                needed_len = c_idx + cspan
                if len(target_row) < needed_len:
                    target_row.extend([None] * (needed_len - len(target_row)))
                for dc in range(cspan):
                    if target_row[c_idx + dc] is None:
                        target_row[c_idx + dc] = text
            c_idx += cspan
    width = max((len(r) for r in grid), default=0)
    return [[(cell or "") for cell in r] + [""] * (width - len(r)) for r in grid if any(r)]


def _is_amendment_diff_table(grid: list[list[str]]) -> bool:
    header_blob = " ".join(" ".join(row) for row in grid[:3])
    compact = re.sub(r"\s+", "", header_blob)
    return "정정전" in compact and "정정후" in compact


def _iter_candidate_grids(xml_texts: list[str]) -> list[list[list[str]]]:
    primary: list[list[list[str]]] = []
    fallback_diff: list[list[list[str]]] = []
    for text in xml_texts:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
            soup = BeautifulSoup(text, "html.parser")
        for table in soup.find_all("table"):
            grid = _expand_table_grid(table)
            if not grid:
                continue
            if _is_amendment_diff_table(grid):
                fallback_diff.append(grid)
            else:
                primary.append(grid)
    return primary or fallback_diff


def _dedupe_consecutive(cells: list[str]) -> list[str]:
    out: list[str] = []
    for cell in cells:
        text = cell.strip()
        if not out or out[-1] != text:
            out.append(text)
    return [c for c in out if c]


def _parse_single_number(
    raw_text: str,
    *,
    allow_zero: bool = False,
    integer_only: bool = False,
    forbid_percent: bool = True,
) -> tuple[float | None, bool]:
    cleaned = _FOOTNOTE_RE.sub("", str(raw_text or "")).strip()
    compact = re.sub(r"\s+", "", cleaned).lower()
    if compact in _EMPTY_CELL_VALUES:
        return None, False
    if forbid_percent and "%" in cleaned:
        return None, True
    tokens = _NUM_TOKEN_RE.findall(cleaned)
    if not tokens:
        return None, False
    values: list[float] = []
    for tok in tokens:
        try:
            num = float(tok.replace(",", ""))
        except ValueError:
            continue
        if math.isfinite(num):
            values.append(num)
    if not values:
        return None, False
    unique = {round(v, 6) for v in values}
    if len(unique) > 1:
        return None, True
    val = values[0]
    if val < 0 or (not allow_zero and val == 0):
        return (0.0 if allow_zero and val == 0 else None), False
    if integer_only and abs(val - round(val)) > 1e-6:
        return None, True
    return (float(round(val)) if integer_only else val), False


def _normalize_date_text(raw: str | None) -> str | None:
    text = str(raw or "").strip()
    m = re.search(r"(\d{4})\s*[-./년]\s*(\d{1,2})\s*[-./월]\s*(\d{1,2})", text)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
    except ValueError:
        return None


def _share_class_of(text: str) -> str | None:
    compact = _compact_label(text)
    if not compact:
        return None
    if "보통주" in compact or compact == "보통":
        return "common"
    if "종류주" in compact or "우선주" in compact or compact in {"종류", "우선"}:
        return "preferred"
    return None


def _extract_inline_class_numbers(text: str, *, integer_only: bool) -> tuple[dict[str, float], bool]:
    cleaned = _FOOTNOTE_RE.sub("", str(text or ""))
    found: dict[str, float] = {}
    ambiguous = False
    patterns = (
        ("common", r"보통주(?:식)?\s*[:：]?\s*([0-9][0-9,]*(?:\.\d+)?)"),
        ("preferred", r"(?:종류주(?:식)?|우선주(?:식)?)\s*[:：]?\s*([0-9][0-9,]*(?:\.\d+)?)"),
    )
    for key, pat in patterns:
        matches = re.findall(pat, cleaned)
        if len(matches) > 1 and len(set(matches)) > 1:
            ambiguous = True
        elif len(matches) == 1:
            val, amb = _parse_single_number(matches[0], allow_zero=True, integer_only=integer_only)
            if amb:
                ambiguous = True
            elif val is not None:
                found[key] = val
    return found, ambiguous


def _record_field(store: dict[str, Any], key: str, value: float | None) -> bool:
    if value is None:
        return False
    prev = store.get(key)
    if prev is not None and abs(float(prev) - float(value)) > 1e-6:
        return True
    store[key] = value
    return False


def _is_unit_only_cell(text: str) -> bool:
    return re.sub(r"\s+", "", text) in {"주", "원", "%", "(원)", "(주)", "(%)"}


def _value_cells_after(cells: list[str], start_idx: int) -> str:
    candidates = [c for c in cells[start_idx:] if c and not _is_unit_only_cell(c)]
    return candidates[0] if len(candidates) == 1 else " / ".join(candidates)


def parse_stock_retirement_xml(xml_texts: list[str]) -> dict[str, Any]:
    """``주식소각결정`` 원문(``document.xml``)에서 소각 주식 수·발행주식총수·예정일·취득방법을 추출한다."""
    extracted: dict[str, Any] = {
        "retired_shares_common": None,
        "retired_shares_preferred": None,
        "issued_shares_common": None,
        "issued_shares_preferred": None,
        "planned_amount_krw": None,
        "scheduled_date": None,
        "acquisition_method": None,
    }
    ambiguous = False

    for grid in _iter_candidate_grids(xml_texts):
        header_map: dict[str, int] = {}
        header_row_idx = -1
        for r_idx, raw_row in enumerate(grid[:3]):
            labels = [_compact_label(c) for c in raw_row]
            col_ret = next(
                (
                    i for i, lb in enumerate(labels)
                    if ("소각할주식" in lb or "소각예정주식" in lb or lb == "소각주식수")
                    and "금액" not in lb and "기간" not in lb and "방법" not in lb
                ),
                None,
            )
            col_class = next((i for i, lb in enumerate(labels) if "주식의종류" in lb or "주식종류" in lb or lb == "구분"), None)
            if col_ret is not None and col_class is not None and col_ret != col_class:
                header_row_idx = r_idx
                header_map["class"] = col_class
                header_map["retired"] = col_ret
                col_iss = next((i for i, lb in enumerate(labels) if "발행주식총수" in lb), None)
                if col_iss is not None:
                    header_map["issued"] = col_iss
                break

        if header_map:
            for raw_row in grid[header_row_idx + 1 :]:
                s_cls = _share_class_of(raw_row[header_map["class"]]) if header_map["class"] < len(raw_row) else None
                if not s_cls:
                    continue
                if header_map["retired"] < len(raw_row):
                    val, amb = _parse_single_number(raw_row[header_map["retired"]], allow_zero=True, integer_only=True)
                    ambiguous = ambiguous or amb or _record_field(extracted, f"retired_shares_{s_cls}", val)
                if "issued" in header_map and header_map["issued"] < len(raw_row):
                    val, amb = _parse_single_number(raw_row[header_map["issued"]], allow_zero=True, integer_only=True)
                    ambiguous = ambiguous or amb or _record_field(extracted, f"issued_shares_{s_cls}", val)
            continue

        for raw_row in grid:
            cells = _dedupe_consecutive(raw_row)
            if len(cells) < 2:
                continue
            head = _compact_label(cells[0])
            sub = _compact_label(cells[1]) if len(cells) >= 3 else ""

            is_retired_row = (
                ("소각할주식의수" in head or "소각할주식의종류와수" in head or "소각예정주식수" in head or head == "소각주식수")
                and "취득예정기간" not in head
                and "취득방법" not in head
                and "금액" not in head
                and "가액" not in head
            )
            if is_retired_row:
                s_cls = _share_class_of(cells[1]) if len(cells) >= 3 else None
                if s_cls:
                    val_text = _value_cells_after(cells, 2)
                    val, amb = _parse_single_number(val_text, allow_zero=True, integer_only=True)
                    ambiguous = ambiguous or amb or _record_field(extracted, f"retired_shares_{s_cls}", val)
                else:
                    val_text = _value_cells_after(cells, 1)
                    inline, amb = _extract_inline_class_numbers(val_text, integer_only=True)
                    ambiguous = ambiguous or amb
                    if inline:
                        for k, v in inline.items():
                            ambiguous = ambiguous or _record_field(extracted, f"retired_shares_{k}", v)
                    else:
                        val, amb2 = _parse_single_number(val_text, allow_zero=False, integer_only=True)
                        ambiguous = ambiguous or amb2 or _record_field(extracted, "retired_shares_common", val)
                continue

            if "발행주식총수" in head and "비율" not in head:
                s_cls = _share_class_of(cells[1]) if len(cells) >= 3 else None
                if s_cls:
                    val_text = _value_cells_after(cells, 2)
                    val, amb = _parse_single_number(val_text, allow_zero=True, integer_only=True)
                    ambiguous = ambiguous or amb or _record_field(extracted, f"issued_shares_{s_cls}", val)
                else:
                    val_text = _value_cells_after(cells, 1)
                    inline, amb = _extract_inline_class_numbers(val_text, integer_only=True)
                    ambiguous = ambiguous or amb
                    if inline:
                        for k, v in inline.items():
                            ambiguous = ambiguous or _record_field(extracted, f"issued_shares_{k}", v)
                    else:
                        val, amb2 = _parse_single_number(val_text, allow_zero=False, integer_only=True)
                        ambiguous = ambiguous or amb2 or _record_field(extracted, "issued_shares_common", val)
                continue

            if "소각예정금액" in head and extracted["planned_amount_krw"] is None:
                val, _ = _parse_single_number(_value_cells_after(cells, 1), allow_zero=False)
                if val is not None:
                    extracted["planned_amount_krw"] = val
            elif "소각예정일" in head and extracted["scheduled_date"] is None:
                extracted["scheduled_date"] = _normalize_date_text(" ".join(cells[1:]))
            elif ("취득방법" in head or "주식소각방법" in head) and extracted["acquisition_method"] is None:
                method = _value_cells_after(cells, 1)
                if method and _compact_label(method) not in _EMPTY_CELL_VALUES:
                    extracted["acquisition_method"] = method[:80]
            elif "소각할주식" in sub:
                s_cls = _share_class_of(cells[2]) if len(cells) >= 4 else None
                if s_cls:
                    val, amb = _parse_single_number(_value_cells_after(cells, 3), allow_zero=True, integer_only=True)
                    ambiguous = ambiguous or amb or _record_field(extracted, f"retired_shares_{s_cls}", val)

    for int_key in (
        "retired_shares_common",
        "retired_shares_preferred",
        "issued_shares_common",
        "issued_shares_preferred",
    ):
        if extracted[int_key] is not None:
            extracted[int_key] = int(round(extracted[int_key]))

    ret_common = extracted["retired_shares_common"] or 0
    ret_pref = extracted["retired_shares_preferred"] or 0
    if ambiguous:
        return {
            "parse_status": "unparseable",
            "parse_message": "수치 확인 불가 (공시 내 복수·모호한 소각 주식 수)",
            "extracted": extracted,
        }
    if ret_common <= 0 and ret_pref <= 0:
        return {
            "parse_status": "unparseable",
            "parse_message": "수치 확인 불가 (소각 주식 수를 찾지 못함)",
            "extracted": extracted,
        }
    return {"parse_status": "ok", "parse_message": None, "extracted": extracted}


def parse_cash_dividend_xml(xml_texts: list[str]) -> dict[str, Any]:
    """``현금ㆍ현물배당결정`` 원문(``document.xml``)에서 1주당 배당금·배당구분·배당기준일을 추출한다."""
    extracted: dict[str, Any] = {
        "dividend_category": None,
        "dividend_kind": None,
        "dps_common": None,
        "dps_preferred": None,
        "yield_common_pct": None,
        "yield_preferred_pct": None,
        "total_dividend_krw": None,
        "record_date": None,
        "differential": None,
    }
    ambiguous = False

    for grid in _iter_candidate_grids(xml_texts):
        header_map: dict[str, int] = {}
        header_row_idx = -1
        for r_idx, raw_row in enumerate(grid[:3]):
            labels = [_compact_label(c) for c in raw_row]
            col_dps = next(
                (i for i, lb in enumerate(labels) if ("1주당배당금" in lb or "주당배당금" in lb or "1주당현금배당금" in lb) and "총액" not in lb),
                None,
            )
            col_class = next((i for i, lb in enumerate(labels) if "주식종류" in lb or "주식의종류" in lb or lb == "구분"), None)
            if col_dps is not None and col_class is not None and col_dps != col_class:
                header_row_idx = r_idx
                header_map["class"] = col_class
                header_map["dps"] = col_dps
                for name, token in (("category", "배당구분"), ("kind", "배당종류"), ("yield", "시가배당")):
                    idx = next((i for i, lb in enumerate(labels) if token in lb), None)
                    if idx is not None:
                        header_map[name] = idx
                break

        if header_map:
            for raw_row in grid[header_row_idx + 1 :]:
                s_cls = _share_class_of(raw_row[header_map["class"]]) if header_map["class"] < len(raw_row) else None
                if not s_cls:
                    continue
                if "category" in header_map and extracted["dividend_category"] is None and header_map["category"] < len(raw_row):
                    extracted["dividend_category"] = raw_row[header_map["category"]].strip() or None
                if "kind" in header_map and extracted["dividend_kind"] is None and header_map["kind"] < len(raw_row):
                    extracted["dividend_kind"] = raw_row[header_map["kind"]].strip() or None
                if header_map["dps"] < len(raw_row):
                    val, amb = _parse_single_number(raw_row[header_map["dps"]], allow_zero=True)
                    ambiguous = ambiguous or amb or _record_field(extracted, f"dps_{s_cls}", val)
                if "yield" in header_map and header_map["yield"] < len(raw_row):
                    y_val, _ = _parse_single_number(raw_row[header_map["yield"]], allow_zero=True, forbid_percent=False)
                    if y_val is not None:
                        extracted[f"yield_{s_cls}_pct"] = y_val
            continue

        for raw_row in grid:
            cells = _dedupe_consecutive(raw_row)
            if len(cells) < 2:
                continue
            head = _compact_label(cells[0])
            sub = _compact_label(cells[1]) if len(cells) >= 3 else ""

            if head == "배당구분" and extracted["dividend_category"] is None:
                extracted["dividend_category"] = _value_cells_after(cells, 1)[:40] or None
                continue
            if head == "배당종류" and extracted["dividend_kind"] is None:
                extracted["dividend_kind"] = _value_cells_after(cells, 1)[:40] or None
                continue
            if "차등배당" in head or "차등배당" in sub:
                idx = 2 if "차등배당" in sub else 1
                extracted["differential"] = _value_cells_after(cells, idx)[:40] or None
                continue

            is_dps_row = (
                ("1주당배당금" in head or "주당배당금" in head or "1주당현금배당금" in head)
                and "총액" not in head
                and "시가배당" not in head
                and "가액" not in head
            )
            if is_dps_row:
                s_cls = _share_class_of(cells[1]) if len(cells) >= 3 else None
                if s_cls:
                    val_text = _value_cells_after(cells, 2)
                    val, amb = _parse_single_number(val_text, allow_zero=True)
                    ambiguous = ambiguous or amb or _record_field(extracted, f"dps_{s_cls}", val)
                else:
                    val_text = _value_cells_after(cells, 1)
                    inline, amb = _extract_inline_class_numbers(val_text, integer_only=False)
                    ambiguous = ambiguous or amb
                    if inline:
                        for k, v in inline.items():
                            ambiguous = ambiguous or _record_field(extracted, f"dps_{k}", v)
                    else:
                        val, amb2 = _parse_single_number(val_text, allow_zero=True)
                        ambiguous = ambiguous or amb2 or _record_field(extracted, "dps_common", val)
                continue

            if "시가배당" in head:
                s_cls = _share_class_of(cells[1]) if len(cells) >= 3 else "common"
                idx = 2 if len(cells) >= 3 and _share_class_of(cells[1]) else 1
                y_val, _ = _parse_single_number(_value_cells_after(cells, idx), allow_zero=True, forbid_percent=False)
                if y_val is not None and s_cls in {"common", "preferred"}:
                    extracted[f"yield_{s_cls}_pct"] = y_val
            elif "배당금총액" in head and extracted["total_dividend_krw"] is None:
                t_val, _ = _parse_single_number(_value_cells_after(cells, 1), allow_zero=False)
                if t_val is not None:
                    extracted["total_dividend_krw"] = t_val
            elif "배당기준일" in head and extracted["record_date"] is None:
                extracted["record_date"] = _normalize_date_text(" ".join(cells[1:]))

    kind_compact = _compact_label(extracted["dividend_kind"])
    if "현물" in kind_compact and "현금" not in kind_compact and extracted["dps_common"] is None and extracted["dps_preferred"] is None:
        return {
            "parse_status": "unparseable",
            "parse_message": "수치 확인 불가 (현물배당 단독 공시)",
            "extracted": extracted,
        }
    if ambiguous:
        return {
            "parse_status": "unparseable",
            "parse_message": "수치 확인 불가 (차등·복수 배당금 표기 — 원문 확인)",
            "extracted": extracted,
        }
    has_common = extracted["dps_common"] is not None and extracted["dps_common"] > 0
    has_pref = extracted["dps_preferred"] is not None and extracted["dps_preferred"] > 0
    if not has_common and not has_pref:
        return {
            "parse_status": "unparseable",
            "parse_message": "수치 확인 불가 (1주당 현금배당금을 찾지 못함)",
            "extracted": extracted,
        }
    return {"parse_status": "ok", "parse_message": None, "extracted": extracted}


def parse_reprice_document_zip(content: bytes, disclosure_type: str, rcept_no: str = "") -> dict[str, Any]:
    """원문 ZIP 바이트를 안전하게 파싱한다. 손상·용량초과·내부 파싱 예외 모두 ``unparseable`` 로 변환."""
    try:
        xml_texts = extract_xml_documents_from_zip(content, rcept_no=rcept_no)
    except _ZipLimitExceededError as exc:
        return {
            "parse_status": "unparseable",
            "parse_message": f"수치 확인 불가 ({exc})",
            "extracted": {},
        }
    except _ZIP_OPEN_ERRORS:
        return {
            "parse_status": "unparseable",
            "parse_message": "수치 확인 불가 (원문 압축 파일을 열지 못함)",
            "extracted": {},
        }
    if not xml_texts:
        return {
            "parse_status": "unparseable",
            "parse_message": "수치 확인 불가 (원문 XML 문서 없음)",
            "extracted": {},
        }
    try:
        if disclosure_type == STOCK_RETIREMENT:
            return parse_stock_retirement_xml(xml_texts)
        if disclosure_type == CASH_DIVIDEND:
            return parse_cash_dividend_xml(xml_texts)
    except _XML_PARSE_ERRORS as exc:
        logger.warning("reprice xml parse error (%s %s): %s", disclosure_type, rcept_no, exc)
        return {
            "parse_status": "unparseable",
            "parse_message": "수치 확인 불가 (원문 표 구조 해석 실패)",
            "extracted": {},
        }
    return {"parse_status": "unparseable", "parse_message": "지원하지 않는 공시 유형", "extracted": {}}


async def _fetch_and_parse_document(
    rcept_no: str,
    disclosure_type: str,
    *,
    allow_download: bool = True,
    essential: bool = False,
) -> dict[str, Any]:
    """``rcept_no`` 별 원문 파싱 결과 조회.

    - ``parse_status == "ok"`` 만 SQLite ``cache_values``(v2, 90일)에 영속 저장하고,
      실패 결과는 인메모리 단기 캐시(1시간)에만 둔다.
    - ``allow_download=False``(비로그인 요청)이면 캐시에 없을 때 upstream 원문을 받지 않는다.
    """
    cache_key = f"{disclosure_type}:{rcept_no}"
    mem_hit = _doc_mem_cache.get(cache_key)
    if mem_hit is not None:
        return mem_hit

    try:
        db_entry = await cache_values.get_cache_value_entry(CACHE_NS_REPRICE_DOC, cache_key)
    except _DB_CACHE_ERRORS:
        db_entry = None
    if db_entry is not None and isinstance(db_entry.value, dict) and db_entry.value.get("parse_status") == "ok":
        _doc_mem_cache.set(cache_key, db_entry.value)
        return db_entry.value

    if not allow_download:
        return {
            "parse_status": "cache_miss",
            "parse_message": "로그인 시 공시 원문 수치를 분석합니다",
            "extracted": {},
        }

    async def _load() -> dict[str, Any]:
        async with _doc_sem:
            raw_zip = await dart_client.fetch_document_xml_bytes(rcept_no, essential=essential)
        parsed = parse_reprice_document_zip(raw_zip, disclosure_type, rcept_no=rcept_no)
        if parsed.get("parse_status") == "ok":
            try:
                await cache_values.set_cache_value(
                    CACHE_NS_REPRICE_DOC, cache_key, parsed, ttl_seconds=DOC_DB_TTL_SECONDS
                )
            except _DB_CACHE_ERRORS as exc:
                logger.debug("reprice doc db cache write skipped (%s): %s", cache_key, exc)
        return parsed

    def _ttl_for(parsed: dict[str, Any]) -> float:
        return DOC_MEM_TTL_SECONDS if parsed.get("parse_status") == "ok" else DOC_FAILURE_TTL_SECONDS

    try:
        return await cached_fetch(_doc_mem_cache, cache_key, _load, ttl=_ttl_for)
    except dart_client.DartQuotaError:
        return {
            "parse_status": "quota",
            "parse_message": "수치 확인 불가 (공시 원문 조회 한도를 알림용으로 보호 중)",
            "extracted": {},
        }
    except _DART_ERRORS as exc:
        logger.info("reprice document fetch failed (%s): %s", rcept_no, exc)
        return {
            "parse_status": "error",
            "parse_message": "수치 확인 불가 (DART 원문 조회 실패)",
            "extracted": {},
        }


def _safe_num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if math.isfinite(num) else None


def _fmt_krw_compact(amount: float | None) -> str | None:
    val = _safe_num(amount)
    if val is None or val <= 0:
        return None
    if val >= 1e8:
        return f"{val / 1e8:,.1f}억원"
    return f"{round(val):,}원"


def _build_retirement_facts(extracted: dict[str, Any]) -> tuple[dict[str, Any], str]:
    ret_common = int(extracted.get("retired_shares_common") or 0)
    ret_pref = int(extracted.get("retired_shares_preferred") or 0)
    iss_common = int(extracted.get("issued_shares_common") or 0)
    iss_pref = int(extracted.get("issued_shares_preferred") or 0)
    planned = _safe_num(extracted.get("planned_amount_krw"))
    sched = extracted.get("scheduled_date")
    method = extracted.get("acquisition_method")

    common_ratio_pct = round(ret_common / iss_common * 100.0, 2) if (ret_common > 0 and iss_common > 0) else None
    pref_ratio_pct = round(ret_pref / iss_pref * 100.0, 2) if (ret_pref > 0 and iss_pref > 0) else None

    parts: list[str] = []
    if ret_common > 0:
        ratio_txt = f"(발행보통주 대비 {common_ratio_pct:.2f}%)" if common_ratio_pct is not None else ""
        parts.append(f"보통주 소각 {ret_common:,}주{ratio_txt}")
    if ret_pref > 0:
        ratio_txt = f"(발행종류주 대비 {pref_ratio_pct:.2f}%)" if pref_ratio_pct is not None else ""
        parts.append(f"종류주 소각 {ret_pref:,}주{ratio_txt}")
    amt_txt = _fmt_krw_compact(planned)
    if amt_txt:
        parts.append(f"소각예정금액 {amt_txt}")
    if sched:
        parts.append(f"소각예정일 {sched}")

    facts = {
        "retired_shares_common": ret_common or None,
        "retired_shares_preferred": ret_pref or None,
        "issued_shares_common": iss_common or None,
        "issued_shares_preferred": iss_pref or None,
        "issued_common_ratio_pct": common_ratio_pct,
        "issued_preferred_ratio_pct": pref_ratio_pct,
        "planned_amount_krw": planned,
        "scheduled_date": sched,
        "acquisition_method": method,
    }
    return facts, " · ".join(parts)


def _classify_acquisition_method(method_raw: str | None) -> str:
    compact = _compact_label(method_raw)
    if not compact:
        return "unknown"
    has_existing = any(h in compact for h in _EXISTING_TREASURY_HINTS)
    has_new = any(h in compact for h in _NEW_ACQUISITION_HINTS)
    if has_existing and not has_new:
        return "existing_treasury"
    if has_new and not has_existing:
        return "new_acquisition"
    return "unknown"


def _confirmed_basis_denominator(basis: dict[str, Any], extracted: dict[str, Any]) -> tuple[str | None, str | None]:
    """finance-pi 기초재무에서 보통주 단독 구성 및 분모(발행 vs 유통) 확정 여부를 검증한다.

    반환: ``(denominator_kind, uncalculable_reason)`` — ``denominator_kind`` 는 ``"outstanding"`` | ``"issued"`` | ``None``.
    """
    if basis.get("source") != "internal_fundamentals":
        return None, UNCALCULABLE_TREASURY_REASON

    bps_excl = basis.get("bpsTreasuryExcluded")
    eps_excl = basis.get("epsTreasuryExcluded")
    has_bps = _safe_num(basis.get("bps")) is not None
    has_eps = _safe_num(basis.get("eps")) is not None
    if not has_bps and not has_eps:
        return None, UNCALCULABLE_TREASURY_REASON

    bps_flag = bps_excl if (has_bps and isinstance(bps_excl, bool)) else None
    eps_flag = eps_excl if (has_eps and isinstance(eps_excl, bool)) else None
    if bps_flag is None and eps_flag is None:
        return None, UNCALCULABLE_TREASURY_REASON
    if bps_flag is not None and eps_flag is not None and bps_flag != eps_flag:
        return None, "BPS와 EPS의 자기주식 차감 기준이 서로 달라 계산하지 않음"
    effective_flag = bps_flag if bps_flag is not None else eps_flag

    comp = basis.get("shareComposition")
    iss_pref_filing = int(extracted.get("issued_shares_preferred") or 0)
    iss_common_filing = int(extracted.get("issued_shares_common") or 0)
    if comp == "mixed":
        return None, "보통주·종류주 혼합 기준이라 주당가치 분모 구성을 확정할 수 없어 미계산"
    if comp == "single_unspecified":
        # components 분해가 없을 때는 공시상 종류주식 미발행(0)이고 보통주 발행주식수가 일치할 때만 보통주 단독으로 인정
        basis_issued = int(round(_safe_num(basis.get("issuedShares")) or 0))
        if iss_pref_filing > 0 or iss_common_filing <= 0 or basis_issued != iss_common_filing:
            return None, "기준 주식수 구성(보통주·종류주 분리 여부)이 확정되지 않아 미계산"
    elif comp != "common_only":
        return None, "기준 주식수 구성이 확정되지 않아 미계산"

    return ("outstanding" if effective_flag else "issued"), None


def _check_freshness(
    disclosure_type: str,
    rcept_dt_iso: str,
    extracted: dict[str, Any],
    *,
    as_of: date,
) -> tuple[bool, str | None]:
    """``rcept_dt`` 가 최근 7일 이내이고 소각예정일/배당기준일이 아직 오지 않았는지 확인."""
    if not rcept_dt_iso:
        return False, STALE_REASON
    try:
        filing_day = date.fromisoformat(rcept_dt_iso)
    except ValueError:
        return False, STALE_REASON
    age_days = (as_of - filing_day).days
    if age_days > PRO_FORMA_FRESH_DAYS or age_days < -1:
        return False, STALE_REASON

    eff_raw = extracted.get("scheduled_date") if disclosure_type == STOCK_RETIREMENT else extracted.get("record_date")
    if not eff_raw:
        return False, STALE_REASON
    try:
        eff_day = date.fromisoformat(str(eff_raw))
    except ValueError:
        return False, STALE_REASON
    if eff_day <= as_of:
        return False, STALE_REASON
    return True, None


def compute_pro_forma(
    disclosure_type: str,
    parsed: dict[str, Any],
    *,
    stock_code: str,
    basis: dict[str, Any],
    trailing_dps: float | None,
    rcept_dt: str = "",
    as_of: date | None = None,
) -> dict[str, Any]:
    """공시 추출값과 현재 주당 기초지표로 Pro-forma 수치를 계산한다."""
    p_status = parsed.get("parse_status")
    if p_status == "superseded":
        reason = "정정됨 — 후속 기재정정 공시에서 계산"
        return {"status": "superseded", "reason": reason, "summary": reason, "assumption": None}
    if p_status != "ok":
        reason = parsed.get("parse_message") or "수치 확인 불가"
        return {
            "status": "cache_miss" if p_status == "cache_miss" else "unavailable",
            "reason": reason,
            "summary": reason,
            "assumption": None,
        }

    today = as_of or today_kst()
    extracted = parsed.get("extracted") or {}
    preferred = is_preferred_stock(stock_code)

    if disclosure_type == STOCK_RETIREMENT:
        facts, facts_summary = _build_retirement_facts(extracted)
        is_fresh, stale_reason = _check_freshness(STOCK_RETIREMENT, rcept_dt, extracted, as_of=today)
        if not is_fresh:
            reason = stale_reason or STALE_REASON
            summary = f"{facts_summary} · {reason}" if facts_summary else reason
            return {
                "status": "stale",
                "reason": reason,
                "facts": facts,
                "facts_summary": facts_summary,
                "summary": summary,
                "assumption": None,
            }

        ret_common = int(extracted.get("retired_shares_common") or 0)
        ret_pref = int(extracted.get("retired_shares_preferred") or 0)
        if preferred or (ret_common <= 0 and ret_pref > 0):
            reason = "종류주식 소각 — 보통주 기준 주당가치(BPS·EPS)에 반영하지 않음"
            return {
                "status": "uncalculable",
                "reason": reason,
                "facts": facts,
                "facts_summary": facts_summary,
                "summary": f"{facts_summary} · {reason}" if facts_summary else reason,
                "assumption": None,
            }
        if ret_common > 0 and ret_pref > 0:
            reason = "보통주·종류주 동시 소각 — 주당가치 귀속 구성을 확정할 수 없어 미계산"
            return {
                "status": "uncalculable",
                "reason": reason,
                "facts": facts,
                "facts_summary": facts_summary,
                "summary": f"{facts_summary} · {reason}" if facts_summary else reason,
                "assumption": None,
            }

        acq_type = _classify_acquisition_method(extracted.get("acquisition_method"))
        denom_kind, denom_err = _confirmed_basis_denominator(basis, extracted)
        if denom_kind is None:
            reason = UNCALCULABLE_TREASURY_REASON if acq_type != "new_acquisition" else (denom_err or UNCALCULABLE_TREASURY_REASON)
            if denom_err and acq_type == "existing_treasury" and "혼합" in denom_err:
                reason = denom_err
            elif denom_err and "구성" in denom_err:
                reason = denom_err
            return {
                "status": "uncalculable",
                "reason": reason,
                "facts": facts,
                "facts_summary": facts_summary,
                "summary": f"{facts_summary} · {reason}" if facts_summary else reason,
                "assumption": None,
            }

        if acq_type == "new_acquisition":
            reason = "신규 취득 예정 자사주 소각 — 취득금액만큼 자본총계가 함께 변동하므로 미계산"
            return {
                "status": "uncalculable",
                "reason": reason,
                "facts": facts,
                "facts_summary": facts_summary,
                "summary": f"{facts_summary} · {reason}" if facts_summary else reason,
                "assumption": None,
            }
        if acq_type != "existing_treasury":
            reason = UNCALCULABLE_TREASURY_REASON
            return {
                "status": "uncalculable",
                "reason": reason,
                "facts": facts,
                "facts_summary": facts_summary,
                "summary": f"{facts_summary} · {reason}" if facts_summary else reason,
                "assumption": None,
            }

        # 이제 acq_type == "existing_treasury" 이고 보통주 단독 구성 + 분모 종류가 확정됨.
        if denom_kind == "outstanding":
            treasury_shares = _safe_num(basis.get("treasuryShares"))
            if treasury_shares is not None and round(treasury_shares) >= ret_common:
                reason = "기취득 자사주 소각 — 유통주식수 기준 주당가치 분모에 이미 차감돼 영향 없음"
                return {
                    "status": "no_impact",
                    "reason": reason,
                    "facts": facts,
                    "facts_summary": facts_summary,
                    "summary": f"{facts_summary} · {reason}" if facts_summary else reason,
                    "assumption": None,
                }
            return {
                "status": "uncalculable",
                "reason": UNCALCULABLE_TREASURY_REASON,
                "facts": facts,
                "facts_summary": facts_summary,
                "summary": f"{facts_summary} · {UNCALCULABLE_TREASURY_REASON}" if facts_summary else UNCALCULABLE_TREASURY_REASON,
                "assumption": None,
            }

        # denom_kind == "issued": 기존 BPS·EPS 가 발행주식총수 분모이고 기취득 자사주를 소각하는 경우
        base_shares = int(round(_safe_num(basis.get("issuedShares")) or 0))
        if base_shares <= 0 or ret_common >= base_shares:
            reason = "기준 발행주식수를 확정할 수 없어 미계산"
            return {
                "status": "uncalculable",
                "reason": reason,
                "facts": facts,
                "facts_summary": facts_summary,
                "summary": f"{facts_summary} · {reason}" if facts_summary else reason,
                "assumption": None,
            }

        post_shares = base_shares - ret_common
        multiplier = base_shares / post_shares
        retired_pct = round(ret_common / base_shares * 100.0, 2)
        uplift_pct = round((multiplier - 1.0) * 100.0, 2)
        pre_bps = _safe_num(basis.get("bps")) if basis.get("bpsTreasuryExcluded") is False else None
        pre_eps = _safe_num(basis.get("eps")) if basis.get("epsTreasuryExcluded") is False else None
        post_bps = round(pre_bps * multiplier, 2) if pre_bps is not None else None
        post_eps = round(pre_eps * multiplier, 2) if pre_eps is not None else None

        parts = [facts_summary] if facts_summary else [f"보통주 소각 {ret_common:,}주({retired_pct:.2f}%)"]
        if pre_bps is not None and post_bps is not None:
            parts.append(f"BPS {round(pre_bps):,}→{round(post_bps):,}원({uplift_pct:+.1f}%)")
        if pre_eps is not None and post_eps is not None:
            parts.append(f"EPS {round(pre_eps):,}→{round(post_eps):,}원({uplift_pct:+.1f}%)")
        return {
            "status": "ok",
            "reason": None,
            "facts": facts,
            "facts_summary": facts_summary,
            "retired_shares": ret_common,
            "base_shares": base_shares,
            "base_shares_label": "발행주식수(보통주)",
            "post_shares": post_shares,
            "retired_pct": retired_pct,
            "per_share_uplift_pct": uplift_pct,
            "pre_bps": pre_bps,
            "post_bps": post_bps,
            "pre_eps": pre_eps,
            "post_eps": post_eps,
            "pre_dps": None,
            "post_dps": None,
            "summary": " · ".join(p for p in parts if p),
            "assumption": (
                f"기취득 자기주식 소각으로 자본총계·순이익 변동 없이 발행보통주 분모({base_shares:,}주)만 "
                f"{post_shares:,}주로 감소하는 잠정(Pro-forma) 계산입니다(기존 DB 미변경)."
            ),
        }

    # CASH_DIVIDEND
    declared_dps = _safe_num(extracted.get("dps_preferred") if preferred else extracted.get("dps_common"))
    share_label = "종류주" if preferred else "보통주"
    if declared_dps is None or declared_dps <= 0:
        reason = f"수치 확인 불가 ({share_label} 1주당 배당금 없음)"
        return {
            "status": "unavailable",
            "reason": reason,
            "summary": reason,
            "assumption": None,
        }

    category = str(extracted.get("dividend_category") or "").strip()
    cat_compact = _compact_label(category)
    rec_date = extracted.get("record_date")
    div_facts = {
        "dividend_category": category or None,
        "share_class": share_label,
        "declared_dps": declared_dps,
        "record_date": rec_date,
    }
    fact_bits = [f"{category + ' ' if category else ''}{share_label} 주당 {round(declared_dps):,}원"]
    if rec_date:
        fact_bits.append(f"배당기준일 {rec_date}")
    facts_summary = " · ".join(fact_bits)

    is_year_end_confirmed = ("결산" in cat_compact) and ("분기" not in cat_compact) and ("중간" not in cat_compact)
    if not is_year_end_confirmed:
        if "분기" in cat_compact or "중간" in cat_compact:
            reason = f"{category} 단건 공시만으로는 연간 누적 DPS를 확정할 수 없어 미계산"
        else:
            reason = "배당구분(결산배당 여부)이 확인되지 않아 연간 DPS로 가정하지 않고 미계산"
        return {
            "status": "uncalculable",
            "reason": reason,
            "facts": div_facts,
            "facts_summary": facts_summary,
            "declared_dps": declared_dps,
            "dividend_category": category or None,
            "dividend_category_confirmed": False,
            "pre_dps": trailing_dps,
            "post_dps": None,
            "summary": f"{facts_summary} · {reason}",
            "assumption": None,
        }

    is_fresh, stale_reason = _check_freshness(CASH_DIVIDEND, rcept_dt, extracted, as_of=today)
    if not is_fresh:
        reason = stale_reason or STALE_REASON
        return {
            "status": "stale",
            "reason": reason,
            "facts": div_facts,
            "facts_summary": facts_summary,
            "declared_dps": declared_dps,
            "dividend_category": category,
            "dividend_category_confirmed": True,
            "summary": f"{facts_summary} · {reason}",
            "assumption": None,
        }

    pre_dps = _safe_num(trailing_dps)
    post_dps = declared_dps
    dps_delta_krw = round(post_dps - pre_dps, 2) if pre_dps is not None else None
    dps_delta_pct = round((post_dps / pre_dps - 1.0) * 100.0, 2) if (pre_dps is not None and pre_dps > 0) else None
    if pre_dps is not None and pre_dps > 0 and dps_delta_pct is not None:
        summary = f"{category} {share_label} 주당배당금 {round(pre_dps):,}→{round(post_dps):,}원({dps_delta_pct:+.1f}%)"
    else:
        summary = f"{category} {share_label} Pro-forma 주당배당금 {round(post_dps):,}원"
    if rec_date:
        summary += f" · 배당기준일 {rec_date}"

    return {
        "status": "ok",
        "reason": None,
        "facts": div_facts,
        "facts_summary": facts_summary,
        "dividend_category": category,
        "dividend_category_confirmed": True,
        "declared_dps": declared_dps,
        "pre_bps": None,
        "post_bps": None,
        "pre_eps": None,
        "post_eps": None,
        "pre_dps": pre_dps,
        "post_dps": post_dps,
        "dps_delta_krw": dps_delta_krw,
        "dps_delta_pct": dps_delta_pct,
        "summary": summary,
        "assumption": (
            f"확인된 결산배당({share_label} 주당 {round(post_dps):,}원)을 연간 DPS로 대입한 잠정(Pro-forma) "
            "계산입니다(중간·분기배당 병행 기업은 기지급 배당 별도 확인 필요, 기존 DB 미변경)."
        ),
    }


def describe_holding_target_context(stock_code: str, holding: dict[str, Any] | None) -> dict[str, Any]:
    if not holding:
        return {"status": "not_held", "formula": None, "note": "미보유 종목"}
    if holding.get("target_price_disabled"):
        return {"status": "disabled", "formula": None, "note": "영향 없음(목표가 비활성)"}
    formula = str(holding.get("target_price_formula") or "").strip()
    if formula:
        vars_used = extract_target_variables(formula)
        if vars_used == {"매입가"}:
            return {"status": "no_impact_auto", "formula": formula, "note": "영향 없음(목표가가 평단 기준)"}
        return {"status": "formula", "formula": formula, "note": None}
    if holding.get("target_price") not in (None, ""):
        return {"status": "no_impact_manual", "formula": None, "note": "영향 없음(직접 입력 고정 목표가)"}
    if is_preferred_stock(stock_code):
        return {"status": "no_impact_auto", "formula": None, "note": "영향 없음(목표가가 본주가격 기준)"}
    if holding_value_meta(stock_code):
        return {"status": "no_impact_auto", "formula": None, "note": "영향 없음(목표가가 보유지분 기준)"}
    return {"status": "no_impact_auto", "formula": None, "note": "영향 없음(목표가가 평단 기준)"}


def compute_target_reprice(
    stock_code: str,
    holding: dict[str, Any] | None,
    *,
    disclosure_type: str,
    pre_variables: dict[str, float | None],
    pro_forma: dict[str, Any],
) -> dict[str, Any] | None:
    """보유 종목의 목표가 수식을 공시 전 변수 vs Pro-forma 변수로 두 번 평가한다."""
    if not holding:
        return None
    pf_status = pro_forma.get("status")
    if pf_status in {"superseded", "stale"}:
        return {
            "status": pf_status,
            "reason": pro_forma.get("reason"),
            "formula": holding.get("target_price_formula"),
            "pre_target": None,
            "post_target": None,
            "delta_krw": None,
            "delta_pct": None,
        }

    ctx = describe_holding_target_context(stock_code, holding)
    if ctx["status"] != "formula":
        return {
            "status": "no_impact",
            "reason": ctx["note"] or "영향 없음(목표가가 평단 기준)",
            "formula": ctx["formula"],
            "pre_target": None,
            "post_target": None,
            "delta_krw": 0.0,
            "delta_pct": 0.0,
        }

    formula = ctx["formula"] or ""
    vars_used = extract_target_variables(formula)
    if not vars_used:
        return {
            "status": "uncalculable",
            "reason": "목표가 수식 변수를 해석할 수 없어 미계산",
            "formula": formula,
            "pre_target": None,
            "post_target": None,
            "delta_krw": None,
            "delta_pct": None,
        }

    if disclosure_type == STOCK_RETIREMENT and not (vars_used & {"BPS", "EPS"}):
        return {
            "status": "no_impact",
            "reason": "영향 없음(목표가 수식이 BPS·EPS를 사용하지 않음)",
            "formula": formula,
            "pre_target": None,
            "post_target": None,
            "delta_krw": 0.0,
            "delta_pct": 0.0,
        }
    if disclosure_type == CASH_DIVIDEND and "DPS" not in vars_used:
        return {
            "status": "no_impact",
            "reason": "영향 없음(목표가 수식이 DPS를 사용하지 않음)",
            "formula": formula,
            "pre_target": None,
            "post_target": None,
            "delta_krw": 0.0,
            "delta_pct": 0.0,
        }

    if pf_status == "no_impact":
        return {
            "status": "no_impact",
            "reason": pro_forma.get("reason") or "영향 없음",
            "formula": formula,
            "pre_target": None,
            "post_target": None,
            "delta_krw": 0.0,
            "delta_pct": 0.0,
        }
    if pf_status != "ok":
        return {
            "status": "uncalculable",
            "reason": pro_forma.get("reason") or "공시 수치를 확인하지 못해 미계산",
            "formula": formula,
            "pre_target": None,
            "post_target": None,
            "delta_krw": None,
            "delta_pct": None,
        }

    post_variables = dict(pre_variables)
    if disclosure_type == STOCK_RETIREMENT:
        if "BPS" in vars_used:
            post_variables["BPS"] = _safe_num(pro_forma.get("post_bps"))
        if "EPS" in vars_used:
            post_variables["EPS"] = _safe_num(pro_forma.get("post_eps"))
    elif disclosure_type == CASH_DIVIDEND:
        if "DPS" in vars_used:
            post_variables["DPS"] = _safe_num(pro_forma.get("post_dps"))

    missing_pre = [v for v in sorted(vars_used) if pre_variables.get(v) is None]
    missing_post = [v for v in sorted(vars_used) if post_variables.get(v) is None]
    if missing_pre or missing_post:
        missing_names = ", ".join(dict.fromkeys([*missing_pre, *missing_post]))
        return {
            "status": "uncalculable",
            "reason": f"미계산 (수식 변수 {missing_names} 기준값 부족)",
            "formula": formula,
            "pre_target": None,
            "post_target": None,
            "delta_krw": None,
            "delta_pct": None,
        }

    try:
        pre_target = evaluate_target_formula(formula, pre_variables)
        post_target = evaluate_target_formula(formula, post_variables)
    except (ValueError, ZeroDivisionError):
        return {
            "status": "uncalculable",
            "reason": "미계산 (목표가 수식 평가 오류)",
            "formula": formula,
            "pre_target": None,
            "post_target": None,
            "delta_krw": None,
            "delta_pct": None,
        }

    if pre_target is None or post_target is None or pre_target <= 0:
        return {
            "status": "uncalculable",
            "reason": "미계산 (유효한 양수 목표가를 산출할 수 없음)",
            "formula": formula,
            "pre_target": pre_target,
            "post_target": post_target,
            "delta_krw": None,
            "delta_pct": None,
        }

    delta_krw = post_target - pre_target
    delta_pct = (post_target / pre_target - 1.0) * 100.0
    return {
        "status": "ok",
        "reason": None,
        "formula": formula,
        "pre_target": round(pre_target, 2),
        "post_target": round(post_target, 2),
        "delta_krw": round(delta_krw, 2),
        "delta_pct": round(delta_pct, 2),
    }


def compute_price_reaction(close_rows: list[dict[str, Any]], filing_date: str) -> dict[str, Any]:
    """접수일 직전 거래일 종가 대비 공시일(당일)·공시일 포함 5거래일·공시일 포함 20거래일 수익률을 계산한다."""
    empty_win = {"status": "unavailable", "date": None, "close": None, "return_pct": None}
    if not filing_date or not close_rows:
        return {
            "status": "unavailable",
            "reason": "종가 시계열 미확인",
            "basis_label": PRICE_REACTION_BASIS_LABEL,
            "base_date": None,
            "base_close": None,
            "day0": {**empty_win, "label": "당일(공시일)"},
            "day5": {**empty_win, "label": "공시일 포함 5거래일"},
            "day20": {**empty_win, "label": "공시일 포함 20거래일"},
        }

    ordered = sorted(
        (r for r in close_rows if r.get("date") and _safe_num(r.get("close")) and _safe_num(r.get("close")) > 0),
        key=lambda r: str(r["date"]),
    )
    prior = [r for r in ordered if str(r["date"]) < filing_date]
    if not prior:
        return {
            "status": "unavailable",
            "reason": "접수일 직전 거래일 종가 미확인",
            "basis_label": PRICE_REACTION_BASIS_LABEL,
            "base_date": None,
            "base_close": None,
            "day0": {**empty_win, "label": "당일(공시일)"},
            "day5": {**empty_win, "label": "공시일 포함 5거래일"},
            "day20": {**empty_win, "label": "공시일 포함 20거래일"},
        }

    base = prior[-1]
    base_close = float(base["close"])
    after = [r for r in ordered if str(r["date"]) >= filing_date]

    def _window(idx: int, label: str) -> dict[str, Any]:
        if len(after) <= idx:
            return {"status": "in_progress", "label": label, "date": None, "close": None, "return_pct": None}
        row = after[idx]
        close_val = float(row["close"])
        return {
            "status": "ok",
            "label": label,
            "date": str(row["date"]),
            "close": close_val,
            "return_pct": round((close_val / base_close - 1.0) * 100.0, 2),
        }

    return {
        "status": "ok",
        "reason": None,
        "basis_label": PRICE_REACTION_BASIS_LABEL,
        "base_date": str(base["date"]),
        "base_close": base_close,
        "day0": _window(0, "당일(공시일)"),
        "day5": _window(4, "공시일 포함 5거래일"),
        "day20": _window(19, "공시일 포함 20거래일"),
    }


def compute_verdict(
    target_reprice: dict[str, Any] | None,
    price_reaction: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Δ목표가% 와 주가 반응% 의 차이로 사실 기반 시차 판정 문구를 생성한다(권유 아님)."""
    if not target_reprice or target_reprice.get("status") != "ok":
        return None
    delta_pct = _safe_num(target_reprice.get("delta_pct"))
    if delta_pct is None or abs(delta_pct) < 0.05:
        return None
    if not price_reaction or price_reaction.get("status") != "ok":
        return None

    chosen_key = None
    chosen_label = None
    reaction_pct = None
    for key, label in (
        ("day20", "공시일 포함 20거래일"),
        ("day5", "공시일 포함 5거래일"),
        ("day0", "당일(공시일)"),
    ):
        win = price_reaction.get(key) or {}
        if win.get("status") == "ok" and _safe_num(win.get("return_pct")) is not None:
            chosen_key = key
            chosen_label = label
            reaction_pct = float(win["return_pct"])
            break
    if chosen_key is None or reaction_pct is None:
        return None

    gap_pct_p = round(delta_pct - reaction_pct, 2)
    if delta_pct > 0:
        if gap_pct_p >= GAP_VERDICT_THRESHOLD_PCT_P:
            code, text = "underreacted", "목표가 상향폭 대비 주가 반응이 작음"
        elif gap_pct_p <= -GAP_VERDICT_THRESHOLD_PCT_P:
            code, text = "priced_in", "주가가 먼저 반영"
        else:
            code, text = "aligned", "목표가 변동폭과 주가 반응이 유사함"
    else:
        if gap_pct_p <= -GAP_VERDICT_THRESHOLD_PCT_P:
            code, text = "underreacted", "목표가 하향폭 대비 주가 반응이 작음"
        elif gap_pct_p >= GAP_VERDICT_THRESHOLD_PCT_P:
            code, text = "priced_in", "주가가 먼저 반영"
        else:
            code, text = "aligned", "목표가 변동폭과 주가 반응이 유사함"

    return {
        "code": code,
        "window": chosen_key,
        "window_label": chosen_label,
        "reaction_pct": reaction_pct,
        "gap_pct_p": gap_pct_p,
        "text": text,
    }


async def _resolve_corp_code(stock_code: str) -> str | None:
    corp = await corp_codes.get_corp_code(stock_code)
    if corp:
        return corp
    if is_korean_stock(stock_code) and is_preferred_stock(stock_code):
        return await corp_codes.get_corp_code(common_stock_code(stock_code))
    return None


async def _load_user_holding(google_sub: str | None, stock_code: str) -> dict[str, Any] | None:
    if not google_sub:
        return None
    items = await portfolio_fx.annotate_avg_price_krw(await portfolio_repo.get_portfolio(google_sub))
    for item in items:
        if str(item.get("stock_code") or "").upper() == stock_code and _safe_num(item.get("quantity")):
            return {**item, "avg_price": item.get("avg_price_krw", item.get("avg_price"))}
    return None


async def _load_formula_base_inputs(
    stock_code: str,
    holding: dict[str, Any] | None,
    *,
    need_basis: bool,
    need_dps: bool,
    as_of: date | None = None,
) -> tuple[dict[str, Any], float | None, dict[str, float | None]]:
    formula = str((holding or {}).get("target_price_formula") or "").strip()
    vars_used = extract_target_variables(formula) if formula else set()

    need_basis = need_basis or bool(vars_used & {"BPS", "EPS"})
    need_dps = need_dps or ("DPS" in vars_used)

    basis: dict[str, Any] = {}
    if need_basis and is_korean_stock(stock_code):
        source = common_stock_code(stock_code) if is_preferred_stock(stock_code) else stock_code
        try:
            basis = await fetch_valuation_basis(source, as_of=as_of or today_kst())
        except _REPRICE_DATA_ERRORS as exc:
            logger.info("reprice valuation basis unavailable (%s): %s", source, exc)
            basis = {}

    trailing_dps: float | None = None
    if need_dps:
        try:
            dps_map = await portfolio_repo.get_trailing_dividends([stock_code])
            trailing_dps = _safe_num(dps_map.get(stock_code))
        except _REPRICE_DATA_ERRORS as exc:
            logger.info("reprice trailing dividend unavailable (%s): %s", stock_code, exc)

    pre_vars: dict[str, float | None] = {}
    if holding and vars_used:
        if "매입가" in vars_used:
            pre_vars["매입가"] = _safe_num(holding.get("avg_price"))
        if "BPS" in vars_used:
            pre_vars["BPS"] = _safe_num(basis.get("bps"))
        if "EPS" in vars_used:
            pre_vars["EPS"] = _safe_num(basis.get("eps"))
        if "DPS" in vars_used:
            pre_vars["DPS"] = trailing_dps
        if "보유지분" in vars_used:
            try:
                pre_vars["보유지분"] = await holding_value_per_share(stock_code)
            except _REPRICE_DATA_ERRORS:
                pre_vars["보유지분"] = None
        if "본주가격" in vars_used:
            common = common_stock_code(stock_code) if is_preferred_stock(stock_code) else ""
            if common and common != stock_code:
                try:
                    quote = await runtime_quotes.fetch_quote(common)
                    pre_vars["본주가격"] = _safe_num((quote or {}).get("price"))
                except _REPRICE_DATA_ERRORS:
                    pre_vars["본주가격"] = None
            else:
                pre_vars["본주가격"] = None

    return basis, trailing_dps, pre_vars


async def get_stock_reprice(
    stock_code: str,
    *,
    google_sub: str | None = None,
    days: int = 90,
    as_of: date | None = None,
) -> dict[str, Any]:
    """``GET /api/stocks/{code}/reprice`` 응답 생성 — 최근 ``days``일 정형 공시 2종 재가격."""
    code = str(stock_code or "").strip().upper()
    authenticated = bool(google_sub)
    if not is_supported_stock_code(code):
        return {
            "stock_code": code,
            "authenticated": authenticated,
            "held": False,
            "status": "unsupported",
            "message": "국내 상장 종목코드만 지원합니다.",
            "target_context": None,
            "items": [],
        }

    holding = await _load_user_holding(google_sub, code) if authenticated else None
    target_ctx = describe_holding_target_context(code, holding) if authenticated else None

    if not dart_client.api_key():
        return {
            "stock_code": code,
            "authenticated": authenticated,
            "held": bool(holding),
            "status": "unsupported",
            "message": "공시 조회가 설정되지 않았습니다.",
            "target_context": target_ctx,
            "items": [],
        }

    corp_code = await _resolve_corp_code(code)
    if not corp_code:
        return {
            "stock_code": code,
            "authenticated": authenticated,
            "held": bool(holding),
            "status": "unsupported",
            "message": "DART 고유번호가 없는 종목입니다.",
            "target_context": target_ctx,
            "items": [],
        }

    today = as_of or today_kst()
    start_day = today - timedelta(days=max(1, min(int(days or 90), 180)))
    try:
        raw_filings = await dart_client.fetch_filing_list(
            corp_code,
            start_day.strftime("%Y%m%d"),
            today.strftime("%Y%m%d"),
            sort="date",
            sort_mth="desc",
            page_no=1,
            page_count=100,
            timeout=15.0,
            essential=False,
            cache_ttl=FILING_LIST_TTL_SECONDS,
        )
    except dart_client.DartQuotaError:
        return {
            "stock_code": code,
            "authenticated": authenticated,
            "held": bool(holding),
            "status": "quota",
            "message": "공시 조회 한도를 알림용으로 아끼는 중입니다.",
            "target_context": target_ctx,
            "items": [],
        }
    except _DART_ERRORS as exc:
        logger.info("reprice filing list failed (%s): %s", code, exc)
        return {
            "stock_code": code,
            "authenticated": authenticated,
            "held": bool(holding),
            "status": "error",
            "message": "공시 목록을 불러오지 못했습니다.",
            "target_context": target_ctx,
            "items": [],
        }

    all_matched: list[dict[str, Any]] = []
    seen_rcept: set[str] = set()
    for filing in raw_filings:
        rcept_no = str(filing.get("rcept_no") or "").strip()
        report_nm = str(filing.get("report_nm") or "").strip()
        disc_type = classify_reprice_disclosure(report_nm)
        if not rcept_no or not disc_type or rcept_no in seen_rcept:
            continue
        seen_rcept.add(rcept_no)
        all_matched.append({
            "rcept_no": rcept_no,
            "rcept_dt": _iso_rcept_dt(filing.get("rcept_dt")),
            "report_nm": report_nm,
            "disclosure_type": disc_type,
        })

    if not all_matched:
        return {
            "stock_code": code,
            "authenticated": authenticated,
            "held": bool(holding),
            "status": "empty",
            "message": None,
            "target_context": target_ctx,
            "items": [],
        }

    superseded_map = mark_superseded_disclosures(all_matched)
    matched = all_matched[:MAX_REPRICE_DOCS]
    active_matched = [m for m in matched if m["rcept_no"] not in superseded_map]

    has_retirement = any(m["disclosure_type"] == STOCK_RETIREMENT for m in active_matched)
    has_dividend = any(m["disclosure_type"] == CASH_DIVIDEND for m in active_matched)

    valid_dates = [m["rcept_dt"] for m in matched if m["rcept_dt"]]
    earliest = date.fromisoformat(min(valid_dates)) if valid_dates else start_day
    close_since = (earliest - timedelta(days=15)).isoformat()

    async def _load_one_doc(meta: dict[str, Any]) -> dict[str, Any]:
        if meta["rcept_no"] in superseded_map:
            return {
                "parse_status": "superseded",
                "parse_message": "정정됨 — 후속 기재정정 공시 기준 계산",
                "extracted": {},
            }
        return await _fetch_and_parse_document(
            meta["rcept_no"],
            meta["disclosure_type"],
            allow_download=authenticated,
            essential=False,
        )

    async def _load_closes() -> list[dict[str, Any]]:
        try:
            return await close_price_client.get_daily_closes(code, since=close_since, until=today.isoformat())
        except close_price_client.ClosePriceClientError as exc:
            logger.info("reprice close prices unavailable (%s): %s", code, exc)
            return []

    parsed_docs, (basis, trailing_dps, pre_vars), close_rows = await asyncio.gather(
        asyncio.gather(*(_load_one_doc(m) for m in matched)),
        _load_formula_base_inputs(
            code, holding, need_basis=has_retirement, need_dps=has_dividend, as_of=today
        ),
        _load_closes(),
    )

    items: list[dict[str, Any]] = []
    for meta, parsed in zip(matched, parsed_docs):
        disc_type = meta["disclosure_type"]
        superseded_by = superseded_map.get(meta["rcept_no"])
        url = DART_VIEWER_URL.format(rcept_no=meta["rcept_no"])
        try:
            pro_forma = compute_pro_forma(
                disc_type,
                parsed,
                stock_code=code,
                basis=basis,
                trailing_dps=trailing_dps,
                rcept_dt=meta["rcept_dt"],
                as_of=today,
            )
            target_reprice = (
                compute_target_reprice(
                    code,
                    holding,
                    disclosure_type=disc_type,
                    pre_variables=pre_vars,
                    pro_forma=pro_forma,
                )
                if authenticated and holding
                else None
            )
            price_reaction = compute_price_reaction(close_rows, meta["rcept_dt"])
            verdict = compute_verdict(target_reprice, price_reaction)
        except Exception as exc:  # noqa: BLE001 — 부가 정보 경계, 개별 공시 계산 오류가 응답 전체를 막지 않음
            logger.warning("reprice item calculation failed (%s %s): %s", code, meta["rcept_no"], exc)
            pro_forma = {"status": "uncalculable", "reason": "미계산 (계산 중 오류)", "summary": "미계산", "assumption": None}
            target_reprice = (
                {"status": "uncalculable", "reason": "미계산 (계산 중 오류)", "formula": None, "pre_target": None, "post_target": None, "delta_krw": None, "delta_pct": None}
                if authenticated and holding
                else None
            )
            price_reaction = compute_price_reaction(close_rows, meta["rcept_dt"])
            verdict = None

        items.append({
            "rcept_no": meta["rcept_no"],
            "rcept_dt": meta["rcept_dt"],
            "report_nm": meta["report_nm"],
            "disclosure_type": disc_type,
            "disclosure_label": DISCLOSURE_LABELS[disc_type],
            "is_amendment": is_amendment_disclosure(meta["report_nm"]),
            "superseded": bool(superseded_by),
            "superseded_by": superseded_by,
            "url": url,
            "parse_status": parsed.get("parse_status") or "unparseable",
            "parse_message": parsed.get("parse_message"),
            "extracted": parsed.get("extracted") or {},
            "pro_forma": pro_forma,
            "target_reprice": target_reprice,
            "price_reaction": price_reaction,
            "verdict": verdict,
        })

    return {
        "stock_code": code,
        "authenticated": authenticated,
        "held": bool(holding),
        "status": "ok",
        "message": None,
        "target_context": target_ctx,
        "items": items,
    }


async def format_notification_reprice_line(
    stock_code: str,
    disclosure: dict[str, Any],
    holding: dict[str, Any] | None = None,
    *,
    google_sub: str | None = None,
    recent_disclosures: list[dict[str, Any]] | None = None,
    timeout: float = NOTIFICATION_REPRICE_TIMEOUT_S,
    as_of: date | None = None,
) -> str:
    """신규 공시 푸시 알림에 덧붙일 목표가 재가격 한 줄(예: ``목표가 78,000→83,100원(+6.5%)``).

    정형 2종이 아니거나, 후속 기재정정으로 대체된 구본이거나, 결산배당/분모가 확정되지
    않았거나, 예외·타임아웃 발생 시 빈 문자열을 반환해 본 공시 알림을 절대 막지 않는다.
    """
    try:
        report_nm = str((disclosure or {}).get("report_nm") or "").strip()
        disc_type = classify_reprice_disclosure(report_nm)
        if not disc_type:
            return ""
        rcept_no = str((disclosure or {}).get("rcept_no") or "").strip()
        code = str(stock_code or "").strip().upper()
        if not rcept_no or not is_supported_stock_code(code):
            return ""

        if recent_disclosures:
            superseded = mark_superseded_disclosures(recent_disclosures)
            if rcept_no in superseded:
                return ""

        today = as_of or today_kst()
        rcept_dt = _iso_rcept_dt((disclosure or {}).get("rcept_dt"))

        async with asyncio.timeout(timeout):
            if holding is None and google_sub:
                holding = await _load_user_holding(google_sub, code)
            if not holding:
                return ""
            ctx = describe_holding_target_context(code, holding)
            if ctx["status"] != "formula":
                return ""

            parsed, (basis, trailing_dps, pre_vars) = await asyncio.gather(
                _fetch_and_parse_document(rcept_no, disc_type, allow_download=True, essential=False),
                _load_formula_base_inputs(
                    code,
                    holding,
                    need_basis=(disc_type == STOCK_RETIREMENT),
                    need_dps=(disc_type == CASH_DIVIDEND),
                    as_of=today,
                ),
            )
            pro_forma = compute_pro_forma(
                disc_type,
                parsed,
                stock_code=code,
                basis=basis,
                trailing_dps=trailing_dps,
                rcept_dt=rcept_dt,
                as_of=today,
            )
            if disc_type == CASH_DIVIDEND and not pro_forma.get("dividend_category_confirmed"):
                return ""
            reprice = compute_target_reprice(
                code,
                holding,
                disclosure_type=disc_type,
                pre_variables=pre_vars,
                pro_forma=pro_forma,
            )
    except Exception as exc:  # noqa: BLE001 — 부가 정보 경계, 본 공시 알림 발송을 막지 않음
        logger.warning("notification reprice skipped (%s %s): %s", stock_code, (disclosure or {}).get("rcept_no"), exc)
        return ""

    if not reprice or reprice.get("status") != "ok":
        return ""
    pre_t = _safe_num(reprice.get("pre_target"))
    post_t = _safe_num(reprice.get("post_target"))
    pct = _safe_num(reprice.get("delta_pct"))
    if pre_t is None or post_t is None or pct is None or round(pre_t) == round(post_t):
        return ""
    return f"목표가 {round(pre_t):,}→{round(post_t):,}원({pct:+.1f}%)"
