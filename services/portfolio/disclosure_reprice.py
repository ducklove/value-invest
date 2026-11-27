"""공시 → 목표가 재가격(Pro-forma) & 주가 반응 시차 계산기 (기능id: reprice).

MVP 범위(정형 공시 2종, LLM 호출 없음):
1. ``주식소각결정``(자사주 소각): 소각 주식 수 → 기준 유통(또는 발행)주식수 차감 →
   Pro-forma BPS·EPS 상향률 및 수식 목표가 재평가.
2. ``현금ㆍ현물배당결정``: 1주당 현금배당금(결산배당 기준) → Pro-forma DPS 및 수식 목표가 재평가.

수치 출처:
- OpenDART 주요사항보고서 정형 JSON API 에는 ``주식소각결정``·``현금ㆍ현물배당결정`` 전용
  엔드포인트가 개방되어 있지 않으므로, ``list.json``(``essential=False``)으로 최근 90일
  대상 공시를 찾고 ``document.xml``(``essential=False``) 원문 ZIP 의 정형 표를 파싱한다.
- 파싱 결과는 ``rcept_no`` 단위로 인메모리(``MemoryTTLCache``)와 DB(``cache_values``)에
  캐시해 동일 공시 원문을 중복 다운로드하지 않는다.
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
CACHE_NS_REPRICE_DOC = "dart.reprice_doc.v1"
DOC_DB_TTL_SECONDS = 90 * 86400.0
DOC_MEM_TTL_SECONDS = 24 * 3600.0
FILING_LIST_TTL_SECONDS = 600.0
MAX_REPRICE_DOCS = 5
GAP_VERDICT_THRESHOLD_PCT_P = 2.0
NOTIFICATION_REPRICE_TIMEOUT_S = 4.0

STOCK_RETIREMENT = "stock_retirement"
CASH_DIVIDEND = "cash_dividend"
DISCLOSURE_LABELS = {
    STOCK_RETIREMENT: "주식소각결정",
    CASH_DIVIDEND: "현금ㆍ현물배당결정",
}

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

_doc_mem_cache = MemoryTTLCache("dart.reprice_doc", DOC_MEM_TTL_SECONDS, evict_expired_after=DOC_MEM_TTL_SECONDS)
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


def is_supported_stock_code(code: str) -> bool:
    return bool(_STOCK_CODE_RE.match(str(code or "").strip().upper()))


def _compact_label(text: str | None) -> str:
    cleaned = unescape(str(text or ""))
    cleaned = re.sub(r"^\s*(?:\d+|[가-하]|[①-⑳])[\.\)]\s*", "", cleaned)
    cleaned = re.sub(r"\s+", "", cleaned)
    for ch in ("ㆍ", "·", "・", "•", ":", "(", ")", "[", "]", "/"):
        cleaned = cleaned.replace(ch, "")
    return cleaned.lower()


def classify_reprice_disclosure(report_nm: str | None) -> str | None:
    """보고서명이 재가격 대상 정형 공시 2종인지 판별한다.

    자회사·종속회사 공시나 철회·취소·예고 공시는 발행사 확정 수치가 아니므로 제외한다.
    """
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


def extract_xml_documents_from_zip(content: bytes, rcept_no: str = "") -> list[str]:
    """``document.xml`` ZIP 에서 본문 XML/HTML 텍스트 목록을 추출한다(주 문서 우선)."""
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = [
            name for name in zf.namelist()
            if name.lower().endswith((".xml", ".html", ".htm"))
        ]
        prefix = str(rcept_no or "").strip()
        names.sort(key=lambda n: (0 if prefix and n.startswith(prefix) else 1, len(n), n))
        docs: list[str] = []
        for name in names:
            docs.append(_decode_bytes(zf.read(name)))
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
    """기재정정 상단의 '정정전 / 정정후' 비교표인지 판별."""
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
    """단일 수치 셀 파싱. 반환: ``(value, is_ambiguous)``.

    '-'·'해당없음'은 ``(None, False)``(결측), 한 셀에 서로 다른 숫자가 여럿이면
    ``(None, True)``(모호함 — 추정 금지)를 돌려준다.
    """
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
    """'보통주 1,000,000주, 종류주 50,000주' 처럼 한 칸에 병기된 수치를 분리한다."""
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
    """같은 필드에 상충하는 양수 값이 두 번 나오면 모호함(True)으로 표시한다."""
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
    """``주식소각결정`` 원문(``document.xml``)에서 소각 주식 수·발행주식총수를 추출한다."""
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
        # 1) 열 헤더형 표 지원 (예: [구분/주식의종류 | 소각할 주식의 수 | 발행주식총수])
        header_map: dict[str, int] = {}
        header_row_idx = -1
        for r_idx, raw_row in enumerate(grid[:3]):
            labels = [_compact_label(c) for c in raw_row]
            col_ret = next(
                (i for i, lb in enumerate(labels) if ("소각할주식" in lb or "소각예정주식" in lb or lb == "소각주식수") and "금액" not in lb and "기간" not in lb and "방법" not in lb),
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

        # 2) 표준 KRX/DART 항목형 표 (행 제목 + 보통주식/종류주식 세부행)
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
                # 3단 표에서 바깥 묶음칸 아래에 '소각할 주식의 수'가 두 번째 열에 오는 변형 방어
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
    """``현금ㆍ현물배당결정`` 원문(``document.xml``)에서 1주당 배당금·배당구분을 추출한다."""
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
        # 1) 열 헤더형 표 (예: [배당구분 | 배당종류 | 주식종류 | 1주당 배당금 | 시가배당률])
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

        # 2) 표준 KRX/DART 항목형 표
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
    try:
        xml_texts = extract_xml_documents_from_zip(content, rcept_no=rcept_no)
    except (zipfile.BadZipFile, OSError, ValueError):
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
    if disclosure_type == STOCK_RETIREMENT:
        return parse_stock_retirement_xml(xml_texts)
    if disclosure_type == CASH_DIVIDEND:
        return parse_cash_dividend_xml(xml_texts)
    return {"parse_status": "unparseable", "parse_message": "지원하지 않는 공시 유형", "extracted": {}}


async def _fetch_and_parse_document(rcept_no: str, disclosure_type: str, *, essential: bool = False) -> dict[str, Any]:
    """``rcept_no`` 별 원문 파싱 결과를 인메모리 + SQLite ``cache_values`` 로 캐시한다."""
    cache_key = f"{disclosure_type}:{rcept_no}"
    mem_hit = _doc_mem_cache.get(cache_key)
    if mem_hit is not None:
        return mem_hit

    try:
        db_entry = await cache_values.get_cache_value_entry(CACHE_NS_REPRICE_DOC, cache_key)
    except _DB_CACHE_ERRORS:
        db_entry = None
    if db_entry is not None and isinstance(db_entry.value, dict):
        _doc_mem_cache.set(cache_key, db_entry.value)
        return db_entry.value

    async def _load() -> dict[str, Any]:
        async with _doc_sem:
            raw_zip = await dart_client.fetch_document_xml_bytes(rcept_no, essential=essential)
        parsed = parse_reprice_document_zip(raw_zip, disclosure_type, rcept_no=rcept_no)
        try:
            await cache_values.set_cache_value(
                CACHE_NS_REPRICE_DOC, cache_key, parsed, ttl_seconds=DOC_DB_TTL_SECONDS
            )
        except _DB_CACHE_ERRORS as exc:
            logger.debug("reprice doc db cache write skipped (%s): %s", cache_key, exc)
        return parsed

    try:
        return await cached_fetch(_doc_mem_cache, cache_key, _load)
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


def _select_retirement_shares_and_base(
    extracted: dict[str, Any],
    basis: dict[str, Any],
    *,
    preferred_stock: bool,
) -> tuple[int | None, int | None, str | None, str | None]:
    """소각 주식 수와 분모 기준 주식수(유통주식수 우선 → 발행주식수 폴백)를 고른다."""
    ret_common = int(extracted.get("retired_shares_common") or 0)
    ret_pref = int(extracted.get("retired_shares_preferred") or 0)
    iss_common = int(extracted.get("issued_shares_common") or 0)
    iss_pref = int(extracted.get("issued_shares_preferred") or 0)

    basis_outstanding = int(round(_safe_num(basis.get("outstandingShares")) or 0))
    basis_issued = int(round(_safe_num(basis.get("issuedShares")) or 0))

    # valuation.py 의 treasury_share_basis 는 보통+우선 합계일 수도, 보통주 단독일 수도 있다.
    if basis_outstanding > 0:
        if iss_pref > 0 and iss_common > 0 and basis_issued > 0 and abs(basis_issued - iss_common) < abs(basis_issued - (iss_common + iss_pref)):
            retired = ret_common
            label = "유통주식수(보통주)"
        else:
            retired = ret_common + ret_pref
            label = "유통주식수(자사주 차감)"
        if retired <= 0:
            return None, basis_outstanding, label, "해당 주식 종류의 소각 수량이 없습니다."
        if retired >= basis_outstanding:
            return retired, basis_outstanding, label, "소각 주식 수가 기준 유통주식수 이상이라 계산할 수 없습니다."
        return retired, basis_outstanding, label, None

    if basis_issued > 0:
        retired = (ret_common + ret_pref) if (iss_pref > 0 and abs(basis_issued - (iss_common + iss_pref)) <= abs(basis_issued - iss_common)) else (ret_common or ret_pref)
        if retired <= 0:
            return None, basis_issued, "발행주식수", "소각 주식 수를 확인할 수 없습니다."
        if retired >= basis_issued:
            return retired, basis_issued, "발행주식수", "소각 주식 수가 기준 발행주식수 이상이라 계산할 수 없습니다."
        return retired, basis_issued, "발행주식수", None

    # 재무 provider 에 주식수가 없으면 공시 본문의 발행주식총수를 분모로 쓴다.
    if preferred_stock and iss_pref > 0 and ret_pref > 0 and ret_common == 0:
        if ret_pref >= iss_pref:
            return ret_pref, iss_pref, "공시 발행주식총수(종류주)", "소각 주식 수가 발행주식총수 이상입니다."
        return ret_pref, iss_pref, "공시 발행주식총수(종류주)", None

    filing_total = iss_common + iss_pref if (ret_pref > 0 and iss_pref > 0 and iss_common > 0) else iss_common
    retired = (ret_common + ret_pref) if (ret_pref > 0 and iss_pref > 0 and iss_common > 0) else ret_common
    if filing_total > 0 and retired > 0:
        if retired >= filing_total:
            return retired, filing_total, "공시 발행주식총수", "소각 주식 수가 공시 발행주식총수 이상입니다."
        return retired, filing_total, "공시 발행주식총수", None

    return (ret_common or ret_pref or None), None, None, "기준 유통·발행주식수를 확인하지 못해 미계산"


def compute_pro_forma(
    disclosure_type: str,
    parsed: dict[str, Any],
    *,
    stock_code: str,
    basis: dict[str, Any],
    trailing_dps: float | None,
) -> dict[str, Any]:
    """공시 추출값과 현재 주당 기초지표(BPS·EPS·DPS)로 Pro-forma 수치를 계산한다."""
    if parsed.get("parse_status") != "ok":
        reason = parsed.get("parse_message") or "수치 확인 불가"
        return {
            "status": "unavailable",
            "reason": reason,
            "summary": reason,
            "assumption": None,
        }

    extracted = parsed.get("extracted") or {}
    preferred = is_preferred_stock(stock_code)

    if disclosure_type == STOCK_RETIREMENT:
        retired, base_shares, base_label, err = _select_retirement_shares_and_base(
            extracted, basis, preferred_stock=preferred
        )
        if err or not retired or not base_shares:
            reason = err or "기준 주식수 부족으로 미계산"
            return {
                "status": "uncalculable",
                "reason": reason,
                "retired_shares": retired,
                "base_shares": base_shares,
                "summary": f"미계산 — {reason}",
                "assumption": None,
            }
        post_shares = base_shares - retired
        multiplier = base_shares / post_shares
        retired_pct = round(retired / base_shares * 100.0, 2)
        uplift_pct = round((multiplier - 1.0) * 100.0, 2)

        pre_bps = _safe_num(basis.get("bps"))
        pre_eps = _safe_num(basis.get("eps"))
        post_bps = round(pre_bps * multiplier, 2) if pre_bps is not None else None
        post_eps = round(pre_eps * multiplier, 2) if pre_eps is not None else None

        parts = [f"소각 {retired:,}주({base_label} 대비 {retired_pct:.2f}%)"]
        if pre_bps is not None and post_bps is not None:
            parts.append(f"BPS {round(pre_bps):,}→{round(post_bps):,}원({uplift_pct:+.1f}%)")
        if pre_eps is not None and post_eps is not None:
            parts.append(f"EPS {round(pre_eps):,}→{round(post_eps):,}원({uplift_pct:+.1f}%)")
        if pre_bps is None and pre_eps is None:
            parts.append(f"주당가치 계수 {uplift_pct:+.2f}%")

        method = extracted.get("acquisition_method")
        method_note = f" (취득방법: {method})" if method else ""
        return {
            "status": "ok",
            "reason": None,
            "retired_shares": retired,
            "base_shares": base_shares,
            "base_shares_label": base_label,
            "post_shares": post_shares,
            "retired_pct": retired_pct,
            "per_share_uplift_pct": uplift_pct,
            "pre_bps": pre_bps,
            "post_bps": post_bps,
            "pre_eps": pre_eps,
            "post_eps": post_eps,
            "pre_dps": None,
            "post_dps": None,
            "summary": " · ".join(parts),
            "assumption": (
                f"{base_label}({base_shares:,}주)에서 소각 {retired:,}주가 순감소하고 자본총계·순이익은 "
                f"동일하다고 가정한 잠정(Pro-forma) 계산입니다{method_note}. 기취득 자사주가 기존 BPS·EPS 분모에서 "
                "이미 차감된 경우 실제 추가 변동폭은 다를 수 있습니다."
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
    if "분기" in cat_compact or "중간" in cat_compact:
        cat_display = category or "중간·분기배당"
        reason = f"{cat_display} 단건 공시(주당 {round(declared_dps):,}원)만으로는 연간 누적 DPS를 확정할 수 없어 미계산"
        return {
            "status": "uncalculable",
            "reason": reason,
            "declared_dps": declared_dps,
            "dividend_category": cat_display,
            "pre_dps": trailing_dps,
            "post_dps": None,
            "summary": f"{cat_display} 주당 {round(declared_dps):,}원 공시 · 연간 DPS 미계산",
            "assumption": "목표가 수식의 DPS는 연간 기준이므로 분기·중간배당 단건 금액으로 연간 DPS를 치환하지 않습니다.",
        }

    pre_dps = _safe_num(trailing_dps)
    post_dps = declared_dps
    dps_delta_krw = round(post_dps - pre_dps, 2) if pre_dps is not None else None
    dps_delta_pct = round((post_dps / pre_dps - 1.0) * 100.0, 2) if (pre_dps is not None and pre_dps > 0) else None
    cat_prefix = f"{category} " if category else ""
    if pre_dps is not None and pre_dps > 0 and dps_delta_pct is not None:
        summary = f"{cat_prefix}주당배당금 {round(pre_dps):,}→{round(post_dps):,}원({dps_delta_pct:+.1f}%)"
    else:
        summary = f"{cat_prefix}Pro-forma 주당배당금 {round(post_dps):,}원"

    return {
        "status": "ok",
        "reason": None,
        "dividend_category": category or "결산배당",
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
            f"공시된 {share_label} 1주당 현금배당금({round(post_dps):,}원)을 연간 DPS로 대입한 잠정(Pro-forma) "
            "계산입니다(중간·분기배당 병행 기업은 별도 합산 필요, 기존 DB 미변경)."
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

    if pro_forma.get("status") != "ok":
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
    """공시 접수일 직전 거래일 종가 대비 당일·5거래일·20거래일 수익률을 계산한다."""
    if not filing_date or not close_rows:
        return {
            "status": "unavailable",
            "reason": "종가 시계열 미확인",
            "base_date": None,
            "base_close": None,
            "day0": {"status": "unavailable", "date": None, "close": None, "return_pct": None},
            "day5": {"status": "unavailable", "date": None, "close": None, "return_pct": None},
            "day20": {"status": "unavailable", "date": None, "close": None, "return_pct": None},
        }

    ordered = sorted(
        (r for r in close_rows if r.get("date") and _safe_num(r.get("close")) and _safe_num(r.get("close")) > 0),
        key=lambda r: str(r["date"]),
    )
    prior = [r for r in ordered if str(r["date"]) < filing_date]
    if not prior:
        return {
            "status": "unavailable",
            "reason": "공시 전일 종가 미확인",
            "base_date": None,
            "base_close": None,
            "day0": {"status": "unavailable", "date": None, "close": None, "return_pct": None},
            "day5": {"status": "unavailable", "date": None, "close": None, "return_pct": None},
            "day20": {"status": "unavailable", "date": None, "close": None, "return_pct": None},
        }

    base = prior[-1]
    base_close = float(base["close"])
    after = [r for r in ordered if str(r["date"]) >= filing_date]

    def _window(idx: int) -> dict[str, Any]:
        if len(after) <= idx:
            return {"status": "in_progress", "date": None, "close": None, "return_pct": None}
        row = after[idx]
        close_val = float(row["close"])
        return {
            "status": "ok",
            "date": str(row["date"]),
            "close": close_val,
            "return_pct": round((close_val / base_close - 1.0) * 100.0, 2),
        }

    return {
        "status": "ok",
        "reason": None,
        "base_date": str(base["date"]),
        "base_close": base_close,
        "day0": _window(0),
        "day5": _window(4),
        "day20": _window(19),
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
    for key, label in (("day20", "20거래일"), ("day5", "5거래일"), ("day0", "당일")):
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
) -> tuple[dict[str, Any], float | None, dict[str, float | None]]:
    formula = str((holding or {}).get("target_price_formula") or "").strip()
    vars_used = extract_target_variables(formula) if formula else set()

    need_basis = need_basis or bool(vars_used & {"BPS", "EPS"})
    need_dps = need_dps or ("DPS" in vars_used)

    basis: dict[str, Any] = {}
    if need_basis and is_korean_stock(stock_code):
        source = common_stock_code(stock_code) if is_preferred_stock(stock_code) else stock_code
        try:
            basis = await fetch_valuation_basis(source, as_of=today_kst())
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

    today = today_kst()
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

    matched: list[dict[str, Any]] = []
    seen_rcept: set[str] = set()
    for filing in raw_filings:
        rcept_no = str(filing.get("rcept_no") or "").strip()
        report_nm = str(filing.get("report_nm") or "").strip()
        disc_type = classify_reprice_disclosure(report_nm)
        if not rcept_no or not disc_type or rcept_no in seen_rcept:
            continue
        seen_rcept.add(rcept_no)
        matched.append({
            "rcept_no": rcept_no,
            "rcept_dt": _iso_rcept_dt(filing.get("rcept_dt")),
            "report_nm": report_nm,
            "disclosure_type": disc_type,
        })
        if len(matched) >= MAX_REPRICE_DOCS:
            break

    if not matched:
        return {
            "stock_code": code,
            "authenticated": authenticated,
            "held": bool(holding),
            "status": "empty",
            "message": None,
            "target_context": target_ctx,
            "items": [],
        }

    has_retirement = any(m["disclosure_type"] == STOCK_RETIREMENT for m in matched)
    has_dividend = any(m["disclosure_type"] == CASH_DIVIDEND for m in matched)

    valid_dates = [m["rcept_dt"] for m in matched if m["rcept_dt"]]
    earliest = date.fromisoformat(min(valid_dates)) if valid_dates else start_day
    close_since = (earliest - timedelta(days=15)).isoformat()

    async def _load_closes() -> list[dict[str, Any]]:
        try:
            return await close_price_client.get_daily_closes(code, since=close_since, until=today.isoformat())
        except close_price_client.ClosePriceClientError as exc:
            logger.info("reprice close prices unavailable (%s): %s", code, exc)
            return []

    parsed_docs, (basis, trailing_dps, pre_vars), close_rows = await asyncio.gather(
        asyncio.gather(*(
            _fetch_and_parse_document(m["rcept_no"], m["disclosure_type"], essential=False)
            for m in matched
        )),
        _load_formula_base_inputs(
            code, holding, need_basis=has_retirement, need_dps=has_dividend
        ),
        _load_closes(),
    )

    items: list[dict[str, Any]] = []
    for meta, parsed in zip(matched, parsed_docs):
        disc_type = meta["disclosure_type"]
        pro_forma = compute_pro_forma(
            disc_type,
            parsed,
            stock_code=code,
            basis=basis,
            trailing_dps=trailing_dps,
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
        items.append({
            "rcept_no": meta["rcept_no"],
            "rcept_dt": meta["rcept_dt"],
            "report_nm": meta["report_nm"],
            "disclosure_type": disc_type,
            "disclosure_label": DISCLOSURE_LABELS[disc_type],
            "url": DART_VIEWER_URL.format(rcept_no=meta["rcept_no"]),
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
    timeout: float = NOTIFICATION_REPRICE_TIMEOUT_S,
) -> str:
    """신규 공시 푸시 알림에 덧붙일 목표가 재가격 한 줄(예: ``목표가 78,000→83,100원(+6.5%)``).

    정형 2종이 아니거나, 비보유·비수식 종목이거나, 비필수 쿼터·시간제한·파싱에
    실패하면 빈 문자열을 돌려 기존 공시 알림을 지연·방해하지 않는다.
    """
    report_nm = str((disclosure or {}).get("report_nm") or "").strip()
    disc_type = classify_reprice_disclosure(report_nm)
    if not disc_type:
        return ""
    rcept_no = str((disclosure or {}).get("rcept_no") or "").strip()
    code = str(stock_code or "").strip().upper()
    if not rcept_no or not is_supported_stock_code(code):
        return ""

    try:
        async with asyncio.timeout(timeout):
            if holding is None and google_sub:
                holding = await _load_user_holding(google_sub, code)
            if not holding:
                return ""
            ctx = describe_holding_target_context(code, holding)
            if ctx["status"] != "formula":
                return ""

            parsed, (basis, trailing_dps, pre_vars) = await asyncio.gather(
                _fetch_and_parse_document(rcept_no, disc_type, essential=False),
                _load_formula_base_inputs(
                    code,
                    holding,
                    need_basis=(disc_type == STOCK_RETIREMENT),
                    need_dps=(disc_type == CASH_DIVIDEND),
                ),
            )
            pro_forma = compute_pro_forma(
                disc_type,
                parsed,
                stock_code=code,
                basis=basis,
                trailing_dps=trailing_dps,
            )
            reprice = compute_target_reprice(
                code,
                holding,
                disclosure_type=disc_type,
                pre_variables=pre_vars,
                pro_forma=pro_forma,
            )
    except (*_DART_ERRORS, *_REPRICE_DATA_ERRORS) as exc:
        logger.info("notification reprice skipped (%s %s): %s", code, rcept_no, exc)
        return ""

    if not reprice or reprice.get("status") != "ok":
        return ""
    pre_t = _safe_num(reprice.get("pre_target"))
    post_t = _safe_num(reprice.get("post_target"))
    pct = _safe_num(reprice.get("delta_pct"))
    if pre_t is None or post_t is None or pct is None or round(pre_t) == round(post_t):
        return ""
    return f"목표가 {round(pre_t):,}→{round(post_t):,}원({pct:+.1f}%)"
