import asyncio
import io
import logging
import os
import re
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta, timezone

from cache_layer import MemoryTTLCache, cached_fetch
from core.errors import ExternalServiceError, RateLimitError
from core.http import get_http_client
from domain.timeutil import KST

logger = logging.getLogger(__name__)

BASE_URL = "https://opendart.fss.or.kr/api"
DART_ANNUAL_DATA_START_YEAR = 2015

# 재무제표 항목명 매핑
ACCOUNT_NAMES = {
    "revenue": ["매출액", "수익(매출액)", "영업수익"],
    "operating_profit": ["영업이익", "영업이익(손실)"],
    "net_income": ["당기순이익", "당기순이익(손실)"],
    "total_assets": ["자산총계"],
    "total_liabilities": ["부채총계"],
    "total_equity": ["자본총계"],
}


def api_key() -> str:
    """OPENDART_API_KEY를 호출 시점에 읽는다.

    main.py가 앱 팩토리를 먼저 import 하므로 이 모듈은 보통
    ``core.config.load_environment()`` 보다 먼저 로드된다. import 시점에 값을
    고정하면 `.env` 로만 키를 주는 환경에서 조용히 빈 키가 된다.
    """
    return os.getenv("OPENDART_API_KEY", "")


async def fetch_corp_codes() -> list[dict]:
    """DART에서 고유번호 XML을 다운로드하여 상장사 목록 반환."""
    client = await get_http_client("dart")
    _quota.note_call()
    resp = await client.get(
        f"{BASE_URL}/corpCode.xml", params={"crtfc_key": api_key()}, timeout=30
    )
    resp.raise_for_status()

    codes = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        xml_name = zf.namelist()[0]
        tree = ET.parse(zf.open(xml_name))
        root = tree.getroot()
        for item in root.iter("list"):
            stock_code = item.findtext("stock_code", "").strip()
            if not stock_code:
                continue
            codes.append(
                {
                    "corp_code": item.findtext("corp_code", "").strip(),
                    "corp_name": item.findtext("corp_name", "").strip(),
                    "stock_code": stock_code,
                    "modify_date": item.findtext("modify_date", "").strip(),
                }
            )
    return codes


def _match_account(account_nm: str, target_key: str) -> bool:
    """계정과목명이 target_key에 매핑되는 항목인지 확인."""
    for pattern in ACCOUNT_NAMES.get(target_key, []):
        if account_nm.startswith(pattern):
            return True
    return False


def _parse_amount(value: str | None) -> float | None:
    """금액 문자열을 float로 변환. 빈값/파싱불가 시 None."""
    if not value:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned or cleaned == "-":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_common_stock_share_status(payload: dict) -> dict:
    """Parse DART stock total quantity status for common shares.

    The important number for per-share valuation is distributable/common
    shares excluding treasury shares. DART exposes both total issued shares
    (`istc_totqy`) and treasury shares (`tesstk_co`) in annual reports.
    """
    if not isinstance(payload, dict) or payload.get("status") != "000":
        return {}

    common_row = None
    fallback_row = None
    for item in payload.get("list") or []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("se") or "").strip()
        if "합계" in kind and fallback_row is None:
            fallback_row = item
        if "보통" in kind:
            common_row = item
            break
    row = common_row or fallback_row
    if not row:
        return {}

    issued = _parse_amount(row.get("istc_totqy"))
    treasury = _parse_amount(row.get("tesstk_co"))
    distributed = _parse_amount(row.get("distb_stock_co"))
    if distributed is None and issued is not None and treasury is not None:
        distributed = max(issued - treasury, 0)

    return {
        "issued_shares": issued,
        "treasury_shares": treasury,
        "distributed_shares": distributed,
        "settlement_date": row.get("stlm_dt"),
    }


def _is_common_stock_dividend_kind(stock_kind: str | None) -> bool:
    text = (stock_kind or "").strip()
    if not text:
        return True
    if "우선" in text or "없는" in text:
        return False
    return "보통" in text or "의결권 있는" in text


def _is_cash_dividend_per_share_row(label: str | None) -> bool:
    normalized = re.sub(r"\s+", "", label or "")
    return normalized == "주당현금배당금(원)"


def parse_dividend_per_share_by_year(
    payload: dict,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict[int, float]:
    """Parse DART allotment matters into common-stock cash DPS by fiscal year.

    The DART annual report endpoint returns three fiscal periods in one row:
    `thstrm`, `frmtrm`, and `lwfr`. For dividend display we need per-share
    cash dividends, not total dividends paid, so this parser intentionally
    ignores aggregate rows and preferred / non-voting stock rows.
    """
    if not isinstance(payload, dict) or payload.get("status") != "000":
        return {}

    out: dict[int, float] = {}
    offsets = {"thstrm": 0, "frmtrm": -1, "lwfr": -2}
    for item in payload.get("list") or []:
        if not _is_cash_dividend_per_share_row(item.get("se")):
            continue
        if not _is_common_stock_dividend_kind(item.get("stock_knd")):
            continue

        settlement_date = str(item.get("stlm_dt") or "")
        match = re.match(r"(\d{4})", settlement_date)
        if not match:
            continue
        base_year = int(match.group(1))

        for field, offset in offsets.items():
            year = base_year + offset
            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue
            value = _parse_amount(item.get(field))
            if value is not None:
                out[year] = value

    return out


async def fetch_dividend_per_share_by_year(
    corp_code: str | None,
    start_year: int = DART_ANNUAL_DATA_START_YEAR,
    end_year: int | None = None,
) -> dict[int, float]:
    """Fetch common-stock cash DPS from DART annual allotment matters."""
    if not corp_code or not api_key():
        return {}
    if end_year is None:
        end_year = datetime.now().year - 1

    start_year = max(start_year, DART_ANNUAL_DATA_START_YEAR)
    # Current-year annual reports are not available yet. Querying them first
    # only adds latency and usually returns "no data".
    latest_report_year = min(end_year, datetime.now().year - 1)
    if latest_report_year < start_year:
        return {}

    out: dict[int, float] = {}
    client = await get_http_client("dart")
    for report_year in range(latest_report_year, start_year - 1, -3):
        _quota.note_call()
        resp = await client.get(
            f"{BASE_URL}/alotMatter.json",
            params={
                "crtfc_key": api_key(),
                "corp_code": corp_code,
                "bsns_year": str(report_year),
                "reprt_code": "11011",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        out.update(parse_dividend_per_share_by_year(data, start_year, end_year))
        await asyncio.sleep(0.15)

    return out


async def fetch_financial_statement(
    corp_code: str, year: int
) -> dict | None:
    """단일 회사의 단일 연도 재무제표를 가져온다. CFS 우선, OFS 폴백."""
    result = {}

    for report_code in ["CFS", "OFS"]:
        params = {
            "crtfc_key": api_key(),
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": "11011",  # 사업보고서(연간)
            "fs_div": report_code,
        }
        client = await get_http_client("dart")
        _quota.note_call()
        resp = await client.get(
            f"{BASE_URL}/fnlttSinglAcnt.json", params=params, timeout=15
        )

        if resp.status_code != 200:
            continue

        data = resp.json()
        if data.get("status") != "000":
            continue

        items = data.get("list", [])
        for item in items:
            account_nm = item.get("account_nm", "")
            amount_str = item.get("thstrm_amount")

            for key in ACCOUNT_NAMES:
                if key not in result and _match_account(account_nm, key):
                    val = _parse_amount(amount_str)
                    if val is not None:
                        result[key] = val

        if result:
            break

    if not result:
        return None

    result["year"] = year
    return result


async def fetch_annual_report_dates(
    corp_code: str, start_year: int = DART_ANNUAL_DATA_START_YEAR, end_year: int | None = None
) -> dict[int, str]:
    """사업보고서 접수일을 회계연도별로 반환한다."""
    if end_year is None:
        from datetime import datetime
        end_year = datetime.now().year - 1

    try:
        items = await fetch_filing_list(
            corp_code,
            f"{start_year + 1}0101",
            f"{end_year + 1}1231",
            last_reprt_at="Y",
            pblntf_ty="A",
            page_count=100,
            timeout=20,
        )
    except DartListError:
        return {}

    report_dates: dict[int, str] = {}
    for item in items:
        report_nm = item.get("report_nm", "")
        if not report_nm.startswith("사업보고서"):
            continue
        match = re.search(r"\((\d{4})\.", report_nm)
        if not match:
            continue
        year = int(match.group(1))
        if year < start_year or year > end_year:
            continue
        report_dates[year] = item.get("rcept_dt", "")

    return report_dates


async def fetch_recent_disclosures(corp_code: str, *, days: int = 30, page_count: int = 20) -> list[dict]:
    """특정 회사의 최근 공시 목록(최신순) — 신규 공시 알림용.

    보고서명 필터(저신호 제외 등)는 호출 측 책임이다. 반환 항목:
    ``{rcept_no, report_nm, rcept_dt, corp_name}``. 접수일 내림차순(최신 우선).
    """
    if not corp_code or not api_key():
        return []
    end = datetime.now()
    start = end - timedelta(days=days)
    try:
        items = await fetch_filing_list(
            corp_code,
            start.strftime("%Y%m%d"),
            end.strftime("%Y%m%d"),
            sort="date",
            sort_mth="desc",
            page_no=1,
            page_count=page_count,
            timeout=15,
            # 알림 엔진이 자체 10분 캐시를 두므로 여기서는 짧게(동시 호출 합치기용).
            cache_ttl=RECENT_DISCLOSURES_CACHE_TTL_S,
        )
    except DartListError:
        return []  # "013"/"014" = 공시 없음(정상), 그 외 상태·HTTP 오류도 빈 목록
    return [
        {
            "rcept_no": item.get("rcept_no"),
            "report_nm": item.get("report_nm"),
            "rcept_dt": item.get("rcept_dt"),
            "corp_name": item.get("corp_name"),
        }
        for item in items
    ]


# ---------------------------------------------------------------------------
# list.json — 단일 클라이언트 (X3/O4)
# ---------------------------------------------------------------------------
#
# 공시검색(list.json)을 호출하던 4곳(마켓테이프·일일 시황·정기보고서 리뷰·사업보고서
# 접수일/신규공시 알림)이 모두 여기를 지난다.
#
# * 캐시: 쿼리(파라미터) 단위 MemoryTTLCache, 기본 10분. 동시 호출은 single-flight.
# * 쿼터 가드: OpenDART 키는 일 20,000회 한도. 이 프로세스의 DART 호출 수를 KST
#   날짜별로 세고, 비필수 호출(마켓테이프)은 예산(기본 16,000)을 넘으면 멈춘다.
#   DART 가 status 020(한도 초과)을 돌려주면 KST 자정까지 list.json 을 막는다.
# * 시간 게이트: 마켓테이프의 공시 조회는 07:00–20:00 KST 에만 한다
#   (``in_disclosure_hours``).

DAILY_QUOTA = int(os.getenv("OPENDART_DAILY_QUOTA", "20000"))
NONESSENTIAL_BUDGET = int(os.getenv("OPENDART_NONESSENTIAL_BUDGET", "16000"))
LIST_CACHE_TTL_S = float(os.getenv("OPENDART_LIST_CACHE_TTL_S", "600"))
RECENT_DISCLOSURES_CACHE_TTL_S = float(os.getenv("OPENDART_RECENT_CACHE_TTL_S", "60"))
DISCLOSURE_HOURS_KST = (7, 20)  # [시작, 끝) — 마켓테이프 공시 조회 허용 시간대

_list_cache = MemoryTTLCache("dart.list", LIST_CACHE_TTL_S, evict_expired_after=0)


class DartListError(ExternalServiceError):
    """list.json 실패 — HTTP 오류(``http_status``) 또는 DART 상태코드(``dart_status``)."""

    default_detail = "DART 공시검색에 실패했습니다."

    def __init__(self, message: str, *, http_status: int | None = None, dart_status: str | None = None):
        super().__init__(message)
        self.message = message
        self.http_status = http_status
        self.dart_status = dart_status


class DartQuotaError(DartListError, RateLimitError):
    """DART 일일 한도 초과(020) 또는 비필수 호출 예산 소진."""

    default_detail = "OpenDART 일일 호출 한도에 도달했습니다."


def _kst_today(now: datetime | None = None) -> str:
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(KST).strftime("%Y%m%d")


class _QuotaGuard:
    """KST 날짜별 DART 호출 수와 020(한도 초과) 차단 상태."""

    def __init__(self) -> None:
        self.day = ""
        self.calls = 0
        self.exhausted_day: str | None = None

    def _roll(self) -> str:
        today = _kst_today()
        if self.day != today:
            self.day = today
            self.calls = 0
        if self.exhausted_day is not None and self.exhausted_day != today:
            self.exhausted_day = None
        return today

    def note_call(self) -> None:
        self._roll()
        self.calls += 1

    def check(self, *, essential: bool) -> None:
        today = self._roll()
        if self.exhausted_day == today:
            raise DartQuotaError(
                "OpenDART 일일 호출 한도 초과(020) — KST 자정까지 공시검색을 중지합니다.",
                dart_status="020",
            )
        if not essential and self.calls >= NONESSENTIAL_BUDGET:
            raise DartQuotaError(
                f"OpenDART 호출 예산 보호 — 오늘 {self.calls}회 사용, 비필수 공시검색을 건너뜁니다."
            )

    async def mark_exhausted(self) -> None:
        today = self._roll()
        first = self.exhausted_day != today
        self.exhausted_day = today
        if first:
            logger.error("OpenDART daily quota exhausted (status 020) after %s calls today", self.calls)
            import observability  # 지연 import — dart_client 는 여러 leaf 모듈이 import 한다

            # record_event 는 기본(wait=False)으로 DB 쓰기를 분리 task 로 띄우고 곧바로 반환한다.
            await observability.record_event(
                "dart",
                "quota_exhausted",
                level="error",
                details={"day": today, "calls": self.calls, "quota": DAILY_QUOTA},
            )

    def reset(self) -> None:
        self.day = ""
        self.calls = 0
        self.exhausted_day = None


_quota = _QuotaGuard()


def quota_status() -> dict:
    """관리/헬스 화면용 — 오늘(KST) DART 호출 수와 차단 여부."""
    today = _quota._roll()
    return {
        "day": today,
        "calls": _quota.calls,
        "quota": DAILY_QUOTA,
        "nonessential_budget": NONESSENTIAL_BUDGET,
        "exhausted": _quota.exhausted_day == today,
    }


def in_disclosure_hours(now: datetime | None = None) -> bool:
    """마켓테이프 공시 조회 시간 게이트 — 07:00 ≤ KST < 20:00."""
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    hour = moment.astimezone(KST).hour
    start, end = DISCLOSURE_HOURS_KST
    return start <= hour < end


async def fetch_filing_list(
    corp_code: str,
    bgn_de: str,
    end_de: str,
    *,
    pblntf_ty: str | None = None,
    last_reprt_at: str | None = None,
    sort: str | None = None,
    sort_mth: str | None = None,
    page_no: int | None = None,
    page_count: int = 100,
    timeout: float = 20.0,
    essential: bool = True,
    cache_ttl: float | None = None,
) -> list[dict]:
    """OpenDART 공시검색(list.json) 단일 진입점. 반환은 원본 ``list`` 항목들.

    status 000 → 목록, 013(조회 데이터 없음) → ``[]``. 그 외 상태·HTTP 오류는
    :class:`DartListError`, 한도(020)·예산 소진은 :class:`DartQuotaError`.
    네트워크 오류(``httpx.HTTPError``)는 그대로 전파한다. 성공 응답만 캐시된다.
    ``essential=False`` 호출은 일일 예산에 가까워지면 upstream 없이 거절된다.
    """
    params: dict[str, str] = {
        "corp_code": corp_code,
        "bgn_de": bgn_de,
        "end_de": end_de,
    }
    optional = {
        "last_reprt_at": last_reprt_at,
        "pblntf_ty": pblntf_ty,
        "sort": sort,
        "sort_mth": sort_mth,
        "page_no": page_no,
    }
    params.update({name: str(value) for name, value in optional.items() if value is not None})
    params["page_count"] = str(page_count)
    cache_key = "&".join(f"{name}={params[name]}" for name in sorted(params))

    async def load() -> list[dict]:
        _quota.check(essential=essential)
        client = await get_http_client("dart")
        _quota.note_call()
        resp = await client.get(
            f"{BASE_URL}/list.json", params={"crtfc_key": api_key(), **params}, timeout=timeout
        )
        if resp.status_code != 200:
            raise DartListError(f"DART HTTP {resp.status_code}", http_status=resp.status_code)
        try:
            data = resp.json()
        except ValueError as exc:
            raise DartListError("DART 응답 JSON 파싱 실패") from exc
        status = str(data.get("status") or "")
        if status == "000":
            return list(data.get("list") or [])
        if status == "013":
            return []
        if status == "020":
            await _quota.mark_exhausted()
            raise DartQuotaError(data.get("message") or "DART status 020", dart_status=status)
        raise DartListError(data.get("message") or f"DART status {status}", dart_status=status)

    ttl = LIST_CACHE_TTL_S if cache_ttl is None else cache_ttl
    return await cached_fetch(_list_cache, cache_key, load, ttl=ttl)
