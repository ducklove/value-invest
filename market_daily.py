from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Any, AsyncIterator

import httpx

import ai_config
import dart_client
import market_indicators
import market_movers
import market_news
import market_sessions
from cache_layer import MemoryTTLCache
from core.http import get_http_client
from repositories import corp_codes
from services import ai_client

logger = logging.getLogger(__name__)

DART_VIEWER_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

QUOTE_CONCURRENCY = int(os.environ.get("MARKET_DAILY_QUOTE_CONCURRENCY", "6"))
DISCLOSURE_CONCURRENCY = int(os.environ.get("MARKET_DAILY_DISCLOSURE_CONCURRENCY", "4"))
NEWS_STOCK_LIMIT = int(os.environ.get("MARKET_DAILY_NEWS_STOCK_LIMIT", "8"))
NEWS_PER_STOCK = int(os.environ.get("MARKET_DAILY_NEWS_PER_STOCK", "4"))
MARKET_DAILY_MAX_TOKENS = int(os.environ.get("MARKET_DAILY_MAX_TOKENS", "1800"))
# 일시 오류(타임아웃·빈 응답)가 실패 본문으로 굳지 않게 LLM 호출을 짧게 재시도.
MARKET_DAILY_LLM_ATTEMPTS = int(os.environ.get("MARKET_DAILY_LLM_ATTEMPTS", "2"))


@asynccontextmanager
async def _market_daily_client(name: str) -> AsyncIterator[httpx.AsyncClient]:
    yield await get_http_client(name)
# 시황 브리프 evidence 상한 — 업종 등락 / 시장 전반 뉴스 / 시장 전체 급등락(프롬프트 압축용)
BRIEF_SECTOR_LIMIT = int(os.environ.get("MARKET_DAILY_SECTOR_LIMIT", "12"))
BRIEF_MARKET_NEWS_LIMIT = int(os.environ.get("MARKET_DAILY_MARKET_NEWS_LIMIT", "8"))
BRIEF_MOVER_LIMIT = int(os.environ.get("MARKET_DAILY_MOVER_LIMIT", "24"))
# 소스별 수집 타임아웃 — 업스트림 하나가 매달려도 브리프 전체가 hang 하지 않게.
BRIEF_SOURCE_TIMEOUT_S = float(os.environ.get("MARKET_DAILY_SOURCE_TIMEOUT_S", "25"))
MARKET_TAPE_TTL_SECONDS = int(os.environ.get("MARKET_TAPE_TTL_SECONDS", "45"))
MARKET_TAPE_EVENT_LIMIT = int(os.environ.get("MARKET_TAPE_EVENT_LIMIT", "40"))
# 마켓테이프 종목 선정(시장 전체 기준) — 시총상위/급등락 각 시장별 상한 개수
TAPE_MARKET_CAP_COUNT = int(os.environ.get("MARKET_TAPE_MARKET_CAP_COUNT", "6"))
TAPE_MOVER_COUNT = int(os.environ.get("MARKET_TAPE_MOVER_COUNT", "8"))
# |등락률| 이 값 이상이면 상한가/하한가로 간주(KRX ±30% 밴드, 호가단위 보정으로 ~29.x%)
TAPE_LIMIT_PCT = float(os.environ.get("MARKET_TAPE_LIMIT_PCT", "29.0"))
# 급등/급락 최소 등락률. Naver 급상승/급하락 페이지는 변동폭과 무관하게 상위 N개를
# 늘 반환하므로, 이 값 미만은 '이슈'로 보지 않고 버린다(시총상위는 변동과 무관하게 유지).
TAPE_SURGE_PCT = float(os.environ.get("MARKET_TAPE_SURGE_PCT", "5.0"))

INVESTOR_FLOW_LIMIT = int(os.environ.get("MARKET_DAILY_INVESTOR_FLOW_LIMIT", "10"))

NAVER_MOBILE_NEWS_API = "https://m.stock.naver.com/api/news/stock/{code}?pageSize={size}&page=1"
NAVER_FRGN_URL = "https://finance.naver.com/item/frgn.naver?code={code}&page=1"

_NEWS_CACHE_TTL_SECONDS = 600
_NEWS_CACHE = MemoryTTLCache("market_daily.news", _NEWS_CACHE_TTL_SECONDS)
_FLOW_CACHE = MemoryTTLCache("market_daily.flows", 600)
_TAPE_CACHE = MemoryTTLCache("market_daily.tape", MARKET_TAPE_TTL_SECONDS)

_MATERIAL_DISCLOSURE_KEYWORDS = [
    "유상증자",
    "무상증자",
    "전환사채",
    "신주인수권",
    "교환사채",
    "자사주",
    "주식소각",
    "배당",
    "현금배당",
    "실적",
    "잠정",
    "매출액",
    "영업이익",
    "공급계약",
    "단일판매",
    "수주",
    "합병",
    "분할",
    "영업양수",
    "영업양도",
    "타법인",
    "최대주주",
    "대표이사",
    "소송",
    "횡령",
    "배임",
    "회생",
    "파산",
    "상장폐지",
    "관리종목",
    "매매거래정지",
    "불성실공시",
]

_LOW_SIGNAL_DISCLOSURE_KEYWORDS = [
    "기업설명회",
    "IR",
    "사업보고서",
    "반기보고서",
    "분기보고서",
    "정기보고서",
    "감사보고서제출",
    "주주총회소집공고",
    "의결권대리행사권유참고서류",
    "주주총회집중일개최사유신고",
]

_SECURITIES_DISCLOSURE_NAME_HINTS = [
    "증권",
    "투자증권",
    "증권금융",
    "선물",
]

_SECURITIES_LOW_SIGNAL_DISCLOSURE_KEYWORDS = [
    "투자설명서",
    "일괄신고",
    "증권신고서",
    "증권발행실적보고서",
    "발행실적보고서",
    "파생결합증권",
    "주식워런트증권",
    "상장지수증권",
    "상장지수집합투자기구",
    "집합투자증권",
    "증권투자신탁",
]


def _today_iso() -> str:
    return date.today().isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text in {"-", "N/A"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _signed_change_pct(raw_pct: Any, direction: str | None = None) -> float | None:
    pct = _safe_float(raw_pct)
    if pct is None:
        return None
    direction = (direction or "").lower()
    if direction in {"down", "dn", "minus"}:
        return -abs(pct)
    if direction in {"up", "plus"}:
        return abs(pct)
    return pct


def estimate_gemini35_flash_cost(input_tokens: int = 0, output_tokens: int = 0, *, batch: bool = False) -> float:
    input_price = 0.75 if batch else 1.50
    output_price = 4.50 if batch else 9.00
    return (input_tokens * input_price + output_tokens * output_price) / 1_000_000


def _material_disclosure_reason(report_name: str) -> str | None:
    normalized = re.sub(r"\s+", "", report_name or "")
    for keyword in _MATERIAL_DISCLOSURE_KEYWORDS:
        if keyword in normalized:
            return keyword
    return None


def _compact_disclosure_text(*values: Any) -> str:
    text = html.unescape(" ".join(str(value or "") for value in values))
    text = re.sub(r"\s+", "", text)
    return text.replace("ㆍ", "").replace("·", "").lower()


def _matches_disclosure_keyword(text: str, keywords: list[str]) -> bool:
    return any(_compact_disclosure_text(keyword) in text for keyword in keywords)


def _is_securities_disclosure(row: dict[str, Any]) -> bool:
    issuer_text = _compact_disclosure_text(row.get("stock_name"), row.get("corp_name"))
    return _matches_disclosure_keyword(issuer_text, _SECURITIES_DISCLOSURE_NAME_HINTS)


def _is_low_signal_disclosure(row: dict[str, Any]) -> bool:
    report_text = _compact_disclosure_text(row.get("report_name"))
    if _matches_disclosure_keyword(report_text, _LOW_SIGNAL_DISCLOSURE_KEYWORDS):
        return True
    if _is_securities_disclosure(row):
        return _matches_disclosure_keyword(report_text, _SECURITIES_LOW_SIGNAL_DISCLOSURE_KEYWORDS)
    return False


def _should_show_disclosure_on_tape(row: dict[str, Any]) -> bool:
    report_name = row.get("report_name") or ""
    if not report_name or _is_low_signal_disclosure(row):
        return False
    return bool(row.get("is_material") or row.get("material_reason") or _material_disclosure_reason(report_name))


async def _market_snapshot() -> tuple[list[dict[str, Any]], float | None]:
    codes = ["KOSPI", "KOSDAQ", "KOSPI200", "USD_KRW", "SPX", "IXIC", "US10Y", "OIL_CL"]
    data = await market_indicators.fetch_indicators(codes)
    rows: list[dict[str, Any]] = []
    kospi_pct = None
    for code in codes:
        raw = data.get(code) or {}
        pct = _signed_change_pct(raw.get("change_pct"), raw.get("direction"))
        row = {
            "code": code,
            "label": market_indicators.CATALOG.get(code, {}).get("label", code),
            "value": raw.get("value") or "",
            "change": raw.get("change") or "",
            "change_pct": pct,
            "direction": raw.get("direction") or "",
        }
        if code == "KOSPI":
            kospi_pct = pct
        rows.append(row)
    return rows, kospi_pct


# 홈 지수·24h 매크로는 상시 노출, 해외 주가지수는 그 시장이 열렸을 때만(닫히면 종가라 stale).
_TAPE_HOME_INDEX_CODES = ["KOSPI", "KOSDAQ"]
_TAPE_OPEN_INDEX_CODES = {
    "US": ["SPX", "IXIC", "DJI"],
    "JP": ["NI225"],
    "HK": ["HSI"],
    "CN": ["SHC"],
}
_TAPE_MACRO_CODES = ["USD_KRW", "OIL_CL"]  # 환율·원유는 사실상 24h라 항상 유효


def _tape_index_codes(open_markets: set[str]) -> list[str]:
    """현재 열린 시장 위주로 테이프에 노출할 지수 코드 목록(순서 보존, 중복 제거)."""
    codes = list(_TAPE_HOME_INDEX_CODES)
    for market in ("US", "JP", "HK", "CN"):
        if market in open_markets:
            codes.extend(_TAPE_OPEN_INDEX_CODES[market])
    codes.extend(_TAPE_MACRO_CODES)
    seen: set[str] = set()
    ordered: list[str] = []
    for code in codes:
        if code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


async def _tape_index_rows(now: datetime | None = None) -> list[dict[str, Any]]:
    """열린 시장 기준 지수 시세 행. KOSPI200은 _market_event 단계에서 한 번 더 걸러짐."""
    codes = _tape_index_codes(market_sessions.open_markets(now))
    data = await market_indicators.fetch_indicators(codes)
    rows: list[dict[str, Any]] = []
    for code in codes:
        raw = data.get(code) or {}
        rows.append({
            "code": code,
            "label": market_indicators.CATALOG.get(code, {}).get("label", code),
            "value": raw.get("value") or "",
            "change": raw.get("change") or "",
            "change_pct": _signed_change_pct(raw.get("change_pct"), raw.get("direction")),
            "direction": raw.get("direction") or "",
        })
    return rows


# 한 종목이 여러 랭킹에 동시에 들면 더 강한 시그널을 채택(상/하한가 > 급등락 > 시총).
_TAPE_BUCKET_RANK = {"상한가": 4, "하한가": 4, "급등": 3, "급락": 3, "시총": 1}


def _mover_bucket(kind: str, pct: float | None) -> str | None:
    """랭킹 종류+등락률을 테이프 배지로 매핑. 급등락 미달(노이즈)은 None(=버림).

    시총상위는 변동폭과 무관하게 항상 노출(대형주 시세). 급상승/급하락 랭킹은
    ±TAPE_LIMIT_PCT 이상이면 상/하한가, ±TAPE_SURGE_PCT 이상이면 급등/급락,
    그 미만이면 None.
    """
    if kind == "market_cap":
        return "시총"
    if not isinstance(pct, (int, float)):
        return None
    if kind == "rising":
        if pct >= TAPE_LIMIT_PCT:
            return "상한가"
        return "급등" if pct >= TAPE_SURGE_PCT else None
    if kind == "falling":
        if pct <= -TAPE_LIMIT_PCT:
            return "하한가"
        return "급락" if pct <= -TAPE_SURGE_PCT else None
    return None


async def _tape_movers() -> list[dict[str, Any]]:
    """시총상위·급등·급락(상/하한가 포함)을 코스피/코스닥에서 모아 종목당 1건으로 정리."""
    specs = [
        ("market_cap", "kospi", TAPE_MARKET_CAP_COUNT),
        ("market_cap", "kosdaq", TAPE_MARKET_CAP_COUNT),
        ("rising", "kospi", TAPE_MOVER_COUNT),
        ("rising", "kosdaq", TAPE_MOVER_COUNT),
        ("falling", "kospi", TAPE_MOVER_COUNT),
        ("falling", "kosdaq", TAPE_MOVER_COUNT),
    ]
    results = await asyncio.gather(
        *(market_movers.fetch_market_movers(kind, market, count) for kind, market, count in specs),
        return_exceptions=True,
    )
    best: dict[str, dict[str, Any]] = {}
    for (kind, market, _count), rows in zip(specs, results):
        if isinstance(rows, BaseException):
            logger.info("tape movers fetch failed (%s/%s): %s", kind, market, rows)
            continue
        for row in rows:
            code = (row.get("code") or "").strip()
            name = (row.get("name") or "").strip()
            if not code or not name:
                continue
            pct = _safe_float(row.get("change_pct"))
            bucket = _mover_bucket(kind, pct)
            if bucket is None:  # 급등락 미달 등 노이즈는 제외
                continue
            candidate = {
                "stock_code": code,
                "stock_name": name,
                "price": row.get("price"),
                "change_pct": pct,
                "direction": row.get("direction") or "",
                "bucket": bucket,
                "market": market,
            }
            existing = best.get(code)
            if existing is None or _TAPE_BUCKET_RANK.get(bucket, 0) > _TAPE_BUCKET_RANK.get(existing["bucket"], 0):
                best[code] = candidate
    return list(best.values())


async def _fetch_dart_disclosures(interests: list[dict[str, Any]], brief_date: str) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    if not dart_client.API_KEY:
        return [], ["OPENDART_API_KEY가 없어 관심종목 공시 조회를 건너뜀"]

    corp_rows: list[tuple[dict[str, Any], str]] = []
    for item in interests:
        try:
            corp_code = await corp_codes.get_corp_code(item["stock_code"])
        except Exception:
            corp_code = None
        if corp_code:
            corp_rows.append((item, corp_code))

    yyyymmdd = brief_date.replace("-", "")
    semaphore = asyncio.Semaphore(max(1, DISCLOSURE_CONCURRENCY))
    disclosures: list[dict[str, Any]] = []

    async with _market_daily_client("dart") as client:
        async def fetch_one(item: dict[str, Any], corp_code: str) -> None:
            async with semaphore:
                params = {
                    "crtfc_key": dart_client.API_KEY,
                    "corp_code": corp_code,
                    "bgn_de": yyyymmdd,
                    "end_de": yyyymmdd,
                    "page_count": "20",
                }
                try:
                    resp = await client.get(f"{dart_client.BASE_URL}/list.json", params=params, timeout=10.0)
                    if resp.status_code != 200:
                        warnings.append(f"{item['stock_code']} DART HTTP {resp.status_code}")
                        return
                    payload = resp.json()
                except Exception as exc:
                    warnings.append(f"{item['stock_code']} DART 조회 실패: {exc}")
                    return
                if payload.get("status") not in {"000", "013"}:
                    warnings.append(f"{item['stock_code']} DART status {payload.get('status')}")
                    return
                for raw in payload.get("list") or []:
                    report_name = str(raw.get("report_nm") or "").strip()
                    rcept_no = str(raw.get("rcept_no") or "").strip()
                    reason = _material_disclosure_reason(report_name)
                    disclosures.append(
                        {
                            "stock_code": item["stock_code"],
                            "stock_name": item.get("stock_name") or raw.get("corp_name") or item["stock_code"],
                            "corp_name": raw.get("corp_name") or item.get("stock_name") or "",
                            "report_name": report_name,
                            "rcept_no": rcept_no,
                            "rcept_dt": raw.get("rcept_dt") or "",
                            "filer": raw.get("flr_nm") or "",
                            "remark": raw.get("rm") or "",
                            "url": DART_VIEWER_URL.format(rcept_no=rcept_no) if rcept_no else "",
                            "is_material": bool(reason),
                            "material_reason": reason,
                        }
                    )

        await asyncio.gather(*(fetch_one(item, corp_code) for item, corp_code in corp_rows))

    disclosures.sort(key=lambda row: (0 if row.get("is_material") else 1, row.get("stock_code") or ""))
    return disclosures[:30], warnings[:8]


def _clean_html_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html.unescape(value or ""))).strip()


def _format_naver_news_datetime(raw: Any) -> str:
    text = str(raw or "").strip()
    if len(text) == 12 and text.isdigit():
        return f"{text[0:4]}.{text[4:6]}.{text[6:8]} {text[8:10]}:{text[10:12]}"
    return text


def _map_news_item(item: dict[str, Any], stock_code: str) -> dict[str, Any] | None:
    """Map one Naver mobile news API item to the brief's news shape.

    Pure (no network) so the field mapping is unit-testable. Adds ``snippet``
    (the article lead paragraph) so the model can reason about causation from
    actual content, not just the headline.
    """
    title = _clean_html_text(item.get("titleFull") or item.get("title") or "")
    if not title:
        return None
    office_id = str(item.get("officeId") or "").strip()
    article_id = str(item.get("articleId") or "").strip()
    url = item.get("mobileNewsUrl") or (
        f"https://finance.naver.com/item/news_read.naver?article_id={article_id}"
        f"&office_id={office_id}&code={stock_code}"
        if article_id and office_id
        else ""
    )
    snippet = _clean_html_text(item.get("body") or "")
    if len(snippet) > 160:
        snippet = snippet[:160].rstrip() + "…"
    return {
        "stock_code": stock_code,
        "title": title,
        "outlet": _clean_html_text(item.get("officeName") or ""),
        "published_at": _format_naver_news_datetime(item.get("datetime")),
        "url": url,
        "snippet": snippet,
    }


def _flatten_news_payload(payload: Any) -> list[dict[str, Any]]:
    items = payload if isinstance(payload, list) else (payload.get("items") if isinstance(payload, dict) else [])
    flat: list[dict[str, Any]] = []
    for entry in items or []:
        if isinstance(entry, dict) and isinstance(entry.get("items"), list):
            flat.extend(x for x in entry["items"] if isinstance(x, dict))
        elif isinstance(entry, dict):
            flat.append(entry)
    return flat


async def _fetch_stock_news(stock_code: str, limit: int = NEWS_PER_STOCK) -> list[dict[str, Any]]:
    cached = _NEWS_CACHE.get_entry(stock_code)
    if cached is not None:
        return cached.value[:limit]

    try:
        url = NAVER_MOBILE_NEWS_API.format(code=stock_code, size=max(limit, 1) * 2)
        async with _market_daily_client("naver") as client:
            resp = await client.get(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://m.stock.naver.com/"},
                timeout=8.0,
            )
        news: list[dict[str, Any]] = []
        for item in _flatten_news_payload(resp.json()):
            mapped = _map_news_item(item, stock_code)
            if mapped:
                news.append(mapped)
            if len(news) >= limit:
                break
        _NEWS_CACHE.set(stock_code, news)
        return news
    except Exception as exc:
        logger.info("daily market: news skipped for %s: %s", stock_code, exc)
        return []


def _parse_investor_flow(html: str) -> dict[str, Any] | None:
    """Parse the latest dated row of Naver's frgn ``table.type2``.

    Columns: 0 date · 1 close · 2 prev-diff · 3 chg% · 4 volume ·
    5 institution net · 6 foreign net · 7 foreign-held · 8 foreign-held%.
    Net columns are signed share counts (음수 = 순매도). Pure/unit-testable.
    """
    for table in re.findall(r'<table[^>]*class="type2".*?</table>', html or "", re.DOTALL):
        for row in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.DOTALL):
            if not re.search(r"\d{4}\.\d{2}\.\d{2}", row):
                continue
            cells = [
                re.sub(r"<[^>]+>", "", c).strip().replace(",", "")
                for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            ]
            if len(cells) < 7:
                continue
            institution = _safe_float(cells[5])
            foreign = _safe_float(cells[6])
            if institution is None and foreign is None:
                return None
            return {
                "date": cells[0].strip(),
                "institution_net": int(institution) if institution is not None else None,
                "foreign_net": int(foreign) if foreign is not None else None,
            }
    return None


async def _fetch_investor_flow(stock_code: str) -> dict[str, Any] | None:
    cached = _FLOW_CACHE.get_entry(stock_code)
    if cached is not None:
        return cached.value
    try:
        async with _market_daily_client("naver") as client:
            resp = await client.get(
                NAVER_FRGN_URL.format(code=stock_code),
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"},
                timeout=8.0,
            )
        parsed = _parse_investor_flow(resp.content.decode("euc-kr", errors="ignore"))
        result = {"stock_code": stock_code, **parsed} if parsed else None
        _FLOW_CACHE.set(stock_code, result)
        return result
    except Exception as exc:
        logger.info("daily market: investor flow skipped for %s: %s", stock_code, exc)
        return None


async def _investor_flows_for_codes(
    codes: list[str], stock_names: dict[str, str] | None = None
) -> list[dict[str, Any]]:
    unique: list[str] = []
    seen: set[str] = set()
    for code in codes:
        if code and code not in seen:
            unique.append(code)
            seen.add(code)
    semaphore = asyncio.Semaphore(max(1, QUOTE_CONCURRENCY))

    async def fetch_one(code: str) -> dict[str, Any] | None:
        async with semaphore:
            return await _fetch_investor_flow(code)

    rows = await asyncio.gather(*(fetch_one(code) for code in unique[:INVESTOR_FLOW_LIMIT]))
    names = stock_names or {}
    out: list[dict[str, Any]] = []
    for row in rows:
        if row:
            name = names.get(row["stock_code"])
            out.append({**row, **({"stock_name": name} if name else {})})
    return out


async def _news_for_focus_codes(
    codes: list[str],
    stock_names: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    unique = []
    seen = set()
    for code in codes:
        if code and code not in seen:
            unique.append(code)
            seen.add(code)
    batches = await asyncio.gather(*(_fetch_stock_news(code) for code in unique[:NEWS_STOCK_LIMIT]))
    names = stock_names or {}
    rows: list[dict[str, Any]] = []
    for batch in batches:
        for item in batch:
            code = item.get("stock_code") or ""
            name = names.get(code)
            rows.append({**item, **({"stock_name": name} if name else {})})
    return rows


def _source_hash(payload: dict[str, Any]) -> str:
    source = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _event_id(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _format_pct(value: float | None) -> str:
    return f"{value:+.2f}%" if isinstance(value, (int, float)) else "-"


def _format_price(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    if abs(number) >= 1000:
        return f"{number:,.0f}"
    return f"{number:,.2f}"


def _severity_rank(severity: str) -> int:
    return {"breaking": 0, "alert": 1, "watch": 2, "info": 3}.get(severity, 4)


def _type_rank(event_type: str) -> int:
    return {"disclosure": 0, "stock_move": 1, "news": 2, "index_move": 3}.get(event_type, 4)


def _market_event(row: dict[str, Any]) -> dict[str, Any] | None:
    code = row.get("code") or ""
    # KOSPI200은 인트라데이 현행화가 안 돼 시세가 stale → 테이프에는 노출하지 않는다.
    # (일일 브리핑용 _market_snapshot 에는 그대로 남겨 둠)
    if code == "KOSPI200":
        return None
    label = row.get("label") or code
    value = row.get("value") or "-"
    pct = row.get("change_pct")
    direction = "up" if isinstance(pct, (int, float)) and pct > 0 else "down" if isinstance(pct, (int, float)) and pct < 0 else "flat"
    abs_pct = abs(pct) if isinstance(pct, (int, float)) else 0.0
    domestic_index = code in {"KOSPI", "KOSDAQ", "KOSPI200"}
    severity = "breaking" if domestic_index and abs_pct >= 2 else "alert" if abs_pct >= 1 else "info"
    return {
        "id": _event_id("index", code, value, pct),
        "type": "index_move",
        "severity": severity,
        "direction": direction,
        "badge": "지수" if domestic_index else "시장",
        "label": label,
        "text": f"{label} {value} {_format_pct(pct)}",
        "value": value,
        "change_pct": pct,
        "sort_key": [_severity_rank(severity), _type_rank("index_move"), code],
    }


def _stock_move_event(row: dict[str, Any]) -> dict[str, Any] | None:
    pct = row.get("change_pct")
    if not isinstance(pct, (int, float)):
        return None
    stock_code = row.get("stock_code") or ""
    name = row.get("stock_name") or stock_code
    price = _format_price(row.get("price"))
    is_notable = bool(row.get("is_notable"))
    abs_pct = abs(pct)
    severity = "breaking" if is_notable else "alert" if abs_pct >= 5 else "watch" if abs_pct >= 2 else "info"
    badge = row.get("move_type") if is_notable else "관심"
    direction = "up" if pct > 0 else "down" if pct < 0 else "flat"
    rel = row.get("relative_pct")
    detail = f"KOSPI 대비 {_format_pct(rel)}p" if isinstance(rel, (int, float)) and is_notable else ""
    parts = [f"{name} {_format_pct(pct)}"]
    if price:
        parts.append(price)
    if detail:
        parts.append(detail)
    return {
        "id": _event_id("stock", stock_code, row.get("date"), pct, row.get("relative_pct")),
        "type": "stock_move",
        "severity": severity,
        "direction": direction,
        "badge": badge or "관심",
        "label": name,
        "text": " · ".join(parts),
        "stock_code": stock_code,
        "stock_name": name,
        "change_pct": pct,
        "relative_pct": rel,
        "is_notable": is_notable,
        "sort_key": [_severity_rank(severity), _type_rank("stock_move"), -abs(row.get("relative_pct") or pct)],
    }


def _mover_event(row: dict[str, Any]) -> dict[str, Any] | None:
    """시장 전체 랭킹(_tape_movers) 1건을 테이프 종목 이벤트로 변환."""
    code = row.get("stock_code") or ""
    pct = row.get("change_pct")
    if not code or not isinstance(pct, (int, float)):
        return None
    name = row.get("stock_name") or code
    bucket = row.get("bucket") or "시총"
    direction = "up" if pct > 0 else "down" if pct < 0 else "flat"
    if bucket in ("상한가", "하한가"):
        severity = "breaking"
    elif bucket in ("급등", "급락"):
        severity = "alert"
    else:  # 시총 상위
        severity = "watch"
    parts = [f"{name} {_format_pct(pct)}"]
    price = _format_price(row.get("price"))
    if price:
        parts.append(price)
    return {
        "id": _event_id("mover", code, bucket, pct, row.get("price")),
        "type": "stock_move",
        "severity": severity,
        "direction": direction,
        "badge": bucket,
        "label": name,
        "text": " · ".join(parts),
        "stock_code": code,
        "stock_name": name,
        "change_pct": pct,
        "sort_key": [_severity_rank(severity), _type_rank("stock_move"), -abs(pct)],
    }


def _disclosure_event(row: dict[str, Any]) -> dict[str, Any] | None:
    report_name = row.get("report_name") or ""
    if not report_name:
        return None
    stock_code = row.get("stock_code") or ""
    name = row.get("stock_name") or row.get("corp_name") or stock_code
    reason = row.get("material_reason") or _material_disclosure_reason(report_name)
    is_material = bool(row.get("is_material") or reason)
    severity = "breaking" if is_material else "watch"
    return {
        "id": _event_id("disclosure", stock_code, row.get("rcept_no"), report_name),
        "type": "disclosure",
        "severity": severity,
        "direction": "flat",
        "badge": "공시",
        "label": name,
        "text": f"{name} {report_name}" + (f" · {reason}" if reason else ""),
        "stock_code": stock_code,
        "stock_name": name,
        "url": row.get("url") or "",
        "is_material": is_material,
        "sort_key": [_severity_rank(severity), _type_rank("disclosure"), stock_code],
    }


def _news_event(row: dict[str, Any]) -> dict[str, Any] | None:
    title = row.get("title") or ""
    if not title:
        return None
    stock_code = row.get("stock_code") or ""
    raw_name = row.get("stock_name") or row.get("corp_name") or ""
    name = raw_name if raw_name and raw_name != stock_code else ""
    outlet = row.get("outlet") or ""
    # \ub274\uc2a4 \uc81c\ubaa9 \uc55e [\uc885\ubaa9\uba85] \uba38\ub9ac\ud45c\ub294 \ubd99\uc774\uc9c0 \uc54a\ub294\ub2e4(\uac00\ub3c5\uc131 \u2014 \uc885\ubaa9 \uc815\ubcf4\ub294 data-stock-code \ub85c \uc720\uc9c0).
    text = title.strip()
    if outlet:
        text = f"{text} \u00b7 {outlet}"
    return {
        "id": _event_id("news", stock_code, row.get("published_at"), title),
        "type": "news",
        "severity": "watch",
        "direction": "flat",
        "badge": "뉴스",
        "stock_code": stock_code,
        "stock_name": name,
        "url": row.get("url") or "",
        "published_at": row.get("published_at") or "",
        "label": name or outlet or "\ub274\uc2a4",
        "text": text,
        "sort_key": [_severity_rank("watch"), _type_rank("news"), stock_code],
    }


def build_market_tape_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert collected market evidence into compact tape events."""
    candidates: list[dict[str, Any]] = []

    for row in payload.get("disclosures") or []:
        if not _should_show_disclosure_on_tape(row):
            continue
        event = _disclosure_event(row)
        if event:
            candidates.append(event)

    for row in payload.get("moves") or []:
        event = _stock_move_event(row)
        if event and (event.get("is_notable") or event.get("severity") != "info"):
            candidates.append(event)

    for row in payload.get("movers") or []:
        event = _mover_event(row)
        if event:
            candidates.append(event)

    for row in payload.get("news") or []:
        event = _news_event(row)
        if event:
            candidates.append(event)

    for row in payload.get("market") or []:
        event = _market_event(row)
        if event:
            candidates.append(event)

    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for event in candidates:
        event_id = event.get("id")
        if not event_id or event_id in seen:
            continue
        seen.add(event_id)
        deduped.append(event)

    deduped.sort(key=lambda event: event.get("sort_key") or [9])
    for event in deduped:
        event.pop("sort_key", None)
    return deduped[:MARKET_TAPE_EVENT_LIMIT]


async def build_market_tape(*, google_sub: str | None = None, refresh: bool = False) -> dict[str, Any]:
    # 테이프는 시장 전체 기준(열린 시장 지수·시총상위·상하한가·급등락)이라 사용자별로
    # 달라지지 않는다 → 공용 캐시 키 하나로 모든 사용자가 공유한다(google_sub는 무시).
    cache_key = "public"
    cached = _TAPE_CACHE.get_entry(cache_key)
    if cached is not None and not refresh:
        return {**cached.value, "cached": True}

    brief_date = _today_iso()
    market_rows = await _tape_index_rows()
    movers = await _tape_movers()

    # 이슈 종목(상/하한가·급등락)만 공시·뉴스 시드로 사용. 시총만인 종목은 제외.
    focus = [m for m in movers if m.get("bucket") in ("상한가", "하한가", "급등", "급락")]
    focus.sort(key=lambda m: abs(m.get("change_pct") or 0), reverse=True)
    focus = focus[:NEWS_STOCK_LIMIT]
    focus_interests = [{"stock_code": m["stock_code"], "stock_name": m["stock_name"]} for m in focus]
    focus_codes = [m["stock_code"] for m in focus]
    names = {m["stock_code"]: m["stock_name"] for m in movers}

    disclosures, disclosure_warnings = (
        await _fetch_dart_disclosures(focus_interests, brief_date) if focus_interests else ([], [])
    )
    news = await _news_for_focus_codes(focus_codes, names) if focus_codes else []
    payload = {
        "brief_date": brief_date,
        "generated_at": datetime.now().isoformat(),
        "market": market_rows,
        "movers": movers,
        "disclosures": disclosures,
        "news": news,
        "source_warnings": disclosure_warnings,
    }
    events = build_market_tape_events(payload)
    result = {
        "brief_date": brief_date,
        "generated_at": payload["generated_at"],
        "cached": False,
        "refresh_interval_seconds": max(30, MARKET_TAPE_TTL_SECONDS),
        "events": events,
        "counts": {
            "events": len(events),
            "breaking": sum(1 for event in events if event.get("severity") == "breaking"),
            "movers": len(movers),
            "disclosures": len(disclosures),
        },
    }
    _TAPE_CACHE.set(cache_key, result)
    return result


def _fallback_markdown(payload: dict[str, Any], message: str) -> str:
    market = payload.get("market") or []
    issues = [m for m in payload.get("movers") or [] if m.get("bucket") in ("상한가", "하한가", "급등", "급락")]
    headlines = payload.get("market_news") or []
    lines = ["### 금일 시황", "", message, ""]
    if market:
        lines.append("**시장 지표**")
        for row in market[:5]:
            pct = row.get("change_pct")
            pct_text = f"{pct:+.2f}%" if isinstance(pct, (int, float)) else "-"
            lines.append(f"- {row.get('label') or row.get('code')}: {row.get('value') or '-'} ({pct_text})")
        lines.append("")
    if issues:
        lines.append("**급등/급락·상하한가**")
        for row in issues[:8]:
            pct = row.get("change_pct")
            pct_text = f"{pct:+.2f}%" if isinstance(pct, (int, float)) else "-"
            lines.append(f"- {row.get('stock_name')}({row.get('stock_code')}): {pct_text} · {row.get('bucket')}")
        lines.append("")
    if headlines:
        lines.append("**주요 뉴스**")
        for row in headlines[:5]:
            outlet = row.get("source") or ""
            lines.append(f"- {row.get('title')}" + (f" · {outlet}" if outlet else ""))
    return "\n".join(lines).strip()


def _build_prompt(payload: dict[str, Any]) -> str:
    compact = dict(payload)
    compact["movers"] = (payload.get("movers") or [])[:BRIEF_MOVER_LIMIT]
    compact["disclosures"] = (payload.get("disclosures") or [])[:30]
    compact["news"] = (payload.get("news") or [])[: NEWS_STOCK_LIMIT * NEWS_PER_STOCK]
    compact["market_news"] = (payload.get("market_news") or [])[:BRIEF_MARKET_NEWS_LIMIT]
    evidence = json.dumps(compact, ensure_ascii=False, indent=2, default=str)
    return f"""당신은 한국 주식시장의 하루를 근거 중심으로 해석하는 투자 리서치 어시스턴트입니다. 아래 evidence bundle만 근거로 "오늘 시장 전체에 무슨 일이 있었는가"를 설명하는 한국어 시황 분석을 쓰세요. 이 글은 모든 독자가 공유하는 시장 분석입니다 — 특정 사용자의 포트폴리오·보유종목·관심종목 관점은 절대 넣지 마세요.

작성 규칙:
1. 시장 큰 그림부터: 첫 문단에서 코스피·코스닥 등락(`market`)과 투자자별 순매수(`market_investor_flows`, 단위=억원)를 묶어 오늘 장의 성격(주도 수급 주체, 위험선호/회피)을 요약하세요. 글로벌 지수·환율·금리·유가는 국내 장 해석에 필요한 만큼만 한 줄 맥락으로 녹이고, 수치를 그대로 나열하는 별도 섹션은 만들지 마세요.
2. 업종 흐름: `sectors`(업종별 등락)에서 두드러진 강세/약세 업종을 짚으세요. 가능하면 `market_news`·`news` 헤드라인과 "가능성" 수준으로만 연결하고, 근거 없는 단정은 금지합니다.
3. 이슈 종목: `movers` 중 상한가/하한가/급등/급락 종목을 골라 해석하세요. 급등락의 원인은 같은 종목의 `disclosures`/`news`(제목과 `snippet` 요약)와 연결하되, 연결할 근거가 없으면 "원인 미확인"으로 두세요. `investor_flows`(이슈 종목별 외국인·기관 순매매, 음수=순매도, 단위=주)는 보조 근거로만 쓰세요.
4. 데이터 누락은 생략: evidence에 없거나 비어 있는 수치를 0·"변동 없음"으로 단정하지 마세요. 값이 비어 있으면 그 지표 자체를 언급하지 마세요.
5. 마지막에 "체크포인트"(2~4개): evidence의 구체적 근거에 연결된 관찰 포인트만 쓰세요(예: 특정 공시 원문 확인, 특정 업종 추세 지속 여부, 환율 레벨). "시장 변동성 주의" 같은 일반론은 금지합니다.
6. 형식: 마크다운, 핵심부터, 인사말·군더더기 없이. 링크가 있는 공시/뉴스는 짧은 출처 표기만 하고 긴 URL은 본문에 노출하지 마세요.

evidence bundle:
{evidence}
"""


async def _call_openrouter(payload: dict[str, Any], google_sub: str | None) -> dict[str, Any]:
    model = await ai_config.get_model_for_feature("market_daily")
    try:
        await ai_client.require_openrouter_key()
    except ai_client.MissingOpenRouterKeyError:
        markdown = _fallback_markdown(payload, "AI API 키가 없어 수집된 근거만 표시합니다.")
        return {
            "markdown": markdown,
            "model": model,
            "tokens_in": 0,
            "tokens_out": 0,
            "cost_usd": 0.0,
            "llm_ok": False,
            "error": "missing_openrouter_key",
        }

    prompt = _build_prompt(payload)
    request_payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "당신은 한국 주식 시황을 근거 중심으로 요약하는 투자 리서치 어시스턴트입니다.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": MARKET_DAILY_MAX_TOKENS,
        **ai_config.openrouter_reasoning_controls(model),
    }
    # 타임아웃·빈 응답 같은 일시 오류로 곧장 실패 본문이 굳지 않게 짧게 재시도한다.
    attempts = max(1, MARKET_DAILY_LLM_ATTEMPTS)
    last_error = "unknown"
    for attempt in range(1, attempts + 1):
        try:
            result = await ai_client.post_chat_completion(
                feature="market_daily",
                payload=request_payload,
                google_sub=google_sub,
                model=model,
                model_profile="market_daily",
                timeout=httpx.Timeout(90.0, read=90.0),
                cost_estimator=estimate_gemini35_flash_cost,
                ok_if_content=True,
            )
        except Exception as exc:
            last_error = str(exc)
            logger.exception("daily market: LLM call failed (attempt %d/%d)", attempt, attempts)
            continue
        markdown = result["content"]
        if markdown:
            return {
                "markdown": markdown,
                "model": result["model"],
                "tokens_in": result["input_tokens"],
                "tokens_out": result["output_tokens"],
                "cost_usd": result["cost_usd"],
                "llm_ok": True,
                "error": None,
            }
        last_error = "empty_content"
        logger.warning("daily market: empty LLM content (attempt %d/%d)", attempt, attempts)
    return {
        "markdown": _fallback_markdown(payload, f"AI 생성에 실패해 수집된 근거만 표시합니다. ({last_error})"),
        "model": model,
        "tokens_in": 0,
        "tokens_out": 0,
        "cost_usd": 0.0,
        "llm_ok": False,
        "error": last_error,
    }


async def build_daily_market_brief(*, google_sub: str | None = None, brief_date: str | None = None) -> dict[str, Any]:
    """시장 전체 기준 금일 시황 — 사용자 포트폴리오·관심종목과 무관한 공용 브리프.

    지수·시장 수급(개인/외국인/기관)·업종 등락·시장 전체 급등락(상하한가 포함)과
    이슈 종목의 공시/뉴스/수급을 모아 LLM 으로 해석한다. 소스별 독립 실패 허용
    (한 소스가 죽어도 나머지 근거로 생성). google_sub 는 AI 비용 귀속용으로만 쓴다.
    """
    brief_date = brief_date or _today_iso()
    warnings: list[str] = []

    async def _safe(name: str, coro, default):
        # 소스별 타임아웃 + 독립 실패 허용: 한 업스트림이 느리거나 죽어도
        # 나머지 근거만으로 브리프를 만든다(전체 요청 hang 방지).
        try:
            return await asyncio.wait_for(coro, timeout=BRIEF_SOURCE_TIMEOUT_S)
        except Exception as exc:
            reason = "타임아웃" if isinstance(exc, asyncio.TimeoutError) else str(exc)
            logger.warning("daily market: %s 수집 실패: %s", name, reason)
            warnings.append(f"{name} 수집 실패")
            return default

    (market_rows, kospi_pct), movers, sectors, market_flows, market_news_rows = await asyncio.gather(
        _safe("market", _market_snapshot(), ([], None)),
        _safe("movers", _tape_movers(), []),
        _safe("sectors", market_movers.fetch_sectors(BRIEF_SECTOR_LIMIT), []),
        _safe("market_investor_flows", market_movers.fetch_investor_flows(), {}),
        _safe("market_news", market_news.fetch_market_news(BRIEF_MARKET_NEWS_LIMIT), []),
    )

    # 이슈 종목(상/하한가·급등락)만 공시·뉴스·종목수급 시드로 사용(마켓테이프와 동일 기준).
    focus = [m for m in movers if m.get("bucket") in ("상한가", "하한가", "급등", "급락")]
    focus.sort(key=lambda m: abs(m.get("change_pct") or 0), reverse=True)
    focus = focus[:NEWS_STOCK_LIMIT]
    focus_interests = [{"stock_code": m["stock_code"], "stock_name": m["stock_name"]} for m in focus]
    focus_codes = [m["stock_code"] for m in focus]
    names = {m["stock_code"]: m["stock_name"] for m in movers}

    if focus_interests:
        (disclosures, disclosure_warnings), news, investor_flows = await asyncio.gather(
            _safe("disclosures", _fetch_dart_disclosures(focus_interests, brief_date), ([], [])),
            _safe("news", _news_for_focus_codes(focus_codes, names), []),
            _safe("investor_flows", _investor_flows_for_codes(focus_codes, names), []),
        )
    else:
        disclosures, disclosure_warnings, news, investor_flows = [], [], [], []

    payload: dict[str, Any] = {
        "brief_date": brief_date,
        "generated_at": datetime.now().isoformat(),
        "scope": "market",  # 시장 전체 기준(포트폴리오 무관) — 표기·하위호환 판별용
        "kospi_change_pct": kospi_pct,
        "market": market_rows,
        "market_investor_flows": market_flows,
        "sectors": sectors,
        "movers": movers,
        "market_news": market_news_rows,
        "disclosures": disclosures,
        "news": news,
        "investor_flows": investor_flows,
        "source_warnings": [*warnings, *disclosure_warnings],
    }
    source_hash = _source_hash(payload)
    llm_result = await _call_openrouter(payload, google_sub)
    return {
        "brief_date": brief_date,
        "source_hash": source_hash,
        "payload": payload,
        **llm_result,
    }
