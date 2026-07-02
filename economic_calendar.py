"""제로인(zeroin) 경제캘린더 스크래퍼 — 투자정보 대시보드용.

한경(datacenter.hankyung.com) 경제캘린더가 임베드하는 제로인 피드의 백엔드
JSON 엔드포인트(``json_getData.php``)를 직접 호출해, 국가·중요도·기간으로 필터된
경제지표 일정을 정규화한다. 공개(무인증). ``market_news`` 와 동일하게 TTL 캐시 +
세마포어 + 스테일 폴백으로, 느린 업스트림이 워커를 잡지 않게 한다.

자립 모듈이다(외부 프로젝트 의존 없음). 필요하면 그대로 떼어내 독립 서비스로
발행할 수 있고, 알림(중요 지표 사전 알림·발표 실제치 서프라이즈)도
``fetch_economic_calendar`` 의 정규화 결과를 그대로 소비하면 된다.

업스트림 파라미터 메모:
- ``str_natcd``  : 국가코드 ``|`` 결합 (예: ``kr|us|eu``).
- ``str_nation`` : 서버가 "영문|한글" 쌍으로 짝지어 필터하므로 zeroin 체크박스의
  정확한 값과 일치해야 한다. 영문만 보내면 다음 항목과 잘못 짝지어져 오매칭된다.
- ``str_importance`` : 중요도 코드 ``|`` 결합 (3=상, 2=중, 1=하).
- ``start_date`` / ``end_date`` : ``YYYY-MM-DD``.
"""

from __future__ import annotations

import asyncio
import json as _json
import logging

import httpx

from cache_layer import MemoryTTLCache
from core.http import get_http_client

logger = logging.getLogger(__name__)

_BASE = "https://asp.zeroin.co.kr"
_DATA_URL = f"{_BASE}/eco/includes/wei/module/json_getData.php"
_REFERER = f"{_BASE}/eco/hkd/wei/0601.php"

_TTL = 300  # 초 — 일정은 안정적, 발표 실제치는 5분 내 갱신이면 충분
_cache = MemoryTTLCache("market.econ_calendar", _TTL)
_SEM = asyncio.Semaphore(3)
_HTTP_TIMEOUT = httpx.Timeout(8.0, connect=4.0)

# 국가코드 → 메타. en/ko 는 zeroin ``str_nation`` 필터용으로 체크박스 원문과
# 정확히 일치해야 한다(필터 정확도). name/flag 는 UI 표시용.
COUNTRY_META: dict[str, dict] = {
    "kr": {"en": "South Korea",    "ko": "대한민국",       "name": "한국",     "flag": "🇰🇷"},
    "us": {"en": "United States",  "ko": "미국",           "name": "미국",     "flag": "🇺🇸"},
    "cn": {"en": "China",          "ko": "중국",           "name": "중국",     "flag": "🇨🇳"},
    "jp": {"en": "Japan",          "ko": "일본",           "name": "일본",     "flag": "🇯🇵"},
    "eu": {"en": "Euro Area",      "ko": "유럽연합",       "name": "유럽",     "flag": "🇪🇺"},
    "gb": {"en": "United Kingdom", "ko": "영국",           "name": "영국",     "flag": "🇬🇧"},
    "de": {"en": "Germany",        "ko": "독일",           "name": "독일",     "flag": "🇩🇪"},
    "fr": {"en": "France",         "ko": "프랑스",         "name": "프랑스",   "flag": "🇫🇷"},
    "hk": {"en": "Hong Kong",      "ko": "홍콩",           "name": "홍콩",     "flag": "🇭🇰"},
    "in": {"en": "India",          "ko": "인도",           "name": "인도",     "flag": "🇮🇳"},
    "id": {"en": "Indonesia",      "ko": "인도네시아",     "name": "인니",     "flag": "🇮🇩"},
    "ph": {"en": "Philippines",    "ko": "필리핀",         "name": "필리핀",   "flag": "🇵🇭"},
    "sa": {"en": "Saudi Arabia",   "ko": "사우디아라비아", "name": "사우디",   "flag": "🇸🇦"},
    "sg": {"en": "Singapore",      "ko": "싱가포르",       "name": "싱가포르", "flag": "🇸🇬"},
    "tw": {"en": "Taiwan",         "ko": "대만",           "name": "대만",     "flag": "🇹🇼"},
    "vn": {"en": "Vietnam",        "ko": "베트남",         "name": "베트남",   "flag": "🇻🇳"},
    "gr": {"en": "Greece",         "ko": "그리스",         "name": "그리스",   "flag": "🇬🇷"},
    "it": {"en": "Italy",          "ko": "이탈리아",       "name": "이탈리아", "flag": "🇮🇹"},
    "ru": {"en": "Russia",         "ko": "러시아",         "name": "러시아",   "flag": "🇷🇺"},
    "ch": {"en": "Switzerland",    "ko": "스위스",         "name": "스위스",   "flag": "🇨🇭"},
    "tr": {"en": "Turkey",         "ko": "터키",           "name": "튀르키예", "flag": "🇹🇷"},
    "br": {"en": "Brazil",         "ko": "브라질",         "name": "브라질",   "flag": "🇧🇷"},
    "ca": {"en": "Canada",         "ko": "캐나다",         "name": "캐나다",   "flag": "🇨🇦"},
    "mx": {"en": "Mexico",         "ko": "멕시코",         "name": "멕시코",   "flag": "🇲🇽"},
    "au": {"en": "Australia",      "ko": "오스트레일리아", "name": "호주",     "flag": "🇦🇺"},
    "nz": {"en": "New Zealand",    "ko": "뉴질랜드",       "name": "뉴질랜드", "flag": "🇳🇿"},
    "eg": {"en": "Egypt",          "ko": "이집트",         "name": "이집트",   "flag": "🇪🇬"},
    "za": {"en": "South Africa",   "ko": "남아공",         "name": "남아공",   "flag": "🇿🇦"},
}

# 중요도: 업스트림 importance_class ↔ 레벨 ↔ 필터 코드 ↔ 표시 라벨.
_LEVEL_BY_CLASS = {"imptt_high": "high", "imptt_md": "mid", "imptt_low": "low"}
_CODE_BY_LEVEL = {"high": "3", "mid": "2", "low": "1"}
_LABEL_BY_LEVEL = {"high": "상", "mid": "중", "low": "하"}

DEFAULT_COUNTRIES = ["kr", "us", "cn", "eu", "jp"]  # 국내 가치투자자 기준 주요국
DEFAULT_IMPORTANCE = ["high", "mid", "low"]


def build_nation_params(codes: list[str]) -> tuple[str, str]:
    """국가코드 목록 → (str_nation, str_natcd). 미지원 코드는 버린다.

    str_nation 은 서버가 짝(영문, 한글)으로 읽으므로 순서·쌍을 정확히 맞춘다.
    """
    valid = [c for c in codes if c in COUNTRY_META]
    nation = "|".join(f"{COUNTRY_META[c]['en']}|{COUNTRY_META[c]['ko']}" for c in valid)
    natcd = "|".join(valid)
    return nation, natcd


def _col(data: dict, key: str) -> list:
    v = data.get(key)
    return v if isinstance(v, list) else []


def _parse_calendar(data: dict) -> list[dict]:
    """업스트림 병렬 배열 dict → 정규화된 이벤트 레코드 리스트(시간 오름차순).

    network 의존이 없어 fixture 로 단위 테스트할 수 있다.
    """
    if not isinstance(data, dict):
        return []
    natcd = _col(data, "natcd")
    n = len(natcd)
    date_temp = _col(data, "date_temp")
    times = _col(data, "time")
    actual = _col(data, "actual")
    previous = _col(data, "previous")
    forecast = _col(data, "forecast")
    nat_hname = _col(data, "nat_hname")
    kevent = _col(data, "kevent")
    imp_class = _col(data, "importance_class")
    index_ids = _col(data, "index")

    def at(arr: list, i: int) -> str:
        return str(arr[i]).strip() if i < len(arr) and arr[i] is not None else ""

    out: list[dict] = []
    for i in range(n):
        code = at(natcd, i).lower()
        # "2026-06-06 09:00:00.000" → "2026-06-06 09:00:00" / "2026-06-06"
        dt = at(date_temp, i).split(".")[0].strip()
        date_iso = dt.split(" ")[0] if dt else ""
        level = _LEVEL_BY_CLASS.get(at(imp_class, i), "")
        meta = COUNTRY_META.get(code)
        out.append({
            "datetime": dt,
            "date": date_iso,
            "time": at(times, i),
            "country": code,
            "country_name": meta["name"] if meta else (at(nat_hname, i) or code),
            "flag": meta["flag"] if meta else "",
            "event": at(kevent, i),
            "importance": level,
            "importance_label": _LABEL_BY_LEVEL.get(level, ""),
            "actual": at(actual, i),
            "forecast": at(forecast, i),
            "previous": at(previous, i),
            "index_id": at(index_ids, i),
        })
    # 업스트림은 최신순(내림차순)이라 달력 표시에 맞게 시간 오름차순으로 정렬.
    out.sort(key=lambda r: r["datetime"])
    return out


async def fetch_economic_calendar(
    *,
    start_date: str,
    end_date: str,
    countries: list[str] | None = None,
    importance: list[str] | None = None,
) -> dict:
    """경제캘린더 조회(캐시). 실패 시 스테일 캐시 폴백."""
    codes = [c for c in (countries or []) if c in COUNTRY_META] or list(DEFAULT_COUNTRIES)
    levels = [l for l in (importance or []) if l in _CODE_BY_LEVEL] or list(DEFAULT_IMPORTANCE)
    nation, natcd = build_nation_params(codes)
    str_importance = "|".join(_CODE_BY_LEVEL[l] for l in levels)

    key = (start_date, end_date, natcd, str_importance)
    cached = _cache.get(key)
    if cached is not None:
        return cached

    def _result(events: list[dict]) -> dict:
        return {
            "events": events,
            "start": start_date,
            "end": end_date,
            "countries": codes,
            "importance": levels,
        }

    try:
        async with _SEM:
            client = await get_http_client("economic_calendar")
            resp = await client.get(
                _DATA_URL,
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                    "str_nation": nation,
                    "str_natcd": natcd,
                    "str_importance": str_importance,
                },
                headers={"User-Agent": "Mozilla/5.0", "Referer": _REFERER},
                timeout=_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
        raw = resp.content
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("cp949", errors="replace")
        data = _json.loads(text)
        if not isinstance(data, dict):
            # 업스트림 SQL 오류 시 null 을 반환한다 — 빈 결과로 캐시하지 않는다.
            raise ValueError("unexpected calendar payload")
        result = _result(_parse_calendar(data))
        _cache.set(key, result)
        return result
    except Exception as exc:
        logger.warning("economic calendar fetch failed: %s", exc)
        entry = _cache.get_entry(key, allow_stale=True) if hasattr(_cache, "get_entry") else None
        if entry and getattr(entry, "value", None):
            return dict(entry.value)
        return _result([])


async def fetch_calendar_by_level(
    *,
    start_date: str,
    end_date: str,
    selection: dict[str, object],
) -> dict:
    """중요도별 국가 선택으로 캘린더를 가져온다.

    ``selection`` 은 ``{level: 'all' | [codes]}`` (level ∈ high/mid/low). 같은
    국가셋을 공유하는 레벨끼리 묶어 zeroin 을 호출하므로(상=전체국가, 중·하=한국만
    같은 기본값에서 과다 페치를 피한다), 각 그룹 결과의 합집합이 곧 원하는 집합이
    되어 별도 후필터가 필요 없다. 빈/미지정 레벨은 비활성(해당 중요도 숨김)."""
    all_codes = list(COUNTRY_META.keys())
    norm: dict[str, tuple] = {}
    for level in ("high", "mid", "low"):
        sel = selection.get(level)
        if sel == "all":
            codes = all_codes
        elif sel:
            codes = [c for c in sel if c in COUNTRY_META]
        else:
            codes = []
        if codes and level in _CODE_BY_LEVEL:
            norm[level] = tuple(sorted(set(codes)))

    if not norm:
        return {"events": [], "start": start_date, "end": end_date}

    # 같은 국가셋을 쓰는 레벨끼리 한 번의 요청으로 묶는다.
    groups: dict[tuple, list[str]] = {}
    for level, codes in norm.items():
        groups.setdefault(codes, []).append(level)

    results = await asyncio.gather(*[
        fetch_economic_calendar(
            start_date=start_date, end_date=end_date,
            countries=list(codes), importance=levels,
        )
        for codes, levels in groups.items()
    ], return_exceptions=True)

    events: list[dict] = []
    seen: set = set()
    for r in results:
        if isinstance(r, Exception) or not isinstance(r, dict):
            continue
        for e in r.get("events", []):
            key = e.get("index_id") or (e.get("datetime"), e.get("country"), e.get("event"))
            if key in seen:
                continue
            seen.add(key)
            events.append(e)
    events.sort(key=lambda e: e.get("datetime") or "")
    return {"events": events, "start": start_date, "end": end_date}
