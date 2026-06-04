"""외부 분석 도구(GitHub Pages) 요약 소비 모듈.

ducklove 계정의 독립 정적 대시보드 세 개가 GitHub Actions 배치로 생성·커밋하는
public JSON을 읽어, 투자정보 대시보드의 허브 위젯과 종목분석 deep-link 카드에
쓸 요약을 만든다.

- holding_value         : 지주사 NAV 디스카운트(보유지분가치 vs 조정시총 비율)
- common_preferred_spread: 우선주-보통주 괴리율
- gold_gap              : 김치프리미엄(금/BTC/USDT 국내외 가격차)
- spac-hunter           : 국내 스팩 현재가/공모가·연환산 기대수익률

데이터는 raw.githubusercontent 에서 받아 길게 캐시한다(배치가 분 단위로만 갱신).
각 도구 fetch는 서로 독립적으로 실패를 허용해, 하나가 죽어도 나머지는 표시된다.
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from cache_layer import MemoryTTLCache

logger = logging.getLogger(__name__)

_RAW = "https://raw.githubusercontent.com/ducklove"
# 사용자에게 보여줄(새 탭) 도구 홈 — GitHub Pages
SITE = {
    "holding": "https://ducklove.github.io/holding_value/",
    "spread": "https://ducklove.github.io/common_preferred_spread/",
    "goldGap": "https://ducklove.github.io/gold_gap/",
    "spac": "https://ducklove.github.io/spac-hunter/",
    "etf": "https://ducklove.github.io/eiayn/",
}

_TTL = 900  # 15분 — 배치 갱신 주기에 맞춤
_cache = MemoryTTLCache("external.tools", _TTL)
_raw_cache = MemoryTTLCache("external.raw", _TTL)  # (current, config) 원본 — 요약·deep-link 공용
_SEM = asyncio.Semaphore(3)
_TIMEOUT = httpx.Timeout(8.0, connect=4.0)

# eiayn(ETF) 프로젝트의 커버 종목 universe. 우선주·지주사처럼 "외부 프로젝트가
# 발행한 목록과 코드 매칭" 패턴 — 국내(6자리/6자 KRX)·해외(VOO 등 티커) 모두 포함.
# 파일이 크고(수 MB) 느리게 바뀌므로 길게 캐시한다.
_ETF_DATA_URL = "https://ducklove.github.io/eiayn/data/etfs.json"
_ETF_UNIVERSE_TTL = 6 * 3600  # 6시간
_etf_universe_cache = MemoryTTLCache("external.etf_universe", _ETF_UNIVERSE_TTL)
_ETF_TIMEOUT = httpx.Timeout(20.0, connect=5.0)


async def fetch_etf_universe() -> set[str]:
    """eiayn 이 커버하는 ETF 코드 집합(대문자 정규화). 실패 시 스테일/빈 집합."""
    cached = _etf_universe_cache.get("universe")
    if cached is not None:
        return cached
    try:
        async with _SEM:
            async with httpx.AsyncClient(timeout=_ETF_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(_ETF_DATA_URL, headers={"User-Agent": "value-invest/1.0"})
                resp.raise_for_status()
        data = resp.json()
        universe = {
            str(c).strip().upper()
            for c in (data.get("universe") or [])
            if str(c).strip()
        }
        if universe:
            _etf_universe_cache.set("universe", universe)
        return universe
    except Exception as exc:
        logger.warning("ETF universe fetch failed: %s", exc)
        entry = _etf_universe_cache.get_entry("universe", allow_stale=True) if hasattr(_etf_universe_cache, "get_entry") else None
        return entry.value if entry and getattr(entry, "value", None) else set()


def etf_deep_link(code: str) -> str:
    """eiayn 종목 딥링크. 쿼리 파라미터 방식(예: .../eiayn/?code=VOO)."""
    return f"{SITE['etf']}?code={code}"


async def etf_link_for(code: str) -> dict | None:
    """code 가 ETF(eiayn universe)면 딥링크 정보를, 아니면 None.

    해외 티커에 거래소 접미사가 붙어 와도(예: VOO.US) 앞부분으로 한 번 더 맞춰본다.
    """
    norm = (code or "").strip().upper()
    if not norm:
        return None
    universe = await fetch_etf_universe()
    if not universe:
        return None
    base = norm.split(".", 1)[0]
    matched = norm if norm in universe else (base if base in universe else None)
    if not matched:
        return None
    return {"code": matched, "url": etf_deep_link(matched)}

_GOLD_LABELS = {"gold": "금", "bitcoin": "비트코인", "usdt": "USDT"}
# gold_gap deep-link용 기본 소스(gold만 소스 선택이 있음)
_GOLD_LINK = {
    "gold": "?asset=gold&gold_source=ny_futures",
    "bitcoin": "?asset=bitcoin",
    "usdt": "?asset=usdt",
}


async def _get_json(url: str):
    async with _SEM:
        async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "value-invest/1.0"})
            resp.raise_for_status()
    return resp.json()


def _code(ticker: str) -> str:
    """'005930.KS' -> '005930'."""
    return (ticker or "").split(".", 1)[0]


def _summarize_holding(current: dict, config: list, top_n: int = 5) -> dict:
    """지주사: ratio(보유가치/시총 %, 높을수록 저평가) 내림차순 TOP."""
    meta = {
        c.get("id"): {"name": c.get("name") or c.get("id"), "code": _code(c.get("holdingTicker", ""))}
        for c in (config or [])
    }
    rows = []
    for p in current.get("pairs", []) or []:
        ratio = p.get("ratio")
        if ratio is None:
            continue
        m = meta.get(p.get("id"), {})
        rows.append({
            "name": m.get("name", p.get("id")),
            "code": m.get("code", ""),
            "ratio": ratio,
            "ratioChange": p.get("ratioChange"),
        })
    rows.sort(key=lambda r: r["ratio"], reverse=True)
    summary = current.get("summary") or {}
    return {
        "averageRatio": summary.get("averageRatio"),
        "pairCount": summary.get("pairCount"),
        "top": rows[:top_n],
        "lastUpdated": current.get("lastUpdated"),
        "url": SITE["holding"],
    }


def _summarize_spread(current: dict, config: list, top_n: int = 5) -> dict:
    """우선주: spread(괴리율 %, 높을수록 우선주 할인 큼) 내림차순 TOP.

    같은 보통주에 우선주가 여럿이면(예: 두산퓨얼셀1우/2우B) 괴리율이 가장 큰
    우선주 하나만 노출한다.
    """
    meta = {
        c.get("id"): {"name": c.get("name") or c.get("commonName") or c.get("id"), "code": _code(c.get("commonTicker", ""))}
        for c in (config or [])
    }
    rows = []
    for cid, v in (current.get("prices") or {}).items():
        m = meta.get(cid)
        if not m:  # config에 정의된 보통주 쌍만(우선주 단독 항목 제외)
            continue
        spread = v.get("spread")
        if spread is None:
            continue
        rows.append({
            "name": m["name"],
            "code": m["code"],
            "spread": spread,
            "spreadChange": v.get("spreadChange"),
        })
    # 같은 보통주의 다중우선주는 괴리율(spread)이 가장 큰 하나만 남긴다.
    best_by_code: dict = {}
    for r in rows:
        cur = best_by_code.get(r["code"])
        if cur is None or r["spread"] > cur["spread"]:
            best_by_code[r["code"]] = r
    rows = sorted(best_by_code.values(), key=lambda r: r["spread"], reverse=True)
    return {
        "averageSpread": current.get("averageSpread"),
        "averageSpreadChange": current.get("averageSpreadChange"),
        "top": rows[:top_n],
        "lastUpdated": current.get("lastUpdated"),
        "url": SITE["spread"],
    }


def _summarize_gold(data: dict) -> dict:
    """김치프리미엄: 자산별 최신 gap_pct(양수=김프, 음수=역프)."""
    assets = []
    for key in ("gold", "bitcoin", "usdt"):
        a = data.get(key) or {}
        gp = a.get("gap_pct") or []
        dates = a.get("dates") or []
        if not gp:
            continue
        assets.append({
            "key": key,
            "label": _GOLD_LABELS[key],
            "gap": gp[-1],
            "date": dates[-1] if dates else None,
            "link": SITE["goldGap"] + _GOLD_LINK.get(key, ""),
        })
    return {"assets": assets, "updatedAt": data.get("updated_at"), "url": SITE["goldGap"]}


def _summarize_spac(current: dict, top_n: int = 5) -> dict:
    """스팩: 현재가가 낮은(공모가 대비 할인 큰) 순 TOP."""
    rows = []
    for code, v in (current.get("prices") or {}).items():
        if not isinstance(v, dict):
            continue
        price = v.get("currentPrice")
        if price is None:
            continue
        rows.append({
            "name": v.get("name") or code,
            "code": code,
            "currentPrice": price,
            "ipoPrice": v.get("ipoPrice"),
            "annualizedReturn": v.get("annualizedReturn"),
            "ratio": v.get("ratio"),
        })
    rows.sort(key=lambda r: r["currentPrice"])
    summary = current.get("summary") or {}
    return {
        "averageAnnualizedReturn": summary.get("averageAnnualizedReturn"),
        "belowIpoCount": summary.get("belowIpoCount"),
        "totalCount": summary.get("totalCount"),
        "top": rows[:top_n],
        "lastUpdated": current.get("lastUpdated"),
        "url": SITE["spac"],
    }


async def _load_pair(repo: str) -> tuple[dict, list]:
    """(current, config) 원본을 받아 캐시. 요약과 deep-link가 함께 쓴다."""
    cached = _raw_cache.get(repo)
    if cached is not None:
        return cached
    cur, cfg = await asyncio.gather(
        _get_json(f"{_RAW}/{repo}/master/current.json"),
        _get_json(f"{_RAW}/{repo}/master/config.json"),
    )
    _raw_cache.set(repo, (cur, cfg))
    return cur, cfg


async def _holding_summary() -> dict | None:
    cur, cfg = await _load_pair("holding_value")
    return _summarize_holding(cur, cfg)


async def _spread_summary() -> dict | None:
    cur, cfg = await _load_pair("common_preferred_spread")
    return _summarize_spread(cur, cfg)


async def _gold_summary() -> dict | None:
    data = await _get_json(f"{_RAW}/gold_gap/master/data.json")
    return _summarize_gold(data)


async def _spac_summary() -> dict | None:
    # spac-hunter 는 기본 브랜치가 main 이고 current.json 만으로 요약 가능하다
    # (종목명이 prices 안에 들어 있어 별도 config 가 필요 없음).
    data = await _get_json(f"{_RAW}/spac-hunter/main/current.json")
    return _summarize_spac(data)


async def fetch_external_insights() -> dict:
    """네 도구 요약을 한 번에. 도구별 독립 실패 허용 + 길게 캐시."""
    cached = _cache.get("insights")
    if cached is not None:
        return cached

    results = await asyncio.gather(
        _holding_summary(), _spread_summary(), _gold_summary(), _spac_summary(),
        return_exceptions=True,
    )
    keys = ("holding", "spread", "goldGap", "spac")
    out: dict = {}
    for key, res in zip(keys, results):
        if isinstance(res, Exception) or res is None:
            logger.warning("external insight '%s' failed: %s", key, res)
            continue
        out[key] = res

    if out:
        _cache.set("insights", out)
        return out
    # 전부 실패 시 스테일 폴백
    entry = _cache.get_entry("insights", allow_stale=True) if hasattr(_cache, "get_entry") else None
    if entry and getattr(entry, "value", None):
        return dict(entry.value)
    return out


def _match_preferred(code: str, current: dict, config: list) -> dict | None:
    """보통주/우선주 코드 어느 쪽이든 매칭되면 그 쌍의 괴리율 정보를 돌려준다."""
    prices = current.get("prices") or {}
    for c in config or []:
        if code in (_code(c.get("commonTicker", "")), _code(c.get("preferredTicker", ""))):
            v = prices.get(c.get("id")) or {}
            if v.get("spread") is None:
                return None
            return {
                "name": c.get("name") or c.get("commonName") or c.get("id"),
                "preferredName": c.get("preferredName"),
                "spread": v.get("spread"),
                "spreadChange": v.get("spreadChange"),
                "commonPrice": v.get("commonPrice"),
                "preferredPrice": v.get("preferredPrice"),
                "url": SITE["spread"],
            }
    return None


def _match_holding(code: str, current: dict, config: list) -> dict | None:
    """종목이 지주사면 보유지분가치/시총 비율 정보를 돌려준다(?code= deep-link)."""
    pairs = {p.get("id"): p for p in current.get("pairs", []) or []}
    for c in config or []:
        if _code(c.get("holdingTicker", "")) == code:
            p = pairs.get(c.get("id")) or {}
            if p.get("ratio") is None:
                return None
            return {
                "name": c.get("name") or c.get("id"),
                "ratio": p.get("ratio"),
                "ratioChange": p.get("ratioChange"),
                "holdingValue": p.get("holdingValue"),
                "marketCap": p.get("marketCap"),
                "url": SITE["holding"] + f"?code={code}",
            }
    return None


async def fetch_stock_links(code: str) -> dict:
    """종목분석 deep-link 카드용 — 이 종목코드에 해당하는 우선주/지주사 정보.

    config 전체를 매핑하므로 TOP 요약과 달리 임의 종목을 커버한다. 두 도구는
    서로 독립적으로 조회·실패 허용.
    """
    code = (code or "").strip()
    if not code:
        return {}
    result: dict = {}
    try:
        cur, cfg = await _load_pair("common_preferred_spread")
        pref = _match_preferred(code, cur, cfg)
        if pref:
            result["preferred"] = pref
    except Exception as exc:
        logger.warning("stock-link preferred lookup failed (%s): %s", code, exc)
    try:
        cur, cfg = await _load_pair("holding_value")
        hold = _match_holding(code, cur, cfg)
        if hold:
            result["holding"] = hold
    except Exception as exc:
        logger.warning("stock-link holding lookup failed (%s): %s", code, exc)
    try:
        etf = await etf_link_for(code)
        if etf:
            result["etf"] = etf
    except Exception as exc:
        logger.warning("stock-link etf lookup failed (%s): %s", code, exc)
    return result
