"""summary.json ``data`` → 허브가 이미 소비하는 레거시 모양.

설계: summary 를 **레거시 원본 파일의 모양**(current.json/config.json/data.json …)으로
되돌려 ``external_tools`` 의 기존 요약기(``_summarize_*``)와 매칭(``_match_*``)을 그대로
태운다. 그래서 ``/api/external/insights``·종목 딥링크·액션보드 신호·SPAC 인사이트의 응답
모양이 소스와 무관하게 바이트 단위로 같은 키 구조를 갖는다(프론트 변경 없음).

buybacks 만 예외다: summary 는 원본 스냅샷이 아니라 이미 고른 결과(top/ratios)라
허브 모양(카드 + 종목별 매칭 인덱스)으로 바로 만든다.

모든 함수는 순수 함수이고, 모양이 어긋나면 KeyError/TypeError/ValueError 를 올린다 —
:func:`services.ecosystem.siblings.summary_or_legacy` 가 잡아 그 도구만 레거시로 보낸다.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable


def _list(value: Any, where: str) -> list:
    if not isinstance(value, list):
        raise TypeError(f"{where} must be a list")
    return value


def _dict(value: Any, where: str) -> dict:
    if not isinstance(value, dict):
        raise TypeError(f"{where} must be an object")
    return value


# --- holding_value ---------------------------------------------------------

def holding_pair(data: dict) -> tuple[dict, list]:
    """→ (current.json, config.json) 모양. §6.1: pairs 는 config 순서의 전체 목록."""
    pairs = [_dict(p, "pairs[]") for p in _list(data["pairs"], "pairs")]
    current = {
        "lastUpdated": data.get("lastUpdated"),
        "summary": {"averageRatio": data.get("averageRatio"), "pairCount": data.get("pairCount")},
        "pairs": [
            {"id": p["id"], "ratio": p.get("ratio"), "ratioChange": p.get("ratioChange"),
             "holdingValue": p.get("holdingValue"), "marketCap": p.get("marketCap")}
            for p in pairs
        ],
    }
    config = [
        {"id": p["id"], "name": p.get("name"), "holdingName": p.get("holdingName"), "holdingTicker": p["code"]}
        for p in pairs
    ]
    return current, config


# --- common_preferred_spread ----------------------------------------------

def spread_pair(data: dict) -> tuple[dict, list]:
    """→ (current.json, config.json) 모양. §6.2."""
    pairs = [_dict(p, "pairs[]") for p in _list(data["pairs"], "pairs")]
    current = {
        "lastUpdated": data.get("lastUpdated"),
        "averageSpread": data.get("averageSpread"),
        "averageSpreadChange": data.get("averageSpreadChange"),
        "prices": {
            p["id"]: {"spread": p.get("spread"), "spreadChange": p.get("spreadChange"),
                      "commonPrice": p.get("commonPrice"), "preferredPrice": p.get("preferredPrice"),
                      "date": p.get("date")}
            for p in pairs
        },
    }
    config = [
        {"id": p["id"], "name": p.get("name"), "commonTicker": p["commonCode"],
         "preferredTicker": p["preferredCode"], "preferredName": p.get("preferredName")}
        for p in pairs
    ]
    return current, config


# --- spac-hunter -----------------------------------------------------------

def spac_current(data: dict) -> dict:
    """→ current.json 모양(카드 '스팩 저가순' 입력)."""
    prices = {}
    for s in _list(data["spacs"], "spacs"):
        s = _dict(s, "spacs[]")
        prices[s["code"]] = {
            "name": s.get("name"), "currentPrice": s.get("currentPrice"), "ipoPrice": s.get("ipoPrice"),
            "annualizedReturn": s.get("annualizedReturn"), "ratio": s.get("ratio"),
        }
    return {"lastUpdated": data.get("lastUpdated"), "summary": dict(_dict(data["summary"], "summary")),
            "prices": prices}


def spac_valuation(data: dict) -> dict:
    """→ ``external_tools.fetch_spac_data()`` 모양(§6.3 허브 유도)."""
    return {
        "spacs": [dict(_dict(s, "spacs[]")) for s in _list(data["spacs"], "spacs")],
        "valuationAssumptions": dict(_dict(data.get("valuationAssumptions") or {}, "valuationAssumptions")),
        "lastUpdated": data.get("lastUpdated"),
    }


# --- nps-tracker -----------------------------------------------------------

def nps_current(data: dict) -> dict:
    """→ current.json 모양(§6.7). top 은 이미 weight 내림차순 상위 N."""
    summary = dict(_dict(data["summary"], "summary"))
    return {
        "lastUpdated": data.get("lastUpdated"),
        "asOf": summary.get("asOf"),
        "summary": summary,
        "allocation": data.get("allocation"),
        "holdings": [
            {"stock_code": h.get("code"), "stock_name": h.get("name"), "weight": h.get("weight"),
             "market_value": h.get("marketValue"), "change_pct": h.get("changePct")}
            for h in (_dict(h, "top[]") for h in _list(data["top"], "top"))
        ],
    }


# --- eiayn -----------------------------------------------------------------

def eiayn_universe(data: dict) -> set[str]:
    return {str(c).strip().upper() for c in _list(data["universe"], "universe") if str(c).strip()}


def eiayn_rankings(envelope: dict, link_for: Callable[[str], str]) -> dict:
    """→ rankings.json 모양(§6.5). 순서가 같으므로 같은 날짜 시드면 같은 5개가 뽑힌다."""
    data = envelope["data"]
    etfs = []
    for r in _list(data["rankings"], "rankings"):
        r = _dict(r, "rankings[]")
        code = str(r["code"])
        etfs.append({"rank": r.get("rank"), "shortName": r.get("name"), "ticker": code,
                     "aiynScore": r.get("score"), "market": r.get("market"), "link": link_for(code)})
    return {"etfs": etfs, "count": len(etfs), "generatedAt": envelope.get("asOf")}


# --- gold_gap --------------------------------------------------------------

def gold_data(data: dict) -> dict:
    """→ data.json 모양(자산별 gap_pct/dates 의 마지막 값만)."""
    out: dict[str, Any] = {"updated_at": data.get("updatedAt")}
    for a in _list(data["assets"], "assets"):
        a = _dict(a, "assets[]")
        key = a["key"]
        if a.get("gap") is None:
            continue
        out[key] = {"gap_pct": [a["gap"]], "dates": [a.get("date")] if a.get("date") else []}
    return out


# --- bond-mate -------------------------------------------------------------

def _utc_text(as_of: Any) -> Any:
    """envelope asOf(KST) → 레거시 ``generated_at`` 표기(UTC, +00:00)."""
    if not isinstance(as_of, str) or "T" not in as_of:
        return as_of
    try:
        return datetime.fromisoformat(as_of).astimezone(timezone.utc).isoformat(timespec="seconds")
    except ValueError:
        return as_of


def bond_current(envelope: dict) -> dict:
    """→ data/current.json 모양(§6.8). camelCase → 허브 카드가 읽는 snake_case."""
    data = envelope["data"]
    highlights = _dict(data["highlights"], "highlights")
    offering = data.get("latestOffering")
    latest = None
    if isinstance(offering, dict):
        latest = {"issuer": offering.get("issuer"), "issuer_name": offering.get("issuerName"),
                  "filing_date": offering.get("filingDate"), "total_amount": offering.get("totalAmount"),
                  "tranches": offering.get("tranches")}

    def quotes(container: Any, where: str) -> dict:
        return {k: {"value": v.get("value"), "change": v.get("change"), "change_pct": v.get("changePct"),
                    "date": v.get("date")}
                for k, v in _dict(container, where).items() if isinstance(v, dict)}

    credit = {rating: {"oas": {"value": bp / 100}}
              for rating, bp in _dict(data["creditSpreadBp"], "creditSpreadBp").items()
              if isinstance(bp, (int, float)) and not isinstance(bp, bool)}
    return {
        "generated_at": _utc_text(envelope.get("asOf")),
        "rates": quotes(data["rates"], "rates"),
        "fx": quotes(data["fx"], "fx"),
        "credit": credit,
        "highlights": {
            "us_curve_spread_bp": highlights.get("usCurveSpreadBp"),
            "us_curve_inverted": highlights.get("usCurveInverted"),
            "kr_curve_spread_bp": highlights.get("krCurveSpreadBp"),
            "ig_hy_spread_bp": highlights.get("igHySpreadBp"),
            "latest_offering": latest,
        },
    }


# --- buybacks --------------------------------------------------------------

def buybacks_index(data: dict, url: str, top_n: int = 5) -> dict:
    """→ {"card": 인사이트 카드, "byCode": 종목별 매칭}. §6.4.

    ``ratios`` 에는 이름이 없다 — byCode 의 name 은 top 에 있으면 그 이름, 없으면 None
    (신호 제목은 호출부가 허브 종목명으로 채운다).
    """
    as_of = data.get("asOf")
    top_rows = [_dict(r, "top[]") for r in _list(data["top"], "top")]
    top_by_code = {str(r["code"]).upper(): r for r in top_rows}
    ratio_as_of = _dict(data.get("ratioAsOf") or {}, "ratioAsOf")

    def row(code: str, pct: float, detail: dict | None) -> dict:
        detail = detail or {}
        return {
            "name": detail.get("name"),
            "asOf": detail.get("asOf") or ratio_as_of.get(code) or as_of,
            "stockKind": detail.get("stockKind"),
            "treasuryRatio": pct / 100,
            "treasuryRatioPct": pct,
            "endingQty": detail.get("endingQty"),
            "issuedShares": detail.get("issuedShares"),
            "url": url,
        }

    by_code: dict[str, dict] = {}
    for code, pct in _dict(data["ratios"], "ratios").items():
        if isinstance(pct, (int, float)) and not isinstance(pct, bool):
            code = str(code).upper()
            by_code[code] = row(code, float(pct), top_by_code.get(code))
    for code, detail in top_by_code.items():  # top 은 소수 4자리 — ratios(2자리)보다 정밀
        pct = detail.get("treasuryRatioPct")
        if isinstance(pct, (int, float)) and not isinstance(pct, bool):
            by_code[code] = row(code, float(pct), detail)

    card_top = []
    for r in top_rows[:top_n]:
        pct = r.get("treasuryRatioPct")
        if not isinstance(pct, (int, float)) or isinstance(pct, bool):
            continue
        card_top.append({
            "name": r.get("name") or r.get("code"), "code": r.get("code"), "asOf": r.get("asOf"),
            "stockKind": r.get("stockKind"), "treasuryRatio": pct / 100, "treasuryRatioPct": pct,
            "endingQty": r.get("endingQty"), "issuedShares": r.get("issuedShares"),
        })
    count = data.get("count")
    return {
        "card": {"asOf": as_of, "count": count if isinstance(count, int) else len(by_code),
                 "top": card_top, "url": url},
        "byCode": by_code,
    }
