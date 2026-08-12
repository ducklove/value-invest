"""ePaper 기기용 포트폴리오 요약.

기기(ESP32-S3 PhotoPainter)는 세로 480x800 e-paper 에 총액·손익·보유 종목만
그린다. 웹 SPA 는 ``/api/portfolio`` + ``/api/asset-quotes`` 를 받아 브라우저에서
합계를 계산하지만 기기에는 그럴 여력이 없으므로 서버에서 미리 접어서 내려준다.

**웹 화면과 같은 숫자가 나와야 한다.** 그래서 두 가지를 그대로 따른다:

* 시세는 캐시만 보고 없으면 포기하는 게 아니라, ``/api/asset-quotes`` 와 같은
  순서로 채운다 — 국내 벌크 조회 → 개별 조회 → stale 캐시 폴백. 캐시만 쓰던
  때는 워밍업이 안 된 종목이 통째로 빠져 총 평가액이 실제보다 작게 나왔다.
* 오늘/YTD 수익률은 매입가가 아니라 **NAV 기준**이다. 입출금이 있으면 평가액
  단순 비교는 성과가 아니라 현금 이동을 반영해버린다. 웹의
  ``_periodReturn(snap, 'nav')`` 과 같은 식을 쓴다.

평가액은 ``qty * quote.price`` 다 — 환율을 곱하지 않는다. 시세 계층이 이미
원화로 환산해서 내려주기 때문이다 (``foreign.fetch_foreign_quote`` 가
``fx.fx_to_krw`` 를 거친다).

종목 순서는 사용자가 서비스에서 정한 순서(``sort_order``)를 그대로 쓴다.
평가액 순으로 다시 정렬하지 않는다.

화면에는 종목 전체가 들어가지 않으므로 두 갈래로 접어서 내려준다:

* ``groups`` — 그룹별 합계. **전 종목** 기준이라 상위 몇 개만 잘라 보내도 화면의
  합이 실제 포트폴리오와 어긋나지 않는다.
* ``movers`` — 오늘 평가액이 가장 크게 움직인 종목. 평가액 상위 목록은 매일
  거의 그대로라 자주 갱신되는 화면에서 볼 이유가 없다. 변동액 절대값으로 뽑고
  부호 내림차순으로 정렬해 내려준다.

둘 다 ``top_n`` 으로 자르기 **전** 의 전체 행에서 계산한다.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from repositories import market_brief as market_brief_repo
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from services import stock_quotes
from services.portfolio import identifiers, quote_service, time_windows

logger = logging.getLogger(__name__)

#: 세로 480x800 화면에 행 높이를 줄이지 않고 들어가는 최대 행 수.
DEFAULT_TOP_N = 12

#: 화면 아래쪽 '움직인 종목' 칸에 들어가는 최대 행 수.
DEFAULT_MOVERS_N = 12

#: 시세가 존재하지 않는 참조용 행 (지수/환율). 보유가 아니므로 미확보로 세지 않는다.
NON_QUOTABLE_PREFIXES = ("IDX_", "FX_")

#: 그룹이 지정되지 않은 종목이 모이는 이름.
UNGROUPED = "기타"

#: 시황 브리프는 포트폴리오와 무관한 시장 기준이라 모든 사용자가 하나를 공유한다
#: (routes/market_daily.py 의 _BRIEF_CACHE_SUB 와 같은 값이어야 한다).
MARKET_BRIEF_SUB = "public"
#: 기기 화면의 코멘트 한 칸을 쓰는 재료다. 원문을 통째로 실어 보낼 이유가 없다.
MARKET_BRIEF_CHARS = 1800

_QUOTE_CONCURRENCY = 2
_QUOTE_ITEM_TIMEOUT = 20.0


def _safe_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct(numerator: float, denominator: float) -> float | None:
    if not denominator:
        return None
    return round(numerator / denominator * 100, 2)


def _day_pnl(value: float, day_pct: float | None) -> float | None:
    """등락률에서 오늘 변동액을 되돌린다.

    시세 계층은 종목별 전일 종가를 따로 주지 않고 등락률만 준다. 지금 평가액이
    전일 대비 ``day_pct`` % 오른 값이므로 전일 평가액은 ``value / (1 + r)`` 이고
    변동액은 그 차이다. ``day_pct`` 가 -100% 면 전일 평가액이 0 이라 되돌릴 수
    없으므로 비워 둔다.
    """
    if day_pct is None:
        return None
    denominator = 100.0 + day_pct
    if denominator == 0:
        return None
    return value * day_pct / denominator


def _change_pct(value: float, day_pnl: float | None) -> float | None:
    """합계 평가액과 합계 변동액에서 그룹 등락률을 되돌린다."""
    if day_pnl is None:
        return None
    return _pct(day_pnl, value - day_pnl)


def _group_rows(rows: list[dict], total_value: float) -> list[dict]:
    """그룹별 합계. 순서는 종목 순서에서 그룹이 처음 나온 순서를 따른다.

    사용자가 서비스에서 정한 정렬을 그대로 물려받는 것이라 화면과 웹의 그룹
    순서가 같아진다.
    """
    buckets: dict[str, dict] = {}
    for row in rows:
        name = row.get("group") or UNGROUPED
        bucket = buckets.setdefault(
            name, {"name": name, "count": 0, "value": 0.0, "day_pnl": None}
        )
        bucket["count"] += 1
        bucket["value"] += row["value"]
        day_pnl = row.get("day_pnl")
        if day_pnl is not None:
            bucket["day_pnl"] = (bucket["day_pnl"] or 0.0) + day_pnl

    groups = []
    for bucket in buckets.values():
        day_pnl = bucket["day_pnl"]
        groups.append({
            "name": bucket["name"],
            "count": bucket["count"],
            "value": round(bucket["value"]),
            "weight_pct": _pct(bucket["value"], total_value),
            "day_pnl": round(day_pnl) if day_pnl is not None else None,
            "day_pct": _change_pct(bucket["value"], day_pnl),
        })
    return groups


def _merge_pairs(rows: list[dict]) -> list[dict]:
    """롱숏 페어를 한 줄로 합친다.

    숏 행은 ``pair_long_code`` 로 같은 포트폴리오의 롱 행을 가리킨다(웹의
    ``pfPairStats`` 와 같은 관계). 두 다리를 따로 세우면 헤지의 한쪽만 보고
    '오늘 크게 빠진 종목' 으로 읽히므로, 합쳐서 순손익 한 줄로 만든다.

    **합친 줄에는 등락률을 주지 않는다.** 순평가액이 0 근처면 비율이 발산하고,
    그럴듯한 분모를 여기서 새로 만들면 웹 화면의 숫자와 어긋난다. 금액은 정의가
    분명하므로 그것만 남기고 ``pair`` 로 표시해 화면이 달리 그리게 한다.
    """
    merged: dict[str, dict] = {}
    order: list[str] = []
    for row in rows:
        key = row.get("pair_long_code") or row["code"]
        bucket = merged.get(key)
        if bucket is None:
            bucket = {**row, "_legs": 1, "_is_long": row["code"] == key}
            merged[key] = bucket
            order.append(key)
            continue
        bucket["_legs"] += 1
        bucket["value"] += row["value"]
        if row.get("day_pnl") is not None:
            bucket["day_pnl"] = (bucket.get("day_pnl") or 0) + row["day_pnl"]
        # 이름은 롱 다리를 따른다 — 세트를 부르는 이름은 롱 쪽이다.
        if row["code"] == key:
            bucket["code"], bucket["name"], bucket["_is_long"] = row["code"], row["name"], True

    out = []
    for key in order:
        bucket = merged[key]
        legs = bucket.pop("_legs")
        bucket.pop("_is_long", None)
        if legs > 1:
            bucket["day_pct"] = None
            bucket["pair"] = True
        out.append(bucket)
    return out


def _movers(rows: list[dict], limit: int) -> list[dict]:
    """오늘 가장 크게 움직인 종목 — 절대값으로 뽑고 부호 내림차순으로 준다.

    뽑을 때는 절대값이라 큰 하락도 놓치지 않고, 내보낼 때는 부호 순이라 화면
    위쪽이 상승, 아래쪽이 하락으로 갈린다. 롱숏 페어는 합쳐진 뒤에 겨룬다.
    """
    if limit <= 0:
        return []
    movable = [row for row in _merge_pairs(rows) if row.get("day_pnl")]
    picked = sorted(movable, key=lambda row: abs(row["day_pnl"]), reverse=True)[:limit]
    picked.sort(key=lambda row: row["day_pnl"], reverse=True)
    return [
        {
            "code": row["code"],
            "name": row["name"],
            "value": row["value"],
            "day_pnl": row["day_pnl"],
            "day_pct": row["day_pct"],
            "pair": bool(row.get("pair")),
        }
        for row in picked
    ]


async def _complete_quotes(codes: list[str]) -> dict[str, dict]:
    """웹의 /api/asset-quotes 와 같은 순서로 시세를 채운다."""
    results: dict[str, dict] = {}
    domestic = [c for c in codes if identifiers.is_korean_stock(c)]
    if domestic:
        try:
            results.update(
                await asyncio.wait_for(
                    stock_quotes.get_bulk_quote_snapshots(domestic), timeout=_QUOTE_ITEM_TIMEOUT
                )
            )
        except Exception as exc:
            logger.warning("device summary: 벌크 시세 실패, 개별 조회로 폴백: %s", exc)

    remaining = [c for c in codes if not results.get(c)]
    if not remaining:
        return results

    sem = asyncio.Semaphore(_QUOTE_CONCURRENCY)

    async def one(code: str) -> tuple[str, dict]:
        if code.startswith(NON_QUOTABLE_PREFIXES):
            return code, {}
        try:
            async with sem:
                quote = await asyncio.wait_for(
                    quote_service.fetch_quote(code), timeout=_QUOTE_ITEM_TIMEOUT
                )
            if quote:
                return code, quote
        except Exception as exc:
            logger.warning("device summary: %s 시세 실패: %s", code, exc)
        # 낡은 값이라도 있으면 쓴다 — 빈 값으로 두면 그 종목이 총액에서 통째로
        # 빠져 화면의 총 평가액이 조용히 작아진다.
        stale = stock_quotes.stock_to_quote(stock_quotes.get_stock_cached(code, allow_stale=True))
        if stale:
            stale["_stale"] = True
        return code, stale or {}

    for code, quote in await asyncio.gather(*(one(c) for c in remaining)):
        results[code] = quote
    return results


async def _market_brief() -> dict | None:
    """오늘(없으면 어제) 만들어진 시황 브리프. **여기서 새로 만들지는 않는다.**

    생성은 LLM 호출이라 비싸고 느리다. 기기 폴링이 그것을 촉발하면 30초 안에
    끝나야 하는 화면 갱신이 시황 생성을 기다리게 되므로 캐시에 있는 것만 쓴다.

    아침에는 오늘 브리프가 아직 없다 — 그때는 어제 것이 '지금 알려진 마지막
    시장 상태'라 더 쓸모 있다. 언제 것인지는 ``is_today`` 로 알려 준다.
    """
    today = date.today()
    for offset in (0, 1):
        brief_date = (today - timedelta(days=offset)).isoformat()
        try:
            brief = await market_brief_repo.get_daily_market_brief(MARKET_BRIEF_SUB, brief_date)
        except Exception as exc:
            logger.warning("device summary: 시황 조회 실패: %s", exc)
            return None
        text = ((brief or {}).get("markdown") or "").strip()
        if not text:
            continue
        payload = (brief or {}).get("payload") or {}
        return {
            "date": brief_date,
            "is_today": offset == 0,
            "indices": [
                {
                    "label": row.get("label") or row.get("code"),
                    "change_pct": _safe_float(row.get("change_pct")),
                }
                for row in (payload.get("market") or [])[:6]
            ],
            "text": text[:MARKET_BRIEF_CHARS],
        }
    return None


async def _current_nav(google_sub: str, total_value: float, baseline: dict) -> float | None:
    """현재 NAV = (평가액 - 미반영 입금) / (좌수 + 미반영 좌수변동).

    웹의 ``_curNavKrw`` 과 같은 식이다. 좌수를 모르면 마지막 스냅샷의 NAV 로
    떨어진다 — 수익률이 조금 낡을 뿐 엉뚱한 값이 나오진 않는다.
    """
    latest = await snapshots_repo.get_latest_snapshot(google_sub)
    if not latest:
        return None
    pending_units = 0.0
    pending_cash = 0.0
    for cashflow in baseline["cashflows"]:
        units = _safe_float(cashflow.get("units_change"))
        if units is not None:
            pending_units += units
        else:
            pending_cash += cashflow["signed_amount"]

    units = _safe_float(latest.get("total_units"))
    if units:
        units += pending_units
    value = total_value - pending_cash
    if units and units > 0 and value > 0:
        return value / units
    return _safe_float(latest.get("nav"))


async def _baseline(google_sub: str) -> dict:
    """Today 카드의 기준선: 직전 20:00 정산 스냅샷 + 그 뒤의 입출금."""
    baseline_date = time_windows.portfolio_today_baseline_date()
    snapshot = await snapshots_repo.get_snapshot_on_or_before(google_sub, baseline_date)
    created_after = (
        time_windows.settlement_marker_seconds(snapshot["date"]) if snapshot else baseline_date
    )
    rows = await snapshots_repo.get_cashflows_created_after(google_sub, created_after)
    cashflows = []
    net = 0.0
    for row in rows:
        signed = row["amount"] if row["type"] == "deposit" else -row["amount"] if row["type"] == "withdrawal" else 0.0
        if not signed:
            continue
        net += signed
        cashflows.append({**row, "signed_amount": signed})
    return {"snapshot": snapshot, "cashflows": cashflows, "net_cashflow": net}


def _empty_summary() -> dict:
    return {
        "total_value": 0.0,
        "day_pnl": 0.0,
        "day_pnl_pct": None,
        "ytd_pnl": 0.0,
        "ytd_pnl_pct": None,
        "holdings": [],
        "holdings_count": 0,
        "groups": [],
        "movers": [],
        "market": None,
        "unpriced": 0,
        "stale": False,
    }


async def build_summary(
    google_sub: str, *, top_n: int = DEFAULT_TOP_N, movers_n: int = DEFAULT_MOVERS_N
) -> dict:
    items = await portfolio_repo.get_portfolio(google_sub)
    if not items:
        return _empty_summary()

    quotes = await _complete_quotes([item["stock_code"] for item in items])

    total_value = 0.0
    unpriced = 0
    stale = False
    rows: list[dict] = []

    for item in items:                       # 서비스에서 정한 순서 그대로
        code = item["stock_code"]
        qty = _safe_float(item.get("quantity")) or 0.0
        quote = quotes.get(code) or {}
        price = _safe_float(quote.get("price"))
        if quote.get("_stale"):
            stale = True
        if price is None:
            if not code.startswith(NON_QUOTABLE_PREFIXES):
                unpriced += 1
            continue
        value = qty * price
        total_value += value
        day_pct = _safe_float(quote.get("change_pct"))
        day_pnl = _day_pnl(value, day_pct)
        rows.append({
            "code": code,
            "name": item.get("stock_name") or code,
            "group": item.get("group_name") or UNGROUPED,
            # 숏 다리는 이것으로 자기 롱을 가리킨다. movers 를 합칠 때 쓴다.
            "pair_long_code": item.get("pair_long_code"),
            "value": round(value),
            "day_pct": day_pct,
            "day_pnl": round(day_pnl) if day_pnl is not None else None,
        })

    for row in rows:
        row["weight_pct"] = _pct(row["value"], total_value)

    baseline = await _baseline(google_sub)
    cur_nav = await _current_nav(google_sub, total_value, baseline)
    year_start = await snapshots_repo.get_year_start_snapshot(google_sub)

    def period(snapshot: dict | None, *, subtract_cashflow: bool) -> tuple[float | None, float | None]:
        """(손익, 수익률%).

        수익률은 NAV 비교 — 입출금이 성과로 잡히지 않는다.
        금액은 평가액 차이 그대로다 (웹과 같은 정의). 그래서 기간 중 출금이
        크면 수익률은 플러스인데 금액은 마이너스로 나올 수 있다. 두 값의
        의미가 다를 뿐 오류가 아니다.
        """
        if not snapshot:
            return None, None
        base_value = _safe_float(snapshot.get("total_value"))
        base_nav = _safe_float(snapshot.get("nav"))
        pnl = None
        if base_value is not None:
            pnl = total_value - base_value
            if subtract_cashflow:
                pnl -= baseline["net_cashflow"]
        pct = None
        if base_nav and cur_nav:
            pct = round((cur_nav / base_nav - 1) * 100, 2)
        return pnl, pct

    day_pnl, day_pct = period(baseline["snapshot"], subtract_cashflow=True)
    ytd_pnl, ytd_pct = period(year_start, subtract_cashflow=False)

    return {
        "total_value": round(total_value),
        "day_pnl": round(day_pnl) if day_pnl is not None else None,
        "day_pnl_pct": day_pct,
        "ytd_pnl": round(ytd_pnl) if ytd_pnl is not None else None,
        "ytd_pnl_pct": ytd_pct,
        "holdings": rows[:top_n] if top_n and top_n > 0 else rows,
        "holdings_count": len(rows),
        # 그룹과 movers 는 잘리기 전 전체 행에서 계산한다.
        "groups": _group_rows(rows, total_value),
        "movers": _movers(rows, movers_n),
        # 화면에 직접 그리지는 않는다 — 아래 코멘트 한 칸을 쓰는 재료다.
        "market": await _market_brief(),
        "unpriced": unpriced,
        "stale": stale,
    }
