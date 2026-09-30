"""정규장 정산의 시간·가격 계약. 최신 시세 캐시를 쓰거나 덮어쓰지 않는다."""

import asyncio
import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from domain.market_calendar import closing_at
from domain.portfolio_codes import is_korean_stock, is_special_asset
from repositories import settlement_inputs
from services.market.sources import kis_proxy as kis_proxy_client
from services.portfolio import foreign, fx, runtime_quotes, time_windows

BASIS = "regular_close_v1"


def positive(value):
    number = float(str(value).replace(",", ""))
    if not math.isfinite(number) or number <= 0:
        raise ValueError("유효한 평가 가격이 없습니다.")
    return number


async def foreign_close(code: str, cutoff: datetime) -> dict:
    await foreign.ensure_ticker_map()
    ticker = foreign._ticker_map.get(code, code)
    payload = await foreign.fetch_yahoo_chart(ticker, range_="1mo")
    meta = payload.get("meta") or {}
    period = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
    end = period.get("end")
    zone = meta.get("exchangeTimezoneName")
    if not end or not zone:
        raise ValueError(f"{code}: 해외 정규장 종료 시각 누락")
    market_zone = ZoneInfo(zone)
    end_at = datetime.fromtimestamp(end, tz=market_zone)
    current_date = end_at.date().isoformat()
    # 진행 중인 일봉과 장후 체결가는 포함하지 않는다.
    dated = [{**r, "date": r.get("session_date") or r["date"]} for r in payload.get("rows", [])]
    rows = [r for r in dated if r["date"] < current_date or
            (r["date"] == current_date and end_at <= cutoff)]
    if not rows:
        raise ValueError(f"{code}: 완료된 해외 정규장 종가 없음")
    row = max(rows, key=lambda r: r["date"])
    if (cutoff.date() - date.fromisoformat(row["date"])).days > 7:
        raise ValueError(f"{code}: 해외 종가 지연")
    return {"native_price": positive(row["close"]), "currency": payload["currency"],
            "price_date": row["date"], "source": "yahoo_regular_daily_unadjusted"}


async def collect_prices(user: str, day: str, inputs: dict) -> dict:
    close = closing_at(day)
    if close is None:
        raise ValueError("휴장일입니다.")
    saved = await settlement_inputs.prices(user, day) or {"prices": {}, "rates": {}, "basis": BASIS}
    items = [r for r in inputs["user_portfolio"] if r["quantity"]]
    now = time_windows.now_kst()
    # KRX 애프터마켓 개시 이전에 J 시장 가격을 보존한다. 기존 daily API는
    # 장후 가격으로 갱신되므로 정규장 종가의 대체 소스로 사용할 수 없다.
    in_window = close <= now < close + timedelta(minutes=10)
    currencies = {"KRW"}
    for item in items:
        currencies.add(item["stock_code"].removeprefix("CASH_") if item["stock_code"].startswith("CASH_") else item.get("currency") or "KRW")
        currencies.update(r.get("avg_price_currency") or "KRW" for r in item.get("account_positions") or [item])
    if in_window:
        # USD 표시 전환용 환율도 보존하되, 원화 전용 포트폴리오의 정산을 막지는 않는다.
        for currency in sorted((currencies | {"USD"}) - saved["rates"].keys()):
            if currency == "KRW":
                saved["rates"][currency] = {"rate": 1.0, "as_of": close.isoformat()}
                continue
            try:
                await fx.fx_rate_for_currency(currency)
                rate = fx.cached_rate_for_currency(currency)
                if rate and time_windows.now_kst() < close + timedelta(minutes=10):
                    saved["rates"][currency] = {"rate": rate, "as_of": time_windows.now_kst().isoformat()}
            except Exception:
                pass
    semaphore = asyncio.Semaphore(3)

    async def collect(item):
        code = item["stock_code"]
        if code in saved["prices"]:
            return
        async with semaphore:
            if not in_window or not close <= time_windows.now_kst() < close + timedelta(minutes=10):
                return
            try:
                if is_korean_stock(code):
                    response = await kis_proxy_client.get_quote(code, market="J")
                    raw = response.get("raw") or {}
                    price = positive(raw.get("stck_prpr") or (response.get("summary") or {}).get("current_price"))
                    value = {"price": price, "currency": "KRW", "price_date": day, "source": "kis_J_regular_close_window"}
                elif code.startswith("CASH_"):
                    currency = code.removeprefix("CASH_")
                    value = {"price": saved["rates"][currency]["rate"], "currency": currency, "source": "closing_fx"}
                elif is_special_asset(code):
                    q = await runtime_quotes.fetch_quote(code, force_refresh=True, use_ws_cache=False)
                    if q.get("_stale"):
                        raise ValueError("지연 시세")
                    value = {"price": positive(q.get("price")), "currency": "KRW", "source": "closing_time_observation", "quote_as_of": q.get("as_of")}
                else:
                    value = await foreign_close(code, close)
                    value["price"] = value["native_price"] * saved["rates"][value["currency"]]["rate"]
                finished = time_windows.now_kst()
                if finished < close + timedelta(minutes=10):
                    saved["prices"][code] = {**value, "captured_at": finished.isoformat()}
                    saved.get("errors", {}).pop(code, None)
            except Exception as exc:
                saved.setdefault("errors", {})[code] = str(exc)

    await asyncio.gather(*(collect(item) for item in items))
    await settlement_inputs.save_prices(user, day, saved)
    missing = [r["stock_code"] for r in items if r["stock_code"] not in saved["prices"]]
    if missing or currencies - saved["rates"].keys():
        raise ValueError("마감 가격/환율 미수집: " + ", ".join(missing + sorted(currencies - saved["rates"].keys())))
    return saved


def value_inputs(inputs: dict, quotes: dict) -> tuple[float, float, list[dict]]:
    total = invested = 0.0
    rows = []
    for item in inputs["user_portfolio"]:
        qty = item["quantity"]
        if not qty:
            continue
        q = quotes["prices"][item["stock_code"]]
        cost = sum(r["quantity"] * r["avg_price"] * quotes["rates"][r.get("avg_price_currency") or "KRW"]["rate"]
                   for r in item.get("account_positions") or [item])
        value = qty * positive(q["price"])
        currency = q["currency"]
        rows.append({"stock_code": item["stock_code"], "stock_name": item.get("stock_name"),
                     "market_value": value, "quantity": qty, "unit_price": q["price"],
                     "avg_price_krw": cost / qty, "cost_basis": cost, "group_name": item.get("group_name"),
                     "currency": currency, "fx_rate": quotes["rates"][currency]["rate"], "priced_from_fallback": False})
        total += value
        invested += cost
    return total, invested, rows


async def settle(user: str, day: str):
    from repositories import snapshots
    from repositories.db import transaction
    from services.portfolio import nav_snapshot as snapshot_nav

    close = closing_at(day)
    if close is None or time_windows.now_kst() < close + timedelta(minutes=5):
        return
    existing = await snapshots.get_snapshot_by_date(user, day)
    if existing:
        if existing.get("price_basis") != BASIS:
            raise ValueError("기존 정산은 보존합니다. 과거 자료 전환 도구를 사용하세요.")
        return
    cutoff = close.replace(tzinfo=None).isoformat(timespec="milliseconds")
    inputs = await settlement_inputs.load(user, cutoff)
    quotes = await collect_prices(user, day, inputs)
    total, invested, rows = value_inputs(inputs, quotes)
    async with transaction():
        if await snapshots.get_snapshot_by_date(user, day):
            return
        latest = await snapshots.get_latest_snapshot(user)
        if latest and latest["date"] > day:
            raise ValueError("후속 정산이 있습니다. 시간순 재계산이 필요합니다.")
        await snapshot_nav._persist_snapshot(user, day, total, invested, rows, cutoff, frozen=inputs,
                                            frozen_fx=quotes["rates"].get("USD", {}).get("rate"))
