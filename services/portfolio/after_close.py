"""국내 장후 가격을 별도로 보존한다. 정산 NAV와 최신가 캐시는 변경하지 않는다."""

import asyncio

from domain.portfolio_codes import is_korean_stock
from repositories import cache_values, portfolio, snapshots
from services.portfolio import regular_close, runtime_quotes, time_windows

NAMESPACE = "after_close_valuation"


async def load(user, day):
    entry = await cache_values.get_cache_value_entry(NAMESPACE, f"{user}:{day}")
    return entry.value if entry else None


async def capture(user):
    now = time_windows.now_kst()
    day = now.date().isoformat()
    saved = await load(user, day)
    if saved and not saved["missing"]:
        return saved
    if not (20, 0) <= (now.hour, now.minute) < (20, 15):
        raise ValueError("장후 평가 수집 시각을 놓쳤습니다. 늦은 현재가를 20시 가격으로 소급하지 않습니다.")
    closing = await snapshots.get_snapshot_by_date(user, day)
    if not closing or closing.get("price_basis") != "regular_close_v1":
        raise ValueError("당일 정규장 정산이 없습니다.")
    before = {r["stock_code"]: r for r in await snapshots.get_stock_snapshots_exact_date(user, day) if is_korean_stock(r["stock_code"])}
    current = {r["stock_code"]: r for r in await portfolio.get_portfolio(user) if is_korean_stock(r["stock_code"])}
    rows = {r["stock_code"]: r for r in (saved or {}).get("holdings", [])}
    missing = []
    semaphore = asyncio.Semaphore(3)

    async def fetch(code):
        if code in rows:
            return
        async with semaphore:
            try:
                quote = await runtime_quotes.fetch_quote(code, force_refresh=True, use_ws_cache=False)
                price = regular_close.positive(quote["price"])
                finished = time_windows.now_kst()
                if finished.date().isoformat() != day or (finished.hour, finished.minute) >= (20, 15):
                    raise ValueError("장후 수집 시간 초과")
                if quote.get("_stale") or (quote.get("date") and quote["date"] != day):
                    raise ValueError("장후 시세 누락")
                old = before.get(code)
                current_qty = current.get(code, {}).get("quantity", 0)
                comparable = bool(old and old.get("quantity") == current_qty and old.get("unit_price"))
                close_price = old.get("unit_price") if old else None
                rows[code] = {"stock_code": code, "stock_name": current.get(code, {}).get("stock_name") or code,
                              "price": price, "regular_close": close_price, "quantity": current_qty,
                              "change_pct": (price / close_price - 1) * 100 if close_price else None,
                              "contribution": (price - close_price) * current_qty if comparable else None,
                              "quantity_changed": not comparable, "quote_as_of": quote.get("as_of"),
                              "captured_at": finished.isoformat()}
            except Exception:
                missing.append(code)

    await asyncio.gather(*(fetch(code) for code in sorted(before.keys() | current.keys())))
    result = {"date": day, "captured_at": now.isoformat(), "source": "after_close",
              "holdings": list(rows.values()), "missing": missing,
              "basis_note": "정규장 종가 대비 장후 가격 변화입니다. 기여 금액은 수량이 같은 종목만 포함하며 장후 매매손익은 제외합니다."}
    await cache_values.set_cache_value(NAMESPACE, f"{user}:{day}", result)
    return result


async def capture_all():
    import snapshot_nav

    if regular_close.closing_at(time_windows.today_kst_date().isoformat()) is None:
        return
    failed = []
    for user in await snapshots.get_all_users_with_portfolio():
        try:
            result = await capture(user)
            if result["missing"]:
                failed.append(user)
        except Exception:
            failed.append(user)
    # 해외 벤치마크 갱신의 기존 저녁 관측 시각은 유지한다.
    await snapshot_nav._update_benchmark_history()
    if failed:
        raise ValueError(f"장후 평가 {len(failed)}개 포트폴리오 미완료")
