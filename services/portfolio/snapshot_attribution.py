"""정산 시점 환율을 보존한다. 정보 누락이 기존 정산 자체를 중단시키지 않는다."""

from services.portfolio import fx
from services.portfolio.identifiers import is_korean_stock, is_special_asset


async def metadata(item: dict, *, stale: bool, snap_date: str, today: str) -> dict:
    code = item["stock_code"]
    currency = "KRW" if is_korean_stock(code) or is_special_asset(code) else item.get("currency")
    if code.startswith("CASH_"):
        currency = code.removeprefix("CASH_")
    if currency == "KRW":
        return {"currency": "KRW", "fx_rate": 1.0}
    if stale or not currency or snap_date != today:
        return {"currency": currency, "fx_rate": None}
    rate = fx.cached_rate_for_currency(currency)
    return {"currency": currency, "fx_rate": rate}
