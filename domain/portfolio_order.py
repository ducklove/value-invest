"""계좌 초기 잔고 순서와 전체 계좌 합산 목록의 롱숏 배치 규칙."""

from domain.broker_assets import is_futures_value
from domain.portfolio_codes import is_cash_asset, is_korean_stock, normalize_portfolio_code


def initial_order_key(code: str, product: str | None = None) -> tuple[int, str]:
    code = normalize_portfolio_code(code)
    if is_cash_asset(code) or code == "CMA_RP_KRW":
        return 2, code
    domestic_code = code.removesuffix(".KS").removesuffix(".KQ")
    domestic = is_korean_stock(domestic_code) or code == "KRX_GOLD"
    if is_futures_value(code):
        domestic = product != "gbfuture"
    return (0 if domestic else 1), code


def pin_long_short_pairs(items: list[dict]) -> list[dict]:
    """합산 목록에서 롱의 순서를 기준으로 연결된 숏을 바로 뒤에 붙인다."""
    by_code = {item["stock_code"]: item for item in items}
    shorts: dict[str, list[dict]] = {}
    for item in items:
        long_code = item.get("pair_long_code")
        long_item = by_code.get(long_code)
        if long_item is not None and long_item is not item and not long_item.get("pair_long_code"):
            shorts.setdefault(long_code, []).append(item)
    pinned = {item["stock_code"] for legs in shorts.values() for item in legs}
    output = []
    for item in items:
        if item["stock_code"] in pinned:
            continue
        output.append(item)
        output.extend(shorts.get(item["stock_code"], []))
    return output
