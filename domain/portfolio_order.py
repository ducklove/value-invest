"""초기 잔고 표시 순서: 국내 자산, 해외 자산, 예수금·현금성 자산."""

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
