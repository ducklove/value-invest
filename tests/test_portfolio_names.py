"""services.portfolio.names — display-name/ticker resolution paths."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from services.portfolio import foreign
from services.portfolio import names


async def test_cash_asset_uses_static_label():
    resolved = await names.resolve_portfolio_name(" cash_usd ")

    assert resolved == {"stock_code": "CASH_USD", "stock_name": "미국 달러"}


async def test_special_asset_uses_static_label():
    resolved = await names.resolve_portfolio_name("KRX_GOLD")

    assert resolved == {"stock_code": "KRX_GOLD", "stock_name": "KRX 금현물"}


async def test_static_foreign_ticker_short_circuits_discovery():
    with patch.object(foreign, "resolve_foreign_reuters", new=AsyncMock(side_effect=AssertionError("no discovery"))):
        resolved = await names.resolve_portfolio_name("a200")

    assert resolved["stock_code"] == "A200.AX"
    assert resolved["reuters_code"] == "A200.AX"
    assert resolved["stock_name"]


async def test_berkshire_class_b_alias_resolves_to_yahoo_ticker():
    with patch.object(foreign, "resolve_foreign_reuters", new=AsyncMock(side_effect=AssertionError("no discovery"))):
        resolved = await names.resolve_portfolio_name("BRK.B")

    assert resolved == {
        "stock_code": "BRK-B",
        "stock_name": "Berkshire Hathaway Inc. Class B",
        "reuters_code": "BRK-B",
    }


async def test_korean_stock_uses_domestic_resolver():
    resolver = AsyncMock(return_value="삼성전자")
    with patch.object(foreign, "resolve_name", new=resolver):
        resolved = await names.resolve_portfolio_name("005930")

    assert resolved == {"stock_code": "005930", "stock_name": "삼성전자"}
    resolver.assert_awaited_once_with("005930")


async def test_domestic_alias_wins_over_foreign_lookup():
    alias = AsyncMock(return_value={"stock_code": "002380", "corp_name": "케이씨씨"})
    reuters = AsyncMock(side_effect=AssertionError("alias must not fall through to foreign"))
    with patch.object(foreign, "resolve_domestic_code_alias", new=alias), \
         patch.object(foreign, "resolve_foreign_reuters", new=reuters):
        resolved = await names.resolve_portfolio_name("KCC")

    assert resolved == {"stock_code": "002380", "stock_name": "케이씨씨"}
    reuters.assert_not_awaited()


async def test_foreign_code_resolves_via_reuters_pipeline():
    with patch.object(foreign, "resolve_domestic_code_alias", new=AsyncMock(return_value=None)), \
         patch.object(foreign, "resolve_foreign_reuters", new=AsyncMock(return_value="AAPL.O")), \
         patch.object(foreign, "fetch_naver_world_stock", new=AsyncMock(return_value={"stockName": "애플", "stockNameEng": "Apple"})):
        resolved = await names.resolve_portfolio_name("AAPL")

    assert resolved == {"stock_code": "AAPL.O", "stock_name": "애플", "reuters_code": "AAPL.O"}


async def test_foreign_name_falls_back_to_english_then_none():
    with patch.object(foreign, "resolve_domestic_code_alias", new=AsyncMock(return_value=None)), \
         patch.object(foreign, "resolve_foreign_reuters", new=AsyncMock(return_value="XYZ.O")), \
         patch.object(foreign, "fetch_naver_world_stock", new=AsyncMock(return_value={"stockNameEng": "Xyz Corp"})):
        resolved = await names.resolve_portfolio_name("XYZ")

    assert resolved["stock_name"] == "Xyz Corp"

    with patch.object(foreign, "resolve_domestic_code_alias", new=AsyncMock(return_value=None)), \
         patch.object(foreign, "resolve_foreign_reuters", new=AsyncMock(return_value="XYZ.O")), \
         patch.object(foreign, "fetch_naver_world_stock", new=AsyncMock(return_value=None)):
        resolved = await names.resolve_portfolio_name("XYZ")

    assert resolved == {"stock_code": "XYZ.O", "stock_name": None, "reuters_code": "XYZ.O"}


async def test_unresolvable_code_returns_none_name():
    with patch.object(foreign, "resolve_domestic_code_alias", new=AsyncMock(return_value=None)), \
         patch.object(foreign, "resolve_foreign_reuters", new=AsyncMock(return_value=None)):
        resolved = await names.resolve_portfolio_name("zzzz")

    assert resolved == {"stock_code": "ZZZZ", "stock_name": None}
