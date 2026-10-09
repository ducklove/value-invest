import json
from unittest.mock import AsyncMock, patch

import pytest

from routes import portfolio
from services import stock_quotes
from services.portfolio import futures_quotes as futures

CODE = "KRFUT_KA486B000"
OTHER_MATURITY = "KRFUT_KA486C000"
MAPPING = {"KA486B000": "006800", "KA486C000": "006800"}
FUTURE_QUOTE = {"price": 355, "previous_close": 350, "change": 5, "change_pct": 1.43,
                "date": "2026-10-06", "source": "namuh_balance"}


@pytest.mark.asyncio
@pytest.mark.parametrize("pct", [2.5, -1.2, 0])
async def test_uses_spot_percentage_without_changing_futures_price_or_short_valuation(pct):
    spot = {"price": 40000, "change_pct": pct}
    quotes = {CODE: FUTURE_QUOTE, OTHER_MATURITY: FUTURE_QUOTE, "006800": spot}
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)), \
         patch.object(futures.stock_quotes, "get_bulk_quote_snapshots", AsyncMock()) as bulk:
        result = await futures.enrich_quotes(quotes)
    for code in (CODE, OTHER_MATURITY):
        assert result[code] == {**FUTURE_QUOTE, "change_pct": pct, "underlying_code": "006800"}
        assert -20 * result[code]["price"] + 8000 == 900
    assert FUTURE_QUOTE["change_pct"] == 1.43  # 원본 시세와 공유 캐시를 변경하지 않는다.
    assert result["006800"] == spot
    bulk.assert_not_awaited()


@pytest.mark.asyncio
async def test_prefers_owned_underlying_nh_tick_without_shared_cache_writes():
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)), \
         patch.object(futures, "namuh_quote", return_value={"price": 40000, "change_pct": -3.1}) as nh, \
         patch.object(stock_quotes, "remember_quote") as remember, \
         patch.object(stock_quotes, "get_bulk_quote_snapshots", AsyncMock()) as bulk:
        result = await futures.enrich_quotes({CODE: FUTURE_QUOTE}, "owner")
    assert result[CODE]["change_pct"] == -3.1
    nh.assert_called_once_with("owner", "006800")
    remember.assert_not_called()
    bulk.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_spot_is_fetched_once_for_multiple_maturities():
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)), \
         patch.object(stock_quotes, "get_stock_cached", return_value=None), \
         patch.object(stock_quotes, "get_bulk_quote_snapshots", AsyncMock(return_value={"006800": {"price": 40000, "change_pct": 4.2}})) as bulk, \
         patch.object(stock_quotes, "get_stock", AsyncMock()) as single:
        result = await futures.enrich_quotes({CODE: FUTURE_QUOTE, OTHER_MATURITY: FUTURE_QUOTE})
    assert result[CODE]["change_pct"] == result[OTHER_MATURITY]["change_pct"] == 4.2
    bulk.assert_awaited_once_with(["006800"])
    single.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_bulk_falls_back_to_standard_stock_quote():
    stock = stock_quotes.stock_from_quote("006800", {"price": 41000, "previous_close": 40000})
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)), \
         patch.object(stock_quotes, "get_stock_cached", return_value=None), \
         patch.object(stock_quotes, "get_bulk_quote_snapshots", AsyncMock(side_effect=TimeoutError)), \
         patch.object(stock_quotes, "get_stock", AsyncMock(return_value=stock)) as single:
        result = await futures.enrich_quotes({CODE: FUTURE_QUOTE})
    assert result[CODE]["change_pct"] == 2.5
    single.assert_awaited_once_with("006800")


@pytest.mark.asyncio
@pytest.mark.parametrize("pct", [None, float("nan"), float("inf"), True])
async def test_unavailable_underlying_rate_keeps_future_price_and_displays_unknown(pct):
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)), \
         patch.object(stock_quotes, "get_stock_cached", return_value=None), \
         patch.object(stock_quotes, "get_bulk_quote_snapshots", AsyncMock(return_value={"006800": {"change_pct": pct}})), \
         patch.object(stock_quotes, "get_stock", AsyncMock(side_effect=TimeoutError)):
        result = await futures.enrich_quotes({CODE: FUTURE_QUOTE})
    assert result[CODE]["price"] == 355
    assert result[CODE]["change_pct"] is None


@pytest.mark.asyncio
async def test_unknown_contract_does_not_guess_underlying_from_name_or_future_percentage():
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value={})), \
         patch.object(stock_quotes, "get_bulk_quote_snapshots", AsyncMock()) as bulk:
        result = await futures.enrich_quotes({CODE: FUTURE_QUOTE})
    assert result[CODE] == {**FUTURE_QUOTE, "change_pct": None}
    bulk.assert_not_awaited()


@pytest.mark.asyncio
async def test_portfolio_enrichment_keeps_quantities_memo_cash_and_spot_rows():
    spot = {"price": 40000, "change_pct": 2.5}
    items = [{"stock_code": CODE, "quantity": -20, "memo": "만기일 2026-11-12", "quote": FUTURE_QUOTE},
             {"stock_code": "CASH_KRW", "quantity": 8000, "quote": {"price": 1}},
             {"stock_code": "006800", "quantity": 2, "quote": spot}]
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)):
        await futures.enrich_items(items, "owner")
    assert items[0]["quote"]["change_pct"] == 2.5
    assert items[0]["quantity"] == -20 and items[0]["memo"] == "만기일 2026-11-12"
    assert items[1]["quantity"] == 8000
    assert items[2]["quote"] == spot


@pytest.mark.asyncio
@pytest.mark.parametrize("fresh", [True, False])
async def test_batch_asset_quote_uses_same_underlying_percentage_for_future(fresh):
    spot = {"price": 40000, "change_pct": -2.5}
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)), \
         patch.object(portfolio, "_fetch_quote", AsyncMock(return_value=FUTURE_QUOTE)), \
         patch.object(portfolio, "_cached_quote_for_code", side_effect=lambda code: FUTURE_QUOTE if code == CODE else spot), \
         patch.object(stock_quotes, "get_bulk_quote_snapshots", AsyncMock(return_value={"006800": spot})):
        result = await portfolio.asset_quotes_batch({"codes": [CODE, "006800"], "fresh": fresh})
    assert result[CODE]["change_pct"] == result["006800"]["change_pct"] == -2.5
    assert result[CODE]["price"] == 355


@pytest.mark.asyncio
async def test_single_asset_quote_uses_authenticated_underlying_tick():
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)), \
         patch.object(futures, "namuh_quote", return_value={"price": 40000, "change_pct": 2.5}) as nh, \
         patch.object(portfolio, "_realtime_quotes_for_request", AsyncMock(return_value={})), \
         patch.object(portfolio, "get_current_user", AsyncMock(return_value={"google_sub": "owner"})), \
         patch.object(portfolio, "_fetch_quote", AsyncMock(return_value=FUTURE_QUOTE)):
        result = await portfolio.asset_quote(CODE, object())
    assert result["change_pct"] == 2.5 and result["price"] == 355
    nh.assert_called_once_with("owner", "006800")


@pytest.mark.asyncio
async def test_non_future_quotes_do_not_load_master_or_fetch_extra_quotes():
    original = {"005930": {"price": 75000, "change_pct": 1.35}}
    with patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock()) as master:
        assert await futures.enrich_quotes(original) is original
    master.assert_not_awaited()


@pytest.mark.asyncio
async def test_streamed_quote_keeps_underlying_percentage_on_later_updates():
    from services.brokers import realtime
    items = [{"stock_code": CODE, "benchmark_code": "KOSPI"}]
    tick = {"price": 40000, "change_pct": -2.5}
    with patch.object(portfolio, "get_current_user", AsyncMock(return_value={"google_sub": "owner"})), \
         patch.object(portfolio.portfolio_repo, "get_portfolio", AsyncMock(return_value=items)), \
         patch.object(portfolio, "_fetch_quote", AsyncMock(return_value=FUTURE_QUOTE)), \
         patch.object(portfolio, "_fetch_benchmark_quote", AsyncMock(return_value={})), \
         patch.object(realtime, "quote", return_value=None), \
         patch.object(futures, "namuh_quote", return_value=tick), \
         patch.object(futures.futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)):
        response = await portfolio.stream_portfolio_quotes(object())
        events = [json.loads(chunk.removeprefix("data: ").strip()) async for chunk in response.body_iterator]
    quote = next(event["quote"] for event in events if event.get("stock_code") == CODE)
    assert quote["change_pct"] == -2.5 and quote["price"] == 355
