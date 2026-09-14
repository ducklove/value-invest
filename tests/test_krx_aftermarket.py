from copy import deepcopy
from unittest.mock import AsyncMock, patch

import kis_ws_manager
import stock_price
from services import stock_intraday, stock_quotes
from services.portfolio.quotes import should_accept_quote_snapshot


def test_bulk_integrated_selects_latest_trade_and_preserves_basis():
    entry = {
        "itemCode": "005930", "closePrice": "102", "compareToPreviousClosePrice": "2",
        "localTradedAt": "2026-09-14T19:59:00+09:00",
        "overMarketPriceInfo": {
            "overPrice": "100", "compareToPreviousClosePrice": "0",
            "localTradedAt": "2026-09-14T17:00:00+09:00",
        },
        "integratedPriceInfo": {
            "accumulatedTradingVolumeRaw": "300", "accumulatedTradingValueRaw": "30000",
        },
    }
    with patch.object(kis_ws_manager, "active_market_code", return_value="UN"):
        quote = stock_price._parse_naver_bulk_entry(entry)[1]
        assert quote["price"] == 102
        assert quote["as_of"] == entry["localTradedAt"]
        assert (quote["market"], quote["volume"], quote["trade_value"]) == ("UN", 300, 30000)
        later_nxt = deepcopy(entry)
        later_nxt["overMarketPriceInfo"]["localTradedAt"] = "2026-09-14T20:00:00+09:00"
        assert stock_price._parse_naver_bulk_entry(later_nxt)[1]["price"] == 100


def test_trade_timestamp_survives_stock_roundtrip_and_prevents_price_rewind():
    latest = {"code": "005930", "price": 102, "date": "2026-09-14",
              "as_of": "2026-09-14T19:59:00+09:00", "source": "naver", "market": "J"}
    result = stock_quotes.stock_to_quote(stock_quotes.stock_from_quote("005930", latest))
    assert result["as_of"] == latest["as_of"]
    assert result["date"] == latest["date"]
    older_ws = {**latest, "price": 100, "source": "ws", "as_of": "2026-09-14T17:00:00+09:00"}
    assert not should_accept_quote_snapshot(result, older_ws)
    assert should_accept_quote_snapshot(older_ws, result)


async def test_intraday_expands_only_sessions_with_aftermarket_trades():
    day = '<item data="20260911|100|100|100|100|1" />'
    for last_time, expected_end in (
        ("1959", "20:00"), ("1600", "20:00"), ("1559", "15:59"), ("1530", "15:30"),
    ):
        minute = (f'<item data="202609140900|null|null|null|100|1" />'
                  f'<item data="20260914{last_time}|null|null|null|102|2" />')
        with patch.object(stock_intraday, "_fetch_fchart", new=AsyncMock(side_effect=[minute, day])):
            result = await stock_intraday._korean_intraday("005930")
        assert result["session"]["end"] == expected_end
        assert result["points"][-1]["p"] == 102


def test_integrated_wire_quote_carries_correct_market_and_trade_time():
    fields = ["0"] * 34
    fields[0], fields[1], fields[2], fields[3], fields[33] = "005930", "195959", "102", "2", "20260914"
    quote = kis_ws_manager._parse_h0stcnt0("0|H0UNCNT0|001|" + "^".join(fields))
    assert quote["market"] == "UN"
    assert quote["as_of"] == "2026-09-14T19:59:59+09:00"


async def test_integrated_rest_zero_price_is_not_a_fresh_zero_valuation():
    with (patch.object(kis_ws_manager, "active_market_code", return_value="UN"),
          patch.object(stock_price.kis_proxy_client, "get_quote", new=AsyncMock(return_value={
              "summary": {"current_price": "0"}})),
          patch.object(stock_price.kis_proxy_client, "get_history", new=AsyncMock(return_value={
              "items": [{"stck_bsop_date": "20260914", "stck_clpr": "102"}]}))):
        quote = await stock_price.fetch_quote_snapshot("005930", use_ws_cache=False)
    assert quote["price"] == 102
    assert quote["_stale"] is True


def test_old_single_market_cache_is_not_used_as_integrated_quote():
    with (patch.object(kis_ws_manager, "active_market_code", return_value="UN"),
          patch.object(kis_ws_manager, "ws_cache_matches_rest_market", return_value=True),
          patch.object(kis_ws_manager, "get_cached_quote", return_value={
              "price": 102, "market": "J"})):
        assert stock_quotes._get_ws_stock("005930", max_age_seconds=None) is None
