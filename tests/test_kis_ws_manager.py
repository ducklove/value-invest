from datetime import datetime, timedelta, timezone

import kis_ws_manager

KST = timezone(timedelta(hours=9))


def _dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 5, 20, hour, minute, tzinfo=KST)


def _sat(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 5, 23, hour, minute, tzinfo=KST)


def test_rest_market_uses_j_during_regular_session():
    assert kis_ws_manager.active_market_code(_dt(9, 30)) == "J"


def test_rest_market_keeps_nxt_after_websocket_session_closes():
    assert kis_ws_manager._active_tr_id(_dt(21, 10)) == "H0STCNT0"
    assert kis_ws_manager.active_market_code(_dt(21, 10)) == "NX"
    assert kis_ws_manager.ws_cache_matches_rest_market(_dt(21, 10)) is False


def test_websocket_nxt_session_closes_at_20_but_rest_keeps_final_nxt():
    assert kis_ws_manager._active_tr_id(_dt(19, 59)) == "H0NXCNT0"
    assert kis_ws_manager.ws_cache_matches_rest_market(_dt(19, 59)) is True
    assert kis_ws_manager._active_tr_id(_dt(20, 5)) == "H0STCNT0"
    assert kis_ws_manager.active_market_code(_dt(20, 5)) == "NX"
    assert kis_ws_manager.ws_cache_matches_rest_market(_dt(20, 5)) is False


def test_rest_market_uses_nxt_before_regular_open():
    assert kis_ws_manager.active_market_code(_dt(8, 30)) == "NX"
    assert kis_ws_manager.ws_cache_matches_rest_market(_dt(8, 30)) is True


def test_rest_market_uses_nxt_on_weekend_regular_hours():
    assert kis_ws_manager.active_market_code(_sat(10, 30)) == "NX"
    assert kis_ws_manager.ws_cache_matches_rest_market(_sat(10, 30)) is False


def test_parse_realtime_quote_carries_market_and_source_metadata():
    fields = [""] * 34
    fields[0] = "000660"
    fields[1] = "153100"
    fields[2] = "1818000"
    fields[3] = "2"
    fields[4] = "73000"
    fields[5] = "4.18"
    fields[13] = "778872"
    fields[14] = "1419765768000"
    fields[33] = "20260521"

    quote = kis_ws_manager._parse_h0stcnt0("0|H0NXCNT0|001|" + "^".join(fields))

    assert quote["date"] == "20260521"
    assert quote["source"] == "ws"
    assert quote["market"] == "NX"


def test_alphanumeric_krx_etf_code_is_korean_stock():
    assert kis_ws_manager.is_korean_stock("0074K0")
    assert kis_ws_manager.is_korean_stock("0074k0")


def test_subscription_plan_uses_single_connection_capacity_by_default():
    codes = [f"{idx:06d}" for idx in range(45)]
    requested = {"portfolio": codes, "sidebar": ["AAPL"]}

    plan = kis_ws_manager.plan_requested_subscriptions(requested)

    assert plan["ws"] == codes[:kis_ws_manager.MAX_SUBSCRIPTIONS]
    assert plan["rest"] == codes[kis_ws_manager.MAX_SUBSCRIPTIONS:] + ["AAPL"]


def test_subscription_plan_keeps_alphanumeric_krx_etf_on_websocket_path():
    plan = kis_ws_manager.plan_requested_subscriptions({"portfolio": ["0074K0", "AAPL"]})

    assert plan["ws"] == ["0074K0"]
    assert plan["rest"] == ["AAPL"]


def test_subscription_plan_can_use_combined_websocket_capacity():
    codes = [f"{idx:06d}" for idx in range(45)]
    requested = {"portfolio": codes, "sidebar": ["AAPL"]}

    plan = kis_ws_manager.plan_requested_subscriptions(
        requested,
        max_subscriptions=kis_ws_manager.MAX_SUBSCRIPTIONS * 2,
    )

    assert plan["ws"] == codes
    assert plan["rest"] == ["AAPL"]
