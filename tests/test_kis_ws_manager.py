from datetime import datetime, timezone, timedelta

import kis_ws_manager


KST = timezone(timedelta(hours=9))


def _dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 5, 20, hour, minute, tzinfo=KST)


def test_rest_market_uses_j_during_regular_session():
    assert kis_ws_manager.active_market_code(_dt(9, 30)) == "J"


def test_rest_market_keeps_nxt_after_websocket_session_closes():
    assert kis_ws_manager._active_tr_id(_dt(21, 10)) == "H0STCNT0"
    assert kis_ws_manager.active_market_code(_dt(21, 10)) == "NX"
    assert kis_ws_manager.ws_cache_matches_rest_market(_dt(21, 10)) is False


def test_rest_market_uses_nxt_before_regular_open():
    assert kis_ws_manager.active_market_code(_dt(8, 30)) == "NX"
    assert kis_ws_manager.ws_cache_matches_rest_market(_dt(8, 30)) is True
