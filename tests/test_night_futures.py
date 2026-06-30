"""야간선물(NIGHT_FUTURES) — KIS proxy 응답 파싱·페치 회귀 테스트.

데이터 소스를 esignal 스크래핑에서 KIS proxy(/v1/futures/kospi-night/
near-month/quote)로 옮기면서, summary 필드 → indicator 결과 매핑(부호 코드
1·2=상승, 4·5=하락)과 실패 시 _EMPTY 폴백을 고정한다.
"""

from unittest.mock import AsyncMock, patch

import pytest

import market_indicators as mi

SUMMARY_UP = {
    "symbol": "A01609",
    "name": "F 202609",
    "current_price": 1332.2,
    "change": 32.35,
    "change_sign": "2",
    "change_rate": 2.49,
    "previous_close": 1329.7,
}


def test_parse_night_futures_up():
    out = mi._parse_night_futures_summary(SUMMARY_UP)
    assert out == {
        "value": "1,332.20",
        "change": "32.35",
        "change_pct": "2.49%",
        "direction": "up",
    }


def test_parse_night_futures_down_sign_code_and_negative_values():
    # KIS 부호 코드 5=하락 — change/change_rate 가 음수로 와도 절대값 표기.
    out = mi._parse_night_futures_summary(
        {"current_price": 1290.0, "change": -10.5, "change_sign": "5", "change_rate": -0.81}
    )
    assert out["value"] == "1,290.00"
    assert out["change"] == "10.50"
    assert out["change_pct"] == "0.81%"
    assert out["direction"] == "down"


def test_parse_night_futures_direction_falls_back_to_change_sign():
    # 부호 코드가 비어 있으면 change 부호로 방향 결정.
    out = mi._parse_night_futures_summary({"current_price": 1300.0, "change": -1.0, "change_rate": 0.08})
    assert out["direction"] == "down"
    # 보합(3) 은 방향 없음.
    flat = mi._parse_night_futures_summary({"current_price": 1300.0, "change": 0, "change_sign": "3", "change_rate": 0})
    assert flat["direction"] == ""


def test_parse_night_futures_missing_price_is_empty():
    assert mi._parse_night_futures_summary({}) == mi._EMPTY
    assert mi._parse_night_futures_summary({"current_price": "n/a"}) == mi._EMPTY


@pytest.mark.asyncio
async def test_fetch_night_futures_uses_kis_proxy():
    with patch.object(
        mi.kis_proxy_client,
        "get_night_futures_quote",
        new=AsyncMock(return_value={"summary": SUMMARY_UP}),
    ) as proxy:
        out = await mi._fetch_night_futures()
    proxy.assert_awaited_once()
    assert out["value"] == "1,332.20"
    assert out["direction"] == "up"


@pytest.mark.asyncio
async def test_fetch_night_futures_falls_back_to_empty_on_proxy_error():
    with patch.object(
        mi.kis_proxy_client,
        "get_night_futures_quote",
        new=AsyncMock(side_effect=RuntimeError("proxy down")),
    ):
        out = await mi._fetch_night_futures()
    assert out == mi._EMPTY
