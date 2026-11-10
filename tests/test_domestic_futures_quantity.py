import pytest

from repositories.broker_secrets import BrokerError
from services.brokers.derivatives import domestic_quantity


@pytest.mark.parametrize("previous,side,buys,sells,expected", [
    (100, "매도", 100, 0, 0),       # 매도 잔고 매수 청산
    (100, "매수", 0, 100, 0),       # 매수 잔고 매도 청산
    (100, "매도", 40, 0, -60),      # 매수 체결이 있어도 남은 잔고는 매도
    (100, "매수", 0, 40, 60),       # 매도 체결이 있어도 남은 잔고는 매수
    (100, "매도", 140, 0, 40),      # 당일 반대 방향으로 전환
    (100, "매수", 0, 140, -40),
    (0, "", 0, 100, -100),
    (0, "", 100, 0, 100),
    (50, "매도", 0, 0, -50),
    (50, "매수", 20, 10, 60),       # 양쪽 체결을 모두 반영
])
def test_signed_open_contracts_use_previous_direction_and_executions(previous, side, buys, sells, expected):
    row = {"bf_dd_ny_stl_qty": previous, "sby_dit_nm2": side,
           "byn_cns_qty": buys, "sll_cns_qty": sells, "bnc_ind_qty": abs(buys-sells),
           "sby_dit_nm": "매수" if buys > sells else "매도", "lqd_pbl_qty": 0}
    assert domestic_quantity(row, night=False) == expected


def test_missing_previous_direction_is_not_guessed_from_todays_trade():
    with pytest.raises(BrokerError):
        domestic_quantity({"bf_dd_ny_stl_qty": 100, "sby_dit_nm": "매수", "byn_cns_qty": 10, "sll_cns_qty": 0}, night=False)
