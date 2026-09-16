from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from repositories.quant import QuantError, digest
from services.quant import rollover, scanner


def contract(month, stock="005930", suffix=""):
    return {"contract": f"K{stock}{month}{suffix}", "spot_code": stock, "name": "검증",
            "contract_name": f"검증 F {month} ( 10)", "market": "1"}


def selected(day, months=("202610", "202611", "202612")):
    return rollover.universe([contract(m) for m in reversed(months)], date.fromisoformat(day))


def test_one_month_per_underlying_and_switch_on_two_trading_days_before_expiry():
    before = selected("2026-10-05")  # 월요일 대체공휴일
    row = before["contracts"][0]
    assert row["delivery_month"] == "202610"
    assert row["expiry"] == "20261008" and row["roll_on"] == "2026-10-06"
    for day in ("2026-10-06", "2026-10-07", "2026-10-08"):
        row = selected(day)["contracts"][0]
        assert row["delivery_month"] == "202611" and row["contract_role"] == "next"
        assert row["rolled_from"] == contract("202610")["contract"]
    # 만기를 지난 날에 한 번 더 전환하면 안 된다.
    assert selected("2026-10-09")["contracts"][0]["delivery_month"] == "202611"
    both = rollover.universe([contract(m, s) for s in ("005930", "000660")
                              for m in ("202610", "202611", "202612")], date(2026, 9, 16))
    assert len(both["contracts"]) == 2 and both["source_contracts"] == 6
    assert len({r["spot_code"] for r in both["contracts"]}) == 2


def test_holidays_shift_expiry_and_roll_across_weekends_and_new_year():
    # 2027 설 연휴 월·화 휴장: 목요일 만기의 2거래일 전은 전주 금요일이다.
    assert rollover.schedule("202702") == (date(2027, 2, 11), date(2027, 2, 5))
    # 두 번째 목요일이 부처님오신날인 경우 최종거래일도 앞당긴다.
    assert rollover.schedule("202705") == (date(2027, 5, 12), date(2027, 5, 10))
    row = selected("2026-12-08", ("202612", "202701"))["contracts"][0]
    assert row["delivery_month"] == "202701" and row["expiry"] == "20270114"
    assert not rollover.trading_day(date(2026, 7, 17))
    assert not rollover.trading_day(date(2026, 12, 31))
    assert rollover.previous_trading_day(date(2027, 1, 4)) == date(2026, 12, 30)
    with pytest.raises(QuantError, match="달력 미확인"):
        selected("2028-01-03", ("202801",))


def test_missing_successor_and_ambiguous_month_never_fall_back_to_old_contract():
    data = selected("2026-10-06", ("202610",))
    assert not data["contracts"] and "다음 월물" in data["excluded"][0]["reason"]
    rows = [contract("202610"), contract("202610", suffix="adjusted"), contract("202610", "000660")]
    data = rollover.universe(rows, date(2026, 9, 16))
    assert [r["spot_code"] for r in data["contracts"]] == ["000660"]
    assert data["excluded"][0]["spot_code"] == "005930"
    malformed = contract("202610") | {"contract_name": "검증 날짜 미확인"}
    assert not rollover.universe([malformed], date(2026, 9, 16))["contracts"]


def test_roll_invalidates_quotes_candidates_and_restart_cursor_without_mixing_history():
    old = selected("2026-10-02");new = selected("2026-10-06")
    row = old["contracts"][0];code = row["contract"]
    old_hash = digest(old["contracts"])
    observer = SimpleNamespace(rows={code: {**row, "catalog_hash": old_hash}}, selected={code: 1},
                               books={"old": {}}, active_signals={code}, public=lambda: None)
    progress = {"catalog_hash": old_hash, "cursor": 90, "rounds": 4}
    with patch.object(scanner.time, "time", return_value=1):
        contracts, new_hash = scanner.sync_universe(observer, new, progress)
    assert contracts[0]["delivery_month"] == "202611" and new_hash != old_hash
    assert not observer.rows and not observer.selected and not observer.books and not observer.active_signals
    assert progress["cursor"] == 0 and progress["rounds"] == 0 and progress["total"] == 1
    assert not rollover.active(row, date(2026, 10, 6))
    assert rollover.active(row, date(2026, 10, 2))
    assert not rollover.active(row, date(2026, 10, 5))  # 휴장일에는 호가가 와도 신호 제외
    assert not rollover.active({"expiry": "20261008"}, date(2026, 9, 16))  # 이전 버전 관측
