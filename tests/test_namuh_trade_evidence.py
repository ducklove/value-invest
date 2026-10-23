from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from repositories.broker_secrets import BrokerError
from services.brokers import cash_trades, namuh

LINK = {"account_no": "12345678901", "credential_id": "test"}


def total(buy=1000000, sell=0, charges=500, **extra):
    return {"tdt_byn_amt": buy, "tdt_sll_amt": sell, "sdr_xps": charges,
            "nxt_dd_dca": 10000000, "nxt2_dd_dca": 10000000 + sell - buy - charges, **extra}


async def fetch(row, pages=None, balances=None):
    with patch.object(namuh, "pages", AsyncMock(return_value=pages or [{"Output_0": row}])):
        return await cash_trades.fetch("u1", LINK, balances or {"KRW": row})


@pytest.mark.parametrize("buy,sell,charges", [(1000000, 0, 500), (0, 30000000, 65000), (2000000, 1000000, 3500), (0, 0, 0)])
async def test_actual_buy_sell_and_all_costs_match_settlement_cash(buy, sell, charges):
    rows = await fetch(total(buy, sell, charges))
    assert {r["kind"]: r["amount"] for r in rows} == {"trade_buy": -buy, "trade_sell": sell, "trade_charges": -charges}
    assert all(r["currency"] == "KRW" for r in rows)
    assert sum(r["amount"] for r in rows) == sell - buy - charges
    assert all("source_key" not in r and "serial" not in r for r in rows)


async def test_repeated_account_totals_across_pages_are_not_added():
    row = total()
    rows = await fetch(row, [{"Output_0": row}, {"Output_0": row}, {"Output_1": []}])
    assert sum(r["amount"] for r in rows) == -1000500


@pytest.mark.parametrize("extra", [{"sdr_xps": None}, {"tdt_byn_amt": "nan"}, {"tdt_sll_amt": -1},
                                   {"sdr_xps": -1}, {"nxt2_dd_dca": 9000000}, {"tdt_byn_amt": True}])
async def test_missing_invalid_or_inconsistent_totals_cannot_explain_cash(extra):
    with pytest.raises(BrokerError):
        await fetch(total(**extra))


async def test_new_execution_between_balance_and_trade_query_is_not_matched_to_stale_balance():
    with pytest.raises(BrokerError):
        await fetch(total(), balances={"KRW": total(buy=0, charges=0)})


async def test_failed_page_and_missing_totals_preserve_previous_evidence():
    for pages in ([{"Output_0": total()}, {"message": {"usr_msg": "조회 실패"}}], [{"Output_1": []}]):
        with pytest.raises(BrokerError):
            await fetch(total(), pages)


async def test_query_uses_read_only_actual_daily_costs_including_out_of_hours():
    with patch.object(namuh, "pages", AsyncMock(return_value=[{"Output_0": total()}])) as query:
        await cash_trades.fetch("u1", LINK, {"KRW": total()})
    args = query.await_args.args
    assert args[2] in namuh.READ_PATHS
    assert "/order/" not in args[2]
    assert args[3] == {"act_no": LINK["account_no"], "iqr_dit_cd1": "2", "fee_dit_cd": "1", "qut_dit_cd": "UNT", "aly_qut_cd": "2"}


async def test_midnight_during_query_does_not_put_yesterdays_trades_in_today():
    before = type("Clock", (), {"date": lambda self: date(2026, 10, 22)})()
    after = type("Clock", (), {"date": lambda self: date(2026, 10, 23)})()
    with patch.object(cash_trades, "datetime") as clock:
        clock.now.side_effect = [before, after]
        with pytest.raises(BrokerError):
            await fetch(total())
