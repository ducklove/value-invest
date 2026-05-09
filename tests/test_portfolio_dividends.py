from services.portfolio import dividends


def test_dividend_warmup_targets_include_common_stock_for_preferred_share():
    assert dividends.dividend_warmup_targets("33637K") == ["33637K", "336370"]


def test_dividend_warmup_targets_ignore_non_domestic_assets():
    assert dividends.dividend_warmup_targets("AAPL") == []
    assert dividends.dividend_warmup_targets("CASH_USD") == []


def test_due_dividend_warmup_targets_deduplicates_and_honors_ttl():
    last = {"005930": 95.0}

    due = dividends.due_dividend_warmup_targets(
        ["005930", "005930", "33637K"],
        now=100.0,
        last_warmup=last,
        ttl_seconds=10.0,
    )

    assert due == ["33637K", "336370"]
    assert last == {"005930": 95.0}


def test_due_dividend_warmup_targets_skips_running_codes():
    due = dividends.due_dividend_warmup_targets(
        ["33637K"],
        now=100.0,
        last_warmup={},
        running_codes={"336370"},
        ttl_seconds=10.0,
    )

    assert due == ["33637K"]
