from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

import snapshot_intraday

ROOT = Path(__file__).resolve().parents[1]


def test_snapshot_intraday_does_not_import_portfolio_route_private_helpers():
    source = (ROOT / "snapshot_intraday.py").read_text(encoding="utf-8")

    assert "from routes.portfolio import" not in source


@pytest.mark.asyncio
async def test_fetch_total_value_uses_prior_stock_snapshot_for_non_korean_quote_missing():
    fetch_quote = AsyncMock(return_value={})
    with patch.object(
        snapshot_intraday.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "AAPL", "quantity": 2, "avg_price": 1000},
        ]),
    ), patch.object(
        snapshot_intraday.snapshots_repo,
        "get_stock_snapshots_by_date",
        new=AsyncMock(return_value=[
            {"stock_code": "AAPL", "market_value": 1234},
        ]),
    ), patch.object(
        snapshot_intraday.portfolio_quotes,
        "is_korean_stock",
        return_value=False,
    ), patch.object(
        snapshot_intraday.portfolio_quotes,
        "fetch_quote",
        new=fetch_quote,
    ):
        total = await snapshot_intraday._fetch_total_value("u1", "2026-05-18")

    fetch_quote.assert_awaited_once_with("AAPL")
    assert total == 1234


@pytest.mark.asyncio
async def test_fetch_total_value_refuses_korean_stock_snapshot_fallback_when_quote_missing():
    fetch_quote = AsyncMock(return_value={})
    with patch.object(
        snapshot_intraday.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000},
        ]),
    ), patch.object(
        snapshot_intraday.snapshots_repo,
        "get_stock_snapshots_by_date",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "market_value": 1234},
        ]),
    ), patch.object(
        snapshot_intraday.portfolio_quotes,
        "is_korean_stock",
        return_value=True,
    ), patch.object(
        snapshot_intraday.portfolio_quotes,
        "fetch_quote",
        new=fetch_quote,
    ):
        with pytest.raises(snapshot_intraday.IntradaySnapshotIncomplete):
            await snapshot_intraday._fetch_total_value("u1", "2026-05-18")

    fetch_quote.assert_awaited_once_with(
        "005930",
        force_refresh=True,
        use_ws_cache=False,
    )


@pytest.mark.asyncio
async def test_fetch_total_value_ignores_stale_quote_when_snapshot_fallback_exists():
    with patch.object(
        snapshot_intraday.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "CASH_USD", "quantity": 10, "avg_price": 1400},
        ]),
    ), patch.object(
        snapshot_intraday.snapshots_repo,
        "get_stock_snapshots_by_date",
        new=AsyncMock(return_value=[
            {"stock_code": "CASH_USD", "market_value": 15190},
        ]),
    ), patch.object(
        snapshot_intraday.portfolio_quotes,
        "is_korean_stock",
        return_value=False,
    ), patch.object(
        snapshot_intraday.portfolio_quotes,
        "fetch_quote",
        new=AsyncMock(return_value={"price": 9999, "_stale": True}),
    ):
        total = await snapshot_intraday._fetch_total_value("u1", "2026-05-18")

    assert total == 15190


@pytest.mark.asyncio
async def test_fetch_total_value_refuses_avg_price_fallback_without_snapshot():
    with patch.object(
        snapshot_intraday.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000},
        ]),
    ), patch.object(
        snapshot_intraday.snapshots_repo,
        "get_stock_snapshots_by_date",
        new=AsyncMock(return_value=[]),
    ), patch.object(
        snapshot_intraday.portfolio_quotes,
        "is_korean_stock",
        return_value=True,
    ), patch.object(
        snapshot_intraday.portfolio_quotes,
        "fetch_quote",
        new=AsyncMock(return_value={}),
    ):
        with pytest.raises(snapshot_intraday.IntradaySnapshotIncomplete):
            await snapshot_intraday._fetch_total_value("u1", "2026-05-18")


# ---------------------------------------------------------------------------
# O10a/X11 — 장중 틱은 사용자 합집합 코드를 한 번만 조회해 공유한다.
# ---------------------------------------------------------------------------

_PORTFOLIOS = {
    "userA": [
        {"stock_code": "005930", "quantity": 10, "avg_price": 1000},
        {"stock_code": "000660", "quantity": 2, "avg_price": 1000},
    ],
    "userB": [
        {"stock_code": "005930", "quantity": 3, "avg_price": 1000},
        {"stock_code": "AAPL", "quantity": 4, "avg_price": 1000},
    ],
    "userC": [
        {"stock_code": "000660", "quantity": 1, "avg_price": 1000},
        {"stock_code": "005930", "quantity": 1, "avg_price": 1000},
        {"stock_code": "AAPL", "quantity": 1, "avg_price": 1000},
    ],
}
_QUOTES = {"005930": {"price": 80000.0}, "000660": {"price": 200000.0}, "AAPL": {"price": 300000.0}}


def _is_korean(code):
    return code.isdigit()


async def _run_tick(*, bulk, fetch_quote, trading_day=True):
    saved = {}

    async def save(google_sub, ts, total):
        saved[google_sub] = total

    get_portfolio = AsyncMock(side_effect=lambda sub: [dict(item) for item in _PORTFOLIOS[sub]])
    with patch.object(snapshot_intraday.snapshots_repo, "delete_old_intraday", new=AsyncMock()), \
         patch.object(snapshot_intraday.snapshots_repo, "get_all_users_with_portfolio",
                      new=AsyncMock(return_value=list(_PORTFOLIOS))), \
         patch.object(snapshot_intraday.snapshots_repo, "get_stock_snapshots_by_date", new=AsyncMock(return_value=[])), \
         patch.object(snapshot_intraday.snapshots_repo, "save_intraday_snapshot", new=save), \
         patch.object(snapshot_intraday.portfolio_repo, "get_portfolio", new=get_portfolio), \
         patch.object(snapshot_intraday.portfolio_quotes, "is_korean_stock", side_effect=_is_korean), \
         patch.object(snapshot_intraday.portfolio_quotes, "kr_trading_day", return_value=trading_day), \
         patch.object(snapshot_intraday.portfolio_quotes, "fetch_bulk_kr_quotes", new=bulk), \
         patch.object(snapshot_intraday.portfolio_quotes, "fetch_quote", new=fetch_quote), \
         patch("observability.record_event", new=AsyncMock()):
        await snapshot_intraday.run(manage_db=False)
    return saved, get_portfolio


async def _legacy_totals(fetch_quote):
    """종전 경로: 사용자마다 _fetch_total_value 가 보유 종목을 하나씩 조회."""
    totals = {}
    with patch.object(snapshot_intraday.portfolio_repo, "get_portfolio",
                      new=AsyncMock(side_effect=lambda sub: [dict(item) for item in _PORTFOLIOS[sub]])), \
         patch.object(snapshot_intraday.snapshots_repo, "get_stock_snapshots_by_date", new=AsyncMock(return_value=[])), \
         patch.object(snapshot_intraday.portfolio_quotes, "is_korean_stock", side_effect=_is_korean), \
         patch.object(snapshot_intraday.portfolio_quotes, "fetch_quote", new=fetch_quote), \
         patch.object(snapshot_intraday.asyncio, "sleep", new=AsyncMock()):
        for sub in _PORTFOLIOS:
            totals[sub] = await snapshot_intraday._fetch_total_value(sub)
    return totals


@pytest.mark.asyncio
async def test_intraday_tick_fetches_shared_codes_once_across_users():
    bulk = AsyncMock(return_value={code: dict(q) for code, q in _QUOTES.items() if code.isdigit()})
    fetch_quote = AsyncMock(side_effect=lambda code, **_: dict(_QUOTES[code]))

    saved, get_portfolio = await _run_tick(bulk=bulk, fetch_quote=fetch_quote)

    # 3명이 공유하는 국내 코드는 벌크 1회, 해외(AAPL)는 개별 1회.
    bulk.assert_awaited_once()
    assert sorted(bulk.await_args.args[0]) == ["000660", "005930"]
    assert [c.args[0] for c in fetch_quote.await_args_list] == ["AAPL"]
    # 보유종목은 사용자당 한 번만 읽는다.
    assert get_portfolio.await_count == 3
    assert set(saved) == set(_PORTFOLIOS)


@pytest.mark.asyncio
async def test_intraday_tick_nav_matches_legacy_per_user_path():
    legacy_fetch = AsyncMock(side_effect=lambda code, **_: dict(_QUOTES[code]))
    legacy = await _legacy_totals(legacy_fetch)
    # 종전 경로는 (사용자 × 보유) 만큼 조회했다.
    assert legacy_fetch.await_count == sum(len(items) for items in _PORTFOLIOS.values())

    bulk = AsyncMock(return_value={code: dict(q) for code, q in _QUOTES.items() if code.isdigit()})
    saved, _ = await _run_tick(bulk=bulk, fetch_quote=AsyncMock(side_effect=lambda code, **_: dict(_QUOTES[code])))

    assert saved == legacy
    assert saved["userA"] == 10 * 80000.0 + 2 * 200000.0


@pytest.mark.asyncio
async def test_intraday_tick_bulk_misses_use_forced_rest_on_trading_day():
    bulk = AsyncMock(return_value={"005930": dict(_QUOTES["005930"])})
    fetch_quote = AsyncMock(side_effect=lambda code, **_: dict(_QUOTES[code]))

    saved, _ = await _run_tick(bulk=bulk, fetch_quote=fetch_quote)

    calls = {c.args[0]: c.kwargs for c in fetch_quote.await_args_list}
    assert calls == {"000660": {"force_refresh": True, "use_ws_cache": False}, "AAPL": {}}
    assert saved == await _legacy_totals(AsyncMock(side_effect=lambda code, **_: dict(_QUOTES[code])))


@pytest.mark.asyncio
async def test_intraday_tick_on_holiday_never_forces_rest_refresh():
    bulk = AsyncMock(return_value={})  # 벌크 전부 결측 → 전부 개별 경로
    fetch_quote = AsyncMock(side_effect=lambda code, **_: dict(_QUOTES[code]))

    saved, _ = await _run_tick(bulk=bulk, fetch_quote=fetch_quote, trading_day=False)

    assert fetch_quote.await_args_list
    assert [c for c in fetch_quote.await_args_list if c.kwargs.get("force_refresh")] == []
    assert sorted(c.args[0] for c in fetch_quote.await_args_list) == ["000660", "005930", "AAPL"]
    assert set(saved) == set(_PORTFOLIOS)


@pytest.mark.asyncio
async def test_intraday_tick_falls_back_per_user_when_shared_map_fails():
    fetch_quote = AsyncMock(side_effect=lambda code, **_: dict(_QUOTES[code]))
    with patch.object(snapshot_intraday, "build_shared_quote_map", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch.object(snapshot_intraday.asyncio, "sleep", new=AsyncMock()):
        saved, _ = await _run_tick(bulk=AsyncMock(return_value={}), fetch_quote=fetch_quote)

    assert saved == await _legacy_totals(AsyncMock(side_effect=lambda code, **_: dict(_QUOTES[code])))


@pytest.mark.asyncio
async def test_fetch_total_value_treats_empty_shared_quote_as_missing():
    # 공유 맵의 {} (조회 실패)는 개별 조회 실패와 같게 결측 처리한다 — 재조회 없음.
    fetch_quote = AsyncMock()
    with patch.object(snapshot_intraday.snapshots_repo, "get_stock_snapshots_by_date",
                      new=AsyncMock(return_value=[{"stock_code": "AAPL", "market_value": 1234}])), \
         patch.object(snapshot_intraday.portfolio_quotes, "is_korean_stock", side_effect=_is_korean), \
         patch.object(snapshot_intraday.portfolio_quotes, "fetch_quote", new=fetch_quote):
        total = await snapshot_intraday._fetch_total_value(
            "u1",
            "2026-05-18",
            quote_map={"AAPL": {}, "005930": {"price": 100.0}},
            items=[{"stock_code": "AAPL", "quantity": 2}, {"stock_code": "005930", "quantity": 3}],
        )
        with pytest.raises(snapshot_intraday.IntradaySnapshotIncomplete):
            await snapshot_intraday._fetch_total_value(
                "u1", "2026-05-18", quote_map={"005930": {}}, items=[{"stock_code": "005930", "quantity": 3}]
            )

    fetch_quote.assert_not_awaited()
    assert total == 1234 + 300.0
