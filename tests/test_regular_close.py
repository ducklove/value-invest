from datetime import datetime
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import httpx
import pytest
from _harness import seed_user

from repositories import portfolio, settlement_inputs, snapshots
from repositories.db import transaction
from services.market.sources import yahoo
from services.portfolio import foreign, nav_link, regular_close, snapshot_views, time_windows

DAY = "2026-09-30"
CUTOFF = DAY + "T15:30:00.000"
NOW = datetime(2026, 9, 30, 15, 35, tzinfo=time_windows.KST)


@pytest.fixture(autouse=True)
def closing_fx():
    with patch.object(regular_close.fx, "fx_rate_for_currency", AsyncMock(return_value=1400)), \
         patch.object(regular_close.fx, "cached_rate_for_currency", return_value=1400):
        yield


async def seed():
    await seed_user()
    await portfolio.save_portfolio_item("u1", "005930", "주식", 10, 100, "KRW")
    await portfolio.save_portfolio_item("u1", "CASH_KRW", "현금", 10000, 1, "KRW")
    async with transaction() as db:
        await db.execute("UPDATE settlement_history_start SET started_at='2026-09-01T00:00:00.000'")
        await db.execute("UPDATE settlement_versions SET recorded_at='2026-09-30T15:29:00.000'")
    await snapshots.save_snapshot("u1", "2026-09-29", 11000, 11000, 1000, 11,
                                  cashflow_cutoff_at="2026-09-29T15:30:00", price_basis=regular_close.BASIS)
    async with transaction() as db:
        return (await (await db.execute("SELECT MAX(id) FROM settlement_versions")).fetchone())[0]


@pytest.mark.asyncio
async def test_close_reconstructs_holdings_before_after_hours_trade_and_retry_is_immutable(temp_db):
    marker = await seed()
    await portfolio.save_portfolio_item("u1", "005930", "주식", 20, 150, "KRW")
    await portfolio.save_portfolio_item("u1", "CASH_KRW", "현금", 8000, 1, "KRW")
    async with transaction() as db:
        await db.execute("UPDATE settlement_versions SET recorded_at='2026-09-30T15:32:00.000' WHERE id>?", (marker,))
    fetched = AsyncMock(return_value={"raw": {"stck_prpr": "200"}})
    with patch.object(time_windows, "now_kst", return_value=NOW), patch.object(regular_close.kis_proxy_client, "get_quote", fetched):
        await regular_close.settle("u1", DAY)
        fetched.assert_awaited_once_with("005930", market="J")
        await regular_close.settle("u1", DAY)
        assert fetched.await_count == 1
    row = await snapshots.get_snapshot_by_date("u1", DAY)
    assert row["total_value"] == 12000
    assert row["cashflow_cutoff_at"] == CUTOFF
    assert row["nav"] == pytest.approx(12000 / 11)
    stocks = await snapshots.get_stock_snapshots_exact_date("u1", DAY)
    assert next(s for s in stocks if s["stock_code"] == "005930")["quantity"] == 10
    assert (await portfolio.get_portfolio_item("u1", "005930"))["quantity"] == 20
    summary = await snapshot_views.regular_performance("u1", DAY)
    assert summary["change_krw"] == 1000


@pytest.mark.asyncio
async def test_uncollected_close_does_not_use_aftermarket_or_existing_daily_api(temp_db):
    await seed()
    with patch.object(time_windows, "now_kst", return_value=NOW.replace(hour=20)), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock()) as quote:
        with pytest.raises(ValueError, match="미수집"):
            await regular_close.settle("u1", DAY)
    quote.assert_not_awaited()
    assert await snapshots.get_snapshot_by_date("u1", DAY) is None


@pytest.mark.asyncio
async def test_retry_after_window_uses_saved_prices_not_new_quotes(temp_db):
    await seed()
    inputs = await settlement_inputs.load("u1", CUTOFF)
    with patch.object(time_windows, "now_kst", return_value=NOW), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": 200}})):
        await regular_close.collect_prices("u1", DAY, inputs)
    with patch.object(time_windows, "now_kst", return_value=NOW.replace(hour=20)), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock()) as quote:
        await regular_close.settle("u1", DAY)
    quote.assert_not_awaited()
    assert (await snapshots.get_snapshot_by_date("u1", DAY))["total_value"] == 12000


@pytest.mark.asyncio
async def test_fixed_dollar_performance_uses_both_closing_exchange_rates(temp_db):
    await seed()
    async with transaction() as db:
        await db.execute("UPDATE portfolio_snapshots SET fx_usdkrw=1300 WHERE google_sub='u1'")
    with patch.object(time_windows, "now_kst", return_value=NOW), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": 200}})):
        await regular_close.settle("u1", DAY)
    summary = await snapshot_views.regular_performance("u1", DAY)
    assert summary["change_usd"] == pytest.approx(12000 / 1400 - 11000 / 1300)
    assert summary["change_usd_pct"] == pytest.approx((12000 / 1400 / (11000 / 1300) - 1) * 100)


@pytest.mark.asyncio
async def test_optional_display_fx_failure_does_not_block_krw_only_settlement(temp_db):
    await seed()
    with patch.object(time_windows, "now_kst", return_value=NOW), \
         patch.object(regular_close.fx, "fx_rate_for_currency", AsyncMock(side_effect=ValueError("FX outage"))), \
         patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": 200}})):
        await regular_close.settle("u1", DAY)
    summary = await snapshot_views.regular_performance("u1", DAY)
    assert summary["change_krw"] == 1000
    assert summary["change_usd_pct"] is None


@pytest.mark.asyncio
async def test_history_before_installation_is_not_invented(temp_db):
    await seed_user()
    with pytest.raises(ValueError, match="변경 이력이 없습니다"):
        await settlement_inputs.load("u1", "2020-01-01T15:30:00")


@pytest.mark.asyncio
async def test_after_close_deposit_is_not_issued_until_next_settlement(temp_db):
    marker = await seed()
    await snapshots.add_cashflow_and_sync_cash("u1", DAY, "deposit", 5000, None, None, None)
    async with transaction() as db:
        await db.execute("UPDATE portfolio_cashflows SET created_at='2026-09-30T15:32:00'")
        await db.execute("UPDATE settlement_versions SET recorded_at='2026-09-30T15:32:00.000' WHERE id>?", (marker,))
    with patch.object(time_windows, "now_kst", return_value=NOW), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": 200}})):
        await regular_close.settle("u1", DAY)
    current = await snapshots.get_snapshot_by_date("u1", DAY)
    assert current["total_value"] == 12000
    assert current["total_units"] == 11
    assert (await snapshots.get_cashflows("u1"))[0]["applied_snapshot_date"] is None
    assert (await snapshot_views.regular_performance("u1", DAY))["after_close_net_cashflow"] == 5000


@pytest.mark.asyncio
async def test_legacy_segment_preserved_without_fake_transition_return(temp_db):
    await seed()
    async with transaction() as db:
        await db.execute("UPDATE portfolio_snapshots SET price_basis='legacy_latest'")
    with patch.object(time_windows, "now_kst", return_value=NOW), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": 200}})):
        await regular_close.settle("u1", DAY)
    raw = await nav_link.get_nav_history("u1", include_legacy=True)
    assert [r["nav"] for r in raw] == [1000, 1000]  # 새 기준 첫 날은 NAV 1,000 재시작
    assert not any(r.get("linked") for r in raw)
    linked = await nav_link.get_nav_history("u1")
    assert len(linked) == 2 and linked[0]["linked"] and linked[0]["price_basis"] == "legacy_latest"
    # 입출금 없는 전환: 연결된 일간 NAV 수익률 = 평가액 수익률 12000/11000 − 1
    assert linked[1]["return_nav"] / linked[0]["return_nav"] == pytest.approx(12000 / 11000)
    assert (await snapshot_views.regular_performance("u1", DAY))["change_pct"] is None
    assert (await snapshots.get_snapshot_by_date("u1", "2026-09-29"))["nav"] == 1000


def test_holiday_and_explicit_close_override(monkeypatch):
    assert regular_close.closing_at("2026-10-05") is None
    with pytest.raises(ValueError, match="수능일"):
        regular_close.closing_at("2026-11-19")
    monkeypatch.setenv("PORTFOLIO_MARKET_SESSIONS", '{"2026-11-19":"16:30"}')
    assert regular_close.closing_at("2026-11-19").hour == 16


@pytest.mark.asyncio
async def test_cancelling_unsettled_flow_next_morning_keeps_yesterdays_closing_ledger(temp_db):
    marker = await seed()
    with patch.object(snapshots, "datetime") as clock:
        clock.now.return_value = NOW.replace(hour=15, minute=20)
        flow = await snapshots.add_cashflow_and_sync_cash("u1", DAY, "deposit", 1000, None, None, None)
    async with transaction() as db:
        await db.execute("UPDATE settlement_versions SET recorded_at='2026-09-30T15:29:00.000' WHERE id>?", (marker,))
    with patch.object(snapshots, "datetime") as clock:
        clock.now.return_value = NOW.replace(month=10, day=1, hour=10)
        await snapshots.delete_cashflow_and_sync_cash("u1", flow["id"])
    flows = await snapshots.get_cashflows("u1")
    assert len(flows) == 2
    assert next(r for r in flows if r["id"] == flow["id"])["cancelled_at"]
    assert next(r for r in flows if r["reversal_of_id"] == flow["id"])["type"] == "withdrawal"
    frozen = await settlement_inputs.load("u1", CUTOFF)
    assert frozen["portfolio_cashflows"][0]["id"] == flow["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("hour,delivered", [(15, True), (16, False)])
async def test_afternoon_batch_uses_only_the_matching_close_window(hour, delivered):
    from services import daily_briefing
    now = NOW.replace(hour=hour, minute=45)
    with patch.object(time_windows, "now_kst", return_value=now), \
         patch.object(daily_briefing, "opted_in_users", AsyncMock(return_value=["u1"])), \
         patch.object(daily_briefing.channels, "has_active_channel", AsyncMock(return_value=True)), \
         patch.object(daily_briefing.channels, "dispatch", AsyncMock(return_value=1)) as dispatch, \
         patch.object(daily_briefing.snapshots_repo, "get_snapshot_by_date", AsyncMock(return_value={"price_basis": regular_close.BASIS})), \
         patch.object(daily_briefing, "generate_briefing", AsyncMock(return_value={"text": "확정 성과", "source": "template", "stats": {"ok": True}})), \
         patch("observability.record_event", AsyncMock()):
        result = await daily_briefing.send_briefings("market_close")
    assert result["sent"] == int(delivered)
    assert dispatch.await_count == int(delivered)


@pytest.mark.asyncio
async def test_afternoon_batch_does_not_send_unsettled_performance():
    from services import daily_briefing
    with patch.object(time_windows, "now_kst", return_value=NOW.replace(minute=45)), \
         patch.object(daily_briefing, "opted_in_users", AsyncMock(return_value=["u1"])), \
         patch.object(daily_briefing.channels, "has_active_channel", AsyncMock(return_value=True)), \
         patch.object(daily_briefing.channels, "dispatch", AsyncMock()) as dispatch, \
         patch.object(daily_briefing.snapshots_repo, "get_snapshot_by_date", AsyncMock(return_value=None)), \
         patch("observability.record_event", AsyncMock()):
        result = await daily_briefing.send_briefings("market_close")
    assert result["failed"] == 1
    dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_foreign_close_excludes_active_exchange_session_and_postmarket():
    cutoff = NOW
    regular_end = int(datetime(2026, 9, 30, 17, tzinfo=time_windows.KST).timestamp())
    data = {"currency": "HKD", "meta": {"exchangeTimezoneName": "Asia/Hong_Kong", "currentTradingPeriod": {"regular": {"end": regular_end}}},
            "rows": [{"date": "2026-09-29", "close": 100}, {"date": DAY, "close": 999}]}
    with patch.object(regular_close.foreign, "ensure_ticker_map", AsyncMock()), patch.object(regular_close.foreign, "fetch_yahoo_chart", AsyncMock(return_value=data)):
        result = await regular_close.foreign_close("0005.HK", cutoff)
    assert result["native_price"] == 100
    assert result["price_date"] == "2026-09-29"


@pytest.mark.asyncio
async def test_exchange_local_date_prevents_australian_active_bar_from_becoming_yesterdays_close():
    data = {"currency": "AUD", "meta": {"exchangeTimezoneName": "Australia/Sydney", "currentTradingPeriod": {"regular": {"end": int(NOW.timestamp())}}},
            "rows": [{"date": "2026-09-28", "session_date": "2026-09-29", "close": 100},
                     {"date": "2026-09-29", "session_date": DAY, "close": 999}]}
    with patch.object(regular_close.foreign, "ensure_ticker_map", AsyncMock()), patch.object(regular_close.foreign, "fetch_yahoo_chart", AsyncMock(return_value=data)):
        result = await regular_close.foreign_close("A200.AX", NOW.replace(hour=10))
    assert result["native_price"] == 100


@pytest.mark.asyncio
async def test_aftermarket_capture_preserves_nav_and_is_repeatable(temp_db):
    from services.portfolio import after_close
    await seed()
    with patch.object(time_windows, "now_kst", return_value=NOW), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": 200}})):
        await regular_close.settle("u1", DAY)
    before = await snapshots.get_snapshot_by_date("u1", DAY)
    with patch.object(time_windows, "now_kst", return_value=NOW.replace(hour=20, minute=5)), patch.object(after_close.runtime_quotes, "fetch_quote", AsyncMock(return_value={"price": 210, "date": DAY})) as quote:
        result = await after_close.capture("u1")
        assert result["holdings"][0]["change_pct"] == pytest.approx(5)
        assert result["holdings"][0]["contribution"] == 100
        assert await after_close.capture("u1") == result
        assert quote.await_count == 1
    assert await snapshots.get_snapshot_by_date("u1", DAY) == before


@pytest.mark.asyncio
async def test_quality_checks_closing_holdings_without_requiring_after_hours_purchase_in_nav(temp_db):
    from services import data_quality
    await seed()
    with patch.object(time_windows, "now_kst", return_value=NOW), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": 200}})):
        await regular_close.settle("u1", DAY)
    await portfolio.save_portfolio_item("u1", "000660", "장후 신규 매수", 1, 100, "KRW")
    async with transaction() as db:
        await db.execute("UPDATE settlement_versions SET recorded_at='2026-09-30T16:00:00.000' WHERE row_key LIKE '%000660'")
    result = await data_quality.check_portfolio_stock_snapshot_freshness(NOW.replace(hour=20))
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_history_rebuild_without_sources_never_changes_original(temp_db):
    import sqlite3

    from scripts.rebuild_regular_close_history import fingerprint, rebuild
    await seed()
    with sqlite3.connect(temp_db) as db:
        before = fingerprint(db)
    result = await rebuild(temp_db, "2026-09-29", apply=True)
    assert result["blocked"]
    assert not result["applied"]
    assert result["backup"]
    with sqlite3.connect(temp_db) as db:
        assert fingerprint(db) == before


@pytest.mark.asyncio
async def test_history_rebuild_on_copy_and_apply_match_original_regular_values(temp_db):
    from scripts.rebuild_regular_close_history import rebuild
    await seed()
    with patch.object(time_windows, "now_kst", return_value=NOW), patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": 200}})):
        await regular_close.settle("u1", DAY)
    before = await snapshots.get_snapshot_by_date("u1", DAY)
    result = await rebuild(temp_db, DAY, apply=True)
    assert result["applied"]
    assert not result["blocked"]
    assert await snapshots.get_snapshot_by_date("u1", DAY) == before


def _yahoo_daily_chart(currency: str, zone: str, gmtoffset: int, regular_end: datetime, bars: list[tuple[datetime, float]]) -> dict:
    return {"chart": {"result": [{
        "meta": {"currency": currency, "exchangeTimezoneName": zone, "gmtoffset": gmtoffset,
                 "currentTradingPeriod": {"regular": {"end": int(regular_end.timestamp())}}},
        "timestamp": [int(at.timestamp()) for at, _ in bars],
        "indicators": {"quote": [{"close": [close for _, close in bars]}]},
    }], "error": None}}


@pytest.mark.asyncio
async def test_reuters_suffixed_foreign_holdings_settle_from_mapped_yahoo_symbols(temp_db):
    """허브가 Reuters/네이버 표기(AGNC.O, FUEVFVND.HM)로 저장한 해외 보유종목은
    Yahoo 심볼(AGNC, FUEVFVND.VN)로 완료된 정규장 일봉 종가를 받는다 — 예전에는
    AGNC.O 그대로 요청해 404 → '해외 정규장 종료 시각 누락'으로 정산이 실패했다."""
    marker = await seed()
    await portfolio.save_portfolio_item("u1", "AGNC.O", "AGNC Investment", 10, 10, "USD")
    await portfolio.save_portfolio_item("u1", "FUEVFVND.HM", "DCVFMVN Diamond ETF", 100, 30000, "VND")
    async with transaction() as db:
        await db.execute("UPDATE settlement_versions SET recorded_at='2026-09-30T15:29:00.000' WHERE id>?", (marker,))

    new_york, saigon = ZoneInfo("America/New_York"), ZoneInfo("Asia/Ho_Chi_Minh")
    charts = {
        # 뉴욕 9/30 정규장은 아직 열리지도 않았다 → 9/29 종가가 완료된 최신 종가.
        "AGNC": _yahoo_daily_chart("USD", "America/New_York", -14400, datetime(2026, 9, 30, 16, tzinfo=new_york), [
            (datetime(2026, 9, 28, 9, 30, tzinfo=new_york), 14.10),
            (datetime(2026, 9, 29, 9, 30, tzinfo=new_york), 14.25),
        ]),
        # 호찌민 9/30 정규장(15:00 ICT = 17:00 KST)은 정산 시각에 진행 중 → 제외.
        "FUEVFVND.VN": _yahoo_daily_chart("VND", "Asia/Ho_Chi_Minh", 25200, datetime(2026, 9, 30, 15, tzinfo=saigon), [
            (datetime(2026, 9, 29, 9, tzinfo=saigon), 33800.0),
            (datetime(2026, 9, 30, 9, tzinfo=saigon), 99999.0),
        ]),
    }
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        symbol = request.url.path.rsplit("/", 1)[-1]
        requested.append(symbol)
        return httpx.Response(200, json=charts[symbol]) if symbol in charts else httpx.Response(404)

    yahoo.reset_rate_limit_state()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        # 운영 사례처럼 ticker_map 에 네이버 reutersCode 가 그대로 저장돼 있어도 매핑된다.
        with patch.dict(foreign._ticker_map, {"AGNC.O": "AGNC.O"}, clear=True), \
             patch.object(foreign, "ensure_ticker_map", AsyncMock()), \
             patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)), \
             patch.object(time_windows, "now_kst", return_value=NOW), \
             patch.object(regular_close.kis_proxy_client, "get_quote", AsyncMock(return_value={"raw": {"stck_prpr": "200"}})):
            await regular_close.settle("u1", DAY)

    assert sorted(requested) == ["AGNC", "FUEVFVND.VN"]
    saved = (await settlement_inputs.prices("u1", DAY))["prices"]
    assert saved["AGNC.O"]["native_price"] == 14.25 and saved["AGNC.O"]["price_date"] == "2026-09-29"
    assert saved["AGNC.O"]["price"] == pytest.approx(14.25 * 1400)
    assert saved["FUEVFVND.HM"]["native_price"] == 33800.0 and saved["FUEVFVND.HM"]["price_date"] == "2026-09-29"
    assert saved["FUEVFVND.HM"]["currency"] == "VND"
    row = await snapshots.get_snapshot_by_date("u1", DAY)
    assert row["total_value"] == pytest.approx(10 * 200 + 10000 + 10 * 14.25 * 1400 + 100 * 33800.0 * 1400)
