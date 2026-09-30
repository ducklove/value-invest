"""Single DART list.json client: cache, quota guard, tape time gate (X3/O4)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import dart_report_review
from services.dart import client as dart_client
from services.market import daily as market_daily

KST = timezone(timedelta(hours=9))


class FakeDart:
    """Stands in for the shared 'dart' httpx client; records list.json calls."""

    def __init__(self, responder=None):
        self.calls: list[dict] = []
        self.responder = responder or (lambda params: httpx.Response(200, json={"status": "013"}))

    async def get(self, url, params=None, timeout=None):
        assert url.endswith("/list.json")
        self.calls.append(dict(params or {}))
        await asyncio.sleep(0)
        return self.responder(params or {})


def _ok(items):
    return lambda params: httpx.Response(200, json={"status": "000", "list": items})


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setenv("OPENDART_API_KEY", "test-key")
    dart_client._list_cache.clear()
    dart_client._quota.reset()
    yield
    dart_client._list_cache.clear()
    dart_client._quota.reset()


def _use(fake: FakeDart):
    async def get_client(name="default"):
        assert name == "dart"
        return fake

    return patch.object(dart_client, "get_http_client", side_effect=get_client)


async def test_repeated_queries_within_ttl_hit_upstream_once():
    fake = FakeDart(_ok([{"rcept_no": "1", "report_nm": "주요사항보고서"}]))
    with _use(fake):
        first = await dart_client.fetch_filing_list("0001", "20260930", "20260930", page_count=20)
        second = await dart_client.fetch_filing_list("0001", "20260930", "20260930", page_count=20)
        await dart_client.fetch_filing_list("0002", "20260930", "20260930", page_count=20)
    assert first == second == [{"rcept_no": "1", "report_nm": "주요사항보고서"}]
    assert [c["corp_code"] for c in fake.calls] == ["0001", "0002"]
    assert fake.calls[0]["crtfc_key"] == "test-key"
    assert fake.calls[0]["page_count"] == "20"
    assert dart_client.quota_status()["calls"] == 2


async def test_concurrent_identical_queries_share_one_call():
    fake = FakeDart(_ok([]))
    with _use(fake):
        await asyncio.gather(*(dart_client.fetch_filing_list("0001", "20260930", "20260930") for _ in range(6)))
    assert len(fake.calls) == 1


async def test_status_errors_are_typed_and_not_cached():
    fake = FakeDart(lambda params: httpx.Response(200, json={"status": "011", "message": "사용할 수 없는 키"}))
    with _use(fake):
        with pytest.raises(dart_client.DartListError) as err:
            await dart_client.fetch_filing_list("0001", "20260930", "20260930")
        assert err.value.dart_status == "011"
        with pytest.raises(dart_client.DartListError):
            await dart_client.fetch_filing_list("0001", "20260930", "20260930")
    assert len(fake.calls) == 2

    fake = FakeDart(lambda params: httpx.Response(503, text="busy"))
    with _use(fake):
        with pytest.raises(dart_client.DartListError) as err:
            await dart_client.fetch_filing_list("0001", "20260930", "20260930")
    assert err.value.http_status == 503


async def test_status_020_blocks_list_calls_until_kst_midnight():
    fake = FakeDart(lambda params: httpx.Response(200, json={"status": "020", "message": "요청 제한을 초과"}))
    with _use(fake), patch("observability.record_event", new=AsyncMock()) as record:
        with pytest.raises(dart_client.DartQuotaError):
            await dart_client.fetch_filing_list("0001", "20260930", "20260930")
        await asyncio.sleep(0)
        # Later calls short-circuit without touching OpenDART.
        with pytest.raises(dart_client.DartQuotaError) as err:
            await dart_client.fetch_filing_list("0002", "20260930", "20260930")
        assert err.value.dart_status == "020"
        assert len(fake.calls) == 1
        assert dart_client.quota_status()["exhausted"] is True
        record.assert_awaited_once()
        assert record.await_args.args[:2] == ("dart", "quota_exhausted")

        fake.responder = _ok([])
        with patch.object(dart_client, "_kst_today", return_value="29991231"):
            assert await dart_client.fetch_filing_list("0002", "20260930", "20260930") == []
    assert len(fake.calls) == 2


async def test_nonessential_calls_stop_near_daily_budget():
    fake = FakeDart(_ok([]))
    dart_client._quota._roll()
    dart_client._quota.calls = dart_client.NONESSENTIAL_BUDGET
    with _use(fake):
        with pytest.raises(dart_client.DartQuotaError):
            await dart_client.fetch_filing_list("0001", "20260930", "20260930", essential=False)
        assert fake.calls == []
        assert await dart_client.fetch_filing_list("0001", "20260930", "20260930") == []
    assert len(fake.calls) == 1


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [(6, 59, False), (7, 0, True), (13, 30, True), (19, 59, True), (20, 0, False), (23, 0, False)],
)
def test_disclosure_time_gate(hour, minute, expected):
    moment = datetime(2026, 9, 30, hour, minute, tzinfo=KST)
    assert dart_client.in_disclosure_hours(moment) is expected
    assert dart_client.in_disclosure_hours(moment.astimezone(timezone.utc)) is expected


async def test_recent_disclosures_maps_items_and_swallows_errors():
    fake = FakeDart(_ok([{"rcept_no": "9", "report_nm": "공시", "rcept_dt": "20260930", "corp_name": "A", "flr_nm": "x"}]))
    with _use(fake):
        items = await dart_client.fetch_recent_disclosures("0001")
    assert items == [{"rcept_no": "9", "report_nm": "공시", "rcept_dt": "20260930", "corp_name": "A"}]
    assert fake.calls[0]["sort"] == "date" and fake.calls[0]["sort_mth"] == "desc"

    dart_client._list_cache.clear()
    with _use(FakeDart(lambda params: httpx.Response(500))):
        assert await dart_client.fetch_recent_disclosures("0001") == []


async def test_annual_report_dates_go_through_list_client():
    items = [
        {"report_nm": "사업보고서 (2024.12)", "rcept_dt": "20250320"},
        {"report_nm": "분기보고서 (2025.03)", "rcept_dt": "20250515"},
    ]
    fake = FakeDart(_ok(items))
    with _use(fake):
        assert await dart_client.fetch_annual_report_dates("0001", 2023, 2024) == {2024: "20250320"}
    assert fake.calls[0]["pblntf_ty"] == "A" and fake.calls[0]["last_reprt_at"] == "Y"
    with _use(FakeDart(lambda params: httpx.Response(200, json={"status": "010"}))):
        dart_client._list_cache.clear()
        assert await dart_client.fetch_annual_report_dates("0001", 2023, 2024) == {}


# ---------------------------------------------------------------------------
# market_daily — tape disclosures
# ---------------------------------------------------------------------------


def _corp_code_patch():
    async def get_corp_code(code):
        return f"C{code}"

    return patch.object(market_daily.corp_codes, "get_corp_code", side_effect=get_corp_code)


async def test_repeated_tape_disclosure_lookups_call_list_once_per_corp():
    fake = FakeDart(lambda params: httpx.Response(200, json={
        "status": "000",
        "list": [{"report_nm": "유상증자결정", "rcept_no": f"R{params['corp_code']}", "corp_name": "X"}],
    }))
    interests = [{"stock_code": "000001", "stock_name": "A"}, {"stock_code": "000002", "stock_name": "B"}]
    with _use(fake), _corp_code_patch():
        first, warn1 = await market_daily._fetch_dart_disclosures(interests, "2026-09-30", essential=False)
        second, warn2 = await market_daily._fetch_dart_disclosures(interests, "2026-09-30", essential=False)
    assert sorted(c["corp_code"] for c in fake.calls) == ["C000001", "C000002"]
    assert first == second and len(first) == 2
    assert first[0]["is_material"] is True
    assert warn1 == warn2 == []
    assert {c["bgn_de"] for c in fake.calls} == {"20260930"}


async def test_tape_disclosure_warnings_keep_their_wording():
    def responder(params):
        if params["corp_code"] == "C000001":
            return httpx.Response(500)
        if params["corp_code"] == "C000002":
            return httpx.Response(200, json={"status": "011"})
        raise httpx.ConnectError("down")

    interests = [{"stock_code": f"00000{i}", "stock_name": str(i)} for i in (1, 2, 3)]
    with _use(FakeDart(responder)), _corp_code_patch():
        rows, warnings = await market_daily._fetch_dart_disclosures(interests, "2026-09-30")
    assert rows == []
    assert "000001 DART HTTP 500" in warnings
    assert "000002 DART status 011" in warnings
    assert any(w.startswith("000003 DART 조회 실패:") for w in warnings)


async def test_tape_disclosures_report_quota_block_once():
    dart_client._quota._roll()
    dart_client._quota.calls = dart_client.NONESSENTIAL_BUDGET
    fake = FakeDart(_ok([]))
    interests = [{"stock_code": f"00000{i}", "stock_name": str(i)} for i in (1, 2, 3)]
    with _use(fake), _corp_code_patch():
        rows, warnings = await market_daily._fetch_dart_disclosures(interests, "2026-09-30", essential=False)
    assert fake.calls == [] and rows == []
    assert warnings == ["DART 호출 한도 보호로 공시 3건 조회를 건너뜀"]


async def _build_tape_with(gate_open: bool):
    movers = [{"stock_code": "000001", "stock_name": "A", "bucket": "급등", "change_pct": 12.0, "market": "KOSPI"}]
    disclosures = AsyncMock(return_value=([], []))
    market_daily._TAPE_CACHE.clear()
    with patch.object(market_daily, "_tape_index_rows", new=AsyncMock(return_value=[])), \
         patch.object(market_daily, "_tape_movers", new=AsyncMock(return_value=movers)), \
         patch.object(market_daily, "_news_for_focus_codes", new=AsyncMock(return_value=[])), \
         patch.object(market_daily, "_fetch_dart_disclosures", new=disclosures), \
         patch.object(market_daily.dart_client, "in_disclosure_hours", return_value=gate_open):
        result = await market_daily.build_market_tape()
    market_daily._TAPE_CACHE.clear()
    return result, disclosures


async def test_tape_skips_dart_outside_disclosure_hours():
    result, disclosures = await _build_tape_with(False)
    disclosures.assert_not_awaited()
    assert result["counts"]["disclosures"] == 0

    _, disclosures = await _build_tape_with(True)
    disclosures.assert_awaited_once()
    assert disclosures.await_args.kwargs == {"essential": False}


async def test_tape_single_flight_and_cache_flag():
    builds = 0

    async def build():
        nonlocal builds
        builds += 1
        await asyncio.sleep(0.01)
        return {"brief_date": "2026-09-30", "cached": False, "events": []}

    market_daily._TAPE_CACHE.clear()
    with patch.object(market_daily, "_build_market_tape_uncached", side_effect=build):
        results = await asyncio.gather(*(market_daily.build_market_tape() for _ in range(4)))
        again = await market_daily.build_market_tape()
        refreshed = await market_daily.build_market_tape(refresh=True)
    market_daily._TAPE_CACHE.clear()
    assert builds == 2
    assert all(r["cached"] is False for r in results)
    assert again["cached"] is True
    assert refreshed["cached"] is False


async def test_tape_serves_previous_tape_when_rebuild_fails(caplog):
    market_daily._TAPE_CACHE.clear()
    market_daily._TAPE_CACHE.set("public", {"events": ["old"], "cached": False}, ttl_seconds=0)
    with caplog.at_level(logging.WARNING, logger=market_daily.logger.name), \
         patch.object(market_daily, "_build_market_tape_uncached", new=AsyncMock(side_effect=httpx.ConnectError("x"))):
        result = await market_daily.build_market_tape()
    market_daily._TAPE_CACHE.clear()
    assert result == {"events": ["old"], "cached": True}
    # 실패 원인이 stale 응답 뒤에 묻히지 않고 로그로 남는다.
    assert any("serving previous tape" in rec.getMessage() for rec in caplog.records)


# ---------------------------------------------------------------------------
# dart_report_review — periodic filings through the shared client
# ---------------------------------------------------------------------------


async def test_periodic_filings_use_list_client_and_keep_status_semantics():
    dart_report_review._filings_cache.clear()
    items = [
        {"rcept_no": "1", "report_nm": "분기보고서 (2026.06)", "rcept_dt": "20260814"},
        {"rcept_no": "2", "report_nm": "주요사항보고서", "rcept_dt": "20260801"},
    ]
    fake = FakeDart(_ok(items))
    with _use(fake):
        filings = await dart_report_review.fetch_periodic_filings("0001")
    assert [f["rcept_no"] for f in filings] == ["1"]
    assert fake.calls[0]["pblntf_ty"] == "A" and fake.calls[0]["page_count"] == "100"

    dart_report_review._filings_cache.clear()
    dart_client._list_cache.clear()
    with _use(FakeDart(lambda params: httpx.Response(200, json={"status": "014"}))):
        assert await dart_report_review.fetch_periodic_filings("0001") == []

    dart_report_review._filings_cache.clear()
    dart_client._list_cache.clear()
    with _use(FakeDart(lambda params: httpx.Response(200, json={"status": "010", "message": "등록되지 않은 키"}))):
        with pytest.raises(dart_report_review.DartReportReviewError, match="등록되지 않은 키"):
            await dart_report_review.fetch_periodic_filings("0001")
    dart_report_review._filings_cache.clear()
