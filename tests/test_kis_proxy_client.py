import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import kis_proxy_client


class _FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True}


class _FakeClient:
    def __init__(self):
        self.calls = []

    async def get(self, url, params=None, headers=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return _FakeResponse()


class _HTTPErrorResponse:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text
        self.request = httpx.Request("GET", "http://test.local")

    def raise_for_status(self):
        raise httpx.HTTPStatusError("transient", request=self.request, response=self)

    def json(self):
        return {}


class _SequenceClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def get(self, url, params=None, headers=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_kis_proxy_token_header_is_forwarded_when_configured():
    fake = _FakeClient()
    with patch.object(kis_proxy_client, "PROXY_TOKEN", "secret"), \
         patch.object(kis_proxy_client, "_get_client", new=AsyncMock(return_value=fake)), \
         patch.object(kis_proxy_client, "_acquire_rate_slot", new=AsyncMock()):
        payload = await kis_proxy_client._get("/v1/stocks/005930/quote")

    assert payload == {"ok": True}
    assert fake.calls[0]["headers"] == {"X-KIS-Proxy-Token": "secret"}


@pytest.mark.asyncio
async def test_kis_proxy_retries_transient_http_status_without_body_match():
    fake = _SequenceClient([_HTTPErrorResponse(502, "proxy busy"), _FakeResponse()])
    with patch.object(kis_proxy_client, "_get_client", new=AsyncMock(return_value=fake)), \
         patch.object(kis_proxy_client, "_acquire_rate_slot", new=AsyncMock()), \
         patch.object(kis_proxy_client.asyncio, "sleep", new=AsyncMock()):
        payload = await kis_proxy_client._get("/v1/stocks/005930/quote")

    assert payload == {"ok": True}
    assert len(fake.calls) == 2


@pytest.mark.asyncio
async def test_daily_adjusted_history_uses_local_daily_api_before_kis():
    local_items = [{"stck_bsop_date": "20260430", "stck_clpr": 220500.0}]
    kis = AsyncMock(side_effect=AssertionError("KIS history should not run when local daily API succeeds"))

    with patch.object(kis_proxy_client.close_price_client, "get_daily_price_items", new=AsyncMock(return_value=local_items)), \
         patch.object(kis_proxy_client, "_get", new=kis):
        payload = await kis_proxy_client.get_history("005930", period="D", adjusted=True)

    assert payload == {"items": local_items, "source": "local_daily_price_api"}
    kis.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_history_falls_back_to_kis_when_local_daily_api_fails():
    kis_payload = {"items": [{"stck_bsop_date": "20260430", "stck_clpr": "220500"}]}

    with patch.object(
        kis_proxy_client.close_price_client,
        "get_daily_price_items",
        new=AsyncMock(side_effect=kis_proxy_client.close_price_client.ClosePriceClientError("local down")),
    ), patch.object(kis_proxy_client, "_get", new=AsyncMock(return_value=kis_payload)):
        payload = await kis_proxy_client.get_history("005930", period="D", adjusted=True)

    assert payload == kis_payload


@pytest.mark.asyncio
async def test_daily_history_falls_back_to_kis_when_local_daily_api_is_empty():
    kis_payload = {"items": [{"stck_bsop_date": "20260430", "stck_clpr": "220500"}]}

    with patch.object(kis_proxy_client.close_price_client, "get_daily_price_items", new=AsyncMock(return_value=[])), \
         patch.object(kis_proxy_client, "_get", new=AsyncMock(return_value=kis_payload)):
        payload = await kis_proxy_client.get_history("005930", period="D", adjusted=True)

    assert payload == kis_payload


@pytest.mark.asyncio
async def test_non_daily_history_stays_on_kis():
    kis_payload = {"items": [{"stck_bsop_date": "20260430", "stck_clpr": "220500"}]}
    local = AsyncMock(side_effect=AssertionError("local daily API is only for adjusted daily history"))

    with patch.object(kis_proxy_client.close_price_client, "get_daily_price_items", new=local), \
         patch.object(kis_proxy_client, "_get", new=AsyncMock(return_value=kis_payload)):
        payload = await kis_proxy_client.get_history("005930", period="W", adjusted=True)

    assert payload == kis_payload
    local.assert_not_awaited()


def test_close_price_rows_are_normalized_to_kis_history_items():
    rows = kis_proxy_client.close_price_client.close_rows_to_kis_items(
        {
            "prices": [
                {"date": "2026-04-30", "close": "220,500"},
                {"date": "20260428", "close": 222000.0},
                {"date": "bad", "close": 1},
            ]
        }
    )

    assert rows == [
        {
            "stck_bsop_date": "20260428",
            "stck_clpr": 222000.0,
            "date": "2026-04-28",
            "close": 222000.0,
            "close_price": 222000.0,
        },
        {
            "stck_bsop_date": "20260430",
            "stck_clpr": 220500.0,
            "date": "2026-04-30",
            "close": 220500.0,
            "close_price": 220500.0,
        },
    ]


def test_daily_price_rows_are_normalized_to_kis_history_items():
    rows = kis_proxy_client.close_price_client.daily_rows_to_kis_items(
        {
            "prices": {
                "005930": [
                    {
                        "date": "2026-04-30",
                        "open": 226000,
                        "high": 227000,
                        "low": 220000,
                        "close": "220,500",
                        "volume": 22161975,
                        "trading_value": 4984706020346,
                    }
                ]
            }
        }
    )

    assert rows == [
        {
            "stck_bsop_date": "20260430",
            "stck_clpr": 220500.0,
            "date": "2026-04-30",
            "close": 220500.0,
            "close_price": 220500.0,
            "stck_oprc": 226000.0,
            "open": 226000.0,
            "stck_hgpr": 227000.0,
            "high": 227000.0,
            "stck_lwpr": 220000.0,
            "low": 220000.0,
            "acml_vol": 22161975.0,
            "volume": 22161975.0,
            "acml_tr_pbmn": 4984706020346.0,
            "trade_value": 4984706020346.0,
            "trading_value": 4984706020346.0,
        },
    ]


@pytest.mark.asyncio
async def test_internal_close_price_requires_explicit_date_range():
    getter = AsyncMock(side_effect=AssertionError("internal close API should not be called without dates"))

    from services.market.sources import finance_pi

    with patch.object(finance_pi, "_get_client", new=getter):
        rows = await kis_proxy_client.close_price_client.get_daily_closes("005930")

    assert rows == []
    getter.assert_not_awaited()


# ---------------------------------------------------------------------------
# X12/O11 — 재무·배당 TTL 캐시 + single-flight, 프로필별 기본 주소
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeated_financials_calls_hit_transport_once():
    transport = AsyncMock(return_value={"output": [{"stac_yymm": "202512"}]})
    with patch.object(kis_proxy_client, "_get", new=transport):
        first = await kis_proxy_client.get_financials("005930")
        # 분석 1회가 재무를 여러 번 부르는 상황 — 두 번째부터는 캐시.
        for _ in range(3):
            again = await kis_proxy_client.get_financials("005930")

    transport.assert_awaited_once_with(
        "/v1/stocks/005930/financials", params={"period_div_code": "0"}
    )
    assert again == first
    # 호출자가 결과를 바꿔도 캐시가 오염되지 않는다.
    again["output"].clear()
    with patch.object(kis_proxy_client, "_get", new=transport):
        assert (await kis_proxy_client.get_financials("005930"))["output"]


@pytest.mark.asyncio
async def test_financials_cache_is_keyed_by_symbol_and_period():
    transport = AsyncMock(side_effect=lambda path, params=None: {"path": path, "params": params})
    with patch.object(kis_proxy_client, "_get", new=transport):
        annual = await kis_proxy_client.get_financials("005930")
        quarterly = await kis_proxy_client.get_financials("005930", period_div_code="1")
        other = await kis_proxy_client.get_financials("000660")
        await kis_proxy_client.get_financials("005930", period_div_code="1")

    assert transport.await_count == 3
    assert annual["params"] == {"period_div_code": "0"}
    assert quarterly["params"] == {"period_div_code": "1"}
    assert other["path"] == "/v1/stocks/000660/financials"


@pytest.mark.asyncio
async def test_concurrent_dividend_calls_share_one_inflight_request():
    release = asyncio.Event()
    calls = []

    async def slow_get(path, params=None):
        calls.append((path, params))
        await release.wait()
        return {"output": [{"per_sto_divi_amt": "361"}]}

    start, end = date(2021, 1, 1), date(2026, 9, 30)
    with patch.object(kis_proxy_client, "_get", new=slow_get):
        tasks = [
            asyncio.create_task(kis_proxy_client.get_dividends("005930", start_date=start, end_date=end))
            for _ in range(3)
        ]
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(*tasks)
        # 날짜 범위가 다르면 별도 요청이다.
        await kis_proxy_client.get_dividends("005930", start_date=date(2025, 1, 1), end_date=end)

    assert calls == [
        ("/v1/stocks/005930/dividends", {"start_date": "2021-01-01", "end_date": "2026-09-30"}),
        ("/v1/stocks/005930/dividends", {"start_date": "2025-01-01", "end_date": "2026-09-30"}),
    ]
    assert all(result == results[0] for result in results)
    # 대기자마다 독립된 복사본을 받는다.
    assert len({id(result) for result in results}) == 3


@pytest.mark.asyncio
async def test_empty_and_failed_responses_are_not_cached():
    transport = AsyncMock(side_effect=[
        {},
        kis_proxy_client.KISProxyError("down"),
        {"output": [1]},
    ])
    with patch.object(kis_proxy_client, "_get", new=transport):
        assert await kis_proxy_client.get_financials("005930") == {}
        with pytest.raises(kis_proxy_client.KISProxyError):
            await kis_proxy_client.get_financials("005930")
        assert await kis_proxy_client.get_financials("005930") == {"output": [1]}
        assert await kis_proxy_client.get_financials("005930") == {"output": [1]}

    assert transport.await_count == 3


@pytest.mark.asyncio
async def test_inflight_failure_propagates_to_waiters_without_caching():
    release = asyncio.Event()

    async def failing_get(path, params=None):
        await release.wait()
        raise kis_proxy_client.KISProxyError("boom")

    with patch.object(kis_proxy_client, "_get", new=failing_get):
        tasks = [asyncio.create_task(kis_proxy_client.get_financials("005930")) for _ in range(2)]
        await asyncio.sleep(0)
        release.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    assert all(isinstance(outcome, kis_proxy_client.KISProxyError) for outcome in outcomes)
    assert kis_proxy_client._response_inflight == {}


@pytest.mark.asyncio
async def test_waiter_refetches_when_owner_call_is_cancelled():
    started = asyncio.Event()
    calls = []

    async def fake_get(path, params=None):
        calls.append(path)
        if len(calls) == 1:
            started.set()
            await asyncio.sleep(3600)
        return {"output": ["ok"]}

    with patch.object(kis_proxy_client, "_get", new=fake_get):
        owner = asyncio.create_task(kis_proxy_client.get_financials("005930"))
        await started.wait()
        waiter = asyncio.create_task(kis_proxy_client.get_financials("005930"))
        await asyncio.sleep(0)
        owner.cancel()
        result = await waiter

    assert owner.cancelled()
    assert result == {"output": ["ok"]}
    assert len(calls) == 2


def test_default_base_url_is_loopback_in_production_and_public_elsewhere():
    assert kis_proxy_client.default_base_url("production") == "http://127.0.0.1:3288"
    assert kis_proxy_client.default_base_url("prod") == "http://127.0.0.1:3288"
    assert kis_proxy_client.default_base_url("development") == "http://ducklove.duckdns.org:3288"
    assert kis_proxy_client.default_base_url("dev") == "http://ducklove.duckdns.org:3288"


def test_resolve_base_url_follows_profile_when_env_unset(monkeypatch):
    monkeypatch.delenv("KIS_PROXY_BASE_URL", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("VALUE_INVEST_ENV", "development")
    assert kis_proxy_client.resolve_base_url() == "http://ducklove.duckdns.org:3288"
    monkeypatch.setenv("VALUE_INVEST_ENV", "production")
    assert kis_proxy_client.resolve_base_url() == "http://127.0.0.1:3288"
    # 프로필 미지정 = production (core.config 기본값과 동일).
    monkeypatch.delenv("VALUE_INVEST_ENV")
    assert kis_proxy_client.resolve_base_url() == "http://127.0.0.1:3288"


def test_resolve_base_url_always_honours_env_override(monkeypatch):
    monkeypatch.setenv("VALUE_INVEST_ENV", "production")
    monkeypatch.setenv("KIS_PROXY_BASE_URL", "http://proxy.example:9999/")
    assert kis_proxy_client.resolve_base_url() == "http://proxy.example:9999"
    assert kis_proxy_client.resolve_base_url("development") == "http://proxy.example:9999"
    monkeypatch.setenv("KIS_PROXY_BASE_URL", "   ")
    assert kis_proxy_client.resolve_base_url("development") == "http://ducklove.duckdns.org:3288"


@pytest.mark.asyncio
async def test_kis_proxy_uses_shared_client_with_env_timeout():
    from core import http as http_manager

    assert http_manager.timeout_for(kis_proxy_client.HTTP_CLIENT_NAME) == kis_proxy_client.TIMEOUT_SECONDS
    manager = http_manager.HttpClientManager()
    with patch.object(http_manager, "_manager", manager):
        client = await kis_proxy_client._get_client()
        assert client is await http_manager.get_http_client("kis_proxy")
        assert client.timeout.read == kis_proxy_client.TIMEOUT_SECONDS
    await manager.close_all()
