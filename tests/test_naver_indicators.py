import httpx
import pytest

from services.market import naver_indicators as ni
from services.market.sources import yahoo
from services.portfolio import fx


@pytest.fixture(autouse=True)
def _isolate_shared_sources():
    # 환율 원본은 services.portfolio.fx 캐시를, Yahoo 대체 경로는 provider 쿨다운을 공유한다.
    fx._fx_daily_cache.clear()
    yahoo.reset_rate_limit_state()
    yield
    fx._fx_daily_cache.clear()
    yahoo.reset_rate_limit_state()


@pytest.mark.asyncio
async def test_json_sources_batch_domestic_and_preserve_dates_units_and_delays():
    requests = []

    def respond(request):
        requests.append(request.url.path)
        row = {"closePrice": "6,864.30", "compareToPreviousClosePrice": "-169.62",
               "fluctuationsRatio": "-2.41", "localTradedAt": "2026-09-11T10:11:14+09:00", "marketStatus": "OPEN"}
        path = request.url.path
        if "/domestic/" in path:
            return httpx.Response(200, json={"datas": [{**row, "itemCode": symbol} for symbol in ("KOSPI", "KOSDAQ", "KPI200")]})
        symbol = path.split("/")[-2] if path.endswith("/basic") else path.split("/")[-1]
        row = {**row, "reutersCode": symbol, "delayTime": 15}
        if symbol == "FX_JPYKRW":
            row.update(closePrice="871.78", fluctuations="-3.31")
            del row["compareToPreviousClosePrice"]
            return httpx.Response(200, json={"exchangeInfo": row})
        return httpx.Response(200, json=row)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await ni.fetch_indicators(client, ["KOSPI", "KOSDAQ", "KOSPI200", "SPX", "JPY_KRW", "USD_IDX"])
    assert len(requests) == 4
    assert len(result) == 6
    assert result["KOSPI"]["direction"] == "down"
    assert result["KOSPI"]["change"] == "169.62"
    assert result["KOSPI"]["change_pct"] == "2.41%"
    assert result["KOSPI"]["as_of"] == "2026-09-11T10:11:14+09:00"
    assert result["JPY_KRW"]["value"] == "871.78"  # 기존 시장바의 100엔 기준 유지
    assert result["JPY_KRW"]["change"] == "3.31"
    assert result["USD_IDX"]["delay_minutes"] == 15
    assert result["SPX"]["source"] == "naver_json"
    assert result["SPX"]["fetched_at"]


@pytest.mark.asyncio
async def test_one_source_failure_or_invalid_row_does_not_erase_other_indicators():
    def respond(request):
        if "/domestic/" in request.url.path:
            return httpx.Response(200, json={"datas": [
                {"itemCode": "KOSPI", "closePrice": "NaN"},
                {"itemCode": "KOSDAQ", "closePrice": "820.50", "fluctuationsRatio": "0.00"},
                {"itemCode": "OTHER", "closePrice": "123"},
            ]})
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await ni.fetch_indicators(client, ["KOSPI", "KOSDAQ", "SPX"])
    assert set(result) == {"KOSDAQ"}
    assert result["KOSDAQ"]["change"] == ""
    assert result["KOSDAQ"]["change_pct"] == "0.00%"


@pytest.mark.parametrize("value", [None, True, "", "NaN", "Infinity", "0", "-1"])
def test_invalid_value_is_not_a_current_quote(value):
    with pytest.raises((TypeError, ValueError)):
        ni._quote({"closePrice": value})


@pytest.mark.asyncio
async def test_redirected_html_is_not_accepted_as_json():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, text="<html>new site</html>"))) as client:
        assert await ni.fetch_indicators(client, ["SPX"]) == {}


@pytest.mark.asyncio
async def test_provider_failure_recovers_via_yahoo_with_explicit_source():
    def respond(request):
        if request.url.host != "query1.finance.yahoo.com":
            return httpx.Response(503)
        assert request.url.params["range"] == "1d"
        return httpx.Response(200, json={"chart": {"result": [{"meta": {
            "regularMarketPrice": 6864.3, "previousClose": 7033.92, "regularMarketTime": 1789088240,
        }}]}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await ni.fetch_indicators(client, ["KOSPI"])
    assert result["KOSPI"]["value"] == "6,864.30"
    assert result["KOSPI"]["direction"] == "down"
    assert result["KOSPI"]["source"] == "yahoo_fallback"
    assert result["KOSPI"]["_degraded"] is True
    assert result["KOSPI"]["as_of"].startswith("2026-09-11")


@pytest.mark.asyncio
async def test_fx_indicator_reuses_shared_payload_for_60_seconds_with_original_fetch_time(monkeypatch):
    calls = []
    clock = [5000.0]
    monkeypatch.setattr(fx.time, "monotonic", lambda: clock[0])
    row = {"reutersCode": "FX_USDKRW", "unit": "KRW", "closePrice": "1,391.50", "fluctuations": "2.50",
           "fluctuationsRatio": "0.18", "localTradedAt": "2026-09-30T10:00:00+09:00"}

    def respond(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"exchangeInfo": row})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        first = await ni.fetch_indicators(client, ["USD_KRW"])
        clock[0] += ni.FX_MAX_AGE_SECONDS - 1
        second = await ni.fetch_indicators(client, ["USD_KRW"])
        assert calls == ["/marketindex/exchange/FX_USDKRW"]
        # 재사용한 값은 원래 받은 시각을 fetched_at 으로 유지한다.
        assert second["USD_KRW"] == first["USD_KRW"]
        clock[0] += 2
        await ni.fetch_indicators(client, ["USD_KRW"])
    assert len(calls) == 2
    assert first["USD_KRW"]["direction"] == "up"
    assert first["USD_KRW"]["change"] == "2.50"


@pytest.mark.asyncio
async def test_yahoo_fallback_uses_previous_close_not_chart_previous_close():
    def respond(request):
        if request.url.host != "query1.finance.yahoo.com":
            return httpx.Response(503)
        assert request.url.raw_path.decode().startswith("/v8/finance/chart/%5EGSPC?")
        assert dict(request.url.params) == {"range": "1d", "interval": "1m"}
        return httpx.Response(200, json={"chart": {"result": [{"meta": {
            "regularMarketPrice": 110.0, "previousClose": 100.0, "chartPreviousClose": 50.0,
            "regularMarketTime": 1789088240,
        }}]}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await ni.fetch_indicators(client, ["SPX"])
    assert result["SPX"]["change"] == "10.00"
    assert result["SPX"]["change_pct"] == "10.00%"


@pytest.mark.asyncio
async def test_yahoo_fallback_without_previous_close_is_dropped():
    def respond(request):
        if request.url.host != "query1.finance.yahoo.com":
            return httpx.Response(503)
        return httpx.Response(200, json={"chart": {"result": [{"meta": {
            "regularMarketPrice": 110.0, "chartPreviousClose": 50.0, "regularMarketTime": 1789088240,
        }}]}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        assert await ni.fetch_indicators(client, ["SPX"]) == {}
