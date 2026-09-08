"""실제 앱 라우팅·직렬화와 정산 누락 경계를 검증한다."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import TempDbMixin, seed_user
from fastapi.exceptions import ResponseValidationError

from core.app_factory import create_app
from core.static_routes import _with_asset_version
from repositories import snapshots
from routes import portfolio
from services.portfolio import snapshot_views


class ResponseContractTests(TempDbMixin):
    async def seed(self):
        await seed_user()

    async def test_full_app_serializes_missing_quotes_without_inventing_zeroes(self):
        app = create_app()
        await portfolio.portfolio_repo.save_portfolio_item("u1", "CASH_KRW", "원화 현금", 100, 1)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            with patch.object(portfolio, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})), \
                 patch.object(portfolio.dividends, "schedule_for_portfolio"), \
                 patch.object(portfolio.insights, "schedule_asset_insight_warmup"):
                response = await client.get("/api/portfolio")
                assert response.status_code == 200
                holding = response.json()[0]
                assert holding["stock_code"] == "CASH_KRW"
                assert holding["quantity"] == 100
                assert holding["avg_price_krw"] == 1
                assert holding["tags"] == []
            with patch.object(portfolio, "_cached_quote_for_code", return_value={}):
                response = await client.post("/api/asset-quotes", json={"codes": ["UNKNOWN"], "fresh": False})
                assert response.status_code == 200
                assert response.json() == {"UNKNOWN": {}}
            with patch.object(portfolio, "_fetch_quote", AsyncMock(return_value={"price": 100, "_stale": True, "_source": "snapshot"})):
                response = await client.get("/api/asset-quote/005930")
                assert response.json() == {"price": 100, "_stale": True, "_source": "snapshot"}
            with patch.object(portfolio, "_fetch_quote", AsyncMock(return_value={"price": float("nan")})):
                with pytest.raises(ResponseValidationError):
                    await client.get("/api/asset-quote/005930")

    async def test_settlement_and_stock_values_share_the_actual_baseline(self):
        await snapshots.save_snapshot("u1", "2026-08-28", 100, 100, 1000, .1)
        await snapshots.save_stock_snapshots("u1", "2026-08-28", [{"stock_code": "CASH_KRW", "market_value": 100}])
        await snapshots.save_stock_snapshots("u1", "2026-08-31", [{"stock_code": "CASH_KRW", "market_value": 999}])
        app = create_app()
        with patch.object(portfolio, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})), \
             patch.object(portfolio, "_portfolio_today_baseline_date", return_value="2026-08-31"):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
                result = (await client.get("/api/portfolio/prev-day-snapshot")).json()
                assert result["date"] == "2026-08-28"
                assert result["total_value"] == 100
                assert result["stock_values"] == {"CASH_KRW": 100}
                assert result["fx_usdkrw"] is None
                nav = (await client.get("/api/portfolio/nav-history")).json()
                assert nav[0]["nav"] == 1000
        baseline = await snapshots.get_snapshot_by_date("u1", "2026-08-28")
        with patch.object(snapshots, "get_month_end_snapshot", AsyncMock(return_value=baseline)):
            assert (await snapshot_views.period_start("u1"))["stock_values"] == {"CASH_KRW": 100}
        await snapshots.save_snapshot("u1", "2026-09-01", 500, 100, 5000, .1)
        result = await snapshot_views.previous_day("u1", "2026-09-01")
        assert result["total_value"] == 500
        assert result["stock_values"] == {}  # 이전 날짜의 999를 섞지 않는다.


def test_core_response_schemas_are_reachable_from_openapi():
    spec = create_app().openapi()
    for path, method in [
        ("/api/portfolio", "get"), ("/api/asset-quote/{stock_code}", "get"), ("/api/asset-quotes", "post"),
        ("/api/portfolio/prev-day-snapshot", "get"), ("/api/portfolio/month-end-value", "get"),
        ("/api/portfolio/year-start-value", "get"), ("/api/portfolio/nav-history", "get"),
        ("/api/portfolio/intraday", "get"), ("/api/portfolio/attribution", "get"), ("/api/portfolio/theses", "get"),
    ]:
        schema = spec["paths"][path][method]["responses"]["200"]["content"]["application/json"]["schema"]
        assert "$ref" in str(schema), path
    assert "price" in spec["components"]["schemas"]["QuoteResponse"]["properties"]
    attribution = spec["paths"]["/api/portfolio/attribution"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert attribution["discriminator"]["propertyName"] == "available"


def test_lazy_asset_urls_receive_the_same_release_version():
    html = '<script data-feature="detail" data-src="./js/detail.js"></script><script src="./js/core.js" defer></script>'
    result = _with_asset_version(html, "release-test", relative=True)
    assert 'data-src="./js/detail.js?v=release-test"' in result
    assert 'src="./js/core.js?v=release-test"' in result
