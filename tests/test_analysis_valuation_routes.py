"""Current valuation endpoints stay independent of annual analysis snapshots."""

from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from routes import analysis


async def test_current_valuation_route_returns_per_share_inputs_without_annual_snapshot():
    basis = {"eps_ttm": 200.0, "bps": 1000.0, "earnings_basis": "ttm", "equity_basis": "latest"}
    app = FastAPI()
    app.include_router(analysis.router)
    fetch = AsyncMock(return_value=basis)
    snapshot = AsyncMock(side_effect=AssertionError("annual snapshot must not be consulted"))
    with patch.object(analysis.stock_price, "fetch_current_valuation_basis", fetch), \
         patch.object(analysis.analysis_repo, "get_analysis_snapshot", snapshot):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/api/analyze/005930/valuation")
    assert response.status_code == 200
    assert response.json() == {"stock_code": "005930", "current": basis}
    fetch.assert_awaited_once_with("005930")


async def test_forward_route_exposes_forecast_period_and_provenance():
    forecast = {"eps": 300.0, "bps": 1200.0, "period": "2026E", "source": "증권사", "as_of": "2026-10-20"}
    with patch.object(analysis.research_valuation, "fetch_forward_valuation", AsyncMock(return_value=forecast)):
        assert await analysis.get_forward_valuation("005930") == {"stock_code": "005930", "forward": forecast}


async def test_provider_failures_do_not_return_annual_multiples_or_exception_details():
    with patch.object(analysis.stock_price, "fetch_current_valuation_basis", AsyncMock(side_effect=RuntimeError("secret"))), \
         patch.object(analysis.research_valuation, "fetch_forward_valuation", AsyncMock(side_effect=RuntimeError("secret"))):
        current = await analysis.get_current_valuation("005930")
        forward = await analysis.get_forward_valuation("005930")
    assert current["current"]["status"] == "unavailable"
    assert "secret" not in str(current)
    assert forward == {"stock_code": "005930", "forward": None}
