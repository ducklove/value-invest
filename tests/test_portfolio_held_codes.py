from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

import httpx
from _harness import TempDbMixin, seed_user
from fastapi import FastAPI

from repositories import accounts, portfolio
from routes import portfolio as portfolio_route


class HeldCodesTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user(sub="u2", email="other@example.com")
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 2, 100)
        await portfolio.save_portfolio_item("u1", "005935", "삼성전자우", 0, 100)
        await portfolio.save_portfolio_item("u1", "000660", "공매도", -1, 100)
        await portfolio.save_portfolio_item("u1", "0131D0", "스팩", 1, 100)
        await portfolio.save_portfolio_item("u2", "005935", "삼성전자우", 5, 100)
        self.app = FastAPI()
        self.app.include_router(portfolio_route.router)

    async def request(self, user, path="/api/portfolio/held-codes?google_sub=u2"):
        with patch.object(portfolio_route, "get_current_user", AsyncMock(return_value=user)):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=self.app), base_url="http://test"
            ) as client:
                return await client.get(path)

    async def test_only_current_users_positive_positions_are_returned(self):
        response = await self.request({"google_sub": "u1"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"codes": ["005930", "0131D0"], "quantities": {"005930": 2, "0131D0": 1}})
        self.assertEqual(response.headers["cache-control"], "private, no-store")
        response = await self.request({"google_sub": "u2"})
        self.assertEqual(response.json(), {"codes": ["005935"], "quantities": {"005935": 5}})

    async def test_guest_does_not_read_portfolio(self):
        with patch.object(portfolio_route.portfolio_repo, "get_portfolio", AsyncMock()) as read:
            response = await self.request(None)
        read.assert_not_awaited()
        self.assertEqual(response.json(), {"codes": [], "quantities": {}})
        self.assertEqual(response.headers["cache-control"], "private, no-store")

    async def test_handoff_reads_current_session_and_uses_fragment_only(self):
        for key in ("holdingValue", "preferredSpread", "spacHunter", "buybacks", "eiayn"):
            response = await self.request({"google_sub": "u1"},
                                          f"/api/portfolio/open/{key}?code=0131D0&theme=dark&url=https://evil.example")
            self.assertEqual(response.status_code, 303)
            target = urlsplit(response.headers["location"])
            self.assertEqual(target.hostname, "ducklove.github.io")
            self.assertEqual(parse_qs(target.query), {"stock" if key == "buybacks" else "code": ["0131D0"], "theme": ["dark"]})
            positions = parse_qs(target.fragment)["vc-held"][0].split(",")
            self.assertEqual({code: float(qty) for code, qty in (entry.split(":") for entry in positions)},
                             {"005930": 2, "0131D0": 1})
            self.assertEqual(response.headers["cache-control"], "private, no-store")

    async def test_handoff_guest_invalid_code_and_unknown_tool(self):
        response = await self.request(None, "/api/portfolio/open/spacHunter?code=javascript:alert(1)&theme=bad")
        target = urlsplit(response.headers["location"])
        self.assertEqual(parse_qs(target.fragment, keep_blank_values=True), {"vc-held": [""]})
        self.assertEqual(parse_qs(target.query), {"theme": ["light"]})
        response = await self.request({"google_sub": "u1"}, "/api/portfolio/open/unknown")
        self.assertEqual(response.status_code, 404)

    async def test_quantities_aggregate_all_accounts_without_cost_or_account_details(self):
        second = await accounts.create_account("u1", name="다른 계좌")
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 3.5, 200,
                                            account_id=second["account_id"])
        response = await self.request({"google_sub": "u1"})
        self.assertEqual(response.json(), {"codes": ["005930", "0131D0"],
                                          "quantities": {"005930": 5.5, "0131D0": 1}})

    async def test_etf_foreign_positions_exclude_cash_and_preserve_exchange(self):
        for code in ("VOO", "1570.T", "CASH_USD", "CRYPTO_BTC"):
            await portfolio.save_portfolio_item("u1", code, code, 2.5, 100)
        response = await self.request({"google_sub": "u1"}, "/api/portfolio/open/eiayn?code=1570.T")
        target = urlsplit(response.headers["location"])
        self.assertEqual(target.path, "/eiayn/")
        self.assertEqual(parse_qs(target.query)["code"], ["1570.T"])
        positions = parse_qs(target.fragment)["vc-held"][0]
        self.assertIn("VOO:2.5", positions)
        self.assertIn("1570.T:2.5", positions)
        self.assertNotIn("CASH", positions)
        self.assertNotIn("CRYPTO", positions)
        response = await self.request({"google_sub": "u1"}, "/api/portfolio/open/buybacks?stock=005930")
        target = urlsplit(response.headers["location"])
        self.assertEqual(parse_qs(target.query)["stock"], ["005930"])
        self.assertNotIn("VOO", target.fragment)
        self.assertNotIn("1570.T", target.fragment)
