"""routes/ecosystem.py — /go/{tool_id} 레지스트리 딥링크 리다이렉트와 /api/ecosystem."""

import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

import httpx
from _harness import TempDbMixin, seed_user
from fastapi import FastAPI

from core import ecosystem
from repositories import portfolio
from routes import ecosystem as ecosystem_route
from routes import portfolio as portfolio_route
from services.ecosystem import links, siblings

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ecosystem"


class GoRedirectTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 2, 100)
        await portfolio.save_portfolio_item("u1", "VOO", "VOO", 1.5, 100)
        self.app = FastAPI()
        self.app.include_router(ecosystem_route.router)
        self.app.include_router(portfolio_route.router)

    async def get(self, path, user=None):
        with patch.object(ecosystem_route, "get_current_user", AsyncMock(return_value=user)), \
             patch.object(portfolio_route, "get_current_user", AsyncMock(return_value=user)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test") as client:
                return await client.get(path)

    def location(self, response):
        self.assertEqual(response.status_code, 303, response.text)
        self.assertEqual(response.headers["cache-control"], "private, no-store")
        self.assertEqual(response.headers["referrer-policy"], "no-referrer")
        return urlsplit(response.headers["location"])

    async def test_handoff_tool_matches_legacy_open_route(self):
        for tool_id, key in (("holding_value", "holdingValue"), ("buybacks", "buybacks"), ("eiayn", "eiayn")):
            code = "VOO" if tool_id == "eiayn" else "005930"
            go = self.location(await self.get(f"/go/{tool_id}?code={code}&theme=dark", {"google_sub": "u1"}))
            legacy = self.location(await self.get(f"/api/portfolio/open/{key}?code={code}&theme=dark",
                                                  {"google_sub": "u1"}))
            self.assertEqual(go, legacy)
            self.assertEqual(go.hostname, "ducklove.github.io")
            held = parse_qs(go.fragment)["vc-held"][0]
            self.assertIn("005930:2", held) if tool_id != "eiayn" else self.assertIn("VOO:1.5", held)
        buybacks = self.location(await self.get("/go/buybacks?code=005930", {"google_sub": "u1"}))
        self.assertEqual(parse_qs(buybacks.query), {"stock": ["005930"]})  # theme 미지정이면 붙이지 않는다

    async def test_non_handoff_templates(self):
        gold = self.location(await self.get("/go/gold_gap?asset=bitcoin&theme=light"))
        self.assertEqual(gold.geturl(), "https://ducklove.github.io/gold_gap/?asset=bitcoin&theme=light")
        bond = self.location(await self.get("/go/bond-mate?view=credit&embed=1"))
        self.assertEqual(parse_qs(bond.query), {"tab": ["credit"], "embed": ["credit"]})
        gold_embed = self.location(await self.get("/go/gold_gap?embed=1"))
        self.assertEqual(parse_qs(gold_embed.query), {"embed": ["1"]})
        aag = self.location(await self.get("/go/all-about-gold?view=etf"))
        self.assertEqual((aag.path, aag.fragment), ("/all-about-gold/", "etf"))
        hub = self.location(await self.get("/go/value-invest?code=005930&theme=dark"))
        self.assertEqual(hub.geturl(), "https://ducklove.duckdns.org:3691/analysis?code=005930&theme=dark")
        screener = self.location(await self.get("/go/hub:screener"))
        self.assertEqual(screener.geturl(), "https://ducklove.duckdns.org:3691/screener")

    async def test_rejected_values_are_dropped_and_host_never_changes(self):
        for path in (
            "/go/gold_gap?asset=https://evil.example&theme=purple",
            "/go/bond-mate?view=../../evil&embed=1",
            "/go/value-invest?code=javascript:alert(1)",
            "/go/holding_value?code=//evil.example",
        ):
            with self.subTest(path=path):
                target = self.location(await self.get(path))
                self.assertIn(target.hostname, {"ducklove.github.io", "ducklove.duckdns.org"})
                self.assertNotIn("evil", target.geturl())
                self.assertNotIn("purple", target.geturl())

    async def test_unknown_and_internal_tools_are_404(self):
        for tool_id in ("nope", "kis-proxy", "finance-pi"):
            response = await self.get(f"/go/{tool_id}")
            self.assertEqual(response.status_code, 404)

    async def test_env_override_is_honoured(self):
        with patch.dict(os.environ, {"GOLD_GAP_BASE_URL": "https://mirror.example/gg"}):
            target = self.location(await self.get("/go/gold_gap?asset=usdt"))
        self.assertEqual(target.geturl(), "https://mirror.example/gg/?asset=usdt")

    async def test_bad_base_url_is_503(self):
        with patch.dict(os.environ, {"HOLDING_VALUE_BASE_URL": "javascript:alert(1)"}):
            response = await self.get("/go/holding_value?code=005930")
        self.assertEqual(response.status_code, 503)

    async def test_api_ecosystem_projection_and_freshness(self):
        siblings.reset()
        env = json.loads((FIXTURES / "gold_gap.summary.json").read_text(encoding="utf-8"))
        with patch.dict(os.environ, {"ECOSYSTEM_SUMMARIES": "1"}), \
             patch.object(siblings, "_http_get", AsyncMock(return_value=(200, env, None))):
            await siblings.fetch_summary("gold_gap")
        try:
            response = await self.get("/api/ecosystem")
        finally:
            siblings.reset()
        self.assertEqual(response.status_code, 200)
        body = response.json()
        ids = {t["id"] for t in body["tools"]}
        self.assertNotIn("kis-proxy", ids)
        self.assertEqual(body["hub"], ecosystem.public_projection()["hub"])
        self.assertNotIn(":3288", response.text)
        self.assertEqual(body["freshness"]["gold_gap"]["asOf"], env["asOf"])
        self.assertEqual(body["freshness"]["gold_gap"]["source"], "summary")
        self.assertIsNone(body["freshness"]["holding_value"]["asOf"])
        self.assertTrue(set(body["freshness"]) <= ids)


class LinkBuilderTests(TempDbMixin):
    async def test_every_public_tool_resolves_inside_its_registry_host(self):
        for tool in ecosystem.public_tools():
            if tool.get("handoff"):
                continue
            url = urlsplit(links.go_url(tool["id"], code="005930", view="overview", asset="gold",
                                        theme="dark", embed=True))
            self.assertEqual(url.netloc, urlsplit(tool["url"]).netloc, tool["id"])

    async def test_template_cannot_change_host(self):
        builder = links._UrlBuilder("https://ducklove.github.io/x", trailing_slash=True)
        with self.assertRaises(links.LinkError):
            builder.apply("https://evil.example/?a={code}", {"code": "1"})
