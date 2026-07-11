import json

import httpx
import pytest

import market_indicators

HL_CODES = ["HL_EWY", "HL_SAMSUNG", "HL_SKHYNIX", "HL_HYUNDAI"]


def test_hyperliquid_catalog_and_symbol_map():
    assert market_indicators._HYPERLIQUID_MAP == {
        "HL_EWY": "xyz:EWY",
        "HL_SAMSUNG": "xyz:SMSN",
        "HL_SKHYNIX": "xyz:SKHX",
        "HL_HYUNDAI": "xyz:HYUNDAI",
    }
    assert {
        code: market_indicators.CATALOG[code]["category"] for code in HL_CODES
    } == dict.fromkeys(HL_CODES, "하이퍼리퀴드")


def test_parse_hyperliquid_context_prefers_mid_price():
    assert market_indicators._parse_hyperliquid_context(
        {"midPx": "105", "markPx": "104", "prevDayPx": "100"}
    ) == {
        "value": "105.00",
        "change": "5.00",
        "change_pct": "5.00%",
        "direction": "up",
    }


def test_parse_hyperliquid_context_falls_back_to_mark_price():
    assert market_indicators._parse_hyperliquid_context(
        {"midPx": None, "markPx": "95", "prevDayPx": "100"}
    ) == {
        "value": "95.00",
        "change": "5.00",
        "change_pct": "5.00%",
        "direction": "down",
    }


@pytest.mark.parametrize(
    "context",
    [None, {}, {"midPx": "bad", "markPx": "also-bad", "prevDayPx": "100"}],
)
def test_parse_hyperliquid_context_rejects_bad_context(context):
    assert market_indicators._parse_hyperliquid_context(context) == market_indicators._EMPTY


@pytest.mark.asyncio
async def test_fetch_hyperliquid_tickers_posts_xyz_meta_and_aligns_contexts():
    seen = {}
    payload = [
        {
            "universe": [
                {"name": "xyz:SKHX"},
                {"name": "xyz:EWY"},
                {"name": "xyz:IGNORED"},
            ]
        },
        [
            {"midPx": "180", "prevDayPx": "200"},
            {"midPx": "80", "prevDayPx": "100"},
            {"midPx": "1", "prevDayPx": "1"},
        ],
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(method=request.method, url=str(request.url), body=json.loads(request.content))
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await market_indicators._fetch_hyperliquid_tickers(
            client, ["HL_EWY", "HL_SKHYNIX", "HL_HYUNDAI"]
        )

    assert seen == {
        "method": "POST",
        "url": "https://api.hyperliquid.xyz/info",
        "body": {"type": "metaAndAssetCtxs", "dex": "xyz"},
    }
    assert result["HL_EWY"] == {
        "value": "80.00",
        "change": "20.00",
        "change_pct": "20.00%",
        "direction": "down",
    }
    assert result["HL_SKHYNIX"]["value"] == "180.00"
    assert result["HL_SKHYNIX"]["change_pct"] == "10.00%"
    assert result["HL_HYUNDAI"] == market_indicators._EMPTY


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{}, [], [{"universe": []}], [{"universe": "bad"}, []]])
async def test_fetch_hyperliquid_tickers_empty_fallback_for_malformed_payload(payload):
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await market_indicators._fetch_hyperliquid_tickers(client, ["HL_EWY", "HL_SAMSUNG"])

    assert result == {code: market_indicators._EMPTY for code in ["HL_EWY", "HL_SAMSUNG"]}


@pytest.mark.asyncio
async def test_fetch_hyperliquid_tickers_empty_fallback_for_http_error():
    valid_payload = [
        {"universe": [{"name": "xyz:EWY"}]},
        [{"midPx": "105", "prevDayPx": "100"}],
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json=valid_payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await market_indicators._fetch_hyperliquid_tickers(client, ["HL_EWY"])

    assert result == {"HL_EWY": market_indicators._EMPTY}
