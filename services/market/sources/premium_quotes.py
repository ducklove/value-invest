"""gold_gap와 같은 비교 기준: Yahoo USD crypto / 빗썸 USDT-KRW."""

from core.http import get_http_client
from services.market.sources import yahoo


async def international_crypto(symbol: str) -> dict:
    chart = yahoo.parse_chart(await yahoo.fetch_chart_json(symbol, range_="1d"))
    if chart is None or chart.currency != "USD":
        return {}
    return {"price": chart.meta.get("regularMarketPrice")}


async def usdt_krw() -> dict:
    client = await get_http_client("bithumb")
    response = await client.get("https://api.bithumb.com/public/ticker/USDT_KRW")
    response.raise_for_status()
    data = response.json()
    if data.get("status") != "0000":
        raise ValueError("Bithumb USDT quote unavailable")
    return {"price": data["data"]["closing_price"]}
