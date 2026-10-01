"""ticker_map 해석 규칙과 자가 치유.

운영 사고: 미국 종목 AAPL 이 2026-04 에 AAPL.DE(Yahoo 에 없는 심볼)로 저장돼,
조광피혁(004700) 보유지분 자회사 시세를 그리는 화면이 5분마다 404 를 반복했다.
원인은 yfinance 접미사 탐색이 미국 심볼 조회의 일시 실패(429·시간 초과)를
"없음"으로 보고 독일 접미사로 넘어간 것이다.
"""

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import TempDbMixin

import cache_layer
from domain.portfolio_codes import is_plain_us_ticker
from repositories import ticker_map as ticker_map_repo
from services.market.sources import yahoo
from services.portfolio import foreign, quote_service

REAL_CHART = {"chart": {"result": [{
    "meta": {"currency": "USD", "symbol": "AAPL", "exchangeName": "NMS", "regularMarketPrice": 330.74,
             "regularMarketTime": 1790884800, "exchangeTimezoneName": "America/New_York"},
    "timestamp": [1790861400],
    "indicators": {"quote": [{"close": [330.74]}]},
}], "error": None}}
# Yahoo 가 모르는 나스닥형 심볼(THF, GOO)에 404 대신 주는 빈 껍데기.
PHANTOM_CHART = {"chart": {"result": [{
    "meta": {"currency": None, "symbol": "THF", "exchangeName": "NMS", "instrumentType": "ECNQUOTE",
             "firstTradeDate": None, "regularMarketTime": None},
    "indicators": {"quote": [{}], "adjclose": [{}]},
}], "error": None}}
NOT_FOUND_BODY = {"chart": {"result": None, "error": {"code": "Not Found",
                                                      "description": "No data found, symbol may be delisted"}}}


def _yahoo_client(routes: dict[str, httpx.Response | Exception], calls: list[str] | None = None):
    """chart 심볼별 응답을 돌려주는 MockTransport 클라이언트. 없는 심볼은 404."""

    def handler(request: httpx.Request) -> httpx.Response:
        symbol = request.url.path.rsplit("/", 1)[-1]
        if calls is not None:
            calls.append(symbol)
        answer = routes.get(symbol)
        if isinstance(answer, Exception):
            raise answer
        return answer if answer is not None else httpx.Response(404, json=NOT_FOUND_BODY)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture(autouse=True)
def _fresh_yahoo_state():
    yahoo.reset_rate_limit_state()
    yahoo.reset_missing_symbols()
    foreign.reset_resolution_state()
    foreign._failed_yf_cache.clear()
    yield
    yahoo.reset_rate_limit_state()
    yahoo.reset_missing_symbols()
    foreign.reset_resolution_state()
    foreign._failed_yf_cache.clear()


# --- 미국식 티커 판별 -----------------------------------------------------------

def test_plain_us_ticker_rule():
    for code in ("AAPL", "aapl", "F", "GOOGL", "BRK.B", "BRK-B", "BRK/B", "GOOGL.O", "AGNC.O", "SIVR.K", "VFIAX"):
        assert is_plain_us_ticker(code), code
    for code in ("AAPL.DE", "BP.L", "SAP.F", "EUN2", "EUN2.DE", "A200", "A200.AX", "0005", "0005.HK",
                 "83188.HK", "7203.T", "7203", "005930", "0074K0", "VNM.HM", "FUEVFVND", "FUEVFVND.HM",
                 "BTC-USD", "CASH_USD", "KRX_GOLD", "^GSPC", "", None):
        assert not is_plain_us_ticker(code), code


# --- Yahoo 상장 판정 --------------------------------------------------------------

def test_classify_listing_shapes():
    assert yahoo.classify_listing(REAL_CHART) == yahoo.LISTING_PRESENT
    assert yahoo.classify_listing(PHANTOM_CHART) == yahoo.LISTING_ABSENT
    assert yahoo.classify_listing(NOT_FOUND_BODY) == yahoo.LISTING_ABSENT
    assert yahoo.classify_listing({"chart": {"result": [], "error": None}}) == yahoo.LISTING_UNKNOWN
    assert yahoo.classify_listing({"oops": 1}) == yahoo.LISTING_UNKNOWN
    # 통화는 있는데 가격·봉이 없는 정지 종목 같은 모양은 단정하지 않는다.
    halted = {"chart": {"result": [{"meta": {"currency": "USD", "regularMarketTime": 1}}]}}
    assert yahoo.classify_listing(halted) == yahoo.LISTING_UNKNOWN


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (httpx.Response(200, json=REAL_CHART), yahoo.LISTING_PRESENT),
        (httpx.Response(404, json=NOT_FOUND_BODY), yahoo.LISTING_ABSENT),
        (httpx.Response(200, json=PHANTOM_CHART), yahoo.LISTING_ABSENT),
        (httpx.Response(500), yahoo.LISTING_UNKNOWN),
        (httpx.Response(503), yahoo.LISTING_UNKNOWN),
        (httpx.Response(429), yahoo.LISTING_UNKNOWN),
        (httpx.ReadTimeout("slow"), yahoo.LISTING_UNKNOWN),
        (httpx.Response(200, text="<html>"), yahoo.LISTING_UNKNOWN),
    ],
)
async def test_probe_listing_only_404_or_empty_means_absent(answer, expected):
    async with _yahoo_client({"AAPL": answer}) as client:
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)), \
             patch.object(yahoo, "MAX_INLINE_RETRY_DELAY", 0.0):
            assert await yahoo.probe_listing("AAPL") == expected


async def test_probe_listing_during_cooldown_is_unknown_without_network():
    yahoo._start_cooldown(None)
    getter = AsyncMock()
    with patch.object(yahoo, "get_http_client", getter):
        assert await yahoo.probe_listing("AAPL") == yahoo.LISTING_UNKNOWN
    getter.assert_not_awaited()


async def test_chart_fetch_records_and_clears_missing_symbol_evidence():
    routes = {"AAPL": httpx.Response(200, json=REAL_CHART), "THF": httpx.Response(200, json=PHANTOM_CHART)}
    async with _yahoo_client(routes) as client:
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)):
            assert await yahoo.fetch_close_series("AAPL.DE", range_="5d") == {"rows": [], "currency": None, "meta": {}}
            await yahoo.fetch_close_series("THF", range_="5d")
            await yahoo.fetch_close_series("AAPL", range_="5d")
            assert yahoo.symbol_missing("AAPL.DE")
            assert yahoo.symbol_missing("THF")
            assert not yahoo.symbol_missing("AAPL")
            # 다시 정상 응답이 오면 증거가 지워진다.
            routes["AAPL.DE"] = httpx.Response(200, json=REAL_CHART)
            await yahoo.fetch_close_series("AAPL.DE", range_="5d")
            assert not yahoo.symbol_missing("AAPL.DE")
    async with _yahoo_client({"MSFT": httpx.Response(503)}) as client:
        with patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)):
            await yahoo.fetch_close_series("MSFT", range_="5d")
    assert not yahoo.symbol_missing("MSFT")  # 5xx 는 "없음" 증거가 아니다


# --- 해석: 미국식 티커는 미국 상장부터 ------------------------------------------------

async def _find(code: str, routes: dict, *, yf_hits: set[str] = frozenset(), calls: list | None = None):
    """yfinance .info 탐색은 yf_hits 에 있는 후보만 맞힌다."""
    probed: list[str] = []

    async def fake_yf_run(fn):
        candidate = fn.args[0]
        probed.append(candidate)
        return candidate if candidate in yf_hits else None

    save = AsyncMock()
    async with _yahoo_client(routes, calls) as client:
        with patch.dict(foreign._ticker_map, {}, clear=True), \
             patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)), \
             patch.object(yahoo, "MAX_INLINE_RETRY_DELAY", 0.0), \
             patch.object(foreign, "yf_run", new=fake_yf_run), \
             patch.object(foreign, "save_ticker", new=save):
            found = await foreign.yfinance_find_ticker(code)
    return found, probed, save


async def test_us_listing_wins_even_if_a_foreign_suffix_would_answer():
    found, probed, save = await _find("AAPL", {"AAPL": httpx.Response(200, json=REAL_CHART)}, yf_hits={"AAPL.DE"})
    assert found == "AAPL"
    save.assert_awaited_once_with("AAPL", "AAPL")
    assert probed == []  # 접미사 탐색(yfinance .info)까지 가지 않는다


@pytest.mark.parametrize(
    "answer",
    [httpx.Response(429), httpx.Response(500), httpx.Response(502), httpx.ReadTimeout("slow"), httpx.ConnectError("down")],
)
async def test_transient_us_failure_never_maps_to_a_foreign_suffix(answer):
    # 2026-04 사고 재현: 미국 심볼 조회가 일시 실패해도 AAPL.DE 로 넘어가지 않는다.
    found, probed, save = await _find("AAPL", {"AAPL": answer}, yf_hits={"AAPL.DE"})
    assert found is None
    assert probed == []
    save.assert_not_awaited()
    assert foreign.yf_marked_failed("AAPL")  # 실패 TTL 뒤에 다시 본다


async def test_us_cooldown_never_maps_to_a_foreign_suffix():
    yahoo._start_cooldown(None)
    found, probed, save = await _find("AAPL", {}, yf_hits={"AAPL.DE"})
    assert (found, probed) == (None, [])
    save.assert_not_awaited()


@pytest.mark.parametrize("answer", [httpx.Response(404, json=NOT_FOUND_BODY), httpx.Response(200, json=PHANTOM_CHART)])
async def test_foreign_suffix_only_after_us_listing_is_positively_absent(answer):
    found, probed, save = await _find("SGO", {"SGO": answer}, yf_hits={"SGO.PA"})
    assert found == "SGO.PA"
    save.assert_awaited_once_with("SGO", "SGO.PA")
    assert "SGO" not in probed and probed[-1] == "SGO.PA"


async def test_non_us_codes_keep_suffix_search_without_us_probe():
    calls: list[str] = []
    found, probed, save = await _find("SXR8", {}, yf_hits={"SXR8.DE"}, calls=calls)
    assert found == "SXR8.DE"
    assert calls == []  # 숫자가 섞인 코드는 미국 상장 확인을 하지 않는다
    assert probed == ["SXR8", "SXR8.DE"]
    save.assert_awaited_once_with("SXR8", "SXR8.DE")


async def test_share_class_ticker_resolves_to_yahoo_dash_symbol():
    for code in ("BF-B", "BF.B", "BF/B"):
        found, probed, save = await _find(code, {"BF-B": httpx.Response(200, json=REAL_CHART)}, yf_hits={"BF-B.DE"})
        assert found == "BF-B"
        assert probed == []
        save.assert_awaited_once_with(code, "BF-B")


async def test_known_dead_ticker_is_not_picked_again():
    foreign._dead_ticker_cache.set("SXR8.DE", True)
    found, probed, _ = await _find("SXR8", {}, yf_hits={"SXR8.DE", "SXR8.F"})
    assert found == "SXR8.F"
    assert "SXR8.DE" not in probed


async def test_naver_fallback_accepts_only_us_exchanges_until_us_absence_is_known():
    seen: list[str] = []

    async def naver(code):
        seen.append(code)
        return {"stockName": "x", "reutersCode": code} if code.endswith(".DE") else None

    with patch.object(foreign, "yfinance_find_ticker", AsyncMock(return_value=None)), \
         patch.object(foreign, "fetch_naver_world_stock", new=naver):
        assert await foreign.resolve_foreign_reuters("AAPL") == "AAPL"
        assert seen == ["AAPL", "AAPL.O", "AAPL.K", "AAPL.N"]
        # 미국 상장 없음이 확인된 뒤에는 해외 거래소도 받는다.
        seen.clear()
        foreign._us_listing_absent_cache.set("SGO", True)
        assert await foreign.resolve_foreign_reuters("SGO") == "SGO.DE"
        # 미국식이 아닌 코드는 원래대로 전체 접미사.
        assert await foreign.resolve_foreign_reuters("SXR8") == "SXR8.DE"


# --- 자가 치유 ----------------------------------------------------------------------

class TickerMapHealTests(TempDbMixin):
    async def seed(self):
        await ticker_map_repo.save_ticker("AAPL", "AAPL.DE")
        await ticker_map_repo.save_ticker("EUN2", "EUN2.DE")
        await ticker_map_repo.save_ticker("GOOGL", "GOOGL.O")
        await ticker_map_repo.save_ticker("MSF", "MSF.DE")

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.clock = _Clock()
        self.chart_calls: list[str] = []
        self.chart_routes = {
            "AAPL": httpx.Response(200, json=REAL_CHART),
            "EUN2.DE": httpx.Response(200, json=REAL_CHART),
            "MSF.DE": httpx.Response(503),
        }
        self.kis_calls: list[str] = []
        self.client = _yahoo_client(self.chart_routes, self.chart_calls)
        self.patches = [
            patch.object(cache_layer, "_monotonic", self.clock),
            patch.dict(foreign._ticker_map, {}, clear=True),
            patch.object(foreign, "_ticker_map_loaded", False),
            patch.object(yahoo, "get_http_client", AsyncMock(return_value=self.client)),
            patch.object(foreign, "kis_fetch_foreign_quote", new=self._kis),
            patch.object(foreign, "yfinance_fetch_quote", AsyncMock(return_value={})),
            patch.object(foreign, "fetch_naver_world_stock", AsyncMock(return_value=None)),
            patch.object(foreign.fx, "fx_rate_for_currency", AsyncMock(return_value=1400.0)),
            patch.object(foreign, "yf_run", AsyncMock(side_effect=AssertionError("no yfinance probing"))),
        ]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        for p in reversed(self.patches):
            p.stop()
        await self.client.aclose()
        await super().asyncTearDown()

    async def _kis(self, ticker):
        self.kis_calls.append(ticker)
        return {"price": 463000, "change": 1000, "change_pct": 0.2} if ticker == "AAPL" else {}

    async def _quote(self, code):
        return await quote_service.fetch_external_quote_for_stock_service(code)

    async def test_dead_foreign_mapping_for_us_ticker_heals_on_second_failure(self):
        # 1차 실패: 관측만 하고 매핑은 둔다.
        self.assertEqual(await self._quote("AAPL"), {})
        self.assertEqual(foreign._ticker_map["AAPL"], "AAPL.DE")
        self.assertEqual((await ticker_map_repo.load_ticker_map())["AAPL"], "AAPL.DE")
        # 같은 순간 겹친 실패는 독립 관측이 아니다.
        self.clock.now += 5
        self.assertEqual(await self._quote("AAPL"), {})
        self.assertEqual(foreign._ticker_map["AAPL"], "AAPL.DE")
        # 실패 TTL(5분) 뒤 두 번째 실패: 매핑을 지우고 원래 코드로 바로 다시 받는다.
        self.clock.now += foreign._FAILED_YF_TTL
        quote = await self._quote("AAPL")
        self.assertEqual(quote["price"], 463000)
        self.assertNotIn("AAPL", foreign._ticker_map)
        self.assertNotIn("AAPL", await ticker_map_repo.load_ticker_map())
        self.assertEqual(self.kis_calls[-1], "AAPL")
        # 치유 판단 자체는 업스트림을 더 부르지 않는다: chart 는 매 실패마다 AAPL.DE 1회뿐.
        self.assertEqual(self.chart_calls, ["AAPL.DE", "AAPL.DE", "AAPL.DE"])
        self.assertTrue(foreign.ticker_known_dead("AAPL.DE"))
        # 이후로는 매핑 없이 AAPL 로 조회한다.
        self.chart_calls.clear()
        self.assertEqual((await self._quote("AAPL"))["price"], 463000)
        self.assertEqual(self.chart_calls, [])

    async def test_heal_falls_back_to_us_first_resolution_when_raw_code_also_fails(self):
        self.kis_calls.clear()

        async def kis_down(ticker):
            self.kis_calls.append(ticker)
            return {}

        with patch.object(foreign, "kis_fetch_foreign_quote", new=kis_down):
            await self._quote("AAPL")
            self.clock.now += foreign._FAILED_YF_TTL
            quote = await self._quote("AAPL")
        # 원래 코드 조회가 Yahoo chart(AAPL)로 성공한다.
        self.assertEqual(quote["price"], round(330.74 * 1400))
        self.assertNotIn("AAPL", foreign._ticker_map)

    async def test_strike_window_expiry_restarts_the_count(self):
        await self._quote("AAPL")
        self.clock.now += foreign._MAPPING_STRIKE_WINDOW + 1
        self.assertEqual(await self._quote("AAPL"), {})  # 창이 지나 다시 1차 관측
        self.assertEqual(foreign._ticker_map["AAPL"], "AAPL.DE")

    async def test_success_in_between_resets_the_count(self):
        await self._quote("AAPL")
        self.chart_routes["AAPL.DE"] = httpx.Response(200, json=REAL_CHART)
        self.clock.now += 60
        self.assertTrue(await self._quote("AAPL"))
        del self.chart_routes["AAPL.DE"]
        self.clock.now += foreign._FAILED_YF_TTL
        self.assertEqual(await self._quote("AAPL"), {})
        self.assertEqual(foreign._ticker_map["AAPL"], "AAPL.DE")

    async def test_transient_failures_never_drop_a_mapping(self):
        # MSF → MSF.DE 는 5xx 만 나온다 — "없음" 증거가 아니므로 몇 번이고 그대로 둔다.
        for _ in range(4):
            self.assertEqual(await self._quote("MSF"), {})
            self.clock.now += foreign._FAILED_YF_TTL
        self.assertEqual(foreign._ticker_map["MSF"], "MSF.DE")
        self.assertEqual((await ticker_map_repo.load_ticker_map())["MSF"], "MSF.DE")

    async def test_timeout_after_a_404_is_not_a_second_strike(self):
        await self._quote("AAPL")  # 404 → 1차 관측
        self.chart_routes["AAPL.DE"] = httpx.ReadTimeout("slow")
        self.clock.now += foreign._FAILED_YF_TTL
        # 이번 실패는 시간 초과다 — 5분 전 404 증거를 재사용하지 않는다.
        self.assertEqual(await self._quote("AAPL"), {})
        self.assertEqual(foreign._ticker_map["AAPL"], "AAPL.DE")
        self.assertEqual((await ticker_map_repo.load_ticker_map())["AAPL"], "AAPL.DE")

    async def test_working_and_equivalent_mappings_are_untouched(self):
        self.assertTrue(await self._quote("EUN2"))
        # GOOGL → GOOGL.O 는 같은 Yahoo 심볼이라 지워도 소용없다 — 실패해도 두지 않는다.
        for _ in range(3):
            await self._quote("GOOGL")
            self.clock.now += foreign._FAILED_YF_TTL
        saved = await ticker_map_repo.load_ticker_map()
        self.assertEqual(saved["EUN2"], "EUN2.DE")
        self.assertEqual(saved["GOOGL"], "GOOGL.O")

    async def test_delete_ticker_only_removes_the_expected_value(self):
        self.assertFalse(await ticker_map_repo.delete_ticker("AAPL", expected_ticker="AAPL"))
        self.assertEqual((await ticker_map_repo.load_ticker_map())["AAPL"], "AAPL.DE")
        self.assertTrue(await ticker_map_repo.delete_ticker("AAPL", expected_ticker="AAPL.DE"))
        self.assertNotIn("AAPL", await ticker_map_repo.load_ticker_map())
        self.assertFalse(await ticker_map_repo.delete_ticker("AAPL"))
