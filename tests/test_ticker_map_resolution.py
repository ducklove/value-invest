"""ticker_map 해석 규칙과 자가 치유, 저장 조건.

운영 사고: 미국 종목 AAPL 이 2026-04 에 AAPL.DE(Yahoo 에 없는 심볼)로 저장돼,
조광피혁(004700) 보유지분 자회사 시세를 그리는 화면이 5분마다 404 를 반복했다.
원인은 yfinance 접미사 탐색이 미국 심볼 조회의 일시 실패(429·시간 초과)를
"없음"으로 보고 독일 접미사로 넘어간 것이다.

운영 사고 2: 2026-06-18 밤, 포트폴리오 추가 검색창이 입력 조각마다
/api/portfolio/resolve-name 을 불렀고 그 해석이 매번 ticker_map 에 저장돼
TSL·QQ→QQ.L·GOL→GOL.L·MIC→MIC.L 같은 아무도 보유하지 않은 행이 수십 개 쌓였다.
이제 탐색 결과는 메모리에만 두고 DB 에는 보유 종목만 저장한다.
"""

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import TempDbMixin, seed_user
from starlette.requests import Request

import cache_layer
import repositories.db
from domain.portfolio_codes import is_plain_us_ticker
from repositories import ticker_map as ticker_map_repo
from routes import portfolio as portfolio_routes
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


def _put_request(path: str) -> Request:
    return Request({"type": "http", "method": "PUT", "path": path, "headers": [], "query_string": b"",
                    "client": ("127.0.0.1", 12345), "server": ("testserver", 80), "scheme": "http"})


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
    """yfinance .info 탐색은 yf_hits 에 있는 후보만 맞힌다.

    탐색은 이름 조회·검색어 해석도 거치는 경로라 ticker_map(DB)에 쓰지 않는다 —
    찾은 값은 ``remembered``(메모리 전용 조회 캐시)로 돌려준다."""
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
    save.assert_not_awaited()
    return found, probed, foreign._lookup_ticker_cache.get(code)


async def test_us_listing_wins_even_if_a_foreign_suffix_would_answer():
    found, probed, remembered = await _find("AAPL", {"AAPL": httpx.Response(200, json=REAL_CHART)}, yf_hits={"AAPL.DE"})
    assert found == remembered == "AAPL"
    assert probed == []  # 접미사 탐색(yfinance .info)까지 가지 않는다


@pytest.mark.parametrize(
    "answer",
    [httpx.Response(429), httpx.Response(500), httpx.Response(502), httpx.ReadTimeout("slow"), httpx.ConnectError("down")],
)
async def test_transient_us_failure_never_maps_to_a_foreign_suffix(answer):
    # 2026-04 사고 재현: 미국 심볼 조회가 일시 실패해도 AAPL.DE 로 넘어가지 않는다.
    found, probed, remembered = await _find("AAPL", {"AAPL": answer}, yf_hits={"AAPL.DE"})
    assert found is None and remembered is None
    assert probed == []
    assert foreign.yf_marked_failed("AAPL")  # 실패 TTL 뒤에 다시 본다


async def test_us_cooldown_never_maps_to_a_foreign_suffix():
    yahoo._start_cooldown(None)
    found, probed, remembered = await _find("AAPL", {}, yf_hits={"AAPL.DE"})
    assert (found, probed, remembered) == (None, [], None)


@pytest.mark.parametrize("answer", [httpx.Response(404, json=NOT_FOUND_BODY), httpx.Response(200, json=PHANTOM_CHART)])
async def test_foreign_suffix_only_after_us_listing_is_positively_absent(answer):
    found, probed, remembered = await _find("SGO", {"SGO": answer}, yf_hits={"SGO.PA"})
    assert found == remembered == "SGO.PA"
    assert "SGO" not in probed and probed[-1] == "SGO.PA"


async def test_non_us_codes_keep_suffix_search_without_us_probe():
    calls: list[str] = []
    found, probed, remembered = await _find("SXR8", {}, yf_hits={"SXR8.DE"}, calls=calls)
    assert found == remembered == "SXR8.DE"
    assert calls == []  # 숫자가 섞인 코드는 미국 상장 확인을 하지 않는다
    assert probed == ["SXR8", "SXR8.DE"]


async def test_share_class_ticker_resolves_to_yahoo_dash_symbol():
    for code in ("BF-B", "BF.B", "BF/B"):
        found, probed, remembered = await _find(code, {"BF-B": httpx.Response(200, json=REAL_CHART)}, yf_hits={"BF-B.DE"})
        assert found == remembered == "BF-B"
        assert probed == []


async def test_fresh_chart_404_skips_the_redundant_us_probe():
    yahoo._missing_symbols.set("SGO", True)  # 방금 시세 체인의 chart 가 SGO 404 를 봤다
    calls: list[str] = []
    found, probed, remembered = await _find("SGO", {}, yf_hits={"SGO.PA"}, calls=calls)
    assert found == remembered == "SGO.PA"
    assert calls == []  # 같은 chart 를 다시 부르지 않는다


async def test_known_dead_ticker_is_not_picked_again():
    foreign._dead_ticker_cache.set("SXR8.DE", True)
    found, probed, _ = await _find("SXR8", {}, yf_hits={"SXR8.DE", "SXR8.F"})
    assert found == "SXR8.F"
    assert "SXR8.DE" not in probed


async def test_repeat_lookup_is_answered_from_memory_without_probing():
    found, _, _ = await _find("SGO", {"SGO": httpx.Response(404, json=NOT_FOUND_BODY)}, yf_hits={"SGO.PA"})
    assert found == "SGO.PA"
    calls: list[str] = []
    again, probed, _ = await _find("SGO", {}, calls=calls)
    assert again == "SGO.PA"
    assert (probed, calls) == ([], [])
    # 치유가 내린 티커는 조회 캐시에 남아 있어도 다시 쓰지 않는다.
    foreign._dead_ticker_cache.set("SGO.PA", True)
    third, probed, _ = await _find("SGO", {}, yf_hits={"SGO.PA", "SGO.F"})
    assert third == "SGO.F"
    assert "SGO.PA" not in probed


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


async def test_naver_fallback_trusts_a_fresh_chart_404_as_us_absence():
    # 시세 체인의 Yahoo chart 가 방금 "없음"을 확인했으면 추가 호출 없이 해외 거래소도 받는다
    # (HPG 는 호찌민 상장 — 시세 경로에서는 yfinance 실패 표시 때문에 상장 확인까지 못 간다).
    clock = _Clock()
    seen: list[str] = []

    async def naver(code):
        seen.append(code)
        return {"stockName": "x", "reutersCode": code} if code.endswith(".HM") else None

    async with _yahoo_client({}) as client:
        with patch.object(cache_layer, "_monotonic", clock), \
             patch.object(yahoo, "get_http_client", AsyncMock(return_value=client)), \
             patch.object(foreign, "yfinance_find_ticker", AsyncMock(return_value=None)), \
             patch.object(foreign, "fetch_naver_world_stock", new=naver):
            await yahoo.fetch_close_series("HPG", range_="5d")  # 404 → 없음 증거
            assert await foreign.resolve_foreign_reuters("HPG") == "HPG.HM"
            # 오래된 증거(체인 밖)는 세지 않는다 — 다시 미국 거래소만.
            foreign.reset_resolution_state()
            seen.clear()
            clock.now += foreign._MAPPING_EVIDENCE_MAX_AGE + 1
            assert await foreign.resolve_foreign_reuters("HPG") == "HPG"
            assert seen == ["HPG", "HPG.O", "HPG.K", "HPG.N"]


async def test_naver_fallback_never_returns_a_ticker_healing_just_dropped():
    foreign._dead_ticker_cache.set("SXR8.DE", True)
    foreign._dead_ticker_cache.set("ABC.F", True)

    async def naver(code):
        if code in ("SXR8.DE", "SXR8.F"):
            return {"stockName": "x", "reutersCode": code}
        if code == "ABC.DE":  # 점이 있는 코드: 네이버가 다른 상장(죽은 티커)으로 돌려주는 경우
            return {"stockName": "x", "reutersCode": "ABC.F"}
        return None

    with patch.object(foreign, "yfinance_find_ticker", AsyncMock(return_value=None)), \
         patch.object(foreign, "fetch_naver_world_stock", new=naver):
        assert await foreign.resolve_foreign_reuters("SXR8") == "SXR8.F"
        assert await foreign.resolve_foreign_reuters("ABC.DE") == "ABC.DE"


# --- 자가 치유 ----------------------------------------------------------------------

async def _hold(*codes: str) -> None:
    """누군가(u1) 보유한 종목으로 만든다 — ticker_map 저장 조건. seed_user() 뒤에 부른다."""
    db = await repositories.db.get_db()
    await db.executemany(
        "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at)"
        " VALUES ('u1', ?, ?, 1, 0, '2026-10-01T00:00:00', '2026-10-01T00:00:00')",
        [(code, code) for code in codes],
    )
    await db.commit()


class TickerMapHealTests(TempDbMixin):
    async def seed(self):
        await seed_user()
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

    def _hpg_naver(self, naver_calls: list[str] | None = None, *, priced: bool = True):
        """호찌민 HPG: 네이버에는 HPG.HM 만 있다. ``priced=False`` 면 이름만 있고 시세가 비었다."""

        async def naver(code):
            if naver_calls is not None:
                naver_calls.append(code)
            if code != "HPG.HM":
                return None
            row = {"stockName": "Hoa Phat", "reutersCode": "HPG.HM", "nationType": "VNM"}
            if priced:
                row.update(closePrice="27,000", compareToPreviousClosePrice="100", fluctuationsRatio="0.37")
            return row

        return naver

    @staticmethod
    async def _fast_info_fails(ticker):
        foreign.yf_mark_failed(ticker)
        return {}

    async def test_unmapped_plain_non_us_code_resolves_from_the_chains_own_404(self):
        # 호찌민 HPG: Yahoo HPG 404, 네이버 HPG 409, HPG.HM 만 있다. 운영처럼 fast_info
        # 실패가 yfinance 실패 표시를 남겨 상장 확인(probe)까지 가지 않아도 풀려야 한다.
        await _hold("HPG")  # 보유 종목이라 풀린 매핑을 저장한다
        naver_calls: list[str] = []
        with patch.object(foreign, "fetch_naver_world_stock", new=self._hpg_naver(naver_calls)), \
             patch.object(foreign, "yfinance_fetch_quote", new=self._fast_info_fails), \
             patch.object(foreign.fx, "fx_to_krw", AsyncMock(side_effect=lambda nation, amount: amount * 0.055)):
            quote = await self._quote("HPG")
        self.assertEqual(quote["price"], round(27000 * 0.055))
        self.assertEqual(foreign._ticker_map["HPG"], "HPG.HM")
        self.assertEqual((await ticker_map_repo.load_ticker_map())["HPG"], "HPG.HM")
        self.assertEqual(self.chart_calls, ["HPG"])  # 상장 확인용 추가 chart 호출 없음
        self.assertIn("HPG.HM", naver_calls)

    async def test_quote_mapping_for_a_code_nobody_holds_stays_in_memory(self):
        # 분석 화면·벤치마크처럼 보유하지 않은 코드: 이번 프로세스에서는 매핑을 쓰되 DB 에는 없다.
        with patch.object(foreign, "fetch_naver_world_stock", new=self._hpg_naver()), \
             patch.object(foreign, "yfinance_fetch_quote", new=self._fast_info_fails), \
             patch.object(foreign.fx, "fx_to_krw", AsyncMock(side_effect=lambda nation, amount: amount * 0.055)):
            self.assertTrue(await self._quote("HPG"))
            self.assertEqual(foreign._ticker_map["HPG"], "HPG.HM")
            self.assertNotIn("HPG", await ticker_map_repo.load_ticker_map())
            # 다음 조회는 매핑 티커로 바로 간다(원래 코드 chart·재해석 없음).
            self.chart_calls.clear()
            self.assertTrue(await self._quote("HPG"))
            self.assertEqual(self.chart_calls, [])
            self.assertNotIn("HPG", await ticker_map_repo.load_ticker_map())

    async def test_held_mapping_is_saved_only_once_its_quote_resolves(self):
        await _hold("HPG")
        fx_patch = patch.object(foreign.fx, "fx_to_krw", AsyncMock(side_effect=lambda nation, amount: amount * 0.055))
        with fx_patch, patch.object(foreign, "yfinance_fetch_quote", new=self._fast_info_fails):
            # 네이버가 이름만 주고 시세가 비었다 — 매핑은 메모리에만.
            with patch.object(foreign, "fetch_naver_world_stock", new=self._hpg_naver(priced=False)):
                self.assertEqual(await self._quote("HPG"), {})
            self.assertEqual(foreign._ticker_map["HPG"], "HPG.HM")
            self.assertNotIn("HPG", await ticker_map_repo.load_ticker_map())
            # 매핑 티커로 시세를 받은 순간 저장한다.
            with patch.object(foreign, "fetch_naver_world_stock", new=self._hpg_naver()):
                self.assertTrue(await self._quote("HPG"))
        self.assertEqual((await ticker_map_repo.load_ticker_map())["HPG"], "HPG.HM")

    async def test_unmapped_us_ticker_with_transient_chart_failure_never_takes_a_foreign_listing(self):
        # 같은 경로에서 미국 조회가 5xx·시간 초과로 실패하면 "없음" 증거가 아니다.
        self.chart_routes["NVDA"] = httpx.ReadTimeout("slow")

        async def naver(code):
            return {"stockName": "x", "reutersCode": code} if code.endswith(".DE") else None

        async def fast_info_fails(ticker):
            foreign.yf_mark_failed(ticker)
            return {}

        with patch.object(foreign, "fetch_naver_world_stock", new=naver), \
             patch.object(foreign, "yfinance_fetch_quote", new=fast_info_fails), \
             patch.object(foreign, "kis_fetch_foreign_quote", AsyncMock(return_value={})):
            self.assertEqual(await self._quote("NVDA"), {})
        self.assertNotIn("NVDA", foreign._ticker_map)
        self.assertNotIn("NVDA", await ticker_map_repo.load_ticker_map())

    async def test_heal_re_resolves_a_plain_non_us_code_to_its_live_listing(self):
        # SGO → SGO.DE 가 죽고 SGO.PA 가 살아 있다. 치유 뒤 원래 코드 조회의 chart 404 가
        # 미국 상장 없음 증거가 되어 네이버 폴백이 SGO.PA 로 다시 푼다.
        await ticker_map_repo.save_ticker("SGO", "SGO.DE")
        await _hold("SGO")
        self.chart_routes["SGO.PA"] = httpx.Response(200, json=REAL_CHART)

        async def naver(code):
            return {"stockName": "Saint-Gobain", "reutersCode": code} if code in ("SGO.DE", "SGO.PA") else None

        async def fast_info_fails(ticker):
            foreign.yf_mark_failed(ticker)
            return {}

        with patch.object(foreign, "fetch_naver_world_stock", new=naver), \
             patch.object(foreign, "yfinance_fetch_quote", new=fast_info_fails):
            self.assertEqual(await self._quote("SGO"), {})  # 1차 관측
            self.clock.now += foreign._FAILED_YF_TTL + 5
            quote = await self._quote("SGO")  # 2차 → 삭제 → SGO.PA 로 재해석
        self.assertEqual(quote["price"], round(330.74 * 1400))
        self.assertEqual(foreign._ticker_map["SGO"], "SGO.PA")
        self.assertEqual((await ticker_map_repo.load_ticker_map())["SGO"], "SGO.PA")

    async def test_healed_mapping_is_not_saved_back_from_naver(self):
        # 네이버가 SXR8.DE 이름은 알지만 시세가 비어 있는 경우 — 지운 매핑을 같은 주기에
        # 네이버 폴백이 되살리면 10분마다 지움·저장을 되풀이한다.
        await ticker_map_repo.save_ticker("SXR8", "SXR8.DE")

        async def naver(code):
            return {"stockName": "iShares", "reutersCode": "SXR8.DE"} if code == "SXR8.DE" else None

        with patch.object(foreign, "fetch_naver_world_stock", new=naver), \
             patch.object(foreign, "yf_run", AsyncMock(return_value=None)):
            self.assertEqual(await self._quote("SXR8"), {})  # 1차 관측
            self.clock.now += foreign._FAILED_YF_TTL + 5
            self.assertEqual(await self._quote("SXR8"), {})  # 2차 → 삭제
            self.assertTrue(foreign.ticker_known_dead("SXR8.DE"))
            self.assertNotIn("SXR8", foreign._ticker_map)
            self.assertNotIn("SXR8", await ticker_map_repo.load_ticker_map())
            for _ in range(3):
                foreign._failed_yf_cache.clear()
                self.clock.now += foreign._FAILED_YF_TTL + 5
                self.assertEqual(await self._quote("SXR8"), {})
                self.assertNotIn("SXR8", await ticker_map_repo.load_ticker_map())

    async def test_delete_ticker_only_removes_the_expected_value(self):
        self.assertFalse(await ticker_map_repo.delete_ticker("AAPL", expected_ticker="AAPL"))
        self.assertEqual((await ticker_map_repo.load_ticker_map())["AAPL"], "AAPL.DE")
        self.assertTrue(await ticker_map_repo.delete_ticker("AAPL", expected_ticker="AAPL.DE"))
        self.assertNotIn("AAPL", await ticker_map_repo.load_ticker_map())
        self.assertFalse(await ticker_map_repo.delete_ticker("AAPL"))


# --- 이름 조회·보유 등록과 ticker_map 저장 ------------------------------------------------

class TickerMapPersistenceTests(TempDbMixin):
    """resolve-name·이름 조회는 ticker_map 에 쓰지 않고, 보유 등록이 확정된 코드만 저장한다."""

    async def seed(self):
        await seed_user()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.chart_calls: list[str] = []
        self.probed: list[str] = []
        # 2026-06-18 의 입력 조각들: GOL·MIC 은 미국에 없고 런던에만, TSL 은 미국 상장(가상).
        self.chart_routes = {"TSL": httpx.Response(200, json=REAL_CHART), "AAPL": httpx.Response(200, json=REAL_CHART)}
        self.yf_hits = {"GOL.L", "MIC.L", "SGO.PA", "AAPL.DE"}
        self.naver_codes = {"GOL.L", "MIC.L", "SGO.PA", "TSL"}
        self.client = _yahoo_client(self.chart_routes, self.chart_calls)
        self.patches = [
            patch.dict(foreign._ticker_map, {}, clear=True),
            patch.object(yahoo, "get_http_client", AsyncMock(return_value=self.client)),
            patch.object(yahoo, "MAX_INLINE_RETRY_DELAY", 0.0),
            patch.object(foreign, "yf_run", new=self._yf_run),
            patch.object(foreign, "fetch_naver_world_stock", new=self._naver),
        ]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        for p in reversed(self.patches):
            p.stop()
        await self.client.aclose()
        await super().asyncTearDown()

    async def _yf_run(self, fn):
        candidate = fn.args[0]
        self.probed.append(candidate)
        return candidate if candidate in self.yf_hits else None

    async def _naver(self, code):
        return {"stockName": f"{code} name", "reutersCode": code} if code in self.naver_codes else None

    async def test_resolve_name_endpoint_never_writes_ticker_map(self):
        for fragment in ("GOL", "MIC", "TSL"):
            await portfolio_routes.resolve_name(code=fragment)
        self.assertEqual(await ticker_map_repo.load_ticker_map(), {})
        self.assertEqual(foreign._ticker_map, {})
        self.assertEqual(foreign._lookup_ticker_cache.get("GOL"), "GOL.L")
        # 같은 조각을 다시 쳐도 메모리 캐시가 답한다(접미사 탐색·chart 재호출 없음).
        self.probed.clear()
        self.chart_calls.clear()
        response = await portfolio_routes.resolve_name(code="GOL")
        self.assertEqual(response["stock_code"], "GOL.L")
        self.assertEqual((self.probed, self.chart_calls), ([], []))
        self.assertEqual(await ticker_map_repo.load_ticker_map(), {})

    async def test_name_lookup_keeps_us_first_rule_without_writing(self):
        await foreign.resolve_name("AAPL")
        self.assertEqual(foreign._lookup_ticker_cache.get("AAPL"), "AAPL")
        self.assertNotIn("AAPL.DE", self.probed)  # 미국 상장이 있으면 해외 접미사 탐색까지 가지 않는다
        self.assertEqual(await ticker_map_repo.load_ticker_map(), {})

    async def test_holding_added_under_the_canonical_code_does_not_save_the_fragment(self):
        response = await portfolio_routes.resolve_name(code="GOL")
        self.assertEqual(response["stock_code"], "GOL.L")
        # UI 는 resolve-name 이 돌려준 정식 코드로 등록한다 — GOL → GOL.L 매핑은 쓸 일이 없다.
        self.assertFalse(await foreign.save_holding_ticker("GOL.L"))
        self.assertEqual(await ticker_map_repo.load_ticker_map(), {})

    async def test_holding_added_by_raw_code_saves_its_lookup(self):
        # 이름 없이 원래 코드(SGO)로 등록: 이름 조회가 찾은 SGO.PA 를 등록 확정 뒤 저장한다.
        request = _put_request("/api/portfolio/SGO")
        saver = AsyncMock(return_value={"stock_code": "SGO", "stock_name": "SGO.PA"})
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("repositories.portfolio.save_portfolio_item", new=saver), \
             patch("repositories.portfolio.get_portfolio_item", new=AsyncMock(return_value=None)), \
             patch("repositories.foreign_dividends.get_foreign_dividend", new=AsyncMock(return_value={"dps_krw": 0})), \
             patch("routes.portfolio.dividends.schedule_for_portfolio"):
            await portfolio_routes.save_portfolio_item("SGO", request, {"stock_name": "", "quantity": 1, "avg_price": 0})
        saver.assert_awaited_once()
        self.assertEqual(await ticker_map_repo.load_ticker_map(), {"SGO": "SGO.PA"})
        self.assertEqual(foreign._ticker_map["SGO"], "SGO.PA")

    async def test_save_holding_ticker_skips_self_mappings_saved_rows_and_unknown_codes(self):
        await foreign.resolve_name("TSL")
        self.assertEqual(foreign._lookup_ticker_cache.get("TSL"), "TSL")
        self.probed.clear()
        self.assertFalse(await foreign.save_holding_ticker("TSL"))  # 자기 매핑은 저장할 이유가 없다
        self.assertFalse(await foreign.save_holding_ticker("NVDA"))  # 조회한 적 없는 코드는 새로 찾지 않는다
        await foreign.save_ticker("SGO", "SGO.DE")
        foreign._lookup_ticker_cache.set("SGO", "SGO.PA")
        self.assertFalse(await foreign.save_holding_ticker("SGO"))  # 이미 저장된 매핑은 그대로
        self.assertEqual(await ticker_map_repo.load_ticker_map(), {"SGO": "SGO.DE"})
        self.assertEqual(self.probed, [])
