"""GET /api/device/portfolio — ePaper 기기용 읽기 전용 요약 엔드포인트."""

import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException, Request

from routes import device
from services.portfolio import device_summary


def _request(headers: dict[str, str] | None = None) -> Request:
    encoded = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in (headers or {}).items()
    ]
    return Request({
        "type": "http",
        "method": "GET",
        "path": "/api/device/portfolio",
        "headers": encoded,
        "query_string": b"",
        "client": ("192.168.68.90", 12345),
        "server": ("testserver", 80),
    })


class DeviceTokenTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_when_token_not_configured(self):
        # internal.py 와 달리 loopback 예외가 없다 — 미설정이면 그냥 꺼진 것.
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request(), top=10)
        self.assertEqual(ctx.exception.status_code, 503)

    async def test_rejects_wrong_token(self):
        env = {"DEVICE_API_TOKEN": "right", "DEVICE_USER_EMAIL": "u@example.test"}
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request({"X-Device-Token": "wrong"}), top=10)
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_rejects_missing_token_header(self):
        env = {"DEVICE_API_TOKEN": "right", "DEVICE_USER_EMAIL": "u@example.test"}
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request(), top=10)
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_requires_device_user_email(self):
        with patch.dict("os.environ", {"DEVICE_API_TOKEN": "right"}, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request({"X-Device-Token": "right"}), top=10)
        self.assertEqual(ctx.exception.status_code, 503)

    async def test_unknown_device_user_is_404(self):
        env = {"DEVICE_API_TOKEN": "right", "DEVICE_USER_EMAIL": "nobody@example.test"}
        with patch.dict("os.environ", env, clear=True), \
             patch("repositories.users.get_user_by_email", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request({"X-Device-Token": "right"}), top=10)
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_valid_token_returns_summary_with_timestamp(self):
        env = {"DEVICE_API_TOKEN": "right", "DEVICE_USER_EMAIL": "u@example.test"}
        summary = {"total_value": 1, "holdings": []}
        with patch.dict("os.environ", env, clear=True), \
             patch("repositories.users.get_user_by_email",
                   new=AsyncMock(return_value={"google_sub": "sub-1"})), \
             patch("services.portfolio.device_summary.build_summary",
                   new=AsyncMock(return_value=summary)) as build:
            result = await device.device_portfolio(
                _request({"X-Device-Token": "right"}), top=5, movers=8
            )

        build.assert_awaited_once_with("sub-1", top_n=5, movers_n=8)
        self.assertEqual(result["total_value"], 1)
        self.assertIn("generated_at", result)


# 서비스에서 정한 순서대로. 평가액 순이라면 AAPL 이 먼저 와야 한다.
# 국내 6자리 코드는 벌크 경로로, 그 외는 개별 경로로 빠진다.
KR = "005930"
US = "AAPL"
ITEMS = [
    {"stock_code": KR, "stock_name": "삼성전자", "quantity": 10, "group_name": "한국주식"},
    {"stock_code": US, "stock_name": "Apple Inc.", "quantity": 2, "group_name": "해외주식"},
    {"stock_code": "IDX_KOSPI", "stock_name": "코스피", "quantity": 0, "group_name": "참조"},
    {"stock_code": "NOQUOTE", "stock_name": "시세없음", "quantity": 5, "group_name": "한국주식"},
]

# price/change_pct 는 시세 계층이 이미 원화로 환산해 내려준 값이다.
BULK = {KR: {"price": 1200, "change_pct": 1.69}}
PER_CODE = {US: {"price": 200_000, "change_pct": -0.5}}

LATEST = {"date": "2026-08-10", "nav": 4000.0, "total_units": 100.0, "total_value": 400_000}
BASELINE = {"date": "2026-08-10", "total_value": 400_000, "nav": 4000.0}
YEAR_START = {"date": "2025-12-30", "total_value": 300_000, "nav": 3000.0}


class BuildSummaryMixin:
    async def _build(self, *, items=None, cashflows=(), latest=LATEST,
                     baseline=BASELINE, year_start=YEAR_START, top_n=10,
                     movers_n=10, stale_quote=None, bulk=None, brief=None):
        """stale_quote 를 주면 개별 조회까지 실패한 코드가 낡은 캐시로 채워진다."""
        async def fetch_quote(code, **kwargs):
            return PER_CODE.get(code, {})

        with patch("repositories.portfolio.get_portfolio",
                   new=AsyncMock(return_value=list(items if items is not None else ITEMS))), \
             patch("services.stock_quotes.get_bulk_quote_snapshots",
                   new=AsyncMock(return_value=dict(BULK if bulk is None else bulk))), \
             patch("services.portfolio.quote_service.fetch_quote", new=fetch_quote), \
             patch("services.stock_quotes.get_stock_cached",
                   return_value=object() if stale_quote else None), \
             patch("services.stock_quotes.stock_to_quote",
                   return_value=dict(stale_quote) if stale_quote else {}), \
             patch("repositories.snapshots.get_snapshot_on_or_before",
                   new=AsyncMock(return_value=baseline)), \
             patch("repositories.snapshots.get_cashflows_created_after",
                   new=AsyncMock(return_value=list(cashflows))), \
             patch("repositories.snapshots.get_latest_snapshot",
                   new=AsyncMock(return_value=latest)), \
             patch("repositories.market_brief.get_daily_market_brief",
                   new=brief if callable(brief) else AsyncMock(return_value=brief)), \
             patch("repositories.snapshots.get_year_start_snapshot",
                   new=AsyncMock(return_value=year_start)):
            return await device_summary.build_summary(
                "sub-1", top_n=top_n, movers_n=movers_n
            )


class DeviceSummaryTests(BuildSummaryMixin, unittest.IsolatedAsyncioTestCase):
    async def test_empty_portfolio_returns_zeroed_summary(self):
        with patch("repositories.portfolio.get_portfolio", new=AsyncMock(return_value=[])):
            result = await device_summary.build_summary("sub-1")
        self.assertEqual(result["total_value"], 0.0)
        self.assertEqual(result["holdings"], [])
        self.assertIsNone(result["ytd_pnl_pct"])

    async def test_holdings_keep_the_order_set_in_the_service(self):
        """평가액 순으로 다시 정렬하지 않는다 — 사용자가 정한 순서가 화면 순서다."""
        result = await self._build()
        self.assertEqual([row["code"] for row in result["holdings"]], [KR, US])

    async def test_quotes_missing_from_bulk_are_fetched_individually(self):
        """캐시만 보고 포기하면 그 종목이 총액에서 빠져 화면이 작아진다."""
        result = await self._build()
        beta = next(row for row in result["holdings"] if row["code"] == US)
        self.assertEqual(beta["value"], 400_000)
        self.assertEqual(result["total_value"], 412_000)

    async def test_reference_rows_are_not_counted_as_unpriced(self):
        """IDX_/FX_ 는 보유가 아니라 참조행이라 시세가 없는 게 정상이다."""
        result = await self._build()
        self.assertEqual(result["unpriced"], 1)          # NOQUOTE 만
        self.assertNotIn("IDX_KOSPI", [r["code"] for r in result["holdings"]])

    async def test_stale_fallback_keeps_the_holding_in_the_total(self):
        result = await self._build(
            items=[{"stock_code": "NOQUOTE", "stock_name": "낡은시세", "quantity": 5}],
            stale_quote={"price": 1000},
        )
        self.assertEqual(result["total_value"], 5000)
        self.assertEqual(result["unpriced"], 0)
        self.assertTrue(result["stale"])

    async def test_day_return_is_nav_based(self):
        # 현재 NAV = 412,000 / 100좌 = 4,120 ; 기준 NAV = 4,000
        result = await self._build()
        self.assertEqual(result["day_pnl_pct"], 3.0)
        self.assertEqual(result["day_pnl"], 12_000)

    async def test_ytd_return_is_nav_based_not_cost_based(self):
        """매입가 대비가 아니라 연초 NAV 대비다 — 입출금이 성과로 잡히면 안 된다."""
        # 4,120 / 3,000 - 1 = 37.33%
        result = await self._build()
        self.assertEqual(result["ytd_pnl_pct"], 37.33)
        self.assertEqual(result["ytd_pnl"], 112_000)

    async def test_pending_deposit_is_removed_from_today_return(self):
        """오늘 입금 10만원은 수익이 아니다 — 손익에서 빼고 NAV 좌수로 흡수한다."""
        cashflows = [{"id": 1, "type": "deposit", "amount": 100_000,
                      "nav_at_time": 4000.0, "units_change": 25.0, "created_at": "x"}]
        result = await self._build(cashflows=cashflows)
        # 손익 = 412,000 - 400,000 - 100,000
        self.assertEqual(result["day_pnl"], -88_000)
        # NAV = 412,000 / (100 + 25) = 3,296 → (3296/4000 - 1) = -17.6%
        self.assertEqual(result["day_pnl_pct"], -17.6)

    async def test_cashflow_without_units_falls_back_to_value_adjustment(self):
        cashflows = [{"id": 1, "type": "deposit", "amount": 12_000,
                      "nav_at_time": None, "units_change": None, "created_at": "x"}]
        result = await self._build(cashflows=cashflows)
        # 좌수 정보가 없으면 평가액에서 빼고 기존 좌수로 나눈다: 400,000/100 = 4,000
        self.assertEqual(result["day_pnl_pct"], 0.0)

    async def test_ytd_amount_is_the_value_difference_even_when_it_opposes_the_percent(self):
        """수익률과 금액은 정의가 다르다 — 부호가 갈려도 오류가 아니다.

        연중 출금이 있으면 평가액은 줄어도 NAV 성과는 플러스일 수 있다.
        수익률은 NAV 비교(성과), 금액은 평가액 차이(잔고 변화)를 뜻하고 웹도
        같은 정의를 쓴다. 여기서 금액을 NAV 로 환산해 부호를 맞추면 실제
        잔고가 얼마나 줄었는지가 화면에서 사라진다.
        """
        # 연초 평가액이 지금보다 큰 계좌(그 사이 출금)인데 NAV 는 올랐다.
        year_start = {"date": "2025-12-30", "total_value": 900_000, "nav": 3000.0}
        result = await self._build(year_start=year_start)
        self.assertEqual(result["ytd_pnl_pct"], 37.33)     # NAV 기준 성과
        self.assertEqual(result["ytd_pnl"], -488_000)      # 412,000 - 900,000

    async def test_missing_snapshots_leave_returns_blank_not_zero(self):
        result = await self._build(baseline=None, year_start=None, latest=None)
        self.assertIsNone(result["day_pnl"])
        self.assertIsNone(result["day_pnl_pct"])
        self.assertIsNone(result["ytd_pnl"])
        self.assertIsNone(result["ytd_pnl_pct"])
        self.assertEqual(result["total_value"], 412_000)

    async def test_weights_are_relative_to_the_whole_portfolio(self):
        result = await self._build()
        weights = {row["code"]: row["weight_pct"] for row in result["holdings"]}
        self.assertEqual(weights[KR], 2.91)
        self.assertEqual(weights[US], 97.09)

    async def test_top_n_truncates_holdings_but_not_the_count(self):
        result = await self._build(top_n=1)
        self.assertEqual([row["code"] for row in result["holdings"]], [KR])
        self.assertEqual(result["holdings_count"], 2)


class GroupAndMoverTests(BuildSummaryMixin, unittest.IsolatedAsyncioTestCase):
    """그룹 합계와 '오늘 움직인 종목'.

    두 값 모두 화면이 종목 전체를 담지 못해서 대신 넣는 것이므로, ``top_n`` 으로
    잘리기 전 전체 행에서 계산돼야 한다.
    """

    async def test_day_pnl_is_reconstructed_from_the_change_percent(self):
        # 12,000 이 +1.69% 오른 값이므로 전일은 11,800.57, 변동액은 199.
        result = await self._build()
        by_code = {row["code"]: row for row in result["holdings"]}
        self.assertEqual(by_code[KR]["day_pnl"], 199)
        self.assertEqual(by_code[US]["day_pnl"], -2010)

    async def test_groups_follow_the_holding_order_and_sum_the_whole_portfolio(self):
        result = await self._build()
        groups = {g["name"]: g for g in result["groups"]}
        self.assertEqual([g["name"] for g in result["groups"]], ["한국주식", "해외주식"])
        self.assertEqual(groups["한국주식"]["value"], 12_000)
        self.assertEqual(groups["해외주식"]["value"], 400_000)
        self.assertEqual(groups["해외주식"]["weight_pct"], 97.09)

    async def test_group_count_only_covers_rows_that_have_a_price(self):
        """시세를 못 구한 종목은 평가액에 없으니 개수에도 넣지 않는다.

        넣으면 '한국주식(2) 1.2만' 처럼 개수와 금액의 근거가 어긋난다.
        미확보 건수는 푸터의 unpriced 로 따로 보여준다.
        """
        result = await self._build()
        groups = {g["name"]: g for g in result["groups"]}
        self.assertEqual(groups["한국주식"]["count"], 1)   # NOQUOTE 는 빠진다
        self.assertEqual(result["unpriced"], 1)
        self.assertNotIn("참조", groups)                   # IDX_ 행도 마찬가지

    async def test_group_percent_round_trips_back_to_the_quote(self):
        """그룹에 한 종목뿐이면 그룹 등락률은 그 종목의 등락률과 같아야 한다."""
        result = await self._build()
        groups = {g["name"]: g for g in result["groups"]}
        self.assertEqual(groups["한국주식"]["day_pct"], 1.69)
        self.assertEqual(groups["해외주식"]["day_pct"], -0.5)

    async def test_ungrouped_holdings_fall_into_one_bucket(self):
        result = await self._build(
            items=[{"stock_code": KR, "stock_name": "삼성전자", "quantity": 10}]
        )
        self.assertEqual([g["name"] for g in result["groups"]], [device_summary.UNGROUPED])

    async def test_movers_are_picked_by_size_then_ordered_by_sign(self):
        """절대값으로 뽑아야 큰 하락을 놓치지 않고, 부호 순으로 내보내야
        화면 위쪽이 상승 아래쪽이 하락으로 갈린다."""
        result = await self._build()
        self.assertEqual([row["code"] for row in result["movers"]], [KR, US])
        self.assertEqual(result["movers"][0]["day_pnl"], 199)
        self.assertEqual(result["movers"][-1]["day_pnl"], -2010)

    async def test_movers_limit_keeps_the_biggest_move_not_the_first_row(self):
        result = await self._build(movers_n=1)
        self.assertEqual([row["code"] for row in result["movers"]], [US])

    async def test_groups_and_movers_ignore_the_holdings_cutoff(self):
        result = await self._build(top_n=1)
        self.assertEqual(len(result["holdings"]), 1)
        self.assertEqual(len(result["groups"]), 2)
        self.assertEqual(len(result["movers"]), 2)

    async def test_flat_holdings_are_not_movers(self):
        """0% 는 '움직인 종목' 이 아니다 — 자리를 차지하면 안 된다."""
        result = await self._build(
            items=[{"stock_code": KR, "stock_name": "삼성전자", "quantity": 10}],
            bulk={KR: {"price": 1200, "change_pct": 0.0}},
        )
        self.assertEqual(result["movers"], [])
        self.assertEqual(result["groups"][0]["day_pct"], 0.0)

    async def test_long_short_pairs_become_one_row(self):
        """헤지의 한쪽만 세우면 '오늘 크게 빠진 종목' 으로 읽힌다.

        숏 다리는 수량이 음수라 평가액도 변동액도 자연히 음수다 — 합치면
        그대로 세트의 순손익이 된다.
        """
        items = [
            {"stock_code": KR, "stock_name": "두산", "quantity": 10, "group_name": "국내"},
            {"stock_code": US, "stock_name": "두산2우B", "quantity": -2,
             "group_name": "국내", "pair_long_code": KR},
        ]
        result = await self._build(items=items)
        self.assertEqual([m["name"] for m in result["movers"]], ["두산"])
        # 롱 +199, 숏 -2 x 200,000 = -400,000 이 -0.5% → +2010
        self.assertEqual(result["movers"][0]["day_pnl"], 199 + 2010)
        self.assertTrue(result["movers"][0]["pair"])

    async def test_a_merged_pair_has_no_percent(self):
        """순평가액이 0 근처면 비율이 발산한다. 웹에 없는 분모를 여기서 만들면
        같은 세트가 두 화면에서 다른 숫자로 보인다."""
        items = [
            {"stock_code": KR, "stock_name": "두산", "quantity": 10},
            {"stock_code": US, "stock_name": "두산2우B", "quantity": -2, "pair_long_code": KR},
        ]
        result = await self._build(items=items)
        self.assertIsNone(result["movers"][0]["day_pct"])

    async def test_unpaired_holdings_keep_their_own_percent(self):
        result = await self._build()
        self.assertEqual([m["pair"] for m in result["movers"]], [False, False])
        self.assertEqual(result["movers"][0]["day_pct"], 1.69)

    async def test_pair_legs_still_count_separately_in_their_group(self):
        """합치는 것은 movers 목록뿐이다 — 그룹 합계는 두 다리 모두 반영한다."""
        items = [
            {"stock_code": KR, "stock_name": "두산", "quantity": 10, "group_name": "국내"},
            {"stock_code": US, "stock_name": "두산2우B", "quantity": -2,
             "group_name": "국내", "pair_long_code": KR},
        ]
        result = await self._build(items=items)
        self.assertEqual(result["groups"][0]["count"], 2)
        self.assertEqual(result["groups"][0]["value"], 12_000 - 400_000)

    async def test_a_pair_pointing_at_a_missing_long_is_left_alone(self):
        """롱이 시세를 못 구해 빠지면 남은 숏은 그냥 한 종목이다."""
        items = [
            {"stock_code": "NOQUOTE", "stock_name": "롱", "quantity": 1},
            {"stock_code": US, "stock_name": "숏", "quantity": -2, "pair_long_code": "NOQUOTE"},
        ]
        result = await self._build(items=items)
        self.assertEqual([m["name"] for m in result["movers"]], ["숏"])
        self.assertFalse(result["movers"][0]["pair"])

    async def test_missing_change_percent_leaves_the_group_percent_blank(self):
        """등락률이 없으면 0% 가 아니라 '모름' 이다."""
        result = await self._build(
            items=[{"stock_code": KR, "stock_name": "삼성전자", "quantity": 10}],
            bulk={KR: {"price": 1200}},
        )
        self.assertIsNone(result["groups"][0]["day_pnl"])
        self.assertIsNone(result["groups"][0]["day_pct"])
        self.assertEqual(result["groups"][0]["value"], 12_000)


# _market_brief() 는 조회한 날짜(오늘 → 어제 순)를 그대로 date 로 돌려주고
# 행의 brief_date 는 읽지 않는다. 그래서 여기에 날짜를 박아 두면 그날 하루만
# 통과하는 테스트가 된다 — 실제로 2026-08-12 이후 계속 깨져 있었다.
BRIEF = {
    "markdown": "### 금일 시황\n\n반도체가 지수를 끌어올렸다." + "가" * 3000,
    "payload": {"market": [{"label": "KOSPI", "change_pct": 0.82},
                           {"code": "KOSDAQ", "change_pct": -0.31}]},
}


class MarketBriefTests(BuildSummaryMixin, unittest.IsolatedAsyncioTestCase):
    """시황은 화면에 직접 그리지 않는다 — 기기 코멘트 한 칸을 쓰는 재료다."""

    async def test_brief_is_attached_when_one_is_cached(self):
        result = await self._build(brief=BRIEF)
        # 캐시에 오늘 것이 있으면 오늘 날짜 — 어제 폴백을 다루는 쪽은
        # test_yesterdays_brief_is_flagged_as_not_today 가 맡는다.
        self.assertEqual(
            result["market"]["date"], device_summary.date.today().isoformat()
        )
        self.assertTrue(result["market"]["is_today"])
        self.assertEqual(
            [i["label"] for i in result["market"]["indices"]], ["KOSPI", "KOSDAQ"]
        )

    async def test_brief_text_is_trimmed(self):
        """원문을 통째로 실어 보낼 이유가 없다 — 한 칸을 쓰는 재료일 뿐이다."""
        result = await self._build(brief=BRIEF)
        self.assertEqual(len(result["market"]["text"]), device_summary.MARKET_BRIEF_CHARS)

    async def test_no_brief_is_not_an_error(self):
        result = await self._build(brief=None)
        self.assertIsNone(result["market"])
        self.assertEqual(result["total_value"], 412_000)

    async def test_a_broken_brief_table_does_not_take_down_the_summary(self):
        """시황 하나 때문에 화면이 통째로 못 뜨게 할 이유는 없다."""
        with patch("repositories.market_brief.get_daily_market_brief",
                   new=AsyncMock(side_effect=RuntimeError("no such table"))):
            result = await self._build(brief=None)
        self.assertIsNone(result["market"])
        self.assertEqual(result["holdings_count"], 2)

    async def test_yesterdays_brief_is_flagged_as_not_today(self):
        """아침에는 오늘 브리프가 아직 없다 — 어제 것이 마지막으로 알려진 시장이다."""
        async def only_yesterday(sub, brief_date, **kwargs):
            today = device_summary.date.today().isoformat()
            return None if brief_date == today else BRIEF

        result = await self._build(brief=only_yesterday)
        self.assertFalse(result["market"]["is_today"])
        self.assertEqual(
            result["market"]["date"],
            (device_summary.date.today() - device_summary.timedelta(days=1)).isoformat(),
        )


if __name__ == "__main__":
    unittest.main()
