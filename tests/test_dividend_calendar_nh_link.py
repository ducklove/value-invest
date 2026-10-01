"""배당 캘린더 ↔ NH 배당 입금 연결: 해외 배당락 이력 연결, NH 입금 행, 세금 정산, 수취 입력 차단.

픽스처는 2026-10 운영 탐사의 형태(AAA.AX 월배당·월중 지급, GOOGL 분기, EUN2.DE, 83199.HK 위안 지급,
'TICKER 시장' 기호, 외화제세금환급)만 재현한다. 실제 계좌·금액이 아니다.
"""

import copy
from datetime import date
from unittest.mock import AsyncMock, patch

import test_nh_dividends as nh
from _harness import TempDbMixin, seed_user

from domain.dividend_receipts import DividendInput
from domain.dividend_verification import (
    NH_CONFIRMED,
    NH_PARTIAL,
    UNCONFIRMED,
    attach_adjustments,
    link_calendar,
    nh_duplicate,
    nh_payment_event,
    same_stock,
)
from domain.portfolio_trades import TradeConflict
from repositories import account_holdings, accounts, broker_activity, brokers, dividend_receipts, portfolio
from services import dividend_calendar as cal
from services.brokers import activity

TODAY = date(2026, 10, 1)


def ex(code, day, currency, kind="ex_date", **extra):
    """Yahoo 배당락 이력 같은 수집 일정(지급일 없음)."""
    return {"stock_code": code, "date": day, "ex_date": day if kind == "ex_date" else None,
            "record_date": day if kind == "record_date" else None, "date_kind": kind, "type": kind,
            "date_status": "observed", "confirmed": False, "currency": currency,
            "source_key": f"{code}:ex_date:{day}", **extra}


def rec(rid, day, symbol, currency, net, code="", **extra):
    return {"id": rid, "account_id": "nh", "date": day, "booked_date": day, "stock_code": code, "symbol": symbol,
            "currency": currency, "net_amount": net, "gross_amount": extra.pop("gross", net), **extra}


class SameStockTests(TempDbMixin):
    def test_resolved_code_wins_and_fallback_requires_market_and_currency(self):
        # 해석된 코드가 다르면 티커가 같아도 다른 종목(이전 리뷰 지적: 시장·통화 무시).
        self.assertFalse(same_stock("AAA", rec(1, "2026-09-15", "AAA AU", "AUD", 1, code="AAA.AX")))
        self.assertTrue(same_stock("AAA.AX", rec(1, "2026-09-15", "AAA AU", "AUD", 1, code="AAA.AX")))
        # 미해석 기록: 시장 접미사·배당 통화가 맞을 때만.
        loose = rec(1, "2026-09-15", "AAA AU", "AUD", 1)
        self.assertTrue(same_stock("AAA.AX", loose))
        self.assertFalse(same_stock("AAA", loose))           # 미국 AAA 와 혼동하지 않는다
        self.assertFalse(same_stock("AAA.AX", loose, "USD"))  # 통화 불일치
        self.assertFalse(same_stock("AAA.AX", {**loose, "currency": "USD"}))  # 호주 시장에 USD 배당 없음
        self.assertTrue(same_stock("83199.HK", rec(1, "2026-09-15", "83199 HK", "CNY", 1), "CNY"))
        self.assertTrue(same_stock("0700.HK", rec(1, "2026-09-15", "700 HK", "HKD", 1)))
        self.assertTrue(same_stock("EUN2.DE", rec(1, "2026-09-15", "EUN2 DE", "EUR", 1)))
        self.assertTrue(same_stock("BRK-B", rec(1, "2026-09-15", "BRK.B US", "USD", 1)))
        self.assertFalse(same_stock("EUN2.F", rec(1, "2026-09-15", "EUN2 DE", "EUR", 1)))
        self.assertFalse(same_stock("005930", rec(1, "2026-09-15", "", "KRW", 1)))


class LinkCalendarTests(TempDbMixin):
    def test_monthly_ex_dates_each_link_own_mid_month_payment(self):
        events = [ex("AAA.AX", d, "AUD") for d in ("2026-07-01", "2026-08-03", "2026-09-01")]
        records = [rec(3, "2026-09-15", "AAA AU", "AUD", 3.0, code="AAA.AX"),
                   rec(1, "2026-07-15", "AAA AU", "AUD", 1.0, code="AAA.AX"),
                   rec(2, "2026-08-14", "AAA AU", "AUD", 2.0, code="AAA.AX")]
        out, unlinked = link_calendar(events, records, [], TODAY, {"AAA.AX"})
        self.assertEqual([(e["date"], e["paid_date"], e["nh_match"]["id"]) for e in out],
                         [("2026-07-01", "2026-07-15", 1), ("2026-08-03", "2026-08-14", 2), ("2026-09-01", "2026-09-15", 3)])
        self.assertEqual({e["verification"] for e in out}, {NH_CONFIRMED})
        self.assertEqual(unlinked, [])
        # 배당락일은 바꾸지 않는다.
        self.assertEqual([e["ex_date"] for e in out], ["2026-07-01", "2026-08-03", "2026-09-01"])

    def test_missing_month_does_not_steal_next_payment_and_waits_before_unconfirmed(self):
        events = [ex("AAA.AX", d, "AUD") for d in ("2026-07-01", "2026-08-03", "2026-09-01")]
        records = [rec(1, "2026-07-15", "AAA AU", "AUD", 1.0, code="AAA.AX"),
                   rec(3, "2026-09-15", "AAA AU", "AUD", 3.0, code="AAA.AX")]
        out, _ = link_calendar(events, records, [], TODAY, {"AAA.AX"})
        self.assertEqual([e["paid_date"] if e["nh_match"] else None for e in out], ["2026-07-15", None, "2026-09-15"])
        # 8/3 + 60일 = 10/2 > 오늘 → 아직 판정하지 않는다.
        self.assertIsNone(out[1]["verification"])
        later, _ = link_calendar(events, records, [], date(2026, 10, 5), {"AAA.AX"})
        self.assertEqual(later[1]["verification"], UNCONFIRMED)
        # 그 전에 같은 종목 NH 배당이 없으면(나중에 산 종목일 수 있다) 미확인으로 단정하지 않는다.
        fresh, _ = link_calendar(events, records[1:], [], date(2026, 10, 5), {"AAA.AX"})
        self.assertEqual([e["verification"] for e in fresh], [None, None, NH_CONFIRMED])
        # NH 밖 계좌에도 보유하면 기다려도 미확인으로 단정하지 않는다(그 계좌로 받았을 수 있다).
        mixed, _ = link_calendar(events, records, [], date(2026, 10, 5), set())
        self.assertIsNone(mixed[1]["verification"])
        self.assertEqual(mixed[0]["verification"], NH_PARTIAL)

    def test_quarterly_and_european_and_cny_on_hong_kong(self):
        events = [ex("GOOGL", "2026-06-09", "USD"), ex("GOOGL", "2026-09-08", "USD"),
                  ex("EUN2.DE", "2026-06-16", "EUR"), ex("83199.HK", "2026-08-20", "CNY")]
        records = [rec(1, "2026-06-16", "GOOGL US", "USD", 1.79, code="GOOGL", gross=2.1, tax_amount=0.31),
                   rec(2, "2026-09-16", "GOOGL US", "USD", 1.79, code="GOOGL", gross=2.1),
                   rec(3, "2026-06-25", "EUN2 DE", "EUR", 12.4, code="EUN2.DE", domestic_tax_krw=3100),
                   rec(4, "2026-09-12", "83199 HK", "CNY", 30.0)]  # 미해석 기록 → 시장·통화 대조
        out, unlinked = link_calendar(events, records, [], TODAY, {"GOOGL", "EUN2.DE", "83199.HK"})
        self.assertEqual([e["nh_match"]["id"] for e in out], [1, 2, 3, 4])
        self.assertEqual(out[0]["nh_match"]["tax_amount"], 0.31)
        self.assertEqual(out[2]["nh_match"]["domestic_tax_krw"], 3100)
        self.assertEqual(unlinked, [])

    def test_currency_mismatch_and_other_market_do_not_link(self):
        events = [ex("83199.HK", "2026-08-20", "HKD"), ex("AAA", "2026-09-01", "USD")]
        records = [rec(1, "2026-09-12", "83199 HK", "CNY", 30.0, code="83199.HK"),
                   rec(2, "2026-09-15", "AAA AU", "AUD", 3.0, code="AAA.AX"),
                   rec(3, "2026-09-16", "AAA AU", "AUD", 3.0)]
        out, unlinked = link_calendar(events, records, [], TODAY, {"83199.HK", "AAA"})
        self.assertEqual([e["nh_match"] for e in out], [None, None])
        self.assertEqual(unlinked, [0, 1, 2])

    def test_payment_window_and_payment_rows_take_priority(self):
        # 국내 공시 지급일과 NH 입금(±)이 먼저 연결되고, 같은 입금이 배당락 행에 다시 쓰이지 않는다.
        events = [{"stock_code": "005930", "date": "2026-08-20", "date_kind": "payment", "type": "payment", "currency": "KRW",
                   "ex_date": "2026-06-29", "source_key": "005930:ex_date:2026-06-29"},
                  ex("005930", "2026-06-29", "KRW", kind="record_date")]
        records = [rec(1, "2026-08-21", "", "KRW", 8460.0, code="005930")]
        out, unlinked = link_calendar(events, records, [], TODAY, {"005930"})
        self.assertEqual((out[0]["verification"], out[0]["paid_date"]), (NH_CONFIRMED, "2026-08-21"))
        self.assertIsNone(out[1]["nh_match"])
        self.assertEqual(unlinked, [])

    def test_estimated_and_future_rows_are_not_linked(self):
        events = [ex("AAA.AX", "2026-10-01", "AUD"), {**ex("AAA.AX", "2026-09-01", "AUD"), "type": "estimated"}]
        out, unlinked = link_calendar(events, [rec(1, "2026-09-15", "AAA AU", "AUD", 3.0, code="AAA.AX")], [], TODAY, {"AAA.AX"})
        self.assertEqual([e["verification"] for e in out], [None, None])
        self.assertEqual(unlinked, [0])

    def test_tax_settlement_attaches_to_dividend_not_a_row(self):
        dividends = [rec(1, "2026-06-16", "GOOGL US", "USD", 1.79, code="GOOGL", gross=2.1),
                     rec(2, "2026-09-16", "GOOGL US", "USD", 1.79, code="GOOGL", gross=2.2),
                     rec(3, "2026-09-10", "AAA AU", "USD", 1.0, code="AAA.AX")]
        exact = rec(10, "2026-09-25", "GOOGL US", "USD", 0.32, code="GOOGL", gross=None, refund_base_gross=2.1, income_krw=-55.0,
                    domestic_tax_krw=500)
        nearest = rec(11, "2026-09-26", "GOOGL US", "USD", 0.01, code="GOOGL", gross=None, refund_base_gross=9.99, income_krw=0)
        orphan = rec(12, "2026-05-01", "GOOGL US", "USD", 0.3, code="GOOGL", gross=None, refund_base_gross=2.1)
        linked, orphans = attach_adjustments(dividends, [nearest, exact, orphan])
        self.assertEqual([[a["id"] for a in r["adjustments"]] for r in linked], [[10], [11], []])
        self.assertEqual([o["id"] for o in orphans], [12])
        self.assertEqual(dividends[0].get("adjustments"), None)  # 원본은 바꾸지 않는다
        events = [ex("GOOGL", "2026-06-09", "USD")]
        out, unlinked = link_calendar(events, linked, [], TODAY, {"GOOGL"})
        self.assertEqual(out[0]["nh_match"]["adjustments"][0]["income_krw"], -55.0)
        self.assertEqual(unlinked, [1, 2])  # 세금 정산은 기록 목록에 없으므로 행이 되지 않는다

    def test_nh_payment_event_uses_actual_amounts_and_is_not_receiptable(self):
        record = rec(5, "2026-08-20", "XYZ US", "USD", 8.5, gross=10.0, tax_amount=1.5, gross_krw=14005.0, stock_name="엑스와이지")
        row = nh_payment_event({**record, "adjustments": []})
        self.assertEqual((row["date_status"], row["verification"], row["receiptable"], row["cashflow"]), ("nh", NH_CONFIRMED, False, True))
        self.assertEqual((row["stock_code"], row["stock_name"], row["expected_amount_krw"], row["gross_amount"]), ("XYZ", "엑스와이지", 14005.0, 10.0))
        self.assertEqual(row["paid_date"], "2026-08-20")
        unverified = nh_payment_event(rec(6, "2026-08-20", "AAA AU", "AUD", 3.0, code="AAA.AX"))
        self.assertEqual((unverified["expected_amount_krw"], unverified["fx_source"]), (None, "unavailable"))
        domestic = nh_payment_event(rec(7, "2026-08-20", "", "KRW", 8460.0, code="005930", gross=10000.0))
        self.assertEqual(domestic["expected_amount_krw"], 10000.0)

    def test_server_guard_covers_overseas_dividends(self):
        records = [rec(1, "2026-09-15", "AAA AU", "AUD", 3.0, code="AAA.AX"), rec(2, "2026-09-12", "83199 HK", "CNY", 30.0)]
        receipt = {"stock_code": "AAA.AX", "currency": "AUD", "received_date": "2026-09-20"}
        self.assertEqual(nh_duplicate(receipt, records, {"AAA.AX"})["id"], 1)
        self.assertIsNone(nh_duplicate({**receipt, "currency": "USD"}, records, {"AAA.AX"}))
        self.assertIsNone(nh_duplicate(receipt, records, set()))  # NH 밖 계좌에도 보유하면 그 몫은 기록 가능
        # 배당락 일정에서 고른 수취: 사용자가 배당락일을 수취일로 넣어도 그 회차 NH 입금이면 거절한다.
        picked = {**receipt, "received_date": "2026-08-30", "source_key": "AAA.AX:ex_date:2026-09-01"}
        self.assertEqual(nh_duplicate(picked, records, {"AAA.AX"})["id"], 1)
        self.assertIsNone(nh_duplicate({**picked, "source_key": "AAA.AX:ex_date:2026-07-01", "received_date": "2026-07-03"},
                                       records, {"AAA.AX"}))
        cny = {"stock_code": "83199.HK", "currency": "CNY", "received_date": "2026-09-13"}
        self.assertEqual(nh_duplicate(cny, records, {"83199.HK"})["id"], 2)


AAA = {"stock_code": "AAA.AX", "stock_name": "호주 단기채", "quantity": 100, "avg_price": 50, "avg_price_currency": "AUD", "currency": "AUD"}
GOOGL = {"stock_code": "GOOGL", "stock_name": "구글", "quantity": 10, "avg_price": 150, "avg_price_currency": "USD", "currency": "USD"}


def nh_row(serial, day, symbol, name, currency, net, tax, rate, **values):
    gross = round(net + tax, 4)
    return nh.daily_row(serial, day=day, iem_cd=symbol, iem_krl_nm=name, cur_cd_nm=currency, aly_xcg_rt=rate,
                        fc_trd_amt=net, fc_tax_sum=tax, fc_icm_tax=tax, krw_sas_amt=round(gross * rate),
                        trd_bf_fc_dca=0, trd_af_fc_dca=net, trd_bf_dca=0, trd_af_dca=-values.get("tax_sum", 0), **values)


def yahoo(*days, currency, amount):
    return {"events": [{"ex_date": d, "record_date": None, "pay_date": None, "declaration_date": None, "amount_per_share": amount,
                        "currency": currency, "source": "Yahoo 배당락 이력", "source_url": None} for d in days],
            "status": "fresh", "fetched_at": "2026-10-01T00:00:00+09:00"}


class CalendarNHIntegrationTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="NH"))["account_id"]
        cid = await brokers.store_credential("u1", "test-nh-cal-key", "test-nh-cal-secret")
        await brokers.link_account("u1", self.aid, cid, nh.ACCOUNT, "live")
        self.link = await brokers.get_link("u1", self.aid)
        rows = copy.deepcopy(nh.CASH) + [dict(AAA), dict(GOOGL)]
        with patch.object(nh.namuh, "pages", nh.FakeNH([], None)), \
             patch.object(nh.sync, "fetch_snapshot", AsyncMock(return_value=(rows, {}))):
            await nh.sync.sync_account("u1", self.aid)
        # GOOGL 은 수동 계좌에도 보유 → NH 입금은 NH 몫만 확인(NH 일부 확인).
        await portfolio.save_portfolio_item("u1", "GOOGL", "구글", 5, 150, "USD", account_id=await accounts.get_default_account_id("u1"))
        entries = [
            activity.overseas_dividend(nh_row(1, date(2026, 8, 14), "AAA AU", "호주 단기채", "AUD", 20.0, 0.1, 950.0,
                                              tax_sum=2000), self.link, "AAA.AX"),
            activity.overseas_dividend(nh_row(2, date(2026, 9, 15), "AAA AU", "호주 단기채", "AUD", 21.0, 0.1, 950.0,
                                              tax_sum=2100), self.link, "AAA.AX"),
            activity.overseas_dividend(nh_row(3, date(2026, 9, 16), "GOOGL US", "구글", "USD", 1.7, 0.3, 1400.0), self.link, "GOOGL"),
            # 지금은 없는 종목(종목코드 해석 실패) — 어느 일정에도 연결되지 않으므로 NH 입금 행.
            activity.overseas_dividend(nh_row(4, date(2026, 8, 20), "XYZ US", "엑스와이지", "USD", 8.5, 1.5, 1400.0), self.link, ""),
            # 창 밖(7월) 입금은 행을 만들지 않는다.
            activity.overseas_dividend(nh_row(5, date(2026, 7, 15), "AAA AU", "호주 단기채", "AUD", 19.0, 0.1, 950.0), self.link, "AAA.AX"),
        ]
        refund = nh.daily_row(9, day=date(2026, 9, 25), sps_cd_nm="외화제세금환급", iem_cd="GOOGL US", iem_krl_nm="구글",
                              aly_xcg_rt=1400.0, fc_trd_amt=0.3, fc_tax_sum=2.0, fc_icm_tax=2.0, tax_sum=431, icm_tax=392, rsd_tax=39,
                              krw_sas_amt=2800, trd_bf_fc_dca=1.7, trd_af_fc_dca=2.0, trd_bf_dca=0, trd_af_dca=-431)
        entries.append(activity.overseas_dividend(refund, self.link, "GOOGL"))
        await broker_activity.store("u1", self.link, entries)

    async def calendar(self):
        histories = {"AAA.AX": yahoo("2025-10-01", "2025-11-03", "2025-12-01", "2026-01-02", "2026-02-02", "2026-03-02", "2026-04-01",
                                     "2026-05-01", "2026-06-01", "2026-07-01", "2026-08-03", "2026-09-01", currency="AUD", amount=0.2),
                     "GOOGL": yahoo("2025-12-08", "2026-03-09", "2026-06-09", "2026-09-08", currency="USD", amount=0.21)}
        rates = {"AUD": 950.0, "USD": 1400.0}
        with patch("services.dividend_sources.get_histories", AsyncMock(return_value=histories)), \
             patch("services.portfolio.fx.fx_rate_for_currency", AsyncMock(side_effect=lambda c: rates[c])), \
             patch.object(cal, "_latest_brief_upcoming_events", AsyncMock(return_value=[])):
            return await cal.build_calendar("u1", months_back=2, months_forward=1, today=TODAY)

    async def test_overseas_ex_dates_link_and_nh_only_rows_count_once(self):
        result = await self.calendar()
        rows = {(e["stock_code"], e["date"]): e for e in result["events"]}
        aug, sep = rows[("AAA.AX", "2026-08-03")], rows[("AAA.AX", "2026-09-01")]
        self.assertEqual((aug["verification"], aug["paid_date"], aug["nh_match"]["net_amount"]), (NH_CONFIRMED, "2026-08-14", 20.0))
        self.assertEqual((sep["verification"], sep["paid_date"], sep["nh_match"]["domestic_tax_krw"]), (NH_CONFIRMED, "2026-09-15", 2100.0))
        googl = rows[("GOOGL", "2026-09-08")]
        self.assertEqual((googl["verification"], googl["paid_date"]), (NH_PARTIAL, "2026-09-16"))
        self.assertEqual(googl["nh_match"]["adjustments"][0]["income_krw"], round(0.3 * 1400) - 431)
        nh_rows = [e for e in result["events"] if e["date_status"] == "nh"]
        self.assertEqual([(e["stock_code"], e["date"], e["expected_amount_krw"]) for e in nh_rows], [("XYZ", "2026-08-20", 14000.0)])
        # 7월 입금(창 밖)과 세금 정산은 행이 되지 않는다.
        self.assertFalse(any(e["date"].startswith("2026-07") for e in result["events"]))
        monthly = {m["month"]: m for m in result["monthly"]}
        # 배당락 행은 합계 밖, NH 입금 행만 실제 세전 원화로 더한다(연결된 입금은 다시 더하지 않는다).
        self.assertEqual((monthly["2026-08"]["total_krw"], monthly["2026-08"]["nh_only_krw"], monthly["2026-08"]["nh_only_count"]),
                         (14000, 14000, 1))
        self.assertEqual((monthly["2026-09"]["total_krw"], monthly["2026-09"]["nh_count"]), (0, 2))
        summary = result["summary"]
        self.assertEqual((summary["nh_count"], summary["nh_only_count"], summary["nh_partial_count"]), (4, 1, 1))
        self.assertEqual(summary["nh_unattached_adjustment_count"], 0)

    async def test_receipt_candidates_hide_nh_rows_and_server_refuses_overseas_duplicate(self):
        from routes import dividend_receipts as route
        with patch.object(route.dividend_calendar, "build_calendar", AsyncMock(return_value=await self.calendar())), \
             patch.object(route, "_user_id", AsyncMock(return_value="u1")):
            data = await route.candidates(None)
        keys = {e["source_key"]: e for e in data["events"]}
        self.assertNotIn("XYZ", {e["stock_code"] for e in data["events"]})  # NH 입금 행은 수취 대상이 아니다
        self.assertEqual(keys["AAA.AX:ex_date:2026-09-01"]["verification"], NH_CONFIRMED)
        self.assertEqual(keys["GOOGL:ex_date:2026-09-08"]["verification"], NH_PARTIAL)
        payload = {"stock_code": "AAA.AX", "stock_name": "호주 단기채", "country": "OTHER", "currency": "AUD",
                   "received_date": "2026-09-16", "gross_amount": 21.1, "fx_rate": 950}
        with self.assertRaises(TradeConflict):
            await dividend_receipts.preview_dividend("u1", DividendInput(**payload))
        with self.assertRaises(TradeConflict):
            await dividend_receipts.preview_dividend("u1", DividendInput(**{**payload, "received_date": "2026-09-01",
                                                                             "source_key": "AAA.AX:ex_date:2026-09-01"}))
        # NH 밖 계좌에도 보유한 GOOGL 은 그 몫의 수취를 막지 않는다.
        googl = {"stock_code": "GOOGL", "stock_name": "구글", "country": "US", "currency": "USD",
                 "received_date": "2026-09-16", "gross_amount": 1.05, "fx_rate": 1400}
        self.assertGreater((await dividend_receipts.preview_dividend("u1", DividendInput(**googl)))["net_amount"], 0)
