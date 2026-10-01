"""배당 캘린더의 기준 시점 보유: 지난 배당은 그때 보유한 종목만, 그때 수량으로.

정산 기록(portfolio_stock_snapshots)은 KRX 거래일 15:30 KST 마감 정산의 전 계좌 합산 수량이다. 픽스처의 종목·수량은
운영 형태(매수 직후 지난 배당락, 매도 뒤 지급, 수량 변경, 국내 기준일 T+2, 미국·홍콩 같은 날 배당락)만 재현한다.
"""

import copy
from datetime import date
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from domain.dividend_verification import NH_CONFIRMED, NH_PARTIAL, UNCONFIRMED, link_calendar
from repositories import portfolio
from repositories.db import transaction
from services import dividend_calendar as cal

TODAY = date(2026, 10, 1)
RATES = {"USD": 1400.0, "HKD": 180.0, "AUD": 950.0, "KRW": 1.0}

# 정산일과 종목별 보유 변화(그 날짜부터의 수량, 0 = 매도).
DATES = ["2026-06-25", "2026-06-26", "2026-07-30", "2026-07-31", "2026-08-31", "2026-09-01", "2026-09-08", "2026-09-10",
         "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-22", "2026-09-23", "2026-09-30"]
CHANGES = {
    "GOOGL": [("2026-06-25", 10), ("2026-09-23", 15)],     # 배당락(9/8) 뒤 5주 추가 매수
    "O": [("2026-06-25", 30), ("2026-09-16", 0)],          # 배당락(9/1) 뒤 지급일(9/15) 다음 날 매도
    "000660": [("2026-06-25", 7), ("2026-08-31", 0)],      # 6/30 기준일 뒤 매도, 8/20 지급
    "AAA.AX": [("2026-06-25", 100)],
    "AGNC": [("2026-09-10", 10)],                          # 지난 배당락 뒤 매수
    "005930": [("2026-09-14", 5), ("2026-09-23", 8)],      # 9/28 기준일: 9/22 종가 보유분만
    "000270": [("2026-09-23", 2)],                         # 9/28 기준일 권리 없음
    "AAPL": [("2026-09-15", 4)],                           # 미국 9/15 배당락: 9/15 KST 정산(전 거래일 미국 장)에 보유
    "0700.HK": [("2026-09-15", 100)],                      # 홍콩 9/15 배당락 당일 매수 — 권리 없음
}
CURRENT = {"GOOGL": 15, "AAA.AX": 100, "AGNC": 10, "005930": 8, "000270": 2, "AAPL": 4, "0700.HK": 100}
CURRENCY = {"GOOGL": "USD", "AAA.AX": "AUD", "AGNC": "USD", "AAPL": "USD", "0700.HK": "HKD"}


def holdings_on(day: str, changes: dict) -> dict:
    out = {}
    for code, steps in changes.items():
        qty = 0
        for start, value in steps:
            if start <= day:
                qty = value
        if qty:
            out[code] = qty
    return out


def official(ex, pay, amount, currency="USD"):
    return {"ex_date": ex, "record_date": ex, "pay_date": pay, "declaration_date": None, "amount_per_share": amount,
            "currency": currency, "source": "공시", "source_url": None}


def kis(record, pay, amount):
    return {"record_date": record, "ex_date": None, "pay_date": pay, "amount_per_share": amount, "currency": "KRW",
            "source": "KIS·예탁원 배당 일정", "source_url": None}


def yahoo(*days, currency, amount):
    return [{"ex_date": d, "record_date": None, "pay_date": None, "declaration_date": None, "amount_per_share": amount,
             "currency": currency, "source": "Yahoo 배당락 이력", "source_url": None} for d in days]


def feed(events, official_feed=False):
    return {"events": events, "status": "fresh", "official": official_feed, "fetched_at": "2026-10-01T00:00:00+00:00"}


def histories():
    # 월초 배당락(2026-08-01은 토요일이라 8/3).
    monthly = [f"{y}-{m:02d}-{3 if (y, m) == (2026, 8) else 1:02d}" for y, m in [(2025, 10), (2025, 11), (2025, 12)] + [(2026, m) for m in range(1, 10)]]
    return {
        "AGNC": feed([official("2026-07-31", "2026-08-11", 0.12), official("2026-08-31", "2026-09-10", 0.12),
                      official("2026-09-30", "2026-10-09", 0.12)], True),
        "GOOGL": feed(yahoo("2026-09-08", currency="USD", amount=0.21)),
        "O": feed([official("2026-09-01", "2026-09-15", 0.27), official("2026-10-01", "2026-10-15", 0.27)], True),
        "000660": feed([kis("2026-06-30", "2026-08-20", 375)], True),
        "005930": feed([kis("2026-09-28", "2026-11-20", 370)], True),
        "000270": feed([kis("2026-09-28", "2026-11-20", 1600)], True),
        "AAPL": feed(yahoo("2026-09-15", currency="USD", amount=0.26)),
        "0700.HK": feed(yahoo("2026-09-15", currency="HKD", amount=4.5)),
        "AAA.AX": feed(yahoo(*monthly, currency="AUD", amount=0.2)),
    }


def nh(rid, day, code, currency, gross):
    return {"id": rid, "account_id": "nh", "date": day, "booked_date": day, "stock_code": code, "symbol": "",
            "currency": currency, "net_amount": round(gross * 0.85, 4), "gross_amount": gross}


class PointInTimeCalendarTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        for code, qty in CURRENT.items():
            await portfolio.save_portfolio_item("u1", code, code, qty, 1, CURRENCY.get(code, "KRW"))
        self.data = histories()
        self.records: list[dict] = []
        self.nh_only: set[str] = set()
        self.history_calls: list[tuple[list[str], float | None]] = []

        def get_histories(codes, timeout=None):
            self.history_calls.append((list(codes), timeout))
            return copy.deepcopy({k: v for k, v in self.data.items() if k in codes})

        for target, value in (("services.dividend_sources.get_histories", AsyncMock(side_effect=get_histories)),
                              ("services.portfolio.fx.fx_rate_for_currency", AsyncMock(side_effect=lambda c: RATES[c])),
                              ("repositories.broker_activity.dividend_records", AsyncMock(side_effect=lambda user: copy.deepcopy(self.records))),
                              ("repositories.broker_activity.dividend_adjustments", AsyncMock(return_value=[])),
                              ("repositories.broker_activity.nh_only_codes", AsyncMock(side_effect=lambda user: set(self.nh_only)))):
            patcher = patch(target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        brief = patch.object(cal, "_latest_brief_upcoming_events", AsyncMock(return_value=[]))
        brief.start()
        self.addCleanup(brief.stop)

    async def snapshots(self, dates=DATES, changes=CHANGES, user="u1", missing_quantity=(), values=None):
        values = values or {}
        async with transaction() as db:
            for day in dates:
                for code, qty in holdings_on(day, changes).items():
                    await db.execute("INSERT INTO portfolio_stock_snapshots (google_sub,date,stock_code,market_value,quantity) VALUES (?,?,?,?,?)",
                                     (user, day, code, values.get((day, code), 1000.0 * qty), None if (day, code) in missing_quantity else qty))

    async def build(self, user="u1"):
        return await cal.build_calendar(user, months_back=2, months_forward=2, today=TODAY)

    @staticmethod
    def rows(result, code):
        return {e["date"]: e for e in result["events"] if e["stock_code"] == code and e["date_status"] != "nh"}

    async def test_bought_after_reference_is_dropped_and_future_or_estimated_use_current(self):
        await self.snapshots()
        result = await self.build()
        agnc = self.rows(result, "AGNC")
        # 9/10 매수: 7/31·8/31 배당락 지급분(8/11·9/10)은 권리가 없다. 9/30 배당락(10/9 지급)은 9/30 정산 보유.
        self.assertEqual(sorted(agnc), ["2026-10-09"])
        self.assertEqual((agnc["2026-10-09"]["holding_basis"], agnc["2026-10-09"]["holding_as_of"], agnc["2026-10-09"]["shares"]),
                         ("snapshot", "2026-09-30", 10.0))
        estimated = [e for e in result["events"] if e["stock_code"] == "AAA.AX" and e["type"] == "estimated"]
        self.assertTrue(estimated)
        self.assertEqual({(e["holding_basis"], e["shares"]) for e in estimated}, {("current", 100.0)})
        past = self.rows(result, "AAA.AX")
        self.assertEqual((past["2026-08-03"]["holding_basis"], past["2026-08-03"]["holding_as_of"]), ("snapshot", "2026-07-31"))
        # 빠진 일정: AGNC 2, 매도한 O의 다음 배당락(오늘 이후), 9/28 기준일 권리 없는 000270, 홍콩 9/15 당일 매수.
        self.assertEqual(result["summary"]["not_held_count"], 5)

    async def test_sold_after_reference_keeps_old_quantity_and_marks_sold(self):
        await self.snapshots()
        async with transaction() as db:
            await db.execute("INSERT INTO portfolio_stock_weight_snapshots (google_sub,date,group_name,stock_code,stock_name,market_value,group_value,total_value)"
                             " VALUES ('u1','2026-09-15','기타','O','리얼티인컴',1,1,1)")
        result = await self.build()
        realty = self.rows(result, "O")
        self.assertEqual(sorted(realty), ["2026-09-15"])  # 10/1 배당락은 지금 보유하지 않아 없다
        row = realty["2026-09-15"]
        self.assertEqual((row["shares"], row["held_now"], row["holding_as_of"], row["stock_name"]), (30.0, False, "2026-09-01", "리얼티인컴"))
        self.assertEqual(row["expected_amount_krw"], round(0.27 * 1400 * 30))
        # 국내 6/30 기준일 뒤 매도: 8/20 지급분은 6/26(2거래일 전) 보유 수량으로 남는다.
        hynix = self.rows(result, "000660")["2026-08-20"]
        self.assertEqual((hynix["shares"], hynix["held_now"], hynix["reference_date"], hynix["expected_amount_krw"]),
                         (7.0, False, "2026-06-26", 375 * 7))
        coverage = {c["stock_code"]: c for c in result["coverage"]}
        self.assertEqual((coverage["O"]["held"], coverage["000660"]["held"], coverage["GOOGL"]["held"]), (False, False, True))
        self.assertEqual(result["summary"]["sold_stock_count"], 2)
        aug = next(m for m in result["monthly"] if m["month"] == "2026-08")
        self.assertEqual(aug["total_krw"], 375 * 7)

    async def test_quantity_change_uses_reference_quantity(self):
        await self.snapshots()
        googl = self.rows(await self.build(), "GOOGL")["2026-09-08"]
        self.assertEqual((googl["shares"], googl["expected_amount_krw"], googl["holding_as_of"]), (10.0, round(0.21 * 1400 * 10), "2026-09-08"))

    async def test_krx_record_t2_and_market_timezone_split(self):
        await self.snapshots()
        result = await self.build()
        samsung = self.rows(result, "005930")["2026-11-20"]
        # 9/28(월) 기준일 → 추석·주말을 건너 9/22 종가 보유(5주). 9/23 추가 3주는 권리가 없다.
        self.assertEqual((samsung["reference_date"], samsung["reference_rule"], samsung["shares"]), ("2026-09-22", "krx_record_t2", 5.0))
        self.assertEqual(self.rows(result, "000270"), {})
        # 같은 9/15 배당락: 미국은 9/15 KST 정산(전 거래일 미국 장 반영)에 보유 → 권리, 홍콩은 9/14 정산에 없음 → 없음.
        aapl = self.rows(result, "AAPL")["2026-09-15"]
        self.assertEqual((aapl["reference_date"], aapl["holding_as_of"], aapl["shares"]), ("2026-09-15", "2026-09-15", 4.0))
        self.assertEqual(self.rows(result, "0700.HK"), {})

    async def test_bound_before_first_record_uses_first_snapshot(self):
        dates = ["2026-08-14", "2026-08-31", "2026-09-30"]
        changes = {"AAA.AX": [("2026-08-14", 100)], "GOOGL": [("2026-08-31", 15)]}
        self.data["GOOGL"] = feed(yahoo("2026-08-05", currency="USD", amount=0.21))
        await self.snapshots(dates, changes, missing_quantity={("2026-08-14", "AAA.AX")})
        result = await self.build()
        aaa = self.rows(result, "AAA.AX")["2026-08-03"]
        # 8/2 기준 시점은 첫 기록(8/14)보다 이르다: 첫 기록에 있으므로 보유, 수량은 이어서 보유한 8/31 기록.
        self.assertEqual((aaa["holding_basis"], aaa["holding_as_of"], aaa["shares"], aaa["quantity_as_of"]),
                         ("earliest_snapshot", "2026-08-14", 100.0, "2026-08-31"))
        self.assertNotIn("2026-08-05", self.rows(result, "GOOGL"))  # 첫 기록에 없음 = 나중에 샀다

    async def test_no_records_falls_back_to_current_holdings(self):
        result = await self.build()
        googl = self.rows(result, "GOOGL")["2026-09-08"]
        self.assertEqual((googl["holding_basis"], googl["shares"]), ("current_fallback", 15.0))
        self.assertEqual(sorted(self.rows(result, "AGNC")), ["2026-08-11", "2026-09-10", "2026-10-09"])
        self.assertEqual(result["summary"]["not_held_count"], 0)

    async def test_nh_amount_decides_confirmed_or_partial(self):
        await self.snapshots()
        # GOOGL은 지금 다른 계좌에도 있다고 보자(nh_only 아님). 9/8 배당락 보유 10주 몫이 NH로 다 들어왔으면 NH 확인.
        self.records = [nh(1, "2026-09-16", "GOOGL", "USD", 2.1),
                        nh(2, "2026-09-15", "O", "USD", 8.1),      # 매도한 종목의 지급분도 연결된다
                        nh(3, "2026-10-01", "AAPL", "USD", 0.52)]  # 4주 중 2주 몫 — 나머지는 다른 계좌
        self.nh_only = {"AAPL"}
        result = await self.build()
        self.assertEqual(self.rows(result, "GOOGL")["2026-09-08"]["verification"], NH_CONFIRMED)
        self.assertEqual(self.rows(result, "O")["2026-09-15"]["verification"], NH_CONFIRMED)
        self.assertEqual(self.rows(result, "AAPL")["2026-09-15"]["verification"], NH_PARTIAL)
        self.assertFalse(any(e["date_status"] == "nh" for e in result["events"]))

    async def test_receipt_candidates_use_reference_quantity(self):
        from routes import dividend_receipts as route
        await self.snapshots()
        with patch.object(route, "_user_id", AsyncMock(return_value="u1")), \
             patch.object(route.repo, "received_source_keys", AsyncMock(return_value=set())):
            data = await route.candidates(None)
        keys = {e["source_key"]: e for e in data["events"]}
        self.assertEqual((keys["GOOGL:ex_date:2026-09-08"]["shares"], keys["GOOGL:ex_date:2026-09-08"]["holding_basis"]), (10.0, "snapshot"))
        self.assertEqual(keys["O:ex_date:2026-09-01"]["shares"], 30.0)            # 매도한 종목도 수취 입력 대상
        self.assertNotIn("AGNC:ex_date:2026-07-31", keys)                        # 매수 전 배당은 후보가 아니다
        self.assertEqual(keys["AGNC:ex_date:2026-09-30"]["shares"], 10.0)

    async def test_sold_histories_only_use_the_time_left_after_held_histories(self):
        await self.snapshots()
        await self.build()
        (held, held_timeout), (sold, sold_timeout) = self.history_calls
        self.assertEqual(sorted(held), sorted(CURRENT))
        self.assertIsNone(held_timeout)                    # 보유 종목: 기본 15초 전부
        self.assertEqual(sorted(sold), ["000660", "O"])    # 매도한 종목: 그 뒤 남은 시간만
        self.assertTrue(0 <= sold_timeout <= cal.dividend_sources.BATCH_TIMEOUT)
        self.history_calls.clear()
        with patch.object(cal, "_clock", side_effect=[100.0, 100.0 + cal.dividend_sources.BATCH_TIMEOUT + 2]):
            await self.build()
        self.assertEqual(self.history_calls[1][1], 0.0)    # 보유 종목이 시간을 다 쓰면 매도 종목은 캐시만

    async def test_unknown_reference_quantity_is_counted_apart_from_fx(self):
        # 9/1 정산에 O 수량이 없고 평가액이 앞뒤 정산(30주)의 5배다. 가격만으로는 설명되지 않으므로(그 사이 매매)
        # 앞뒤 기록 수량을 9/1 배당에 쓰지 않는다.
        values = {("2026-09-01", "O"): 150000.0}
        await self.snapshots(missing_quantity={("2026-09-01", "O")}, values=values)
        result = await self.build()
        row = self.rows(result, "O")["2026-09-15"]
        self.assertEqual((row["shares"], row["expected_amount_krw"], row["quantity_unknown_reason"]),
                         (None, None, "changed_before_record"))
        sept = next(m for m in result["monthly"] if m["month"] == "2026-09")
        self.assertEqual((sept["quantity_unknown_count"], sept["unconverted_count"]), (1, 0))


def ex_row(code, day, currency="USD", **extra):
    return {"stock_code": code, "date": day, "ex_date": day, "record_date": None, "date_kind": "ex_date", "type": "ex_date",
            "date_status": "observed", "confirmed": False, "currency": currency, "source_key": f"{code}:ex_date:{day}", **extra}


def record(rid, day, code, gross, currency="USD"):
    return {"id": rid, "account_id": "nh", "date": day, "booked_date": day, "stock_code": code, "symbol": "",
            "currency": currency, "net_amount": gross, "gross_amount": gross}


class AmountVerdictTests(TempDbMixin):
    def test_amount_rule_overrides_current_account_rule(self):
        base = {"amount_per_share": 0.21, "shares": 10.0, "holding_basis": "snapshot"}
        full = [record(1, "2026-09-16", "GOOGL", 2.1)]
        # 지금은 다른 계좌에도 있지만(nh_only 아님) 그 시점 보유분 전체가 NH로 들어왔다 → 확인.
        out, _ = link_calendar([ex_row("GOOGL", "2026-09-08", **base)], full, [], TODAY, set())
        self.assertEqual(out[0]["verification"], NH_CONFIRMED)
        # 반올림·세목 차이(3% 이내)는 확인.
        out, _ = link_calendar([ex_row("GOOGL", "2026-09-08", **base)], [record(1, "2026-09-16", "GOOGL", 2.05)], [], TODAY, set())
        self.assertEqual(out[0]["verification"], NH_CONFIRMED)
        # 그 시점 수량 그대로인데 NH가 뚜렷이 적다 → 다른 계좌 몫이 있다(NH 계좌에만 있다고 해도 일부 확인).
        out, _ = link_calendar([ex_row("GOOGL", "2026-09-08", **{**base, "shares": 15.0})], full, [], TODAY, {"GOOGL"})
        self.assertEqual(out[0]["verification"], NH_PARTIAL)

    def test_approximate_quantity_or_unknown_amount_falls_back_to_account_rule(self):
        short = [record(1, "2026-09-16", "GOOGL", 2.1)]
        for extra in ({"amount_per_share": 0.21, "shares": 15.0, "holding_basis": "earliest_snapshot"},
                      {"amount_per_share": 0.21, "shares": 15.0, "holding_basis": "snapshot", "quantity_as_of": "2026-06-30"},
                      {"amount_per_share": 0.21, "shares": 15.0, "holding_basis": "current_fallback"},
                      {"amount_per_share": None, "shares": 15.0, "holding_basis": "snapshot"}):
            for nh_only, expected in (({"GOOGL"}, NH_CONFIRMED), (set(), NH_PARTIAL)):
                out, _ = link_calendar([ex_row("GOOGL", "2026-09-08", **extra)], short, [], TODAY, nh_only)
                self.assertEqual(out[0]["verification"], expected, (extra, nh_only))
        # 근사 수량이라도 NH가 예상을 덮으면 확인이다.
        cover = {"amount_per_share": 0.21, "shares": 8.0, "holding_basis": "earliest_snapshot"}
        out, _ = link_calendar([ex_row("GOOGL", "2026-09-08", **cover)], short, [], TODAY, set())
        self.assertEqual(out[0]["verification"], NH_CONFIRMED)

    def test_excluded_rows_bound_the_previous_rows_window(self):
        # 8/3 배당락 행만 남고 9/1 배당락은 그때 보유하지 않아 뺐다. 9/15 입금은 8/3 회차가 가져가지 않는다.
        events = [ex_row("AAA.AX", "2026-08-03", "AUD")]
        deposit = [record(1, "2026-09-15", "AAA.AX", 20.0, "AUD")]
        out, unlinked = link_calendar(events, deposit, [], TODAY, {"AAA.AX"}, boundaries=[ex_row("AAA.AX", "2026-09-01", "AUD")])
        self.assertEqual((out[0]["nh_match"], unlinked), (None, [0]))
        out, unlinked = link_calendar(events, deposit, [], TODAY, {"AAA.AX"})  # 경계가 없으면 +60일 안이라 붙는다
        self.assertEqual((out[0]["paid_date"], unlinked), ("2026-09-15", []))

    def test_rights_row_needs_an_earlier_nh_dividend_to_be_unconfirmed(self):
        late = date(2026, 11, 15)
        snapshot = ex_row("AAA.AX", "2026-08-03", "AUD", holding_basis="snapshot")
        # 정산 기록(전 계좌 합산)의 보유는 그때 NH 계좌에 있었다는 뜻이 아니다(nh_only는 지금 계좌 구성).
        for basis in ("snapshot", "earliest_snapshot", "current_fallback"):
            out, _ = link_calendar([{**snapshot, "holding_basis": basis}], [], [], late, {"AAA.AX"})
            self.assertIsNone(out[0]["verification"], basis)
        # 그 전에 같은 종목 NH 배당을 받았으면(그때도 NH로 보유) 받을 배당을 못 본 것이다.
        earlier = [record(1, "2026-06-15", "AAA.AX", 20.0, "AUD")]
        out, _ = link_calendar([snapshot], earlier, [], late, {"AAA.AX"})
        self.assertEqual(out[0]["verification"], UNCONFIRMED)

    def test_sold_stock_with_incomparable_amount_is_nh_confirmed(self):
        # 지금 보유하지 않는 종목은 지금 계좌 구성(nh_only)이 그때를 말해 주지 않는다. 금액으로 판정할 수 없으면
        # NH 입금을 그 배당으로 본다(예전에는 'NH 입금' 행이었다) — 수취 입력 버튼으로 현금을 이중 기록하지 않게.
        deposit = [record(1, "2026-09-16", "O", 8.1)]
        for extra in ({"amount_per_share": None, "shares": 30.0, "holding_basis": "snapshot"},
                      {"amount_per_share": 0.27, "shares": 40.0, "holding_basis": "snapshot", "quantity_as_of": "2026-06-30"},
                      {"amount_per_share": 0.27, "shares": None, "holding_basis": "earliest_snapshot"}):
            out, _ = link_calendar([ex_row("O", "2026-09-01", held_now=False, **extra)], deposit, [], TODAY, set())
            self.assertEqual(out[0]["verification"], NH_CONFIRMED, extra)
        # 수량이 그 시점 그대로인데 NH가 뚜렷이 적으면 매도한 종목도 일부 확인이다(다른 계좌 몫).
        exact = {"amount_per_share": 0.27, "shares": 60.0, "holding_basis": "snapshot"}
        out, _ = link_calendar([ex_row("O", "2026-09-01", held_now=False, **exact)], deposit, [], TODAY, set())
        self.assertEqual(out[0]["verification"], NH_PARTIAL)
