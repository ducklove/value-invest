"""Tests for preferred_dividends module.

Covered:
  * CSV parser robustness (real-world header quirks, quoted numbers,
    blanks, non-numeric cells)
  * Dynamic column discovery — parser must find "2025우" wherever it
    lives, not a fixed offset
  * cache.upsert_preferred_dividends idempotency
  * cache.get_trailing_dividends resolution priority:
      exact market_data > preferred_dividends > common-stock fallback

No network. fetch_csv / refresh loop is not unit-tested — those are
integration paths exercised manually (the Pi startup log shows whether
they succeeded).
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cache
import preferred_dividends as pd_mod


# Minimal CSV that mirrors the real sheet's layout. Column count matches
# the header so the parser's slicing logic is exercised. AI column
# (35th, 0-indexed 34) is "2025우".
_HEADER_COLS = [
    "우선주", "우선주종목코드", "보통주종목코드", "우선주 주가", "보통주 주가",
    "괴리율", "보통주시가배당", "우선주시가배당률", "우선주-보통주 배당율",
    "우선주 일변동", "보통주 일변동", "일변동 차이", "우선주 시총", "",
    "2025보", "2024보", "2023보", "2022보", "2021보", "2020보", "2019보",
    "2018보", "2017보", "2016보", "2015보", "2014보", "2013보", "2012보",
    "2011보", "2010보", "2009보", "2008보", "2007보", "2006보",
    "2025우", "2024우", "2023우", "2022우", "2021우",
]


def _make_csv(*rows) -> str:
    """Build a CSV string from parallel arrays. Rows shorter than the
    header are right-padded with blanks so the parser's bounds checks
    are exercised."""
    import csv as _csv
    import io as _io
    buf = _io.StringIO()
    w = _csv.writer(buf)
    w.writerow(_HEADER_COLS)
    for row in rows:
        padded = list(row) + [""] * (len(_HEADER_COLS) - len(row))
        w.writerow(padded)
    return buf.getvalue()


class ParseSheetCsvTests(unittest.TestCase):
    def test_basic_row(self):
        row = ["계양전기우", "012205", "012200"] + [""] * 31 + ["123"]
        assert len(_HEADER_COLS) == 39
        assert len(row) == 35  # pad in _make_csv
        csv_text = _make_csv(row)
        out = pd_mod.parse_sheet_csv(csv_text)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["stock_code"], "012205")
        self.assertEqual(out[0]["common_code"], "012200")
        self.assertEqual(out[0]["source_name"], "계양전기우")
        self.assertEqual(out[0]["dividend_per_share"], 123.0)
        self.assertEqual(out[0]["sheet_year"], 2025)

    def test_quoted_comma_number(self):
        # Real-world values like 1,444 arrive as the string "1,444" after
        # csv.reader unwraps the surrounding quotes Google Sheets added.
        # _clean_number strips the comma → 1444.0.
        row = ["삼성전자우", "005935", "005930"] + [""] * 31 + ["1,444"]
        csv_text = _make_csv(row)
        out = pd_mod.parse_sheet_csv(csv_text)
        self.assertEqual(out[0]["dividend_per_share"], 1444.0)

    def test_blank_dividend_yields_none(self):
        row = ["XXX우", "099999", "099998"] + [""] * 31 + [""]
        csv_text = _make_csv(row)
        out = pd_mod.parse_sheet_csv(csv_text)
        self.assertEqual(len(out), 1)
        self.assertIsNone(out[0]["dividend_per_share"])

    def test_zero_dividend_preserved(self):
        """0 은 '올해 배당 없음' 의 유효 값이라 None 과 구분해야 함."""
        row = ["XXX우", "099999", "099998"] + [""] * 31 + ["0"]
        out = pd_mod.parse_sheet_csv(_make_csv(row))
        self.assertEqual(out[0]["dividend_per_share"], 0.0)

    def test_skips_rows_without_code(self):
        row_valid = ["삼성전자우", "005935", "005930"] + [""] * 31 + ["1444"]
        row_blank_code = ["", "", ""] + [""] * 31 + ["100"]
        row_bogus_code = ["label", "not a code!", ""] + [""] * 31 + ["100"]
        csv_text = _make_csv(row_valid, row_blank_code, row_bogus_code)
        out = pd_mod.parse_sheet_csv(csv_text)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["stock_code"], "005935")

    def test_header_missing_year_column_returns_empty(self):
        """AI (우선주 연도) 컬럼 자체가 없으면 안전하게 빈 리스트."""
        broken_header = _HEADER_COLS[:34]  # drop all 우 year columns
        import csv as _csv, io as _io
        buf = _io.StringIO()
        w = _csv.writer(buf)
        w.writerow(broken_header)
        w.writerow(["X우", "012345", "012340"] + [""] * 31)
        out = pd_mod.parse_sheet_csv(buf.getvalue())
        self.assertEqual(out, [])

    def test_picks_most_recent_preferred_year(self):
        """연도 컬럼이 여러 개 있을 때 가장 최신 (2025우) 을 쓴다."""
        # Header already has 2025우~2021우. Put a recognizable value in
        # 2025우 and different values in older ones; verify parser chose
        # 2025.
        row = ["삼성전자우", "005935", "005930"] + [""] * 31 + ["9999", "1111", "2222", "3333", "4444"]
        out = pd_mod.parse_sheet_csv(_make_csv(row))
        self.assertEqual(out[0]["dividend_per_share"], 9999.0)
        self.assertEqual(out[0]["sheet_year"], 2025)

    def test_alphanumeric_code_accepted(self):
        """한화 종류우선주 같은 6자리 영숫자 코드 (예: 00088K) 허용."""
        row = ["종류우선", "00088K", "000880"] + [""] * 31 + ["800"]
        out = pd_mod.parse_sheet_csv(_make_csv(row))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["stock_code"], "00088K")


class UpsertAndLookupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_upsert_is_idempotent(self):
        rows = [
            {"stock_code": "005935", "dividend_per_share": 1444.0, "source_name": "삼성전자우",
             "common_code": "005930", "sheet_year": 2025},
        ]
        self.assertEqual(await cache.upsert_preferred_dividends(rows), 1)
        # Second import with updated value — should update, not duplicate.
        rows[0]["dividend_per_share"] = 1500.0
        await cache.upsert_preferred_dividends(rows)
        self.assertEqual(await cache.get_preferred_dividends_count(), 1)

    async def test_trailing_dividend_priority(self):
        """해결 우선순위 검증:
          1. market_data 직접 매치 (우선주 자체 row) — 가장 드물지만
             존재하면 최우선.
          2. preferred_dividends 시트 — 큐레이션된 값.
          3. 보통주 market_data — 자동 근사치.
        """
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.executemany(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            [
                # 보통주 값 — 우선주 fallback 경로의 최하단.
                ("005930", current_year - 1, 72000, 1444.0),
                # 보통주만 있고 우선주 자체 row 는 없음 (= 흔한 경우).
            ],
        )
        await db.commit()

        # 시트 데이터가 없을 때: 보통주 값으로 fallback.
        dps_fallback = await cache.get_trailing_dividends(["005935"])
        self.assertEqual(dps_fallback.get("005935"), 1444.0)

        # 시트 데이터 주입: 우선주 프리미엄 반영된 값.
        await cache.upsert_preferred_dividends([{
            "stock_code": "005935", "dividend_per_share": 1445.0,
            "source_name": "삼성전자우", "common_code": "005930",
            "sheet_year": 2025,
        }])
        dps_sheet = await cache.get_trailing_dividends(["005935"])
        self.assertEqual(dps_sheet.get("005935"), 1445.0)

        # 우선주 자체 market_data row 가 생기면 그게 최우선.
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            ("005935", current_year - 1, 58000, 1450.0),
        )
        await db.commit()
        dps_direct = await cache.get_trailing_dividends(["005935"])
        self.assertEqual(dps_direct.get("005935"), 1450.0)

    async def test_sheet_zero_is_authoritative(self):
        """시트에 '올해 배당 0' 으로 기록된 우선주는 **그대로 0 을 사용**
        해야 한다. 시트 관리자의 확정 값이므로 보통주 fallback 으로
        가로채면 안 됨 ('이미 공시 다 끝났는데 0 원으로 결정됐다' 는 경우)."""
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            ("005930", current_year - 1, 72000, 1444.0),
        )
        await db.commit()

        await cache.upsert_preferred_dividends([{
            "stock_code": "005935", "dividend_per_share": 0.0,  # 시트 0 = 확정 '배당 없음'
            "source_name": "삼성전자우", "common_code": "005930",
            "sheet_year": 2025,
        }])
        dps = await cache.get_trailing_dividends(["005935"])
        self.assertEqual(dps.get("005935"), 0.0)  # 시트 값 그대로

    async def test_missing_sheet_row_falls_back_to_common(self):
        """시트에 row 자체가 없는 우선주는 기존처럼 보통주 fallback.
        (sheet NULL vs sheet 0 구분 — 전자만 fallback)"""
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            ("005930", current_year - 1, 72000, 1444.0),
        )
        await db.commit()
        # 시트에 005935 row 없음 (upsert 안 함) → 보통주 fallback.
        dps = await cache.get_trailing_dividends(["005935"])
        self.assertEqual(dps.get("005935"), 1444.0)

    async def test_non_preferred_code_unaffected(self):
        """보통주(끝자리 0) 에는 시트 우선 조회 자체가 적용되지 않음.
        market_data 경로만 본다."""
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            ("005930", current_year - 1, 72000, 1444.0),
        )
        await db.commit()
        # Insert a preferred_dividends row for the common code — should
        # be ignored since code doesn't end in a preferred suffix.
        await cache.upsert_preferred_dividends([{
            "stock_code": "005930", "dividend_per_share": 99999.0,
            "source_name": "bogus", "common_code": None, "sheet_year": 2025,
        }])
        dps = await cache.get_trailing_dividends(["005930"])
        self.assertEqual(dps.get("005930"), 1444.0)


if __name__ == "__main__":
    unittest.main()
