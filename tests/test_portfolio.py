import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import cache
from routes import portfolio as portfolio_route
from services.portfolio import dividends
from services.portfolio import target_metrics as target_metrics_service


class PortfolioTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        # Previous test may have left cache._conn pointing at a now-deleted
        # temp DB or a closed handle. close_db() is idempotent and resets
        # the singleton so init_db() opens a fresh conn on the patched path.
        await cache.close_db()
        await cache.init_db()
        portfolio_route._dividend_warmup_last.clear()
        portfolio_route._dividend_warmup_tasks.clear()

        db = await cache.get_db()
        await db.execute(
            "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "user@example.com", "User", "", 1, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        await db.executemany(
            "INSERT INTO corp_codes (stock_code, corp_code, corp_name, updated_at) VALUES (?, ?, ?, ?)",
            [
                ("005930", "00126380", "삼성전자", "2026-01-01"),
                ("000660", "00164779", "SK하이닉스", "2026-01-01"),
                ("002380", "00105271", "케이씨씨", "2026-01-01"),
                ("021320", "00105466", "KCC건설", "2026-01-01"),
            ],
        )
        await db.commit()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    # --- CRUD ---

    async def test_empty_portfolio(self):
        items = await cache.get_portfolio("u1")
        self.assertEqual(items, [])

    async def test_add_item(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["stock_code"], "005930")
        self.assertEqual(items[0]["quantity"], 100)
        self.assertEqual(items[0]["avg_price"], 65000)
        self.assertEqual(items[0]["tags"], [])

    async def test_portfolio_tags_roundtrip(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        saved = await cache.set_portfolio_tags(
            "u1",
            "005930",
            ["자산주", "#턴어라운드", "자산주", "  AI 관련주  ", "ai 관련주"],
        )
        self.assertEqual(saved, ["자산주", "턴어라운드", "AI 관련주"])

        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["tags"], ["자산주", "턴어라운드", "AI 관련주"])
        self.assertEqual(await cache.get_portfolio_tags("u1", "005930"), ["자산주", "턴어라운드", "AI 관련주"])

    async def test_target_price_formula_roundtrip(self):
        await cache.save_portfolio_item(
            "u1",
            "005930",
            "삼성전자",
            100,
            65000,
            target_price_formula="BPS*0.4+DPS*10",
        )

        items = await cache.get_portfolio("u1")
        self.assertIsNone(items[0]["target_price"])
        self.assertEqual(items[0]["target_price_formula"], "BPS*0.4+DPS*10")
        self.assertEqual(items[0]["target_price_disabled"], 0)

        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000, target_price=100000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["target_price"], 100000)
        self.assertIsNone(items[0]["target_price_formula"])

        db = await cache.get_db()
        cursor = await db.execute(
            "SELECT target_price_formula FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
            ("u1", "005930"),
        )
        row = await cursor.fetchone()
        self.assertEqual(row["target_price_formula"], "")

    async def test_target_price_formula_keeps_calculated_fallback(self):
        await cache.save_portfolio_item(
            "u1",
            "005930",
            "삼성전자",
            100,
            65000,
            target_price=32000,
            target_price_formula="BPS*0.5",
        )

        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["target_price"], 32000)
        self.assertEqual(items[0]["target_price_formula"], "BPS*0.5")

    async def test_regular_edit_preserves_formula_with_calculated_fallback(self):
        await cache.save_portfolio_item(
            "u1",
            "005930",
            "삼성전자",
            100,
            65000,
            target_price=32000,
            target_price_formula="BPS*0.5",
        )

        await cache.save_portfolio_item("u1", "005930", "삼성전자", 150, 64000)

        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["quantity"], 150)
        self.assertEqual(items[0]["target_price"], 32000)
        self.assertEqual(items[0]["target_price_formula"], "BPS*0.5")

    async def test_target_formula_resolves_holding_value_fallback(self):
        holding_meta = {
            "002380": {
                "totalShares": 1000,
                "treasuryShares": 100,
                "subsidiaries": [
                    {"code": "028260", "sharesHeld": 10},
                    {"code": "009540", "sharesHeld": 5},
                ],
            }
        }

        async def fake_quote(code):
            return {"price": {"028260": 100, "009540": 200}[code]}

        with patch.object(
            portfolio_route.integrations,
            "build_public_integrations",
            return_value={"holdingValue": {"meta": holding_meta}},
        ), patch.object(portfolio_route, "_fetch_quote", new=AsyncMock(side_effect=fake_quote)):
            target_price = await portfolio_route._resolve_target_formula_price("002380", "보유지분", 611500)

        self.assertAlmostEqual(target_price, (100 * 10 + 200 * 5) / 900)

    async def test_target_formula_uses_holding_value_snapshot_without_quotes(self):
        holding_meta = {
            "004700": {
                "totalShares": 1000,
                "treasuryShares": 100,
                "holdingValuePerShare": 12345.67,
                "subsidiaries": [{"code": "BRK-A", "sharesHeld": 1}],
            }
        }

        with patch.object(
            portfolio_route.integrations,
            "build_public_integrations",
            return_value={"holdingValue": {"meta": holding_meta}},
        ), patch.object(portfolio_route, "_fetch_quote", new=AsyncMock()) as fetch_quote:
            target_price = await portfolio_route._resolve_target_formula_price("004700", "보유지분", 611500)

        fetch_quote.assert_not_awaited()
        self.assertEqual(target_price, 12345.67)

    async def test_portfolio_target_metrics_use_latest_positive_values(self):
        db = await cache.get_db()
        await db.executemany(
            """INSERT INTO market_data (stock_code, year, eps, bps, dividend_per_share)
               VALUES (?, ?, ?, ?, ?)""",
            [
                ("005930", 2025, 1000, 50000, 1500),
                ("005930", 2026, None, None, None),
            ],
        )
        await db.commit()

        metrics = await cache.get_portfolio_target_metrics(["005930"])

        self.assertEqual(metrics["005930"], {"eps": 1000, "bps": 50000, "dps": 1500})

    async def test_latest_market_valuation_joins_financial_data(self):
        db = await cache.get_db()
        await db.execute(
            """INSERT INTO market_data
               (stock_code, year, close_price, per, pbr, eps, bps, market_cap)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("005930", 2025, 70000, 12.5, 1.2, 5600, 58000, 400_000_000_000_000),
        )
        await db.execute(
            """INSERT INTO financial_data
               (stock_code, year, report_date, net_income, total_equity)
               VALUES (?, ?, ?, ?, ?)""",
            ("005930", 2025, "2025-12-31", 44_000_000_000_000, 420_000_000_000_000),
        )
        await db.commit()

        valuation = await cache.get_latest_market_valuation("005930")

        self.assertEqual(valuation["year"], 2025)
        self.assertEqual(valuation["per"], 12.5)
        self.assertEqual(valuation["pbr"], 1.2)
        self.assertEqual(valuation["eps"], 5600)
        self.assertEqual(valuation["bps"], 58000)
        self.assertEqual(valuation["net_income"], 44_000_000_000_000)
        self.assertEqual(valuation["total_equity"], 420_000_000_000_000)

    async def test_target_metric_uses_shared_valuation_basis_without_writing_cache(self):
        target_metrics_map = {"037350": {"eps": None, "bps": None, "dps": None}}
        shared_basis = {
            "applicable": True,
            "source": "internal_fundamentals",
            "sourceCode": "037350",
            "bps": 18017.424347205786,
            "eps": 1982.0,
        }

        with patch.object(target_metrics_service, "fetch_valuation_basis_map", new=AsyncMock(return_value={"037350": shared_basis})) as shared:
            await target_metrics_service.supplement_target_metrics(
                [{"stock_code": "037350", "target_price_formula": "BPS*0.5"}],
                target_metrics_map,
            )

        shared.assert_awaited_once()
        self.assertEqual(target_metrics_map["037350"]["bps"], 18017.424347205786)
        self.assertIsNone(target_metrics_map["037350"]["eps"])

        saved = await cache.get_portfolio_target_metrics(["037350"])
        self.assertIsNone(saved["037350"]["bps"])
        self.assertIsNone(saved["037350"]["eps"])

    async def test_target_metric_does_not_refetch_cached_values(self):
        target_metrics_map = {"037350": {"eps": None, "bps": 18017.424347205786, "dps": None}}

        with patch.object(target_metrics_service, "fetch_valuation_basis_map", new=AsyncMock()) as shared:
            await target_metrics_service.supplement_target_metrics(
                [{"stock_code": "037350", "target_price_formula": "BPS*0.5"}],
                target_metrics_map,
            )

        shared.assert_not_awaited()
        self.assertEqual(target_metrics_map["037350"]["bps"], 18017.424347205786)

    async def test_target_metric_uses_shared_valuation_basis_for_eps_and_bps(self):
        target_metrics_map = {"005930": {"eps": None, "bps": None, "dps": None}}
        shared_basis = {
            "applicable": True,
            "source": "internal_fundamentals",
            "sourceCode": "005930",
            "bps": 63997.24,
            "eps": 6675.68,
        }

        with patch.object(target_metrics_service, "fetch_valuation_basis_map", new=AsyncMock(return_value={"005930": shared_basis})):
            await target_metrics_service.supplement_target_metrics(
                [{"stock_code": "005930", "target_price_formula": "BPS*0.5+EPS"}],
                target_metrics_map,
            )

        self.assertEqual(target_metrics_map["005930"]["bps"], 63997.24)
        self.assertEqual(target_metrics_map["005930"]["eps"], 6675.68)

    async def test_portfolio_tag_suggestions_by_usage(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        await cache.set_portfolio_tags("u1", "005930", ["AI", "자산주"])
        await cache.set_portfolio_tags("u1", "000660", ["AI", "반도체"])

        self.assertEqual(await cache.get_portfolio_tag_suggestions("u1"), ["AI", "반도체", "자산주"])

    async def test_update_item(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 200, 70000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["quantity"], 200)
        self.assertEqual(items[0]["avg_price"], 70000)

    async def test_update_benchmark_reports_missing_row(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)

        updated = await cache.update_portfolio_benchmark("u1", "005930", "IDX_KOSPI")
        missing = await cache.update_portfolio_benchmark("u1", "999999", "IDX_KOSPI")
        items = await cache.get_portfolio("u1")

        self.assertTrue(updated)
        self.assertFalse(missing)
        self.assertEqual(items[0]["benchmark_code"], "IDX_KOSPI")

    async def test_benchmark_quotes_use_fast_default_without_market_prefetch(self):
        await cache.save_portfolio_item("u1", "005930", "?쇱꽦?꾩옄", 100, 65000)

        with patch.object(portfolio_route, "get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch.object(portfolio_route, "_prefetch_market_types", new=AsyncMock(side_effect=AssertionError("slow prefetch"))), \
             patch.object(portfolio_route, "_fetch_benchmark_quote", new=AsyncMock(return_value={"change_pct": 1.23})):
            data = await portfolio_route.get_benchmark_quotes(object())

        self.assertEqual(data["IDX_KOSPI"]["change_pct"], 1.23)

    async def test_delete_item(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.set_portfolio_tags("u1", "005930", ["자산주"])
        await cache.delete_portfolio_item("u1", "005930")
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 0)
        self.assertEqual(await cache.get_portfolio_tags("u1", "005930"), [])

    async def test_get_portfolio_exposes_created_at(self):
        """UI 의 '등록일자' 컬럼이 비어있지 않도록, get_portfolio SELECT 에
        created_at 이 반드시 포함돼야 한다는 계약 고정."""
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertIn("created_at", items[0])
        self.assertTrue(items[0]["created_at"])  # non-empty ISO string

    async def test_save_portfolio_preserves_created_at_on_edit(self):
        """수량/매입가만 편집할 때 등록일자가 리셋되면 안 된다."""
        import asyncio
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        original = (await cache.get_portfolio("u1"))[0]["created_at"]
        # Wait a beat so the timestamp would differ if we accidentally reset
        # created_at to now() during the second save.
        await asyncio.sleep(0.01)
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 200, 70000)
        updated = (await cache.get_portfolio("u1"))[0]["created_at"]
        self.assertEqual(updated, original)

    async def test_save_portfolio_accepts_explicit_created_at(self):
        """등록일자 edit form 에서 넘어온 명시적 값은 존중되어야 함."""
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        explicit = "2025-01-15T00:00:00"
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 100, 65000, created_at=explicit,
        )
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["created_at"], explicit)

    async def test_target_price_default_null(self):
        """신규 등록 시 target_price 미전달이면 NULL → 프론트에서 자동 계산 경로."""
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        items = await cache.get_portfolio("u1")
        self.assertIn("target_price", items[0])
        self.assertIsNone(items[0]["target_price"])

    async def test_target_price_explicit_value_persists(self):
        """수동 override 값은 그대로 저장."""
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 100, 65000, target_price=85000.0,
        )
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["target_price"], 85000.0)

    async def test_target_price_unchanged_sentinel_preserves(self):
        """수량/매입가만 편집할 때 (target_price 인자 미전달) 기존 override 유지.
        sentinel 처리가 깨지면 자동 계산으로 reset 되어 사용자 의도 위반."""
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 100, 65000, target_price=85000.0,
        )
        # target_price 인자 안 넘김 → 기존 85000 유지되어야
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 200, 70000,
        )
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["target_price"], 85000.0)
        self.assertEqual(items[0]["quantity"], 200)
        self.assertEqual(items[0]["avg_price"], 70000)

    async def test_target_price_explicit_none_clears(self):
        """target_price=None 명시는 자동 계산으로 되돌림 (override 해제)."""
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 100, 65000, target_price=85000.0,
        )
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 100, 65000, target_price=None,
        )
        items = await cache.get_portfolio("u1")
        self.assertIsNone(items[0]["target_price"])

    async def test_get_trailing_dividends(self):
        """market_data 의 가장 최근 positive 배당금을 종목별로 반환.
        0 또는 NULL 인 해는 건너뛰고, 올해는 아직 공시 전일 수 있으므로
        제외 (stock_price.py 의 dividend fallback 과 동일한 원칙)."""
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.executemany(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            [
                # 삼성전자: current_year-1 은 0 이지만 MAX(year) 가 그 해.
                # 새 로직은 0 도 유효값으로 돌려주므로 0 반환 (배당 중단
                # 상태 정직 표시).
                ("005930", current_year - 2, 70000, 1444.0),
                ("005930", current_year - 1, 72000, 0.0),
                ("005930", current_year, 75000, None),
                # SK하이닉스: 단일 positive 연도
                ("000660", current_year - 1, 100000, 1200.0),
                # 네이버: 최근 해 0 → 0 반환 ('-' 아님).
                ("035420", current_year - 1, 200000, 0.0),
            ],
        )
        await db.commit()
        dps = await cache.get_trailing_dividends(["005930", "000660", "035420", "999999"])
        # 005930 은 current_year-1 = 0 이므로 그 값 반환.
        self.assertEqual(dps.get("005930"), 0.0)
        self.assertEqual(dps.get("000660"), 1200.0)
        # 035420 도 0 그대로 (이전엔 dict 에서 제외됐지만 이제 포함).
        self.assertEqual(dps.get("035420"), 0.0)
        self.assertNotIn("999999", dps)  # no rows → 여전히 제외

    async def test_get_trailing_dividends_empty_list(self):
        self.assertEqual(await cache.get_trailing_dividends([]), {})

    async def test_dividend_warmup_fetches_missing_domestic_market_data(self):
        """신규 국내 종목은 분석 화면을 열기 전에도 포트폴리오 배당 캐시가
        백그라운드로 채워져야 한다. KCC(002380)처럼 alias 로 막 등록한
        종목의 배당액이 계속 '-' 로 남는 회귀를 막는다."""
        from datetime import datetime

        current_year = datetime.now().year
        rows = [
            {"year": current_year - 1, "close_price": 400000.0, "dividend_per_share": 10000.0},
        ]
        fetch = AsyncMock(return_value=rows)
        dart_fetch = AsyncMock(return_value={})

        with patch.object(portfolio_route.dart_client, "fetch_dividend_per_share_by_year", dart_fetch), \
             patch.object(portfolio_route.stock_price, "fetch_market_data", fetch):
            await portfolio_route._warm_market_data_for_dividend("002380")

        dart_fetch.assert_awaited_once()
        fetch.assert_awaited_once()
        dps = await cache.get_trailing_dividends(["002380"])
        self.assertEqual(dps.get("002380"), 10000.0)

    async def test_dividend_warmup_skips_when_trailing_dps_exists(self):
        from datetime import datetime

        db = await cache.get_db()
        current_year = datetime.now().year
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            ("002380", current_year - 1, 400000, 10000.0),
        )
        await db.commit()

        fetch = AsyncMock(return_value=[])
        dart_fetch = AsyncMock(return_value={})
        with patch.object(portfolio_route.dart_client, "fetch_dividend_per_share_by_year", dart_fetch), \
             patch.object(portfolio_route.stock_price, "fetch_market_data", fetch):
            await portfolio_route._warm_market_data_for_dividend("002380")

        dart_fetch.assert_awaited_once()
        fetch.assert_not_awaited()

    async def test_dividend_warmup_overwrites_stale_latest_dps_from_dart(self):
        from datetime import datetime

        db = await cache.get_db()
        current_year = datetime.now().year
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            ("002380", current_year - 1, 420500, 10000.0),
        )
        await db.commit()

        dart_fetch = AsyncMock(return_value={current_year - 1: 15000.0})
        fetch = AsyncMock(return_value=[])
        with patch.object(portfolio_route.dart_client, "fetch_dividend_per_share_by_year", dart_fetch), \
             patch.object(portfolio_route.stock_price, "fetch_market_data", fetch):
            await portfolio_route._warm_market_data_for_dividend("002380")

        dart_fetch.assert_awaited_once()
        fetch.assert_not_awaited()
        dps = await cache.get_trailing_dividends(["002380"])
        self.assertEqual(dps.get("002380"), 15000.0)
        rows = await cache.get_market_data("002380")
        self.assertEqual(rows[-1]["dividend_yield"], 3.57)

    def test_dividend_warmup_targets_preferred_common_too(self):
        self.assertEqual(
            dividends.dividend_warmup_targets("005935"),
            ["005935", "005930"],
        )
        self.assertEqual(dividends.dividend_warmup_targets("002380"), ["002380"])
        self.assertEqual(dividends.dividend_warmup_targets("AAPL"), [])

    async def test_search_kcc_alias_prefers_parent_company(self):
        results = await cache.search_corp("KCC")

        self.assertGreaterEqual(len(results), 2)
        self.assertEqual(results[0]["stock_code"], "002380")
        self.assertEqual(results[0]["corp_name"], "케이씨씨")
        self.assertEqual(results[1]["stock_code"], "021320")

    async def test_resolve_corp_search_query_accepts_kcc_alias(self):
        resolved = await cache.resolve_corp_search_query("KCC")

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["stock_code"], "002380")
        self.assertEqual(resolved["corp_name"], "케이씨씨")

    async def test_get_trailing_dividends_preferred_falls_back_to_common(self):
        """우선주 (005935, 00088K 같은 코드) 는 market_data 에 자체 row 가
        없는 경우가 대부분이므로 해당 보통주 (005930, 000880) 의 값으로
        근사하도록 fallback. 이게 없으면 우선주는 배당액이 전부 0 으로 표시됨."""
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.executemany(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            [
                # 보통주만 등록 — 우선주 직접 row 는 없음.
                ("005930", current_year - 1, 72000, 1444.0),
                ("000880", current_year - 1, 9000, 800.0),
            ],
        )
        await db.commit()
        dps = await cache.get_trailing_dividends([
            "005935",   # 삼성전자 우선주 → 005930 fallback
            "00088K",   # 한화 종류우선주 → 000880 fallback (6자리, K suffix)
            "005930",   # 직접 매치
            "999999",   # 매치 없음
        ])
        self.assertEqual(dps.get("005935"), 1444.0)
        self.assertEqual(dps.get("00088K"), 800.0)
        self.assertEqual(dps.get("005930"), 1444.0)
        self.assertNotIn("999999", dps)

    async def test_get_trailing_dividends_preferred_prefers_direct_match(self):
        """우선주 자체 row 가 DB 에 있으면 그 값이 우선. 즉 실제 우선주
        배당 프리미엄이 정확히 기록된 경우 fallback 을 건너뛴다."""
        from datetime import datetime
        db = await cache.get_db()
        current_year = datetime.now().year
        await db.executemany(
            """INSERT INTO market_data (stock_code, year, close_price, dividend_per_share)
               VALUES (?, ?, ?, ?)""",
            [
                ("005930", current_year - 1, 72000, 1444.0),
                # 우선주는 보통주보다 배당이 약간 많게 실제 기록되었다고 가정
                ("005935", current_year - 1, 58000, 1445.0),
            ],
        )
        await db.commit()
        dps = await cache.get_trailing_dividends(["005935"])
        self.assertEqual(dps.get("005935"), 1445.0)

    async def test_delete_nonexistent(self):
        await cache.delete_portfolio_item("u1", "999999")
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 0)

    async def test_multiple_items(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 2)

    # --- Ordering ---

    async def test_reorder(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        await cache.save_portfolio_order("u1", ["000660", "005930"])
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["stock_code"], "000660")
        self.assertEqual(items[1]["stock_code"], "005930")

    async def test_new_item_goes_to_top(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_order("u1", ["005930"])
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(items[0]["stock_code"], "000660")

    # --- Delete + re-add ---

    async def test_delete_then_readd(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.delete_portfolio_item("u1", "005930")
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 50, 72000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["quantity"], 50)

    async def test_delete_one_keeps_others(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        await cache.delete_portfolio_item("u1", "005930")
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["stock_code"], "000660")

    # --- Preferred stock name resolution ---

    async def test_resolve_common_stock(self):
        name = await cache.resolve_stock_name("005930")
        self.assertEqual(name, "삼성전자")

    async def test_resolve_returns_none_for_preferred(self):
        """Preferred stocks are not in corp_codes; resolve returns None (Naver fallback in route)."""
        name = await cache.resolve_stock_name("005935")
        self.assertIsNone(name)

    # --- Bulk / clear ---

    async def test_clear_portfolio(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        await cache.clear_portfolio("u1")
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 0)

    async def test_clear_then_add(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.clear_portfolio("u1")
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 50, 190000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["stock_code"], "000660")

    async def test_bulk_add_preserves_existing(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 30, 180000)
        items = await cache.get_portfolio("u1")
        self.assertEqual(len(items), 2)

    async def test_bulk_replace_clears_first(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)
        await cache.clear_portfolio("u1")
        await cache.save_portfolio_item("u1", "000660", "SK하이닉스", 50, 190000)
        items = await cache.get_portfolio("u1")
        self.assertNotIn("005930", [i["stock_code"] for i in items])
        self.assertIn("000660", [i["stock_code"] for i in items])

    async def test_resolve_unknown_stock(self):
        name = await cache.resolve_stock_name("999999")
        self.assertIsNone(name)

    async def test_group_weight_history_uses_snapshot_values(self):
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 10, 1000,
            group_name="국내주식",
        )
        await cache.save_portfolio_item(
            "u1", "000660", "SK하이닉스", 10, 1000,
            group_name="반도체",
        )
        await cache.save_snapshot("u1", "2026-01-02", 1000, 800, 1000, 1, 1400)
        await cache.save_stock_snapshots(
            "u1",
            "2026-01-02",
            [
                {"stock_code": "005930", "market_value": 300},
                {"stock_code": "000660", "market_value": 700},
            ],
        )

        rows = await cache.get_group_weight_history("u1")

        by_group = {row["group_name"]: row for row in rows}
        self.assertAlmostEqual(by_group["국내주식"]["weight_pct"], 30.0)
        self.assertAlmostEqual(by_group["반도체"]["weight_pct"], 70.0)
        self.assertEqual(by_group["반도체"]["market_value"], 700)
        self.assertEqual(by_group["반도체"]["stock_count"], 1)

    async def test_group_weight_history_prefers_snapshot_group_name(self):
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 10, 1000,
            group_name="현재그룹",
        )
        await cache.save_snapshot("u1", "2026-01-02", 1000, 800, 1000, 1, 1400)
        await cache.save_stock_snapshots(
            "u1",
            "2026-01-02",
            [{"stock_code": "005930", "market_value": 1000, "group_name": "스냅샷그룹"}],
        )

        rows = await cache.get_group_weight_history("u1")

        self.assertEqual(rows[0]["group_name"], "스냅샷그룹")
        self.assertAlmostEqual(rows[0]["weight_pct"], 100.0)
        self.assertEqual(rows[0]["stock_count"], 1)

    async def test_group_constituent_history_uses_group_total(self):
        await cache.save_portfolio_item(
            "u1", "005930", "삼성전자", 10, 1000,
            group_name="반도체",
        )
        await cache.save_portfolio_item(
            "u1", "000660", "SK하이닉스", 10, 1000,
            group_name="반도체",
        )
        await cache.save_portfolio_item(
            "u1", "035420", "NAVER", 10, 1000,
            group_name="인터넷",
        )
        await cache.save_snapshot("u1", "2026-01-02", 2000, 1500, 1000, 2, 1400)
        await cache.save_stock_snapshots(
            "u1",
            "2026-01-02",
            [
                {"stock_code": "005930", "market_value": 300},
                {"stock_code": "000660", "market_value": 700},
                {"stock_code": "035420", "market_value": 1000},
            ],
        )

        rows = await cache.get_group_constituent_history("u1", "반도체")

        by_code = {row["stock_code"]: row for row in rows}
        self.assertEqual(set(by_code), {"005930", "000660"})
        self.assertAlmostEqual(by_code["005930"]["weight_pct"], 30.0)
        self.assertAlmostEqual(by_code["000660"]["weight_pct"], 70.0)
        self.assertEqual(by_code["005930"]["group_value"], 1000)
        self.assertEqual(by_code["000660"]["stock_name"], "SK하이닉스")

    async def test_group_weight_history_normalizes_to_stock_snapshot_total(self):
        await cache.save_portfolio_item(
            "u1", "AAA", "Alpha", 10, 1000,
            group_name="Core",
        )
        await cache.save_portfolio_item(
            "u1", "BBB", "Beta", 10, 1000,
            group_name="Satellite",
        )
        # Deliberately lower than the per-stock snapshot sum. Group trend
        # should still stack to 100% for the visible stock snapshot universe.
        await cache.save_snapshot("u1", "2026-01-02", 900, 800, 1000, 1, 1400)
        await cache.save_stock_snapshots(
            "u1",
            "2026-01-02",
            [
                {"stock_code": "AAA", "market_value": 300, "group_name": "Core"},
                {"stock_code": "BBB", "market_value": 700, "group_name": "Satellite"},
            ],
        )

        rows = await cache.get_group_weight_history("u1")

        self.assertAlmostEqual(sum(row["weight_pct"] for row in rows), 100.0)
        by_group = {row["group_name"]: row for row in rows}
        self.assertAlmostEqual(by_group["Core"]["weight_pct"], 30.0)
        self.assertAlmostEqual(by_group["Satellite"]["weight_pct"], 70.0)
        self.assertEqual(by_group["Core"]["total_value"], 1000)

    async def test_group_and_stock_weights_are_materialized_on_snapshot_save(self):
        await cache.save_portfolio_item(
            "u1", "AAA", "Alpha", 10, 1000,
            group_name="Core",
        )
        await cache.save_portfolio_item(
            "u1", "BBB", "Beta", 10, 1000,
            group_name="Core",
        )
        await cache.save_snapshot("u1", "2026-01-02", 1000, 800, 1000, 1, 1400)
        await cache.save_stock_snapshots(
            "u1",
            "2026-01-02",
            [
                {"stock_code": "AAA", "market_value": 300, "group_name": "Core"},
                {"stock_code": "BBB", "market_value": 700, "group_name": "Core"},
            ],
        )

        db = await cache.get_db()
        cursor = await db.execute(
            "SELECT group_name, market_value, stock_count, weight_pct FROM portfolio_group_snapshots WHERE google_sub = ?",
            ("u1",),
        )
        group_rows = [dict(row) for row in await cursor.fetchall()]
        self.assertEqual(len(group_rows), 1)
        self.assertEqual(group_rows[0]["group_name"], "Core")
        self.assertEqual(group_rows[0]["stock_count"], 2)
        self.assertAlmostEqual(group_rows[0]["weight_pct"], 100.0)

        rows = await cache.get_group_constituent_history("u1", "Core")
        by_code = {row["stock_code"]: row for row in rows}
        self.assertEqual(set(by_code), {"AAA", "BBB"})
        self.assertAlmostEqual(by_code["AAA"]["weight_pct"], 30.0)
        self.assertAlmostEqual(by_code["BBB"]["weight_pct"], 70.0)
        self.assertAlmostEqual(by_code["AAA"]["portfolio_weight_pct"], 30.0)
        self.assertEqual(by_code["AAA"]["stock_name"], "Alpha")
