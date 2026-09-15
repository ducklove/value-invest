import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from repositories import quant
from services.quant import service
from services.quant.models import ResearchConfig


def config():
    return ResearchConfig(
        catalog_snapshot_id="a" * 64,
        common="005930", preferred="005935", start=date(2024, 1, 1), end=date(2025, 12, 31)
    ).model_dump(mode="json")


def result():
    c = config()
    snapshot = {"common": c["common"], "preferred": c["preferred"], "bars": [],
                "catalog": {"provider": "common_preferred_spread", "catalog_snapshot_id": c["catalog_snapshot_id"],
                            "pair": {"common": c["common"], "preferred": c["preferred"], "execution_eligible": False}}}
    return {
        "config": c,
        "config_hash": quant.digest(c),
        "engine_version": service.EXPECTED_ENGINE,
        "snapshot": {**snapshot, "snapshot_id": quant.digest(snapshot)},
        "live_eligible": False,
        "signals": [{"date": "2025-12-31", "target": "common"}],
        "latest_signal": {"date": "2025-12-31", "target": "common"},
        "scenarios": [
            {
                "mode": mode,
                "nav": [{"date": "2025-12-31", "nav": 10000000}],
                "trades": [],
                "return_pct": 0,
                "max_drawdown_pct": 0,
                "cost": 0,
                "trade_count": 0,
            }
            for mode in ("switch", "common", "preferred", "mixed")
        ],
        "stress": {"return_pct": 0},
    }


class QuantTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")

    async def test_factor_audit_failure_does_not_hide_pair_research(self):
        with patch.object(
            service,
            "fetch",
            AsyncMock(
                side_effect=[
                    {"pairs": [{"common": "005930", "preferred": "005935"}]},
                    {"status": "not_ready"},
                    {"status": "ready", "scope": "pair_daily_prices"},
                    service.ExternalServiceError("입력 검사 연결 실패"),
                ]
            ),
        ):
            cap = await service.capabilities()
        self.assertEqual(len(cap["pairs"]), 1)
        self.assertEqual(cap["factor_inputs"]["status"], "unavailable")
        self.assertIsNone(cap["error"])

    async def test_idempotency_scope_and_conflicting_payload(self):
        a, b = await asyncio.gather(
            quant.create_run("u1", "same-key", config()), quant.create_run("u1", "same-key", config())
        )
        self.assertEqual(a["id"], b["id"])
        with self.assertRaises(quant.QuantError):
            await quant.create_run("u1", "same-key", {**config(), "capital": 20000})
        with self.assertRaises(quant.QuantError):
            await quant.get_run("u2", a["id"])
        self.assertEqual(await quant.list_runs("u2"), [])

    async def test_cancel_wins_race_with_completion(self):
        a = await quant.create_run("u1", "test-cancel", config())
        claimed = await quant.claim()
        self.assertEqual(claimed["id"], a["id"])
        await quant.cancel("u1", a["id"])
        await quant.finish(a["id"], result())
        final = await quant.get_run("u1", a["id"])
        self.assertEqual(final["status"], "cancelled")
        self.assertIsNone(final["result"])

    async def test_worker_persists_result_and_restart_requeues(self):
        a = await quant.create_run("u1", "test-restart", config())
        await quant.claim()
        await quant.recover()
        with patch.object(service, "fetch", AsyncMock(return_value=result())):
            self.assertTrue(await service.run_one())
        final = await quant.get_run("u1", a["id"])
        self.assertEqual(final["status"], "succeeded")
        self.assertEqual(final["result"], result())

    async def test_wrong_config_or_snapshot_is_never_accepted(self):
        for key, value in [("config_hash", "bad"), ("engine_version", "wrong"), ("live_eligible", True)]:
            with self.assertRaises(quant.QuantError):
                service.verify({**result(), key: value}, config())
        invalid = result()
        invalid["snapshot"]["common"] = "000000"
        with self.assertRaises(quant.QuantError):
            service.verify(invalid, config())

    async def test_etf_requires_its_own_engine(self):
        c = {**config(), "strategy": "etf_switch"}
        r = {**result(), "config": c, "config_hash": quant.digest(c)}
        with self.assertRaises(quant.QuantError):
            service.verify(r, c)
        r["engine_version"] = "etf-switch-3"
        r["snapshot"]["catalog"]["provider"] = "eiayn"
        r["snapshot"]["snapshot_id"] = quant.digest({k: v for k, v in r["snapshot"].items() if k != "snapshot_id"})
        service.verify(r, c)

    async def test_catalog_provenance_is_checked_even_with_valid_snapshot_hash(self):
        for field, wrong in (("provider", "eiayn"), ("catalog_snapshot_id", "b" * 64)):
            r = result()
            r["snapshot"]["catalog"][field] = wrong
            r["snapshot"]["snapshot_id"] = quant.digest({k: v for k, v in r["snapshot"].items() if k != "snapshot_id"})
            with self.assertRaises(quant.QuantError):
                service.verify(r, config())

    async def test_watch_requires_owned_completed_run_and_blocks_bad_readiness(self):
        a = await quant.create_run("u1", "watch-1", config())
        with self.assertRaises(quant.QuantError):
            await quant.set_watch("u1", a["id"], True)
        await quant.claim()
        await quant.finish(a["id"], result())
        with self.assertRaises(quant.QuantError):
            await quant.set_watch("u2", a["id"], True)
        await quant.set_watch("u1", a["id"], True)
        watch = (await quant.watches())[0]
        with patch.object(service, "fetch", AsyncMock(return_value={"status": "not_ready"})) as fetch:
            await service.observe_one(watch)
            self.assertEqual(fetch.await_count, 1)
        self.assertEqual(await quant.observations("u1"), [])
        self.assertIn("보류", (await quant.watches("u1"))[0]["error"])

    async def test_observation_duplicate_and_stop_during_fetch(self):
        a = await quant.create_run("u1", "observe-1", config())
        await quant.claim()
        await quant.finish(a["id"], result())
        await quant.set_watch("u1", a["id"], True)
        watch = (await quant.watches())[0]
        payload = {"signal": {"date": "2026-01-01"}, "orders_sent": 0}
        await quant.record_observation(watch, payload)
        await quant.record_observation(watch, payload)
        self.assertEqual(len(await quant.observations("u1")), 1)
        self.assertEqual(await quant.observations("u2"), [])
        await quant.set_watch("u1", a["id"], False)
        await quant.record_observation(watch, {"signal": {"date": "2026-01-02"}})
        self.assertEqual(len(await quant.observations("u1")), 1)

    async def test_price_scoped_readiness_records_observation_with_its_evidence(self):
        a = await quant.create_run("u1", "price-only-observe", config())
        await quant.claim()
        await quant.finish(a["id"], result())
        await quant.set_watch("u1", a["id"], True)
        ready = {
            "status": "ready",
            "scope": "pair_daily_prices",
            "global_status": "not_ready",
            "checks": {"latest_price_date": "2026-01-02"},
            "unrelated_macro_failures": 1,
        }
        fresh = result()
        fresh["config"]["end"] = "2026-01-02"
        fresh["config_hash"] = quant.digest(fresh["config"])
        fresh["signals"] = [{"date": "2026-01-02", "target": "common"}]
        fresh["latest_signal"] = fresh["signals"][-1]
        with (
            patch.object(service, "completed_date", return_value=date(2026, 1, 2)),
            patch.object(service, "fetch", AsyncMock(side_effect=[ready, fresh])),
        ):
            await service.observe_one((await quant.watches())[0])
        rows = await quant.observations("u1")
        self.assertEqual(rows[0]["payload"]["data_readiness"], ready)
        self.assertEqual(rows[0]["payload"]["orders_sent"], 0)

    async def test_no_broker_calls_in_worker_failure(self):
        a = await quant.create_run("u1", "failure-1", config())
        with patch.object(service, "fetch", AsyncMock(side_effect=quant.QuantError("자료 부족"))):
            await service.run_one()
        self.assertEqual((await quant.get_run("u1", a["id"]))["status"], "failed")

    async def test_observation_protects_prices_added_after_original_research(self):
        a = await quant.create_run("u1", "protected-extension", config())
        await quant.claim()
        await quant.finish(a["id"], result())
        await quant.set_watch("u1", a["id"], True)
        watch = (await quant.watches())[0]
        await quant.record_observation(
            watch,
            {"signal": {"date": "2026-01-02"}, "input_extension": [{"date": "2026-01-02", "common": {"price": 100}}]},
        )
        fresh = result()
        fresh["config"]["end"] = "2026-01-05"
        fresh["config_hash"] = quant.digest(fresh["config"])
        fresh["signals"] = [{"date": "2026-01-05", "target": "common"}]
        fresh["latest_signal"] = fresh["signals"][-1]
        ready = {"status": "ready", "scope": "pair_daily_prices", "checks": {"latest_price_date": "2026-01-05"}}
        with (
            patch.object(service, "completed_date", return_value=date(2026, 1, 5)),
            patch.object(service, "fetch", AsyncMock(side_effect=[ready, fresh])),
        ):
            await service.observe_one(watch)
        self.assertEqual(len(await quant.observations("u1")), 1)
        self.assertIn("수정", (await quant.watches("u1"))[0]["error"])
