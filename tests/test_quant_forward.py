import copy
import json
from datetime import date
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user
from test_quant import config, result

from repositories import quant, quant_forward
from repositories.db import transaction
from services.quant import forward, service


def fresh_result():
    r = result()
    r["config"]["end"] = "2026-01-05"
    r["config_hash"] = quant.digest(r["config"])
    bars = [
        {
            "date": day,
            **{leg: {"price": 100, "trading_value": 1e9, "tradable": True} for leg in ("common", "preferred")},
        }
        for day in ("2026-01-02", "2026-01-05")
    ]
    snap = {"common": "005930", "preferred": "005935", "bars": bars, "catalog": r["snapshot"]["catalog"]}
    r["snapshot"] = {**snap, "snapshot_id": quant.digest(snap)}
    r["signals"] = [{"date": b["date"], "target": "common", "z": 0} for b in bars]
    r["latest_signal"] = r["signals"][-1]
    ledger = {
        "version": forward.VERSION,
        "start": "2026-01-02",
        "status": "available",
        "orders_sent": 0,
        "execution_model": "next_observation_close_replay",
        "scenarios": [
            {
                "mode": mode,
                "nav": [
                    {"date": b["date"], "nav": 1e7, "cash": 1e7, "common_quantity": 0, "preferred_quantity": 0}
                    for b in bars
                ],
                "trades": [],
                "return_pct": 0,
                "trade_count": 0,
                "cost": 0,
                "ending_cash": 1e7,
                "max_drawdown_pct": 0,
            }
            for mode in ("switch", "mixed")
        ],
    }
    r["forward"] = {**ledger, "ledger_hash": quant.digest(ledger)}
    return r


class ForwardTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")

    async def setup_run(self):
        run = await quant.create_run("u1", "forward-test", config())
        await quant.claim()
        await quant.finish(run["id"], result())
        return run["id"]

    async def session(self):
        rid = await self.setup_run()
        await quant_forward.start("u1", rid)
        async with transaction() as db:
            await db.execute("UPDATE quant_forward SET start_date='2026-01-02' WHERE run_id=?", (rid,))
        return await quant_forward.due()

    async def test_registration_idempotency_ownership_and_permanent_stop(self):
        rid = await self.setup_run()
        with self.assertRaises(quant.QuantError):
            await quant_forward.start("u2", rid)
        await quant_forward.start("u1", rid)
        first = await quant_forward.get("u1", rid)
        await quant_forward.start("u1", rid)
        self.assertEqual(first, await quant_forward.get("u1", rid))
        self.assertGreater(first["start_date"], str(service.completed_date()))
        session = await quant_forward.due()
        await quant_forward.stop("u1", rid)
        await quant_forward.save(session, {"late": True})
        self.assertIsNone((await quant_forward.get("u1", rid))["payload"])
        with self.assertRaises(quant.QuantError):
            await quant_forward.start("u1", rid)
        with self.assertRaises(quant.QuantError):
            await quant_forward.get("u2", rid)

    async def test_future_start_does_not_request_market_data(self):
        rid = await self.setup_run()
        await quant_forward.start("u1", rid)
        with patch.object(service, "fetch", AsyncMock()) as fetch:
            await forward.run_one(await quant_forward.due())
            fetch.assert_not_awaited()

    async def test_readiness_failure_preserves_previous_ledger(self):
        session = await self.session()
        await quant_forward.save(session, {"saved": True})
        with patch.object(service, "fetch", AsyncMock(return_value={"status": "not_ready"})):
            await forward.run_one(session)
        row = await quant_forward.get("u1", session["run_id"])
        self.assertEqual(row["payload"], {"saved": True})
        self.assertIn("보류", row["error"])

    async def test_replay_restart_and_later_input_revision_are_checked(self):
        session = await self.session()
        fresh = fresh_result()
        ready = {"status": "ready", "scope": "pair_daily_prices", "checks": {"latest_price_date": "2026-01-05"}}

        async def run(data):
            with (
                patch.object(service, "fetch", AsyncMock(side_effect=[ready, data])),
                patch.object(forward, "completed_date", return_value=date(2026, 1, 5)),
            ):
                await forward.run_one(session)

        await run(fresh)
        row = await quant_forward.get("u1", session["run_id"])
        self.assertIsNone(row["error"])
        saved = row["payload"]
        session["payload_json"] = json.dumps(saved)
        await run(fresh)
        self.assertEqual((await quant_forward.get("u1", session["run_id"]))["payload"], saved)
        changed = copy.deepcopy(fresh)
        changed["snapshot"]["bars"][0]["common"]["price"] = 101
        changed["snapshot"]["snapshot_id"] = quant.digest(
            {k: v for k, v in changed["snapshot"].items() if k != "snapshot_id"}
        )
        await run(changed)
        row = await quant_forward.get("u1", session["run_id"])
        self.assertIn("입력이 변경", row["error"])
        self.assertEqual(row["payload"], saved)

    async def test_hash_recalculation_cannot_hide_unbalanced_ledger(self):
        r = fresh_result()
        r["forward"]["scenarios"][0]["nav"][-1]["cash"] -= 10
        r["forward"]["ledger_hash"] = quant.digest({k: v for k, v in r["forward"].items() if k != "ledger_hash"})
        with self.assertRaises(quant.QuantError):
            forward.verify_ledger(r["forward"], r["snapshot"], r["config"], "2026-01-02")

    async def test_fill_cost_and_liquidity_contracts(self):
        r = fresh_result()
        scenario = r["forward"]["scenarios"][0]
        price = 100 * (1 + config()["slippage_bps"] / 10000)
        fee = price * config()["commission_bps"] / 10000
        cost = fee + price - 100
        scenario["trades"] = [
            {
                "date": "2026-01-05",
                "signal_date": "2026-01-02",
                "leg": "common",
                "side": "buy",
                "quantity": 1,
                "price": price,
                "fee": fee,
                "cost": cost,
            }
        ]
        cash = 1e7 - price - fee
        scenario["nav"][-1].update(cash=cash, common_quantity=1, nav=cash + 100)
        ret = ((cash + 100) / 1e7 - 1) * 100
        scenario.update(cost=cost, trade_count=1, ending_cash=cash, return_pct=ret, max_drawdown_pct=ret)

        def check():
            r["forward"]["ledger_hash"] = quant.digest({k: v for k, v in r["forward"].items() if k != "ledger_hash"})
            forward.verify_ledger(r["forward"], r["snapshot"], r["config"], "2026-01-02")

        check()
        for field, value in (("tradable", False), ("trading_value", 0)):
            before = r["snapshot"]["bars"][-1]["common"][field]
            r["snapshot"]["bars"][-1]["common"][field] = value
            with self.assertRaises(quant.QuantError):
                check()
            r["snapshot"]["bars"][-1]["common"][field] = before
        scenario["trades"][0]["fee"] = 0
        with self.assertRaises(quant.QuantError):
            check()
