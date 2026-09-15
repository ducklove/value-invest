from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from repositories import quant_basis
from repositories.quant import QuantError, digest
from services.quant import basis


def response(payload):
    body = {
        "engine_version": basis.VERSION,
        "input_hash": digest(payload),
        "config": {"contract": "TEST", "capital": 10000},
        "mode": "scenario",
        "live_eligible": False,
        "source_verified": False,
        "orders_sent": 0,
        "scenarios": [
            {
                "orders_sent": 0,
                "ledger": [{"pnl": 100, "spot_pnl": 0, "future_pnl": 105, "dividends": 0, "costs": 5, "equity": 10100}],
                "pnl": 100,
            }
        ],
    }
    return {**body, "result_hash": digest(body)}


class BasisTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")

    async def test_persist_scope_and_idempotency(self):
        payload = {"config": {}, "scenario": {}}
        r = response(payload)
        first = await quant_basis.save("u1", "basis-key", payload, r)
        again = await quant_basis.save("u1", "basis-key", payload, r)
        self.assertEqual(first["id"], again["id"])
        self.assertEqual((await quant_basis.get("u1", first["id"]))["result"], r)
        self.assertEqual(len(await quant_basis.listing("u1")), 1)
        self.assertEqual(await quant_basis.listing("u2"), [])
        with self.assertRaises(QuantError):
            await quant_basis.get("u2", first["id"])
        with self.assertRaises(QuantError):
            await quant_basis.save("u1", "basis-key", {"changed": True}, r)

    async def test_verify_rejects_hash_orders_and_pnl_mismatch(self):
        payload = {"config": {}, "scenario": {}}
        basis.verify(response(payload), payload)
        for field, value in (("orders_sent", 1), ("input_hash", "wrong"), ("source_verified", True)):
            r = response(payload)
            r[field] = value
            with self.assertRaises(QuantError):
                basis.verify(r, payload)
        r = response(payload)
        r["scenarios"][0]["pnl"] = 1000
        r["result_hash"] = digest({k: v for k, v in r.items() if k != "result_hash"})
        with self.assertRaises(QuantError):
            basis.verify(r, payload)

    async def test_repeated_request_does_not_recompute_or_call_broker(self):
        payload = {"config": {}, "scenario": {}}
        row = await quant_basis.save("u1", "basis-key", payload, response(payload))
        with patch.object(basis, "get_http_client", AsyncMock()) as client:
            repeated = await basis.run("u1", "basis-key", payload)
        self.assertEqual(row["id"], repeated["id"])
        client.assert_not_awaited()

    async def test_oversized_request_is_rejected_before_network(self):
        with self.assertRaises(QuantError):
            await basis.run("u1", "basis-key", {"source_note": "가" * 200000})
        with self.assertRaises(QuantError):
            await basis.run("u1", "basis-key", {"config": {"capital": float("nan")}})
