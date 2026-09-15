import asyncio
import json
from datetime import datetime, timedelta
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from services.brokers import realtime


def gold_message(at, **fields):
    return {"header": {"tr_cd": "g4", "tr_key": "M04020000"}, "body": {
        "shcode": "M04020000", "time": at.strftime("%H%M%S"), "cheprice": "199480",
        "change": "1670", "sign": "2", "totvalue": "8130761510", **fields,
    }}


class GoldRealtimeTests(IsolatedAsyncioTestCase):
    def setUp(self):
        self.now = datetime.now(realtime._KST).replace(microsecond=0)
        realtime._quotes.clear()
        realtime._status.clear()

    def tearDown(self):
        realtime._quotes.clear()
        realtime._status.clear()

    def test_gold_units_change_and_channel_mapping(self):
        for sign, change in [("2", 1670), ("5", -1670)]:
            tick = realtime.normalize(gold_message(self.now, sign=sign), self.now)
            self.assertEqual(tick["code"], "KRX_GOLD")
            self.assertEqual(tick["price"], 199480)
            self.assertEqual(tick["previous_close"], 199480 - change)
            self.assertEqual(tick["change"], change)
            self.assertAlmostEqual(tick["change_pct"], change / (199480 - change) * 100)
            self.assertEqual(tick["trade_value"], 8130761510)
            self.assertEqual((tick["source"], tick["market"], tick["currency"]), ("namuh_ws", "KRX", "KRW"))

    def test_rejects_other_gold_products_non_trades_and_invalid_values(self):
        invalid = [{"shcode": "M04020100"}, {"shcode": "005930"}, {"cheprice": "0"},
                   {"cheprice": "NaN"}, {"cheprice": "Infinity"}, {"change": "NaN"},
                   {"totvalue": "Infinity"}, {"time": "bad"}]
        for fields in invalid:
            with self.subTest(fields=fields):
                self.assertIsNone(realtime.normalize(gold_message(self.now, **fields), self.now))
        for head in [{"tr_cd": "g5"}, {"tr_cd": "gE"}, {"tr_cd": "g4", "rsp_cd": "00000"},
                     {"tr_cd": "g4", "tr_type": "1"}, {"tr_cd": "g4", "tr_key": "M04020100"}]:
            message = gold_message(self.now)
            message["header"] = head
            self.assertIsNone(realtime.normalize(message, self.now))

    def test_gold_freshness_and_per_user_cache_fallback(self):
        for seconds in [-90, 1]:
            message = gold_message(self.now + timedelta(seconds=seconds))
            self.assertIsNone(realtime.normalize(message, self.now))
        tick = realtime.normalize(gold_message(self.now), self.now)
        realtime._quotes.set(("owner", "KRX_GOLD"), tick)
        self.assertEqual(realtime.quote("owner", "KRX_GOLD")["price"], 199480)
        self.assertIsNone(realtime.quote("other", "KRX_GOLD"))
        tick["as_of"] = (self.now - timedelta(seconds=91)).isoformat()
        realtime._quotes.set(("owner", "KRX_GOLD"), tick)
        self.assertIsNone(realtime.quote("owner", "KRX_GOLD"))

    def test_gold_shares_session_limit_and_is_deduplicated_across_accounts(self):
        rows = [{"stock_code": f"{code:06d}"} for code in range(40)]
        rows += [{"stock_code": code} for code in ["KRX_GOLD", "KRX_GOLD", "CASH_KRW", "AAPL", "M04020100"]]
        codes = realtime.select_codes(rows, 30)
        self.assertEqual(len(codes), 30)
        self.assertEqual(codes[0], "KRX_GOLD")
        self.assertEqual(realtime.subscription(codes[0]), ("g4", "M04020000"))
        self.assertEqual(realtime.subscription(codes[1]), ("mc", "000000"))
        self.assertEqual(realtime.select_codes(rows, 1), ["KRX_GOLD"])

    async def test_single_connection_subscribes_stock_and_gold_and_rejects_older_tick(self):
        now = self.now
        sent = []

        class Socket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def send(self, message):
                sent.append(json.loads(message)["body"])

            async def __aiter__(self):
                yield json.dumps({"header": {"rsp_cd": "00000"}, "body": {"tr_key": ["005930", "M04020000"]}})
                yield json.dumps(gold_message(now))
                yield json.dumps(gold_message(now - timedelta(seconds=1), cheprice="199000"))
                raise asyncio.CancelledError

        with patch.object(realtime.namuh, "token", AsyncMock(return_value="test-token")), \
             patch.object(realtime.websockets, "connect", return_value=Socket()) as connect:
            with self.assertRaises(asyncio.CancelledError):
                await realtime.stream("owner", "credential", ["005930", "KRX_GOLD"], "live")
        self.assertEqual(connect.call_count, 1)
        self.assertEqual(sent, [{"tr_cd": "mc", "tr_key": "005930"}, {"tr_cd": "g4", "tr_key": "M04020000"}])
        self.assertEqual(realtime.quote("owner", "KRX_GOLD")["price"], 199480)
        self.assertEqual(realtime.status("owner")["subscribed"], 2)
        self.assertEqual(realtime.status("owner")["state"], "live")

    async def test_gold_only_account_starts_background_subscription(self):
        stop = asyncio.Event()

        async def stream(*args):
            stop.set()

        link = {"google_sub": "owner", "credential_id": "credential", "account_id": "account", "environment": "live"}
        with patch.object(realtime.brokers, "list_links", AsyncMock(return_value=[link])), \
             patch.object(realtime.account_holdings, "list_positions", AsyncMock(return_value=[{"stock_code": "KRX_GOLD"}])), \
             patch.object(realtime, "sync_account", AsyncMock()), \
             patch.object(realtime, "stream", AsyncMock(side_effect=stream)) as stream_mock:
            await asyncio.wait_for(realtime.run(stop), timeout=2)
        stream_mock.assert_awaited_once_with("owner", "credential", ["KRX_GOLD"], "live")
