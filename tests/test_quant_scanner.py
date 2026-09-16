import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from _harness import TempDbMixin, seed_user

from repositories import quant_scanner
from repositories.quant import QuantError
from services.brokers import namuh
from services.quant import scanner, scanner_feed
from services.quant.scanner_model import KST, ScannerConfig, book, edge, master_rows, realtime_book, watch_list

NOW = datetime(2026, 9, 16, 10, 0, 0, tzinfo=KST)


def config(**values):
    return ScannerConfig(account_id="account", **values)


def test_master_links_underlying_without_name_guessing():
    raw = b"KA0A6C000" + "한글 선물".encode("cp949").ljust(30) + b"future".ljust(30) + b"005930" + "삼성전자".encode("cp949").ljust(20) + b"1\n"
    row = master_rows(raw)[0]
    assert row["spot_code"] == "005930" and row["name"] == "삼성전자"
    for invalid in (raw[:-1], raw * 2, raw[:-1] + b"x", b"<html>error</html>"):
        with pytest.raises(QuantError):
            master_rows(invalid)


def test_costs_and_scan_cannot_become_live_signal_from_old_quote():
    spot = book(99, 100, 100, 100, "100000", NOW)
    future = book(105, 106, 10, 10, "100000", NOW)
    result = edge(spot, future, "20260917", config(), NOW, realtime=True)
    assert result["net_per_share"] == pytest.approx(4.572439726027)
    assert result["live_eligible"] is False
    spot["at"] -= 60
    assert edge(spot, future, "20260917", config(), NOW)["net_bps"] > 0
    with pytest.raises(ValueError):
        edge(spot, future, "20260917", config(), NOW, realtime=True)
    spot["at"] = NOW.timestamp()
    for expiry in ("20260916", "20280101"):
        with pytest.raises(ValueError):
            edge(spot, future, expiry, config(), NOW)


def test_candidate_hysteresis_and_slot_limit():
    now = NOW.timestamp()
    rows = [{"contract": str(i), "net_bps": 100 + i, "observed_at": now} for i in range(50)]
    selected = watch_list(rows, {}, config(max_pairs=15), now)
    assert len(selected) == 15 and "49" in selected and "0" not in selected
    rows[49]["net_bps"] = 0
    assert "49" in watch_list(rows, selected, config(max_pairs=15), now + 20)
    assert "49" not in watch_list(rows, selected, config(max_pairs=15), now + 400)
    assert not watch_list(rows, selected, config(), now + 7201)
    with pytest.raises(ValueError):
        config(watch_bps=100, signal_bps=50)


def test_realtime_book_checks_identity_numbers_and_time():
    msg = {"header": {"tr_cd": "vH", "tr_key": "A0A6C000"},
           "body": {"fuitem": "A0A6C000", "futime": "10:00:00", "offer": "106", "bid": "105", "offerjan": "2", "bidjan": "3"}}
    pair, q = realtime_book(msg, NOW)
    assert pair == ("vH", "A0A6C000") and q["bid_size"] == 3
    msg["body"]["bid"] = "nan"
    assert realtime_book(msg, NOW) is None
    msg["body"]["bid"] = "105"
    msg["header"]["tr_key"] = "OTHER"
    assert realtime_book(msg, NOW) is None
    with pytest.raises(ValueError):
        book(1, 2, 2, 3, "100500", NOW)


class ScannerTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")

    async def test_generation_stop_and_owner_isolation(self):
        await quant_scanner.configure("u1", config(enabled=True).model_dump())
        generation = (await quant_scanner.settings("u1"))[0]["generation"]
        row = {"contract": "KA0A6C000", "net_bps": 50}
        assert await quant_scanner.record("u1", generation, {"cursor": 1}, row=row, event=row)
        assert await quant_scanner.rows("u2") == []
        assert await quant_scanner.events("u2") == []
        await quant_scanner.stop("u1")
        assert not (await quant_scanner.settings("u1"))[0]["config"]["enabled"]
        assert not await quant_scanner.record("u1", generation, {"cursor": 99}, event=row)
        assert len(await quant_scanner.events("u1")) == 1
        assert (await quant_scanner.settings("u1"))[0]["progress"]["cursor"] == 1

    async def test_config_rejects_another_users_connection(self):
        with patch.object(scanner.brokers, "list_links", AsyncMock(return_value=[{"google_sub": "u2", "account_id": "account"}])):
            with self.assertRaises(QuantError):
                await scanner.configure("u1", config(enabled=True))
        assert await quant_scanner.settings("u1") == []

    async def test_rest_mapping_and_shared_request_adapter(self):
        now = datetime.now(KST)
        at = (now - timedelta(seconds=1)).strftime("%H%M%S")
        future = {"Output_0": {"iem_cd": "A0A6C000", "bidp1": 105, "askp1": 106,
                  "bidp_rsqn1": 10, "askp_rsqn1": 10, "bsop_hour": at, "last_tr_date": "20261210",
                  "cncc_cls_code": "0", "dynmc_prc_lmt_yn": "N"}}
        spot = {"Output_0": {"iem_cd": "005930", "bidp1": 99, "askp1": 100, "bidp_rsqn1": 100,
                "askp_rsqn1": 100, "hoga_bsop_hour": at}}
        with patch.object(namuh, "pages", AsyncMock(side_effect=[[future], [spot]])) as call:
            s, f, expiry = await scanner_feed.snapshot("u1", "cid", {"contract": "KA0A6C000", "spot_code": "005930"}, "mock", {})
        assert call.call_count == 2 and s["ask"] == 100 and f["bid"] == 105 and expiry == "20261210"
        assert call.call_args_list[1].args[3]["market_cd"] == "KRX"
        assert not any("/order/" in p for p in namuh.READ_PATHS)

        # 동적 가격제한 적용 자체는 거래 정지가 아니다. 경계·누락 상태는 차단한다.
        future["Output_0"].update(dynmc_prc_lmt_yn="Y", dynmc_lwlmtprc=90, dynmc_uplmtprc=110)
        with patch.object(namuh, "pages", AsyncMock(side_effect=[[future], [spot]])):
            _, f, _ = await scanner_feed.snapshot("u1", "cid", {"contract": "KA0A6C000", "spot_code": "005930"}, "mock", {})
        assert f["bid"] == 105
        future["Output_0"]["dynmc_uplmtprc"] = 106
        with patch.object(namuh, "pages", AsyncMock(side_effect=[[future], [spot]])):
            with self.assertRaisesRegex(ValueError, "가격제한"):
                await scanner_feed.snapshot("u1", "cid", {"contract": "KA0A6C000", "spot_code": "005930"}, "mock", {})

    async def test_watcher_requires_ack_and_both_books_before_recording(self):
        messages = [
            {"header": {"rsp_cd": "00000"}, "body": {"tr_key": ["005930", "A0A6C000"]}},
            {"header": {"tr_cd": "ob", "tr_key": "005930"}, "body": {"code": "005930", "hotime": "100000", "bid": 99, "offer": 100, "bidrem": 100, "offerrem": 100}},
            {"header": {"tr_cd": "vH", "tr_key": "A0A6C000"}, "body": {"fuitem": "A0A6C000", "futime": "100000", "bid": 105, "offer": 106, "bidjan": 10, "offerjan": 10}},
        ]

        class Socket:
            def __init__(self):
                self.sent = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def send(self, value):
                self.sent.append(json.loads(value))

            async def recv(self):
                return json.dumps(messages.pop(0))

        socket = Socket()
        watcher = scanner.Watcher("u1", "cid", "mock", config(), "generation", {})
        watcher.selected = {"KA0A6C000": NOW.timestamp()}
        watcher.rows = {"KA0A6C000": {"spot_code": "005930", "expiry": "20261210", "name": "검증"}}
        with patch.object(scanner.websockets, "connect", return_value=socket) as connect, \
             patch.object(namuh, "token", AsyncMock(return_value="test-only")), \
             patch.object(scanner, "datetime") as clock, \
             patch.object(quant_scanner, "record", AsyncMock(return_value=False)) as record:
            clock.now.return_value = NOW
            await asyncio.wait_for(watcher.run(), timeout=2)
        assert len(socket.sent) == 2
        assert connect.call_args.args[0] == "wss://api.nhplug.com:7070/websocket"
        assert {s["body"]["tr_cd"] for s in socket.sent} == {"ob", "vH"}
        event = record.call_args.kwargs["event"]
        assert record.call_count == 1 and event["type"] == "opportunity"
        assert event["spot"]["ask"] == 100 and event["future"]["bid"] == 105
        assert event["live_eligible"] is False
