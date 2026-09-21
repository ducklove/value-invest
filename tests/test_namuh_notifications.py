import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from services.brokers import notifications, realtime


@pytest.mark.asyncio
async def test_shared_quote_socket_subscribes_empty_notification_key_and_never_books_push_amounts():
    sent, changed = [], []

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, value):
            sent.append(json.loads(value)["body"])

        async def __aiter__(self):
            yield json.dumps({"header": {"tr_cd": "d2", "rsp_cd": "00000"}})
            yield json.dumps({"header": {"tr_cd": "d2"}, "body": {"accountno": "12345678901", "concprc": "invalid"}})
            raise asyncio.CancelledError

    with patch.object(realtime.namuh, "token", AsyncMock(return_value="token")), \
         patch.object(realtime.websockets, "connect", return_value=Socket()) as connect, \
         patch.object(notifications, "account_for_message", AsyncMock(return_value="account")) as resolve:
        with pytest.raises(asyncio.CancelledError):
            await realtime.stream("owner", "cid", ["KRX_GOLD"], "live", notice_channels=("d2",), changed=changed.append)
    assert connect.call_count == 1
    assert connect.call_args.args[0] == "wss://api.nhplug.com:7070/websocket"
    assert sent == [{"tr_cd": "d2", "tr_key": ""}, {"tr_cd": "g4", "tr_key": "M04020000"}]
    assert changed == [None, "account"]  # 접속 직후 복구 조회와 실제 통보만 재조회 신호가 된다.
    assert resolve.await_count == 1  # 구독 ACK는 계좌 변경이 아니다.
    assert notifications.status("owner")["subscribed"] == 1
    notifications._states.clear()


@pytest.mark.asyncio
async def test_server_starts_notification_sockets_for_every_key_without_holdings_or_browsers():
    links = [{"google_sub": "owner", "credential_id": cid, "account_id": aid, "environment": "live", "product": product}
             for cid, aid, product in [("key1", "a1", "stocks"), ("key2", "a2", "gold"), ("key2", "a3", "krfuture"), ("key2", "a4", "gbfuture")]]
    calls = []
    stop = asyncio.Event()

    async def stream(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 2:
            stop.set()
        await stop.wait()

    with patch.object(realtime.brokers, "list_links", AsyncMock(return_value=links)), \
         patch.object(realtime.account_holdings, "list_positions", AsyncMock(return_value=[])), \
         patch.object(realtime, "sync_account", AsyncMock()) as sync, patch.object(realtime, "stream", side_effect=stream):
        await asyncio.wait_for(realtime.run(stop), 3)
    assert {args[1] for args, _ in calls} == {"key1", "key2"}
    assert all(args[2] == [] for args, _ in calls)
    assert set(next(options["notice_channels"] for args, options in calls if args[1] == "key2")) == {"d2", "d3", "de", "dv", "dn", "dk", "dj"}
    assert sync.await_count == 4
    assert all(call.kwargs["include_activity"] for call in sync.await_args_list)
