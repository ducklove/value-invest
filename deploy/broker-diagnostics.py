"""Read-only broker diagnostics. Never print journal messages or credentials."""

import asyncio
import json
import re
import ssl
import subprocess
from collections import Counter

import truststore
import websockets


def journal_summary():
    result = subprocess.run(
        ["journalctl", "-u", "value-invest.service", "--since", "2 hours ago", "-o", "cat", "--no-pager"],
        capture_output=True, text=True, check=False,
    )
    patterns = [
        r"NH WebSocket disconnected: market=(domestic|foreign) reason=([a-z_]+) error=([A-Za-z]+)",
        r"NH WebSocket subscription rejected: channel=([A-Za-z0-9]+) code=([A-Z0-9]{1,20}|unknown)",
        r"NH WebSocket closed: market=(domestic|foreign) code=(\d+|None)",
        r"NH 계좌 동기화 보류: ([A-Za-z]+)",
        r"NH 연결 목록 재확인 예정: ([A-Za-z]+)",
    ]
    counts = Counter()
    for line in result.stdout.splitlines():
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                counts[match.group()] += 1
    print(json.dumps({"journal_exit": result.returncode, "broker_errors": dict(counts)}, ensure_ascii=False))


async def probe(port):
    endpoint = f"wss://api.nhplug.com:{port}/websocket"
    try:
        async with websockets.connect(
            endpoint, ssl=truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
            open_timeout=15, close_timeout=2, ping_interval=None,
        ):
            print(json.dumps({"port": port, "handshake": "ok"}))
    except Exception as exc:
        print(json.dumps({"port": port, "error": type(exc).__name__,
                          "verify_code": getattr(exc, "verify_code", None),
                          "errno": getattr(exc, "errno", None)}))


async def main():
    journal_summary()
    await asyncio.gather(probe(7070), probe(7080))


asyncio.run(main())
