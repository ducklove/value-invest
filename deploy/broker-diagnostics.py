"""Sanitized broker diagnostics; explicit optional cached-token recovery."""

import asyncio
import json
import os
import re
import sqlite3
import ssl
import subprocess
import time
from collections import Counter
from pathlib import Path

import truststore
import websockets
from dotenv import load_dotenv


def journal_summary():
    invocation = subprocess.run(["systemctl", "show", "value-invest.service", "-p", "InvocationID", "--value"],
                                capture_output=True, text=True, check=False).stdout.strip()
    command = ["journalctl", "-u", "value-invest.service", "--since", "2 hours ago", "-o", "cat", "--no-pager"]
    if re.fullmatch(r"[a-f0-9]{32}", invocation):
        command.append(f"_SYSTEMD_INVOCATION_ID={invocation}")
    result = subprocess.run(
        command,
        capture_output=True, text=True, check=False,
    )
    patterns = [
        r"NH WebSocket disconnected: market=(domestic|foreign) reason=([a-z_]+) error=([A-Za-z]+)",
        r"NH WebSocket subscription rejected: channel=([A-Za-z0-9]+) code=([A-Z0-9]{1,20}|unknown)",
        r"NH WebSocket closed: market=(domestic|foreign) code=(\d+|None)",
        r"NH WebSocket subscribed: market=(domestic|foreign) approved=(\d+) requested=(\d+)",
        r"NH WebSocket receiving: market=(domestic|foreign)",
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


def connection_summary():
    main_pid = subprocess.run(["systemctl", "show", "value-invest.service", "-p", "MainPID", "--value"],
                              capture_output=True, text=True, check=False).stdout.strip()
    result = subprocess.run(["ss", "-Hntp", "state", "established"], capture_output=True, text=True, check=False)
    counts = Counter()
    for line in result.stdout.splitlines():
        match = re.search(r":(7070|7080)\b", line)
        if match:
            owner = "app" if f"pid={main_pid}," in line else "other_or_unknown"
            counts[f"{owner}:{match[1]}"] += 1
    with sqlite3.connect("file:cache.db?mode=ro", uri=True) as db:
        scanners = db.execute("SELECT count(*) FROM quant_scanners WHERE json_extract(config_json,'$.enabled')=1").fetchone()[0]
    processes = 0
    for process in Path("/proc").glob("[0-9]*"):
        try:
            if (process / "cwd").resolve() == Path.cwd() and b"main:app" in (process / "cmdline").read_bytes().split(b"\0"):
                processes += 1
        except (OSError, RuntimeError):
            continue
    print(json.dumps({"nh_connections": dict(counts), "enabled_scanners": scanners, "app_processes": processes}))


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


async def subscription_probe():
    # Keep tokens on the server; emit only fixed channel names and response codes.
    load_dotenv(Path.cwd() / ".env")
    from repositories.broker_secrets import decrypt

    with sqlite3.connect("file:cache.db?mode=ro", uri=True) as db:
        rows = db.execute("""SELECT DISTINCT c.credential_id, c.token_ciphertext, c.token_expires_at, l.environment
                             FROM broker_credentials c JOIN broker_account_links l USING(credential_id)
                             WHERE l.provider='namuh'""").fetchall()
    print(json.dumps({"linked_credentials": len(rows)}))
    for index, (_, sealed, expires, env) in enumerate(rows):
        if not sealed or (expires or 0) <= time.time():
            print(json.dumps({"credential_index": index, "cached_token": "expired_or_missing"}))
            continue
        access = decrypt(sealed)
        for port, channel, key in [(7070, "mc", "005930"), (7080, "RC", "USAAAPL")]:
            endpoint = "wss://moapi.nhplug.com:17070/websocket" if env == "mock" else f"wss://api.nhplug.com:{port}/websocket"
            result = {"credential_index": index, "port": port, "channel": channel}
            try:
                async with websockets.connect(endpoint, ssl=truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
                                               open_timeout=15, close_timeout=2, ping_interval=None) as ws:
                    await ws.send(json.dumps({"header": {"token": access, "tr_type": "1"},
                                              "body": {"tr_cd": channel, "tr_key": key}}))
                    for _ in range(5):
                        message = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                        code = str(message.get("header", {}).get("rsp_cd", ""))
                        if code:
                            result["response_code"] = code if re.fullmatch(r"[A-Z0-9]{1,20}", code) else "unknown"
                            break
                    if result.get("response_code") == "00000":
                        await asyncio.sleep(.15)
                        await ws.send(json.dumps({"header": {"token": access, "tr_type": "2"},
                                                  "body": {"tr_cd": channel, "tr_key": key}}))
            except Exception as exc:
                result["error"] = type(exc).__name__
                result["close_code"] = getattr(getattr(exc, "rcvd", None), "code", None)
            print(json.dumps(result))


async def refresh_tokens():
    load_dotenv(Path.cwd() / ".env")
    from core import http
    from repositories import brokers, db
    from services.brokers import namuh

    try:
        await http.init_http_clients()
        keys = {(link["google_sub"], link["credential_id"]) for link in await brokers.list_links() if link["provider"] == "namuh"}
        for index, (user, cid) in enumerate(keys):
            try:
                await namuh.token(user, cid, force=True)
                print(json.dumps({"credential_index": index, "token_refreshed": True}))
            except Exception as exc:
                print(json.dumps({"credential_index": index, "token_refreshed": False, "error": type(exc).__name__}))
    finally:
        await http.close_http_clients()
        await db.close_db()


async def main():
    journal_summary()
    connection_summary()
    await asyncio.gather(probe(7070), probe(7080))
    if os.environ.get("REFRESH_TOKEN") == "true":
        await refresh_tokens()
    if os.environ.get("SUBSCRIPTION_PROBE") == "true":
        await subscription_probe()


asyncio.run(main())
