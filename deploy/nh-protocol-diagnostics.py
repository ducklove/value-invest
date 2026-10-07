"""Isolated cached-token NH checks without preliminary WebSocket connections."""

import asyncio
import hashlib
import importlib.util
import io
import json
import os
import re
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import types
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import httpx
import truststore
import websockets
from dotenv import load_dotenv
from websockets.uri import get_proxy, parse_uri


def emit(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


def safe_message(value, secrets):
    value = str(value)
    for secret in secrets:
        if secret:
            value = value.replace(secret, "[redacted]")
    value = re.sub(r"[A-Za-z0-9_./+=-]{16,}|\d{7,}", "[redacted]", value)
    return value[:200]


def safe_code(value):
    value = str(value or "")
    return value if re.fullmatch(r"[A-Z0-9]{1,20}", value) else "unknown"


def inventory():
    commands, unreadable = 0, 0
    for path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            if b"main:app" in path.read_bytes().split(b"\0"):
                commands += 1
        except OSError:
            unreadable += 1
    try:
        docker = subprocess.run(["docker", "ps", "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}"],
                                capture_output=True, text=True, check=False, timeout=5)
        containers = len(docker.stdout.splitlines()) if docker.returncode == 0 else None
        for line in docker.stdout.splitlines() if docker.returncode == 0 else []:
            cid, name, image = line.split("\t")
            if not re.fullmatch(r"[a-f0-9]{12,64}", cid):
                continue
            result = subprocess.run(["docker", "exec", cid, "sh", "-c", "cat /proc/net/tcp /proc/net/tcp6"],
                                    capture_output=True, text=True, check=False, timeout=5)
            states = {}
            for row in result.stdout.splitlines():
                fields = row.split()
                if len(fields) > 3 and ":" in fields[2]:
                    try:
                        port = int(fields[2].split(":")[1], 16)
                    except ValueError:
                        continue
                    if port in {7070, 7080, 17070}:
                        label = f"{port}:state_{fields[3]}"
                        states[label] = states.get(label, 0) + 1
            emit({"container_name": name, "image": image, "network_read_ok": result.returncode == 0,
                  "nh_port_connections": states})
    except (OSError, subprocess.TimeoutExpired):
        containers = None
    proxy = get_proxy(parse_uri("wss://api.nhplug.com:7070/websocket"))
    app_proxy = None
    pid = subprocess.run(["systemctl", "show", "value-invest.service", "-p", "MainPID", "--value"],
                         capture_output=True, text=True, check=False).stdout.strip()
    if pid.isdigit() and pid != "0":
        try:
            environment = dict(item.decode().split("=", 1) for item in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if b"=" in item)
            saved = dict(os.environ)
            try:
                os.environ.clear()
                os.environ.update(environment)
                app_proxy = bool(get_proxy(parse_uri("wss://api.nhplug.com:7070/websocket")))
            finally:
                os.environ.clear()
                os.environ.update(saved)
        except (OSError, UnicodeError):
            pass
    addresses = sorted({row[4][0] for row in socket.getaddrinfo("api.nhplug.com", 7070, type=socket.SOCK_STREAM)})
    emit({"all_app_processes": commands, "unreadable_processes": unreadable,
          "running_containers": containers, "ws_proxy_configured": bool(proxy),
          "app_ws_proxy_configured": app_proxy,
          "dns_addresses": len(addresses)})
    return addresses


async def account_probe(access, secrets, context):
    result = {"check": "cached_token_rest"}
    try:
        async with httpx.AsyncClient(verify=context, trust_env=False, timeout=15) as client:
            response = await client.post("https://api.nhplug.com:8443/n2/acctinfo",
                                         headers={"Authorization": "Bearer " + access}, json={"Input_0": {}})
        result["http_status"] = response.status_code
        data = response.json()
        if isinstance(data, dict):
            result["response_code"] = safe_code(data.get("rsp_cd"))
            result["response_message"] = safe_message(data.get("rsp_msg", ""), secrets)
            rows = data.get("Output_0")
            result["accounts_count"] = len(rows) if isinstance(rows, list) else int(isinstance(rows, dict))
    except Exception as exc:
        result["error"] = type(exc).__name__
    emit(result)


async def reset_sessions(access, secrets, context):
    # Official session recovery API; no account or order operation.
    result = {"check": "reset_sessions"}
    try:
        async with httpx.AsyncClient(verify=context, trust_env=False, timeout=15) as client:
            response = await client.post("https://api.nhplug.com:8443/websocket/close/session",
                                         headers={"Authorization": "Bearer " + access,
                                                  "Content-Type": "application/json;charset=utf-8"})
        result["http_status"] = response.status_code
        data = response.json()
        result["response_code"] = safe_code(data.get("rsp_cd"))
        result["response_message"] = safe_message(data.get("rsp_msg", ""), secrets)
    except Exception as exc:
        result["error"] = type(exc).__name__
    emit(result)


async def read_ack(ws, secrets):
    async with asyncio.timeout(5):
        for _ in range(10):
            message = json.loads(await ws.recv())
            head = message.get("header", {}) if isinstance(message, dict) else {}
            if isinstance(head, dict) and "rsp_cd" in head:
                return {"response_code": safe_code(head["rsp_cd"]),
                        "response_message": safe_message(head.get("rsp_msg", ""), secrets)}
    return {"response_code": "no_ack"}


async def quote_probe(access, secrets, context, *, port=7070, origin=None, host=None, notice=False):
    mock = port == 17070
    endpoint = f"wss://{'moapi' if mock else 'api'}.nhplug.com:{port}/websocket"
    result = {"check": "mock_notice" if notice else "single_quote", "port": port, "origin": bool(origin), "dns_override": bool(host),
              "proxy": False, "compression": False}
    pair = ("d2", "") if notice else ("RC", "USAAAPL") if port == 7080 else ("mc", "005930")
    packet = {"header": {"token": access, "tr_type": "1"}, "body": {"tr_cd": pair[0], "tr_key": pair[1]}}
    options = {"host": host, "server_hostname": "api.nhplug.com"} if host else {}
    try:
        async with websockets.connect(endpoint, ssl=context, proxy=None, compression=None, origin=origin,
                                      ping_interval=None, open_timeout=15, close_timeout=3, **options) as ws:
            await ws.send(json.dumps(packet))
            result.update(await read_ack(ws, secrets))
            if result.get("response_code") == "00000":
                try:
                    async with asyncio.timeout(3):
                        for _ in range(10):
                            message = json.loads(await ws.recv())
                            if isinstance(message, dict) and message.get("header", {}).get("tr_cd") == pair[0] and "rsp_cd" not in message.get("header", {}):
                                result["data_push_received"] = True
                                break
                except TimeoutError:
                    result["data_push_received"] = False
                finally:
                    try:
                        packet["header"]["tr_type"] = "2"
                        await ws.send(json.dumps(packet))
                        result["deregistration_code"] = (await read_ack(ws, secrets))["response_code"]
                    except (OSError, websockets.exceptions.WebSocketException, TimeoutError):
                        result["deregistration_code"] = "connection_closed_or_timeout"
    except Exception as exc:
        result["error"] = type(exc).__name__
        result["close_code"] = getattr(getattr(exc, "rcvd", None), "code", None)
    emit(result)
    return result


def sdk_probe(access, secrets):
    # Run the pinned official subscription implementation; reuse the cached token.
    root = Path(os.environ["NH_SDK_PROBE_DIR"])
    sys.path.insert(0, str(root / "deps"))
    package = types.ModuleType("nhplug")
    package.__path__ = []
    auth = types.ModuleType("nhplug.auth")
    auth.get_base_url = lambda: "https://api.nhplug.com:8443"
    auth.get_token = lambda: access
    sys.modules.update({"nhplug": package, "nhplug.auth": auth})
    spec = importlib.util.spec_from_file_location("nhplug.realtime", root / "realtime.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = {"check": "official_sdk", "sdk_commit": "13866ff45dadb25d89e7e68db69ffe79620b198a"}

    def receive(message):
        head = message.get("header", {}) if isinstance(message, dict) else {}
        if "rsp_cd" in head:
            result.update(response_code=safe_code(head["rsp_cd"]),
                          response_message=safe_message(head.get("rsp_msg", ""), secrets))
        else:
            result["data_push_received"] = True

    try:
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            module._run_session(["005930"], receive, tr_cd="mc", url="wss://api.nhplug.com:7070/websocket",
                                max_messages=1, timeout=5, stop=threading.Event(), include_ack=True)
    except Exception as exc:
        result["error"] = type(exc).__name__
    emit(result)


async def main():
    load_dotenv(Path.cwd() / ".env")
    from repositories.broker_secrets import decrypt

    addresses = inventory()
    if os.environ.get("NH_INVENTORY_ONLY") == "true":
        return
    with sqlite3.connect("file:cache.db?mode=ro", uri=True) as db:
        rows = db.execute("""SELECT DISTINCT c.secret_ciphertext,c.token_ciphertext,c.token_expires_at,c.key_fingerprint
                             FROM broker_credentials c JOIN broker_account_links l USING(credential_id)
                             WHERE l.provider='namuh'""").fetchall()
    emit({"linked_credentials": len(rows)})
    context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    for index, (sealed_secret, sealed_token, expires, digest) in enumerate(rows):
        if not sealed_token or (expires or 0) <= time.time():
            emit({"credential_index": index, "cached_token": "missing_or_expired"})
            continue
        secret, access = json.loads(decrypt(sealed_secret)), decrypt(sealed_token)
        secrets = [access, secret.get("app_key", ""), secret.get("app_secret", "")]
        emit({"credential_index": index, "key_fingerprint_matches": hashlib.sha256(secret["app_key"].encode()).hexdigest() == digest,
              "token_has_surrounding_whitespace": access != access.strip(), "token_has_bearer_prefix": access.startswith("Bearer ")})
        await account_probe(access, secrets, context)
        if os.environ.get("NH_RESET_SESSIONS") == "true":
            await asyncio.sleep(1.1)
            await reset_sessions(access, secrets, context)
            await asyncio.sleep(1)
        if os.environ.get("NH_SDK_PROBE_DIR"):
            await asyncio.to_thread(sdk_probe, access, secrets)
            await asyncio.sleep(1)
        base = await quote_probe(access, secrets, context)
        if base.get("response_code") != "00000":
            await asyncio.sleep(1)
            await quote_probe(access, secrets, context, origin="https://api.nhplug.com:7070")
            await asyncio.sleep(1)
            await quote_probe(access, secrets, context, port=17070)
            await asyncio.sleep(1)
            await quote_probe(access, secrets, context, port=17070, notice=True)
            if len(addresses) > 1:
                for address in addresses[:2]:
                    await asyncio.sleep(1)
                    await quote_probe(access, secrets, context, host=address)


asyncio.run(main())
