from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RuntimeState:
    """Mutable process state that operational endpoints may inspect."""

    last_loop_tick: float = 0.0


def get_asset_version(project_root: Path) -> str:
    """Return a stable asset version for cache busting."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=project_root,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return str(int(time.time()))


def sd_notify(msg: str) -> None:
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    try:
        if addr[0] == "@":
            addr = "\0" + addr[1:]
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(addr)
            sock.sendall(msg.encode("utf-8"))
    except Exception:
        pass


async def watchdog_loop(state: RuntimeState, interval_seconds: float = 10.0) -> None:
    while True:
        state.last_loop_tick = time.monotonic()
        sd_notify("WATCHDOG=1")
        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break

