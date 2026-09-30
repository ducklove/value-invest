from __future__ import annotations

import asyncio
import hashlib
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
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=project_root,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain", "--", "static"],
            cwd=project_root,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        if status:
            return f"{commit}-{_static_assets_mtime(project_root)}"
        return commit
    except (OSError, subprocess.SubprocessError, UnicodeError):
        return str(int(time.time()))


def _static_assets_mtime(project_root: Path) -> str:
    static_dir = project_root / "static"
    latest = 0
    for path in static_dir.rglob("*"):
        try:
            if path.is_file():
                latest = max(latest, path.stat().st_mtime_ns)
        except OSError:
            continue
    return format(latest or time.time_ns(), "x")


# 파일별 콘텐츠 해시 캐시버스팅(F4) — 이 디렉터리(static/ 기준) 아래 파일만
# 개별 해시를 받는다. 나머지는 get_asset_version() 의 저장소 단위 버전으로 폴백.
HASHED_ASSET_DIRS = ("js", "css", "ecosystem")
ASSET_HASH_LENGTH = 10


class AssetManifest:
    """Per-file ``sha1[:10]`` content hashes for ``?v=`` asset stamps.

    Computed once at startup: a deploy that touches only backend code (or one
    asset) no longer changes every asset URL, so browsers and the service worker
    keep what they already cached. Keys are POSIX paths relative to ``static/``
    (``js/utils.js``, ``css/base.css``, ``ecosystem/vc-shell.js``).

    ``refresh()`` re-stats the tree and re-hashes only files whose
    (mtime, size) changed; production calls it once, development before every
    HTML render so edits show up without a restart.
    """

    def __init__(self, static_dir: Path, fallback: str, *, live: bool = False) -> None:
        self.static_dir = Path(static_dir)
        self.fallback = fallback
        self.live = live
        self.generation = 0
        self._entries: dict[str, tuple[int, int, str]] = {}
        self.refresh()

    def refresh(self) -> bool:
        """Rescan the hashed directories; return True when any hash changed."""
        seen: dict[str, tuple[int, int, str]] = {}
        for folder in HASHED_ASSET_DIRS:
            base = self.static_dir / folder
            if not base.is_dir():
                continue
            for path in sorted(base.rglob("*")):
                try:
                    if not path.is_file():
                        continue
                    stat = path.stat()
                    key = path.relative_to(self.static_dir).as_posix()
                    previous = self._entries.get(key)
                    if previous and previous[0] == stat.st_mtime_ns and previous[1] == stat.st_size:
                        seen[key] = previous
                        continue
                    digest = hashlib.sha1(path.read_bytes()).hexdigest()[:ASSET_HASH_LENGTH]
                    seen[key] = (stat.st_mtime_ns, stat.st_size, digest)
                except OSError:
                    continue
        changed = {k: v[2] for k, v in seen.items()} != {k: v[2] for k, v in self._entries.items()}
        self._entries = seen
        if changed:
            self.generation += 1
        return changed

    def versions(self) -> dict[str, str]:
        return {key: entry[2] for key, entry in self._entries.items()}

    def version_for(self, rel_path: str) -> str:
        entry = self._entries.get(rel_path)
        return entry[2] if entry else self.fallback

    def is_current(self, rel_path: str, version: str) -> bool:
        """True when ``version`` is exactly what the server stamps for ``rel_path``.

        Only such URLs are immutable. Old stamps (a page loaded before a deploy)
        and hand-maintained ones (sibling tools hotlink
        ``/js/portfolio-held-badges.js?v=<registry version>``) must keep
        revalidating, otherwise a browser would pin stale bytes for a year.
        """
        if not version:
            return False
        entry = self._entries.get(rel_path)
        if entry is not None:
            return version == entry[2]
        return version == self.fallback


def sd_notify(msg: str) -> None:
    addr = os.environ.get("NOTIFY_SOCKET")
    family = getattr(socket, "AF_UNIX", None)
    if not addr or family is None:
        return
    try:
        if addr[0] == "@":
            addr = "\0" + addr[1:]
        with socket.socket(family, socket.SOCK_DGRAM) as sock:
            sock.connect(addr)
            sock.sendall(msg.encode("utf-8"))
    except OSError:
        pass


async def watchdog_loop(state: RuntimeState, interval_seconds: float = 10.0) -> None:
    while True:
        state.last_loop_tick = time.monotonic()
        sd_notify("WATCHDOG=1")
        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break
