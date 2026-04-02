"""KIS API key pool — issues and reclaims credential slots for WebSocket connections.

Each KIS API key pair can support one WebSocket connection with up to 40
real-time subscriptions. This manager loads all available key pairs from
environment variables and provides acquire/release semantics so multiple
browser sessions can stream quotes simultaneously.

Public API
----------
- load_keys()       — read KIS_APP_KEY / KIS_APP_KEY2 / ... from env
- acquire()         — get an available KeySlot (or None)
- release(slot)     — return a KeySlot to the pool
- total_count()     — total key pairs loaded
- available_count() — key pairs currently available
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import httpx

logger = logging.getLogger(__name__)

APPROVAL_KEY_TTL_S = 23 * 3600  # 23 hours — KIS keys valid ~24h


class KeySlot:
    """One KIS API credential pair with its own approval-key lifecycle."""

    def __init__(self, slot_id: int, app_key: str, app_secret: str, base_url: str):
        self.slot_id = slot_id
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url
        self._approval_key: str = ""
        self._approval_key_ts: float = 0.0

    async def get_approval_key(self) -> str:
        """Return a valid approval key, refreshing from KIS REST API if expired."""
        now = time.monotonic()
        if self._approval_key and (now - self._approval_key_ts) < APPROVAL_KEY_TTL_S:
            return self._approval_key

        url = f"{self.base_url}/oauth2/Approval"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()

        key = data.get("approval_key", "")
        if not key:
            raise RuntimeError(f"KIS approval key empty (slot {self.slot_id}): {data}")

        self._approval_key = key
        self._approval_key_ts = now
        logger.info("KIS approval key obtained for slot %d", self.slot_id)
        return self._approval_key

    def __repr__(self) -> str:
        return f"KeySlot(id={self.slot_id}, key=...{self.app_key[-6:]})"


# ---------------------------------------------------------------------------
# Pool state
# ---------------------------------------------------------------------------

_slots: list[KeySlot] = []
_available: list[KeySlot] = []
_in_use: dict[int, KeySlot] = {}
_lock = asyncio.Lock()


def load_keys() -> int:
    """Load all KIS key pairs from environment. Returns count of keys found."""
    global _slots, _available, _in_use
    _slots = []
    _available = []
    _in_use = {}

    base_url = os.environ.get("KIS_BASE_URL", "").rstrip("/")
    if not base_url:
        logger.warning("KIS_BASE_URL not set — no keys loaded")
        return 0

    # Primary key pair
    key = os.environ.get("KIS_APP_KEY", "")
    secret = os.environ.get("KIS_APP_SECRET", "")
    if key and secret:
        _slots.append(KeySlot(0, key, secret, base_url))

    # Additional key pairs: KIS_APP_KEY2/SECRET2, KIS_APP_KEY3/SECRET3, ...
    i = 2
    while True:
        key = os.environ.get(f"KIS_APP_KEY{i}", "")
        secret = os.environ.get(f"KIS_APP_SECRET{i}", "")
        if not key or not secret:
            break
        _slots.append(KeySlot(i - 1, key, secret, base_url))
        i += 1

    _available = list(_slots)
    logger.info("KIS key manager: %d key(s) loaded", len(_slots))
    return len(_slots)


async def acquire() -> KeySlot | None:
    """Acquire an available key slot. Returns None if all in use."""
    async with _lock:
        if not _available:
            return None
        slot = _available.pop(0)
        _in_use[slot.slot_id] = slot
        logger.info(
            "Key slot %d acquired (%d/%d available)",
            slot.slot_id, len(_available), len(_slots),
        )
        return slot


async def release(slot: KeySlot) -> None:
    """Return a key slot to the pool."""
    async with _lock:
        if slot.slot_id in _in_use:
            del _in_use[slot.slot_id]
            _available.append(slot)
            logger.info(
                "Key slot %d released (%d/%d available)",
                slot.slot_id, len(_available), len(_slots),
            )


def total_count() -> int:
    """Total number of key pairs loaded."""
    return len(_slots)


def available_count() -> int:
    """Number of key slots currently available."""
    return len(_available)
