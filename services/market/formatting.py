"""Shared presentation helpers for market indicator data."""

from __future__ import annotations

from typing import Any, Mapping


def format_indicator_change(item: Mapping[str, Any]) -> str:
    """Return a signed market move using the canonical up/down markers."""
    raw = str(item.get("change_pct") or item.get("change") or "").strip()
    if not raw:
        return ""
    raw = raw.removeprefix("up").removeprefix("down").strip()
    direction = str(item.get("direction") or "").strip().lower()
    if direction == "up":
        return f"▲{raw.lstrip('+-')}"
    if direction == "down":
        return f"▼{raw.lstrip('+-')}"
    return raw
