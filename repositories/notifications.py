"""Notification channels, bot-link handshake codes, and portfolio alert rules.

Per-user notification config lives here so routes/services stay thin and the
data path matches the other repositories. Re-exported as ``cache.<fn>``.

Three tables (created in ``cache.init_db``):
* ``notification_channels`` — delivery targets (telegram now, kakao later).
* ``notification_links``     — one-shot ``/start <code>`` handshake codes.
* ``portfolio_alerts``       — alert rules + edge-trigger (``armed``) state.
"""

from __future__ import annotations

import json
from datetime import datetime

import cache


def _now() -> str:
    return datetime.now().isoformat()


# --- Channels ---------------------------------------------------------------

async def get_notification_channel(google_sub: str, channel: str) -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT google_sub, channel, enabled, config_json, verified, created_at, updated_at"
        " FROM notification_channels WHERE google_sub = ? AND channel = ?",
        (google_sub, channel),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    item = dict(row)
    try:
        item["config"] = json.loads(item.get("config_json") or "{}")
    except (TypeError, ValueError):
        item["config"] = {}
    return item


async def list_notification_channels(google_sub: str) -> list[dict]:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT google_sub, channel, enabled, config_json, verified, created_at, updated_at"
        " FROM notification_channels WHERE google_sub = ? ORDER BY channel ASC",
        (google_sub,),
    )
    items: list[dict] = []
    for row in await cursor.fetchall():
        item = dict(row)
        try:
            item["config"] = json.loads(item.get("config_json") or "{}")
        except (TypeError, ValueError):
            item["config"] = {}
        items.append(item)
    return items


async def upsert_notification_channel(
    google_sub: str,
    channel: str,
    *,
    config: dict | None = None,
    enabled: bool = True,
    verified: bool = True,
) -> None:
    db = await cache.get_db()
    now = _now()
    await db.execute(
        """
        INSERT INTO notification_channels
            (google_sub, channel, enabled, config_json, verified, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(google_sub, channel) DO UPDATE SET
            enabled = excluded.enabled,
            config_json = excluded.config_json,
            verified = excluded.verified,
            updated_at = excluded.updated_at
        """,
        (
            google_sub,
            channel,
            1 if enabled else 0,
            json.dumps(config or {}, ensure_ascii=False),
            1 if verified else 0,
            now,
            now,
        ),
    )
    await db.commit()


async def set_notification_channel_enabled(google_sub: str, channel: str, enabled: bool) -> bool:
    db = await cache.get_db()
    cursor = await db.execute(
        "UPDATE notification_channels SET enabled = ?, updated_at = ?"
        " WHERE google_sub = ? AND channel = ?",
        (1 if enabled else 0, _now(), google_sub, channel),
    )
    await db.commit()
    return cursor.rowcount > 0


async def delete_notification_channel(google_sub: str, channel: str) -> bool:
    db = await cache.get_db()
    cursor = await db.execute(
        "DELETE FROM notification_channels WHERE google_sub = ? AND channel = ?",
        (google_sub, channel),
    )
    await db.commit()
    return cursor.rowcount > 0


# --- Bot-link handshake codes ----------------------------------------------

async def create_notification_link(code: str, google_sub: str, channel: str, expires_at: str) -> None:
    db = await cache.get_db()
    # One pending link per user+channel — replace any earlier code.
    await db.execute(
        "DELETE FROM notification_links WHERE google_sub = ? AND channel = ?",
        (google_sub, channel),
    )
    await db.execute(
        "INSERT INTO notification_links (code, google_sub, channel, created_at, expires_at)"
        " VALUES (?, ?, ?, ?, ?)",
        (code, google_sub, channel, _now(), expires_at),
    )
    await db.commit()


async def pop_notification_link(code: str) -> dict | None:
    """Resolve and consume a link code. Returns the row if still valid, else None."""
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT code, google_sub, channel, created_at, expires_at FROM notification_links WHERE code = ?",
        (code,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    item = dict(row)
    await db.execute("DELETE FROM notification_links WHERE code = ?", (code,))
    await db.commit()
    if item.get("expires_at") and item["expires_at"] < _now():
        return None
    return item


async def delete_expired_notification_links() -> int:
    db = await cache.get_db()
    cursor = await db.execute(
        "DELETE FROM notification_links WHERE expires_at < ?", (_now(),)
    )
    await db.commit()
    return cursor.rowcount


# --- Alert rules ------------------------------------------------------------

_ALERT_COLUMNS = (
    "id, google_sub, scope, stock_code, alert_type, threshold, enabled, note,"
    " armed, last_triggered_at, last_value, created_at, updated_at"
)


async def list_portfolio_alerts(google_sub: str, *, enabled_only: bool = False) -> list[dict]:
    db = await cache.get_db()
    query = f"SELECT {_ALERT_COLUMNS} FROM portfolio_alerts WHERE google_sub = ?"
    params: tuple = (google_sub,)
    if enabled_only:
        query += " AND enabled = 1"
    query += " ORDER BY id ASC"
    cursor = await db.execute(query, params)
    return [dict(row) for row in await cursor.fetchall()]


async def get_portfolio_alert(google_sub: str, alert_id: int) -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        f"SELECT {_ALERT_COLUMNS} FROM portfolio_alerts WHERE google_sub = ? AND id = ?",
        (google_sub, alert_id),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def create_portfolio_alert(
    google_sub: str,
    *,
    scope: str,
    alert_type: str,
    threshold: float,
    stock_code: str | None = None,
    note: str = "",
    enabled: bool = True,
) -> int:
    db = await cache.get_db()
    now = _now()
    cursor = await db.execute(
        """
        INSERT INTO portfolio_alerts
            (google_sub, scope, stock_code, alert_type, threshold, enabled, note,
             armed, last_triggered_at, last_value, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, NULL, NULL, ?, ?)
        """,
        (google_sub, scope, stock_code, alert_type, threshold, 1 if enabled else 0, note, now, now),
    )
    await db.commit()
    return int(cursor.lastrowid)


async def update_portfolio_alert(google_sub: str, alert_id: int, **fields) -> bool:
    """Update editable fields. Any change re-arms the rule (armed=1)."""
    allowed = {"scope", "stock_code", "alert_type", "threshold", "enabled", "note"}
    sets: list[str] = []
    params: list = []
    for key, value in fields.items():
        if key not in allowed or value is None:
            continue
        if key == "enabled":
            value = 1 if value else 0
        sets.append(f"{key} = ?")
        params.append(value)
    if not sets:
        return False
    # Editing thresholds/type should reset the edge-trigger so a freshly
    # changed rule can fire again.
    sets.append("armed = 1")
    sets.append("updated_at = ?")
    params.append(_now())
    params.extend([google_sub, alert_id])
    db = await cache.get_db()
    cursor = await db.execute(
        f"UPDATE portfolio_alerts SET {', '.join(sets)} WHERE google_sub = ? AND id = ?",
        params,
    )
    await db.commit()
    return cursor.rowcount > 0


async def delete_portfolio_alert(google_sub: str, alert_id: int) -> bool:
    db = await cache.get_db()
    cursor = await db.execute(
        "DELETE FROM portfolio_alerts WHERE google_sub = ? AND id = ?",
        (google_sub, alert_id),
    )
    await db.commit()
    return cursor.rowcount > 0


async def set_portfolio_alert_state(
    alert_id: int,
    *,
    armed: bool,
    last_value: float | None = None,
    triggered: bool = False,
) -> None:
    """Persist edge-trigger state after an evaluation pass."""
    db = await cache.get_db()
    if triggered:
        await db.execute(
            "UPDATE portfolio_alerts SET armed = ?, last_value = ?, last_triggered_at = ?, updated_at = ? WHERE id = ?",
            (1 if armed else 0, last_value, _now(), _now(), alert_id),
        )
    else:
        await db.execute(
            "UPDATE portfolio_alerts SET armed = ?, last_value = ?, updated_at = ? WHERE id = ?",
            (1 if armed else 0, last_value, _now(), alert_id),
        )
    await db.commit()
