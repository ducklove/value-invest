"""Country/importance calendar rules and their automatically enrolled events."""

from repositories.db import get_db, transaction


async def list_rules(google_sub: str | None = None) -> list[dict]:
    db = await get_db()
    query = "SELECT google_sub, country, min_importance, starts_at FROM economic_calendar_alert_rules"
    params = ()
    if google_sub is not None:
        query += " WHERE google_sub = ?"
        params = (google_sub,)
    cursor = await db.execute(query + " ORDER BY google_sub, country", params)
    return [dict(row) for row in await cursor.fetchall()]


async def replace_rules(google_sub: str, rules: list[dict], starts_at: str) -> None:
    async with transaction() as db:
        previous = {r["country"]: r for r in await list_rules(google_sub)}
        await db.execute("DELETE FROM economic_calendar_alert_rules WHERE google_sub = ?", (google_sub,))
        for rule in rules:
            old = previous.get(rule["country"])
            since = old["starts_at"] if old and old["min_importance"] == rule["min_importance"] else starts_at
            await db.execute(
                "INSERT INTO economic_calendar_alert_rules (google_sub, country, min_importance, starts_at) VALUES (?, ?, ?, ?)",
                (google_sub, rule["country"], rule["min_importance"], since),
            )
        # Keep explicit subscriptions and delivered history. Only automatic
        # pending subscriptions no longer covered by a rule are cancelled.
        await db.execute(
            """DELETE FROM economic_calendar_subscriptions AS s
               WHERE s.google_sub = ? AND s.automatic = 1 AND s.fired = 0
               AND NOT EXISTS (
                 SELECT 1 FROM economic_calendar_alert_rules r
                 WHERE r.google_sub = s.google_sub AND r.country = s.country
                 AND (r.min_importance = 'all' OR s.importance = 'high'
                      OR (r.min_importance = 'mid' AND s.importance = 'mid'))
               )""", (google_sub,),
        )


async def enroll_events(matches: list[tuple[dict, dict]], now: str) -> None:
    async with transaction() as db:
        for rule, event in matches:
            # Recheck the rule snapshot after the network fetch, so a removed
            # or edited rule cannot recreate cancelled subscriptions.
            await db.execute(
                """INSERT INTO economic_calendar_subscriptions
                   (google_sub, event_id, event_date, event_datetime, country, country_name,
                    event, importance, forecast, previous, fired, automatic, created_at, updated_at)
                   SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?
                   WHERE EXISTS (SELECT 1 FROM economic_calendar_alert_rules
                     WHERE google_sub = ? AND country = ? AND min_importance = ? AND starts_at = ?)
                   ON CONFLICT(google_sub, event_id) DO NOTHING""",
                (rule["google_sub"], str(event["index_id"]), event["date"], event.get("datetime", ""),
                 event.get("country", ""), event.get("country_name", ""), event.get("event", ""),
                 event.get("importance", ""), event.get("forecast", ""), event.get("previous", ""), now, now,
                 rule["google_sub"], rule["country"], rule["min_importance"], rule["starts_at"]),
            )
