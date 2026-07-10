"""Persistence for household assets and the retirement planning profile."""

from __future__ import annotations

from datetime import datetime

from repositories import db as db_repo

# Repository layer deliberately stays below services (dependency policy).  The
# values mirror the schema defaults; the service owns source labels/metadata.
_DEFAULT_RETIREMENT_PROFILE = {
    "household_type": "couple",
    "current_age": None,
    "retirement_age": 65,
    "plan_to_age": 90,
    "monthly_spending": 2_981_000,
    "monthly_public_pension": 0,
    "monthly_other_income": 0,
    "monthly_contribution": 0,
    "annual_return_pct": 4.0,
    "inflation_pct": 2.0,
}


async def get_household_plan(google_sub: str) -> dict:
    db = await db_repo.get_db()
    cursor = await db.execute(
        """SELECT asset_id, category, name, owner, amount, retirement_eligible,
                  sort_order, created_at, updated_at
           FROM household_assets
           WHERE google_sub = ?
           ORDER BY sort_order, created_at, asset_id""",
        (google_sub,),
    )
    items = []
    for row in await cursor.fetchall():
        item = dict(row)
        item["amount"] = int(item.get("amount") or 0)
        item["retirement_eligible"] = bool(item.get("retirement_eligible"))
        items.append(item)

    cursor = await db.execute(
        """SELECT household_type, current_age, retirement_age, plan_to_age,
                  monthly_spending, monthly_public_pension, monthly_other_income,
                  monthly_contribution, annual_return_pct, inflation_pct, updated_at
           FROM household_retirement_profiles WHERE google_sub = ?""",
        (google_sub,),
    )
    row = await cursor.fetchone()
    retirement = dict(_DEFAULT_RETIREMENT_PROFILE)
    updated_at = None
    if row:
        saved = dict(row)
        updated_at = saved.pop("updated_at", None)
        retirement.update(saved)
    return {"items": items, "retirement": retirement, "updated_at": updated_at}


async def replace_household_plan(google_sub: str, items: list[dict], retirement: dict) -> dict:
    now = datetime.now().isoformat()
    async with db_repo.transaction() as db:
        await db.execute("DELETE FROM household_assets WHERE google_sub = ?", (google_sub,))
        if items:
            await db.executemany(
                """INSERT INTO household_assets
                   (asset_id, google_sub, category, name, owner, amount,
                    retirement_eligible, sort_order, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        item["asset_id"], google_sub, item["category"], item["name"],
                        item["owner"], item["amount"], int(item["retirement_eligible"]),
                        index, now, now,
                    )
                    for index, item in enumerate(items)
                ],
            )
        await db.execute(
            """INSERT INTO household_retirement_profiles
               (google_sub, household_type, current_age, retirement_age, plan_to_age,
                monthly_spending, monthly_public_pension, monthly_other_income,
                monthly_contribution, annual_return_pct, inflation_pct, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(google_sub) DO UPDATE SET
                 household_type = excluded.household_type,
                 current_age = excluded.current_age,
                 retirement_age = excluded.retirement_age,
                 plan_to_age = excluded.plan_to_age,
                 monthly_spending = excluded.monthly_spending,
                 monthly_public_pension = excluded.monthly_public_pension,
                 monthly_other_income = excluded.monthly_other_income,
                 monthly_contribution = excluded.monthly_contribution,
                 annual_return_pct = excluded.annual_return_pct,
                 inflation_pct = excluded.inflation_pct,
                 updated_at = excluded.updated_at""",
            (
                google_sub,
                retirement["household_type"], retirement["current_age"],
                retirement["retirement_age"], retirement["plan_to_age"],
                retirement["monthly_spending"], retirement["monthly_public_pension"],
                retirement["monthly_other_income"], retirement["monthly_contribution"],
                retirement["annual_return_pct"], retirement["inflation_pct"], now,
            ),
        )
    return await get_household_plan(google_sub)
