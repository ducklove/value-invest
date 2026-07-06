# ruff: noqa: E402, I001
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from repositories import bootstrap

from repositories import db as db_repo


async def main() -> None:
    await bootstrap.init_db()
    db = await db_repo.get_db()
    await db.execute(
        "DELETE FROM portfolio_intraday WHERE ts >= ? AND ts < ?",
        ("2026-05-27T17:00", "2026-05-27T20:30"),
    )
    await db.commit()
    await bootstrap.close_db()


asyncio.run(main())
