"""Restore the missing 2026-09-23 settlement from reviewed, dated price evidence.

Default is read-only validation. --apply makes an online SQLite backup and commits
NAV, stock/group snapshots and ledger settlement together. The candidate is an
operator-produced JSON artifact kept outside Git with its full source evidence.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DAY = "2026-09-23"


def holdings_signature(items):
    fields = ("stock_code", "quantity", "avg_price", "avg_price_currency", "currency", "group_name", "pair_long_code")
    position_fields = ("account_id", "quantity", "avg_price", "avg_price_currency", "currency")
    return sorted((
        tuple(item.get(k) for k in fields),
        sorted(tuple(p.get(k) for k in position_fields) for p in item.get("account_positions", [])),
    ) for item in items)


def valued_rows(candidate):
    items, quotes, rates = candidate["items"], candidate["quotes"], candidate["fx"]
    if candidate["date"] != DAY or candidate["cutoff"] != DAY + "T20:00:00":
        raise ValueError("Unexpected settlement boundary")
    if set(quotes) != {i["stock_code"] for i in items}:
        raise ValueError("Incomplete price coverage")
    rows = []
    for item in items:
        code = item["stock_code"]
        quote = quotes[code]
        price = float(quote["price"])
        if not quote.get("source") or not math.isfinite(price) or price <= 0:
            raise ValueError(f"Invalid price evidence: {code}")
        positions = item.get("account_positions") or [item]
        cost = sum(float(p["quantity"]) * float(p["avg_price"]) *
                   float(rates[p.get("avg_price_currency") or "KRW"]["rate"]) for p in positions)
        qty = float(item["quantity"])
        currency = item.get("currency") or "KRW"
        # The old .KS alias has a USD label despite a KRW listing. The price
        # evidence, rather than that display label, defines its conversion.
        if quote["source"] == "dated domestic close":
            currency = "KRW"
        rows.append({
            "stock_code": code, "market_value": qty * price, "quantity": qty,
            "unit_price": price, "avg_price_krw": cost / qty if qty else 0,
            "cost_basis": cost, "group_name": item.get("group_name"),
            "priced_from_fallback": False, "currency": currency,
            "fx_rate": float(rates[currency]["rate"]),
        })
    if not all(math.isfinite(r["market_value"]) and math.isfinite(r["cost_basis"]) for r in rows):
        raise ValueError("Non-finite valuation")
    return rows


async def run(candidate_path: Path, *, apply: bool):
    from core.config import load_environment
    load_environment(ROOT, force=True)
    import snapshot_nav
    from repositories import db, portfolio, snapshots
    from services.portfolio import snapshot_views

    raw = candidate_path.read_bytes()
    candidate = json.loads(raw)
    rows = valued_rows(candidate)
    user = candidate["user"]
    total = sum(r["market_value"] for r in rows)
    invested = sum(r["cost_basis"] for r in rows)
    try:
        current = await portfolio.get_portfolio(user)
        if holdings_signature(current) != holdings_signature(candidate["items"]):
            raise ValueError("Holdings changed since evidence capture")
        expected = await snapshots.get_nav_input_state(user)
        if await snapshots.get_snapshot_on_or_before(user, "9999-12-31") is None:
            raise ValueError("Missing preceding NAV")
        latest = await snapshots.get_latest_snapshot(user)
        if latest["date"] >= DAY:
            raise ValueError("Target or subsequent settlement already exists; refusing overwrite")
        conn = await db.get_db()
        for table in ("portfolio_cashflows", "portfolio_distributions", "portfolio_dividend_receipts", "portfolio_trades"):
            count = await (await conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE google_sub=? AND created_at>?", (user, candidate["cutoff"]),
            )).fetchone()
            if count[0]:
                raise ValueError(f"Post-cutoff activity requires explicit reconciliation: {table}")
        report = {"date": DAY, "stock_count": len(rows), "total_value": total,
                  "total_invested": invested, "candidate_sha256": hashlib.sha256(raw).hexdigest(),
                  "previous_date": latest["date"], "previous_nav": latest["nav"], "applied": False}
        if apply:
            backup_path = candidate_path.parent / f"before-recovery-{datetime.now():%Y%m%d-%H%M%S}.db"
            with sqlite3.connect(db.DB_PATH) as src, sqlite3.connect(backup_path) as dst:
                src.backup(dst)
            report["backup"] = str(backup_path)
            snapshot_nav._fx_usdkrw = float(candidate["fx"]["USD"]["rate"])
            async with db.transaction():
                if await snapshots.get_nav_input_state(user) != expected:
                    raise ValueError("NAV inputs changed; refusing stale recovery")
                await snapshot_nav._persist_snapshot(user, DAY, total, invested, rows, candidate["cutoff"])
                saved = await snapshots.get_snapshot_by_date(user, DAY)
                stocks = await snapshots.get_stock_snapshots_exact_date(user, DAY)
                if len(stocks) != len(rows) or not math.isclose(sum(r["market_value"] for r in stocks), saved["total_value"], abs_tol=0.01):
                    raise ValueError("Recovery consistency check failed")
                report.update(applied=True, nav=saved["nav"], total_units=saved["total_units"])
            view = await snapshot_views.previous_day(user, DAY)
            if view["date"] != DAY:
                raise ValueError("Today baseline verification failed")
            report["today_baseline"] = view["date"]
            import observability
            await observability.record_event("snapshot_nav", "recovered", level="info", details=report, wait=True)
        (candidate_path.parent / ("applied.json" if apply else "validation.json")).write_text(json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))
    finally:
        await db.close_db()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.candidate, apply=args.apply))
