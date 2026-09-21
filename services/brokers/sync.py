"""증권사 계좌 스냅샷을 완전히 조회한 뒤 원장·합산 잔고를 원자적으로 교체한다."""

import asyncio
import json
from datetime import date, datetime, timedelta, timezone

from domain.broker_assets import is_futures_value
from repositories import account_holdings as holdings
from repositories import broker_activity, brokers
from repositories.broker_secrets import BrokerError
from repositories.db import transaction
from services.brokers import activity
from services.brokers.symbols import domestic_code as domestic_code
from services.brokers.symbols import foreign_code as foreign_code
from services.brokers.symbols import records as records
from services.brokers.symbols import summary as summary

_sync_locks: dict[str, asyncio.Lock] = {}


async def fetch_snapshot(user: str, link: dict) -> tuple[list[dict], dict]:
    from services.brokers.registry import get_adapter
    return await get_adapter(link.get("provider", "namuh")).fetch_snapshot(user, link)


async def sync_account(user: str, aid: str, *, include_activity: bool = False, start: date | None = None, end: date | None = None) -> dict:
    async with _sync_locks.setdefault(aid, asyncio.Lock()):
        link = await brokers.get_link(user, aid)
        from services.brokers.registry import get_adapter
        adapter = get_adapter(link.get("provider", "namuh"))
        import_activity = include_activity and adapter.definition.activity and link["environment"] == "live"
        if not adapter.definition.activity and (start is not None or end is not None):
            raise BrokerError(f"{adapter.definition.name} 수입·입출금 거래내역 가져오기는 아직 지원하지 않습니다. 잔고는 자동 갱신됩니다.")
        try:
            entries = None
            if import_activity:
                previous_state = await broker_activity.state(user, aid)
                until = end or datetime.now(activity.KST).date()
                since = start or (date.fromisoformat(previous_state["last_import_at"][:10]) - timedelta(days=7)
                                  if previous_state and previous_state["last_import_at"] else until - timedelta(days=90))
                entries = await adapter.fetch_activity(user, link, since, until)
            rows, balances = await fetch_snapshot(user, link)
            async with transaction() as db:
                current = await brokers.get_link(user, aid)
                if any(current.get(key) != link.get(key) for key in ("credential_id", "account_fingerprint", "product", "provider", "environment")):
                    raise BrokerError("동기화 중 계좌 연결이 변경되었습니다. 다시 시도해 주세요.")
                await holdings.initialize(db, user)
                if entries is not None:
                    await broker_activity.store(user, link, entries)
                previous = await holdings.list_positions(user, aid)
                other = [r for r in await holdings.list_positions(user) if r["account_id"] != aid]
                for row in rows:
                    conflict = next((r for r in other if r["stock_code"] == row["stock_code"] and r["currency"] != row["currency"]), None)
                    if conflict:
                        raise BrokerError(f"{row['stock_code']}의 거래 통화가 다릅니다 "
                                          f"(기존 계좌 {conflict['currency']}, 증권사 {row['currency']}). 종목과 통화를 확인해 주세요.")
                    if not row["stock_code"].startswith("CASH_") and not is_futures_value(row["stock_code"]) and any(r["stock_code"] == row["stock_code"] and r["quantity"] * row["quantity"] < 0 for r in other):
                        raise BrokerError("다른 계좌의 공매도 잔고와 충돌하여 동기화를 보류했습니다.")
                now = datetime.now(timezone.utc).isoformat()
                created = {r["stock_code"]: r["created_at"] for r in previous}
                await db.execute("DELETE FROM account_holdings WHERE google_sub=? AND account_id=?", (user, aid))
                for row in rows:
                    await db.execute("INSERT INTO account_holdings VALUES (?,?,?,?,?,?,?,?,?,?)", (user, aid, row["stock_code"], row["stock_name"],
                        row["quantity"], row["avg_price"], row["avg_price_currency"], row["currency"], created.get(row["stock_code"], now), now))
                for code in {r["stock_code"] for r in previous + rows}:
                    await holdings.rebuild(user, code)
                snapshot = {**balances.get("_snapshot", {}), "synced_at": now} if "_snapshot" in balances else {}
                await db.execute("UPDATE portfolio_accounts SET broker_snapshot_json=? WHERE google_sub=? AND account_id=?",
                                 (json.dumps(snapshot, ensure_ascii=False), user, aid))
                await db.execute("UPDATE broker_account_links SET last_sync_at=?,sync_error=NULL,balances_json=? WHERE google_sub=? AND account_id=?",
                                 (now, json.dumps(balances), user, aid))
            return {"ok": True, "holdings_count": len(rows), "synced_at": now, "balances": balances}
        except BrokerError as exc:
            if import_activity:
                await broker_activity.set_error(user, aid, str(exc))
            async with transaction() as db:
                await db.execute("UPDATE broker_account_links SET sync_error=? WHERE google_sub=? AND account_id=?", (str(exc), user, aid))
            raise
