import json
import sqlite3

import pytest
from _harness import seed_user

from repositories import accounts, portfolio, settlement_inputs
from repositories.db import get_db
from scripts.repair_portfolio_code import repair_code


@pytest.mark.asyncio
async def test_code_repair_preserves_cost_metadata_history_and_settlement_keys(temp_db):
    await seed_user()
    await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 65000)
    aid = await accounts.get_default_account_id("u1")
    db = await get_db()
    await db.execute(
        "INSERT INTO user_portfolio(google_sub,stock_code,stock_name,quantity,avg_price,currency,"
        "group_name,created_at,updated_at,account_id,memo,target_price,sort_order) "
        "VALUES ('u1','0074K0.KS','수출핵심 30',4000,20330,'KRW','국내/일반','2026-01-01',"
        "'2026-01-01',?,'기존 메모',25000,7)", (aid,),
    )
    await db.execute(
        "INSERT INTO account_holdings VALUES ('u1',?,'0074K0.KS','수출핵심 30',4000,20330,'KRW','KRW',"
        "'2026-01-01','2026-01-01')", (aid,),
    )
    await db.execute("INSERT INTO portfolio_tags(google_sub,stock_code,tag,created_at) VALUES ('u1','0074K0.KS','수출','2026-01-01')")
    await db.execute(
        "INSERT INTO portfolio_stock_snapshots(google_sub,date,stock_code,market_value,quantity,unit_price,cost_basis) "
        "VALUES ('u1','2026-10-02','0074K0.KS',71520000,4000,17880,81320000)"
    )
    await db.execute("UPDATE user_portfolio SET pair_long_code='0074K0.KS' WHERE stock_code='005930'")
    await db.commit()
    with sqlite3.connect(temp_db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        before = dict(conn.execute("SELECT * FROM user_portfolio WHERE stock_code='0074K0.KS'").fetchone())
        position = dict(conn.execute("SELECT * FROM account_holdings WHERE stock_code='0074K0.KS'").fetchone())
        snapshot = dict(conn.execute("SELECT * FROM portfolio_stock_snapshots WHERE stock_code='0074K0.KS'").fetchone())
        changed = repair_code(conn, "u1", "0074K0.KS", "0074K0")
        assert changed["user_portfolio"] == changed["account_holdings"] == changed["portfolio_tags"] == 1
        assert dict(conn.execute("SELECT * FROM user_portfolio WHERE stock_code='0074K0'").fetchone()) == {**before, "stock_code": "0074K0"}
        assert dict(conn.execute("SELECT * FROM account_holdings WHERE stock_code='0074K0'").fetchone()) == {**position, "stock_code": "0074K0"}
        assert dict(conn.execute("SELECT * FROM portfolio_stock_snapshots WHERE stock_code='0074K0'").fetchone()) == {**snapshot, "stock_code": "0074K0"}
        assert conn.execute("SELECT tag FROM portfolio_tags WHERE stock_code='0074K0'").fetchone()[0] == "수출"
        assert conn.execute("SELECT pair_long_code FROM user_portfolio WHERE stock_code='005930'").fetchone()[0] == "0074K0"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert repair_code(conn, "u1", "0074K0.KS", "0074K0") == {}
    # The latest settlement replay must contain one holding under the new key.
    replay = await settlement_inputs.load("u1", "2099-01-01T00:00:00")
    for table in ("user_portfolio", "account_holdings"):
        assert [row["stock_code"] for row in replay[table]].count("0074K0") == 1
        assert all(row["stock_code"] != "0074K0.KS" for row in replay[table])
    # Historical versions are retained for cutoffs before the repair.
    rows = await (await db.execute(
        "SELECT payload FROM settlement_versions WHERE table_name='user_portfolio' AND row_key='0074K0.KS'"
    )).fetchall()
    assert any(row["payload"] and json.loads(row["payload"])["quantity"] == 4000 for row in rows)


@pytest.mark.asyncio
async def test_code_repair_rolls_back_on_historical_key_conflict(temp_db):
    await seed_user()
    await portfolio.save_portfolio_item("u1", "0074K0", "ETF", 10, 20000)
    db = await get_db()
    await db.execute("UPDATE user_portfolio SET stock_code='0074K0.KS' WHERE stock_code='0074K0'")
    await db.execute("UPDATE account_holdings SET stock_code='0074K0.KS' WHERE stock_code='0074K0'")
    await db.executemany(
        "INSERT INTO portfolio_stock_snapshots(google_sub,date,stock_code,market_value) VALUES ('u1','2026-10-02',?,100)",
        [("0074K0",), ("0074K0.KS",)],
    )
    await db.commit()
    with sqlite3.connect(temp_db) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(sqlite3.IntegrityError):
            repair_code(conn, "u1", "0074K0.KS", "0074K0")
        assert conn.execute("SELECT stock_code FROM user_portfolio").fetchall() == [("0074K0.KS",)]
        assert conn.execute("SELECT stock_code FROM account_holdings").fetchall() == [("0074K0.KS",)]
        assert conn.execute("SELECT COUNT(*) FROM portfolio_stock_snapshots").fetchone()[0] == 2


@pytest.mark.asyncio
async def test_code_repair_refuses_to_merge_existing_native_holding(temp_db):
    await seed_user()
    await portfolio.save_portfolio_item("u1", "0074K0", "ETF", 10, 20000)
    db = await get_db()
    await db.execute(
        "INSERT INTO user_portfolio(google_sub,stock_code,stock_name,quantity,avg_price,created_at,updated_at) "
        "VALUES ('u1','0074K0.KS','ETF',10,20000,'2026-01-01','2026-01-01')"
    )
    await db.commit()
    with sqlite3.connect(temp_db) as conn:
        with pytest.raises(ValueError, match="already exists"):
            repair_code(conn, "u1", "0074K0.KS", "0074K0")
        assert conn.execute("SELECT COUNT(*) FROM user_portfolio").fetchone()[0] == 2
