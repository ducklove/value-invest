from _harness import seed_user

from repositories import db, portfolio, snapshots


async def test_sync_timestamp_does_not_invalidate_nav_but_real_inputs_do(temp_db):
    await seed_user()
    await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 60000)
    before = await snapshots.get_nav_input_state("u1")
    async with db.transaction() as conn:
        await conn.execute("UPDATE user_portfolio SET updated_at='2026-09-24T06:00', memo='note' WHERE google_sub='u1'")
        await conn.execute("UPDATE account_holdings SET updated_at='2026-09-24T06:00' WHERE google_sub='u1'")
    assert await snapshots.get_nav_input_state("u1") == before
    async with db.transaction() as conn:
        await conn.execute("UPDATE account_holdings SET avg_price=61000 WHERE google_sub='u1'")
    assert await snapshots.get_nav_input_state("u1") != before
    before = await snapshots.get_nav_input_state("u1")
    async with db.transaction() as conn:
        await conn.execute("UPDATE user_portfolio SET quantity=11 WHERE google_sub='u1'")
    assert await snapshots.get_nav_input_state("u1") != before
