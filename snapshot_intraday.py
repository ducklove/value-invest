"""Intraday portfolio snapshot. Run via systemd timer every 10 minutes (08:00-20:00 KST)."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from repositories import bootstrap
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from services.portfolio import runtime_quotes as portfolio_quotes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


class IntradaySnapshotIncomplete(RuntimeError):
    """Raised when a user portfolio cannot be valued without distorting NAV."""


def _today_kst() -> str:
    return datetime.now(KST).date().isoformat()


def _quote_price(quote: dict | None) -> float | None:
    if not quote or quote.get("_stale") is True:
        return None
    value = quote.get("price")
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def _fetch_total_value(
    google_sub: str,
    snap_date: str | None = None,
    *,
    quote_map: dict[str, dict] | None = None,
    items: list[dict] | None = None,
    force_kr: bool = True,
) -> float:
    """사용자 포트폴리오의 장중 총평가액.

    ``quote_map`` 이 주어지면(패스 공유 시세, ``runtime_quotes.fetch_quote_map``)
    그 값을 쓰고, 맵에 없는 코드만 종전처럼 개별 조회한다. 맵의 ``{}`` 는 조회
    실패와 같게 취급한다 — 결측 처리(해외만 직전 스냅샷 폴백)는 동일하다.
    """
    snap_date = snap_date or _today_kst()
    if items is None:
        items = await portfolio_repo.get_portfolio(google_sub)
    prev_stock_values = {
        row["stock_code"]: float(row["market_value"])
        for row in await snapshots_repo.get_stock_snapshots_by_date(google_sub, snap_date)
        if row.get("stock_code") and row.get("market_value") is not None
    }
    total = 0.0
    missing: list[str] = []
    fallback_count = 0
    for item in items:
        code = item["stock_code"]
        qty = float(item["quantity"])
        is_korean = portfolio_quotes.is_korean_stock(code)
        fetched_now = False
        if quote_map is not None and code in quote_map:
            price = _quote_price(quote_map[code])
        else:
            fetched_now = True
            try:
                if is_korean and force_kr:
                    quote = await portfolio_quotes.fetch_quote(code, force_refresh=True, use_ws_cache=False)
                else:
                    quote = await portfolio_quotes.fetch_quote(code)
                price = _quote_price(quote)
            except Exception as exc:
                logger.warning("Intraday quote fetch failed for %s: %s", code, exc)
                price = None

        if price is not None:
            total += qty * price
        elif code in prev_stock_values and not is_korean:
            total += prev_stock_values[code]
            fallback_count += 1
            logger.warning("Intraday quote unavailable for %s, using latest stock snapshot", code)
        else:
            missing.append(code)
        if fetched_now:
            await asyncio.sleep(0.15)
    if missing:
        raise IntradaySnapshotIncomplete(
            "missing intraday quotes without stock snapshot fallback: " + ", ".join(missing[:8])
        )
    if fallback_count:
        logger.warning("Intraday snapshot used stock-snapshot fallback for %d holdings", fallback_count)
    return total


async def _load_portfolios(users: list[str]) -> dict[str, list[dict]]:
    """사용자별 보유종목. 읽기 실패한 사용자는 빠진다(개별 평가 단계에서 다시 시도·기록)."""
    loaded = await asyncio.gather(
        *(portfolio_repo.get_portfolio(google_sub) for google_sub in users),
        return_exceptions=True,
    )
    return {
        google_sub: items
        for google_sub, items in zip(users, loaded)
        if isinstance(items, list)
    }


async def build_shared_quote_map(
    portfolios: dict[str, list[dict]], *, force_kr: bool
) -> dict[str, dict]:
    """모든 사용자 보유종목의 합집합을 한 번만 조회한다(국내는 벌크 1회)."""
    codes = [item["stock_code"] for items in portfolios.values() for item in items if item.get("stock_code")]
    return await portfolio_quotes.fetch_quote_map(codes, force_kr=force_kr)


async def run(manage_db: bool = True):
    if manage_db:
        await bootstrap.init_db()
    await snapshots_repo.delete_old_intraday(days_to_keep=7)
    ts = datetime.now(KST).strftime("%Y-%m-%dT%H:%M")
    users = await snapshots_repo.get_all_users_with_portfolio()
    logger.info("Intraday snapshot for %d users at %s", len(users), ts)
    # 휴장일에는 REST 강제 조회 대신 캐시된 마지막 시세를 우선한다.
    force_kr = portfolio_quotes.kr_trading_day(datetime.now(KST).date())
    portfolios = await _load_portfolios(users)
    # 공유 시세 맵을 못 만들면(프로바이더 오류 등) 종전의 사용자별 개별 조회로 간다.
    (quote_map,) = await asyncio.gather(
        build_shared_quote_map(portfolios, force_kr=force_kr), return_exceptions=True
    )
    if isinstance(quote_map, BaseException):
        logger.warning("Intraday shared quote map failed; valuing per user: %s", quote_map)
        quote_map = None
    ok = 0
    failed: list[str] = []
    for google_sub in users:
        try:
            total_value = await _fetch_total_value(
                google_sub,
                quote_map=quote_map,
                items=portfolios.get(google_sub),
                force_kr=force_kr,
            )
            if total_value > 0:
                await snapshots_repo.save_intraday_snapshot(google_sub, ts, total_value)
                logger.info("  %s: %.0f", google_sub[:8], total_value)
                ok += 1
        except Exception as e:
            logger.error("  %s failed: %s", google_sub[:8], e)
            failed.append(google_sub[:8])
    # Dashboard signal — "last intraday tick" with user counts. wait=True
    # because this script closes its own DB handle right after.
    try:
        import observability
        await observability.record_event(
            "snapshot_intraday",
            "tick_ok" if not failed else "tick_partial",
            level="info" if not failed else "warning",
            details={"ts": ts, "users_total": len(users), "users_ok": ok, "users_failed": failed},
            wait=True,
        )
    except Exception:
        pass
    if manage_db:
        await bootstrap.close_db()


if __name__ == "__main__":
    asyncio.run(run())
