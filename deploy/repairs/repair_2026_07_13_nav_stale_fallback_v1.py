# ruff: noqa: E402, I001
"""2026-07-13 일일정산 재정산 — 정산 시각 시세 장애로 직전 값이 복사된 스냅샷 복구.

2026-07-13 20:05 정산이 시세 소스 장애로 보유 종목을 직전 스냅샷 값으로 채워
(priced_from_fallback), 당일 정산이 사실상 직전 거래일의 복사본이 됐다. 시세가
복구된 지금 그 하루치만 다시 계산한다.

이 스크립트가 하는 일:
* 2026-07-13 하루치 스냅샷만 재정산한다 (그 전후 날짜·당일 gold/benchmark 는 건드리지 않음).
* 과거 날짜이므로 take_snapshot 은 국내 종목을 그 날짜의 실제 종가로 다시 값매김하고,
  덮어쓰면서 priced_from_fallback=0 으로 리셋한다.
* 해당 날짜 스냅샷이 이미 있으므로 좌수(units)는 보존되고 total_value/NAV 만 정정된다
  — 같은 날 현금흐름을 다시 적용하지 않는다.

재실행해도 안전하다(멱등). 시세가 아직도 안 잡히는 종목이 있으면 그 수를 로그로 남긴다.
"""
from __future__ import annotations

import asyncio
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import load_environment

load_environment(ROOT, force=True)

from repositories import bootstrap
from repositories import db as db_repo
from repositories import snapshots as snapshots_repo
import snapshot_nav

TARGET_DATE = "2026-07-13"


def backup_db() -> str:
    db_path = Path(db_repo.DB_PATH).resolve()
    backup_dir = ROOT / "data" / "db-imports"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"cache.before-2026-07-13-nav-refresh.{stamp}.db"
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return str(backup_path)


async def main() -> None:
    await bootstrap.init_db()
    try:
        backup_path = backup_db()
        print(f"DB backup written: {backup_path}")

        await snapshot_nav._fetch_fx_usdkrw()

        users = await snapshots_repo.get_all_users_with_portfolio()
        print(f"Re-settling {TARGET_DATE} for {len(users)} user(s)")

        total_fallback = 0
        for google_sub in users:
            existing = await snapshots_repo.get_snapshot_by_date(google_sub, TARGET_DATE)
            if not existing:
                # 그 날짜에 정산이 없던 사용자는 재정산 대상이 아니다 — 신규
                # 좌수/현금흐름 재적용을 피하려고 건너뛴다.
                print(f"  {google_sub[:8]}: {TARGET_DATE} 스냅샷 없음 — 건너뜀")
                continue
            fallback_count = await snapshot_nav.take_snapshot(google_sub, TARGET_DATE)
            total_fallback += fallback_count
            refreshed = await snapshots_repo.get_snapshot_by_date(google_sub, TARGET_DATE)
            note = f", 여전히 폴백 {fallback_count}종목" if fallback_count else ""
            print(
                f"  {google_sub[:8]}: value {existing['total_value']:.0f} -> "
                f"{refreshed['total_value']:.0f}, nav {existing['nav']:.2f} -> "
                f"{refreshed['nav']:.2f}{note}"
            )

        if total_fallback:
            print(
                f"주의: {total_fallback}개 종목이 재정산 후에도 실측 시세를 못 받아 "
                f"직전 값으로 남았습니다 — 시세 복구 후 다시 실행하세요."
            )
        else:
            print("모든 종목이 실측 시세로 재정산됐습니다.")
    finally:
        await bootstrap.close_db()


if __name__ == "__main__":
    asyncio.run(main())
