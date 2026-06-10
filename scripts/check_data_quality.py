#!/usr/bin/env python3
# 주가/배당 시계열 이상치 점검 CLI — 실제 점검 로직은 services/data_quality.py
# 로 승격됐고(정기 점검은 data-quality.timer 가 구동), 이 스크립트는 종목
# 선택 + 사람이 읽는 출력만 담당하는 thin wrapper 로 남는다.
import argparse
import asyncio
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cache
from repositories import user_stocks as user_stocks_repo
from services import data_quality


async def _get_target_codes(args: argparse.Namespace) -> list[str]:
    if args.stock_code:
        return sorted(set(args.stock_code))

    if args.all:
        db = await cache.get_db()
        try:
            cursor = await db.execute("SELECT stock_code FROM corp_codes ORDER BY stock_code LIMIT ?", (args.limit,))
            rows = await cursor.fetchall()
            return [row["stock_code"] for row in rows]
        finally:
            await db.close()

    cached_items = await user_stocks_repo.get_cached_analyses()
    codes = [item["stock_code"] for item in cached_items]
    if args.limit:
        codes = codes[:args.limit]
    return codes


async def _inspect_stock(stock_code: str, args: argparse.Namespace) -> dict:
    # 서비스로 위임 — args namespace 를 명시 파라미터로 풀어 전달.
    return await data_quality.inspect_stock(
        stock_code,
        start_year=args.start_year,
        end_year=args.end_year,
        max_dividend_yield=args.max_dividend_yield,
        max_dividend_jump=args.max_dividend_jump,
    )


def _print_human(results: list[dict]) -> None:
    total_findings = 0
    for result in results:
        findings = result["findings"]
        total_findings += len(findings)
        print(f"[{result['stock_code']}] {result['corp_name']}")
        if result["split_events"]:
            splits = ", ".join(f"{item['date']} x{item['ratio']:g}" for item in result["split_events"])
            print(f"  split_events: {splits}")
        if not findings:
            print("  findings: none")
            continue
        for finding in findings:
            print(f"  - {finding['severity']} | {finding['type']} | {finding['message']}")
    print()
    print(f"stocks={len(results)} findings={total_findings}")


async def async_main(args: argparse.Namespace) -> int:
    await cache.init_db()
    codes = await _get_target_codes(args)
    if not codes:
        print("검사할 종목이 없습니다. --stock-code 또는 캐시된 종목을 사용하세요.")
        return 1

    if args.limit and not args.all and not args.stock_code:
        codes = codes[:args.limit]

    results = []
    for stock_code in codes:
        results.append(await _inspect_stock(stock_code, args))

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        _print_human(results)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="주가/배당 시계열 이상치 자동 점검")
    parser.add_argument("--stock-code", action="append", help="검사할 종목코드. 여러 번 지정 가능")
    parser.add_argument("--all", action="store_true", help="corp_codes 전체를 검사")
    parser.add_argument("--limit", type=int, default=50, help="검사 종목 수 제한. 기본값 50")
    parser.add_argument("--start-year", type=int, default=2000, help="검사 시작 연도")
    parser.add_argument("--end-year", type=int, default=None, help="검사 종료 연도")
    parser.add_argument("--max-dividend-yield", type=float, default=data_quality.DEFAULT_MAX_DIVIDEND_YIELD,
                        help="배당수익률 이상치 임계치")
    parser.add_argument("--max-dividend-jump", type=float, default=data_quality.DEFAULT_MAX_DIVIDEND_JUMP,
                        help="전년 대비 주당배당금 점프 배수 임계치")
    parser.add_argument("--json", action="store_true", help="JSON으로 출력")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
