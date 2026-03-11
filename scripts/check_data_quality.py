#!/usr/bin/env python3
import argparse
import asyncio
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cache
import stock_price


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

    cached_items = await cache.get_cached_analyses()
    codes = [item["stock_code"] for item in cached_items]
    if args.limit:
        codes = codes[:args.limit]
    return codes


def _detect_series_anomalies(
    stock_code: str,
    corp_name: str,
    market_data: list[dict],
    raw_dividends_by_year: dict[int, float],
    adjusted_dividends_by_year: dict[int, float],
    split_events: list[tuple],
    max_dividend_yield: float,
    max_dividend_jump: float,
) -> list[dict]:
    findings: list[dict] = []
    split_year_pairs = set()
    for split_ts, _ in split_events:
        split_year_pairs.add((split_ts.year - 1, split_ts.year))
        split_year_pairs.add((split_ts.year, split_ts.year + 1))

    for row in market_data:
        year = row["year"]
        dividend_yield = row.get("dividend_yield")
        if dividend_yield is not None and dividend_yield > max_dividend_yield:
            findings.append({
                "severity": "high",
                "type": "dividend_yield_outlier",
                "year": year,
                "message": f"{year} 배당수익률 {dividend_yield}%가 임계치 {max_dividend_yield}%를 초과합니다.",
            })

    prev_row = None
    for row in market_data:
        dps = row.get("dividend_per_share")
        if prev_row and dps is not None and prev_row.get("dividend_per_share") not in (None, 0):
            prev_dps = prev_row["dividend_per_share"]
            ratio = dps / prev_dps
            year_pair = (prev_row["year"], row["year"])
            if year_pair in split_year_pairs:
                prev_row = row
                continue
            if ratio >= max_dividend_jump or ratio <= (1 / max_dividend_jump):
                findings.append({
                    "severity": "medium",
                    "type": "dividend_jump",
                    "year": row["year"],
                    "message": (
                        f"{prev_row['year']} -> {row['year']} 주당배당금이 "
                        f"{prev_dps:.2f}원 -> {dps:.2f}원으로 크게 변했습니다."
                    ),
                })
        prev_row = row

    if split_events:
        split_summaries = [f"{ts.date()} x{ratio:g}" for ts, ratio in split_events]
        for year, raw_value in sorted(raw_dividends_by_year.items()):
            adjusted_value = adjusted_dividends_by_year.get(year)
            if raw_value is None or adjusted_value is None:
                continue
            if raw_value == 0:
                continue
            diff_ratio = abs(adjusted_value - raw_value) / abs(raw_value)
            if diff_ratio >= 0.5:
                findings.append({
                    "severity": "info",
                    "type": "dividend_adjusted_for_split",
                    "year": year,
                    "message": (
                        f"{year} raw 주당배당금 {raw_value:.2f}원을 "
                        f"split/감자 이력({', '.join(split_summaries)}) 기준으로 {adjusted_value:.2f}원으로 보정했습니다."
                    ),
                })

    for row in market_data:
        if row.get("close_price") is None:
            findings.append({
                "severity": "low",
                "type": "missing_close_price",
                "year": row["year"],
                "message": f"{row['year']} 종가 데이터가 비어 있습니다.",
            })

    return findings


async def _inspect_stock(stock_code: str, args: argparse.Namespace) -> dict:
    corp_name = await cache.get_corp_name(stock_code) or stock_code
    end_year = args.end_year
    if end_year is None:
        end_year = stock_price.datetime.now().year
    financial_data = await cache.get_financial_data(stock_code)
    market_data = await stock_price.fetch_market_data(
        stock_code,
        financial_data,
        start_year=args.start_year,
        end_year=end_year,
    )

    loop = asyncio.get_event_loop()
    try:
        _, _, raw_dividends, raw_splits = await loop.run_in_executor(
            None,
            stock_price._get_yfinance_aux,
            stock_code,
            args.start_year,
            end_year,
        )
    except Exception:
        raw_dividends = None
        raw_splits = None

    split_events = stock_price._normalized_split_events(raw_splits)
    adjusted_dividends = stock_price._adjust_dividends_for_splits(raw_dividends, split_events)
    raw_dividends_by_year = stock_price._group_sum_by_year(raw_dividends)
    adjusted_dividends_by_year = stock_price._group_sum_by_year(adjusted_dividends)

    findings = _detect_series_anomalies(
        stock_code,
        corp_name,
        market_data,
        raw_dividends_by_year,
        adjusted_dividends_by_year,
        split_events,
        args.max_dividend_yield,
        args.max_dividend_jump,
    )

    return {
        "stock_code": stock_code,
        "corp_name": corp_name,
        "years": [row["year"] for row in market_data],
        "findings": findings,
        "split_events": [{"date": ts.strftime("%Y-%m-%d"), "ratio": ratio} for ts, ratio in split_events],
    }


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
    parser.add_argument("--max-dividend-yield", type=float, default=50.0, help="배당수익률 이상치 임계치")
    parser.add_argument("--max-dividend-jump", type=float, default=5.0, help="전년 대비 주당배당금 점프 배수 임계치")
    parser.add_argument("--json", action="store_true", help="JSON으로 출력")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
