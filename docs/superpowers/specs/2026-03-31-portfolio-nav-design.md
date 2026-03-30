# Portfolio NAV Index System Design

## Overview
펀드 기준가(NAV) 방식으로 포트폴리오 수익률을 추적한다. 자금 입출금과 무관하게 순수 투자 수익률을 측정하기 위해 좌수(unit) 기반 시스템을 사용한다.

## Core Concept
- 최초 스냅샷일: NAV = 1000, units = total_value / 1000
- 입금 시: new_units = amount / current_nav, total_units += new_units
- 출금 시: redeemed_units = amount / current_nav, total_units -= redeemed_units
- 매일 22시: NAV = total_value / total_units
- NAV 곡선은 순수 투자 수익률만 반영

## Data Model

### `portfolio_snapshots`
| Column | Type | Description |
|---|---|---|
| google_sub | TEXT NOT NULL | User ID |
| date | TEXT NOT NULL | Date (YYYY-MM-DD) |
| total_value | REAL | Total market value in KRW |
| total_invested | REAL | Sum of qty * avg_price |
| nav | REAL | Net Asset Value (starts at 1000) |
| total_units | REAL | Total units outstanding |
| PRIMARY KEY | (google_sub, date) | |

### `portfolio_cashflows`
| Column | Type | Description |
|---|---|---|
| id | INTEGER PRIMARY KEY AUTOINCREMENT | |
| google_sub | TEXT NOT NULL | User ID |
| date | TEXT NOT NULL | Date (YYYY-MM-DD) |
| type | TEXT NOT NULL | 'deposit' or 'withdrawal' |
| amount | REAL NOT NULL | Amount in KRW |
| nav_at_time | REAL | NAV when cashflow occurred |
| units_change | REAL | Units added/removed |
| memo | TEXT | Optional note |
| created_at | TEXT NOT NULL | Timestamp |

## Snapshot Scheduler
- Cron: daily at 22:00 KST
- For each user with portfolio items:
  1. Fetch quotes for all items (reuse existing _fetch_quote logic)
  2. Calculate total market value
  3. Get previous day's snapshot for units/nav
  4. If first snapshot: nav=1000, units=total_value/1000
  5. Process any cashflows for the day (adjust units)
  6. Calculate nav = total_value / total_units
  7. Save snapshot

## API Endpoints
| Method | Path | Description |
|---|---|---|
| GET | /api/portfolio/nav-history | NAV time series for chart |
| GET | /api/portfolio/cashflows | Cashflow history list |
| POST | /api/portfolio/cashflows | Add deposit/withdrawal |
| DELETE | /api/portfolio/cashflows/{id} | Delete cashflow entry |

### POST /api/portfolio/cashflows body
```json
{"type": "deposit", "amount": 1000000, "date": "2026-03-31", "memo": "월급"}
```

### GET /api/portfolio/nav-history response
```json
[
  {"date": "2026-03-01", "nav": 1000.0, "total_value": 50000000},
  {"date": "2026-03-02", "nav": 1005.2, "total_value": 50260000}
]
```

## Frontend
Portfolio view gets a sub-tab toggle: 보유종목 | 성과분석

### 성과분석 tab:
- Top: NAV line chart (X=date, Y=NAV, base 1000)
- Below chart: return summary cards (전일, 1주, 1개월, 3개월, 전체)
- Below: cashflow history table with add/delete
- Cashflow form: date, type (입금/출금), amount, memo

## On cashflow registration
When a cashflow is registered (not during snapshot), it records nav_at_time from the most recent snapshot's NAV and calculates units_change. These units are applied during the next snapshot calculation.
