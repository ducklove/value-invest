# Portfolio Groups Feature Design

## Overview
포트폴리오 종목에 그룹 분류 기능을 추가한다. 기본 그룹(한국주식/해외주식/기타)은 종목 코드 패턴으로 자동 배정되며, 사용자가 커스텀 그룹을 추가/삭제/변경할 수 있다.

## Data Model

### New table: `portfolio_groups`
| Column | Type | Description |
|---|---|---|
| google_sub | TEXT NOT NULL | User ID (FK) |
| group_name | TEXT NOT NULL | Group name |
| sort_order | INTEGER | Display order in filter bar |
| is_default | BOOLEAN DEFAULT 0 | If true, cannot be deleted |
| PRIMARY KEY | (google_sub, group_name) | |

### `user_portfolio` table addition
- `group_name TEXT DEFAULT NULL` — NULL means auto-assign by code pattern

### Default groups (undeletable)
| group_name | Auto-assign rule | sort_order |
|---|---|---|
| 한국주식 | 6-digit code starting with digits | 0 |
| 해외주식 | Not Korean or special | 1 |
| 기타 | KRX_GOLD, CRYPTO_BTC, CRYPTO_ETH | 2 |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/portfolio/groups` | List user's groups |
| POST | `/api/portfolio/groups` | Add group `{name}` |
| PUT | `/api/portfolio/groups/{name}` | Rename group `{new_name}` |
| DELETE | `/api/portfolio/groups/{name}` | Delete group (block defaults, items revert to default) |

Existing `PUT /api/portfolio/{code}` gains optional `group_name` field.

## Frontend Changes

### Filter bar
- Replace hardcoded 한국/해외/기타 toggles with dynamic group-based toggles
- Add gear icon button next to filter area → opens group management modal

### Table
- Add "그룹" column after stock name column
- Sortable via header click (3-state: asc → desc → manual)
- Group cell click opens dropdown to reassign group

### Group management modal
- List all groups (default groups have disabled delete button)
- Add new group (name input)
- Rename group (inline edit)
- Delete group (shows item count, confirms, items revert to default group)

## Migration
1. Create `portfolio_groups` table
2. Add `group_name` column to `user_portfolio`
3. Auto-create 3 default groups for existing users
4. Auto-assign existing items using `pfMarketType()` logic

## On custom group deletion
Items in deleted group revert to their default group based on stock code pattern.
