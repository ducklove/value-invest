# 실서버 포트폴리오 DB를 로컬 테스트 DB로 가져오기

실서버 `cache.db` 전체를 로컬 DB에 덮어쓰지 말고, 포트폴리오 관련 사용자 행만 가져온다.
이 방식은 `user_sessions` 같은 로그인 세션과 대부분의 운영 로그를 복사하지 않는다.

## 원칙

- 실서버에서는 SQLite `.backup` API로 일관된 스냅샷을 만든 뒤 내려받는다.
- 로컬 `cache.db`는 import 전에 자동 백업된다.
- 기본적으로 같은 `google_sub`로 가져온다. 같은 Google 계정으로 로컬 로그인하면 바로 포트폴리오가 보인다.
- 다른 로컬 계정에 심고 싶으면 `--dest-google-sub`, `--dest-email`, `--dest-name`을 지정한다.
- 가져온 DB 스냅샷과 로컬 백업 DB는 git에 올리지 않는다.

## PowerShell로 실서버에서 바로 가져오기

SSH 접속이 가능한 환경에서 실행한다.

```powershell
.\scripts\import-prod-portfolio-db.ps1 `
  -Remote "pi@cantabile.tplinkdns.com" `
  -RemoteDbPath "/home/pi/value-invest/cache.db" `
  -SourceEmail "your-google-email@example.com"
```

실서버 경로가 다르면 `-RemoteDbPath`만 실제 경로로 바꾼다.

## 이미 내려받은 DB 스냅샷에서 가져오기

```powershell
python .\scripts\import_portfolio_db.py `
  --source .\data\db-imports\prod-cache.db `
  --target .\cache.db `
  --source-email "your-google-email@example.com"
```

다른 로컬 사용자로 매핑하려면:

```powershell
python .\scripts\import_portfolio_db.py `
  --source .\data\db-imports\prod-cache.db `
  --target .\cache.db `
  --source-email "your-google-email@example.com" `
  --dest-google-sub "local-test-sub" `
  --dest-email "local@example.com" `
  --dest-name "Local Test User"
```

## 복사되는 테이블

- `users`의 대상 사용자 1명
- `user_portfolio`
- `portfolio_groups`
- `portfolio_tags`
- `portfolio_snapshots`
- `portfolio_cashflows`
- `portfolio_stock_snapshots`
- `portfolio_group_snapshots`
- `portfolio_stock_weight_snapshots`
- `portfolio_intraday`
- `user_stock_preferences`

`user_sessions`는 복사하지 않는다.

## 로컬 서버

import 전에는 로컬 서버를 잠시 끄는 편이 가장 안전하다. 현재 테스트 서버를 8010에서 띄웠다면:

```powershell
Get-NetTCPConnection -LocalPort 8010 |
  Where-Object State -eq Listen |
  ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
```

import 후에는 다시 실행한다.

```powershell
$env:VALUE_INVEST_ENV = "development"
python -m uvicorn main:app --host 127.0.0.1 --port 8010
```
