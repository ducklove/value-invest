# value-invest (Value Compass)

가치투자 포트폴리오·종목분석 허브. FastAPI 단일 서버가 API, 정적 SPA, 관리자
콘솔, KIS 실시간 시세 WebSocket, 내부 배치 트리거를 함께 제공하고, SQLite
(`cache.db`) 하나에 분석 캐시·사용자·포트폴리오·NAV 스냅샷·AI 사용량·공시/
리포트 요약을 저장한다. 운영은 라즈베리파이 + systemd.

연결 대시보드 9개(지주사·우선주·스팩·자사주·ETF·김치프리미엄·금 리서치·국민연금·채권)와
index-popup, kis-proxy, finance-pi 등은 독립 배포를 유지한다. 허브는 도구 레지스트리
[config/ecosystem.json](config/ecosystem.json)(정본), 딥링크, 형제가 발행하는 `summary.json`,
공용 셸(`static/ecosystem/`)로만 결합한다 — [docs/ecosystem/](docs/ecosystem/README.md) 참고.

## 빠른 시작 (로컬 개발)

```bash
pip install --require-hashes -r requirements-dev.lock
cp .env.example .env      # 시크릿·설정 단일 파일. VALUE_INVEST_ENV=development 로 조정.
python3 -m uvicorn main:app --reload --port 8000
```

- Windows는 `scripts/run-dev.ps1` 사용.
- 설정·시크릿은 `.env` 하나로 단일화되어 있다: [docs/environment-profiles.md](docs/environment-profiles.md)
- `.env`는 저장소에 커밋하지 않는다(추적 대상은 `.env.example`뿐).

## 테스트 / 린트

```bash
python3 -m pytest -q          # Python 전체 (배포 게이트와 동일)
npm ci && npm test            # JS jsdom 행위 테스트 (tests/js/)
npx playwright install chromium
npm run test:e2e              # 실제 브라우저 로그인·저장·재접속
python3 -m ruff check .       # 린트 — 규칙은 pyproject.toml (F, E9 시작)
python3 -m pytest --cov=. -q  # 커버리지 측정 (게이트 아님)
```

세 가지 모두 배포 스크립트가 실행하며 실패 시 배포가 중단·롤백된다.

## 코드 구조

```
main.py               ASGI 진입점 (조립은 core.app_factory)
core/                 config(env 프로파일)·app factory·lifespan·정적 라우트·http 클라이언트·
                      ecosystem(레지스트리 로더)·logging_setup(비밀 마스킹)
routes/               HTTP/WS 핸들러 (포트폴리오·분석·알림·관리자·위키·ecosystem(/go, /api/ecosystem) …)
services/             도메인 로직
  portfolio/          NAV 정산(nav_snapshot)·장중 스냅샷·벤치마크·시세·리포트 …
  market/             시장 지표(indicators)·브리프/테이프(daily)·등락(movers)·경제캘린더·뉴스
  market/sources/     외부 provider (finance_pi·close_price·kis_proxy·yahoo·yfinance_runner)
  ecosystem/          형제 도구: summary 로더(siblings)·링크(links)·요약(external_tools)·
                      연결 설정(integrations)·config 편집(linked_admin)
  dart/, dividends/   OpenDART 클라이언트, 우선주·해외 배당 수집기
  notifications/ …    알림 엔진·채널, stock_price(국내 시세 저수준)·stock_quotes(시세 경계)
repositories/         SQLite 접근 (테이블별 모듈; db=커넥션/transaction,
                      bootstrap=init_db/close_db, schema=스키마·마이그레이션)
domain/               순수 도메인 규칙 (market_calendar·timeutil·numbers …)
config/               ecosystem.json(도구 레지스트리 정본)·schemas/(summary.json 계약)·analytics-projects.json
ecosystem/            형제에 벤더링되는 발행 헬퍼 정본 (python/vc_publish.py, js/vc-publish.mjs)
루트 *.py (main 외)   [레거시] ai_config·cache_layer·observability·wiki_ingestion 등 —
                      services/core로 이전 중. snapshot_nav.py 는 복구 스크립트용 shim
static/               빌드 없는 vanilla JS SPA (로드 순서가 계약). static/ecosystem/ = 공용 셸·토큰·theme-boot 정본
scripts/, deploy/     운영 스크립트(sync-ecosystem.mjs 등), 배포 스크립트, systemd 유닛(저장소 루트)
```
리팩토링 방향과 현재 진행 상태는 [docs/rearchitecture-plan.md](docs/rearchitecture-plan.md),
생태계 차원의 중복 정리·백로그는 [docs/ecosystem/modularization.md](docs/ecosystem/modularization.md)가
기준 문서다. 지난 리뷰 보고서는 [docs/archive/](docs/archive/README.md)에 있다.

## 배포

`master` push → self-hosted runner가 `deploy/deploy.sh` 실행:

1. 새 커밋을 별도 임시 체크아웃에 풀고 `.venvs/<커밋>` 환경에 해시 고정 의존성 설치
2. 임시 체크아웃에서 **ruff → pytest → JS 테스트** 실행 (실패하면 운영 파일 유지)
3. 검증된 코드 반영 → systemd 유닛 동기화 → `.venv-current` 전환 → 서비스 재시작
4. **healthz·readyz 검사**. 재시작 자체를 포함한 실패 시 이전 코드·유닛·Python 환경 복구

`requirements*.txt`는 의존성 범위의 원본이고 `requirements*.lock`은 실제 설치 버전과
배포 파일 해시다. 갱신 시 아래 명령으로 두 파일을 재생성하고 전체 테스트를 실행한다.

```bash
uv pip compile --universal --python-version 3.11 --generate-hashes -o requirements.lock requirements.txt
uv pip compile --universal --python-version 3.11 --generate-hashes -c requirements.lock -o requirements-dev.lock requirements-dev.txt
```

Markdown 라이브러리는 `package-lock.json`과 일치하는 파일을 `static/js/vendor/`에
포함한다. 버전을 바꿀 때 `npm run vendor` 후 라이선스와 원본 일치 테스트를 함께 확인한다.
운영 호스트에는 `python3-venv`, Node/npm이 필요하다. `.venvs/`와 `.deploy-state/`에는
복구용 환경·유닛·설정이 보존되므로 현재/이전 배포를 제외한 오래된 항목만 정리한다.

## 운영 메모

- 일정산: 거래일 15:30 기준, 15:35 실행. 오후 브리핑은 확정 일간 성과,
  저녁은 장후 변화, 아침은 해외시장 중심이다. 가격·입출금·과거 자료 전환은
  [정규장 정산 운영 절차](docs/regular-close-settlement.md)를 따른다.
- 배치: systemd timer가 내부 API(`routes/internal.py`)를 호출한다 — NAV/장중
  스냅샷, 조건 알림, 경제캘린더 알림, 위키/DART 인제스트, DB 백업.
- 백업: `scripts/backup_cache_db.sh`가 매일 WAL-safe 온라인 백업 + 무결성 검사,
  일 14회·주 60일 보존. 복구: 서비스 중지 → `gunzip` 후 `cache.db` 교체 →
  서비스 시작 → `/healthz` 확인.
- 운영 이벤트/슬로우 요청은 `system_events` 테이블(30일 TTL)에 기록되고
  `/admin.html` 관측성 패널에서 본다.
- 장애 시 systemd `OnFailure` 훅이 ntfy.sh로 알림을 보낸다.
- 공개 주소는 `https://ducklove.duckdns.org:3691`(DuckDNS). uvicorn 이 직접 TLS 를
  종단하며 인증서는 certbot webroot(`/srv/acme`, Caddy 가 80 포트 챌린지 경로 서빙)로
  자동 갱신된다. 도메인을 옮기면 새 인증서 발급 → `.env` 의 `TLS_CERT_NAME` 변경 →
  `core/config.py`·`deps.py`·`static/app-config.js` 등의 기본 origin 을 함께 바꾼다
  (`git grep -n duckdns` 로 목록 확인). Google 로그인은 GIS 팝업 모드라 Google Cloud
  콘솔의 "승인된 JavaScript 원본"에 새 origin(포트 포함)을 등록해야 한다.
- `/healthz`는 프로세스 응답, `/readyz`는 필수 DB 테이블 조회 가능 여부를 확인한다.
  외부 시세 신선도는 데이터 품질 점검으로 구분한다. 해당 점검의 오류는 HTTP 503으로
  전달되어 timer의 `curl -f`와 `OnFailure`까지 이어진다.
- 신뢰 프록시를 쓰는 경우 Uvicorn의 `--forwarded-allow-ips`를 해당 프록시 주소로 제한한다.
  IP별 요청 제한은 검증된 연결 주소를 사용하며 원시 전달 헤더를 신뢰하지 않는다.

## 문서 색인 (docs/)

생태계(허브 + 연결 대시보드 + 인프라):

| 문서 | 내용 |
| --- | --- |
| [ecosystem/README.md](docs/ecosystem/README.md) | 생태계 문서 색인, 프로젝트 표, 어디서부터 읽나 |
| [ecosystem/architecture.md](docs/ecosystem/architecture.md) | 전체 구조도·데이터 흐름·호스팅/배포·통합 계약 |
| [ecosystem/ui-contract.md](docs/ecosystem/ui-contract.md) | 딥링크 계약·테마·공용 셸(vc-shell)·iframe 메시지·새 도구 추가 절차 |
| [ecosystem/data-contract.md](docs/ecosystem/data-contract.md) | 형제 → 허브 `summary.json` 발행 계약 |
| [ecosystem/external-data.md](docs/ecosystem/external-data.md) | 외부 데이터 소스·인증 env·캐시·중복 |
| [ecosystem/modularization.md](docs/ecosystem/modularization.md) | 중복 정리 현황과 백로그 |
| [ecosystem/roadmap.md](docs/ecosystem/roadmap.md) | 소유자 조치·후속 과제·확장 아이디어 |
| [linked-projects.md](docs/linked-projects.md) | 허브 쪽 연동 설정·환경변수·공용 알림 API |

허브:

| 문서 | 내용 |
| --- | --- |
| [rearchitecture-plan.md](docs/rearchitecture-plan.md) | 단계별 재설계 계획 (진행 상태 포함) |
| [architecture-improvements-2026-09.md](docs/architecture-improvements-2026-09.md) | 조회 격리·출처 검증·종료 정리·응답 계약·지연 로딩 개선 |
| [portfolio-frontend-structure.md](docs/portfolio-frontend-structure.md) | 프런트 JS 분할 구조·로드 순서 계약 |
| [environment-profiles.md](docs/environment-profiles.md) | `.env` 단일 설정 소스·프로파일 |
| [regular-close-settlement.md](docs/regular-close-settlement.md) | 정규장 정산 운영 절차 |
| [dependency-policy.md](docs/dependency-policy.md) | 의존성 정책 |
| [analytics.md](docs/analytics.md) | GA4 공용 analytics.js |
| [nav-trend-performance.md](docs/nav-trend-performance.md) | NAV 차트 성능 개선 기록 |
| [local-prod-portfolio-import.md](docs/local-prod-portfolio-import.md) | 운영 DB → 로컬 import 절차 |
| [archive/](docs/archive/README.md) | 지난 리뷰·감사 보고서(시점 기록) |

기능별 설계 문서(브로커·퀀트·배당·NAV 회계 등)는 `docs/`에 주제별 파일로 있다.
[project-architecture-graph.md](docs/project-architecture-graph.md)는 ecosystem/architecture.md로 옮겨졌다는 안내만 남아 있다.
