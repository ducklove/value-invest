# value-invest (Value Compass)

가치투자 포트폴리오·종목분석 허브. FastAPI 단일 서버가 API, 정적 SPA, 관리자
콘솔, KIS 실시간 시세 WebSocket, 내부 배치 트리거를 함께 제공하고, SQLite
(`cache.db`) 하나에 분석 캐시·사용자·포트폴리오·NAV 스냅샷·AI 사용량·공시/
리포트 요약을 저장한다. 운영은 라즈베리파이 + systemd.

연결 서브프로젝트(지주사/우선주/스팩/금·BTC 대시보드, nps-tracker, kis-proxy,
finance-pi)는 독립 배포를 유지하고, 이 허브는 딥링크·published JSON·서버사이드
프록시로만 결합한다 — [docs/linked-projects.md](docs/linked-projects.md) 참고.

## 빠른 시작 (로컬 개발)

```bash
pip install -r requirements-dev.txt   # requirements.txt 포함
cp .env.development.example .env.development
VALUE_INVEST_ENV=development python3 -m uvicorn main:app --reload --port 8000
```

- Windows는 `scripts/run-dev.ps1` 사용.
- 환경 프로파일/로드 순서: [docs/environment-profiles.md](docs/environment-profiles.md)
- 시크릿(.env.production, `.kis.env`, `keys.txt`)은 저장소에 커밋하지 않는다.

## 테스트 / 린트

```bash
python3 -m pytest -q          # Python 전체 (배포 게이트와 동일)
npm ci && npm test            # JS jsdom 행위 테스트 (tests/js/)
python3 -m ruff check .       # 린트 — 규칙은 pyproject.toml (F, E9 시작)
python3 -m pytest --cov=. -q  # 커버리지 측정 (게이트 아님)
```

세 가지 모두 배포 스크립트가 실행하며 실패 시 배포가 중단·롤백된다.

## 코드 구조

```
main.py               ASGI 진입점 (조립은 core.app_factory)
core/                 config(env 프로파일)·app factory·lifespan·정적 라우트
routes/               HTTP/WS 핸들러 (포트폴리오·분석·알림·관리자·위키 …)
services/             도메인 로직 (portfolio/*, notifications/*, stock_quotes)
repositories/         SQLite 접근 (테이블별 모듈)
cache.py              [축소 중] DB 연결 + 스키마 + 레거시 재수출 허브
stock_price.py 등     [레거시] 시세·시장 데이터 수집 모듈 — services로 이전 중
static/               빌드 없는 vanilla JS SPA (로드 순서가 계약)
scripts/, deploy/     운영 스크립트, 배포 스크립트, systemd 유닛(저장소 루트)
```

리팩토링 방향과 현재 진행 상태는
[docs/rearchitecture-plan.md](docs/rearchitecture-plan.md)와
[docs/refactoring-review-2026-06.html](docs/refactoring-review-2026-06.html)이
기준 문서다.

## 배포

`master` push → self-hosted runner가 `deploy/deploy.sh` 실행:

1. 프로덕션 체크아웃을 origin/master로 reset
2. requirements 변경 시에만 pip 설치
3. **ruff → pytest → JS 테스트** (하나라도 실패하면 OLD_SHA로 롤백)
4. systemd 유닛 동기화 → 서비스 재시작
5. **healthz 검사 — 실패 시 OLD_SHA로 롤백 후 재기동**

## 운영 메모

- 배치: systemd timer가 내부 API(`routes/internal.py`)를 호출한다 — NAV/장중
  스냅샷, 조건 알림, 경제캘린더 알림, 위키/DART 인제스트, DB 백업.
- 백업: `scripts/backup_cache_db.sh`가 매일 WAL-safe 온라인 백업 + 무결성 검사,
  일 14회·주 60일 보존. 복구: 서비스 중지 → `gunzip` 후 `cache.db` 교체 →
  서비스 시작 → `/healthz` 확인.
- 운영 이벤트/슬로우 요청은 `system_events` 테이블(30일 TTL)에 기록되고
  `/admin.html` 관측성 패널에서 본다.
- 장애 시 systemd `OnFailure` 훅이 ntfy.sh로 알림을 보낸다.

## 문서 색인 (docs/)

| 문서 | 내용 |
| --- | --- |
| refactoring-review-2026-06.html | 전체 구조·품질 평가 + 리팩토링 로드맵 + 기능 제언 |
| rearchitecture-plan.md | 단계별 재설계 계획 (진행 상태 포함) |
| project-architecture-graph.md | 허브·서브프로젝트·외부 소스 전체 그래프 |
| linked-projects.md | 서브프로젝트 연동 방식·환경변수 |
| environment-profiles.md | env 프로파일 로드 순서 |
| project-health-review.md | 2026-04 전체 점검 기록 |
| nps-separation-plan.md | NPS 분리 계획 (대시보드 분리 완료) |
| nav-trend-performance.md | NAV 차트 성능 개선 기록 |
| portfolio-frontend-structure.md | 프런트 JS 분할 구조·로드 순서 계약 |
| local-prod-portfolio-import.md | 운영 DB → 로컬 import 절차 |
