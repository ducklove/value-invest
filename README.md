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
core/                 config(env 프로파일)·app factory·lifespan·정적 라우트
routes/               HTTP/WS 핸들러 (포트폴리오·분석·알림·관리자·위키 …)
services/             도메인 로직 (portfolio/*, notifications/*, stock_quotes)
repositories/         SQLite 접근 (테이블별 모듈; db=커넥션/transaction,
                      bootstrap=init_db/close_db, schema=스키마·마이그레이션)
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

- 배치: systemd timer가 내부 API(`routes/internal.py`)를 호출한다 — NAV/장중
  스냅샷, 조건 알림, 경제캘린더 알림, 위키/DART 인제스트, DB 백업.
- 백업: `scripts/backup_cache_db.sh`가 매일 WAL-safe 온라인 백업 + 무결성 검사,
  일 14회·주 60일 보존. 복구: 서비스 중지 → `gunzip` 후 `cache.db` 교체 →
  서비스 시작 → `/healthz` 확인.
- 운영 이벤트/슬로우 요청은 `system_events` 테이블(30일 TTL)에 기록되고
  `/admin.html` 관측성 패널에서 본다.
- 장애 시 systemd `OnFailure` 훅이 ntfy.sh로 알림을 보낸다.
- `/healthz`는 프로세스 응답, `/readyz`는 필수 DB 테이블 조회 가능 여부를 확인한다.
  외부 시세 신선도는 데이터 품질 점검으로 구분한다. 해당 점검의 오류는 HTTP 503으로
  전달되어 timer의 `curl -f`와 `OnFailure`까지 이어진다.
- 신뢰 프록시를 쓰는 경우 Uvicorn의 `--forwarded-allow-ips`를 해당 프록시 주소로 제한한다.
  IP별 요청 제한은 검증된 연결 주소를 사용하며 원시 전달 헤더를 신뢰하지 않는다.

## 문서 색인 (docs/)

| 문서 | 내용 |
| --- | --- |
| refactoring-review-2026-06.html | 전체 구조·품질 평가 + 리팩토링 로드맵 + 기능 제언 |
| rearchitecture-plan.md | 단계별 재설계 계획 (진행 상태 포함) |
| project-architecture-graph.md | 허브·서브프로젝트·외부 소스 전체 그래프 |
| linked-projects.md | 서브프로젝트 연동 방식·환경변수 |
| environment-profiles.md | `.env` 단일 설정 소스·프로파일 |
| project-health-review.md | 2026-04 전체 점검 기록 |
| nps-separation-plan.md | NPS 분리 계획 (대시보드 분리 완료) |
| nav-trend-performance.md | NAV 차트 성능 개선 기록 |
| portfolio-frontend-structure.md | 프런트 JS 분할 구조·로드 순서 계약 |
| local-prod-portfolio-import.md | 운영 DB → 로컬 import 절차 |
