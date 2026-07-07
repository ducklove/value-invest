# value-invest — 에이전트 온보딩 가이드

가치투자 포트폴리오·종목분석 허브. FastAPI 단일 서버 + SQLite(`cache.db`) +
빌드 없는 vanilla JS SPA. 운영은 라즈베리파이 + systemd. 개요·실행법은
[README.md](README.md), 리팩토링 로드맵은 [docs/rearchitecture-plan.md](docs/rearchitecture-plan.md).

## 지도 — 어디를 고치나

| 위치 | 역할 |
|---|---|
| `routes/` | HTTP/WS 핸들러. 기존 파일에 `@router.get` 추가는 그걸로 끝. **새 라우터 파일**을 만들면 `core/app_factory.py`에 include_router 등록 |
| `services/` | 도메인 로직 (`portfolio/*`, `notifications/*`, `market/*`, `stock_quotes`) |
| `repositories/` | 테이블별 SQLite 접근. `db.py`=커넥션 싱글톤+`transaction()`, `schema.py`=스키마·마이그레이션, `bootstrap.py`=`init_db()`/`close_db()` |
| `core/` | config(env 프로파일)·app_factory·lifespan·정적 라우트·http 클라이언트·errors |
| `static/` | 프론트엔드 — 아래 "프론트엔드 계약" 필독 |
| 루트 `*.py` 32개 | [레거시] 시장데이터 수집기 등. services로 이전 중 — **새 코드를 여기 만들지 말 것** |

`cache.py`는 2026-07 삭제됐다. 오래된 문서·커밋에서 `cache.get_db` 류를 보면
`repositories/{db,bootstrap,corp_codes,cache_values}`가 현재 위치다.

## 철칙

- DB 쓰기는 단문이라도 `async with transaction() as db:` ([repositories/db.py](repositories/db.py)) — 공유 커넥션에서 직접 `db.commit()` 금지.
- 외부 HTTP는 `core/http.get_http_client("이름")` 공유 클라이언트 (+`_TIMEOUT_PROFILES`에 타임아웃 등록).
- 캐시: 인메모리 TTL은 `cache_layer.MemoryTTLCache`, DB 영속은 `repositories/cache_values`.
- 예외는 `core/errors.py` 계층 사용. 광역 `except Exception` 신설 금지 (기존 281곳은 점진 교체 중).

## 프론트엔드 계약 (빌드 시스템 없음 — 중요)

- classic `<script defer>` 전역 함수 방식이다. ES 모듈(import/export) 아님.
- **index.html의 `<script>`/`<link>` 순서가 의존성 계약** — [docs/portfolio-frontend-structure.md](docs/portfolio-frontend-structure.md) 필독.
- CSS는 `static/css/` 8개 파일(base→dashboard→analysis→portfolio→mobile-overrides→admin-wiki→mobile-shell→labs), 로드 순서 고정(구 styles.css의 연속 분할). 새 규칙은 해당 화면 파일 끝에, 전 화면 공통은 base.css에.
- 파일 간 공유 상태는 `portfolio-store.js`(PfStore), 공용 헬퍼는 `utils.js`(`apiFetchJson`, `escapeHtml`, `fmtPct` …).
- JS 테스트는 `tests/js/*.test.mjs`(jsdom 행위 테스트)가 표준. `tests/test_frontend_structure_*.py` 문자열 검사는 구조 계약 고정용.

## 레시피

**A. 보유종목에 DB 컬럼 추가**
1. [repositories/schema.py](repositories/schema.py) `CORE_COLUMN_MIGRATIONS`에 `("user_portfolio", "컬럼명", "타입 DEFAULT …")` 추가 — idempotent, `bootstrap.init_db()`가 적용.
2. [repositories/portfolio.py](repositories/portfolio.py) `get_portfolio()` SELECT와 `save_portfolio_item()`에 반영 — "미전달 = 기존값 유지" 패턴 준수.
3. [routes/portfolio.py](routes/portfolio.py) `PUT /api/portfolio/{stock_code}`에서 payload 검증 후 전달.
4. `tests/test_portfolio.py`에 roundtrip 테스트 (`TempDbMixin` 하니스).

**B. 외부 데이터 API 엔드포인트 추가**
모범 예시: [routes/stocks.py](routes/stocks.py)의 `/api/external/insights` + [external_tools.py](external_tools.py) — `get_http_client` fetch → `MemoryTTLCache` → 독립 실패 허용.

**C. 대시보드 위젯 추가**
1. index.html에 컨테이너 `<div id="…">` (main 컬럼/우측 rail 구분은 HTML 주석 참고).
2. `market-dashboard.js`에 `loadXxx()` 작성, `loadInvestingDashboard()`에 호출 한 줄 추가.
3. `tests/js/market-dashboard.test.mjs`에 행위 테스트 추가.

## 테스트·배포

```bash
python -m pytest -q        # 전체 — 배포 게이트와 동일, 푸시 전 필수
npm test                   # jsdom 행위 테스트
python -m ruff check .     # F, E9, I
```

- **master push = 곧 배포**: self-hosted runner가 `deploy/deploy.sh` 실행 (ruff→pytest→npm test→restart→healthz, 실패 시 OLD_SHA 롤백). 배포 의도 없이 master에 push 금지.
- 커밋까지만 하고 push(=배포)는 사용자가 요청할 때만.

## 병렬 작업 가이드

- 화면별 파일 홈이 분리돼 있다: CSS(`static/css/화면.css`), 구조 테스트(`tests/test_frontend_structure_화면.py`), JS(기능별 파일) — 서로 다른 화면의 병렬 작업은 충돌하지 않는다.
- 여전히 공유라 겹치면 충돌하는 파일: `static/index.html`(전 화면 마크업·스크립트 등록), `core/app_factory.py`(라우터 등록), `repositories/schema.py`(마이그레이션 목록) — 모두 append 위주라 머지는 쉬운 편이나 동시 편집은 피할 것.
