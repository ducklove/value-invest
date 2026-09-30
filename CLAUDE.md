# value-invest — 에이전트 온보딩 가이드

가치투자 포트폴리오·종목분석 허브. FastAPI 단일 서버 + SQLite(`cache.db`) +
빌드 없는 vanilla JS SPA. 운영은 라즈베리파이 + systemd. 개요·실행법은
[README.md](README.md), 리팩토링 로드맵은 [docs/rearchitecture-plan.md](docs/rearchitecture-plan.md).
형제 대시보드·인프라까지 포함한 생태계 구조·계약은 [docs/ecosystem/](docs/ecosystem/README.md).

## 지도 — 어디를 고치나

| 위치 | 역할 |
|---|---|
| `routes/` | HTTP/WS 핸들러. 기존 파일에 `@router.get` 추가는 그걸로 끝. **새 라우터 파일**을 만들면 `core/app_factory.py`에 include_router 등록 |
| `services/` | 도메인 로직 — `portfolio/*`(NAV 정산 `nav_snapshot`·장중 `intraday_snapshot`·`benchmark_history` 포함), `market/*`(`indicators`·`daily`·`movers`·`economic_calendar`·`news`·`sessions`), `market/sources/*`(외부 provider: `finance_pi`·`close_price`·`kis_proxy`·`yahoo`·`yfinance_runner` — **새 외부 소스는 여기**), `ecosystem/*`(형제 도구: summary 로더 `siblings`·링크 `links`·`external_tools`·`integrations`·`linked_admin`), `dart/client`, `dividends/*`, `notifications/*`, `stock_price`·`stock_quotes` |
| `repositories/` | 테이블별 SQLite 접근. `db.py`=커넥션 싱글톤+`transaction()`, `schema.py`=스키마·마이그레이션, `bootstrap.py`=`init_db()`/`close_db()` |
| `core/` | config(env 프로파일)·app_factory·lifespan·정적 라우트·http 클라이언트·errors·`ecosystem`(레지스트리 로더)·`logging_setup`(로그 비밀 마스킹) |
| `config/ecosystem.json` | **생태계 도구 레지스트리 정본**(21개, 공개 15). URL·딥링크 템플릿·데이터 URL·벤더링 경로. 파생: integrations 기본 URL, handoff 목록, `APP_CONFIG.ecosystem`, `/go/{tool}`, `/api/ecosystem`, vc-shell 인라인 블록 |
| `config/schemas/` | 형제 `summary.json` 계약(envelope + 도구별) — [docs/ecosystem/data-contract.md](docs/ecosystem/data-contract.md) |
| `static/ecosystem/` + `scripts/sync-ecosystem.mjs` | 공용 셸 `vc-shell.js`·토큰 `vc-tokens.css`·`vc-theme-boot.js` 정본. 스크립트가 허브 블록을 생성하고 형제 저장소(`../<도구 id>`)에 바이트 동일 사본을 쓴다/검증한다 |
| `ecosystem/python`, `ecosystem/js` | 형제에 벤더링되는 발행 헬퍼 정본(`vc_publish.py`, `vc-publish.mjs`) |
| `domain/` | 순수 규칙 — `market_calendar`(KRX 휴장일·수능), `timeutil`(`KST`·`now_kst`), `numbers`(`parse_number`) … |
| `static/` | 프론트엔드 — 아래 "프론트엔드 계약" 필독 |
| 루트 `*.py` 15개 | `main.py`(ASGI 진입점) + [레거시] `ai_config`·`analyzer`·`asset_insights`·`auth_service`·`cache_layer`·`dart_report_review`·`deps`·`dr_registry`·`kis_key_manager`·`kis_ws_manager`·`observability`·`report_client`·`wiki_ingestion` — services/core로 이전 중, **새 코드를 여기 만들지 말 것**. `snapshot_nav.py`는 deploy/repairs 용 호환 shim(정본 `services/portfolio/nav_snapshot`). 옮긴 모듈의 옛 이름 import 는 `tests/test_legacy_root_modules.py`가 막는다 |

`cache.py`는 2026-07 삭제됐다. 오래된 문서·커밋에서 `cache.get_db` 류를 보면
`repositories/{db,bootstrap,corp_codes,cache_values}`가 현재 위치다.

## 철칙

- DB 쓰기는 단문이라도 `async with transaction() as db:` ([repositories/db.py](repositories/db.py)) — 공유 커넥션에서 직접 `db.commit()` 금지.
- 외부 HTTP는 `core/http.get_http_client("이름")` 공유 클라이언트 (+`_TIMEOUT_PROFILES`에 타임아웃 등록).
- 캐시: 인메모리 TTL은 `cache_layer.MemoryTTLCache`(+ fetch는 `cached_fetch` — single-flight·stale 허용), DB 영속은 `repositories/cache_values`.
- KST는 `domain.timeutil`, 로그인 가드는 `deps.require_user`/`require_user_id`. 형제 URL·링크는 하드코딩하지 말고 레지스트리(`core.ecosystem`, `services/ecosystem/links`)에서.
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
모범 예시: [routes/stocks.py](routes/stocks.py)의 `/api/external/insights` + [services/ecosystem/external_tools.py](services/ecosystem/external_tools.py) — `get_http_client` fetch → `MemoryTTLCache` → 독립 실패 허용.

**C. 대시보드 위젯 추가**
1. index.html에 컨테이너 `<div id="…">` (main 컬럼/우측 rail 구분은 HTML 주석 참고).
2. `market-dashboard.js`에 `loadXxx()` 작성, `loadInvestingDashboard()`에 호출 한 줄 추가.
3. `tests/js/market-dashboard.test.mjs`에 행위 테스트 추가.

**D. 생태계에 새 도구 추가** ([docs/ecosystem/ui-contract.md §6](docs/ecosystem/ui-contract.md#6-새-대시보드를-생태계에-추가하기))
1. `config/ecosystem.json` `tools[]`에 항목 추가(`vendor.shell/themeBoot`는 처음엔 `false`). 공개 항목에 사설 IP·내부 포트 금지.
2. `node scripts/sync-ecosystem.mjs --write --hub-only`로 vc-shell 블록 재생성 → `python -m pytest -q tests/test_ecosystem_registry.py`.
3. 형제 체크아웃(`../<id>`)에 `node scripts/sync-ecosystem.mjs --write --only <id>` → 형제에서 셸 마크업 채택.
4. 채택 뒤 `vendor` 플래그를 `true`로, `tests/test_ecosystem_registry.py` `ADOPTED_SIBLINGS` 갱신. 허브가 요약을 읽으면 `config/schemas/summary/<id>.schema.json`·`tests/fixtures/ecosystem/<id>.summary.json` 먼저.

**E. 공용 셸/토큰 변경 후 형제 저장소 동기화**
1. `static/ecosystem/*`만 고친다(형제 사본 직접 수정 금지). 토큰은 추가만, 값은 `base.css`와 같게. 셸을 바꾸면 `vc-shell.js`의 `VERSION`을 올린다.
2. `node scripts/sync-ecosystem.mjs --write` — 허브 블록 재생성 + 형제 사본·theme-boot 쓰기·`?v=` 라벨 갱신(git은 안 건드림).
3. `node scripts/sync-ecosystem.mjs`가 FAIL 0이어야 한다. 허브 pytest·npm test와 각 형제 테스트(버전을 고정한 테스트가 있다)를 돌린다.
4. 저장소별로 커밋. 형제 push = Pages 배포이므로 허브를 먼저 배포하고, push는 사용자가 요청할 때만.

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
- 여전히 공유라 겹치면 충돌하는 파일: `static/index.html`(전 화면 마크업·스크립트 등록), `core/app_factory.py`(라우터 등록), `repositories/schema.py`(마이그레이션 목록), `config/ecosystem.json`(레지스트리) — 모두 append 위주라 머지는 쉬운 편이나 동시 편집은 피할 것.
