# project_audit_report2.html 개선 진행 체크리스트

기준 리포트: `project_audit_report2.html`

작업 브랜치: `codex/project-audit-report2-fixes`

## 진행 현황

| 상태 | 항목 | 근거/결과 |
| --- | --- | --- |
| 완료 | 작업 브랜치 생성 | `codex/project-audit-report2-fixes` |
| 완료 | `/login` `return_to` 반사형 XSS 차단 | `routes/auth.py`, `tests/test_auth_password.py`, commit `aac6c2e0` |
| 완료 | 인증 엔드포인트 레이트리밋 추가 | `routes/auth.py`, `tests/test_auth_password.py`, commit `aac6c2e0` |
| 완료 | PR/브랜치 CI 신설 | `.github/workflows/ci.yml`, commit `aac6c2e0` |
| 완료 | 포트폴리오 본체 로드 실패 가시화 | `static/index.html`, `static/js/portfolio-data.js`, `tests/js/portfolio-load-sort.test.mjs`, commit `aac6c2e0` |
| 완료 | 알림 엔진의 포트폴리오 로드 실패 침묵 제거 | `services/notifications/engine.py`, `tests/test_notifications.py`, commit `aac6c2e0` |
| 완료 | PDF 파싱 이벤트 루프 블로킹 완화 | `wiki_ingestion.py`, `tests/test_wiki_ingestion.py`, commit `aac6c2e0` |
| 완료 | 포트폴리오 정렬 헤더 키보드/ARIA 보강 | `static/js/portfolio-render.js`, `tests/js/portfolio-load-sort.test.mjs`, commit `aac6c2e0` |
| 완료 | CSS 미정의 토큰 별칭 추가 | `static/styles.css`, `tests/test_frontend_structure.py`, commit `25583fcb` |
| 완료 | 실서버 배포 및 헬스체크 | GitHub Actions run `28555401145`, `/healthz` `asset_version=25583fc` |
| 완료 | `apiFetchJson` 공통 헬퍼 추가와 반복 오류 처리 축소 | `static/js/utils.js`, 대표 호출부 3개 파일, `tests/js/api-fetch.test.mjs` |
| 완료 | 토스트/로딩 상태 `aria-live` 적용 범위 확대 | `static/index.html`, `static/js/utils.js`, `static/js/analysis.js`, 접근성 회귀 테스트 |
| 완료 | 보안 헤더 미들웨어 + CSP Report-Only 추가 | `core/app_factory.py`, `tests/test_app_factory.py` |
| 완료 | 앱 셸 `main` 랜드마크·숨김 `h1`·스킵 링크 추가 | `static/index.html`, `static/styles.css`, `static/js/utils.js`, 동작 테스트 |
| 완료 | 경제 캘린더 알림 체크박스 계약을 jsdom 동작 테스트로 이관 | `tests/js/economic-calendar.test.mjs`, 기존 Python 문자열 검사 제거 |
| 완료 | 외부 fetch 일부를 공용 HTTP 클라이언트로 전환 | `external_tools.py`, `economic_calendar.py`, `preferred_dividends.py`, `core/http.py` |
| 완료 | `apiFetchJson` 파일 단위 전환 확대 | `static/js` JSON API 호출부와 `static/admin.html` 전환, SSE/외부 fetch/헬퍼 내부만 raw 예외 유지 |
| 완료 | `deploy.sh` one-time repair 5블록 분리 + lockfile | `deploy/deploy.sh` 180줄, `deploy/repairs/*`, `.deploy-repairs/repairs.lock` |
| 완료 | 공통 모달 유틸 + 포커스 트랩 | `static/js/utils.js` `openManagedModal`/`closeManagedModal`, 주요 모달 호출부 전환, jsdom focus-trap 테스트 |
| 완료 | P1 잔여 정리 | `wiki_ingestion`의 `routes` 역참조 제거, 알림 엔진 silent pass 2곳 warning 로그화, AI/wiki burst 레이트리밋 추가 |
| 완료 | 공용 HTTP 클라이언트 상위 호출부 전환 | `market_indicators.py`, `stock_price.py`, `market_daily.py`; 해당 3파일 직접 `httpx.AsyncClient` 생성 제거 |

## 다음 작업 후보

| 우선순위 | 상태 | 항목 | 비고 |
| --- | --- | --- | --- |
| P1 | 완료 | `apiFetchJson` 헬퍼로 JSON API 오류 처리 공통화 | 파일 단위 전환 완료, raw 예외는 SSE/외부 fetch/헬퍼 내부 |
| P1 | 완료 | 토스트/로딩 상태의 `aria-live` 적용 범위 확대 | 접근성 즉시 개선 항목 |
| P1 | 진행중 | string-presence 테스트를 jsdom 동작 테스트로 점진 이관 | 경제 캘린더 알림 체크박스 계약 1건 이관 완료, 남은 구조 계약은 순차 이관 |
| P1 | 완료 | 공통 모달/상태 표현과 포커스 트랩 | 주요 정적/동적 모달에 focus trap, Escape, focus restore 적용 |
| P1 | 완료 | `wiki_ingestion` → `routes` 역참조 제거 | PDF URL 허용 정책을 `services/report_url_policy.py`로 이동 |
| P1 | 완료 | 알림 엔진 silent pass 제거 | `get_all_users_with_alerts`, stale calendar cleanup 실패를 warning 로그로 기록 |
| P1 | 완료 | 고비용 AI/wiki 엔드포인트 레이트리밋 | `core/rate_limit.py`, wiki Q&A/portfolio AI burst limit |
| P2 | 완료 | 보안 헤더 미들웨어 + CSP Report-Only | XSS 방어층 강화 |
| P2 | 완료 | 앱 셸 `main` 랜드마크·숨김 `h1`·스킵 링크 | 접근성 문서 구조 개선 |
| P2 | 진행중 | 공용 HTTP 클라이언트 채택 확대 | 상위 `market_indicators`/`stock_price`/`market_daily` 전환 완료, 직접 생성 32곳 남음 |
| P2 | 완료 | `deploy.sh` 과거 리페어 블록 분리 | one-time repair 스크립트 5개와 shared lock runner로 분리 |
| P2 | 대기 | 의존성 lock/pinning 정책 정리 | 배포 버전 드리프트 완화 |
| P3 | 대기 | `cache.py` `init_db` 분해와 마이그레이션 체계화 | 중기 구조 과제 |
| P3 | 대기 | 배치 실행 원장과 SLO 대시보드 | 장기 운영 관측성 과제 |

## 검증 로그

| 날짜 | 범위 | 결과 |
| --- | --- | --- |
| 2026-07-02 | `python -m pytest tests/test_frontend_structure.py -q` | 54 passed |
| 2026-07-02 | `python -m ruff check ...` | 통과 |
| 2026-07-02 | `npm test` | 157 passed |
| 2026-07-02 | 로컬 브라우저 QA `/portfolio` | 렌더링/정렬 헤더/콘솔 확인 완료 |
| 2026-07-02 | 실서버 `/healthz` | `status=ok`, `asset_version=25583fc` |
| 2026-07-02 | `npm test` | 160 passed |
| 2026-07-02 | `python -m pytest tests/test_frontend_structure.py -q` | 54 passed |
| 2026-07-02 | `git diff --check` | 통과 |
| 2026-07-02 | `python -m pytest tests/test_app_factory.py tests/test_frontend_structure.py -q` | 60 passed |
| 2026-07-02 | `npm test` | 162 passed |
| 2026-07-02 | `python -m ruff check core/app_factory.py tests/test_app_factory.py tests/test_frontend_structure.py` | 통과 |
| 2026-07-02 | `python -m pytest tests/test_app_factory.py tests/test_frontend_structure.py -q` | 61 passed |
| 2026-07-02 | `npm test` | 163 passed |
| 2026-07-02 | `node --check static/js/utils.js; node --check static/js/analysis.js` | 통과 |
| 2026-07-02 | `python -m pytest tests/test_economic_calendar.py tests/test_external_tools.py tests/test_preferred_dividends.py -q` | 53 passed |
| 2026-07-02 | `npm test` | 166 passed |
| 2026-07-02 | `node --check tests/js/economic-calendar.test.mjs` | 통과 |
| 2026-07-02 | `python -m ruff check external_tools.py economic_calendar.py preferred_dividends.py tests/test_economic_calendar.py tests/test_external_tools.py tests/test_preferred_dividends.py` | 통과 |
| 2026-07-02 | `python -m pytest -q` | 1088 passed |
| 2026-07-02 | `python -m ruff check .` | 통과 |
| 2026-07-02 | `git diff --check` | 통과 |
| 2026-07-02 | `node --check` 변경 JS/MJS | 통과 |
| 2026-07-02 | `python -m pytest tests/test_frontend_structure.py -q` | 55 passed |
| 2026-07-02 | `npm test` | 167 passed |
| 2026-07-02 | `bash -n deploy/deploy.sh deploy/repairs/run_one_time_repairs.sh deploy/repairs/repair_2026_05_18_nav_v3.sh` | 통과 |
| 2026-07-02 | `python -m py_compile deploy/repairs/*.py` | 통과 |
| 2026-07-02 | `python -m pytest tests/test_wiki_ingestion.py tests/test_wiki_routes.py tests/test_portfolio_ai_with_wiki.py tests/test_notifications.py -q` | 119 passed |
| 2026-07-02 | `python -m pytest tests/test_market_indicators_kr_index.py tests/test_market_bond_sources.py tests/test_market_daily.py tests/test_stock_price.py tests/test_stock_price_utils.py -q` | 110 passed |
| 2026-07-02 | `python -m ruff check` 관련 변경 Python | 통과 |
| 2026-07-02 | `python -m pytest -q` | 1090 passed |
| 2026-07-02 | `npm test` | 168 passed |
| 2026-07-02 | `python -m ruff check .` | 통과 |
| 2026-07-02 | `git diff --check` | 통과 |
| 2026-07-02 | `node --check` 변경 JS/MJS | 통과 |
