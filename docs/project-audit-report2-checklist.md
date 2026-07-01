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

## 다음 작업 후보

| 우선순위 | 상태 | 항목 | 비고 |
| --- | --- | --- | --- |
| P1 | 완료 | `apiFetchJson` 헬퍼로 JSON API 오류 처리 공통화 | 30일 계획의 `apiFetchJson 헬퍼`; 대표 호출부 전환 완료 |
| P1 | 대기 | 토스트/로딩 상태의 `aria-live` 적용 범위 확대 | 접근성 즉시 개선 항목 |
| P1 | 대기 | string-presence 테스트를 jsdom 동작 테스트로 점진 이관 | 90일 계획 |
| P2 | 대기 | 보안 헤더 미들웨어 + CSP Report-Only | XSS 방어층 강화 |
| P2 | 대기 | 공용 HTTP 클라이언트 채택 확대 | `httpx.AsyncClient` 직접 생성 축소 |
| P2 | 대기 | 공통 모달/상태 표현과 포커스 트랩 | UX/접근성 구조 개선 |
| P2 | 대기 | `deploy.sh` 과거 리페어 블록 분리 | 배포 스크립트 유지보수 개선 |
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
