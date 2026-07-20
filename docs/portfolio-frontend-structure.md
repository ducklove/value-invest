# Portfolio Frontend Structure

작성일: 2026-04-30 · 갱신: 2026-06-10 (analysis/admin 분할, PfStore 상태 통합 반영)

`static/js/portfolio.js`가 4,000줄을 넘으면서 작은 UI 변경이 실시간 시세, 그래프, 메뉴, 태그, 현금흐름에 예상치 못한 영향을 주는 상태가 되었다. 1차 리팩토링은 빌드 시스템을 새로 도입하지 않고, classic script 전역 계약과 실행 순서를 유지하면서 기능별 파일 경계를 나누는 방식으로 진행했다.

## Script Order

`static/index.html`은 아래 순서로 포트폴리오 기능을 로드한다. 순서가 곧 의존성 계약이므로 임의로 바꾸지 않는다.

1. `utils.js`: 공통 API fetch, 포맷, 앱 설정, markdown 렌더링.
2. `stock-hover-chart.js`: 종목 hover 일봉 캔들 툴팁 — utils.js(`apiFetchJson`, `escapeHtml`)만 의존하는 전 화면 공통 위임 핸들러. 새 표면은 요소에 `data-candle-code`를 붙이면 자동 적용.
3. `portfolio-trend-chart.js`: NAV/평가금액 추이용 canvas chart adapter.
4. `quote-manager.js`: WebSocket 시세와 polling fallback lifecycle.
5. `auth.js`: 로그인, 세션, 사용자 상태.
6. `search.js`: 종목 검색 UI.
7. `analysis-charts.js`: 주간/연간 가치평가 차트 렌더, 기간 전환, 목표가 오버레이, 차트 모달.
8. `analysis-filings.js`: DART AI 리뷰 카드/상세, 리포트 테이블, 위키 요약 로딩.
9. `analysis.js`: 단일 종목 분석 본체 — 검색, 기업 헤더, 개인화, analyze SSE, 위키 Q&A.
10. `portfolio-store.js`: `PfStore` — 파일 간 공유 포트폴리오 상태의 단일 컨테이너.
11. `portfolio-shell.js`: 컬럼 표시 설정, 뷰 전환, NPS 탭(nps-tracker iframe) 진입점, 파일 로컬 plumbing.
12. `portfolio-data.js`: 포트폴리오 API 로딩, 정렬/필터 상태, 실시간 quote row 업데이트.
13. `portfolio-order.js`: 보유종목 drag/drop 정렬과 저장.
14. `portfolio-render.js`: 보유종목 테이블/카드 렌더링, 숫자 포맷, sparkline, benchmark 표시.
15. `portfolio-actions.js`: 그룹/benchmark/편집/삭제/검색/목표가/외부 링크 액션.
16. `portfolio-insights.js`: 투자 인사이트 모달, 태그 관리, linked dashboard 액션, 우선주/지주사 helper.
17. `portfolio-groups-market.js`: 그룹 관리 모달, market bar, CSV import/export, 통화 전환.
18. `portfolio-ai.js`: 포트폴리오 AI 분석 요청 흐름.
19. `portfolio-reports.js`: 월간/연간 기간 투자 보고서 목록·생성·렌더링. v2 보고서의 매수/매도 기반 `composition_changes` 블록을 가장 먼저 보여준다.
20. `portfolio-performance.js`: 심층 분석 탭 shell, 영역지도, 성과 데이터 로딩.
21. `portfolio-trends-benchmark.js`: NAV 차트 비교지수 선택/캐시, beta·R² 통계와 beta 오버레이.
22. `portfolio-trends.js`: NAV/평가금액 추이 차트, 수익률 카드, 기간/Y축 동기화, 공용 차트 헬퍼.
23. `portfolio-trends-group-weight.js`: 그룹 비중 100% 누적 면적 차트와 drill-down 진입(`_GROUP_WEIGHT_COLORS` 공급).
24. `portfolio-group-composition.js`: 그룹 구성 추이 drill-down.
25. `portfolio-cashflows.js`: 자금 입출금 표와 입출금 mutation.
26. `portfolio-tag-summary.js`: 태그 요약 UI.
27. `portfolio-events.js`: document-level delegated event handlers.
28. `insights.js`: 인사이트 목록 UI.
29. `app-main.js`: 앱 초기화와 전역 lifecycle 연결.

## Legacy Entrypoint

`static/js/portfolio.js`는 더 이상 구현 파일이 아니다. 새 HTML은 split 파일을 직접 로드한다. 다만 사용자가 배포 직후 오래된 HTML을 들고 있어 `/js/portfolio.js`만 요청하는 경우를 위해, `portfolio.js`는 split 파일을 동기 로드하는 호환 로더로 남겨둔다.

## Maintenance Rules

- 새 포트폴리오 기능은 가장 가까운 split 파일에 넣는다.
- split 파일이 1,000줄에 가까워지면 기능을 더 나눈다.
- UI 이벤트는 가능하면 `portfolio-events.js`의 delegated handler에 추가한다.
- 테이블 전체 재렌더가 필요한지, 특정 셀 in-place update로 충분한지 먼저 구분한다.
- NAV/평가금액 추이 차트는 `portfolio-trends.js`와 `portfolio-trend-chart.js` 안에서만 수정한다. 비교지수/beta 오버레이는 `portfolio-trends-benchmark.js`, 그룹 비중 차트는 `portfolio-trends-group-weight.js`가 기능 홈이다.
- 파일 간 공유 상태는 `PfStore`(portfolio-store.js)에 둔다 — sort/filters/edit/manualOrder/snapshots/currency/prefs 그룹 또는 최상위 속성. 단일 파일만 쓰는 plumbing(타이머, DOM ref, 상수)만 해당 파일 상단 `let`으로 두고 이름 앞에 `_pf`를 붙인다.

## Test Guardrails

`tests/test_frontend_structure.py`는 다음을 고정한다.

- `index.html`이 split 파일을 계약 순서대로 로드한다.
- 새 HTML이 legacy `portfolio.js`를 직접 로드하지 않는다.
- 포트폴리오 split 파일이 1,000줄 미만을 유지한다.

- `ANALYSIS_SPLIT_FILES` / `ADMIN_SPLIT_FILES` 계약: analysis/admin 분할 파일의 로드 순서, 기능 귀속(함수가 약속된 파일에 존재), 1,000줄 상한.

이 테스트가 실패하면 단순히 숫자를 고치지 말고, 왜 해당 파일이 커졌는지 보고 도메인 경계를 다시 잘라야 한다.

## Admin Page

`static/admin.html`은 inline `apiFetch` → `admin.js`(부트스트랩·AI 설정·공용 헬퍼) →
`admin-observability.js`(관측성 패널·라이브 갱신·수동 잡) →
`admin-linked-projects.js`(연결 프로젝트 config·외국/우선주 배당 관리) 순서로 로드한다.
