# Portfolio Frontend Structure

작성일: 2026-04-30

`static/js/portfolio.js`가 4,000줄을 넘으면서 작은 UI 변경이 실시간 시세, 그래프, 메뉴, 태그, 현금흐름에 예상치 못한 영향을 주는 상태가 되었다. 1차 리팩토링은 빌드 시스템을 새로 도입하지 않고, classic script 전역 계약과 실행 순서를 유지하면서 기능별 파일 경계를 나누는 방식으로 진행했다.

## Script Order

`static/index.html`은 아래 순서로 포트폴리오 기능을 로드한다. 순서가 곧 의존성 계약이므로 임의로 바꾸지 않는다.

1. `utils.js`: 공통 API fetch, 포맷, 앱 설정, markdown 렌더링.
2. `portfolio-trend-chart.js`: NAV/평가금액 추이용 canvas chart adapter.
3. `quote-manager.js`: WebSocket 시세와 polling fallback lifecycle.
4. `auth.js`: 로그인, 세션, 사용자 상태.
5. `search.js`: 종목 검색 UI.
6. `analysis.js`: 단일 종목 분석 UI.
7. `portfolio-shell.js`: 공유 상태, 컬럼 표시 설정, 뷰 전환, NPS 탭(nps-tracker iframe) 진입점.
8. `portfolio-data.js`: 포트폴리오 API 로딩, 정렬/필터 상태, 실시간 quote row 업데이트.
9. `portfolio-order.js`: 보유종목 drag/drop 정렬과 저장.
10. `portfolio-render.js`: 보유종목 테이블/카드 렌더링, 숫자 포맷, sparkline, benchmark 표시.
11. `portfolio-actions.js`: 그룹/benchmark/편집/삭제/검색/목표가/외부 링크 액션.
12. `portfolio-insights.js`: 투자 인사이트 모달, 태그 관리, linked dashboard 액션, 우선주/지주사 helper.
13. `portfolio-groups-market.js`: 그룹 관리 모달, market bar, CSV import/export, 통화 전환.
14. `portfolio-ai.js`: 포트폴리오 AI 분석 요청 흐름.
15. `portfolio-performance.js`: 추이 분석 탭 shell, 영역지도, 성과 데이터 로딩.
16. `portfolio-trends.js`: NAV/평가금액 추이 차트, 수익률 카드, 기간/Y축 동기화.
17. `portfolio-group-composition.js`: 그룹 구성 추이 drill-down.
18. `portfolio-cashflows.js`: 자금 입출금 표와 입출금 mutation.
19. `portfolio-tag-summary.js`: 태그 요약 UI.
20. `portfolio-events.js`: document-level delegated event handlers.
21. `insights.js`: 인사이트 목록 UI.
22. `app-main.js`: 앱 초기화와 전역 lifecycle 연결.

## Legacy Entrypoint

`static/js/portfolio.js`는 더 이상 구현 파일이 아니다. 새 HTML은 split 파일을 직접 로드한다. 다만 사용자가 배포 직후 오래된 HTML을 들고 있어 `/js/portfolio.js`만 요청하는 경우를 위해, `portfolio.js`는 split 파일을 동기 로드하는 호환 로더로 남겨둔다.

## Maintenance Rules

- 새 포트폴리오 기능은 가장 가까운 split 파일에 넣는다.
- split 파일이 1,000줄에 가까워지면 기능을 더 나눈다.
- UI 이벤트는 가능하면 `portfolio-events.js`의 delegated handler에 추가한다.
- 테이블 전체 재렌더가 필요한지, 특정 셀 in-place update로 충분한지 먼저 구분한다.
- NAV/평가금액 추이 차트는 `portfolio-trends.js`와 `portfolio-trend-chart.js` 안에서만 수정한다.
- 전역 변수 추가가 필요하면 `portfolio-shell.js` 또는 해당 도메인 파일 상단에 두고, 이름 앞에 `pf` 또는 `_pf`를 붙인다.

## Test Guardrails

`tests/test_frontend_structure.py`는 다음을 고정한다.

- `index.html`이 split 파일을 계약 순서대로 로드한다.
- 새 HTML이 legacy `portfolio.js`를 직접 로드하지 않는다.
- 포트폴리오 split 파일이 1,000줄 미만을 유지한다.

이 테스트가 실패하면 단순히 숫자를 고치지 말고, 왜 해당 파일이 커졌는지 보고 도메인 경계를 다시 잘라야 한다.
