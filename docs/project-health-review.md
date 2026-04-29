# value-invest 전체 점검 기록

작성일: 2026-04-30

이 문서는 `value-invest` 본 프로젝트와 연결 서브프로젝트 운영 구조를 함께 보는 유지보수 기록이다. 이번 점검의 목표는 단순히 “테스트 통과”가 아니라, 실제 사용자가 느끼는 대기/불안정/관리 난이도를 줄이는 것이다.

## 1. 현재 구조

### 런타임

- FastAPI 단일 서버가 API, 정적 SPA, 관리자 콘솔, WebSocket 실시간 시세, 내부 배치 트리거를 함께 담당한다.
- SQLite(`cache.db`)가 분석 캐시, 사용자/세션, 포트폴리오, NAV 스냅샷, AI 사용량, 공시/리포트 요약, 운영 이벤트를 저장한다.
- systemd timer가 NAV/NPS/장중 스냅샷, wiki/DART ingestion 같은 운영 작업을 호출한다.
- 프론트엔드는 별도 빌드 시스템 없이 `static/index.html` + 여러 JS 파일 + `styles.css`로 구성되어 있다.

### 연결 프로젝트

- `holdingValue`: 지주사/자회사 가치 링크 및 설정 관리.
- `preferredSpread`: 우선주-본주 괴리율 링크 및 설정 관리.
- `goldGap`: 금/비트코인 gap 링크 및 포트폴리오 특수자산 연결.
- `kisProxy`: KIS API 호출 프록시. 브라우저가 아니라 서버에서 `kis_proxy_client.py`를 통해 사용한다.
- 통합 설정은 `integrations.py`, `/app-config.js`, `/api/integrations`, `/admin.html`의 linked-project config 관리가 담당한다.

### 규모

- Python: 66개 파일, 약 873KB.
- JavaScript: 12개 파일, 약 411KB.
- CSS: 1개 파일, 약 76KB.
- 테스트: 22개 Python 테스트 파일.
- 주요 대형 파일:
  - `static/js/portfolio.js`: 약 4,039줄.
  - `cache.py`: 약 2,588줄.
  - `routes/portfolio.py`: 약 2,087줄.
  - `static/styles.css`: 약 2,133줄.
  - `static/js/analysis.js`, `static/js/admin.js`: 각각 1,500줄 이상.

## 2. 이번에 확인한 핵심 문제

### 유지보수성

- `portfolio.js`, `cache.py`, `routes/portfolio.py`가 너무 크다. 기능이 한 파일에 누적되어 수정 범위와 사이드이펙트를 예측하기 어렵다.
- 프론트엔드에 빌드/타입체크가 없어 JS 계약 변경이 런타임까지 가서야 드러난다.
- 차트, 실시간 시세, 포트폴리오 편집, 태그, AI 인사이트, 현금흐름, NAV 분석이 `portfolio.js`에 함께 들어 있어 작은 UI 변경이 핵심 동작을 건드릴 위험이 높다.

### 효율성

- NAV/평가금액 추이는 필요한 기능 대비 ECharts가 과했다. 외부 bundle 로드, 초기화, 레이아웃 측정이 모두 사용자가 보는 “그래프 안 뜸/느림”으로 연결되었다.
- 관리자 `batch-status`, `deploy-status`는 async endpoint 안에서 `subprocess.run()`을 직접 호출해 systemd가 느릴 때 FastAPI event loop를 막을 수 있었다.
- 비교지수 최초 조회는 운영 DB 캐시가 없으면 여전히 `/api/portfolio/benchmark-history`가 데이터를 채우느라 느릴 수 있다. 다만 NAV 자체 표시가 이 API에 묶이면 안 된다.

### 보안

- 관리자 mutation API는 세션 쿠키만으로 권한을 판단하면 CSRF 위험이 남는다. GitHub Pages/앱 서버 흐름 때문에 cookie SameSite가 완전히 방어막이 되기 어렵다.
- internal API는 현재 토큰 또는 엄격한 loopback 검증을 갖추고 있어 이전 reverse-proxy 위험은 완화되어 있다.
- 공시/리포트/AI 응답은 이미 markdown sanitizer 경로를 사용하고 있으나, 새 UI를 추가할 때 반드시 `_renderSafeMarkdown()`을 계속 써야 한다.

### 사용성/접근성

- 포트폴리오의 정보량이 많아 데스크톱은 강력하지만, 모바일은 정보 밀도가 곧 피로도로 이어진다. 최근 요청으로 모바일 카드/그래프 옵션은 많이 덜어낸 상태다.
- 그래프 기간 시작/종료를 텍스트로 명확히 보여주는 것은 좋은 방향이다. 차트 자체의 hover보다 “현재 표시 기간” 라벨이 사용자 확신을 더 잘 준다.
- 관리자 화면은 기능이 많아졌지만 mutation 성공/실패 기록과 job history가 아직 충분히 독립적인 운영 객체로 관리되지는 않는다.

### AI 활용성

- AI key, 기능별 모델, 사용량 기록의 기초는 들어가 있다.
- DART 공시 리뷰, 리포트 wiki, 종목 Q&A가 서로 같은 기반 지식을 바라보는 방향은 맞다.
- 다만 AI 결과를 “생성 요청” 위주로 노출하면 사용자는 기다리게 된다. 중요한 종목은 배치로 미리 생성하고, 화면에서는 캐시된 요약을 즉시 보여주는 패턴이 계속 필요하다.

## 3. 이번에 반영한 개선

### NAV/평가금액 추이

- `static/js/portfolio-trend-chart.js`를 추가해 NAV/평가금액 추이에서 ECharts 의존을 제거했다.
- 기존 호출부가 크게 흔들리지 않도록 `setOption`, `getOption`, `dispatchAction`, `on('datazoom')`, `resize`, `dispose`를 제공하는 작은 호환 인터페이스로 만들었다.
- NAV 단독 그래프와 비교지수 그래프가 같은 렌더러를 사용한다.
- 비교지수는 별도 축이 아니라 NAV와 같은 축에 점선으로 그린다.
- 표시 구간 변경 시 NAV와 비교지수 값을 함께 보고 Y축을 다시 계산한다.
- 모든 표시 값이 양수인데 padding 때문에 Y축 min이 음수로 내려가는 경우를 막았다.
- NAV/평가금액 그래프를 같은 데이터로 병렬 렌더하도록 바꿨다.
- 앱 시작 직후 외부 chart bundle preload를 제거해 초기 페이지 로딩과 네트워크 경쟁을 줄였다.

### 관리자 보안/운영성

- 관리자 POST/PUT/PATCH/DELETE 공통 가드 `_require_admin_mutation()`을 추가했다.
- mutation 요청은 `application/json`이어야 하며, `Origin`/`Referer`가 신뢰 출처여야 한다.
- 프론트 `apiFetch()`가 관리자 mutation에 자동으로 `Content-Type: application/json`과 `X-Requested-With: fetch`를 붙이도록 했다.
- CORS 허용 헤더에 `X-Requested-With`를 추가했다.
- `batch-status`의 systemd polling을 `asyncio.to_thread()`로 분리했다.
- `deploy-status`의 systemd runner/service 조회도 event loop 밖으로 분리했다.
- 수동 job trigger, 외국 배당 refresh/upsert/delete, 우선주 배당 refresh에 운영 이벤트 기록을 추가했다.

### 문서화

- `docs/nav-trend-performance.md`: NAV 그래프 장애/성능 원인, 적용한 차트 교체 방향, 검증 기준, 남은 관찰 포인트.
- `docs/project-health-review.md`: 전체 구조, 규모, 문제 지점, 이번 반영 사항, 다음 리팩토링 순서.

## 4. 검증 결과

### 자동 검증

- JS syntax check:
  - `static/js/portfolio-trend-chart.js`
  - `static/js/portfolio.js`
  - `static/js/app-main.js`
  - `static/js/utils.js`
- Python compile:
  - `routes/admin.py`
  - `main.py`
  - `cache.py`
- 주요 부분 테스트:
  - `tests/test_main.py`
  - `tests/test_foreign_dividends.py`
  - `tests/test_observability.py`
  - `tests/test_portfolio.py`
  - `tests/test_portfolio_asset_insights.py`
- 전체 테스트:
  - `290 passed`

### UX 관점 검증

- 인증된 운영 DB의 실제 포트폴리오 데이터는 로컬 `cache.db`에 없어, 실제 사용자의 `/api/portfolio/nav-history` 응답 시간은 로컬에서 재현하지 못했다.
- 대신 프론트 렌더 병목을 분리해 synthetic canvas 렌더를 검증했다. 420개 포인트와 비교지수 1개 기준 draw/zoom/setOption/resize 경로가 약 14ms로 끝났다.
- 이제 사용자가 느끼는 첫 대기 시간은 주로 `nav-history` API 응답과 브라우저 정적 asset 갱신 여부에 좌우된다. 차트 라이브러리 다운로드는 NAV/평가금액 추이의 필수 경로에서 제거되었다.

## 5. 다음 리팩토링 순서

### 1순위: 포트폴리오 프론트 분리

- `portfolio.js`를 다음 모듈로 나눈다.
  - `portfolio-state.js`: 전역 상태, 필터, 통화, 선택 그룹.
  - `portfolio-table.js`: 보유종목 표/카드/행 렌더.
  - `portfolio-menu.js`: 종목 클릭 메뉴, 인사이트 팝업, 외부 링크.
  - `portfolio-trends.js`: NAV/평가금액/수익률 카드.
  - `portfolio-cashflows.js`: 자금 입출금 표/폼.
  - `portfolio-tags.js`: 투자 아이디어 태그.
- 이 분리가 끝나야 UI 효과 수정이 실시간 시세나 그래프를 망치는 식의 사이드이펙트를 크게 줄일 수 있다.

### 2순위: DB 계층 분리

- `cache.py`를 도메인별 repository로 분리한다.
  - `repositories/users.py`
  - `repositories/portfolio.py`
  - `repositories/analysis_cache.py`
  - `repositories/wiki.py`
  - `repositories/admin_settings.py`
  - `repositories/events.py`
- 다중 statement write는 명시적 transaction helper로만 실행하게 한다.

### 3순위: 관리자 job run 모델

- 현재 수동 job은 이벤트 로그는 남지만, “job run” 자체는 아직 독립 테이블이 아니다.
- `admin_job_runs`를 만들어 job id, actor, started_at, ended_at, status, stdout tail, stderr tail, retry/cancel 정보를 저장해야 한다.
- 관리자 UI는 fire-and-forget 알림 대신 run detail 화면을 가져야 한다.

### 4순위: 프론트 테스트 최소 도입

- 빌드 시스템을 크게 들이지 않더라도, 핵심 JS 모듈에는 Node 기반 contract test를 붙인다.
- 특히 다음은 회귀 테스트가 필요하다.
  - NAV Y축 min/max 계산.
  - 비교지수 왼쪽 시작점 정규화.
  - portfolio row click과 price flash가 서로 이벤트를 방해하지 않는지.
  - 관리자 mutation fetch header.

### 5순위: 운영 성능 계측

- `/api/portfolio/nav-history`, `/api/portfolio/benchmark-history`, `/api/admin/*` 주요 endpoint에 duration event를 남긴다.
- 화면에서는 “데이터 로딩 중”, “차트 렌더링 중”, “비교지수 불러오는 중”을 분리해서 보여준다.
- 사용자가 느끼는 문제를 다음부터는 추측이 아니라 endpoint별 시간으로 바로 분리한다.
