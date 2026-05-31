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

### 완료: 포트폴리오 프론트 1차 분리

- `portfolio.js`를 다음 실행 순서형 모듈로 나눴다.
  - `portfolio-shell.js`: 공유 상태, 컬럼 설정, 뷰 전환.
  - `portfolio-data.js`: 포트폴리오 로딩, 필터/정렬, row quote update.
  - `portfolio-render.js`: 보유종목 렌더링, 포맷, sparkline.
  - `portfolio-actions.js`: 편집/삭제/검색/그룹/benchmark/외부 링크 액션.
  - `portfolio-insights.js`: 투자 인사이트, 태그, linked dashboard helper.
  - `portfolio-groups-market.js`: 그룹 모달, market bar, CSV, 통화.
  - `portfolio-ai.js`: 포트폴리오 AI 분석.
  - `portfolio-performance.js`: 추이 탭 shell, 영역지도, 성과 데이터 로딩.
  - `portfolio-trends.js`: NAV/평가금액 추이와 수익률 카드.
  - `portfolio-cashflows.js`: 자금 입출금.
  - `portfolio-events.js`: delegated event handlers.
- 2026-04-30에 classic script 실행 순서를 보존하는 방식으로 1차 분리를 완료했다. 실제 파일 경계는 `docs/portfolio-frontend-structure.md`를 기준으로 관리한다.
- 다음 단계는 전역 공유 상태를 줄이고, domain별 contract test를 추가하면서 더 작은 순수 함수 단위로 이동하는 것이다.

### 1순위: DB 계층 분리

- `cache.py`를 도메인별 repository로 분리한다.
  - `repositories/users.py`
  - `repositories/portfolio.py`
  - `repositories/analysis_cache.py`
  - `repositories/wiki.py`
  - `repositories/admin_settings.py`
  - `repositories/events.py`
- 다중 statement write는 명시적 transaction helper로만 실행하게 한다.

### 2순위: 관리자 job run 모델

- 현재 수동 job은 이벤트 로그는 남지만, “job run” 자체는 아직 독립 테이블이 아니다.
- `admin_job_runs`를 만들어 job id, actor, started_at, ended_at, status, stdout tail, stderr tail, retry/cancel 정보를 저장해야 한다.
- 관리자 UI는 fire-and-forget 알림 대신 run detail 화면을 가져야 한다.

### 3순위: 프론트 테스트 최소 도입

- 빌드 시스템을 크게 들이지 않더라도, 핵심 JS 모듈에는 Node 기반 contract test를 붙인다.
- 특히 다음은 회귀 테스트가 필요하다.
  - NAV Y축 min/max 계산.
  - 비교지수 왼쪽 시작점 정규화.
  - portfolio row click과 price flash가 서로 이벤트를 방해하지 않는지.
  - 관리자 mutation fetch header.

### 4순위: 운영 성능 계측

- `/api/portfolio/nav-history`, `/api/portfolio/benchmark-history`, `/api/admin/*` 주요 endpoint에 duration event를 남긴다.
- 화면에서는 “데이터 로딩 중”, “차트 렌더링 중”, “비교지수 불러오는 중”을 분리해서 보여준다.
- 사용자가 느끼는 문제를 다음부터는 추측이 아니라 endpoint별 시간으로 바로 분리한다.
- (진행됨) 2026-05-31에 endpoint별 계측의 1차를 깔았다. 아래 7번 메모 참조.

## 6. 2026-05-28 재점검 메모

이번 재점검에서도 변경 비용의 핵심 원인은 보안보다 결합도와 경계 부재였다.

- `routes/portfolio.py`는 파일 분리가 일부 진행됐지만 여전히 quote, benchmark, AI, cashflow, snapshot orchestration을 함께 가진다. 라우터는 HTTP 입력/출력만 담당하고 나머지는 service로 밀어내야 한다.
- `cache.py`는 스키마, migration, repository, 일부 도메인 규칙이 섞여 있다. repository 분리 전 단계로 cashflow와 `CASH_KRW` 잔액 갱신은 단일 transaction 함수로 묶었다.
- 프론트는 `portfolio.js` 단일 파일은 해소됐지만 classic script 순서와 전역 상태가 여전히 실제 의존성 계약이다. 파일 분리 다음 단계는 store/namespace 도입과 동작 기반 테스트다.
- 로컬 산출물(`.venv`, `.claude`, `server.*.log`, repair marker)이 작업 트리를 오염시켜 검색과 리뷰 비용을 키웠다. `.gitignore`를 현재 운영 산출물에 맞춰 확장했다.
- `/api/portfolio/quotes` SSE 경로에는 `asyncio.create_task(asyncio.gather(...))` 형태의 비동기 task 오류 가능성과 disconnect 시 pending task 정리 누락이 있었다. gather 사용과 cleanup을 보강했다.
- `services/stock_quotes.py`를 추가해 국내 주식 현재가 조회는 이 service를 통해서만 수행하게 했다. 이 service가 `Stock` 모델, 캐시, WS cache 조회, REST fallback, 지속 callback 구독을 관리한다.
- 테스트는 44개 Python 테스트 파일 규모까지 늘었다. `pytest.ini`로 테스트 경로와 async 모드를 명시해 실행 환경 drift를 줄였다.

근본 해결 순서는 다음이 현실적이다.

1. `routes/portfolio.py`에서 cashflow, quote, history를 service 단위로 먼저 추출한다. 국내 주식 현재가는 `services.stock_quotes`로 1차 강제 분리했다.
2. `cache.py`에 transaction helper를 만든 뒤 portfolio item update처럼 함께 성공/실패해야 하는 write를 추가로 묶는다. cashflow와 `CASH_KRW` 동기화는 2026-05-28에 전용 transaction 함수로 먼저 처리했다.
3. 배치 스크립트가 route private 함수가 아니라 public service API를 호출하게 바꾼다. 2026-05-28에 `services.portfolio.runtime_quotes` provider seam을 추가해 `snapshot_nav.py`, `snapshot_intraday.py`, `routes/wiki.py`, `foreign_dividends.py`의 직접 의존은 제거했다. 실제 quote 구현은 아직 `routes.portfolio`에 남아 있으므로 다음 단계에서 `QuoteService`로 완전히 옮겨야 한다.
4. 프론트는 ES module 전환을 한 번에 하지 말고 `portfolioStore` 같은 단일 namespace부터 만들고, 기존 전역 변수 접근을 점진적으로 줄인다.
5. 문자열 존재 테스트는 최소 유지하고 quote stream, cashflow mutation, portfolio render 같은 사용자 흐름 중심의 contract test를 늘린다.

## 7. 2026-05-31 메모: 요청 계측 + 흐름 통합 테스트

5번 항목(사용자 흐름 contract test)과 4순위(운영 성능 계측)를 함께 진행했다.

- **요청 latency 계측 미들웨어**를 `core/app_factory.py`에 추가했다(`_RequestLatencyMiddleware`).
  - `/api/*` 요청만 계측하고, 느린 요청(`>= app.state.slow_request_ms`, 기본 1000ms, env `SLOW_REQUEST_MS`)과 5xx 에러만 `observability.record_event(source="http", ...)`로 `system_events`에 남긴다. 정상·빠른 트래픽은 한 줄도 쓰지 않아 quote 폴링이 테이블을 채우지 않는다.
  - `BaseHTTPMiddleware`가 아니라 **순수 ASGI 미들웨어**로 구현했다. SSE(`/api/portfolio/quotes`, 분석 스트림) 응답을 버퍼링하지 않고, latency는 `http.response.start`까지의 time-to-first-byte로 측정해 장시간 스트림이 “느림”으로 오분류되지 않는다.
  - 기록은 fire-and-forget이라 응답을 막지 않고, 실패해도 요청을 깨지 않는다.
  - 다음 단계: 관리자 대시보드가 `(source="http")` 이벤트를 endpoint별 p95/에러율로 렌더하면, “느린 endpoint”를 추측이 아니라 데이터로 본다.
- **흐름 통합 테스트**를 `tests/test_integration_flows.py`에 추가했다(9 케이스). 라우트 핸들러를 직접 부르는 기존 단위 테스트와 달리, `create_app()`을 임시 SQLite로 띄우고 `httpx.ASGITransport`로 실제 HTTP 스택(라우팅·미들웨어·직렬화·핸들러→repository→DB)을 통과시킨다. lifespan만 건너뛰고(KIS/DART client 기동 회피) 인증·백그라운드 워밍업만 스텁한다.
  - 포트폴리오 종목 GET/DELETE 왕복과 재삭제 404.
  - 현금흐름 입금/출금 ↔ `CASH_KRW` 잔액 동기화, 잔액 부족 시 400과 트랜잭션 롤백(커밋된 현금흐름 2건만 남음).
  - 미들웨어: 느린 요청 기록 / 빠른 요청 미기록 / 비-API 미계측 / 5xx 에러 기록 / 실제 `system_events` 영속화(목 없이) / 스트리밍 청크 무버퍼 통과.
