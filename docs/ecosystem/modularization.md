# 중복 정리와 모듈화

작성일: 2026-09-30

허브와 형제 저장소에서 찾은 중복, 이번 작업(허브 `ecosystem-2026-09`, 형제 `vc-ecosystem-2026-09`)에서 합친 것,
공유 코드를 나눠 주는 방법, 남은 백로그를 정리한다.

---

## 1. 배포 메커니즘 — 공유 코드를 나눠 주는 네 가지 방법

독립 배포 원칙(허브가 내려가도 형제가 깨지지 않는다) 때문에 공유 코드는 아래 넷 중 하나로만 나간다.

| 방식 | 대상 | 독립성 | 현재 예 |
|---|---|---|---|
| **A. 벤더링 사본 + 검증기** | 브라우저 셸·토큰·작은 stdlib 헬퍼 | 사본이 각 저장소에 커밋된다 | `scripts/sync-ecosystem.mjs`(vc-shell·vc-tokens·theme-boot·vc_publish), `scripts/sync-analytics.mjs`(analytics.js) |
| **B. 공개 데이터 계약** | 도구 간 요약 데이터, 계산 결과 | 소비자는 모르는 필드를 무시하고 폴백을 유지 | `summary.json` envelope v1([data-contract.md](data-contract.md)), research v1 카탈로그 |
| **C. 서비스 경유 + 로컬 폴백** | KIS·Naver·Yahoo 시세, EOD, 매크로 | 서비스가 죽어도 폴백 체인이 있다 | kis-proxy `/v1/*`, finance-pi `/api/*`, 허브 `/api/internal/notify` |
| **D. 태그 고정 pip 의존성** | GitHub Actions의 Python 파이프라인 | 태그를 올리기 전까지 영향 없음 | fin-commons `@v0.2.0`(holding_value, cps, spac-hunter, nps-tracker) |

허브 핫링크(`portfolio-held-badges.js`)는 "없어도 화면이 깨지지 않는 장식"에만 허용한다.

---

## 2. 찾은 중복

### 2-1. 저장소 간

| # | 중복 | 규모 | 상태 |
|---|---|---|---|
| X1 | `price_revisions.py` | holding_value = cps(바이트 동일) | 남음(fin-commons v0.3) |
| X2 | KIS 부호 코드 파싱 | holding_value·cps·JS·kis-proxy, 의미가 이미 갈라짐 | 남음 |
| X3 | KIS 토큰 발급·캐시 | 5곳(holding_value, cps, nps-tracker 2, finance-pi) + kis-proxy | 남음 |
| X4 | finance-pi 클라이언트 | 5개 저장소, env 이름 5종 | 허브 안은 단일화. 저장소 간 남음 |
| X5 | OpenDART 클라이언트 | 6개 저장소 | 허브 안은 단일화. 저장소 간 남음 |
| X6 | Yahoo chart / yfinance | 10곳 이상 | 허브 안은 단일화 |
| X7 | Naver 실시간 폴링 | 5곳 | 브라우저는 kis-proxy로 모임. 서버 파이프라인 남음 |
| X8 | 우선주 배당 Google Sheet | 같은 시트, 파서 2개(허브·cps) | 남음 |
| X9 | 히스토리 병합·증분 창·split 출력 | holding_value·cps·spac-hunter | 남음 |
| X10 | SPAC 청산가치 계산 | Python·JS·허브 Python 3중 | 부분: spac-hunter summary가 `currentLiquidationValue`를 발행하고 허브는 당일 값이면 그걸 쓴다 |
| X11 | 지주사 NAV 비율 | 5곳 이상(허브 사본은 `holdingAdjustedShares` 무시) | 남음 |
| X12 | 프런트 셸(테마 부트·embed·허브 링크·토큰) | 형제 10개 + 허브 | **완료**(vc-shell·vc-tokens·theme-boot) |
| X13 | `analytics.js` | 11부 | 관리되는 중복(sync-analytics) |
| X14 | `portfolio-held-badges.js` 버전 태그 | `?v=` 2종 혼재 | **완료**(레지스트리 `heldBadges.version` 하나, sync가 검증) |
| X15 | GitHub Actions 커밋·재시도·실패 이슈 블록 | 8개 이상 워크플로 | 남음(composite action) |
| X16 | KRX 거래일·장 시간 | 허브·finance-pi 달력 2벌 + 평일 판정 여러 곳 | 부분: 2027 목록 일치, 테스트가 대조 |
| X17 | 형제 목록(허브 안) | `integrations`·`external_tools`·`linked_project_admin`·`routes/portfolio`·JS 등 9곳 | **완료**(`config/ecosystem.json`) |

### 2-2. 허브 내부

| # | 중복 | 상태 |
|---|---|---|
| H1 | 로그인 가드 16개(호출 93곳) | **완료** — `deps.require_user` / `require_user_id` |
| H2 | KST 정의 27곳 | **완료** — `domain/timeutil.py`(`KST`, `now_kst`, `today_kst`) |
| H3 | 숫자 파서 23개 | 1단계 완료 — `domain/numbers.parse_number`(본문이 같은 사본만). 의미가 다른 파서는 남음 |
| H4 | Yahoo chart 준동일 2개 + 호출 5곳 | **완료** — `services/market/sources/yahoo.py` |
| H5 | 도달 불가 레거시 Naver 스크레이퍼 약 250줄 | **완료**(삭제, 회귀 테스트) |
| H6 | finance-pi 요청 경로 2벌(close_price + quant) | **완료** — `services/market/sources/finance_pi.py`(quant는 쿨다운 무시 유지) |
| H7 | KR 일봉 폴백 3중(`kis_proxy`, `portfolio/history`, `stock_price`) | 남음 |
| H8 | 시세 보완 파이프라인(`routes/portfolio` ↔ `device_summary`) | 남음 |
| H9 | 형제 레지스트리 | 대부분 완료. `integrations._base_url`/`DEFAULT_BASE_URLS`와 `core.ecosystem.resolved_url`이 공존 |
| H10 | CSS 토큰 리더·다크 판정 | **완료** — `utils.js` `cssToken`, `isDarkTheme` |
| H11 | null-safe 숫자 래퍼(fmtNum 계열) | 남음 |
| H12 | admin `_esc`(따옴표 미이스케이프) | **완료** — `escapeHtml` 위임 |
| H13 | 테마 토글 2종(search·admin) | 부분 — 허브 SPA는 theme-boot 규칙, admin은 `safeStorage*` 사용. admin의 vc 토큰·theme-boot는 남음 |
| H14 | iframe 임베드 코드 2벌(NPS·bonds) | 부분 — 메시지 브리지는 `ecosystem-links.js`로 모임. 프레임 생성 코드는 2벌 |
| H15 | market-summary 이중 캐시(시장 바 ↔ 대시보드) | 남음 |
| H16 | Python KRW 포매터 4벌 | 남음(문구가 바뀌므로 골든 출력 합의 필요) |
| H17 | fetch → TTL → stale 보일러플레이트 | **완료** — `cache_layer.cached_fetch`(뉴스·지표·테이프·DART·형제). movers·economic_calendar 등 일부 남음 |
| H18 | 스케줄러 이중(lifespan 루프 ↔ timer) | 남음 |
| H19 | 복구 CLI 스캐폴딩 | 남음 |
| — | 폴링 타이머 | **완료** — `utils.js` `schedulePoll`/`cancelPoll`(숨김 탭 정지, 중복 실행 방지) |
| — | storage 접근 | **완료** — `utils.js` `safeStorageGet/Set/Remove` |

---

## 3. 이번 작업에서 합친 것(파일 경로)

| 영역 | 새 위치 | 대체한 것 |
|---|---|---|
| 도구 레지스트리 | [config/ecosystem.json](../../config/ecosystem.json), [core/ecosystem.py](../../core/ecosystem.py) | `integrations.DEFAULT_BASE_URLS`, `external_tools.SITE`/`_RAW`, `linked_project_admin.PROJECT_SPECS`의 URL, handoff 허용 목록(Python·JS), index-popup·bond-mate URL 상수 |
| 링크 조립 | [services/ecosystem/links.py](../../services/ecosystem/links.py) | `/api/portfolio/open` 빌더, 액션보드·인사이트의 손 조립 URL |
| 형제 JSON 로딩 | [services/ecosystem/siblings.py](../../services/ecosystem/siblings.py), `envelope.py`, `adapters.py`, `fetch.py` | 형제별 raw fetch와 무캐시 액션보드 fetch |
| 발행 헬퍼 | [ecosystem/python/vc_publish.py](../../ecosystem/python/vc_publish.py), [ecosystem/js/vc-publish.mjs](../../ecosystem/js/vc-publish.mjs) | 형제별 JSON 쓰기·변경 감지 |
| 공용 셸 | [static/ecosystem/](../../static/ecosystem/) `vc-shell.js`, `vc-tokens.css`, `vc-theme-boot.js` | 형제 10개의 허브 링크 마크업, 테마 부트 7종, 레거시 테마 키 |
| 동기화기 | [scripts/sync-ecosystem.mjs](../../scripts/sync-ecosystem.mjs) | 손 복사 |
| 외부 provider | [services/market/sources/](../../services/market/sources/) `yahoo`, `yfinance_runner`, `finance_pi`, `close_price`, `kis_proxy` | 루트 `close_price_client.py`, `kis_proxy_client.py`, Yahoo 파서 5벌, yfinance 실행기 3종 |
| 캐시 | [cache_layer.py](../../cache_layer.py) `cached_fetch`, `SingleFlight` | 모듈별 try/set/stale 보일러플레이트 |
| DART | [services/dart/client.py](../../services/dart/client.py) `fetch_filing_list` | `list.json` 조립 4곳 |
| 도메인 헬퍼 | [domain/timeutil.py](../../domain/timeutil.py), [domain/numbers.py](../../domain/numbers.py), [deps.py](../../deps.py) | KST 27곳, 동일 파서, 로그인 가드 16개 |
| 프런트 헬퍼 | `static/js/utils.js`, `static/js/ecosystem-links.js` | CSS 토큰 리더 5벌, 폴링 `setInterval`, storage 직접 접근, iframe 메시지 처리 |
| 레거시 루트 모듈 16개 | `services/` 아래(아래 표) | 루트 `*.py` 32 → 15 |

레거시 루트 모듈 이동(각 커밋은 `git mv`, 호출부는 옛 이름을 alias로 import):

| 옛 루트 모듈 | 새 위치 |
|---|---|
| `linked_project_admin` | `services/ecosystem/linked_admin.py` |
| `integrations` | `services/ecosystem/integrations.py` |
| `external_tools` | `services/ecosystem/external_tools.py` |
| `market_movers` | `services/market/movers.py` |
| `market_daily` | `services/market/daily.py` |
| `economic_calendar` | `services/market/economic_calendar.py` |
| `market_indicators` | `services/market/indicators.py` |
| `close_price_client` | `services/market/sources/close_price.py` |
| `kis_proxy_client` | `services/market/sources/kis_proxy.py` |
| `dart_client` | `services/dart/client.py` |
| `preferred_dividends` | `services/dividends/preferred.py` |
| `foreign_dividends` | `services/dividends/foreign.py` |
| `snapshot_intraday` | `services/portfolio/intraday_snapshot.py` |
| `snapshot_nav` | `services/portfolio/nav_snapshot.py`(루트 `snapshot_nav.py`는 `deploy/repairs/`용 shim) |
| `benchmark_history` | `services/portfolio/benchmark_history.py` |
| `stock_price` | `services/stock_price.py` |

`tests/test_legacy_root_modules.py`가 옮긴 모듈이 루트에 다시 생기거나 옛 이름으로 import되면 실패한다.
로거 이름은 모듈 경로를 따르므로 로그에는 `services.market.movers` 같은 새 이름이 찍힌다.

남은 루트 모듈 13개: `ai_config`, `analyzer`, `asset_insights`, `auth_service`, `cache_layer`, `dart_report_review`, `deps`,
`dr_registry`, `kis_key_manager`, `kis_ws_manager`, `observability`, `report_client`, `wiki_ingestion`(+ `main.py`, `snapshot_nav.py` shim).

---

## 4. 남은 백로그(가치/위험 순)

| 순위 | 항목 | 이유 | 위험 | 비고 |
|---|---|---|---|---|
| 1 | KRX 휴장일 JSON 정본 + 허브 평일 판정 통합(X16) | 정확성. 2028 목록이 2027-11까지 필요 | 중 | 허브 `config/krx-holidays.json` → finance-pi·fin-commons로 동기화 |
| 2 | fin-commons v0.3(X1, X2, X9, X15) | 바이트 동일·준동일 코드 제거, 워크플로 블록 8개 | 낮음~중 | 태그 발행은 소유자 승인 필요 |
| 3 | 공용 finance-pi·OpenDART 클라이언트(X4, X5) | 클라이언트 11개, env 5종 | 중 | fin-commons v0.4 |
| 4 | 일봉 폴백 한 곳으로(H7) | 미스 한 번에 finance-pi 최대 3회 왕복 | 중 | `finance-pi → kis-proxy → yahoo` 체인 하나 |
| 5 | 데이터 계약·테스트 벡터(X10, X11, X8) | SPAC·지주사 비율·우선주 DPS를 여러 곳에서 계산 | 중 | 방식 B |
| 6 | KIS 접근을 kis-proxy로 수렴(X3) | 토큰 5개가 같은 앱키로 서로 무효화 가능 | 중상 | kis-proxy 토큰 버킷 선행 |
| 7 | 남은 루트 모듈 이전 | 신규 코드 위치 혼동 | 중 | 쉬운 것: `analyzer`, `dr_registry`, `kis_key_manager`, `asset_insights`, `dart_report_review`, `report_client`, `wiki_ingestion`. `cache_layer`·`observability`·`deps`·`auth_service`는 core/services 결정 먼저 |
| 8 | `services/stock_price.py`(약 1,330줄)·`services/market/indicators.py`(약 1,460줄) 분할 | 관심사 혼재, 다른 모듈이 private 헬퍼 약 15개 import | 중 | 공개 API 승격 먼저 |
| 9 | `integrations._base_url` → `core.ecosystem.resolved_url`, JS 하드코딩 폴백 제거 | 레지스트리 잔재 | 낮음 | `APP_CONFIG.ecosystem`이 항상 있다는 전제 |
| 10 | 숫자 파서 2단계, fmtNum 래퍼(H3, H11) | 의미가 다른 파서 약 20개 | 낮음 | 옵션을 명시해 이전 |
| 11 | 시장 바 ↔ 대시보드 공용 시세 캐시(H15) | 60초마다 겹치는 요청 | 낮음 | `market-store.js` |
| 12 | 스케줄러 이중(H18), 복구 CLI(H19), KRW 포매터(H16) | 운영 위생 | 낮음 | H16은 문구 합의 필요 |
| 13 | `snapshot_nav.py` shim 삭제 | 복구 마커가 모든 환경에서 실행된 뒤 | 낮음 | — |
| 14 | 형제 테스트의 벤더링 버전 고정 방식 통일 | 셸 버전을 올릴 때마다 여러 형제 테스트가 깨짐 | 낮음 | spac-hunter처럼 사본의 `VERSION`을 읽는다 |

### 하지 않을 것

- 형제가 허브 API·JS를 **필수 런타임 의존성**으로 쓰게 만들지 않는다.
- 벤더링 사본에 도메인 로직을 싣지 않는다. 셸·토큰·작은 헬퍼만. 계산은 데이터 계약과 벡터로 맞춘다.
- `deploy/repairs/*`의 실행 완료된 복구 스크립트는 리팩터링하지 않는다.
- 사용자에게 보이는 알림·브리핑 문구는 골든 출력 합의 없이 일괄 교체하지 않는다.
