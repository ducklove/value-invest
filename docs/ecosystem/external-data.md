# 외부 데이터 인벤토리

작성일: 2026-09-30 · 범위: 허브 + 연결 대시보드 9개 + 인프라(kis-proxy, finance-pi, index-popup, portfolio-epaper, morning-bell)

외부 소스를 누가, 어떤 인증 env로, 얼마나 자주, 어떤 캐시로 부르는지 정리한다. 인증은 **env 이름만** 적는다.
호출량 추정치는 분석 시점(2026-09 하순) 코드와 `gh run list` 관측에서 나온 값이다.

- 공개 JSON 계약: [data-contract.md](data-contract.md)
- 전체 구조: [architecture.md](architecture.md)
- 후속 과제: [roadmap.md](roadmap.md)

---

## 1. 소스 × 프로젝트 매트릭스

범례: **●** 직접 호출(주 소스) · **○** 폴백·보조 · **p** kis-proxy 경유 · **f** finance-pi 경유 · **b** 브라우저에서 호출

| 외부 소스 | 허브 | finance-pi | kis-proxy | index-popup | holding_value | cps | spac-hunter | buybacks | eiayn | gold_gap | all-about-gold | nps-tracker | bond-mate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| KIS Open API (REST/WS) | ● WS · p | ● | ● | p | p(b) · ○ | p(b) · ○ | p(b) | p | | | | ● | |
| Naver 실시간 폴링 | ● | | ● | | p(b) | ● · p(b) | ● · p(b) | p(b) | | | | | |
| Naver api.stock / 모바일 JSON | ● | ● | | | | | | ● | ● | ● | | | ● |
| Naver PC HTML·fchart·siseJson | ● | ○ | ● | | | ● | ● | | ● | | | | |
| Naver 리서치 API + PDF | ● | | | | | | | | | | | | |
| Yahoo v8 chart | ● | ● | ● | ● | | | | | ● | | f | | |
| yfinance 라이브러리 | ● | | | | ● | ● | | | | ● | | ● | |
| OpenDART | ● | ● | | | ● | | ● | ● | | | | ● | |
| DART 웹뷰어 | ● | ○ | | | | | ● | ● | | | | | |
| KRX · KIND · pykrx · data.go.kr · KOSIS · FnGuide | | ○ | | | | | ● | | | | | ● | |
| ECOS | ● | ● | | | | | | | | | | | ● |
| BIS · FRED · NY Fed · BOK · BOJ · MOF | ● | ● | | | | | | | | | | | ● |
| CNBC quote | ● | ● | | | | | | | | | | | ● |
| Upbit · Bithumb · Binance · Hyperliquid | ● | | | ● | | | | | | ● · b | | | |
| WGC · gold-api · World Bank · USGS · DBnomics | | ● | | | | | | | | ● · b | f | | |
| SEC EDGAR · StockAnalysis · KOFR · Zeroin | ● Zeroin | | | | | | ● KOFR | | ● SA | | | | ● EDGAR |
| Google Sheets(우선주 DPS) | ● | | | | | ● | | | | | | ● | |
| 형제 Pages/raw JSON | ● | ● research | | | | | | | | | | | |
| OpenRouter | ● | | | | | | | | | | | | |
| Telegram · Kakao · ntfy | ● | ○ | | | ● | ○ | ● | 허브 notify | | ○ | | | |

portfolio-epaper와 morning-bell은 OpenRouter를, morning-bell은 Polymarket·ntfy를 따로 부른다. 허브와 형제 10개는
GA4(`analytics.js`)를 쓴다.

---

## 2. 소스별 상세

### 2-1. 시세·일봉

| 소스 | 엔드포인트 | 인증 env | 호출자 · 주기 | 캐시 |
|---|---|---|---|---|
| **kis-proxy** | `/v1/stocks/{c}/quote\|history\|financials\|dividends\|overview`, `/v1/indexes/{m}/quote\|intraday\|history`, `/v1/futures/kospi-night/near-month/*`, `/v1/overseas/{ex}/{s}/quote`, `/v1/naverfinance/stocks/quotes?symbols=`(≤100, `^[0-9A-Z]{6}$`), `/v1/yfinance/*` | 호출자: `KIS_PROXY_TOKEN` → `X-KIS-Proxy-Token` · 프록시: `KIS_APP_KEY{N}`/`KIS_APP_SECRET{N}`, `KIS_PROXY_PUBLIC_TOKENS` | 허브 서버(4 req/s 리미터), 형제 Actions(holding_value·buybacks), 형제 브라우저(holding_value·cps·spac-hunter·buybacks), index-popup | 프록시: quote 계열 `KIS_PROXY_QUOTE_CACHE_SECONDS`(기본 2초) + single-flight, 야간선물 근월물 해석 캐시. 허브: financials·dividends 60분 |
| **finance-pi** | `/api/prices/close\|daily`, `/api/macro/*`, `/api/fundamentals/*`, `/api/research/*`, `/api/ready` | `FINANCE_PI_API_TOKEN`(구 이름 `CLOSE_PRICE_API_TOKEN`) → `X-Admin-Token` | 허브(요청 시), cps·holding_value(LAN 실행 때만), bond-mate, all-about-gold 발행기 | 허브: 실패 시 60초 쿨다운(quant는 쿨다운 무시) |
| **Naver 실시간 폴링** | `polling.finance.naver.com/api/realtime/domestic/stock/…` | 없음 | 허브 벌크(40개 청크), cps Actions(종목별), kis-proxy | 허브 `stock_quotes` 60초 + 벌크 5초 마이크로 캐시 |
| **KIS 일봉 직접** | `inquire-daily-itemchartprice` | `KIS_APP_KEY`/`KIS_APP_SECRET` | finance-pi(유니버스 약 4,300/일), nps-tracker(약 1,000/일, 자체 토큰) | 각자 파일 캐시 |
| **Yahoo v8 chart** | `query1.finance.yahoo.com/v8/finance/chart/{sym}` | 없음 | 허브(해외 시세·히스토리·인트라데이·배당·지표 폴백·`^TNX`/`GC=F`/`CL=F`), finance-pi, kis-proxy, index-popup, eiayn | 허브: 호출 모듈별 TTL, 호스트 단위 동시성 제한·429 쿨다운 |
| **yfinance** | 라이브러리 | 없음 | 허브(`stock_price`, 해외, 벤치마크, 해외 배당, 품질 점검), holding_value, cps, gold_gap, nps-tracker | 허브: 4-스레드 실행기 + 실패 60초 skip |

### 2-2. 공시·펀더멘털

| 소스 | 엔드포인트 | 인증 env | 호출자 · 주기 | 캐시 |
|---|---|---|---|---|
| **OpenDART** | `corpCode.xml`, `list.json`, `fnlttSinglAcnt(All)`, `alotMatter`, `stockTotqySttus`, `tesstkAcqsDspsSttus`, `majorstock`, `company`, `document.xml` 등 | `OPENDART_API_KEY`(허브·finance-pi·spac-hunter), `DART_API_KEY`(holding_value·buybacks·nps-tracker). 모두 `crtfc_key` 쿼리 | 허브(테이프·리뷰·알림·분석), buybacks 월간 전수 스캔, spac-hunter 일간, finance-pi 일간, holding_value 주간, nps-tracker 일간 | 허브: `list.json` 10분 캐시 + 일일 쿼터 가드(§4). corpCode는 저장소마다 따로 받는다 |
| **DART 웹뷰어** | `dsaf001/main.do`, `report/viewer.do` | 없음 | 허브, spac-hunter, buybacks | 파일 캐시(CI에서는 유지 안 됨) |
| **KRX · KIND · pykrx** | `data.krx.co.kr`, `kind.krx.co.kr` | `KRX_ID`/`KRX_PW`, `KRX_OPENAPI_KEY` | spac-hunter, nps-tracker, finance-pi | 부분적 |
| **Naver 리서치** | `stock.naver.com/api/stockSecurity/researches/…`, PDF | 없음 | 허브만 | DB `cache_values`. 새로고침 때 이미 받은 `nid` 상세는 다시 부르지 않는다 |

### 2-3. 거시·금리·FX·원자재

| 소스 | 인증 env | 호출자 | 허브 TTL |
|---|---|---|---|
| ECOS | `ECOS_API_KEY`(URL 경로에 포함) | 허브, bond-mate, finance-pi | 1800초(일 단위 그룹) |
| BIS 정책금리 | 없음 | 허브(월간), bond-mate(일간) | 21600초(월 단위 그룹) |
| FRED | `FRED_API_KEY`(finance-pi) | 허브(DFEDTARU 폴백, 공유 `fred` 클라이언트), bond-mate, finance-pi | 1800초 |
| NY Fed · MOF · BOJ · BOK 페이지 | 없음 | 허브, bond-mate(MOF) | 1800초 |
| CNBC quote | 없음 | 허브, bond-mate, finance-pi | 60초(장중 그룹) |
| Naver FX (`api.stock.naver.com/marketindex/exchange`) | 없음 | 허브(단일 경로 `services/portfolio/fx.py`), bond-mate | 지표 60초 · 포트폴리오 환산 300초, 같은 응답 공유 |
| Upbit | 없음 | 허브(KRW-BTC·ETH·USDT 한 번에), gold_gap | 5초 + 동시 호출 병합 |
| Hyperliquid | 없음 | 허브(REST + 브라우저 WS) | 8초(live) |

### 2-4. 형제 발행 JSON(허브가 소비)

| 발행물 | 크기(분석 시점) | 허브 소비 | 실제로 쓰는 부분 |
|---|---|---|---|
| 각 도구 `summary.json` | 1~16 KB | [siblings.py](../../services/ecosystem/siblings.py): 900초, `If-None-Match`, 404·계약 위반 음성 캐시 900초, 네트워크 오류 시 1일 stale | 카드·신호용 요약 |
| holding_value `current.json`·`config.json` | 수십 KB | summary 실패 시, 900초 | 비율·NAV |
| spac-hunter `data.json` | 약 3.8 MB | summary 실패 시, 900초, 슬림 형태로 캐시 | SPAC별 필드 |
| buybacks `holding_snapshots.json` | 약 1.2 MB | summary 실패 시, 종목별 인덱스로 캐시 | 최신 보통주 비율 |
| eiayn `etfs.json` | 약 9 MB | summary 실패 시, 6시간 | `universe` |
| gold_gap `data.json` | 약 560 KB | Pages, 마지막 값만 캐시 | 자산 4개의 마지막 갭 |
| nps-tracker `current.json` | 약 310 KB | summary 실패 시, 900초 | 요약·상위 N |
| bond-mate `data/current.json` | 약 110 KB | 서버 900초 + 브라우저 직접 병합 | 금리·FX 최신값 |

액션보드 신호·종목 링크·인사이트가 같은 형제 파일을 TTL 안에서 한 번만 받는다(`tests/test_external_tools.py`).

---

## 3. 중복 분석

| 데이터 | 중복 | 결과 |
|---|---|---|
| 한국 종목 EOD 종가 | finance-pi·eiayn·nps-tracker·buybacks·spac-hunter·holding_value·cps가 각자 수집(합계 추정 약 1만 건/일) | finance-pi 레이크에 이미 있는 데이터. GitHub 러너가 LAN API에 닿지 못해 생긴 중복 |
| 장중 시세 | 형제 Actions·브라우저·허브·index-popup이 KIS·Naver를 각자 호출 | kis-proxy에 2초 quote 캐시가 생겼지만 소비자 이전은 미완 |
| USD/KRW | 허브(지금은 1경로), bond-mate, gold_gap, finance-pi, holding_value, eiayn, nps-tracker, index-popup, all-about-gold가 서로 다른 기준(고시·시장·레퍼런스)으로 수집 | 허브 안에서는 대시보드와 포트폴리오 환산이 같은 값을 쓴다. 생태계 전체 기준 태그는 없다 |
| 금리·정책금리 | 허브 `services/market/indicators.py`, bond-mate, finance-pi가 같은 소스(BIS·ECOS·FRED·CNBC·MOF)를 다른 파라미터로 | 허브 브라우저는 이미 bond-mate를 병합한다. 서버 쪽 이전은 미완 |
| OpenDART `corpCode.xml` | 6개 저장소가 따로 받음 | 키를 공유한다면 일일 한도(20,000)를 함께 쓴다 |
| KRX 휴장일 | 허브 `domain/market_calendar.py`와 finance-pi `trading_calendar.py`가 손으로 맞춤 | 2027년까지 같은 목록(테스트가 대조) |

---

## 4. 이번 작업에서 통합한 것

| 항목 | 위치 | 내용 |
|---|---|---|
| provider 계층 | [services/market/sources/](../../services/market/sources/) | `yahoo.py`(chart 파서·호스트 동시성·429 쿨다운, `YahooRateLimitedError`), `yfinance_runner.py`(실행기 하나), `finance_pi.py`(base URL·토큰·공유 클라이언트·쿨다운), `close_price.py`(엔드포인트·파싱), `kis_proxy.py`(공유 `kis_proxy` 클라이언트, 응답 캐시) |
| single-flight 캐시 | [cache_layer.py](../../cache_layer.py) `cached_fetch` / `cached_fetch_result` | 키마다 로드 1회, 실패 시 `stale_ttl` 안의 마지막 값. market-summary·지표·테이프·DART·뉴스·형제 JSON이 사용. `MemoryTTLCache`는 연산당 deepcopy 1회, `copy=False` 읽기, opt-in 만료 정리 |
| 소스별 TTL | `services/market/indicators.py` `SOURCE_GROUP_TTL` | 장중 60초 · 일 단위 1800초 · 월 단위 21600초. 실패·stale 항목은 60초 뒤 재시도 |
| DART list 단일 클라이언트 | [services/dart/client.py](../../services/dart/client.py) `fetch_filing_list` | 10분 캐시(`OPENDART_LIST_CACHE_TTL_S`), 동일 요청 병합, KST 일일 카운트, 비필수 호출은 16,000에서 중단(`OPENDART_NONESSENTIAL_BUDGET`), status `020`이면 KST 자정까지 차단 + `system_events`(dart/quota_exhausted). 테이프는 07~20시에만 공시 조회 |
| DART corpCode 기동 보호 | `services/dart/client.py` | zip이 아닌 응답(오류 본문)은 도메인 오류로 처리해 앱 기동이 죽지 않는다 |
| 시세 중복 제거 | `services/notifications/engine.py`, `services/portfolio/runtime_quotes.py`, `services/portfolio/intraday_snapshot.py`, `services/stock_quotes.py` | 알림 패스·인트라데이 틱이 모든 사용자의 코드를 한 번에 조회해 공유. 휴장일에는 강제 새로고침 생략. 벌크 조회 5초 마이크로 캐시 |
| KIS 응답 캐시 | `services/market/sources/kis_proxy.py` | financials·dividends 60분(`KIS_PROXY_RESPONSE_CACHE_TTL_SECONDS`), 실패·빈 응답은 캐시 안 함. 운영 기본 URL이 루프백 `http://127.0.0.1:3288` |
| FX 단일 경로 | `services/portfolio/fx.py` `fetch_exchange_payload` | 지표 바와 포트폴리오 환산이 같은 USD/KRW |
| 형제 JSON | [services/ecosystem/](../../services/ecosystem/) | summary-first 로더, 레지스트리 URL(env override), 전 fetch single-flight + 1일 stale, 액션보드 캐시 |
| 로그 비밀 마스킹 | [core/logging_setup.py](../../core/logging_setup.py) | `crtfc_key`·`apikey`·`token`·`appkey` 등 쿼리, Telegram 봇 경로, ECOS 키 경로를 로그·예외에서 가린다. httpx/httpcore 로거는 WARNING |
| 기타 배치화 | `services/portfolio/special_assets.py`, `report_client.py`, `services/stock_intraday.py` | Upbit 한 번 호출, 리서치 `nid` 재사용, 인트라데이 전일종가 재사용 |
| 죽은 호출 제거 | `services/market/indicators.py`, `services/portfolio/nav_snapshot.py`, `core/http.py` | 도달 불가 Naver HTML 스크레이퍼, 읽는 곳 없던 gold-api 쓰기, `gold_api` 타임아웃 프로필 |
| 형제 쪽 | 각 저장소 `vc-ecosystem-2026-09` | kis-proxy 근월물 캐시·2초 quote 캐시·레이트리미터 IP 수정·원자적 쓰기, index-popup 요청 캐시·10초 타임아웃·숨김 탭 정지, finance-pi research 매니페스트 캐시·readiness 45초 캐시·gold 무변경 릴리스 생략, 변경 없으면 커밋·배포 생략(gold_gap·bond-mate·all-about-gold·nps-tracker·spac-hunter filings) |

### env 이름 표준(finance-pi)

`FINANCE_PI_BASE_URL`, `FINANCE_PI_API_TOKEN`이 표준이다. 구 이름 `CLOSE_PRICE_API_BASE_URL`, `CLOSE_PRICE_API_TOKEN`도 읽는다.
**둘 다 설정되어 있으면 구 이름이 이긴다**(기존 `.env` 동작 보존). 이전할 때는 구 이름 줄을 지운다. 기본 URL은
`http://192.168.68.84:8400`. 가격·매크로 호출은 지금도 타임아웃이 없다(`timeout=None`, 기존 동작 유지 — 소유자 결정 대기).

---

## 5. 남은 것과 목표 설계

### 5-1. 데이터셋별 단일 소유자

| 데이터셋 | 소유자 | 소비 방식 | 폴백 | 상태 |
|---|---|---|---|---|
| KR 실시간·지연 시세 | **kis-proxy** | 허브(루프백), 형제 브라우저(`:3298`), 형제 Actions | Naver 벌크 직접 → 스냅샷 | 프록시 캐시 완료, 배치 엔드포인트·소비자 이전 미완 |
| 멀티자산 시세(보유 인지) | **허브** `POST /api/asset-quotes` | 허브 SPA·디바이스 | 내부 체인 | 완료 |
| KR 일봉 EOD·펀더멘털·스크리너 | **finance-pi** | 허브(API), GitHub 러너(공개 번들, 미구현) | kis-proxy history → Yahoo | 번들 미구현 |
| 거시 이력 | **finance-pi** | 허브, all-about-gold | Yahoo/FRED 직접 | 부분 |
| 금리·정책금리·FX 이력 | **bond-mate** | 허브 서버·브라우저 | 허브 장중 라이브 코드 | 브라우저만 |
| USD/KRW(환산) | **허브** `services/portfolio/fx.py` | 허브 전체 | Yahoo `KRW=X` | 완료(허브 안) |
| OpenDART | 각 저장소 + 공용 클라이언트 | — | — | 허브만 단일화 |
| 형제 도메인 요약 | **각 형제 `summary.json`** | 허브 | 레거시 JSON → 마지막 캐시 | 완료 |
| KRX 거래일 달력 | 허브 정본 → finance-pi | 허브, finance-pi, 형제 | 내장 표 | 손 동기화 |
| 알림 발송 | **허브** `/api/internal/notify` | buybacks(현재), 다른 형제(미이전) | 각자 직접 발송 | 부분 |

원칙:

1. 데이터셋마다 소유자는 하나다. 소유자가 수집·정규화·캐시·품질 검사를 하고, 소비자는 계약(JSON 스키마)만 본다.
2. 모든 소비자는 "소유자 → 자체 수집 → 마지막 스냅샷" 폴백을 유지한다. 허브나 Pi가 죽어도 형제 Pages는 보인다.
3. LAN 안에서는 API(finance-pi `:8400`, kis-proxy 루프백, 허브 루프백)를, GitHub 러너·브라우저는 공개 정적 번들과
   kis-proxy HTTPS(`:3298`)만 쓴다. finance-pi를 WAN에 노출하지 않는다.
4. 기준을 명시한다: 가격 `adjusted`, FX `basis`, 시각은 `asOf`(데이터 기준)와 `generatedAt`(생성)을 따로.

### 5-2. 남은 백로그

| 항목 | 이유 | 로드맵 |
|---|---|---|
| finance-pi 공개 EOD·매크로·달력 번들 | GitHub 러너의 EOD 재수집(약 1만 건/일) 제거 | R-2 |
| kis-proxy 배치 `/v1/stocks/quotes`, 토큰 버킷, 소비자 이전 | 장중 시세 중복 호출 | R-4 |
| bond-mate를 서버 금리의 source of record로 | 허브 스크레이퍼 약 500줄 제거 | R-12 |
| 공용 OpenDART·finance-pi 클라이언트(fin-commons) | 클라이언트 6개·5개 중복 | R-3 |
| finance-pi 가격·매크로 호출 타임아웃 | 행이 멈추면 요청이 무한 대기 | §0 소유자 결정 |
| 허브 일봉 폴백 3중(`kis_proxy`·`portfolio/history`·`stock_price`) | 미스 한 번에 finance-pi 최대 3회 왕복 | [modularization.md](modularization.md) |
| 형제 파이프라인 KIS 직접 토큰(5개) | 같은 앱키로 재발급하면 서로 무효화 | R-4 |
