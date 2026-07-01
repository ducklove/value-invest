# 증권사 계좌 연동 방안

한국 증권사 계좌의 보유 종목·예수금·거래 내역을 프로그램으로 가져오는 방법을
정리한다. value-invest 허브는 Raspberry Pi 위의 단일 사용자 앱이므로, "개인이
자기 계좌를 자동 import" 가능한 경로만을 다룬다. 기준 시점: 2026-07.

> 결론부터: **한국투자증권(KIS) Open API**가 유일하게 Pi·Linux·개인 사용자
> 관점에서 실현 가능한 공식 경로다. 그 외 증권사는 Windows 전용이거나
> 개인 개발자에게 열려 있지 않다. MyData·오픈뱅킹은 사업자 등록/자본금 요건
> 탓에 개인 앱엔 닿을 수 없다. 화면 스크래핑은 약관·법적 위험이 있고 주요
> 증권사가 공식 API를 제공하므로 더 이상 정당화되지 않는다.

## 후보별 평가

### 1. 한국투자증권 KIS Developers — **권장(유일한 실현 경로)**

REST API로 계좌 조회·잔고·일별 체결내역을 모두 지원한다. 국내·해외주식 모두.

- **포털**: https://apiportal.koreainvestment.com
- **계좌/잔고 엔드포인트**:
  - 국내주식 잔고: `GET /uapi/domestic-stock/v1/trading/inquire-balance`
    (`tr_id: TTTC8434R`)
  - 일별주문체결: `GET /uapi/domestic-stock/v1/trading/inquire-daily-ccld`
    (호출당 ~100건, 페이징)
  - 해외주식 잔고: `GET /uapi/overseas-stock/v1/trading/inquire-balance`
- **인증**: 실전투자 / 모의투자 App Key+Secret 분리. 접근토큰(~1일 TTL) 갱신.
  - 실전: `https://openapi.koreainvestment.com:9443`
  - 모의: `https://openapivts.koreainvestment.com:29443`
- **가입 조건**: 개인 OK. 한투 계좌 개설 → KIS Developers에서 Open API 서비스
  신청 → App Key/Secret 발급. 회사 불필요, 무료.
- **생태계**: 공식 샘플(github.com/koreainvestment/open-trading-api),
  커뮤니티 `pykis`(github.com/pjueon/pykis),
  `python-kis`(github.com/Soju06/python-kis).
- **주의(2026)**: 초당 호출수 제한이 신설됐으나, 하루 1~2회 폴링 대시보드엔
  무관하다.

허브 통합 시 `kis_proxy_client.py`가 이미 KIS proxy(시세용)를 쓰고 있으므로,
계좌 조회는 별도 클라이언트(`services/portfolio/broker_import.py`)로
분리하는 것이 자연스럽다. App Key/Secret은 `.env.<profile>`에 두고
`core/config`로 로드(legacy `keys.txt`/`.kis.env`는 배제).

### 2. 키움증권 OpenAPI+ (REST) — 차선

2025년부터 REST API가 추가돼 Windows/OCX 종속이 해제됐다. 계좌평가잔고내역
(`kt00018`, 페이징 지원) 등 계좌 조회 가능. 단, 한투만큼 커뮤니티 문서가
풍부하지 않고 한투 계좌가 없는 사용자에게만 의미가 있다.

- **포털**: https://openapi.kiwoom.com
- **래퍼**: github.com/younghwan91/kiwoom-rest-api

### 3. 토스증권 Open API — 추후 검토

2026년 출시. REST+WebSocket, 국내/해외 통합, 설계가 가장 깔끔. 단, 아직 단계적
오픈 중(H2 2026 GA 목표)이라 **2026년 현재는 KIS가 안정적 선택**이다. 토스
계좌 보유자는 사전등록 후 키 발급 가능.

- **포털**: https://corp.tossinvest.com/ko/open-api

### 4. 기타 증권사 — 개인 개발자에게 사실상 닫힘

| 증권사 | 상태 |
| --- | --- |
| NH투자(나무) | QV Open API 존재하나 DLL(Windows) 전용 |
| 미래에셋·삼성·카카오페이증권 | 개인 Open API 없음 |
| KB·신한·유안타 | 파트너/BaaS 지향, 개인 키 발급 흐름 아님 |

### 5. 오픈뱅킹(금융결제원) — 부적합

증권계좌 일부 잔고/거래내역 조회는 지원하나, (a) 핀테크 이용기관 등록이 필요해
개인이 직접 호출 불가, (b) 현금 잔고/입출금만 보여주고 **보유 종목/평가금액은
안 됨**. 허브 용도에 맞지 않는다.

### 6. 마이데이터 — 부적합(사업자 요건)

2025년 6월 MyData 2.0으로 증권 계좌 정보 이동이 가능해졌으나, 직접 API 호출은
**자본금 5억원 이상의 마이데이터사업자**만 가능. 개인 앱엔 닿을 수 없다.
집계를 대행하는 유료 서업(CODEF)은 있으나 신뢰 제3자 의존+과금이 붙어
단일 사용자 앱엔 과하다.

### 7. 화면 스크래핑/RPA — 권장하지 않음

각 증권사 약관이 자동 접속을 금지(계정 정지 위험)하며, 정보통신망법·저작권법
노출도 있다. 주요 증권사가 공식 REST API를 제공하는 2026년에는 정당화되지
않는다. 유지되는 오픈소스 "내 계좌 스크래핑" 라이브러리도 사실상 소멸했다.

## 추천 구현 순서

value-invest 허브에 계좌 자동 import를 도입할 때의 단계다. 멀티계좌(계정
분리) 기반 위에서 의미가 있으므로, accounts 작업(phase 1) 이후를 전제한다.

1. **CSV import 파서(증권사 무관)** — 모든 증권사 MTS/HTS의 CSV 내보내기를
   매핑하는 드라이런 검증 레이어. broker key(한투/키움/…)별 컬럼 매핑
   `services/portfolio/broker_import.py`. 이게 가장 보편적 baseline이고
   KIS API 의존 없이 동작한다.
2. **KIS 계좌 연동(한투 보유자)** — 하루 1~2회 잔고+체결내역 폴링 →
   `user_stocks`/`portfolio_cashflows` 동기화. systemd timer 기반(기존
   `routes/internal.py` 배치 패턴 재사용). 토큰 갱신은 lifespan에서 관리.
3. **(선택) 키움 REST** — 한투가 아닌 사용자. 동일 인터페이스, 다른 provider.

## 보안·운영 메모

- App Key/Secret은 **`.env.<profile>`에만** 두고 저장소에 커밋하지 않는다
  (`.gitignore`에 이미 반영된 패턴).
- 접근토큰은 메모리에만 보관(디스크 캐시 금지). 서비스 재시작 시 재발급.
- 조회 전용(`tr_id` 조회계)만 사용하고 주문 API는 연결하지 않는다 — 쓰기
  권한이 노출되면 계정 정지/법적 리스크가 급증한다.
- 백업(`scripts/backup_cache_db.sh`)에 계좌 데이터가 포함되므로 백업 파일
  보관 정책을 함께 점검한다.
