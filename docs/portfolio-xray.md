# 포트폴리오 X-Ray

심층 분석 탭의 `#pfExposureWrap` 카드. 입력 없이 ETF·지주사·우선주를 풀어 본 실제 노출과 통화·자산군·업종
비중, 집중도, 위험 기여를 보여 준다. 선택 계좌(`PfStore.accountId`)를 따르고 현금은 따로 표시한다.

| 위치 | 역할 |
|---|---|
| `routes/portfolio_exposure.py` | `GET /api/portfolio/exposure[?account_id=…]` 인증 API |
| `services/portfolio/exposure.py` | 순수 계산. 수식 규약은 모듈 docstring, 손계산은 `tests/test_portfolio_exposure.py` |
| `services/portfolio/exposure_data.py` | 자료 로더(메모리 → DB → 네트워크, 실패해도 예외 없음) |
| `services/portfolio/exposure_service.py` | 조립. 사용자·계좌별 10분 캐시 |
| `services/market/sources/kind.py` | KIND 상장법인목록 provider |
| `static/js/portfolio-exposure.js` | 화면 |

## 데이터 출처와 판단

| 항목 | 출처 | 캐시 | 낡음 기준 |
|---|---|---|---|
| ETF 구성 | eiayn `data/etfs.json` | ETag 조건부, 압축본 DB 보관, 6시간마다 확인 | 3일 |
| 지주사 지분 | holding_value `api/holdings.json` | DB 보관, 6시간 | 120일 |
| 우선주 쌍 | common_preferred_spread summary → config 폴백 | 메모리 1시간 | 7일 |
| 업종·회사명 | KIND 상장법인목록 | DB 보관, 30일 | 45일 |
| 일봉 | finance-pi `/api/prices/daily` | 결과 캐시 10분 | 5일 |
| 해외 ETF 판별 | Yahoo chart `instrumentType` | DB 보관, 90일 | - |

- **ETF는 허브가 캐시한다.** 비중 있는 구성 행이 약 5,500개(200 KB 이상)라 `summary.json` 64 KB 예산에 들어가지 않는다.
  ETag로 파일이 바뀐 때만 9 MB를 파싱하므로 요청 경로에는 영향이 없다. 재시작해도 DB 압축본을 쓴다. eiayn에 경량 파일을
  새로 발행하면 다운로드 자체를 줄일 수 있지만, 지금 이득이 작아 하지 않았다.
- **업종은 KIND를 쓴다.** KRX 업종분류는 로그인이 필요하고 DART 기업개황은 회사마다 호출해야 한다. KIND는 익명 요청 하나로 전 시장을
  받는다. 표의 열 순서가 바뀐 적이 있어(시장구분 추가) 머리글 이름으로 열을 찾는다. 해외 종목과 목록에 없는 종목은 `미분류`로 둔다.

## 한계

- **ETF**: 공개된 상위 구성만 안다. 비중이 없거나 구성이 없는 ETF는 커버리지 0%이고, 그 평가액은 `ETF 미공개 구성`으로 남긴다.
  채권·원자재형은 풀지 않고 한 줄로 둔다. HHI는 미공개분을 완전 분산으로 보는 하한이다.
- **지주사 환산은 추정치다.** 주식수는 holding_value `config.json` 설정값이고 공시와 다를 수 있다. `as_of`는 공시일이 아니라 파일을 만든
  날짜다. 지주사 평가액과 별개의 참고치라 합계·비중·HHI에 더하지 않는다. 1단계 자회사까지만 보고, 우선주는 보통주 1주와 같은 지분으로 환산한다.
- **위험 기여**: finance-pi는 국내 일봉만 있어 해외·금·가상자산은 빠지고 null이다(커버리지 표시). 선물은 기초자산 일봉으로 대신한다.
  공통 구간을 절반 아래로 줄이는 신규 상장 종목은 표본 부족으로 뺀다. 현재 비중을 과거 수익률에 곱한 추정치다.
- **통화**: 거래 통화 기준이라 국내 상장 해외 ETF는 원화로 잡힌다.
