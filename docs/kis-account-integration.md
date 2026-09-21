# 한국투자증권 계좌 자동 연동

계좌 관리에서 빈 계좌를 만들고 **한국투자증권 연동**을 선택한다. 본인 명의의
Open API 앱키·시크릿, 계좌번호 8자리와 상품코드 `01`을 입력한다.
키 확인 → 잔고 미리보기 → 연결하고 잔고 가져오기 순서다. 미리보기 취소는
계좌 연결이나 잔고 저장을 만들지 않는다. 가져온 잔고는 초기 잔고이므로
매수 거래를 추가하거나 현금을 이중 차감하지 않는다.

## 범위

| 항목 | 처리 |
|---|---|
| 실전 일반 주식 계좌 | 국내주식·ETF, 미국·일본·홍콩·중국 주식 |
| 원화 현금 | `prvs_rcdl_excc_amt` 가수도정산금액(D+2) |
| 외화 현금 | 체결기준현재잔고의 통화별 `frcr_dncl_amt_2 - frcr_buy_amt_smtl + frcr_sll_amt_smtl`. 매매 결제 예정액을 반영하며 주문가능액과 구별 |
| 국내 비상장·상장폐지 | 주식기본조회의 상장일·폐지일로 제외. 거래정지만으로 제외하지 않음 |
| 모의계좌 | 국내주식과 원화 예수금. 모의 전용 키·호스트·TR 사용 |
| 금·선물·연금·CMA·신용/대출 | 현재 한국투자 어댑터에서 미지원. 해당 잔고를 발견하면 부분 합산하지 않고 갱신 보류 |
| 미니스탁·그 외 해외시장 | 완전한 잔고를 확인하지 못하면 갱신 보류 |
| 수입·입출금 거래원장 | 한국투자증권의 전체 원장 API는 확인하지 못했으며 자동 수집 미지원. 현금 차액을 배당·이자·원금 입출금으로 추정하지 않음 |

원화·외화 조회 중 하나라도 실패하면 해당 계좌의 이전 잔고를 보존한다.
외화 예수금은 해외주식 포함 옵션을 꺼도 조회한다. 동일 종목은 기존 계좌별
원장을 통해 전체 포트폴리오에 합산하고 통화 충돌은 저장 전에 거부한다.
연결 해제는 보유분을 보존한 채 수동 관리로 전환한다.

한국투자 계좌의 외부 입출금은 자동으로 NAV 원금 흐름에 분류하지 못한다.
따라서 입출금이 있는 기간의 수익률은 이 한계를 감안해야 한다.
NH의 기존 수입·입출금 원장과 금·국내외선물 기능은 유지된다.

## 서버 자동 갱신

- 앱 수명주기의 `kis-accounts` 작업이 브라우저 접속과 무관하게 60초마다 조회한다.
- HTS ID를 함께 등록하면 키별 소켓에서 국내 `H0STCNI0`, 해외 `H0GSCNI0`를
  구독한다. 모의는 국내 `H0STCNI9`만 사용한다.
- `/oauth2/Approval`로 접속키를 발급받고 구독 ACK의 AES key/IV로 통보를
  복호화한다. ACK와 실제 통보는 구별한다.
- 현재 연결된 **사용자·증권사·키·환경·계좌번호**가 일치하는 통보만 반영한다.
  통보는 잔고 재조회 신호이며 체결 금액 자체를 현금에 더하거나 빼지 않는다.
- 통보를 2초 간격으로 모아 조회하고 12초 후 한 번 더 조회하여 증권사 반영
  지연을 보완한다. 재접속 직후에도 조회한다.
- HTS ID가 없거나 소켓이 끊겨도 60초 조회를 지속한다. 실제 연결 상태는
  계좌 선택 영역에 표시한다.
- 인증된 브라우저는 기존 `/ws/namuh` 또는 별칭 `/ws/broker-accounts`에서
  `accounts_changed`, `kis_account_status`를 받는다. 시세의 NH 우선/KIS
  보조 정책과 공용 시세 키는 바꾸지 않는다.

## 저장·운영

- `broker_credentials.provider`, `broker_account_links.provider`를 추가하며
  기존 행은 `namuh`로 마이그레이션한다.
- 한국투자 키 지문은 증권사와 투자 환경을 포함하고 계좌 지문도 증권사별로
  분리한다. 키·시크릿·토큰·HTS ID·계좌번호는 기존 서버 암호화 방식으로 저장한다.
- 계좌번호 전체는 본인의 인증된 계좌 관리 응답에만 제공한다. 비밀·암호문·
  통보 원문은 브라우저에 전달하지 않는다. HTTP 로그의 계좌 쿼리도 지운다.
- 토큰은 환경별로 재사용하며 만료 재발급은 키별로 직렬화한다. 조회 한도
  오류는 유효 토큰을 버리지 않고 재시도한다.
- REST 경로는 읽기 허용 목록으로 제한한다. 주문·자동매매 경로는 없다.
- 연속조회 실패·반복·누락·상한 도달은 오류로 처리한다. 일부 페이지로 전체
  계좌를 덮어쓰지 않는다.
- `BROKER_ENCRYPTION_KEY` 또는 기존 `.broker-encryption.key`를 DB와 함께
  복구할 수 있어야 한다. 새 공용 한국투자 키 환경변수는 필요하지 않다.

## 공식 자료와 검증

2026-09-21 한국투자증권 공식 샘플을 확인했다.

- [주식잔고조회](https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/domestic_stock/inquire_balance)
- [해외주식 잔고](https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/overseas_stock/inquire_balance)
- [해외 체결기준현재잔고](https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/overseas_stock/inquire_present_balance)
- [국내 체결통보](https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/domestic_stock/ccnl_notice), [해외 체결통보](https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/overseas_stock/ccnl_notice)

테스트: `tests/test_kis_accounts.py`, `tests/js/portfolio-accounts.test.mjs`,
`tests/browser/kis-accounts.spec.mjs`. 합성 응답으로 금액·인증·페이지·AES 통보·
다른 사용자 격리·NH 공존·취소·연결 해제·휴대폰 화면을 검증한다.
실계좌 API 응답 및 실제 체결 통보 수신은 별도 확인이 필요하다.
