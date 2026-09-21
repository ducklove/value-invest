# 증권사 계좌 어댑터

2026-09-21 기준. NH·한국투자증권에 키움·LS증권을 추가했다. 증권사 선택 → 앱키 확인 → 전체 계좌번호·잔고 미리보기 → 연결 순서다. 미리보기와 닫기는 보유분을 변경하지 않는다.

## 조사와 구현 범위

| 증권사 | 이번 서비스에서 가져오는 잔고 | 서버 통보 | 제한 |
|---|---|---|---|
| NH | 기존 국내·해외주식, 금, 국내·해외 선물, 원화·외화 | 기존 상품별 통보, 시세 소켓 공유 | 기존 수입·입출금 수집 유지 |
| 한국투자 | 기존 일반 주식계좌의 국내·해외주식, 원화·외화 | HTS ID가 있으면 국내·해외 체결 | 상품코드 01, 모의는 국내주식 |
| 키움 | 국내 상장주식·ETF, 미국주식, 원화·USD, 별도 금현물 계좌 | 국내 주문체결 `00`, 잔고 `04` | 모의는 국내주식. 금은 `M04020000`만 지원. 다른 외화·신용·대출·선물·연금은 미지원 |
| LS | 코스피·코스닥·ETF, CMA RP, 미국주식, 원화·외화 | 국내 `SC1`, 미국 `AS1` | 실계좌만. 신용·대출·금·선물·연금은 미지원 |

키움·LS의 배당·이자·입출금 원장 자동 수집은 아직 제공하지 않는다. REST 잔고의 현금 변화만으로 수입을 추정하지 않는다. 통보가 없는 입출금 등은 60초 주기 잔고 조회로 반영한다. 새 증권사의 시세 WebSocket은 추가하지 않았으며 기존 NH 우선·KIS 보조 시세 경로를 유지한다.

### 추가 후보

- **DB증권**: [공식 SDK](https://github.com/DBsecurities/dbsec-open-api)에 국내·해외주식, 국내·해외 선물옵션, 채권, 계좌 체결 통보 명세가 있다. 이번 조사에서 확인한 주식 잔고·예수금 응답은 앱키에 연결된 계좌를 사용하지만 전체 계좌번호를 반환하지 않는다. 읽기 전용으로 키와 계좌번호를 확실히 연결하는 수단을 더 확인한 후 추가한다. 계좌번호를 돌려주는 WebSocket 세션 초기화 API는 기존 연결을 끊는 동작이므로 계좌 확인 용도로 호출하지 않는다.
- 다른 증권사는 공개 REST 계좌 조회·서버 통보 명세와 개인 이용 신청 절차가 확인된 경우 같은 어댑터 계약으로 추가한다. 공개 문서를 찾지 못한 것을 API가 없다는 의미로 해석하지 않는다.

## 공식 명세와 금액 기준

### 키움

출처: [공식 가이드](https://openapi.kiwoom.com/guide/apiguide), [공식 명세·예제](https://github.com/Kiwoom-Securities/Kiwoom-REST-API/tree/main/kiwoom).

- OAuth `/oauth2/token`, `appkey`·`secretkey`, `token`·`expires_dt`. 실전/모의 도메인을 분리한다.
- `ka00001.acctNo`로 키에 연결된 10자리 계좌번호를 확인한다. 연결 이후에도 매번 대조한다.
- `kt00018`의 `rmnd_qty`·`pur_pric`를 사용한다. KRX·NXT 잔고를 각각 더하지 않는다. `ka10100` 시장·종목상태로 비상장·상장폐지를 제외한다. 거래정지 자체는 제외 사유가 아니다.
- 원화 `kt00001.d2_entra`. 주문가능액·출금가능액은 현금으로 사용하지 않는다.
- 미국주식 `ust21070`의 거래소별 `poss_qty`·`frgn_stk_book_uv`. USD 현금은 `ust21160.d4_usd_fx_entr`로 모든 제공 결제일을 반영한다. D0·D2 값도 비교용으로 보관한다.
- USD 외 통화에 잔액이 확인되면 동기화를 보류한다. 해외주식을 선택하지 않아도 예수금 응답에 USD가 있으면 상세 현금을 조회한다.
- 금 `kt50020.real_qty`는 g 수량, `avg_prc`는 원/g. 현금은 `kt50021.prsm_entra`이며 주식 D+2와 구분한다. 미니금을 1kg 금 종목으로 합치지 않는다.
- WebSocket은 LOGIN 성공 후 `00`·`04`를 등록한다. `REG` ACK는 잔고 갱신 신호가 아니다. `REAL.data[].values.9201`과 연결 계좌를 대조한다. 해외주식·금의 통보 범위를 확대 해석하지 않고 주기 조회로 보완한다.
- [키움 신청 안내](https://openapi.kiwoom.com/intro/serviceInfo)에 따라 접속 IP 등록이 필요하다. 사용자는 서비스 서버의 공인 출발 IP를 등록해야 한다.

### LS

출처: [공식 API 가이드](https://openapi.ls-sec.co.kr/apiservice), [공식 호출 예제](https://openapi.ls-sec.co.kr/howto-sample), [인증 안내](https://openapi.ls-sec.co.kr/howto-use).

- OAuth 폼 인증, `CSPAQ12200OutBlock1.AcntNo`로 전체 계좌번호 확인. 이 TR의 정상 코드 `00136`을 처리한다.
- `t0424`를 체결 기준(`chegb=2`)·평균단가(`prcgb=1`)로 조회한다. 본문 `cts_expcode`를 다음 요청에 넘긴다. 잔고 0은 정상적인 빈 보유분이다.
- `t8436` 공식 코스피·코스닥 종목 목록을 기준으로 합산한다. 목록 밖 종목은 제외 수와 사유를 미리보기에 표시한다. 따라서 코넥스 등 목록에 없는 시장까지 지원한다고 표시하지 않는다.
- 원화는 `CSPAQ12200.D2Dps`. CMA RP는 `t0424`의 `CMARP` 평가액을 `CMA_RP_KRW`로 보관하고 매입액과 분리한다. 현금에 중복 합산하지 않는다.
- 미국주식 `COSOQ00201`: `AstkBalQty`·`FcstckUprc`, 일반·소수점 모두 조회한다. `AstkSettQty`만 쓰면 미결제 매매가 빠지므로 사용하지 않는다. 미국 외 시장이 반환되면 보류한다.
- 외화 `COSOQ02701.PrsmptFcurrDps4`를 사용하며 `PrsmptFcurrDps2`도 보관한다. 원화 환산 합계와 주문가능액은 더하지 않는다. 해외주식 선택 여부와 관계없이 예수금을 조회하므로 해당 API 이용 권한이 필요하다.
- `SC1`의 `ordacntno`, `AS1`의 `sAcntNo`를 확인한다. ACK 후 실제 계좌 통보만 재조회를 요청한다.

## 구조

```text
domain/broker_catalog.py       지원 상품·환경·입력 항목·안내 문구
services/brokers/base.py       BrokerAdapter 추상 인터페이스
services/brokers/registry.py   구현체 등록·선택
services/brokers/adapters.py   기존 NH/KIS 구현의 어댑터
services/brokers/kiwoom.py     키움 요청·응답 해석
services/brokers/ls.py         LS 요청·응답 해석
services/brokers/rest.py       공통 토큰·호출 제한·재시도·연속조회
services/brokers/runtime.py    공통 서버 감시·2초 병합·12초 후속 조회·60초 보완
services/brokers/push.py       새 증권사 통보 프로토콜과 소유 계좌 검사
services/brokers/sync.py       어댑터 호출 후 원장·합산 잔고의 원자적 교체
```

NH는 키당 두 소켓 한도를 지키도록 기존 시세·계좌 통보 공유 런타임을 `run()`에서 사용한다. 나머지는 공통 런타임을 사용한다. 앱 수명주기는 등록된 어댑터를 각각 독립 태스크로 시작하고 종료 시 모두 정리한다.

계좌 라우터는 `/api/portfolio/{provider}/credentials`, `/api/portfolio/accounts/{id}/{provider}` 및 `preview`·`sync` 경로를 공통 처리한다. 기존 NH/KIS URL을 유지한다. `/api/portfolio/brokers` 메타데이터로 화면을 구성해 증권사별 버튼·폼 조건문의 증가를 막는다.

### 새 증권사 추가 순서

1. `BrokerDefinition`에 실제 구현한 상품·환경만 선언한다.
2. `BrokerAdapter`의 `discover()`·`fetch_snapshot()`를 구현한다. 통보가 있으면 `stream()`·`status()`·`forget()`을 구현한다. 거래내역이 검증된 경우에만 `activity`를 켜고 `fetch_activity()`를 구현한다.
3. `registry.ADAPTERS`에 등록한다. REST는 고정 도메인·조회 허용 목록만 사용하고 `core/http.py`에 시간 제한 프로파일을 등록한다.
4. 공식 응답 필드로 계좌 소유, 결제 기준, 빈 잔고, 누락·중복·연속조회 실패, 재접속, 원장 보존을 검증한다. 일반 주식 계좌를 선물 계좌로 해석하지 않는다.

키·토큰은 기존 암호화 저장소를 사용한다. NH/KIS의 기존 계좌·키 지문 형식을 보존한다. 키에 한 계좌가 결합된 증권사는 같은 키를 다른 계좌로 중복 연결할 수 없다. 증권사 원문 오류·통보·자격증명은 응답·로그에 노출하지 않는다. 자동 주문 메서드는 계약과 REST 허용 목록에 없다.

## 검증 한계

자동 테스트는 명세에 맞춘 가상 응답으로 금액 변환·격리·실패 보존을 검증하고 브라우저 테스트는 실제 인증·임시 DB 저장을 검증한다. 키움·LS 실계좌 키가 등록되기 전에는 실제 upstream 잔고 및 장중 통보 수신을 검증했다고 볼 수 없다.
