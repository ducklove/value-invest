# 실현손익·세금 추정

포트폴리오 → 심층 분석의 `실현손익 · 세금 추정` 카드와 액션 보드 항목의 계약이다. 입력은 없다.
서버가 NH 연결 계좌의 기간 손익을 수집하고 화면은 저장된 원장만 읽는다. **신고용이 아닌 추정이다.**

## 데이터 흐름

`NH 기간 손익 API → services/brokers/realized_pnl.py(백그라운드) → broker_realized_pnl → services/portfolio/realized_tax.py → GET /api/portfolio/realized-tax`

| 코드 | 역할 |
|---|---|
| `services/brokers/namuh.py` `READ_PATHS` | 조회 전용 허용 목록. 아래 4개를 추가했고 주문 경로는 없다 |
| `services/brokers/realized_pnl.py` | NH 조회·정규화·검산·수집 루프(`nh-realized-pnl`). 호출은 `namuh.pages`로 앱키 잠금·1.1초 간격을 공유 |
| `repositories/realized_pnl.py` | 원장(`broker_realized_pnl`)·수집 상태(`broker_realized_pnl_state`)·세금 계산용 읽기 |
| `domain/realized_tax.py` | 시행일별 세율표, 순수 계산, `NH_PNL_FIELDS_VERIFIED` |
| `services/portfolio/realized_tax.py` | 연도별 요약과 액션 보드 항목 |
| `static/js/portfolio-tax.js` | 카드 렌더링(`portfolio-performance` 지연 그룹) |

`NH_REALIZED_PNL_INTERVAL_S`(기본 1800초)가 올해 분의 조회 간격이고 0이면 수집만 멈춘다. 지난해는 7일 간격이다.
연속 실패는 5분·10분·20분…(최대 1시간) 간격으로만 재시도한다. 수집 실패는 잔고 동기화에 영향을 주지 않는다.

## NH 조회

명세: [해외주식](https://www.nhplug.com/openapi-docs/gbstock/openapi.json), [국내주식](https://www.nhplug.com/openapi-docs/krstock/openapi.json) (명세서 261015 기준).

| API | 쓰임 | 호출 |
|---|---|---|
| `/gbstock/inquiry/v1/periodPnl` | 일자·국가·통화별 합계. 연도 합계의 기준 | 연도마다 `iqr_dit` 1(거래통화)·2(원화) 각 1회 |
| `/gbstock/inquiry/v1/periodPnlDetail` | 그 묶음의 종목별 행 | 내용이 바뀐 묶음만 기준별 1회 |
| `/krstock/inquiry/v1/tradingPnl` | 종목별 합계. 연도 합계의 기준 | 연도마다 1회 |
| `/krstock/inquiry/v1/dailyTradingPnl` | 그 종목의 일별 행. 명세가 "수수료율을 가정"한다고 밝힌다 | 내용이 바뀐 종목만 1회 |

- 합계를 먼저 저장하고 상세로 교체한다. 상세가 늦거나 실패하거나 합계와 맞지 않아도 연도 합계는 NH 기간 합계와 같다.
- 상세는 양쪽 기준이 같은 종목으로 짝지어지고 합계와 맞을 때만 쓴다. 아니면 합계 1행을 `detail_missing`으로 유지하고 내용이 바뀔 때까지 다시 조회하지 않는다.
- 응답에 목록 블록이 없으면 `rsp_cd` 13578(운영 확인)이나 합계가 모두 0일 때만 0건이다. 그 밖에는 실패로 기록하고 이전 원장을 유지한다.
- 대상은 `provider=namuh`, `environment=live`, `product=stocks` 연결이다.

## 원장

- 재조회는 멱등이다. 묶음(`group_key`)별 내용 해시가 같으면 행을 건드리지 않고, 바뀌면 그 묶음만 교체하며, 응답에서 사라진 묶음(취소)은 지운다.
- 키는 계좌 지문 기준이라 같은 NH 계좌를 다른 계좌 행으로 다시 연결해도 한 번만 남는다. 계좌번호 원문은 저장하지 않는다.
- 쓰기는 연결 소유자만 할 수 있다(`brokers.get_link`). 수집 중 연결이 바뀌면 반영하지 않는다.
- 연결 해제 뒤에도 행은 남고, 행이 있는 계좌는 삭제할 수 없다. 잔고·현금·NAV 입출금은 바꾸지 않는다.
- 행 표시(`issues_json`): `detail_pending`·`detail_missing`, `arith_mismatch`(실현손익 ≠ 매매손익 − 제비용), `native_missing`,
  `krw_missing`·`fx_implausible`. 마지막 둘은 `realized_krw`를 비워 세액 합계에서 뺀다.
- `fx_implausible`은 원화 기준 매도금액 ÷ 거래통화 매도금액이 현재 환율의 0.6~1.5배를 벗어난 경우다. 원화 기준 응답이 원화가 아니어서 세액이 크게 줄어드는 사고를 막는 단위 검산이며 환율의 정확성 검증이 아니다.

## 계산

세율·공제액은 `domain/realized_tax.py`의 시행일별 표에 있다. 세법이 바뀌면 새 시행일 행을 추가하고 `RULES_REVIEWED_THROUGH`를 올린다. 그 연도를 넘으면 마지막 행을 쓰되 화면에 미확인으로 알린다.

- 해외 양도세: `연간 해외 실현손익(통산) − 기본공제 250만원`에 22%(지방소득세 포함). 손실이면 0원. 손익은 NH `iqr_dit=2`의 `fc_rzt_pls` 합계다.
- 손실 매도 감소액: 마지막 일별 정산의 `수량 × (원화 단가 − 현지 매입단가 × 정산 환율)`이 음수인 NH 해외 보유분을 실현한다고 가정한 세액 차이. 수수료·환전 비용은 빼지 않았다. 대체 시세로 정산한 종목은 제외하고 건수를 표시한다.
- 금융소득: 올해 세전 배당 + 세전 이자 + 남은 지급 예정. 80% 이상이면 근접, 2,000만원 초과면 초과 예상.
- 국내 실현손익은 표시만 한다. 소액주주 장내 매매를 전제로 세액을 계산하지 않는다.
- 일반 유형 계좌만 계산한다. ISA·연금·배우자 계좌는 과세 단위나 규칙이 달라 미포함으로 표시한다.

### 금융소득의 원천

2026-11-08 운영 DB를 읽기 전용으로 확인한 결과(건수만):

- `broker_transactions`는 배당 76건뿐이고 이자 행은 0건이다. 일반 수입 가져오기가 [보류](namuh-account-activity.md#계속-보류하는-것) 중이기 때문이다.
- 이자는 `broker_cash_review_evidence`의 일별 합계에만 있다. 예탁금이용료 `interest` 4건, MMW `mmw_interest` 30건이고 2026-09-22 이후뿐이다.

그래서 다음과 같이 계산한다.

| 항목 | 금액 | 범위 |
|---|---|---|
| 국내 배당 | `gross_amount` | `kind='dividend'`만. `review`와 세전 원화를 모르는 행은 건수만 표시 |
| 해외 배당 | `gross_krw`(NH 과세표준) | 세금 정산(`외화제세금환급`)은 새 소득이 아니므로 제외 |
| 이자 | `mmw_interest`는 세전 그대로, `interest`는 세후만 남아 있어 ÷(1 − 15.4%). `broker_transactions`에 이자 행이 생기면 그 행을 쓰고 같은 날 일별 합계는 세지 않음 | RP·발행어음·외화 이자는 수집하지 않음 |
| 남은 지급 예정 | 배당 캘린더의 오늘 뒤 올해 지급 행 × NH 보유 비율 | 입금과 연결된 행 제외. 지급일 없는 행은 합계에 넣지 않고 근접 판단에만 사용 |

연초 이자를 채우기 위해 수집 루프가 계좌·연도별로 한 번 `sync_account(start=1월 1일)`을 실행한다. 사용자가 쓰는 기간 지정 가져오기와 같은 경로라 같은 키는 다시 넣지 않는다. 대여수수료(`other_income`)는 금융소득이 아니므로 뺀다.

## 필드 검증 상태

**2026-11-09에 공개 명세만으로 구현했다. 2026-11-10 운영 응답 점검에서 1~5번을 통과했고(아래 결과), 6번 NH 앱 대조가 남았다.** 그래서 `NH_PNL_FIELDS_VERIFIED = False`이고, 화면과 액션 보드는 해외 손익에서 나온 금액을 어림값(`약 165만원`)과 `미검증`으로만 표시한다.

배포하면 수집 루프가 양쪽 기준 금액을 `data_json`에 쌓는다. 운영 DB에서 읽기 전용으로 다음을 확인한 뒤 플래그를 바꾼다. 바꿔도 화면의 `추정`·`신고용 아님` 표시는 유지된다.

```bash
ssh 192.168.68.202 'cd ~/Works/value_invest && sqlite3 -header "file:cache.db?mode=ro" "
SELECT market,year,COUNT(*) n,SUM(error IS NOT NULL) errors,MAX(last_success_at) last FROM broker_realized_pnl_state GROUP BY market,year;
SELECT market,issues_json,COUNT(*) n FROM broker_realized_pnl GROUP BY market,issues_json;
SELECT currency,COUNT(*) n,MIN(sell_krw/sell_native) fx_min,MAX(sell_krw/sell_native) fx_max FROM broker_realized_pnl WHERE market=\"overseas\" AND sell_native>0 GROUP BY currency;
"'
```

1. 수집 오류가 없다. 있으면 `error`로 기간 길이 제한·빈 응답 코드 등을 먼저 고친다. 연 단위 조회가 되는지는 아직 모른다.
2. `iqr_dit=2`의 금액이 원화다: 통화별 `fx_min`·`fx_max`가 그 기간 환율 범위 안이고 `fx_implausible`이 없다.
3. `fc_rzt_pls = fc_sby_pls − fc_sdr_xps`다: `arith_mismatch`가 없다.
4. 원장 합계가 NH 합계(`fc_rzt_pls_sum`)와 같다. 화면에 차이가 표시되지 않는다.
5. 매수 필드(`fc_byn_amt1`)가 그날의 매수인지 매도분 원가인지, 원화 기준 원가가 취득 시 환율인지 확인한다(`json_extract(data_json,'$.krw.buy_amount')`와 `$.native.buy_amount` 비교). 판매 시점 환율 하나로 환산한 값이면 세법 기준과의 차이가 커지므로 화면 문구를 조정한다.
6. 아는 매도 한 건을 NH 앱의 기간손익·양도세 자료와 대조한다.
7. 호주·독일·베트남 매매가 응답에 나오는지 확인한다. 명세의 국가 목록에 없어 보유 중이면 화면에 누락 가능성을 표시한다.

### 2026-11-10 운영 점검 결과

NH 연결 계좌는 4개, 원장은 75행(국내 56, 해외 19)이다.

| 항목 | 결과 |
|---|---|
| 1. 수집 오류 | 통과. 1년 범위 조회가 국내·해외 모두 된다. 오류는 당일(11-10) 매도한 33626K의 일별 상세 1건뿐으로, `httpx` 전송 오류(15초 초과)다. 기간 합계 행이 남아 연도 합계에는 영향이 없다. |
| 2. 원화 기준 | 통과. USD 19행의 매도 환율(원화 매도금액 ÷ 달러 매도금액)이 1,336~1,511로 그 기간 환율 범위 안이다. `fx_implausible`은 0건이다. |
| 3. 손익 산식 | 통과. `arith_mismatch`가 0건이다. |
| 4. NH 합계 대조 | 통과. 계좌·연도·시장별 원장 합계와 NH 기간 합계의 차이가 모두 0원이다. |
| 5. 원가 기준 | 매수 필드는 그날 매수분이다(매도 행 모두 0). 원가 환율((원화 매도 − 원화 손익) ÷ (달러 매도 − 달러 손익))이 종목마다 다르고 매도 시점 환율과도 다르다. 예를 들어 SIVR은 매도 1,472·원가 1,125, PPLT는 매도 1,339·원가 1,139다. 그래서 원화 손익은 취득 시 환율 기준이고 환차익을 포함한다. 예로 SCHP 2026-06-03은 달러 손익이 +120달러인데 원화 손익은 +619만원이다. 원가 단가가 평균인지 선입선출인지는 이 자료로 알 수 없어 화면 문구는 그대로 둔다. |
| 6. NH 앱 대조 | 소유자 확인 대기 |
| 7. 호주·독일·베트남 | 판정 불가. AUD·EUR·VND 종목을 보유하고 있지만 원장에 그 통화의 매도가 없다. 응답 국가는 미국(200)뿐이다. 화면의 누락 가능성 경고를 유지한다. |

6번이 맞으면 `NH_PNL_FIELDS_VERIFIED = True`로 바꾼다.

## 화면에 밝히는 한계

- 신고용이 아닌 추정이다. NH 산식과 세법의 선입선출·취득일/양도일 기준환율은 다를 수 있다.
- 연도는 NH 주문일 기준이다. 세법상 귀속은 결제일 기준이라 연말 매도분은 다음 해로 넘어갈 수 있다.
- NH 외 계좌는 미포함이다. 계좌 이름을 표시하고 NH 외 계좌 몫의 예정 배당은 합계 밖에 따로 적는다.
- 국내 상장 해외·채권 ETF 매매차익(배당소득), 고배당기업 분리과세 특례, ISA·연금·RIA는 반영하지 않는다. 고배당 분리과세 세율은 자료마다 달라 넣지 않았다.

세율·공제액은 2026-11 기준 2차 자료(증권·세무 안내)에서 확인했고 국세청 원문은 대조하지 못했다.

## 액션 보드

`services/portfolio/action_board.py`가 `realized_tax.actions`를 합친다. 항목을 누르면 `#pfTaxWrap`으로 이동한다.

| 키 | 조건 | 심각도 |
|---|---|---|
| `tax:overseas:{연도}:remaining` | 11~12월, 공제 잔여 > 0, NH 해외 손익 또는 보유 있음 | watch |
| `tax:overseas:{연도}:exhausted` | 11~12월, 공제 소진. 손실 매도 시 최대 감소액 표시 | watch |
| `tax:financial-income:{연도}:near` | 연중, 합계 + 지급일 미확인 일정 ≥ 80% | watch |
| `tax:financial-income:{연도}:over` | 연중, 합계 > 2,000만원 | high |

상태가 바뀌면 키가 달라져 검토 완료한 항목도 다시 올라온다. 해외 항목은 수집 전(`tax` 없음)에는 만들지 않는다. 외부 메시지는 보내지 않는다.

## API

`GET /api/portfolio/realized-tax`(로그인 필요). NH를 호출하지 않는다. 외부 호출은 배당 일정뿐이며 10분 캐시·20초 제한이 있고, 실패해도 받은 금액은 표시한다.
주요 필드: `fields_verified`, `current`·`years[]`(`domestic`·`overseas`·`tax`·`tax_status`), `loss_harvest`, `financial_income`, `coverage`, `unlisted_currencies`, `notes`.
`tax_status`는 `estimated`, `incomplete`(일부 계좌·행 누락 또는 최근 조회 실패), `unavailable`(수집 전·실패, 세액 없음)이다.
