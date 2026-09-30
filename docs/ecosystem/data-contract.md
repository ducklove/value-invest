# 생태계 발행 데이터 계약 (Published Data Contract v1)

> 정본: 이 문서 + `config/schemas/vc-envelope.schema.json` + `config/schemas/summary/<tool>.schema.json`.
> 참조 구현: `ecosystem/python/vc_publish.py`(stdlib), `ecosystem/js/vc-publish.mjs`(Node ESM, 의존성 없음).
> 예시: `tests/fixtures/ecosystem/<tool>.summary.json` (실데이터에서 추려 만든 발행 파일 그대로의 모양).
> 테스트: `tests/test_vc_publish.py`, `tests/js/vc-publish.test.mjs` (두 언어의 해시가 같은지 교차 검증).

## 1. 목적

형제 대시보드(holding_value, common_preferred_spread, spac-hunter, buybacks, eiayn, gold_gap,
all-about-gold, nps-tracker, bond-mate)는 각자 GitHub Pages에 큰 JSON을 발행한다. 허브는 카드 몇 장과
포트폴리오 신호를 만들려고 지금 이 파일들을 통째로 받는다.

| 지금 허브가 받는 파일 | 크기 | 실제로 쓰는 것 |
|---|---|---|
| eiayn `data/etfs.json` | 9.3 MB | `universe` 코드 목록 |
| spac-hunter `data.json` | 3.8 MB | 스팩별 필드 약 15개 |
| buybacks `holding_snapshots.json` | 1.2 MB | 종목별 최신 자사주 비율 |
| gold_gap `data.json` | 563 KB | 자산 4개의 마지막 갭 |
| nps-tracker `current.json` | 313 KB | 요약·자산배분·상위 5 |
| bond-mate `data/current.json` | 112 KB | 시리즈별 최신값·하이라이트 |

이 계약은 각 도구가 **허브용 요약 `summary.json`(공통 envelope)** 과 **변경 감지용 `version.json`** 을
추가로 발행하게 한다. 기존 파일은 그대로 두므로(추가만) 기존 소비자는 깨지지 않는다.

목표:

1. 허브가 받는 사이블링 데이터를 합계 약 15 MB에서 약 150 KB(압축 전)로 줄인다.
2. `raw.githubusercontent.com` + 브랜치 하드코딩을 없애고 Pages URL 하나(`<도구 URL>/summary.json`)로 통일한다.
3. 타임스탬프만 바뀐 실행은 커밋·배포하지 않는다(no-op 규칙, §8).

## 2. Envelope v1

```json
{
  "schemaVersion": 1,
  "tool": "holding_value",
  "kind": "summary",
  "generatedAt": "2026-09-30T09:00:00+09:00",
  "asOf": "2026-09-26T09:21:15+09:00",
  "sources": [{"id": "kis-proxy", "name": "KIS 시세 프록시"},
              {"id": "opendart", "name": "OpenDART", "url": "https://opendart.fss.or.kr/"}],
  "contentHash": "sha256:639df464…",
  "data": { "…도구별 payload (§6)…": null }
}
```

| 필드 | 형식 | 규칙 |
|---|---|---|
| `schemaVersion` | 정수 `1` | envelope 구조 버전. v1 안에서는 envelope 최상위 키를 추가하지 않는다. 알 수 없는 최상위 키가 있으면 검증 실패로 처리한다. |
| `tool` | 레지스트리 id | `config/ecosystem.json`의 `tools[].id`와 같아야 한다(`holding_value`, `spac-hunter` …). 패턴 `^[a-z0-9][a-z0-9_-]*$`. |
| `kind` | `"summary"` | v1에서 정의한 종류는 `summary` 하나다. |
| `generatedAt` | `YYYY-MM-DDTHH:MM:SS+09:00` | 파일을 만든 시각(KST, 오프셋 명시, `Z` 금지). **해시에 포함하지 않는다.** |
| `asOf` | `YYYY-MM-DD` 또는 `YYYY-MM-DDTHH:MM[:SS]+09:00` | 데이터가 **설명하는** 시점(KST). 실행 시각이 아니다. 신선도 판정 기준. |
| `sources` | `[{id, name, url?}]` (1개 이상) | 원천 데이터. `id`는 `^[a-z0-9][a-z0-9_.-]*$`이고 중복되지 않는다. `url`은 http(s). 다른 키는 쓰지 않는다. |
| `contentHash` | `sha256:<64 hex>` | `data`의 canonical JSON(§3)의 SHA-256. |
| `data` | 객체 | 도구별 payload. 스키마: `config/schemas/summary/<tool>.schema.json`. |

값 규칙(모든 `data` 공통):

- **모르는 숫자는 `null`이다. 0으로 채우지 않는다.** 0은 "실제로 0"일 때만 쓴다.
- NaN·Infinity는 쓸 수 없다. 참조 구현이 발행 전에 거부한다.
- 숫자의 절댓값은 2^53−1 이하여야 한다. JS가 정확히 표현할 수 있는 범위다. 원화 금액(조 단위)도 이 범위에 들어간다.
- 날짜는 `YYYY-MM-DD`(KST), 월 단위는 `YYYY-MM`이다. 원본 문자열을 그대로 옮기는 필드(`lastUpdated`, `updatedAt`, `publishedAt`)만 예외다.
- 키 이름은 camelCase다. 종목코드는 접미사 없이 6자리(`005930`, `0209J0`)로 쓰고, 해외 ETF는 대문자 티커(`VOO`, `1321.T`)로 쓴다.
- 스키마가 `required`로 정한 키는 값이 없어도 반드시 넣는다(`null`). 선택 키는 빼도 된다.

## 3. Canonical JSON과 contentHash

`contentHash = "sha256:" + hex(sha256(utf8(canonical(data))))`

canonical 규칙은 Python `canonical_json()`과 JS `canonicalJson()`이 **바이트 단위로 같게** 구현한다
(`tests/js/vc-publish.test.mjs`가 Python을 실행해 대조한다).

1. 객체 키는 유니코드 코드포인트 순으로 정렬한다(Python `sorted()` 순서).
2. 구분자는 `,`와 `:`이고 공백은 넣지 않는다.
3. 문자열은 `json.dumps(ensure_ascii=False)`와 `JSON.stringify` 방식으로 쓴다. 한글은 이스케이프하지 않는다.
4. 숫자는 ECMAScript `Number#toString` 형식으로 쓴다: `1.0 → 1`, `1e-07 → 1e-7`, `-0 → 0`. 따라서 Python의 `1`과 `1.0`은 같은 해시가 된다.
5. NaN/Infinity, 짝 없는 서로게이트, 문자열이 아닌 키, JSON이 아닌 타입(date, set 등)은 거부한다.

**발행 파일 = envelope 전체의 canonical JSON + `\n`.** 한 줄짜리 compact JSON이 되고, 같은 입력이면
Python과 JS가 같은 파일을 만든다(`generatedAt`만 다를 수 있다).

## 4. 파일 위치

공개 URL은 모든 도구에서 같은 규칙을 따른다.

```
<레지스트리 url>/summary.json
<레지스트리 url>/version.json
```

예: `https://ducklove.github.io/spac-hunter/summary.json`. 저장소 안에서 어디에 두는지는 Pages 배포
방식에 따라 다르다.

| tool | 저장소 경로 → Pages 루트 | 만드는 곳 | 벤더링된 헬퍼 (`sync-ecosystem.mjs`) |
|---|---|---|---|
| holding_value | `summary.json`, `version.json` (master 루트, 브랜치 Pages) | `update-current.yml`: `fetch_current.py` 다음 단계 | `pipeline/vc_publish.py` |
| common_preferred_spread | 루트 (master, 브랜치 Pages) | `update-current.yml` | `vc_publish.py` |
| spac-hunter | 루트 (main, `upload-pages-artifact path: .`) | `pages.yml`: `fetch_data.py` 다음, 커밋 목록에 추가 | `spac_hunter/vc_publish.py` |
| buybacks | `public/summary.json`, `public/version.json` → Vite가 `dist/` 루트로 복사 | `update-buybacks-data.yml`·`refresh-holdings.yml`: `build_buybacks_dataset.py` 다음, `public/data/buybacks`와 함께 커밋 | `scripts/buybacks/vc_publish.py` |
| eiayn | `dist/summary.json`, `dist/version.json` (빌드 산출물, 커밋하지 않음) | `npm run build` 끝에서 `build-rankings.mjs` 다음에 실행하는 `node scripts/build-summary.mjs` | `scripts/vc-publish.mjs` |
| gold_gap | **data 브랜치** 루트 `summary.json`, `version.json` → `deploy.yml`이 `_site/`로 복사 | `update-data.yml`: `generate_data.py` 다음. data 브랜치 커밋 여부를 `cmp -s data.json` 대신 헬퍼 반환값으로 정한다 | `goldgap/vc_publish.py` |
| all-about-gold | `_site/summary.json`, `_site/version.json` (빌드 산출물) | `scripts/build_pages.py --snapshot …` | `scripts/vc_publish.py` |
| nps-tracker | 루트 (main). X0(아티팩트 축소) 이후에는 `_site/` 스테이징 목록에 두 파일을 추가 | `pages.yml`: `fetch_data.py` 다음, `Commit refreshed data` 목록에 추가 | `nps_tracker/vc_publish.py` |
| bond-mate | **data 브랜치** `data/summary.json`, `data/version.json` → `deploy.yml`이 `_site/summary.json`, `_site/version.json`으로 복사 (현재의 `for name in current rates …` 루프와 별도 단계) | `update-data.yml` | `bondmate/vc_publish.py` |

빌드 시점에 만드는 도구(eiayn, all-about-gold)는 이전 파일이 없어서 no-op 비교를 할 수 없다. 이런
도구는 `generated_at`에 **원천 스냅샷 시각**(eiayn `etfs.json.generatedAt`, finance-pi
`publishedAt`)을 넘긴다. 그러면 같은 입력으로 빌드했을 때 같은 파일이 나온다.

### version.json

```json
{"files":{"summary.json":"sha256:639df464…"},"generatedAt":"2026-09-30T09:00:00+09:00","schemaVersion":1,"tool":"holding_value"}
```

- `files`는 발행 파일 이름과 해시를 짝지은 맵이다. envelope 파일에는 그 `contentHash`를 쓰고, envelope가 아닌 파일에는 `file_hash()`(바이트 SHA-256)를 쓴다.
- 기본은 `summary.json`만 넣는다. 레거시 파일(`current.json` 등)은 바이트가 결정적일 때만 넣는다. 타임스탬프가 들어 있는 파일을 넣으면 version.json이 매번 바뀌어 no-op 규칙이 깨진다.
- `write_version()`은 `tool`과 `files`가 그대로면 파일을 쓰지 않는다(`generatedAt` 갱신도 하지 않는다).
- 1 KB 미만이므로 허브나 워치독이 싸게 폴링할 수 있다.

## 5. 참조 구현 API

| Python (`vc_publish`) | JS (`vc-publish.mjs`) | 설명 |
|---|---|---|
| `canonical_json(obj)` / `dumps_compact(obj)` | `canonicalJson` / `dumpsCompact` | §3 canonical 문자열 (개행 없음) |
| `content_hash(data)` | `contentHash` | `sha256:<hex>` |
| `file_hash(path)` | `fileHash` | 파일 바이트 해시 |
| `build_envelope(tool, data, *, as_of, sources, generated_at=None)` | `buildEnvelope(tool, data, {asOf, sources, generatedAt})` | 해시를 계산하고 검증한 envelope. `as_of`/`generated_at`은 문자열 또는 date·datetime(Date)을 받고 KST로 바꾼다. |
| `validate_envelope(obj)` | `validateEnvelope` | 구조 검증 + 해시 재계산. 실패하면 `EnvelopeError` |
| `write_if_changed(path, envelope) -> bool` | `writeIfChanged` | §8 no-op 규칙. 임시 파일에 쓴 뒤 `os.replace`/`rename`으로 원자적으로 교체 |
| `write_version(path, files, *, tool=None, generated_at=None) -> bool` | `writeVersion(path, files, {tool, generatedAt})` | version.json |
| `python vc_publish.py validate f.json …` / `hash f.json` | — | CI용 CLI |

두 파일은 **허브가 정본**이다. 형제 저장소의 사본은 직접 고치지 않고, 허브에서 고친 뒤
`node scripts/sync-ecosystem.mjs --write`로 다시 복사한다(파일 첫 줄 주석에 적혀 있다).

## 6. 도구별 payload (`data`)

설계 원칙은 "허브가 지금 쓰는 필드만, 그러나 허브가 원본 파일 없이 기존 기능을 전부 재현할 수 있을
만큼"이다. 기준이 된 허브 소비자는 `services/ecosystem/external_tools.py`(인사이트 카드 `_summarize_*`, 종목 딥링크
`_match_*`, 액션보드 `fetch_portfolio_signals`, ETF `fetch_etf_universe`/`etf_link_for`, 스팩
`fetch_spac_data`), `services/portfolio/spac.py`(청산가치 지표), `static/js/market-bond-mate.js`
(브라우저 금리·환율 병합)다. 크기는 실데이터 기준 전체 발행 시 추정치(압축 전)다.

### 6.1 holding_value (≈ 9 KB)

| 필드 | 타입 | 설명 |
|---|---|---|
| `lastUpdated` | string\|null | current.json `lastUpdated` |
| `isPartial` | bool | 일부 쌍 시세 누락 여부 |
| `pairCount` | int\|null | 원본 요약 쌍 수 |
| `averageRatio` | number\|null | 평균 보유가치/시총 % |
| `pairs[]` | 배열 | **config.json 순서의 전체 목록**. `_average` 같은 의사 행은 제외 |
| `pairs[].id, name, holdingName` | string | `name`은 `영풍→고려아연` 형식 |
| `pairs[].code` | 6자리 | 지주사 종목코드 (`.KS` 제거) |
| `pairs[].ratio, ratioChange` | number\|null | 보유가치/조정시총 %, 전일 대비 %p |
| `pairs[].holdingValue, marketCap` | number\|null | 억원 |

허브 유도: TOP 카드 = `ratio`가 있는 행을 내림차순으로 N개. 종목 매칭 = `code`가 같은 **첫** 행이고, 그 행의 `ratio`가 null이면 매칭 없음(`_match_holding`과 같은 의미).

### 6.2 common_preferred_spread (≈ 10 KB)

| 필드 | 타입 | 설명 |
|---|---|---|
| `lastUpdated` | string\|null | |
| `averageSpread`, `averageSpreadChange` | number\|null | 평균 괴리율 %, 변화 %p |
| `indexSpread`, `indexSpreadChange` | number\|null | (선택) 지수 괴리율 |
| `pairs[]` | 배열 | **config.json 순서의 전체 목록** |
| `pairs[].id, name, preferredName` | string | |
| `pairs[].commonCode, preferredCode` | 6자리 | |
| `pairs[].spread, spreadChange` | number\|null | 괴리율 %, %p |
| `pairs[].commonPrice, preferredPrice` | number\|null | 원 |
| `pairs[].date` | date\|null | 시세 기준일 |

허브 유도: TOP 카드 = `spread` 내림차순. 단, 같은 `commonCode`에서는 괴리율이 가장 큰 우선주 하나만 남긴다. 종목 매칭 = `commonCode` 또는 `preferredCode`가 같은 첫 행.

### 6.3 spac-hunter (≈ 38 KB, data.json 3.8 MB 대체)

| 필드 | 타입 | 설명 |
|---|---|---|
| `lastUpdated` | string\|null | |
| `valuationDate` | date\|null (선택) | 아래 `currentLiquidationValue`·`liquidationDiscountPct`의 기준일(= envelope `asOf`) |
| `summary.totalCount, belowIpoCount` | int\|null | |
| `summary.averageRatio, averageAnnualizedReturn` | number\|null | |
| `valuationAssumptions` | `{trustFeePct, interestTaxPct, payoutLagDays}` | 스팩별 `valuationBasis`가 비었을 때 쓰는 기본값 |
| `spacs[]` | 배열 | 상장 스팩 전체 |
| `spacs[].code, name` | | |
| `spacs[].currentPrice, ipoPrice, ratio, annualizedReturn` | number\|null | 현재가는 current.json 값을 우선한다 |
| `spacs[].status, mergerStatus` | string\|null | |
| `spacs[].listingDate, liquidationDate, payoutDate` | date\|null | |
| `spacs[].liquidationValuePerShare` | number\|null | 수령 예정일 기준 예상 분배금/주 |
| `spacs[].currentLiquidationValue, liquidationDiscountPct` | number\|null (선택) | `valuationDate` 기준 누적 청산가/주와 청산가 괴리(%) — spac-hunter 목록의 '청산가 괴리'와 같은 식 |
| `spacs[].escrowRatePeriods[]` | `{startDate, ratePct}` | 예치 이율 구간(출처 필드는 뺀다) |
| `spacs[].valuationBasis` | `{trustStartDate, trustFeePct, interestTaxPct, rolloverMonths, anchor: {date, valuePerShare}\|null}` | `current_liquidation_value()` 입력 |

허브 유도: 카드 = `currentPrice` 오름차순. `fetch_spac_data()` 대체 = `{"spacs": data.spacs, "valuationAssumptions": …, "lastUpdated": …}`. `services/portfolio/spac.py`는 그대로 동작한다(읽는 필드가 모두 들어 있다). history·filing·events·kind·quote는 넣지 않는다.

### 6.4 buybacks (≈ 22 KB, holding_snapshots.json 1.2 MB 대체)

선택 규칙은 `external_tools._summarize_buybacks`와 같다. 종목×주식종류별로 최신 스냅샷을 고르고
(날짜 → 채움 정도 → report_code 순), 그중 **보통주**이면서 `treasury_ratio`가 있는 행만 남긴다.

| 필드 | 타입 | 설명 |
|---|---|---|
| `asOf` | date\|null | 스냅샷 날짜 중 최신값. `ratioAsOf`의 기본값 |
| `count` | int | `ratios`에 든 종목 수 |
| `top[]` (≤20, 권장 10) | | `{code, name, asOf, stockKind, treasuryRatioPct, endingQty, issuedShares}`, 비율 내림차순 |
| `ratios` | `{code: pct}` | 종목별 자사주 비율 %(소수 2자리) |
| `ratioAsOf` | `{code: date}` | 날짜가 `asOf`와 **다른** 종목만 넣는다(크기 절약) |

`ratios`에는 이름을 넣지 않는다. 신호 제목은 허브가 가진 종목명으로 만든다. envelope `asOf`는 데이터셋 스캔일(`data_status.generated_at`의 KST 날짜)이고, `data.asOf`는 공시 기준일이다.

### 6.5 eiayn (≈ 22 KB, etfs.json 9.3 MB 대체)

| 필드 | 타입 | 설명 |
|---|---|---|
| `scoreModelVersion` | string\|null | |
| `universeSize` | int | 커버 ETF 수 |
| `universe[]` | string | 커버 코드(대문자, 정렬, 중복 없음). ETF 딥링크·포트폴리오 신호용 |
| `rankings[]` (≤100) | | `{rank, code, name, score, market}`. `rankings.json`과 같은 순서(rank 오름차순) |

허브 유도: 오늘의 추천 = `random.Random(KST 날짜).sample(rankings, 5)`. 목록 순서가 rankings.json과 같으므로 기존과 같은 5개가 뽑힌다. 딥링크는 레지스트리의 `?code={code}` 템플릿으로 만든다. `name`은 앞뒤 공백을 제거한 `shortName`이고, 없으면 `name`을 쓴다.

### 6.6 gold_gap (≈ 1 KB, data.json 563 KB 대체)

| 필드 | 타입 | 설명 |
|---|---|---|
| `updatedAt` | string\|null | 원본 `updated_at` (`2026-09-27 08:47 KST`) |
| `goldIntlMode` | string\|null | 금 갭의 국제가 기준(`ny_futures`) |
| `assets[]` | `gold, bitcoin, eth, usdt` 순서 | `{key, label, date, gap, prevGap, domesticPrice, intlPrice, usdKrw}` |

허브 유도: 카드와 `KRX_GOLD`/`CRYPTO_BTC`/`CRYPTO_ETH`/`CRYPTO_USDT` 신호를 만든다. 자산 딥링크(`?asset=…`)는 허브가 붙인다.

### 6.7 nps-tracker (≈ 2 KB, current.json 313 KB 대체)

| 필드 | 타입 | 설명 |
|---|---|---|
| `lastUpdated`, `source` | string\|null | |
| `summary` | `{totalValue, nav, count, todayPct, mtdPct, ytdPct, asOf}` | totalValue는 원 |
| `allocation` | `{asOf, estimated, classes[{key, label, pct, target?}]}` \| null | 기금 자산배분(추정 현재 비중). `target`이 있으면 허브 상수보다 우선한다 |
| `top[]` (≤20, 권장 10) | | `{code, name, weight, marketValue, changePct, ownershipPct, sector}`, weight 내림차순 |

### 6.8 bond-mate (≈ 10 KB, data/current.json 112 KB 대체)

| 필드 | 타입 | 설명 |
|---|---|---|
| `highlights` | `{usCurveSpreadBp, usCurveInverted, krCurveSpreadBp, igHySpreadBp}` | |
| `creditSpreadBp` | `{rating: bp}` | OAS × 100, 소수 1자리 |
| `latestOffering` | `{issuer, issuerName, filingDate, totalAmount, tranches}` \| null | totalAmount는 USD |
| `rates` | `{seriesId: {country, maturity, tenor, value, change, changePct, date}}` | **전체 시리즈의 최신값**(히스토리 없음). maturity: 년, 0=익일물, −1=기준금리 |
| `fx` | `{pair: {label, value, change, changePct, date}}` | 전체 통화쌍 최신값 |

허브 유도:
- 서버 카드: `usBase = rates.US_BASE`, `krBase = rates.KR_BASE`, `us10y/us2y/kr10y/kr3y`, `usdKrw = fx.USD_KRW`. 이 값들을 `{value, change, asOf: date}`로 바꿔 쓴다.
- 브라우저 `mergeBondMate`: `rates`/`fx`를 그대로 쓸 수 있다. 단, 키가 `changePct`(원본은 `change_pct`)다.

### 6.9 all-about-gold (≈ 1.5 KB)

| 필드 | 타입 | 설명 |
|---|---|---|
| `publishedAt` | string\|null | finance-pi 발행 시각(원본 UTC 문자열) |
| `prices[]` | | `{id, name, unit, date(YYYY-MM), value, prevDate, prevValue, changePct}`. gold·silver·bitcoin·dollar·usdkrw 월평균 |
| `ratios` | `{date, goldSilver, bitcoinGold}` | 최신 월 기준 |
| `marketSize` | `{date, aboveGroundTonnes, miningTonnes, marketCapUsd, goldDebtRatioPct, stockToFlowYears}` | 연말 기준 |

현재 허브는 이 도구의 데이터를 읽지 않는다(카드 링크만 표시). summary가 생기면 "금 투자 리서치" 카드에 헤드라인 수치를 붙일 수 있다.

## 7. 허브 소비 규칙 (Wave B 로더)

1. **summary 우선, 레거시 폴백.** 도구마다 독립적으로 `<url>/summary.json`을 먼저 받는다. 다음 중 하나면 그 도구만 기존 경로(`current.json`/`data.json` …)로 폴백한다.
   - HTTP 오류(404 포함)
   - JSON 파싱 실패
   - `validate_envelope` 실패(해시 불일치 포함)
   - `schemaVersion`이 1이 아님
   - `tool`이 요청한 도구와 다름
   - `data`가 도구 스키마의 필수 키를 갖추지 않음

   도구 하나가 실패해도 나머지 도구는 영향을 받지 않는다.
2. **캐시**: `MemoryTTLCache` 900초. TTL이 만료되면 조건부 GET(`If-None-Match`/ETag, Pages가 지원)을 보낸다. 304 응답이면 TTL만 연장한다. 404는 **음성 캐시**한다(TTL 동안 summary를 다시 시도하지 않고 바로 레거시로 간다). 사이블링이 아직 summary를 발행하지 않은 동안 요청이 두 배로 늘지 않게 하기 위해서다.
3. **변경 감지(선택)**: 워치독이나 폴러는 `version.json`만 받는다. `files["summary.json"]`이 캐시한 `contentHash`와 같으면 본문을 다시 받지 않는다.
4. **stale-while-error**: 새로 받는 데 실패하면 마지막으로 성공한 envelope를 `stale: true`로 표시해 1일까지 쓴다.
5. **신선도**: `asOf`가 도구별 허용치(예: 장중 도구 1거래일, buybacks 분기, all-about-gold 월)를 넘으면 UI에 "기준일" 경고를 붙인다. 판정 기준은 `generatedAt`이 아니라 `asOf`다. no-op 규칙 때문에 `generatedAt`은 갱신되지 않을 수 있다.
6. 허브 응답 모양(`/api/external/insights`, `fetch_stock_links`, `fetch_portfolio_signals`, `fetch_spac_data`)은 바꾸지 않는다. §6의 "허브 유도"대로 summary를 기존 모양으로 바꿔 돌려준다.
7. URL은 레지스트리(`config/ecosystem.json`)의 도구 `url`에서 만든다. `raw.githubusercontent.com`과 브랜치 이름은 쓰지 않는다.

## 8. no-op 규칙과 GitHub Actions

`write_if_changed(path, envelope)`는 기존 파일과 `schemaVersion`, `tool`, `kind`, `asOf`, `contentHash`가
모두 같으면 **쓰지 않고 `False`를 반환**한다. `generatedAt`과 `sources`만 달라진 경우도 쓰지 않는다.
그래서 데이터가 같은 재실행은 git diff를 만들지 않고, 커밋이나 Pages 배포도 일어나지 않는다.
`asOf`는 비교 대상이다. 따라서 `asOf`에 실행 시각을 넣으면 안 된다.

레거시 파일(`current.json` 등)에 `lastUpdated` 같은 타임스탬프가 있으면 그 파일 때문에 매번 커밋이
생긴다. 두 가지 방법 중 하나로 막는다. ① summary가 레거시 파일의 의미 있는 변화를 모두 담고 있으면,
summary 변경 여부를 커밋 게이트로 쓴다. ② 그렇지 않으면 레거시 파일은 volatile 필드를 뺀
`content_hash({k: v for k, v in cur.items() if k not in VOLATILE})`로 이전 값과 비교해서, 같으면
원본을 그대로 둔다.

Python 발행 단계 예시(holding_value):

```python
# scripts/publish_summary.py — pipeline/vc_publish.py 는 sync-ecosystem.mjs 가 벤더링
import json
import os
import sys

sys.path.insert(0, "pipeline")
import vc_publish as vp

cur = json.load(open("current.json", encoding="utf-8"))
data = build_holding_summary(cur, json.load(open("config.json", encoding="utf-8")))  # §6.1
env = vp.build_envelope(
    "holding_value", data,
    as_of=cur["generatedAt"],                 # 데이터 기준 시각, 실행 시각 아님
    sources=[{"id": "kis-proxy", "name": "KIS 시세 프록시"}],
)
changed = vp.write_if_changed("summary.json", env)
vp.write_version("version.json", {"summary.json": env})
with open(os.environ.get("GITHUB_OUTPUT", os.devnull), "a") as fh:
    fh.write(f"summary_changed={'true' if changed else 'false'}\n")
```

워크플로 커밋 단계:

```yaml
- name: Publish summary.json
  id: summary
  run: python scripts/publish_summary.py

- name: Commit if changed
  run: |
    git add current.json summary.json version.json
    if git diff --cached --quiet; then
      echo "No changes — skip commit/deploy"
      exit 0
    fi
    git commit -m "Update current prices ($(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M'))"
    git push
```

data 브랜치를 쓰는 도구(gold_gap, bond-mate)는 `if cmp -s data.json /tmp/data_prev.json` 대신
`steps.summary.outputs.summary_changed == 'true'` 또는 data 워크트리에서의 `git diff --cached --quiet`로
커밋과 배포 트리거 여부를 정한다.

JS(빌드 시점, eiayn) 예시:

```js
// scripts/build-summary.mjs — build-rankings.mjs 다음에 실행
import { readFileSync } from 'node:fs';
import { buildEnvelope, writeIfChanged, writeVersion } from './vc-publish.mjs';

const snapshot = JSON.parse(readFileSync('public/data/etfs.json', 'utf8'));
const rankings = JSON.parse(readFileSync('dist/data/rankings.json', 'utf8'));
const env = buildEnvelope('eiayn', buildEiaynSummary(snapshot, rankings), {   // §6.5
  asOf: new Date(snapshot.generatedAt),
  generatedAt: new Date(snapshot.generatedAt),   // 결정적 빌드
  sources: [{ id: 'naver', name: '네이버 증권 ETF', url: 'https://finance.naver.com/sise/etf.naver' }],
});
writeIfChanged('dist/summary.json', env);
writeVersion('dist/version.json', { 'summary.json': env });
```

## 9. 호환성·버전 규칙

- **schemaVersion 1 안에서는 추가만 한다.**
  - 허용: `data`에 선택 필드 추가, 배열 항목 추가, 새 도구 추가.
  - 금지: 필드 삭제, 이름 변경, 타입 변경, 단위 변경, 배열 정렬 기준 변경, 필수(`required`) 필드 추가.
- 금지된 변경이 필요하면 새 이름의 필드를 추가하고, 옛 필드는 최소 30일 동안 같이 발행한다. 그동안 허브를 새 필드로 옮긴 뒤 옛 필드를 뺀다.
- envelope 구조를 바꾸는 일(최상위 키 추가 등)은 `schemaVersion: 2`다. 순서는 허브 로더가 1과 2를 모두 지원하도록 먼저 배포하고, 그다음 사이블링을 전환한다.
- 소비자(허브)는 `data`에서 모르는 필드를 무시하고, 필수 필드가 빠졌으면 그 도구만 레거시로 폴백한다.
- 스키마를 먼저 바꾼다. 필드를 추가할 때는 허브의 `config/schemas/summary/<tool>.schema.json`과 fixture를 먼저 고치고 테스트를 통과시킨 다음 사이블링 발행 코드를 바꾼다. 스키마는 `data`에 대해 열려 있다(`additionalProperties` 제한 없음). 그래서 사이블링이 조금 앞서 필드를 추가해도 검증은 깨지지 않는다.
- 크기 예산: summary는 64 KB 이하(권장 16 KB)다. 넘을 것 같으면 compact 맵(buybacks `ratios` 방식)을 쓰거나 필드를 줄인다.

## 10. 생산자 체크리스트 (사이블링)

- [ ] 헬퍼는 벤더링된 사본(`vc_publish.py` / `vc-publish.mjs`)을 쓴다. 직접 수정하지 않는다.
- [ ] `tool`은 레지스트리 id다(`spac-hunter`처럼 하이픈을 그대로 쓴다).
- [ ] `asOf`는 데이터 기준 시각(KST)이다. 실행 시각을 넣지 않는다.
- [ ] `generatedAt`은 `+09:00`이다(헬퍼 기본값). 빌드 시점 발행이면 원천 스냅샷 시각을 넘긴다.
- [ ] 모르는 숫자는 `null`이다. 0이나 빈 문자열을 쓰지 않는다. NaN이 들어가면 헬퍼가 예외를 낸다(발행 실패 = 이전 파일 유지).
- [ ] `data`가 허브 스키마(`config/schemas/summary/<tool>.schema.json`)의 필수 키를 모두 갖는다.
- [ ] `write_if_changed` + `write_version`을 쓰고, `git diff --cached --quiet`면 커밋을 건너뛴다.
- [ ] Pages 아티팩트에 `summary.json`, `version.json`이 루트로 들어간다(`_site`/`dist` 스테이징 목록 확인).
- [ ] CI에서 `python <vendor>/vc_publish.py validate summary.json`을 실행하거나 JS에서 `validateEnvelope`를 호출한다.
- [ ] 기존 파일(current.json/data.json …)은 그대로 발행한다. 허브 폴백과 외부 소비자(finance-pi 등)가 쓴다.
- [ ] 비밀값(토큰·키·LAN 주소)이 `data`나 `sources.url`에 들어가지 않았는지 확인한다.
