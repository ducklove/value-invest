# Linked Projects — 허브 쪽 연동 설정

갱신: 2026-09-30

`value-invest`는 포트폴리오·분석 허브다. 연결 저장소는 각자 독립 배포를 유지하고, 허브는 공개 URL·발행 JSON·
서버 쪽 환경변수로만 결합한다(코드를 복사해 오지 않는다). 이 문서는 **허브 쪽 설정·환경변수·운영 메모**를 다룬다.
전체 구조와 계약은 [ecosystem/](ecosystem/README.md) 문서가 정본이다.

- 도구 목록·URL·딥링크 템플릿·데이터 URL의 정본: [config/ecosystem.json](../config/ecosystem.json)
  (로더 [core/ecosystem.py](../core/ecosystem.py)). 아래 표는 요약이고, 다르면 레지스트리가 맞다.
- 딥링크·테마·셸 규칙: [ecosystem/ui-contract.md](ecosystem/ui-contract.md)
- 허브용 발행 데이터(`summary.json`): [ecosystem/data-contract.md](ecosystem/data-contract.md)

## 프로젝트 맵

| integrationKey | 저장소 | 로컬 디렉터리 | 허브가 쓰는 방식 |
| --- | --- | --- | --- |
| `holdingValue` | `ducklove/holding_value` | `../hodling-value` 또는 `../holding_value` | 서버: `summary.json` 우선, 실패 시 `current.json`·`config.json`. Pi 로컬 체크아웃의 `config.json`·`current.json`으로 목표가·알림용 설정(`/app-config.js`). 브라우저: `api/holdings.json`은 로컬 설정이 없을 때의 폴백. 지주사 행을 `?code=`로 링크(handoff) |
| `preferredSpread` | `ducklove/common_preferred_spread` | `../common_preferred_spread` | `summary.json` 우선. 로컬 `config.json`의 보통주·우선주 쌍. 우선주 행을 `?code=<우선주>`로 링크(handoff) |
| `spacHunter` | `ducklove/spac-hunter` | — | `summary.json` 우선, 실패 시 `current.json`·`data.json`. 스팩 행을 `?code=`로 링크(handoff). `baseUrl`만 노출 |
| `buybacks` | `ducklove/buybacks` | — | `summary.json` 우선, 실패 시 `holding_snapshots.json`. 종목을 `?stock=`으로 링크(`?code=`는 buybacks가 받는 별칭, handoff). `baseUrl`만 노출 |
| `eiayn` | `ducklove/eiayn` | — | `summary.json`(빌드 산출물) 우선, 실패 시 `data/rankings.json`·`data/etfs.json`(universe). 국내·해외 ETF를 `?code=<티커>`로 링크(handoff). 시각 테마는 `?theme=light\|dark`, ETF 카테고리 필터는 `?etf_theme=`이다(옛 `?theme=<카테고리>` 링크는 eiayn이 `etf_theme`으로 바꿔 준다) |
| `goldGap` | `ducklove/gold_gap` | `../gold_gap` | `summary.json` 우선, 실패 시 Pages `data.json`. `KRX_GOLD`→`?asset=gold`, `CRYPTO_BTC`→`?asset=bitcoin`. 대시보드 카드는 eth·usdt도 링크한다. 서버 기본 자산 맵(`DEFAULT_GOLD_GAP_ASSETS`)은 gold·bitcoin·usdt뿐이고 eth는 없다 |
| `allAboutGold` | `ducklove/all-about-gold` | `../all-about-gold` | 분석 도구 금 리서치 카드, `summary.json`. 사이트 데이터는 all-about-gold 저장소의 `ops/update_pages.py`가 pi-control 타이머(07:17)에서 finance-pi gold 수집을 돌린 뒤 `data` 브랜치에 push해 발행한다. `/app-config.js`가 노출하는 `dataUrl`은 허브가 읽지 않는다 |
| `npsTracker` | `ducklove/nps-tracker` | — | `/nps` 뷰에 iframe(`?embed=true&theme=`), 투자정보 인사이트 카드는 `summary.json` 우선, 실패 시 `current.json` |
| `bondMate` | `ducklove/bond-mate` | `../bond-mate` | 투자정보 국채·환율 패널의 원천: 브라우저가 `data/current.json`을 지표 카탈로그에 병합. `/bonds` 뷰가 `?embed=<탭>`으로 화면 임베드. 서버 인사이트는 `summary.json` 우선. `baseUrl`/`dataUrl`/`embedUrl`/`views` 노출 |
| (없음) index-popup | `ducklove/index-popup` | — | 투자정보 지수 위젯 iframe(`https://ducklove.duckdns.org:3358/?index=…&theme=…&period=…&headless=1`). URL은 레지스트리에서 읽고 JS 상수는 폴백 |
| `kisProxy` | `ducklove/kis-proxy` | `../kis-proxy` | 서버 전용. [services/market/sources/kis_proxy.py](../services/market/sources/kis_proxy.py)가 `KIS_PROXY_BASE_URL`로 호출. `/app-config.js`에는 나가지 않는다(`integrations.build_server_integrations()`) |

> `finance-pi`(pi-control 데이터 레이크 `:8400`)는 레지스트리의 내부(`internal`) 항목이고 integrationKey가 없다.
> 허브는 [services/market/sources/finance_pi.py](../services/market/sources/finance_pi.py)로 포트폴리오 히스토리·일봉·
> 매크로·기초재무·스크리너·quant 리서치를 조회한다(아래 로컬 설정 참고).

## 운영 모델

- 각 프로젝트는 혼자 배포할 수 있어야 한다. 허브는 데이터와 내비게이션을 조합할 뿐 형제 코드를 들여오지 않는다.
  공용 셸·토큰·발행 헬퍼는 `scripts/sync-ecosystem.mjs`가 형제 저장소에 **사본으로** 넣는다(허브 런타임 의존 아님).
- `/admin.html`은 holding_value·cps·gold_gap의 `config.json`을 편집한다. **Pi 로컬 체크아웃에만** 검증 후 원자적으로
  쓰고 GitHub로 push하지 않는다. 형제 Actions는 커밋된 `config.json`을 읽으므로 허브 편집은 형제 대시보드 데이터에
  반영되지 않는다(형제 자체 `admin.html`과도 경합한다).
- 브라우저로 나가는 연결 설정은 `window.APP_CONFIG.integrations`(형제별 설정)와 `window.APP_CONFIG.ecosystem`
  (레지스트리 공개 투영)이다. `/app-config.js`가 형제 로컬 파일이 있으면 읽고, 없으면 공개 Pages URL로 폴백한다.
  `/api/integrations`로 현재 서버가 내보내는 설정을, `/api/ecosystem`으로 레지스트리 투영과 형제별 신선도를 본다.
- kis-proxy: 허브는 서버에서만 호출한다(운영 기본 `http://127.0.0.1:3288`). 형제 4개(holding_value, cps, spac-hunter,
  buybacks)의 **브라우저**는 kis-proxy HTTPS `:3298`을 실시간 시세 경로로 쓴다. 공개 토큰과 IP당 레이트리밋을 전제로 한
  설계다.
- `scripts/sync-linked-projects.ps1`은 개발 PC에서 빠진 형제 저장소를 clone하고 원격 상태를 fetch한다(dirty 트리는 건드리지 않음).
- 운영 서버에서는 `linked-projects-sync.timer`(매시 05분)가 `scripts/sync_linked_projects.sh`로 형제 **데이터 파일만**
  덮어쓴다: `hodling-value/current.json`(origin 기본 브랜치), `gold_gap/data.json`(**`origin/data` 브랜치** — master에는 없다).
  `config.json`은 `/admin.html`이 서버에서 직접 편집하므로 제외한다. `git pull`이 아니라 파일 단위 덮어쓰기다.
- GitHub cron의 명목 주기와 실제 실행 간격은 크게 다르다(예: "10분"이 3~6시간). [ecosystem/architecture.md §2](ecosystem/architecture.md#2-데이터-흐름) 참고.

## 허브 → 형제 링크

- 허브 SPA의 형제 링크는 레지스트리 템플릿으로 만든다: `static/js/ecosystem-links.js`(브라우저), `services/ecosystem/links.py`(서버).
- 우선주 → `preferredSpread` `?code=<우선주 코드>`, 지주사 → `holdingValue` `?code=`, 스팩 → `spacHunter` `?code=`,
  자사주 → `buybacks` `?stock=`, ETF → `eiayn` `?code=<티커>`, `KRX_GOLD` → `goldGap` `?asset=gold`(+`gold_source=ny_futures`),
  `CRYPTO_BTC` → `?asset=bitcoin`.
- 투자정보 국채·환율 섹션의 "히스토리 ↗"는 `bondMate`를 `?tab=government|fx`로 연다.
- 도구 허브의 "채권·금리"(`/bonds`)는 `bondMate`를 `?embed=<탭>&theme=`로 임베드한다(탭: overview·government·policy·fx·credit·issuance,
  라벨은 레지스트리 `viewLink.labels`). `/bonds?view=<탭>`으로 탭을 직접 열 수 있다.
- labs의 '연결 대시보드' 카드는 레지스트리의 공개 형제 전부를 `/go/{tool}?theme=`로 연다.

## 보유 배지(Portfolio holding badges)

holding_value, common_preferred_spread, spac-hunter, buybacks, eiayn **다섯** 대시보드가 허브의
`/js/portfolio-held-badges.js`를 로드한다(`?v=` = 레지스트리 `heldBadges.version`, 현재 `20260930-vc`. sync 스크립트가
일치를 검사한다). eiayn은 `async`로, 나머지는 `defer`로 로드해 허브가 느려도 첫 렌더를 막지 않는다.

- 종목 라벨이 `data-portfolio-code`로 옵트인한다. 정확한 코드만 매칭하고(`.KS`/`.KQ`/`.US` 접미사만 정규화), **보유** 배지를 붙인다.
  보통주와 우선주는 다른 코드다.
- `data-portfolio-price`(표시 중인 주당 원화 가격)로 툴팁에 수량과 평가액(수량 × 표시 가격)을 보여 주고, 가격이 바뀌면 갱신한다.
  가격·수량을 모르면 0이 아니라 "확인 불가"를 보여 준다.
- eiayn은 `data-portfolio-currency`(원 통화, 환산 없음)와 `data-portfolio-aliases`(쉼표 구분 티커 별칭)도 준다. 같은 별칭은 중복 합산하지 않는다.
- React 대시보드는 빈 span 호스트를 둬서 React가 배지 자식을 reconcile하지 않게 한다.

허브 링크는 새 탭에서 먼저 `/go/{tool}`(별칭 `/api/portfolio/open/{integrationKey}`)을 방문한다. 이 1st-party 요청이 세션을 읽어
양수 수량 포지션을 `#vc-held=code:quantity,...`에 실어 레지스트리에 있는 대시보드로만 303 리다이렉트한다(eiayn 외에는 국내 코드만).
쿼리로는 `code`(buybacks는 `stock`)와 `theme`만 넘긴다. 임의 목적지는 받지 않는다. 대시보드는 fragment의 `vc-held` 세그먼트만
즉시 지우고(다른 세그먼트는 그대로 둔다) 스냅샷은 메모리에만 둔다. 수량은 모든 계좌 합계이고, 매입가·계좌·신원·자격증명은 넘기지
않는다. 서드파티 쿠키가 막혀도 동작하며, 배지는 링크를 연 시점의 보유 상태다.

스냅샷 없이 직접 방문하면 `GET /api/portfolio/held-codes`를 교차 출처 세션 쿠키로 호출한다(브라우저가 허용할 때). 손님은 빈 목록을
받는다. 응답과 리다이렉트는 `private, no-store`다. API 기반 배지는 focus/visibility에서 갱신하고 실패하면 지운다. 개인화가 실패해도
공개 대시보드는 그대로 쓸 수 있다. 배포는 허브 API·스크립트를 먼저, 다섯 대시보드 변경을 나중에 한다.

## 서버 쪽 외부 인사이트

브라우저 딥링크와 별개로 [services/ecosystem/external_tools.py](../services/ecosystem/external_tools.py)
(`fetch_external_insights`, 액션보드 신호, 종목 링크)가 형제 데이터를 요약해 AI 포트폴리오 인사이트와 카드에 쓴다.

- 도구마다 [services/ecosystem/siblings.py](../services/ecosystem/siblings.py)가 `<레지스트리 url>/summary.json`을 먼저 받는다
  (900초 캐시, `If-None-Match`, 404·계약 위반은 900초 음성 캐시, 네트워크 오류면 마지막 성공값을 1일까지). 실패하면 그 도구만
  레지스트리 `data[]`의 레거시 파일로 폴백하고, `adapters.py`가 summary를 레거시 모양으로 바꿔 기존 요약기를 그대로 쓴다.
- 모든 형제 fetch는 `MemoryTTLCache` + single-flight이고 실패 시 1일 stale을 허용한다. 액션보드의 buybacks·gold_gap 데이터도 캐시한다.
- URL은 레지스트리 도구 `url`(+`*_BASE_URL` env override)에서 만든다. `raw.githubusercontent` 주소는 레지스트리 `data[]`에만 남아 있다
  (holding_value·cps `current.json`/`config.json`, spac-hunter `current.json`/`data.json`, nps-tracker `current.json`).
- 요약 내용: holdingValue 최대 NAV 할인, preferredSpread 최대 괴리, goldGap 자산별 갭, spacHunter 공모가 대비 최대 할인,
  npsTracker 비중 상위·NAV·총액, buybacks 최신 보통주 자사주 비율 상위, bondMate 장단기 스프레드·주요국 금리·달러/원·신용 스프레드·최근 발행,
  eiayn AIYN 상위 100에서 뽑은 오늘의 ETF(허브가 등락률 보강).
- `ECOSYSTEM_SUMMARIES=0`이면 summary 단계를 끄고 항상 레거시 파일을 쓴다(비상 스위치).

로컬 설정이나 `/admin.html` 없이도 형제의 공개 Pages/raw 주소만 닿으면 동작한다.

## 공용 알림 API

서브프로젝트는 텔레그램 봇 토큰이나 카카오 OAuth 토큰을 **직접 들고 있지 않아도 된다**. 알림 채널 설정·토큰·카카오 refresh 갱신은
허브에 있고, 다른 프로젝트는 HTTP 한 번으로 발송을 위임할 수 있다. 카카오 refresh token은 갱신 때 회전하므로 여러 프로세스가
같은 토큰을 공유하면 서로를 무효화한다 — 발송 주체를 허브로 모아야 하는 이유다.

```
POST /api/internal/notify
{
  "text":   "금 시세 괴리 5% 초과",   // 필수
  "title":  "골드갭 알림",            // 선택 — 첫 줄에 📌 표기
  "source": "gold_gap",              // 선택 — 마지막 줄 "— gold_gap"
  "google_sub": "..."                // 선택 — 생략 시 활성 채널 보유 전체 사용자
}
→ {"ok": true, "sent": 2, "users": 1}
```

인증은 다른 `/api/internal/*`와 같다([routes/internal.py](../routes/internal.py)). 요청은 다음 중 하나면 통과한다.

- `X-Internal-Token` 헤더가 `.env`의 `INTERNAL_API_TOKEN`과 같다(상수 시간 비교). 토큰이 비어 있으면 어떤 헤더도 맞지 않는다.
- 프록시 헤더(`X-Forwarded-For`, `X-Real-IP`, `Forwarded`) 없는 루프백 직접 연결이다. `INTERNAL_API_TOKEN` 설정 여부와 무관하다 —
  그래서 토큰을 켜도 같은 Pi의 systemd 타이머(`curl https://127.0.0.1:3691/...`)는 계속 동작한다.

지금 실제로 호출하는 형제는 **buybacks**뿐이다(데이터·배포 워크플로 4개의 실패 알림, 저장소 시크릿 `VALUE_INVEST_NOTIFY_URL`·
`VALUE_INVEST_INTERNAL_TOKEN`, 둘 중 하나라도 없으면 건너뜀). holding_value·spac-hunter·gold_gap은 fin-commons Telegram을 직접,
finance-pi는 자체 웹훅을, morning-bell은 ntfy를 쓴다.

```bash
# 같은 Pi 의 다른 프로젝트 (loopback)
curl -s -X POST https://127.0.0.1:3691/api/internal/notify -k \
  -H 'Content-Type: application/json' \
  -d '{"text":"백테스트 완료","source":"nps-tracker"}'
```

```python
# 다른 호스트의 프로젝트 — 의존성은 httpx 뿐
import httpx

def notify(text: str, *, title: str = "", source: str = "finance-pi") -> None:
    httpx.post(
        "https://ducklove.duckdns.org:3691/api/internal/notify",
        json={"text": text, "title": title, "source": source},
        headers={"X-Internal-Token": INTERNAL_API_TOKEN},
        timeout=10,
    )
```

메시지는 텔레그램 한도 아래(3,800자)로 잘리고, 채널 단위 실패는 허브가 삼키므로 호출자는 fire-and-forget으로 쓰면 된다.

`POST /api/asset-quotes`(국내·해외·금·암호화폐 공통 시세, 인증 없음)도 있지만 지금 형제 사용처는 없다. 형제의 실시간 시세는
kis-proxy를 쓴다([ecosystem/external-data.md](ecosystem/external-data.md)).

## 기타 허브 제공 표면

- `GET /api/device/portfolio` — `X-Device-Token`(`DEVICE_API_TOKEN`, 사용자 `DEVICE_USER_EMAIL`). portfolio-epaper가 쓴다.
- `/login?return_to=<URL>` — `deps.TRUSTED_RETURN_ORIGINS`에 있는 origin으로만 복귀.
- `/go/{tool}` — 레지스트리 딥링크 303([ecosystem/ui-contract.md](ecosystem/ui-contract.md)).
- research 카탈로그 흐름: cps·eiayn이 `data/research/v1/`을 발행 → finance-pi가 검증·소비 → 허브 quant가 finance-pi에서 받는다.

## 로컬 설정과 환경변수

허브는 기본으로 저장소 한 단계 위에서 형제 프로젝트를 찾는다. `LINKED_PROJECTS_ROOT` 또는 프로젝트별 디렉터리로 바꾼다:

- `HOLDING_VALUE_DIR`
- `PREFERRED_SPREAD_DIR`
- `GOLD_GAP_DIR`

공개 기본 URL override(레지스트리 `envOverride`):

- `HOLDING_VALUE_BASE_URL`
- `PREFERRED_SPREAD_BASE_URL`
- `SPAC_HUNTER_BASE_URL`
- `BUYBACKS_BASE_URL`
- `EIAYN_BASE_URL`
- `ALL_ABOUT_GOLD_BASE_URL`
- `GOLD_GAP_BASE_URL`
- `NPS_TRACKER_BASE_URL`
- `BOND_MATE_BASE_URL`

서버 전용:

- `KIS_PROXY_BASE_URL` — 비우면 운영 프로필(프로필 미지정 포함) `http://127.0.0.1:3288`, 그 외 프로필 `http://ducklove.duckdns.org:3288`.
  값이 있으면 항상 이긴다.
- `KIS_PROXY_TOKEN` — 선택. 프록시가 `KIS_PROXY_PUBLIC_TOKENS`로 설정돼 있으면 `X-KIS-Proxy-Token`으로 보낸다.
- `KIS_PROXY_TIMEOUT_SECONDS`(기본 20), `KIS_PROXY_RATE_PER_SEC`(기본 4), `KIS_PROXY_RESPONSE_CACHE_TTL_SECONDS`(financials·dividends 캐시, 기본 3600).
- `FINANCE_PI_BASE_URL`(구 이름 `CLOSE_PRICE_API_BASE_URL`) — 기본 `http://192.168.68.84:8400`. finance-pi 내부 API
  (포트폴리오 히스토리, 일봉·종가, 매크로, 기초재무, 스크리너, quant).
- `FINANCE_PI_API_TOKEN`(구 이름 `CLOSE_PRICE_API_TOKEN`) — `X-Admin-Token`으로 보낸다. 둘 다
  [services/market/sources/finance_pi.py](../services/market/sources/finance_pi.py)가 읽는다. **새 이름과 구 이름이 둘 다 있으면
  구 이름이 이긴다**(기존 `.env` 동작 보존). 옮길 때는 구 이름 줄을 지운다.
- `CLOSE_PRICE_API_ENABLED` — `0`이면 finance-pi 경로를 끄고 KIS 프록시만 쓴다. `CLOSE_PRICE_API_TIMEOUT_SECONDS`,
  `CLOSE_PRICE_API_FUNDAMENTALS_TIMEOUT_SECONDS`, `CLOSE_PRICE_API_FAILURE_COOLDOWN_SECONDS`.
- `INTERNAL_API_TOKEN` — 선택. 다른 호스트에서 `/api/internal/*`를 부를 때.
- `ECOSYSTEM_SUMMARIES` — `0`이면 형제 `summary.json` 단계를 끈다.

## AI 운영 설정

메인 admin 콘솔은 런타임 AI 설정도 맡는다.

- OpenRouter 키: admin에서 저장하면 서버에만 두고 UI에는 마스킹해 보여 준다. DB 값이 없으면 프로세스 env 또는 `.env`의
  `OPENROUTER_API_KEY`를 쓴다.
- 기능별 모델 레지스트리: `portfolio_fast`, `portfolio_balanced`, `portfolio_premium`, `wiki_qa`, `wiki_ingestion`.
- 사용 원장: 포트폴리오 인사이트, 위키 Q&A, 위키 인제스트 호출이 토큰·비용·지연을 `ai_usage_events`에 기록하고 admin이 요약한다.
