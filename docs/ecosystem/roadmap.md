# 생태계 로드맵과 확장 제안

작성일: 2026-09-30

§0은 **지금 소유자가 해야 할 일**이다(보안 먼저). §1은 이번 작업에서 미룬 기술 과제, §2는 통합된 생태계로
새로 할 수 있게 된 제품 아이디어다. 공수는 S(하루 이내)·M(며칠)·L(1주 이상).

---

## 0. 지금 필요한 소유자 조치

아무것도 push하지 않았다. 허브 master push는 곧 운영 배포이고, 형제는 push가 곧 Pages 배포다.

### 0-1. 보안(먼저)

| # | 조치 | 방법 |
|---|---|---|
| S1 | **nps-tracker 수정 배포 후 KIS 토큰 폐기** | ① nps-tracker `vc-ecosystem-2026-09`를 main에 머지·push(Pages가 `scripts/stage_pages.py` 허용 목록 `_site`만 올린다). ② `curl -I https://ducklove.github.io/nps-tracker/data/kis_token.json`과 `/data/price_cache.json`이 404인지, `data.json`·`current.json`·`summary.json`·`vc-shell.js`가 200인지 확인. ③ 노출된 KIS 액세스 토큰을 폐기(KIS `/oauth2/revokeP`)하거나 앱키를 교체하고, `KIS_APP_KEY`/`KIS_APP_SECRET`/`KIS_ACCESS_TOKEN` 시크릿을 갱신. 공개 기간에 옛 아티팩트를 누구나 받을 수 있었다 |
| S2 | `KIS_PROXY_BASE_URL` 정리 | Pi `.env`에 `http://ducklove.duckdns.org:3288`(`.env.example` 값)이 있으면 지우거나 `http://127.0.0.1:3288`로. 값이 있으면 항상 이기므로, 그대로 두면 프록시 토큰이 공용 DNS 평문으로 나간다. 운영 프로필 기본값은 이제 루프백이다 |
| S3 | `INTERNAL_API_TOKEN`(선택) | 이제 설정해도 로컬 타이머가 깨지지 않는다(루프백 직접 연결은 토큰 없이 통과). 설정하면 buybacks 저장소 시크릿 `VALUE_INVEST_NOTIFY_URL`, `VALUE_INVEST_INTERNAL_TOKEN`(= 허브 토큰)을 넣어야 실패 알림이 도착한다. 지금은 둘 중 하나라도 없으면 알림을 건너뛴다 |
| S4 | buybacks `VITE_KIS_PROXY_URL` | `secrets.KIS_PROXY_URL`이 공개 번들에 인라인된다. 비밀이 아니므로 repository variables로 옮긴다 |

### 0-2. 배포 순서

1. **허브 먼저** — `ecosystem-2026-09`를 master에 머지(= 배포). 레지스트리·`/go`·summary 로더·보유 배지 `?v=20260930-vc`가 먼저 있어야 한다.
   형제는 허브가 옛 버전이어도 동작하지만(summary 404면 레거시 폴백), eiayn은 레지스트리 갱신 후에 push하라는 조건이 있다.
2. **nps-tracker**는 보안 때문에 허브와 무관하게 즉시 가능(S1).
3. 나머지 Pages 형제(holding_value, common_preferred_spread, spac-hunter, buybacks, eiayn, gold_gap, all-about-gold, bond-mate) —
   `vc-ecosystem-2026-09` → 기본 브랜치. 순서 무관.
4. **수동 배포** 5곳 — `the_admin`(pull + `the-admin.service` 재시작, 먼저), `portfolio-epaper`(pull + `pip install -e` + 재시작),
   `kis-proxy`(pull + `systemctl --user restart kis-proxy` + `curl localhost:3288/health`), `index-popup`(pull + `npm ci` + build + 재시작),
   `finance-pi`(main 머지 후 Pi에서 `bash ops/deploy.sh`). `morning-bell`은 main 머지 후 2분 안에 자동 배포.
5. **all-about-gold 발행기** — pi-control의 발행 체크아웃을 갱신해야 새 `ops/update_pages.py`(+ `scripts/summary.py`, `scripts/vc_publish.py`)가
   돈다. 안 하면 매일 무변경 커밋·배포가 계속된다.
6. 허브 배포 뒤 `scripts/sync_linked_projects.sh`를 한 번 돌려 `ok: …/gold_gap/data.json <- origin/data`가 찍히는지 확인.

### 0-3. 일정이 있는 작업

| 기한 | 조치 |
|---|---|
| **2026-11-19 전**(WARN은 2026-10-20부터) | 수능일 KRX 공지를 확인해 Pi `.env`에 `PORTFOLIO_MARKET_SESSIONS={"2026-11-19":"HH:MM"}`(휴장이면 `null`)을 넣고 재시작. 마감이 15:30보다 늦으면 NAV·장후·장마감 브리핑 타이머도 확인. 설정 전에는 그날 정산·브리핑이 500으로 실패하고 OnFailure 알림이 온다 |
| **2027-11-01 무렵** | KRX 2028 휴장일을 허브 `domain/market_calendar.py` `HOLIDAYS`와 finance-pi `trading_calendar.py`에 추가. 그 뒤로는 오늘+60일이 2028에 들어가 `tests/test_market_calendar.py`(= 배포 게이트)와 finance-pi 테스트가 실패한다 |

### 0-4. 결정 필요

| 항목 | 내용 |
|---|---|
| eiayn 색 관례 | 상승·하락 색이 서구식(초록/빨강)에서 한국식(빨강/파랑)으로 바뀐다. 머지 전 확인 |
| finance-pi 가격·매크로 타임아웃 | 원래부터 `timeout=None`(무제한)이다. `CLOSE_PRICE_API_TIMEOUT_SECONDS`(2.5초)는 이 호출에 적용되지 않는다. 값을 정해 고칠지 결정 |
| finance-pi env 우선순위 | `FINANCE_PI_*`와 `CLOSE_PRICE_API_*`가 둘 다 있으면 **구 이름이 이긴다**. Pi `.env`에 둘 다 다른 값으로 있는지 확인 |
| 형제 `?v=` 라벨 | holding_value·spac-hunter·gold_gap·nps-tracker의 셸·토큰 라벨을 날짜(`20260930-vc`)에서 `1.1.0`으로 바꿨다. 날짜 라벨을 허용하려면 검사를 완화하고 그 커밋을 되돌린다 |
| kis-proxy 뒤 프록시 | 루프백이 아닌 리버스 프록시 뒤에 두면 `KIS_PROXY_TRUSTED_PROXIES`를 설정해야 한다. 아니면 모든 클라이언트가 한 IP로 레이트리밋된다 |
| the_admin `epaper-dashboard :3266` | `stale: true`로 표시했다. Pi에서 아직 도는지 확인하고, 아니면 항목 삭제 |

### 0-5. 배포 후 확인

- `https://ducklove.github.io/<tool>/summary.json`이 200(holding_value·cps·spac-hunter·buybacks·nps-tracker는 머지 직후, gold_gap·bond-mate는 다음 `update-data` 실행 후, eiayn·all-about-gold는 다음 Pages 빌드 후).
- `GET /api/ecosystem`의 `freshness`에 도구별 `asOf`가 채워지는지.
- `system_events`에서 `source=dart kind=quota_exhausted`, 로그에서 `Yahoo 429` 경고.
- buybacks 첫 데이터 실행은 `public/data/buybacks/*.json`을 한 줄 1레코드 형식으로 다시 쓴다(큰 diff, 내용 변화 없음).

---

## 1. 기술 로드맵(이번에 미룬 것)

| 순위 | ID | 제안 | 이유 | 공수 | 선행 조건 |
|---|---|---|---|---|---|
| 1 | R-1 | **Pi 디스패처로 형제 데이터 워크플로 구동** — nps-tracker `nps-trigger.sh` 패턴을 허브 저장소의 매니페스트 기반 systemd 타이머로 일반화. KRX 달력·장 시간을 보고 `gh workflow run`, 실패하면 `/api/internal/notify`. GitHub cron은 드문 폴백으로 | "10분·30분" cron이 실제로는 3~7시간 간격이다. 허브 위젯·목표가·알림이 묵은 값을 쓴다 | M | Pi용 fine-grained PAT(workflow 권한), 레지스트리에 워크플로 이름 필드 |
| 2 | R-2 | **finance-pi 공개 정적 번들** — 일일 실행 뒤 envelope v1 번들(최근 N일 전 종목 종가·거래량·상장주식수 `adjusted` 명시, `macro_latest.json`, `calendar.json`)을 data 브랜치/Pages에 발행(all-about-gold와 같은 deploy key 방식). 형제는 번들 우선, 기존 수집기 폴백 | GitHub 러너가 LAN API에 못 닿아 EOD를 약 1만 건/일 다시 받는다 | M | deploy key, 번들 스키마를 `config/schemas/`에 추가 |
| 3 | R-3 | **fin-commons v0.3 → v0.4** — v0.3: `revisions`, `guards.incremental_start`, `jsonpub`(split 출력), `kis.signed_value` + 테스트 벡터, composite action `commit-push-retry`·`report-failure`. v0.4: `finance_pi`(env 별칭·health·쿨다운), `opendart`(corpCode 캐시·페이징·상태코드·리미터), `http.fetch` | 바이트 동일 파일, 워크플로 블록 8개 이상, 클라이언트 11개 | L | 태그 발행 승인, 저장소별 파사드 PR |
| 4 | R-4 | **kis-proxy를 캐시된 시세 게이트웨이로** — 배치 `/v1/stocks/quotes?symbols=`(세션에 따라 KIS/Naver), 앱키별 토큰 버킷, intraday `since=`, 장외·휴장 수집 생략, financials·dividends·overview 6~24시간 캐시, 헬스 알림. 소비자(형제 Actions·브라우저, 허브, index-popup)를 하나씩 이전하고 직접 폴백 유지 | 같은 시세 요청이 소비자마다 KIS·Naver·Yahoo로 나간다. 형제 KIS 토큰 5개가 같은 앱키로 서로 무효화할 수 있다 | L | 이번 kis-proxy 변경(2초 캐시·근월물 캐시·리미터) 배포 |
| 5 | R-5 | **수 MB 파생 데이터 커밋 중단** — cps `data.js`(약 27 MB), holding_value `data.js`(약 13 MB), spac·nps `data.js`는 Pages 아티팩트에서만 생성, buybacks 백필 raw는 gitignore | git 저장소와 모든 Pages 배포가 비대해진다 | M | 소비자 이전(허브 integrations의 cps `dataUrl` 제거 등) |
| 6 | R-6 | **토큰 alias 2단계 + 허브 admin·로그인 페이지** — 표면·텍스트 토큰을 도구별 스크린샷 비교 후 alias. 허브 `admin.html`·서버 렌더 로그인·관리자 포트폴리오 페이지에 `vc-tokens.css`와 theme-boot(다크 지원) | 같은 생태계 안에서 팔레트·다크 지원이 다르다 | M | 도구별 before/after 스크린샷, 대비 검사 |
| 7 | R-7 | **iframe 프로토콜 완성** — nps-tracker 보유표 행 클릭 → `vc:open-stock`(embed 모드), nps-tracker·index-popup `vc:height`, 레지스트리에 postMessage 지원 플래그, bond-mate 구 형식 메시지 제거 | 지금은 테마만 postMessage이고 높이·종목 이동은 일부만 | S | 허브 쪽은 이미 수신 가능 |
| 8 | R-8 | **CSP 강제** — 지금 `Content-Security-Policy-Report-Only`. theme-boot 같은 인라인 스크립트의 sha256을 sync 스크립트가 계산해 헤더에 넣고, 보고를 확인한 뒤 강제 | XSS 방어 심화 | M | 인라인 스크립트 목록 확정, 보고 수집 |
| 9 | R-9 | **x3를 허브 디바이스 API로** — x3 `briefing.py`가 SSH로 `cache.db`를 직접 SQL로 읽어 NAV·현금흐름을 재구현한다(분배금 누락). portfolio-epaper처럼 `/api/device/portfolio` 사용 | 스키마 결합 제거, 계산 불일치 제거 | S | 디바이스 토큰 발급 |
| 10 | R-10 | **the_admin ⇄ 레지스트리 병합** — the_admin `registry.yaml`의 서비스·포트·시크릿 목록과 `config/ecosystem.json` 내부 항목을 상호 참조하거나 한쪽에서 생성 | 호스트·포트 정보가 두 곳에 있다 | M | 어느 쪽을 정본으로 할지 결정 |
| 11 | R-11 | **KRX 휴장일 정본** — 허브 `config/krx-holidays.json`(버전 포함) → finance-pi·fin-commons로 동기화, 허브 안 평일 판정 여러 곳을 `domain.market_calendar`로 | 두 표를 손으로 맞춘다. 2028 추가가 반복된다 | M | 미포함 연도 정책 결정 |
| 12 | R-12 | **bond-mate를 서버 금리의 source of record로** — 허브 서버가 정책금리·ECOS·MOF·BOJ·SOFR 코드를 bond-mate `current.json`에서 읽고(플래그 뒤 shadow 모드 1주) 스크레이퍼 약 500줄 제거. 장중 CNBC·Yahoo·Naver 코드는 유지 | 같은 소스를 두 곳에서 수집 | M | 코드 매핑·커버리지 비교 |
| 13 | R-13 | **형제 README '생태계 계약' 절** — 각 형제가 남에게 쓰이는 파일·필드·파라미터·embed 모드를 명시하고 이 문서로 링크 | 스키마 변경이 허브·finance-pi를 조용히 깨뜨릴 수 있다 | S | — |
| 14 | R-14 | **보유 배지 immutable 캐시** — 레지스트리 `heldBadges.version`이 파일 sha1 앞 10자를 따라가게 해 형제의 핫링크도 1년 캐시 | 지금은 버전이 콘텐츠 해시가 아니라 매번 재검증 | S | sync 스크립트가 형제 태그를 갱신 |
| 15 | R-15 | **finance-pi catchup 재시도 단계화·pykrx 의존성 선언** | 매크로 하나 실패로 과거 날짜 전체를 재실행하고 `/api/ready`가 503 | M | — |
| 16 | R-16 | **형제 실시간 시세 배치화** — holding_value(145회 직렬), cps(종목별 107회), spac-hunter(영숫자 코드 개별 호출)를 kis-proxy 배치로 | 이번에 보류: 배포된 배치가 Naver 기반이라 스냅샷 출처 의미가 바뀐다 | S~M | kis-proxy 영숫자 배치 배포(완료 코드), 출처 라벨 합의 |
| 17 | R-17 | **admin config 편집을 GitHub로 반영** — 허브 `/admin.html`의 형제 `config.json` 편집이 Pi 로컬에만 쓰인다. GitHub Contents API로 PR/커밋을 만들거나 편집 기능을 형제 admin으로 일원화 | 형제 Actions는 커밋된 config를 읽어 허브 편집이 반영되지 않는다 | M | 토큰 범위 결정 |
| 18 | R-18 | **`?from` 유입 집계** — `analytics.js`가 `from`을 `vc_from` 이벤트 파라미터로 기록 | 도구 간 이동 측정 | S | GA 속성 정의 |

---

## 2. 새 제품 아이디어

레지스트리·summary.json·`/api/ecosystem`·공용 셸·공용 알림이 생기면서 가능해진 것들이다.

| 순위 | ID | 아이디어 | 무엇을 쓰나 | 공수 | 선행 조건 |
|---|---|---|---|---|---|
| 1 | N-1 | **도구 상태(신선도) 패널** — 허브 labs나 admin에 도구별 `asOf`·마지막 확인·stale·오류를 표로. 기준보다 오래되면 경고 배지, 연속 실패면 알림 | `/api/ecosystem` `freshness`, data-contract §7 신선도 허용치 | S | 지금 `freshness`는 로더가 한 번 돈 도구만 채운다. 주기적으로 summary를 미리 받는 작업(타이머나 lifespan 루프) 추가 |
| 2 | N-2 | **종목 360° 페이지** — 한 종목에 대해 모든 도구의 신호를 모은다: 지주사 할인율(holding_value), 우선주 괴리·백분위(cps), 자사주 비율(buybacks), 국민연금 비중(nps-tracker), SPAC 청산가치 할인(spac-hunter), 편입 ETF(eiayn). 허브 분석 탭의 섹션 또는 `/analysis?code=…&view=360` | 각 형제 summary.json(이미 허브에 캐시), 레지스트리 `stockLink` | M | summary 스키마에 종목별 필드가 있는 도구부터(cps·buybacks·nps·spac). eiayn 편입 종목은 새 필드 필요 |
| 3 | N-3 | **교차 도구 관심종목·알림** — 사용자가 종목별 조건을 건다(예: 우선주 괴리 백분위 90 이상, 지주사 할인 확대, 자사주 소각 공시, 국민연금 신규 편입, 김프 5% 초과). 허브 알림 엔진이 summary 갱신 때 평가하고 기존 채널(Telegram·Kakao)로 보낸다 | `services/notifications/engine`, summary.json, 알림 채널 | M | N-1의 주기적 summary 갱신. 형제 쪽 이벤트는 `/api/internal/notify`로도 보낼 수 있다 |
| 4 | N-4 | **모든 대시보드의 포트폴리오 컨텍스트** — 보유 배지를 nps-tracker 보유표·all-about-gold ETF 카드로 확대하고, 배지 툴팁에 '포트폴리오에서 보기'(`/portfolio?focus=CODE`) 링크. 보유 비중 오버레이(예: cps 표에 내 보유 수량) | `portfolio-held-badges.js`(fragment 보존 완료), `/portfolio?focus` | S~M | 레지스트리 `heldBadges: true` 전환, 형제 `data-portfolio-code` 마크업 |
| 5 | N-5 | **생태계 검색** — 허브 검색창에서 종목을 찾으면 그 종목을 다루는 도구를 함께 보여 준다(`accepts` 정규식 + summary 포함 여부). 셸 팝오버에도 같은 검색 | 레지스트리, summary 캐시 | S | — |
| 6 | N-6 | **주간 생태계 다이제스트** — 주 1회 summary.json 변화(괴리 상위 변동, 새 SPAC, 소각 공시, 국민연금 비중 변화, 김프, 금리 커브)를 보유 종목 중심으로 요약해 기존 브리핑 채널로 발송 | `services/daily_briefing` 인프라, summary 스냅샷 보관(주간 diff용) | M | summary 이력 저장 테이블 |
| 7 | N-7 | **디바이스 카드 확장** — e-paper·X3에 '오늘의 생태계 신호' 카드(보유 종목에 걸린 신호 3개) | `/api/device/portfolio` 확장 | S | R-9(x3 API 전환) |
| 8 | N-8 | **형제 간 크로스 링크 확대** — 이번에 생긴 cps→holding_value, gold_gap↔all-about-gold, all-about-gold ETF→eiayn 외에 spac-hunter 합병 완료 → 허브 분석, buybacks 기업 → cps(우선주 있으면), nps-tracker 행 → holding_value(지주사면) | `VCShell.linkTo` + 레지스트리 `accepts` | S | — |

## 관련 문서

- [architecture.md](architecture.md) · [external-data.md](external-data.md) · [modularization.md](modularization.md) · [ui-contract.md](ui-contract.md)
