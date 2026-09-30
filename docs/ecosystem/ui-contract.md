# UI 계약 — 딥링크·테마·공용 셸

작성일: 2026-09-30 · 적용 대상: 허브 SPA + 공개 형제 10개(Pages 9개 + index-popup)

형제 대시보드 관리자가 지켜야 할 규칙이다. 도구 목록과 링크 템플릿의 정본은
[config/ecosystem.json](../../config/ecosystem.json), 셸·토큰·theme-boot의 정본은
[static/ecosystem/](../../static/ecosystem/)이다. 형제 저장소의 사본은 직접 고치지 않는다.

---

## 1. 딥링크 계약 v1

### 1-1. 공통 inbound 파라미터

| 파라미터 | 의미 | 규칙 |
|---|---|---|
| `?theme=light\|dark` | 시각 테마 | 적용만 하고 **저장하지 않는다**. 다른 값은 무시한다. 다른 의미로 쓰지 않는다(eiayn은 카테고리 필터를 `?etf_theme=`로 옮겼다) |
| `?code=<코드>` | 종목 포커스 | 대소문자 무시. 대상이 없으면 조용히 기본 화면. 선택이 바뀌면 `history.replaceState`로 되쓴다. buybacks는 `?stock=`이 정식이고 `?code=`는 별칭 |
| `?embed[=<view>]` | 크롬 없는 임베드 | 값이 없거나 `0`·`false`가 아니면 embed. 값은 선택적 뷰 이름. theme-boot가 `html[data-embed]`를 설정한다. index-popup은 `?headless=1`이 같은 뜻 |
| `?from=<toolId>` | 유입 출처 | 레지스트리 id만 의미가 있다. 동작을 바꾸지 않는다(허브 분석만 '돌아가기' 칩을 그린다) |
| `#vc-held=CODE:QTY,…` | 허브가 넘긴 보유 스냅샷 | `portfolio-held-badges.js`가 읽고 즉시 지운다. 다른 fragment 세그먼트(`#gold-history` 등)는 원문 그대로 남긴다 |
| `?vc-shell=0` | 에코시스템 바 끄기 | 디버그·스크린샷용 |

도구 고유 파라미터는 자유지만 위 이름과 겹치면 안 된다.

### 1-2. 도구별 파라미터

레지스트리 템플릿(허브·셸이 만드는 링크)과 도구가 따로 받는 파라미터다.

| 도구 | 레지스트리 템플릿 | 도구 고유 inbound |
|---|---|---|
| holding_value | `?code={code}` · embed `?embed=1` | — |
| common_preferred_spread | `?code={code}` · `?embed=1` | — |
| spac-hunter | `?code={code}` · `?embed=1` | `?filter=` `?sort=` |
| buybacks | `?stock={code}` · `?embed=1` | `?market=` `?types=` `?year=` `?search=` |
| eiayn | `?code={code}`(`^[A-Z0-9][A-Z0-9.-]{0,29}$`) · `?view={list\|ranking\|analysis\|compare}` · `?embed=1` | `?etf_theme=` `?compare=` `?active=` `?q=` `?market=` `?provider=` `?risk=` `#section`. `?embed=<view>`는 `?view`가 없을 때 뷰 선택 |
| gold_gap | `?asset={asset}` · `?embed=1` | `?<asset>_source=`(예: `gold_source=ny_futures`) `?range=` `?lang=` |
| all-about-gold | `#{view}`(섹션 앵커) · `?embed=1` | — |
| nps-tracker | `?code={code}`(행 하이라이트·스크롤) · `?embed=1` | — |
| bond-mate | `?tab={overview\|government\|policy\|fx\|credit\|issuance}` · embed `?embed={view}` | `?bg=` `?data=` |
| index-popup | `?index={view}` · embed `?headless=1` | `?period=` `?controls=` `?compact=` `?refresh=` `?bg=` `?locale=` |

템플릿 값은 `accepts` 정규식을 통과해야 붙는다. 통과하지 못하면 조용히 빠지고 도구 홈으로 간다.

### 1-3. 허브 inbound 라우트

| 라우트 | 동작 | 구현 |
|---|---|---|
| `/investing` `/analysis` `/portfolio` `/nps` `/labs` `/tools` `/insights` `/screener` `/masters` `/bonds` `/quant` | SPA 뷰 | `core/static_routes.py` `SPA_PATHS` |
| `/analysis?code=005930`(또는 `?code=`가 붙은 아무 경로) | 분석 탭으로 가서 종목 분석. 분석이 성공하면 URL을 `/analysis?code=CODE`로 되쓴다(`theme`·`focus`·`view` 제거, `from`은 같은 종목일 때만 유지). 뒤로/앞으로 가기에서 종목이 다를 때만 다시 분석 | `static/js/analysis.js`, `app-main.js` |
| `?theme=light\|dark` | head의 theme-boot가 첫 페인트 전에 적용. 저장하지 않고, 토글하면 URL에서 지운다 | `static/index.html` theme-boot 블록, `search.js` |
| `?from=<toolId>` | 분석 헤더 `#analysisEcoLinks`에 '← {도구명}(으)로 돌아가기' 칩(같은 탭, 원 도구의 stockLink). 알 수 없는 id나 내부 도구는 무시. 같은 줄에 해당 종목을 받는 '연결 도구' 칩 | `analysis-valuation.js`, `ecosystem-links.js` |
| `/bonds?view=<tab>` | bond-mate 탭을 열고, 탭을 바꾸면 URL을 되쓴다(`overview`는 파라미터 생략) | `market-bond-mate.js` |
| `/portfolio?focus=CODE` | 보유 행으로 한 번 스크롤하고 4초 강조. 보유하지 않은 코드면 조용히 포기 | `app-main.js`, `portfolio-render.js` `pfFocusHolding` |
| `/go/{tool}?code=&view=&asset=&theme=&embed=` | 레지스트리 템플릿으로만 목적지를 만들어 303. `theme`은 light/dark만. handoff 도구는 `#vc-held` 스냅샷을 붙인다. 알 수 없는·내부 도구는 404, 잘못된 기본 URL은 503. `private, no-store`, `no-referrer` | [routes/ecosystem.py](../../routes/ecosystem.py), [services/ecosystem/links.py](../../services/ecosystem/links.py) |
| `/api/portfolio/open/{integrationKey}?code=&stock=&theme=` | `/go` handoff 경로의 별칭(옛 링크 호환) | `routes/portfolio.py` |
| `/login?return_to=<URL>` | 로그인 후 `TRUSTED_RETURN_ORIGINS`에 있는 origin으로만 복귀 | `routes/auth.py`, `deps.py` |

`/go`는 **허브에서 나가는 링크 전용**이다. 형제끼리의 링크는 허브를 거치지 않는다(`VCShell.linkTo`).

---

## 2. 테마 규칙

```
유효 테마 = ?theme(light|dark, 저장 안 함)
         > localStorage['theme'](light|dark, 없으면 auto)
         > prefers-color-scheme
적용: <html data-theme="light|dark"> 를 항상 명시한다(CSS 기본값에 기대지 않는다)
```

- **공용 키는 `theme` 하나다.** `ducklove.github.io`의 9개 대시보드는 같은 origin이라 이 키를 공유한다.
  theme-boot가 레거시 키(`bondmate.theme`, `eiayn:theme:v1`, `spac-hunter-theme`, `preferred-theme`, `nps-theme`)를
  `theme`이 없을 때 한 번 옮긴다.
- 허브(`:3691`)와 index-popup(`:3358`)은 origin이 달라 저장소를 공유하지 못한다. 다른 origin 사이에서는 링크의
  `?theme=`와 iframe `vc:theme` 메시지만 쓴다. `VCShell.linkTo`와 허브의 레지스트리 링크는 현재 테마를 자동으로 붙인다.
- 같은 origin의 다른 탭은 `storage` 이벤트로 따라온다. 저장값이 없으면 OS 설정 변화도 따라간다.
- 테마가 바뀌면 `document`에 `vc:themechange`(`detail.theme`)가 발생한다. 캔버스·차트 재그리기는 여기에 건다.
- 도구 자체 토글은 `VCShell.setTheme('light'|'dark'|'auto')`를 부르게 바꾼다(셸이 없으면 기존 로직). `auto`는 키를 지운다.
  셸 바의 토글은 `theme-toggle` 속성으로 opt-in이다.
- **pre-paint**: theme-boot 블록은 모든 스타일시트보다 먼저 `<head>`에 인라인으로 둔다. 다크 사용자의 흰 화면
  깜빡임을 막는다. 블록 내용은 `sync-ecosystem.mjs`가 주입·검증한다.

---

## 3. vc-shell 사용 가이드(형제 관리자용)

### 3-1. 마크업

```html
<head>
  <!-- vc:theme-boot --><!-- /vc:theme-boot -->   <!-- 비워 두고 sync --write 가 채운다. 모든 CSS 보다 먼저 -->
  <link rel="stylesheet" href="./vc-tokens.css?v=1.1.0">   <!-- 자체 CSS 보다 먼저 -->
  <link rel="stylesheet" href="css/app.css">
  <script defer src="./vc-shell.js?v=1.1.0"></script>
</head>
<body>
  <vc-shell tool="holding_value"><a class="hub-link" href="https://ducklove.duckdns.org:3691" rel="noopener">Value Compass ↗</a></vc-shell>
  …
```

- 경로는 레지스트리 `vendor.src`(예: `./`, `./assets/`, `./static/`, Vite는 `%BASE_URL%`)를 따른다.
- `?v=` 라벨은 `VCShell.version`(지금 `1.1.0`)과 같아야 한다. 다르면 sync verify가 실패하고 `--write`가 고친다. 라벨이 없어도 통과한다.
- `<vc-shell>` 안의 앵커는 **폴백**이다. 스크립트가 막히거나 실패해도 지금과 같은 허브 링크가 보인다.
- 셸은 embed(`?embed`, `?headless=1`, `?vc-shell=0`, iframe 안)일 때 스스로 숨는다.

### 3-2. React 호스트 규칙(buybacks, eiayn, index-popup)

`<vc-shell>`은 `index.html`에서 `<div id="root">`의 **형제**로 둔다. React가 관리하지 않는 노드라 reconcile과
충돌하지 않는다. 앱 상태는 명령형 API로만 넘긴다.

```jsx
useEffect(() => {
  window.VCShell?.setStock(/^[0-9A-Z]{6}$/.test(code) ? code : null, name);
}, [code, name]);
```

index-popup은 대시보드 모드(`?index` 없음)에서만 셸을 보여 준다.

### 3-3. 공개 API — `window.VCShell`

| 멤버 | 설명 |
|---|---|
| `version` | `'1.1.0'` |
| `registry`, `tools` | 인라인 공개 레지스트리(네트워크 요청 없음) |
| `getTheme()` / `setTheme(t)` | 유효 테마 / `'light' \| 'dark' \| 'auto'` 설정·저장·적용·이벤트 |
| `setStock(code, name?)` | 셸 바에 '허브에서 분석 ↗' 칩을 띄운다. `null`이면 해제. 허브 코드 형식(`^[0-9A-Z]{6}$`)이 아니면 칩이 안 나온다 |
| `linkTo(toolId, {code, view, asset})` | 레지스트리 템플릿 → URL. `theme`과 `from=<현재 도구>`를 붙인다. `accepts`에 안 맞는 값은 뺀다 |
| `hubAnalysisUrl(code)` | 허브 분석 딥링크(`/analysis?code=…`), 형식이 안 맞으면 `null` |
| `icon(name)` | 레지스트리 아이콘 SVG 문자열 |

요소 속성: `tool`(필수, 레지스트리 id), `variant="menu"`(▾ 전환 버튼만 — 허브 헤더가 사용), `stock`, `stock-name`,
`theme-toggle`. 이벤트: `vc:themechange`.

`setStock` 호출 시점 예: 목록에서 종목을 선택할 때(holding_value, cps, spac-hunter, buybacks, nps-tracker 행 클릭),
eiayn 국내 ETF 분석 뷰. 종목 개념이 없는 도구(gold_gap, all-about-gold, bond-mate, index-popup)는 부르지 않는다.

---

## 4. 토큰 — `vc-tokens.css`

- 접두어 `--vc-`는 안정 계약이다. 이름은 추가만 하고 바꾸거나 지우지 않는다. 값은 허브 `static/css/base.css`와 같다
  (`tests/test_ecosystem_registry.py`가 대조). 허브는 토큰을 도입해도 시각 변화가 없다.
- 라이트/다크 블록과, `data-theme`이 없을 때의 `prefers-color-scheme` 블록이 있다. 두 다크 블록이 같은지 sync가 검사한다.
- **한국식 방향색**: 상승 = 빨강(`--vc-up` `#b91c1c` / 다크 `#fca5a5`), 하락 = 파랑(`--vc-down` `#1d4ed8` / 다크 `#93c5fd`).
  상태색(`--vc-success`/`--vc-danger` …)은 시장 방향과 별개다.
- 주요 토큰: 브랜드 `--vc-brand`·`--vc-link`·`--vc-tool-accent`(도구가 override), 표면 `--vc-bg`·`--vc-surface`·`--vc-surface-2`·`--vc-border`,
  텍스트 `--vc-text`·`--vc-text-muted`·`--vc-text-faint`, 모양 `--vc-radius-*`·`--vc-space-*`·`--vc-shadow-*`·`--vc-focus-ring`,
  글꼴 `--vc-font-sans`·`--vc-font-mono`, 바 높이 `--vc-bar-h`.
- **도입은 alias로 한다.** 각 도구는 자기 변수 이름을 두고 값만 `var(--vc-*, <옛 값>)`으로 바꾼다. 1단계(완료)는 방향색과
  본문 글꼴만이다. 표면·텍스트(2단계)는 도구별 스크린샷 비교 후 진행한다.

```css
:root, :root[data-theme="light"], :root[data-theme="dark"] {
  --up: var(--vc-up, #d1433f);
  --down: var(--vc-down, #2a78c9);
}
body { font-family: var(--vc-font-sans, -apple-system, sans-serif); }
```

- eiayn은 이번에 서구식(상승 초록·하락 빨강)에서 한국식으로 바꿨다. 소유자 확인이 남아 있다([roadmap.md §0](roadmap.md#0-지금-필요한-소유자-조치)).

---

## 5. iframe 메시지 프로토콜

허브가 임베드하는 자식: nps-tracker(`/nps`), bond-mate(`/bonds`), index-popup(투자정보 지수 위젯).

| 방향 | 메시지 | 규칙 |
|---|---|---|
| 자식 → 부모 | `{source:'vc', type:'vc:ready', tool, features?}` | 로드 후 한 번. 대상 origin은 허브 origin(레지스트리 `hub`), `'*'` 금지 |
| 부모 → 자식 | `{source:'vc', type:'vc:theme', theme}` | `vc:ready`를 보낸 자식에게만. 아직 안 보낸 자식은 기존대로 `src`를 다시 할당해 리로드 |
| 자식 → 부모 | `{source:'vc', type:'vc:height', tool, height}` | 허브가 200~20000px로 제한해 iframe 높이로 쓴다 |
| 자식 → 부모 | `{source:'vc', type:'vc:open-stock', tool, code}` | 허브가 분석 탭으로 이동. 코드는 `^[0-9A-Z]{6}$`만 |
| 자식 → 부모(구 형식) | `{source:'bond-mate', type:'height', height}` | bond-mate 호환용, 병행 송신 중 |

- 부모는 메시지가 **등록된 iframe(`data-vc-tool`)의 contentWindow**에서 왔고 origin이 그 도구의 레지스트리 URL origin과
  같을 때만 받는다([static/js/ecosystem-links.js](../../static/js/ecosystem-links.js)). nps-tracker와 bond-mate는 같은
  github.io origin이라 origin만으로는 구분할 수 없기 때문이다.
- 자식은 `vc:theme`을 허브 origin에서 온 것만 받는다(vc-shell이 처리, 셸이 없는 도구는 자체 처리). 테마를 저장하지 않는다.
- 현재 자식 지원: nps-tracker(`vc:ready` + `vc:theme`), bond-mate(`vc:ready` + `vc:height` + `vc:theme` + 구 형식),
  index-popup(`vc:ready` + `vc:theme`). `vc:open-stock`은 허브만 받을 준비가 됐고 보내는 자식은 아직 없다.
- 허브는 index-popup을 `referrerpolicy="no-referrer"`로 연다. 자식은 referrer 없이도 신뢰할 부모를 판단해야 한다.

---

## 6. 새 대시보드를 생태계에 추가하기

1. **레지스트리 항목** — [config/ecosystem.json](../../config/ecosystem.json) `tools[]`에 추가한다. 필수: `id`(저장소 이름,
   `^[a-z0-9][a-z0-9_:-]*$`), `name`, `description`, `category`, `icon`(허용 목록), `accent`(`#rrggbb`), `url`(https,
   사설 주소·내부 포트·쿼리 금지), `themeParam`, `handoff`, `heldBadges`, `data`(배열), `repo`, `branch`, `deploy`,
   `visibility`. 선택: `integrationKey`, `envOverride`, `stockLink`/`viewLink`/`assetLink`(`template` + `accepts`),
   `embed`, `hubView`, `vendor`. 처음에는 `vendor.shell`/`vendor.themeBoot`를 `false`로 둔다.
2. **허브 검증** — `python -m pytest -q tests/test_ecosystem_registry.py`, `node scripts/sync-ecosystem.mjs --write --hub-only`
   (vc-shell.js 인라인 블록 재생성). `integrationKey`가 있으면 `services/ecosystem/integrations.py`의
   `build_public_integrations`가 그 키를 내보내야 한다(테스트가 확인).
3. **형제에 벤더링** — 형제 저장소를 허브 옆에 도구 id와 같은 디렉터리 이름으로 두고
   `node scripts/sync-ecosystem.mjs --write --only <id>`. 셸·토큰·(선택) `vc_publish` 헬퍼가 `vendor.dir`에 복사된다.
4. **채택** — 형제에서 §3-1 마크업을 넣고 `--write`로 theme-boot 블록을 채운다. 토글을 `VCShell.setTheme`으로 연결하고,
   종목 개념이 있으면 `setStock`을 부르고, 방향색·글꼴 alias(§4)를 넣는다.
5. **레지스트리 플래그** — `vendor.shell`/`vendor.themeBoot`를 `true`로 바꾸고 `tests/test_ecosystem_registry.py`의
   `ADOPTED_SIBLINGS`를 갱신한다. 이제 sync verify가 채택 상태를 엄격하게 검사한다.
6. **데이터(선택)** — 허브가 요약을 읽어야 하면 `config/schemas/summary/<id>.schema.json`과
   `tests/fixtures/ecosystem/<id>.summary.json`을 먼저 추가하고, 형제가 [data-contract.md](data-contract.md)대로 발행한다.
7. **GA(선택)** — `config/analytics-projects.json`에 추가하고 `node scripts/sync-analytics.mjs --write`.
8. **테스트** — 허브 `python -m pytest -q`, `npm test`, `node scripts/sync-ecosystem.mjs --only <id>`, 형제 자체 테스트.
   허브 labs '연결 대시보드' 카드는 레지스트리에서 자동으로 생긴다(`tests/js/ecosystem-links.test.mjs`).
9. **배포 순서** — 허브(레지스트리) 먼저, 형제 나중. 형제는 허브 없이도 동작해야 한다.

### 체크리스트

- [ ] `?theme=light|dark`를 적용하고 저장하지 않는다. `theme`을 다른 의미로 쓰지 않는다
- [ ] `?embed`(0/false 제외)에서 헤더·셸을 숨긴다
- [ ] 종목형 도구는 `?code=`(또는 레지스트리 템플릿의 이름)를 받고 선택 시 URL을 되쓴다
- [ ] theme-boot 블록이 모든 스타일시트보다 먼저 있다
- [ ] `vc-tokens.css`가 자체 CSS보다 먼저, `vc-shell.js`는 `defer`
- [ ] `<vc-shell tool="<id>">` 안에 폴백 허브 링크가 있다(React는 `#root` 형제)
- [ ] 자체 토글이 `VCShell.setTheme`을 부르고, 차트는 `vc:themechange`에서 다시 그린다
- [ ] 방향색은 `--vc-up`/`--vc-down` alias(상승 빨강, 하락 파랑)
- [ ] localStorage 접근은 try/catch로 감싼다
- [ ] 허브가 내려가도 페이지가 뜬다(보유 배지 스크립트는 `defer`/`async`)
- [ ] 벤더링 사본을 직접 고치지 않았다(`node scripts/sync-ecosystem.mjs --only <id>` 통과)

## 관련 문서

- [architecture.md](architecture.md) · [data-contract.md](data-contract.md) · [../portfolio-frontend-structure.md](../portfolio-frontend-structure.md)(허브 스크립트 순서 계약)
