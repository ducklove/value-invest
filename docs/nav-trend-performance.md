# NAV 기준가 추이 성능/안정화 기록

작성일: 2026-04-30

## 사용자 증상

- `포트폴리오 > 추이 분석`에서 `NAV 기준가 추이`가 늦게 나타나거나, 오래 기다려도 빈 화면처럼 보였다.
- 비교지수 선택 후 Y축 min이 비정상적으로 낮아지는 경우가 있었다. 실제 값이 모두 양수인데도 축 하단이 음수로 잡히면 그래프가 납작해져 사용자가 핵심 추이를 읽기 어렵다.
- “보유종목” 탭과 “추이 분석” 탭을 반복해서 오가면 뒤늦게 그래프가 나타나는 경우가 있었다. 이는 데이터 자체보다 차트 초기화/레이아웃 타이밍 의존성이 강하다는 신호다.
- 사용자는 서버 로딩까지 느려진 것으로 체감했다. 직전 구현에서는 앱 준비 이후 chart bundle preload를 시작했기 때문에, 사용자가 추이 탭을 열기 전에도 1MB급 외부 차트 번들 요청이 네트워크와 메인 스레드에 부담을 줄 수 있었다.

## 확인한 구조

- `loadPerformanceData()`는 `/api/portfolio/nav-history`와 `/api/portfolio/cashflows`를 병렬로 가져온 뒤 NAV 그래프, 평가금액 그래프, 수익률 카드, 자금 입출금 표를 렌더한다.
- 기존 NAV/평가금액 추이 그래프는 `loadChartLib()`를 통해 외부 ECharts 또는 모바일 uPlot을 동적으로 로드했다.
- ECharts 로드는 다음 사용자 경험 리스크가 있었다.
  - 외부 CDN 상태에 따라 최초 그래프 표시가 직접 지연된다.
  - 라이브러리 다운로드가 끝나도 초기화와 레이아웃 측정이 늦으면 빈 차트가 된다.
  - 추이 분석에 필요한 기능은 선 그래프, 툴팁, 기간 선택, Y축 재계산 정도인데, 전체 ECharts 번들은 기능 대비 무겁다.
  - 이전 preload는 “추이 탭을 누르기 전부터” 외부 차트 번들을 내려받아 초기 사이트 체감 속도까지 해칠 수 있었다.

## 적용한 방향

- `NAV 기준가 추이`와 `평가금액 추이`는 ECharts 의존을 제거하고, 프로젝트 내부의 경량 canvas 렌더러 `static/js/portfolio-trend-chart.js`로 교체했다.
- 새 렌더러는 기존 코드가 유지보수하기 쉽도록 ECharts와 유사한 최소 인터페이스를 제공한다.
  - `create(container, option)`
  - `setOption(next)`
  - `getOption()`
  - `dispatchAction({ type: 'dataZoom', start, end })`
  - `on('datazoom', handler)`
  - `resize()`
  - `dispose()`
- 비교지수는 기존 요구대로 별도 Y축을 만들지 않고 NAV와 같은 축에 그린다.
- 비교지수는 현재 표시 구간의 첫 겹치는 날짜에서 NAV와 시작점을 맞추도록 정규화한다.
- Y축은 현재 표시 구간의 NAV와 비교지수를 모두 포함해 다시 계산한다.
- Y축 0부터 옵션이 꺼져 있어도, 표시 값이 모두 양수인데 padding 때문에 음수로 내려가는 경우는 0에서 잘리도록 했다.
- 평가금액 그래프도 같은 렌더러를 사용해 NAV 그래프와 높이/동작/기간 라벨 UX를 맞췄다.
- `loadPerformanceData()`에서 수익률 카드를 먼저 렌더하고 NAV/평가금액 그래프를 병렬 렌더하도록 바꿔 초기 대기 시간을 줄였다.
- 앱 시작 시 `scheduleChartPreload()` 호출을 제거해, 추이 탭과 무관한 초기 페이지 진입에서 외부 chart bundle이 끼어들지 않도록 했다.

## 의도적으로 남긴 것

- 영역지도 treemap은 여전히 ECharts를 사용한다. 영역지도는 ECharts가 상대적으로 적합하고, 모달을 열 때만 필요하므로 추이 탭 초기 표시 병목과 분리했다.
- `loadChartLib()`와 `createLineChart()`는 분석 화면 및 백테스트 등 다른 화면에서 아직 사용한다. 이번 변경은 NAV/평가금액 추이 문제를 격리해서 해결하는 범위로 제한했다.
- 로컬 DB에는 실제 사용자 포트폴리오 스냅샷이 없어, 인증된 운영 데이터의 `/api/portfolio/nav-history` 응답 시간을 직접 재현하지는 못했다. 이번 변경은 API 이후의 프론트엔드 렌더 병목을 제거하는 조치다.

## UX 검증 기준

- 추이 탭 진입 시 외부 ECharts 다운로드 없이, API 응답 직후 canvas 그래프가 그려져야 한다.
- NAV와 평가금액 그래프가 모두 빈 화면으로 남지 않아야 한다.
- 비교지수를 켜도 별도 오른쪽 축이 생기지 않아야 한다.
- 비교지수는 점선으로 나타나고, NAV는 기존처럼 실선/영역 채움 형태를 유지해야 한다.
- 기간 카드 클릭 시 표시 기간 라벨, CAGR 카드, Y축이 함께 갱신되어야 한다.
- 하단 기간 슬라이더를 움직이면 해당 구간 기준으로 Y축이 다시 계산되어야 한다.
- Y축 0부터 옵션이 꺼진 상태에서 모든 표시 값이 양수이면 축 min이 음수로 과도하게 내려가지 않아야 한다.
- 모바일에서는 기존 정책대로 비교지수/기간 옵션이 숨겨지고 단순 그래프만 보여야 한다.

## 검증 명령

```powershell
node --check static/js/portfolio-trend-chart.js
node --check static/js/portfolio.js
node --check static/js/app-main.js
node --check static/js/utils.js
python -m pytest tests/test_portfolio.py tests/test_portfolio_asset_insights.py tests/test_main.py -q
```

추가로 전체 테스트를 실행해 회귀 여부를 확인한다.

```powershell
python -m pytest -q
```

## 남은 관찰 포인트

- 첫 비교지수 선택 시 `/api/portfolio/benchmark-history`가 운영 DB에 캐시되지 않은 구간을 채우는 경우, 비교지수 라인 추가는 API 응답을 기다릴 수 있다. 그래도 NAV 자체는 더 이상 ECharts 로드 때문에 지연되지 않아야 한다.
- 실제 운영 세션에서 느린 경우는 다음 순서로 분리해서 봐야 한다.
  - `/api/portfolio/nav-history` 응답 시간
  - `/api/portfolio/benchmark-history` 응답 시간
  - 정적 자산 캐시가 최신 commit hash로 교체되었는지
  - 브라우저 콘솔의 JS 오류 여부
- 추이 그래프를 더 고급화하려면, 지금 만든 `PortfolioTrendChart`에 필요한 기능을 작은 단위로 추가하는 방식이 안전하다. 다시 ECharts 옵션을 크게 만지는 방식은 재발 위험이 높다.
