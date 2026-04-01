# 사이드바 마켓 지표 커스터마이징

## 개요

사이드바 마켓바에 표시되는 지표를 사용자가 선택/삭제/재정렬할 수 있도록 한다. 최대 10개, 20개 카탈로그에서 선택.

## UI

1. 마켓바: 선택된 지표가 3-column grid로 표시 (기존과 동일)
2. 각 항목 hover → 우측 ✕ 버튼 (삭제). 드래그로 순서 변경.
3. 10개 미만 → 마지막 줄 아래 `+ 항목 추가` 행 표시
4. `+ 항목 추가` 클릭 → 카테고리별 드롭다운:
   - 국내 지수, 해외 지수, 원자재, 환율, 금리, 파생
   - 이미 추가된 항목은 비활성(회색) 표시
5. 항목 선택 → 마켓바에 추가, 드롭다운 닫힘

## API 변경

`GET /api/market-summary?codes=KOSPI,KOSDAQ,USD_KRW,CMDT_GC,NIGHT_FUTURES`

codes 파라미터로 원하는 지표만 요청. 생략 시 기본 5개.

## 저장

- 로그인: 서버 DB user_preferences (key: `market_bar_codes`, value: JSON array)
- 비로그인: localStorage `market_bar_codes`
- 기본값: `["KOSPI", "KOSDAQ", "USD_KRW", "CMDT_GC", "NIGHT_FUTURES"]`

## 지표 카탈로그

| 코드 | 라벨 | 카테고리 | 소스 |
|------|------|----------|------|
| KOSPI | KOSPI | 국내 지수 | 네이버 sise_index KOSPI |
| KOSDAQ | KOSDAQ | 국내 지수 | 네이버 sise_index KOSDAQ |
| KOSPI200 | KOSPI200 | 국내 지수 | 네이버 sise_index KPI200 |
| SPX | S&P500 | 해외 지수 | 네이버 worldstock SPI@SPX |
| IXIC | NASDAQ | 해외 지수 | 네이버 worldstock NAS@IXIC |
| DJI | 다우존스 | 해외 지수 | 네이버 worldstock DJI@DJI |
| NI225 | 니케이225 | 해외 지수 | 네이버 worldstock NII@NI225 |
| HSI | 항셍 | 해외 지수 | 네이버 worldstock HSI@HSI |
| SHC | 상해종합 | 해외 지수 | 네이버 worldstock SHS@SHC |
| CMDT_GC | 국제금 | 원자재 | 네이버 marketindex gold_inter |
| CMDT_SI | 은 | 원자재 | 네이버 marketindex silver |
| OIL_CL | WTI유 | 원자재 | 네이버 marketindex wti |
| OIL_BRT | 브렌트유 | 원자재 | 네이버 marketindex brent |
| USD_KRW | USD/KRW | 환율 | 네이버 marketindex |
| EUR_KRW | EUR/KRW | 환율 | 네이버 marketindex exchg |
| JPY_KRW | JPY/KRW | 환율 | 네이버 marketindex exchg |
| CNY_KRW | CNY/KRW | 환율 | 네이버 marketindex exchg |
| US10Y | 미국10년물 | 금리 | 네이버 marketindex bond |
| KR3Y | 한국3년물 | 금리 | 네이버 marketindex bond |
| NIGHT_FUTURES | 야간선물 | 파생 | esignal socket.io |
