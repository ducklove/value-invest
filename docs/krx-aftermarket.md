# KRX 애프터마켓 시세

2026-09-14부터 국내 주식 현재가는 KIS REST `UN`, WebSocket `H0UNCNT0`로
KRX/NXT를 함께 조회한다. 거래소 정규장 시계와 포트폴리오 20시 정산 경계는
그대로 유지한다. 시행 전 날짜를 재현하면 기존 정규장 J / 장외 NX 규칙을 쓴다.

통합 WebSocket 구독이 거절되면 브라우저가 슬롯을 해제하고 기존 REST 폴링을
유지한다. 통합 REST의 0원 응답은 정상 시세로 채택하지 않는다.

네이버 일괄 시세는 `localTradedAt`으로 KRX/NXT 마지막 체결을 비교하고
`as_of`와 거래소를 전달한다. 통합 거래량·대금은 `integratedPriceInfo`의
숫자 원본 필드를 사용한다. 서버·브라우저 모두 실제 체결 시각이 더 오래된
값으로 가격을 되돌리지 않는다.

미니 차트는 16시 이후 실제 분봉이 있는 세션에 20시 축을 적용한다.
15:40~16:00 장후 종가 거래만 있는 ETF 등은 받은 시각까지만 확장하고,
정규장 분봉만 있는 종목은 기존 축을 유지한다. 프론트도 실제 점을
세션 경계 밖으로 잘라내지 않는다.

관련 일봉 공급원 finance-pi는 당일 종가를 20:10 KST 이후 확정하며
20:30 수집, 21:30 실패 재시도로 운영한다. 최신 거래일의 KIS 수집은
기존 종목도 다시 읽어 장중 저장값을 최종 종가로 갱신한다.

근거: [삼성증권 시행 안내](https://www.samsungpop.com/ux/kor/customer/notice/notice/noticeViewContent.do?MenuSeqNo=24420),
[KIS 현재가 시장 구분](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_price/inquire_price.py).
