# Project Architecture Graph (이동됨)

이 문서는 2026-09-30에 [ecosystem/architecture.md](ecosystem/architecture.md)로 대체됐다. 옛 링크가 깨지지 않게
안내만 남긴다.

- 전체 구조도·데이터 흐름·호스팅 표·통합 계약: [ecosystem/architecture.md](ecosystem/architecture.md)
- 생태계 문서 색인: [ecosystem/README.md](ecosystem/README.md)
- 도구 목록 정본: [config/ecosystem.json](../config/ecosystem.json)

옛 그래프(2026-06-04 기준)에서 틀렸던 점:

- 대시보드가 4개만 있었다(지금 9개 + index-popup). portfolio-epaper, x3, the_admin, fin-commons도 없었다.
- 허브 `/ws/quotes`가 kis-proxy를 거친다고 그렸지만, 허브는 자체 KIS 앱키로 KIS 웹소켓에 직접 붙는다.
- holding_value가 kis-proxy에서 시세 history를 받는다고 그렸지만, 실제로는 quote·지수 quote(Actions)와 Naver 배치 시세(브라우저)를 쓴다.
- 외부 인사이트를 전부 raw.githubusercontent로 적었다. 지금은 레지스트리 URL과 형제 `summary.json`(Pages)이 1순위다.
- finance-pi를 종가 백업으로만 적었다. 포트폴리오 히스토리·매크로·스크리너·quant 리서치의 원천이다.
- systemd 타이머를 4개로 적었다. 14개다.
- 허브가 형제에게 제공하는 표면(보유 배지, `/go/{tool}`, 공용 알림, 디바이스 API, 공용 셸)이 없었다.
