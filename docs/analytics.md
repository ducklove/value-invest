# 통합 접속 통계

기존 **value-invest** GA4 속성(527825888), 웹 스트림(13881342022),
측정 ID **G-KE611DTCFZ**를 허브와 10개 프론트엔드에서 함께 사용합니다.

- [Google Analytics](https://analytics.google.com/analytics/web/#/a45493991p527825888/reports/reportinghub): 실시간 보고서에서 접속 확인.
- 페이지 및 화면 보고서에서 `콘텐츠 그룹`을 선택하면 프로젝트별로 구분합니다.
  그룹 이름은 저장소 이름이며 목록은 `config/analytics-projects.json`에 있습니다.
- 도메인이 다른 허브/Pages 간 이동은 동일 태그의 linker 설정을 사용합니다.
  GA4 태그의 도메인 구성에도 `ducklove.duckdns.org`와 `ducklove.github.io`를
  정확히 일치하는 조건으로 등록했습니다.
- iframe으로 삽입된 도구는 별도 태그를 로드하지 않습니다. 허브 페이지 방문만
  집계하며, 도구를 직접 열면 해당 프로젝트로 집계합니다.
- 최초 방문과 SPA 경로 변경마다 page_view를 한 번 보냅니다. 필터의 query/hash
  변경, 로컬 개발, 알 수 없는 호스트, 허브 리디렉션 페이지는 집계하지 않습니다.
- URL의 query/hash 및 동적 화면 제목은 보내지 않습니다. 기존 허브의 사용 이벤트는
  유지하며 로그인 상태 외에 사용자 ID, 계좌, 보유량을 추가 수집하지 않습니다.
- 향상된 측정에서 **브라우저 방문 기록 기반 페이지 변경**을 껐습니다.
  다시 켜면 수동 SPA 조회와 중복됩니다. 검색어와 폼 이벤트 자동 수집도 껐고,
  기존 스크롤·이탈 클릭·동영상·다운로드 측정은 유지합니다.

공통 소스는 `static/js/analytics.js`입니다. sibling checkout이 있는 작업 환경에서
`node scripts/sync-analytics.mjs --write`로 배포용 복사본을 갱신하고,
`node scripts/sync-analytics.mjs`로 모든 프로젝트의 연결과 파일 일치를 확인합니다.
각 저장소에 로컬 파일을 포함하므로 허브 서버 장애가 다른 사이트의 태그 로딩에
영향을 주지 않습니다. Vite 프로젝트는 public 파일을 빌드 결과로 복사합니다.

참고: [GA4 페이지 조회](https://developers.google.com/analytics/devguides/collection/ga4/views),
[교차 도메인 측정](https://developers.google.com/tag-platform/devguides/cross-domain).
