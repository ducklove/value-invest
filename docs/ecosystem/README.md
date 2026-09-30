# Value Compass 생태계 문서

허브(`value-invest`)와 연결 대시보드·인프라 저장소를 하나의 생태계로 묶는 문서 모음이다.
작성·갱신: 2026-09-30.

## 어디서부터 읽나

처음이면 [architecture.md](architecture.md)의 §0·§1로 전체 그림을 잡는다. 도구 목록·URL·링크 템플릿은 문서가 아니라
[config/ecosystem.json](../../config/ecosystem.json)(레지스트리 정본, 21개 도구 중 공개 15개)에서 온다. 형제 대시보드를
고치는 사람은 [ui-contract.md](ui-contract.md)(딥링크·테마·공용 셸)와 [data-contract.md](data-contract.md)(허브용
`summary.json`)를 지키면 된다. 외부 API 호출을 추가하거나 캐시를 바꾸려면 [external-data.md](external-data.md)를,
코드를 어디에 두고 어떻게 나눠 줄지는 [modularization.md](modularization.md)를 본다. 배포 전에 해야 할 소유자 조치와
다음 과제는 [roadmap.md](roadmap.md) §0에 있다.

## 문서

| 문서 | 내용 |
|---|---|
| [architecture.md](architecture.md) | 계층 구조도, 데이터 흐름(명목·관측 주기), 호스팅·배포 표, 통합 계약(딥링크·발행 데이터·서버 API·공유 자산·admin 쓰기) |
| [ui-contract.md](ui-contract.md) | 딥링크 계약 v1, 허브 inbound 라우트, 테마 규칙, vc-shell 사용법, 토큰, iframe 메시지, 새 대시보드 추가 절차 |
| [data-contract.md](data-contract.md) | Published Data Contract v1 — envelope, canonical JSON·contentHash, 도구별 payload, no-op 커밋 규칙 |
| [external-data.md](external-data.md) | 외부 소스 × 프로젝트 매트릭스, 엔드포인트·인증 env 이름·주기·캐시, 중복 분석, 통합한 것과 남은 것 |
| [modularization.md](modularization.md) | 중복 클러스터, 이번에 합친 것(경로), 공유 코드 배포 방식, 남은 백로그 |
| [roadmap.md](roadmap.md) | 지금 필요한 소유자 조치, 미룬 기술 과제, 새 제품 아이디어 |
| [../linked-projects.md](../linked-projects.md) | 허브 쪽 연동 설정·환경변수·공용 알림 API·운영 메모 |

## 프로젝트

레지스트리 `visibility`: 공개(public) 항목만 브라우저·형제 저장소로 나간다. 저장소는 모두 `github.com/ducklove/<이름>`.

| 프로젝트 | 역할 | 호스팅 | 레지스트리 | 저장소 문서 |
|---|---|---|---|---|
| value-invest | 허브 — 포트폴리오·종목 분석·알림·AI, 레지스트리·공용 셸 정본 | pi-worker `:3691`(master push = 배포) | public | [README](../../README.md), [CLAUDE.md](../../CLAUDE.md) |
| holding_value | 지주사 보유지분가치/시총 비율·할인율 | GitHub Pages | public, handoff·보유 배지 | 저장소 `README.md` |
| common_preferred_spread | 보통주·우선주 괴리율, research v1 카탈로그 | GitHub Pages | public, handoff·보유 배지 | `README.md`, `docs/research-api.md` |
| spac-hunter | SPAC 청산가치·합병 일정 | GitHub Pages | public, handoff·보유 배지 | `README.md` |
| buybacks | 자사주 취득·처분·소각·보유 비율 | GitHub Pages(Vite) | public, handoff·보유 배지 | `README.md`, `docs/buybacks-data-sources.md` |
| eiayn | ETF 평가·랭킹, research v1 카탈로그 | GitHub Pages(Vite) | public, handoff·보유 배지 | `README.md`, `docs/research-api.md` |
| gold_gap | 금·BTC·ETH·USDT 김치프리미엄 | GitHub Pages(`data` 브랜치) | public | `README.md` |
| all-about-gold | 금 투자 리서치(finance-pi 스냅샷 렌더러) | GitHub Pages(`data` 브랜치, Pi 발행) | public | `README.md`, `docs/deployment.md` |
| nps-tracker | 국민연금 국내주식 포트폴리오, 허브 `/nps` 임베드 | GitHub Pages(Pi crontab dispatch) | public | `README.md`, `docs/embed.md` |
| bond-mate | 국채 커브·정책금리·환율·크레딧, 허브 `/bonds` 임베드 | GitHub Pages(`data` 브랜치) | public | `README.md` |
| index-popup | 지수 미니 차트 위젯, 허브 iframe | pi-worker `:3358`(수동 배포) | public | `README.md` |
| hub:screener · hub:quant · hub:insights · hub:masters | 허브 내부 화면(셸 전환기에 노출) | 허브 | public | — |
| finance-pi | 국내 주식 데이터 레이크·PIT 리서치 API | pi-control `:8400` LAN(수동 배포) | internal | `README.md`, `docs/` |
| kis-proxy | KIS 토큰·시세 중계, Naver·Yahoo 릴레이 | pi-worker `:3288`/`:3298`(수동 배포) | internal | `README.md` |
| the_admin | 홈 인프라 상태 대시보드(`registry.yaml`) | pi-worker `/admin/`(수동 배포) | internal | `deploy/DEPLOY.md` |
| portfolio-epaper | 허브 포트폴리오를 6색 e-paper로 렌더 | pi-worker `:8801` LAN(수동 배포) | internal | `README.md` |
| x3 | Xteink X3 e-reader 브리핑 피드 | pi-control `:8765` LAN | internal | `README.md`, `docs/firmware.md` |
| morning-bell | Polymarket 예측시장 일일 브리핑 | pi-control(pull 기반 자동 배포) | internal | `README.md`, `AGENTS.md` |
| fin-commons | 형제 Python 파이프라인 공용 라이브러리 | pip git 태그 `@v0.2.0` | (레지스트리 밖) | 별도 저장소 |

## 자주 쓰는 명령

```bash
node scripts/sync-ecosystem.mjs               # 레지스트리·허브·형제 사본 검증(기본). 형제는 ../<도구 id> 에서 찾는다
node scripts/sync-ecosystem.mjs --write       # vc-shell 레지스트리 블록 재생성 + 형제 사본·theme-boot 쓰기(git 은 안 건드림)
node scripts/sync-ecosystem.mjs --only eiayn  # 특정 형제만
node scripts/sync-ecosystem.mjs --hub-only    # 허브만(CI·pytest 가 실행)
python ecosystem/python/vc_publish.py validate summary.json   # 발행 파일 검증
```

## 옛 문서

- [../project-architecture-graph.md](../project-architecture-graph.md) — 이 폴더로 대체됨(링크 호환용 안내만 남김)
- [../archive/](../archive/README.md) — 시점 기록 보고서
