"""Value Compass 생태계(형제 대시보드) 소비 계층.

- :mod:`services.ecosystem.fetch`    — MemoryTTLCache + single-flight ``cached_fetch``
- :mod:`services.ecosystem.envelope` — 발행 데이터 계약 v1 envelope 검증(허브 정본 헬퍼 재사용)
- :mod:`services.ecosystem.siblings` — summary.json 우선 로더(ETag·음성 캐시·stale 1일) + 레거시 폴백
- :mod:`services.ecosystem.adapters` — summary ``data`` → 기존 허브 모양(레거시 파일 모양) 변환
- :mod:`services.ecosystem.links`    — 레지스트리 기반 딥링크/handoff URL 조립(``/go``)

URL·브랜치는 전부 ``config/ecosystem.json``(``core.ecosystem``)에서 온다.
"""
