"""해외 DR(주식예탁증서) 스태틱 레지스트리.

한국 원주(6자리 코드) → 해외 거래 중인 DR 목록. 종목분석에서 DR 시세를
원주 1주 환산가(원)로 보여줄 때 쓴다. 런타임 탐색이 아니라 검증된 스태틱
매핑(거래소·티커·통화·교환비율 고정).

각 DR 항목:
- ticker:         yfinance 조회 티커(거래소 접미사 포함)
- exchange:       거래소(표시용)
- currency:       거래 통화(표시용 — 실제 환산은 yfinance 가 보고하는 통화를 사용)
- shares_per_dr:  DR 1개가 대표하는 원주 수.
                  원주 1주 환산가(원) = (DR 현재가의 원화환산) / shares_per_dr
- label:          표시 라벨

교환비율 출처(2026-06 deep-research, 1차 출처 다수 검증):
- 삼성전자 GDR/GDS(SMSN/SMSD, 런던): LSE 증권명 'each Rep 25', 2018 50:1 액면분할 → 1 DR = 25주
- SK하이닉스 GDR(HY9H, 프랑크푸르트): Deutsche Börse / skhynix IR 'Rep 1 Ord' → 1 DR = 1주
- POSCO홀딩스 ADS(PKX): SEC 20-F 'one-fourth of one share' → 0.25
- SK텔레콤 ADS(SKM): SEC 6-K 'five-ninths of one share'(2021 분할/분사 후) → 5/9
- 신한지주 ADR(SHG): SEC 6-K(2012 변경) → 1
- 우리금융 ADS(WF): SEC F-6 'three (3) Shares' → 3
- 한국전력 ADR(KEP): 1 ADR = 0.5주
- KT ADR(KT): SEC 20-F 'one-half of one share' → 0.5

각 항목은 환산가÷원주가 비율이 0.9~1.05(DR 디스카운트/프리미엄 정상범위)로
실측 확인된 것만 등재했다. 예: 삼성 프랑크푸르트 SSUN.F 는 비율이 어긋나(≈0.58)
제외. 현대차 HYUD 는 2024-12-19 LSE 상장폐지로 제외. KB/LG디스플레이(LPL)/
LG전자(LGLD)/TOUR 는 비율 미확정으로 보류(확정 시 추가).
"""

from __future__ import annotations

DR_REGISTRY: dict[str, list[dict]] = {
    "005930": [  # 삼성전자 보통주
        {"ticker": "SMSN.L", "exchange": "런던", "currency": "USD", "shares_per_dr": 25, "label": "삼성전자 GDR"},
    ],
    "005935": [  # 삼성전자 우선주
        {"ticker": "SMSD.L", "exchange": "런던", "currency": "USD", "shares_per_dr": 25, "label": "삼성전자우 GDS"},
    ],
    "000660": [  # SK하이닉스
        {"ticker": "HY9H.F", "exchange": "프랑크푸르트", "currency": "EUR", "shares_per_dr": 1, "label": "SK하이닉스 GDR"},
    ],
    "005490": [  # POSCO홀딩스
        {"ticker": "PKX", "exchange": "NYSE", "currency": "USD", "shares_per_dr": 0.25, "label": "POSCO홀딩스 ADR"},
    ],
    "017670": [  # SK텔레콤
        {"ticker": "SKM", "exchange": "NYSE", "currency": "USD", "shares_per_dr": 5 / 9, "label": "SK텔레콤 ADR"},
    ],
    "055550": [  # 신한지주
        {"ticker": "SHG", "exchange": "NYSE", "currency": "USD", "shares_per_dr": 1, "label": "신한지주 ADR"},
    ],
    "316140": [  # 우리금융지주
        {"ticker": "WF", "exchange": "NYSE", "currency": "USD", "shares_per_dr": 3, "label": "우리금융 ADR"},
    ],
    "015760": [  # 한국전력
        {"ticker": "KEP", "exchange": "NYSE", "currency": "USD", "shares_per_dr": 0.5, "label": "한국전력 ADR"},
    ],
    "030200": [  # KT
        {"ticker": "KT", "exchange": "NYSE", "currency": "USD", "shares_per_dr": 0.5, "label": "KT ADR"},
    ],
}


def drs_for(stock_code: str) -> list[dict]:
    """해당 종목코드의 DR 목록(없으면 빈 리스트)."""
    return DR_REGISTRY.get((stock_code or "").strip(), [])
