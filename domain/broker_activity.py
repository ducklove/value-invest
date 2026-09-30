"""증권사 수입·입출금의 공용 분류와 기록 시각."""

from datetime import datetime

from domain.timeutil import KST

KINDS = {"dividend", "interest", "other_income", "transfer", "internal", "trade", "fee", "review"}
INCOME_KINDS = {"dividend", "interest", "other_income"}


def stamp() -> str:
    return datetime.now(KST).replace(tzinfo=None).isoformat(timespec="microseconds")
