"""증권사 추가 지점. HTTP 라우터·동기화·서버 수명주기는 이 레지스트리만 사용한다."""

from functools import lru_cache

from repositories.broker_secrets import BrokerError
from services.brokers.adapters import KisAdapter, NamuhAdapter
from services.brokers.base import BrokerAdapter
from services.brokers.kiwoom import KiwoomAdapter
from services.brokers.ls import LsAdapter

ADAPTERS = {"namuh": NamuhAdapter, "kis": KisAdapter, "kiwoom": KiwoomAdapter, "ls": LsAdapter}


@lru_cache
def get_adapter(provider: str) -> BrokerAdapter:
    factory = ADAPTERS.get(provider)
    if factory is None:
        raise BrokerError("지원하지 않는 증권사입니다.")
    return factory()


def catalog() -> list[dict]:
    return [get_adapter(provider).definition.public() for provider in ADAPTERS]


def all_adapters() -> list[BrokerAdapter]:
    return [get_adapter(provider) for provider in ADAPTERS]
