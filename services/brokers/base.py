"""계좌 연동 어댑터 계약. 주문 실행은 이 인터페이스에 포함하지 않는다."""

from abc import ABC, abstractmethod
from datetime import date

from domain.broker_catalog import BrokerDefinition
from repositories.broker_secrets import BrokerError

Snapshot = tuple[list[dict], dict]


class BrokerAdapter(ABC):
    definition: BrokerDefinition

    def validate_inputs(self, inputs: dict) -> dict:
        return inputs

    @abstractmethod
    async def discover(self, user: str, cid: str, inputs: dict) -> list[dict]:
        """인증 후 연결 가능한 계좌번호·환경을 반환한다."""

    @abstractmethod
    async def fetch_snapshot(self, user: str, link: dict) -> Snapshot:
        """모든 연속조회가 완료된 보유분과 현금을 반환한다. 실패 시 부분 잔고를 반환하지 않는다."""

    async def fetch_activity(self, user: str, link: dict, start: date, end: date) -> list[dict]:
        raise BrokerError(f"{self.definition.name} 수입·입출금 거래내역 가져오기는 아직 지원하지 않습니다.")

    async def stream(self, user: str, cid: str, environment: str, changed):
        """통보는 재조회 신호로만 사용한다. 잔고 변경은 REST 스냅샷에서만 수행한다."""

    def status(self, user: str) -> dict:
        return {"type": "broker_account_status", "provider": self.definition.id, "state": "polling", "poll_seconds": 60}

    def forget(self, user: str, cid: str):
        """연결 해제 시 통보 상태를 정리한다."""

    async def run(self, stop):
        from services.brokers.runtime import run_accounts
        await run_accounts(self, stop)
