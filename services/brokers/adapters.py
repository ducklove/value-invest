"""기존 NH·한국투자 구현을 공통 계좌 연동 계약에 연결한다."""

import re

from domain.broker_catalog import BROKERS
from repositories.broker_secrets import BrokerError
from services.brokers.base import BrokerAdapter


class NamuhAdapter(BrokerAdapter):
    definition = BROKERS["namuh"]

    async def discover(self, user, cid, inputs):
        from services.brokers import namuh
        return await namuh.accounts(user, cid)

    async def fetch_snapshot(self, user, link):
        from services.brokers.namuh_balance import fetch_snapshot
        return await fetch_snapshot(user, link)

    async def fetch_activity(self, user, link, start, end):
        from services.brokers import activity
        return await activity.fetch(user, link, start, end)

    def status(self, user):
        from services.brokers import notifications, realtime
        return {"type": "namuh_status", **realtime.status(user), "notifications": notifications.status(user)}

    async def run(self, stop):
        # NH는 키당 두 소켓 한도 안에서 시세와 계좌 통보를 함께 구독한다.
        from services.brokers import realtime
        await realtime.run(stop)


class KisAdapter(BrokerAdapter):
    definition = BROKERS["kis"]

    def validate_inputs(self, inputs):
        account = re.sub(r"[-\s]", "", inputs["account_no"])
        if not re.fullmatch(r"[0-9]{8}01", account):
            raise BrokerError("한국투자증권 주식 계좌번호 8자리와 상품코드 01을 입력해 주세요.")
        return {**inputs, "account_no": account}

    async def discover(self, user, cid, inputs):
        from services.brokers import kis
        await kis.token(user, cid, inputs["environment"])
        return [{"account_no": inputs["account_no"], "environment": inputs["environment"]}]

    async def fetch_snapshot(self, user, link):
        from services.brokers import kis
        return await kis.fetch_snapshot(user, link)

    async def stream(self, user, cid, environment, changed):
        from services.brokers import kis_realtime
        await kis_realtime.stream(user, cid, environment, changed)

    def status(self, user):
        from services.brokers import kis_realtime
        return {"type": "broker_account_status", "provider": self.definition.id, **kis_realtime.status(user)}

    def forget(self, user, cid):
        from services.brokers import kis_realtime
        kis_realtime._states.pop((user, cid), None)
