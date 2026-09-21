"""LS 공식 Open API: 국내·미국주식, CMA RP와 결제 반영 예수금."""

import re
from datetime import datetime, timedelta, timezone

from cache_layer import MemoryTTLCache
from domain.broker_catalog import BROKERS
from repositories.broker_secrets import BrokerError
from services.brokers.normalized import merge, no_debt, number, position, records, stock_code, summary
from services.brokers.rest import Endpoint, RestAdapter
from services.brokers.symbols import foreign_code


class LsAdapter(RestAdapter):
    definition = BROKERS["ls"]
    bases = {"live": "https://openapi.ls-sec.co.kr:8080"}
    api_header, flag_header, cursor_header, result_field = "tr_cd", "tr_cont", "tr_cont_key", "rsp_cd"
    token_errors = frozenset({"IGW00121", "IGW00122", "IGW00123"})
    rate_errors = frozenset({"IGW00201"})
    endpoints = {
        "CSPAQ12200": Endpoint("/stock/accno", "CSPAQ12200InBlock1", ("00136", "00000")),
        "t0424": Endpoint("/stock/accno", "t0424InBlock", ("00000",)),
        "t8436": Endpoint("/stock/etc", "t8436InBlock", ("00000",)),
        "COSOQ00201": Endpoint("/overseas-stock/accno", "COSOQ00201InBlock1", ("00001", "00000", "00136")),
        "COSOQ02701": Endpoint("/overseas-stock/accno", "COSOQ02701InBlock1", ("00136", "00000")),
    }

    def __init__(self):
        super().__init__()
        self._listing = MemoryTTLCache("ls_account_listing", default_ttl_seconds=3600)

    def next_page(self, tr, data, headers, inputs):
        if tr == "t0424":
            total = summary(data, "t0424OutBlock")
            if "cts_expcode" not in total:
                raise BrokerError("LS 잔고의 연속조회 상태가 누락되었습니다.")
            cursor = str(total["cts_expcode"] or "").strip()
            if str(headers.get(self.flag_header, "")).upper() == "Y" and not cursor:
                raise BrokerError("LS 잔고의 연속조회 키가 누락되었습니다.")
            return cursor, {**inputs, "cts_expcode": cursor}
        return super().next_page(tr, data, headers, inputs)

    async def account_summary(self, user, cid, env):
        pages = await self.pages(user, cid, env, "CSPAQ12200", {"BalCreTp": "1"})
        if len(pages) != 1:
            raise BrokerError("LS 계좌 합계를 확인하지 못했습니다.")
        return pages[0]

    async def discover(self, user, cid, inputs):
        env = inputs["environment"]
        page = await self.account_summary(user, cid, env)
        number = summary(page, "CSPAQ12200OutBlock1").get("AcntNo", "")
        if not isinstance(number, str) or not re.fullmatch(r"[0-9]{11}", number):
            raise BrokerError("LS 앱키에 연결된 전체 계좌번호를 확인하지 못했습니다.")
        return [{"account_no": number, "environment": env}]

    async def listed_codes(self, user, cid, env):
        key = (cid, env)
        cached = self._listing.get(key)
        if cached is not None:
            return cached
        codes = {stock_code(row.get("shcode", "")) for page in await self.pages(user, cid, env, "t8436", {"gubun": "0"})
                 for row in records(page, "t8436OutBlock")}
        if not codes:
            raise BrokerError("LS 상장 종목 목록이 비어 있어 잔고를 유지합니다.")
        self._listing.set(key, codes)
        return codes

    async def fetch_snapshot(self, user, link):
        if link.get("product", "stocks") != "stocks":
            raise BrokerError("LS 주식 계좌만 지원합니다.")
        cid, env = link["credential_id"], link["environment"]
        page = await self.account_summary(user, cid, env)
        if summary(page, "CSPAQ12200OutBlock1").get("AcntNo") != link["account_no"]:
            raise BrokerError("LS 앱키의 연결 계좌가 변경되어 기존 잔고를 유지합니다.")
        total = summary(page, "CSPAQ12200OutBlock2")
        no_debt(total, "MloanAmt", "DpspdgLoanAmt", "MnyrclAmt", "EtclndAmt", "RcvblAmt")
        balances = {"KRW": {key: number(total, key) for key in ("Dps", "D1Dps", "D2Dps")}, "_excluded": [],
                    "_excluded_reason": "LS 코스피·코스닥 상장 종목 목록에 없는"}
        rows = [position("CASH_KRW", "원화 현금", balances["KRW"]["D2Dps"], 1)]
        pages = await self.pages(user, cid, env, "t0424", {"prcgb": "1", "chegb": "2", "dangb": "0", "charge": "0", "cts_expcode": ""})
        seen_positions = set()
        for page in pages:
            for row in records(page, "t0424OutBlock1"):
                qty = number(row, "janqty")
                if not qty:
                    continue
                no_debt(row, "sinamt")
                raw = str(row.get("expcode", ""))
                identity = (raw, row.get("loandt", ""), row.get("jonggb", ""))
                if identity in seen_positions:
                    raise BrokerError("LS 연속조회에서 잔고가 중복되어 갱신을 보류했습니다.")
                seen_positions.add(identity)
                if raw == "CMARP":
                    value, cost = number(row, "appamt"), number(row, "mamt")
                    if value <= 0 or cost < 0 or qty < 0:
                        raise BrokerError("LS CMA RP 평가액을 확인하지 못했습니다.")
                    rows.append(position("CMA_RP_KRW", "CMA 원화RP", value, cost / value))
                    continue
                code = stock_code(raw)
                if code not in await self.listed_codes(user, cid, env):
                    balances["_excluded"].append(code)
                    continue
                rows.append(position(code, str(row.get("hname") or code), qty, number(row, "pamt")))
        # 외화는 해외주식 가져오기 선택과 관계없이 조회한다. 원화 환산 합계는 더하지 않는다.
        for page in await self.pages(user, cid, env, "COSOQ02701", {"RecCnt": 1, "CrcyCode": "ALL"}):
            for row in records(page, "COSOQ02701OutBlock2"):
                currency = str(row.get("CrcyCode", "")).strip()
                if currency in balances:
                    raise BrokerError("LS 외화 예수금이 중복 반환되었습니다.")
                balances[currency] = {key: number(row, key) for key in ("PrsmptFcurrDps2", "PrsmptFcurrDps4")}
                rows.append(position("CASH_" + currency, currency + " 현금", balances[currency]["PrsmptFcurrDps4"], 1, currency))
        if link.get("include_overseas", True):
            today = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
            seen = set()
            for page in await self.pages(user, cid, env, "COSOQ00201", {"RecCnt": 1, "BaseDt": today, "CrcyCode": "ALL", "AstkBalTpCode": "00"}):
                no_debt(summary(page, "COSOQ00201OutBlock2"), "LoanAmt")
                for row in records(page, "COSOQ00201OutBlock4"):
                    qty = number(row, "AstkBalQty")
                    if not qty:
                        continue
                    no_debt(row, "LoanAmt")
                    if row.get("CrcyCode") != "USD" or row.get("MktTpNm") not in {"NASDAQ", "NYSE", "AMEX"}:
                        raise BrokerError("LS 미국 외 해외주식은 아직 지원하지 않아 갱신을 보류했습니다.")
                    if "USD" not in balances:
                        raise BrokerError("LS 미국주식의 외화 예수금 조회가 불완전하여 기존 잔고를 유지합니다.")
                    code = foreign_code(row.get("ShtnIsuNo", ""), "200")
                    identity = (code, row.get("AstkBalTpCode"))
                    if identity in seen:
                        raise BrokerError("LS 해외 잔고가 중복 반환되었습니다.")
                    seen.add(identity)
                    rows.append(position(code, str(row.get("JpnMktHanglIsuNm") or code), qty, number(row, "FcstckUprc"), "USD"))
        return merge(rows), balances

    async def stream(self, user, cid, environment, changed):
        from services.brokers.push import ls_stream
        await ls_stream(self, user, cid, environment, changed)

    def status(self, user):
        from services.brokers.push import status
        return status(self.definition.id, user)

    def forget(self, user, cid):
        from services.brokers.push import forget
        forget(self.definition.id, user, cid)
