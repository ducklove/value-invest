"""키움 공식 REST API: 키에 연결된 계좌, 국내·미국주식과 금현물 조회."""

import re
from datetime import datetime, timedelta, timezone

from cache_layer import MemoryTTLCache
from domain.broker_catalog import BROKERS
from repositories.broker_secrets import BrokerError
from services.brokers.normalized import merge, no_debt, number, position, records, stock_code
from services.brokers.rest import Endpoint, RestAdapter
from services.brokers.symbols import foreign_code

KST = timezone(timedelta(hours=9))


class KiwoomAdapter(RestAdapter):
    definition = BROKERS["kiwoom"]
    bases = {"live": "https://api.kiwoom.com", "mock": "https://mockapi.kiwoom.com"}
    api_header, flag_header, cursor_header, result_field = "api-id", "cont-yn", "next-key", "return_code"
    token_errors = frozenset({"8005", "8031", "8103"})
    endpoints = {
        **{tr: Endpoint("/api/dostk/acnt") for tr in ("ka00001", "kt00018", "kt00001", "kt50020", "kt50021")},
        "ka10100": Endpoint("/api/dostk/stkinfo"),
        **{tr: Endpoint("/api/us/acnt") for tr in ("ust21070", "ust21160")},
    }

    def __init__(self):
        super().__init__()
        self._listing = MemoryTTLCache("kiwoom_account_listing", default_ttl_seconds=3600)

    def token_request(self, secret):
        return {"json": {"grant_type": "client_credentials", "appkey": secret["app_key"], "secretkey": secret["app_secret"]}}

    def token_result(self, data):
        if self.error_code(data) != "0":
            raise ValueError
        return data["token"], datetime.strptime(data["expires_dt"], "%Y%m%d%H%M%S").replace(tzinfo=KST).timestamp()

    def error_code(self, data):
        code = super().error_code(data)
        if re.fullmatch(r"[0-9]{1,6}", code):
            code = str(int(code))
        if code != "0" and isinstance(data, dict):
            match = re.search(r"\[(8005|8031|8103):|CODE=(8005|8031|8103)", str(data.get("return_msg", "")))
            if match:
                return match.group(1) or match.group(2)
        return code

    async def discover(self, user, cid, inputs):
        env = inputs["environment"]
        pages = await self.pages(user, cid, env, "ka00001", {})
        if len(pages) != 1 or not re.fullmatch(r"[0-9]{10}", str(pages[0].get("acctNo", ""))):
            raise BrokerError("키움 앱키에 연결된 전체 계좌번호를 확인하지 못했습니다.")
        return [{"account_no": pages[0]["acctNo"], "environment": env}]

    async def listed(self, user, cid, env, code):
        key = (cid, env, code)
        cached = self._listing.get(key)
        if cached is not None:
            return cached
        row = (await self.pages(user, cid, env, "ka10100", {"stk_cd": code}))[0]
        if row.get("code") != code or "marketCode" not in row or "state" not in row:
            raise BrokerError("키움 종목의 상장 상태를 확인하지 못했습니다.")
        active = str(row["marketCode"]) in {"0", "10", "50", "8", "6", "60", "70", "90"} and "상장폐지" not in str(row["state"])
        self._listing.set(key, active)
        return active

    async def fetch_snapshot(self, user, link):
        cid, env = link["credential_id"], link["environment"]
        if {"account_no": link["account_no"], "environment": env} not in await self.discover(user, cid, {"environment": env}):
            raise BrokerError("키움 앱키의 연결 계좌가 변경되어 기존 잔고를 유지합니다.")
        if link.get("product", "stocks") == "gold":
            return await self.gold(user, cid, env)
        if link.get("product", "stocks") != "stocks" or (env == "mock" and link.get("include_overseas")):
            raise BrokerError("키움에서 지원하지 않는 계좌 조회 범위입니다.")
        pages = await self.pages(user, cid, env, "kt00018", {"qry_tp": "2", "dmst_stex_tp": "KRX"})
        rows, excluded, seen = [], [], set()
        for page in pages:
            no_debt(page, "tot_loan_amt", "tot_crd_loan_amt", "tot_crd_ls_amt")
            for row in records(page, "acnt_evlt_remn_indv_tot"):
                qty = number(row, "rmnd_qty")
                if not qty:
                    continue
                code = stock_code(row.get("stk_cd", ""))
                if code in seen:
                    raise BrokerError("키움 연속조회에서 종목이 중복되어 갱신을 보류했습니다.")
                seen.add(code)
                if not await self.listed(user, cid, env, code):
                    excluded.append(code)
                    continue
                rows.append(position(code, str(row.get("stk_nm") or code), qty, number(row, "pur_pric")))
        deposits = await self.pages(user, cid, env, "kt00001", {"qry_tp": "2"})
        total = deposits[-1]
        no_debt(total, "loan_sum", "ls_sum", "ch_uncla_tot", "etc_loan_tot", "dpst_grntl_remn", "sell_grntl_remn")
        balances = {"KRW": {key: number(total, key) for key in ("entr", "d1_entra", "d2_entra")}, "_excluded": excluded}
        rows.append(position("CASH_KRW", "원화 현금", balances["KRW"]["d2_entra"], 1))
        foreign_cash = [row for page in deposits for row in records(page, "stk_entr_prst")]
        for row in foreign_cash:
            currency = str(row.get("crnc_cd", "")).strip()
            # 미국주식 API로 확인할 수 없는 통화에 잔액이 있으면 부분 합산하지 않는다.
            if currency not in {"USD", "KRW"} and any(number(row, key) != 0 for key in ("fx_entr", "d1_fx_entr", "d2_fx_entr", "d3_fx_entr", "d4_fx_entr")):
                raise BrokerError("키움의 USD 이외 외화 잔고는 아직 지원하지 않아 갱신을 보류했습니다.")
        if env == "live" and (link.get("include_overseas", True) or any(row.get("crnc_cd") == "USD" for row in foreign_cash)):
            dollar = (await self.pages(user, cid, env, "ust21160", {}))[-1]
            no_debt(dollar, "won_dfr_amt", "won_etc_loana")
            balances["USD"] = {key: number(dollar, key) for key in ("d0_usd_fx_entr", "d2_usd_fx_entr", "d4_usd_fx_entr")}
            rows.append(position("CASH_USD", "USD 현금", balances["USD"]["d4_usd_fx_entr"], 1, "USD"))
        if env == "live" and link.get("include_overseas", True):
            seen = set()
            for market in ("ND", "NY", "NA"):
                for page in await self.pages(user, cid, env, "ust21070", {"stex_tp": market, "stk_cd": ""}):
                    for row in records(page, "result_list"):
                        qty = number(row, "poss_qty")
                        if not qty:
                            continue
                        if row.get("crnc_code") != "USD":
                            raise BrokerError("키움 미국주식의 거래 통화를 확인하지 못했습니다.")
                        code = foreign_code(row.get("stk_cd", ""), "200")
                        if code in seen:
                            raise BrokerError("키움 해외 잔고가 중복 반환되어 갱신을 보류했습니다.")
                        seen.add(code)
                        rows.append(position(code, str(row.get("frgn_stk_nm") or code), qty, number(row, "frgn_stk_book_uv"), "USD"))
        return merge(rows), balances

    async def gold(self, user, cid, env):
        if env != "live":
            raise BrokerError("키움 금현물은 실계좌만 지원합니다.")
        rows = []
        for page in await self.pages(user, cid, env, "kt50020", {}):
            for row in records(page, "gold_acnt_evlt_prst"):
                qty = number(row, "real_qty")
                if not qty:
                    continue
                if row.get("stk_cd") != "M04020000":
                    raise BrokerError("키움 미니금 등 다른 금현물 종목은 아직 지원하지 않습니다.")
                rows.append(position("KRX_GOLD", "KRX 금현물", qty, number(row, "avg_prc")))
        cash = (await self.pages(user, cid, env, "kt50021", {}))[-1]
        no_debt(cash, "etc_loan_tot", "dly_amt")
        balances = {"KRW": {key: number(cash, key) for key in ("entra", "prsm_entra")}}
        rows.append(position("CASH_KRW", "원화 현금", balances["KRW"]["prsm_entra"], 1))
        return merge(rows), balances

    async def stream(self, user, cid, environment, changed):
        from services.brokers.push import kiwoom_stream
        await kiwoom_stream(self, user, cid, environment, changed)

    def status(self, user):
        from services.brokers.push import status
        return status(self.definition.id, user)

    def forget(self, user, cid):
        from services.brokers.push import forget
        forget(self.definition.id, user, cid)
