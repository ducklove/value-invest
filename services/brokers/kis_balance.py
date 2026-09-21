"""한국투자증권 주식 잔고를 포트폴리오 수량·통화로 정규화한다."""

import math
import re
from datetime import datetime, timedelta, timezone

from cache_layer import MemoryTTLCache
from repositories.broker_secrets import BrokerError
from services.brokers import kis
from services.brokers.symbols import foreign_code
from services.portfolio.identifiers import CASH_FX_CODE

_listing = MemoryTTLCache("kis_account_listing", default_ttl_seconds=3600)
MARKETS = (("NASD", "USD", "200"), ("TKSE", "JPY", "070"), ("SEHK", "HKD", "120"),
           ("SHAA", "CNY", "160"), ("SZAA", "CNY", "170"))


def number(row: dict, key: str) -> float:
    try:
        raw = row[key]
        if isinstance(raw, bool) or raw is None or raw == "":
            raise ValueError
        value = float(str(raw).replace(",", ""))
        if not math.isfinite(value):
            raise ValueError
        return value
    except (KeyError, ValueError, TypeError):
        raise BrokerError("한국투자증권 잔고의 수량·금액이 누락되거나 올바르지 않아 갱신하지 않았습니다.") from None


def block(page: dict, name: str) -> list[dict]:
    value = page.get(name)
    if isinstance(value, dict) and value:
        value = [value]
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise BrokerError("한국투자증권 잔고 응답이 불완전하여 기존 잔고를 유지합니다.")
    return value


def position(code: str, name: str, quantity: float, price: float, currency: str) -> dict:
    if not code.startswith("CASH_") and (quantity < 0 or price < 0):
        raise BrokerError("음수 주식 잔고·매입가는 자동 합산을 지원하지 않습니다.")
    return {"stock_code": code, "stock_name": name, "quantity": quantity, "avg_price": price,
            "avg_price_currency": currency, "currency": currency}


async def listed(user: str, link: dict, code: str) -> bool:
    cache_key = (link["credential_id"], code)
    cached = _listing.get(cache_key)
    if cached is not None:
        return cached
    page = (await kis.pages(user, link["credential_id"], kis.STOCK_INFO,
                           {"PRDT_TYPE_CD": "300", "PDNO": code}, link["environment"]))[0]
    rows = block(page, "output")
    if len(rows) != 1:
        raise BrokerError("한국투자증권 종목의 상장 상태를 확인하지 못했습니다.")
    row = rows[0]
    today = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
    def valid_date(value):
        return isinstance(value, str) and bool(re.fullmatch(r"[12][0-9]{7}", value))
    abolished = str(row.get("lstg_abol_dt") or "").strip()
    active = any(valid_date(row.get(prefix + "_lstg_dt")) and row[prefix + "_lstg_dt"] <= today
                 and (not valid_date(row.get(prefix + "_lstg_abol_dt")) or row[prefix + "_lstg_abol_dt"] > today)
                 for prefix in ("scts_mket", "kosdaq_mket"))
    if "scts_mket_lstg_dt" not in row or "kosdaq_mket_lstg_dt" not in row or "lstg_abol_dt" not in row:
        raise BrokerError("한국투자증권 종목의 상장일·상장폐지일 응답이 불완전합니다.")
    active = active and not (valid_date(abolished) and abolished <= today)
    _listing.set(cache_key, active)
    return active


async def fetch_snapshot(user: str, link: dict) -> tuple[list[dict], dict]:
    cid, env, account = link["credential_id"], link["environment"], link["account_no"]
    if not re.fullmatch(r"[0-9]{10}", account) or account[-2:] != "01" or link.get("product", "stocks") != "stocks":
        raise BrokerError("한국투자증권은 주식 계좌(8자리 계좌번호 + 상품코드 01)를 지원합니다. 금·선물·연금 계좌는 별도로 관리해 주세요.")
    base = {"CANO": account[:8], "ACNT_PRDT_CD": account[8:]}
    domestic = await kis.pages(user, cid, kis.DOMESTIC, {**base, "AFHR_FLPR_YN": "N", "OFL_YN": "",
        "INQR_DVSN": "02", "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "00"}, env)
    totals = [row for page in domestic for row in block(page, "output2")]
    if not totals:
        raise BrokerError("한국투자증권 원화 예수금 합계를 확인하지 못했습니다.")
    total = totals[-1]
    if number(total, "tot_loan_amt") != 0:
        raise BrokerError("대출이 있는 한국투자증권 계좌는 자동 합산을 지원하지 않습니다.")
    if number(total, "cma_evlu_amt") != 0:
        raise BrokerError("CMA 평가잔고가 있는 한국투자증권 계좌는 현재 자동 합산을 지원하지 않습니다. 수동 계좌로 관리해 주세요.")
    balances = {"KRW": {key: number(total, key) for key in ("dnca_tot_amt", "nxdy_excc_amt", "prvs_rcdl_excc_amt")},
                "_scope": "domestic" if env == "mock" else "stocks", "_excluded": []}
    output = [position("CASH_KRW", "원화 현금", balances["KRW"]["prvs_rcdl_excc_amt"], 1, "KRW")]
    for page in domestic:
        for row in block(page, "output1"):
            qty = number(row, "hldg_qty")
            if not qty:
                continue
            if number(row, "loan_amt") != 0:
                raise BrokerError("융자 잔고는 자동 합산을 지원하지 않습니다.")
            code = str(row.get("pdno", "")).strip()
            if not re.fullmatch(r"[0-9][0-9A-Z]{5}", code):
                raise BrokerError("한국투자증권에 주식·ETF 외 잔고가 있어 전체 갱신을 보류했습니다.")
            if env == "live" and not await listed(user, link, code):
                balances["_excluded"].append(code)
                continue
            output.append(position(code, str(row.get("prdt_name") or code), qty, number(row, "pchs_avg_pric"), "KRW"))
    if env == "mock":
        if link.get("include_overseas", True):
            raise BrokerError("한국투자증권 모의계좌는 국내주식·원화 예수금만 지원합니다.")
        return output, balances
    # 주식 포함 옵션과 무관하게 외화 예수금은 항상 조회한다.
    present = await kis.pages(user, cid, kis.PRESENT, {**base, "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "000",
                                                     "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"}, env)
    for page in present:
        for row in block(page, "output2"):
            currency = str(row.get("crcy_cd", "")).strip()
            if currency == "KRW":
                continue
            if "CASH_" + currency not in CASH_FX_CODE or currency in balances:
                raise BrokerError("한국투자증권 외화 예수금의 통화가 누락되거나 중복되었습니다.")
            values = {key: number(row, key) for key in ("frcr_dncl_amt_2", "frcr_buy_amt_smtl", "frcr_sll_amt_smtl")}
            # 체결기준 보유주식과 맞춰 미결제 매매대금을 반영한 현금이다. 주문가능액은 사용하지 않는다.
            values["settlement_cash"] = values["frcr_dncl_amt_2"] - values["frcr_buy_amt_smtl"] + values["frcr_sll_amt_smtl"]
            balances[currency] = values
            output.append(position("CASH_" + currency, currency + " 현금", values["settlement_cash"], 1, currency))
        totals = block(page, "output3")
        if not totals or any(number(row, "tot_loan_amt") != 0 for row in totals):
            raise BrokerError("해외계좌 합계가 누락되었거나 대출이 있어 잔고를 유지합니다.")
        if link.get("include_overseas", True):
            for row in block(page, "output1"):
                if number(row, "ccld_qty_smtl1") and str(row.get("buy_crcy_cd", "")).strip() not in {"USD", "HKD", "JPY", "CNY"}:
                    raise BrokerError("미국·일본·홍콩·중국 외 해외주식 잔고가 있어 자동 합산을 보류했습니다.")
    if link.get("include_overseas", True):
        markets = list(MARKETS)
        for page in present:
            for row in block(page, "output1"):
                if str(row.get("ovrs_excg_cd", "")).strip() in {"SEHK", "HKS"} and str(row.get("buy_crcy_cd", "")).strip() in {"CNY", "USD"}:
                    market = ("SEHK", str(row["buy_crcy_cd"]).strip(), "120")
                    if market not in markets:
                        markets.append(market)
        for exchange, currency, country in markets:
            pages = await kis.pages(user, cid, kis.OVERSEAS, {**base, "OVRS_EXCG_CD": exchange, "TR_CRCY_CD": currency}, env)
            for page in pages:
                for row in block(page, "output1"):
                    qty = number(row, "ovrs_cblc_qty")
                    if not qty:
                        continue
                    row_currency = str(row.get("tr_crcy_cd", "")).strip()
                    if row_currency not in {"USD", "HKD", "JPY", "CNY"}:
                        raise BrokerError("한국투자증권 해외주식 통화를 확인하지 못했습니다.")
                    if row_currency != currency:
                        raise BrokerError("한국투자증권 해외조회 통화와 잔고 통화가 달라 갱신을 보류했습니다.")
                    code = foreign_code(row.get("ovrs_pdno", ""), country)
                    output.append(position(code, str(row.get("ovrs_item_name") or code), qty, number(row, "pchs_avg_pric"), row_currency))
    merged = {}
    for row in output:
        code = row["stock_code"]
        if code in merged:
            previous = merged[code]
            if previous["currency"] != row["currency"] or row["quantity"] + previous["quantity"] <= 0:
                raise BrokerError("한국투자증권 중복 잔고의 통화·수량을 확인하지 못했습니다.")
            qty = row["quantity"] + previous["quantity"]
            previous["avg_price"] = (row["quantity"] * row["avg_price"] + previous["quantity"] * previous["avg_price"]) / qty
            previous["quantity"] = qty
        else:
            merged[code] = row
    if env == "live" and link.get("include_overseas", True):
        country_by_exchange = {"NASD": "200", "NAS": "200", "NYSE": "200", "NYS": "200", "AMEX": "200", "AMS": "200",
                               "TKSE": "070", "TSE": "070", "SEHK": "120", "HKS": "120", "SHAA": "160", "SHA": "160", "SZAA": "170", "SZA": "170"}
        for page in present:
            for row in block(page, "output1"):
                if not number(row, "ccld_qty_smtl1"):
                    continue
                country = country_by_exchange.get(str(row.get("ovrs_excg_cd", "")).strip())
                code = foreign_code(row.get("pdno", ""), country) if country else None
                if code not in merged or merged[code]["currency"] != str(row.get("buy_crcy_cd", "")).strip():
                    raise BrokerError("한국투자증권 해외 잔고를 모두 확인하지 못했습니다. 미니스탁·미지원 시장 또는 조회 중 잔고 변경을 확인해 주세요.")
    return list(merged.values()), balances
