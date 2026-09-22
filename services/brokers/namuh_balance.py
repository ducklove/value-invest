"""NH 상품별 전체 잔고 조회·정규화."""

from domain.broker_assets import ACCOUNT_PRODUCTS
from repositories.broker_secrets import BrokerError
from services.brokers import namuh, namuh_listing, overseas_realtime
from services.brokers.parsing import number
from services.brokers.symbols import domestic_code, foreign_code, records, summary
from services.portfolio.identifiers import CASH_FX_CODE


async def overseas_code(row: dict, currency: str) -> str:
    country = str(row.get("fc_sec_trd_nat_cd", "")).strip()
    symbol = str(row.get("iem_cd", "")).strip().upper()
    # 운영 전체국가 잔고에서 확인한 NH 국가코드. 동일 심벌(예: AAA)이 여러
    # 시장에 있으므로 호주·독일·베트남은 공식 GIC와 결제통화까지 대조한다.
    nation = {"020": "AUS", "050": "DEU", "660": "VNM"}.get(country)
    if nation:
        await overseas_realtime.ensure_master()
        code = overseas_realtime.code_for_balance(nation + symbol, currency)
        if not code:
            raise BrokerError("NH 해외 잔고의 거래소·통화를 확인하지 못해 기존 잔고를 유지합니다.")
        return code
    return foreign_code(symbol, country)


async def fetch_snapshot(user: str, link: dict) -> tuple[list[dict], dict]:
    cid, env, account = link["credential_id"], link["environment"], link["account_no"]
    own_accounts = await namuh.accounts(user, cid)
    if {"account_no": account, "environment": env} not in own_accounts:
        raise BrokerError("앱키에서 연결 계좌를 확인할 수 없습니다. 계좌 연결을 확인해 주세요.")
    product = link.get("product", "stocks")
    if product not in ACCOUNT_PRODUCTS:
        raise BrokerError("지원되지 않는 NH 계좌 종류입니다.")
    if product in {"krfuture", "gbfuture"}:
        from services.brokers.derivatives import fetch_derivatives
        return await fetch_derivatives(user, link)
    domestic = await namuh.pages(user, cid, "/krstock/inquiry/v1/balance" if product == "stocks" else "/krgold/inquiry/v1/goldDepositAndBalance", {
        # NH 상장폐지구분: 1=상장종목. 비상장도 반환될 수 있어 종목마스터로 재검증한다.
        "act_no": account, "bnc_bse_cd": "1", "ltg_aot_dit_cd": "1", "aet_bse": "1",
        "qut_dit_cd": "UNT", "aly_qut_cd": "2",
    } if product == "stocks" else {"act_no": account}, env)
    # 연속조회에서는 마지막 합계 블록에 최종 평가금액이 채워진다.
    total = summary(next((page for page in reversed(domestic) if page.get("Output_0")), domestic[-1]))
    if any(number({key: total.get(key) or 0}, key) != 0 for key in ("fnn_amt", "rba", "lon_amt")):
        raise BrokerError("융자·미수·대출이 있는 계좌는 자동 합산을 지원하지 않습니다. 수동 계좌로 관리해 주세요.")
    balances = {"KRW": {key: number(total, key) for key in ("dca", "nxt_dd_dca", "nxt2_dd_dca")}}
    # 계좌에 따라 일반 주문가능액 없이 증거금률별 금액(20/30/40/100%)만 온다.
    # 서로 다른 주문가능액을 대체하거나 예수금으로 합산하지 않는다.
    balances["KRW"].update({key: number(total, key) for key in (
        "drn_pbl_amt", "orr_pbl_amt", "orr_pbl_amt1", "orr_pbl_amt2", "orr_pbl_amt3", "orr_pbl_amt4",
    ) if key in total})
    output = []
    listed = None
    excluded = set()
    for page in domestic:
        for row in records(page):
            raw_code = str(row.get("iem_cd", "")).strip()
            code = "CMA_RP_KRW" if raw_code == "RKRW221" else domestic_code(raw_code)
            if code not in {"KRX_GOLD", "CMA_RP_KRW"}:
                if listed is None:
                    listed = await namuh_listing.listed_codes()
                if code not in listed:
                    # 비상장 잔고의 수량·매입가가 비어 있어도 다른 잔고를 가져온다.
                    excluded.add(code)
                    continue
            # itg_bnc_qty는 결제 잔고다. rsdl_qty에는 미결제 매수·매도(ny_stl_qty)가
            # 반영되어 있으므로 체결 직후 잔량을 사용한다. 미결제 수량을 다시 더하지 않는다.
            qty = number(row, "rsdl_qty")
            if not qty:
                continue
            if code == "CMA_RP_KRW":
                # CMA RP는 수량=원금, 현재가/매입가=0으로 반환된다.
                # 평가액을 원 단위 잔액으로 보관해 고정 단위가 1원으로 평가한다.
                value = number(row, "eal_amt")
                cost = value - number(row, "eal_pls_amt")
                if value <= 0 or cost < 0 or qty < 0:
                    raise BrokerError("CMA RP의 평가액·매입금액을 확인할 수 없어 잔고를 유지합니다.")
                output.append({"stock_code": "CMA_RP_KRW", "stock_name": "CMA 원화RP",
                               "quantity": value, "avg_price": cost / value, "avg_price_currency": "KRW", "currency": "KRW"})
                continue
            output.append({"stock_code": code, "stock_name": str(row.get("iem_nm") or row["iem_cd"]),
                           "quantity": qty, "avg_price": number(row, "phs_pr"), "avg_price_currency": "KRW", "currency": "KRW"})
    if excluded:
        balances["_excluded"] = sorted(excluded)
    output.append({"stock_code": "CASH_KRW", "stock_name": "원화 현금", "quantity": balances["KRW"]["nxt2_dd_dca"],
                   "avg_price": 1, "avg_price_currency": "KRW", "currency": "KRW"})
    if product == "stocks" and link.get("include_overseas", True):
        # 운영에서 000=전체국가로 확인했다. 국가를 나열하면 베트남·호주·독일 등이
        # 조용히 누락된다. 각 행의 국가코드로 해석하며 모든 연속조회 페이지를 반영한다.
        pages = await namuh.pages(user, cid, "/gbstock/inquiry/v1/balance", {
            "act_no": account, "qut_iqr_dit_cd": "9", "fc_sec_trd_nat_cd": "000", "cur_cd": "KRW", "xns_dit_cd": "1",
        }, env)
        summary(pages[0])
        for page in pages:
            for row in records(page):
                qty = number(row, "cns_bse_bnc_qty")
                if not qty:
                    continue
                currency = str(row.get("cur_cd", "")).strip()
                if "CASH_" + currency not in CASH_FX_CODE:
                    raise BrokerError("해외 잔고의 거래 통화를 확인할 수 없습니다.")
                output.append({"stock_code": await overseas_code(row, currency),
                               "stock_name": str(row.get("iem_nm") or row.get("oss_iem_eng_nm") or row["iem_cd"]),
                               "quantity": qty, "avg_price": number(row, "fc_phs_uit_pr"), "avg_price_currency": currency, "currency": currency})
    # 외화 예수금은 해외주식 조회 여부와 별개로 항상 결제 후 잔액을 가져온다.
    margins = await namuh.pages(user, cid, "/gbstock/inquiry/v1/margin", {"act_no": account}, env) if product == "stocks" else []
    for page in margins:
        rows = page.get("Output_0")
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise BrokerError("통화별 예수금 조회가 완료되지 않았습니다.")
        for row in rows:
            currency = str(row.get("cur_cd", "")).strip()
            # 통화별 잔고 뒤에 오는 원화 환산 합계는 실제 통화 잔고가 아니다.
            if currency in {"KRW", "<원화환산합계>"}:
                continue
            if "CASH_" + currency not in CASH_FX_CODE:
                raise BrokerError("지원되지 않는 예수금 통화입니다.")
            if currency in balances:
                raise BrokerError("중복된 통화 잔고가 반환되어 갱신하지 않았습니다.")
            balances[currency] = {key: number(row, key) for key in ("fc_dca", "stl_af_fc_dca", "fc_drn_pbl_amt")}
            output.append({"stock_code": "CASH_" + currency, "stock_name": currency + " 현금",
                           "quantity": balances[currency]["stl_af_fc_dca"], "avg_price": 1, "avg_price_currency": currency, "currency": currency})
    merged = {}
    for row in output:
        code = row["stock_code"]
        if not code.startswith("CASH_") and (row["quantity"] < 0 or row["avg_price"] < 0):
            raise BrokerError("음수 보유 수량 또는 매입가가 반환되어 동기화를 보류했습니다.")
        if code in merged:
            before = merged[code]
            qty = before["quantity"] + row["quantity"]
            if before["currency"] != row["currency"] or not qty:
                raise BrokerError("NH 잔고의 중복 종목을 합산할 수 없습니다.")
            before["avg_price"] = (before["quantity"] * before["avg_price"] + row["quantity"] * row["avg_price"]) / qty
            before["quantity"] = qty
        else:
            merged[code] = row
    return list(merged.values()), balances
