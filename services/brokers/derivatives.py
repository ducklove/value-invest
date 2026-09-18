"""선물의 계약 잔고와 증권사 계좌 평가액을 분리해 가져온다.

계약수×가격(명목금액)을 자산으로 더하지 않는다. 합산용 원화 평가기준액과
평가손익 두 항목의 합은 증권사가 반환한 계좌 순자산과 정확히 일치한다.
계약별 원본 단위·방향은 계좌 스냅샷에 따로 보존한다.
"""

import re
from datetime import datetime
from zoneinfo import ZoneInfo

from repositories.broker_secrets import BrokerError
from services.brokers import namuh
from services.brokers.parsing import number, object_block, record_block

_KST = ZoneInfo("Asia/Seoul")


def now_kst() -> datetime:
    return datetime.now(_KST)


def contract_code(row: dict) -> str:
    code = str(row.get("iem_cd", "")).strip()
    if not re.fullmatch(r"[A-Za-z0-9._/-]{1,32}", code):
        raise BrokerError("선물 종목코드를 확인할 수 없어 기존 잔고를 유지합니다.")
    return code


def quantity(row: dict, key: str) -> float:
    value = number(row, key)
    if value < 0 or not value.is_integer():
        raise BrokerError("선물 계약 수량을 확인할 수 없어 기존 잔고를 유지합니다.")
    return value


def valuation_rows(equity: float, pnl: float) -> list[dict]:
    # 수량 열은 계약 수가 아니라 원화 평가 금액이다. UI에도 그 단위를 명시한다.
    return [
        {"stock_code": "FUTURES_BASE_KRW", "stock_name": "선물 평가기준액", "quantity": equity - pnl,
         "avg_price": 1, "avg_price_currency": "KRW", "currency": "KRW"},
        {"stock_code": "FUTURES_PNL_KRW", "stock_name": "선물 평가손익", "quantity": pnl,
         "avg_price": 0, "avg_price_currency": "KRW", "currency": "KRW"},
    ]


async def fetch_derivatives(user: str, link: dict) -> tuple[list[dict], dict]:
    product, cid, env, account = (link[key] for key in ("product", "credential_id", "environment", "account_no"))
    now = now_kst()
    if product == "krfuture":
        path = "/krfuture/inquiry/v1/nightBalance" if now.hour >= 18 or now.hour < 6 else "/krfuture/inquiry/v1/balance"
        pages = await namuh.pages(user, cid, path, {"act_no": account}, env)
        total = object_block(next((page for page in reversed(pages) if page.get("Output_1")), pages[-1]), "Output_1")
        equity, pnl = number(total, "nas_tal"), number(total, "tot_eal_pls")
        positions = []
        for page in pages:
            for row in record_block(page, "Output_0"):
                qty = quantity(row, "tdy_ny_stl_qty")
                if not qty:
                    continue
                side = str(row.get("sby_dit_nm", "")).strip()
                if side not in {"매수", "매도"}:
                    raise BrokerError("선물의 매수·매도 구분을 확인할 수 없습니다.")
                positions.append({"code": contract_code(row), "name": str(row.get("iem_nm") or row["iem_cd"]),
                                  "side": side, "quantity": qty, "currency": "KRW",
                                  "average_price": number(row, "avg_pr"), "current_price": number(row, "now_pr"),
                                  "pnl": number(row, "eal_pls_amt")})
        details = {key: number(total, key) for key in ("dsg_csh", "dsg_sba_amt", "drn_pbl_amt") if key in total}
        basis = "NH 순자산총액 · 예탁금과 대용자산·평가손익 포함"
    else:
        # NH의 영업일자 입력을 양쪽 조회에 동일하게 적용한다. 빈 합계·조회 실패는
        # 휴장일 등을 0 잔고로 처리하지 않고 기존 스냅샷을 보존한다.
        date = now.strftime("%Y%m%d")
        body = {"act_no": account, "sls_dt": date}
        deposits = await namuh.pages(user, cid, "/gbfuture/inquiry/v1/deposit", {**body, "cur_cd": "TKR"}, env)
        total = object_block(deposits[-1], "Output_0")
        if str(total.get("cur_cd", "")).strip() not in {"TKR", "KRW"}:
            raise BrokerError("해외선물 원화 환산 합계를 확인할 수 없습니다.")
        equity, pnl = number(total, "fdv_dsg_aet_tot_eal_amt"), number(total, "fdv_ny_stl_eal_pls")
        margins = await namuh.pages(user, cid, "/gbfuture/inquiry/v1/margin", {**body, "cur_cd": "KRW"}, env)
        object_block(margins[0], "Output_0")
        positions = []
        for page in margins:
            for row in record_block(page, "Output_1"):
                for side, key in (("매수", "byn_ny_stl_bnc_qty"), ("매도", "sll_ny_stl_bnc_qty")):
                    qty = quantity(row, key)
                    if not qty:
                        continue
                    currency = str(row.get("cur_cd", "")).strip()
                    if not re.fullmatch(r"[A-Z]{3}", currency) or currency == "TKR":
                        raise BrokerError("해외선물의 계약 통화를 확인할 수 없습니다.")
                    # 증거금 API에는 진입단가·개별 평가손익이 없다. 0이나 당일 체결가로 채우지 않는다.
                    positions.append({"code": contract_code(row), "name": str(row.get("iem_nm") or row["iem_cd"]),
                                      "side": side, "quantity": qty, "currency": currency,
                                      "average_price": None, "current_price": None, "pnl": None})
        details = {key: number(total, key) for key in ("fdv_dsg_amt", "nxt_dd_dga_rnd", "fdv_brg_wtm", "fdv_wrw_pbl_amt") if key in total}
        basis = "NH 원화환산 예탁자산총평가액 · 계약 통화는 별도 표시"
    if pnl and not positions:
        raise BrokerError("선물 평가손익은 있으나 계약 잔고가 없어 동기화를 보류했습니다.")
    snapshot = {"product": product, "positions": positions, "equity": equity, "pnl": pnl, "currency": "KRW",
                "basis": basis, "as_of_date": now.date().isoformat(), "details": details}
    return valuation_rows(equity, pnl), {"_snapshot": snapshot}
