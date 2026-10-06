"""국내선물은 부호 있는 종목 잔고와 조정 예수금으로, 해외선물은 별도 평가액으로 가져온다."""

import re
from decimal import Decimal

from domain.timeutil import now_kst
from repositories.broker_secrets import BrokerError
from services.brokers import namuh
from services.brokers.futures_contracts import contract_terms
from services.brokers.parsing import number, object_block, record_block


def contract_code(row: dict, key: str = "iem_cd") -> str:
    code = str(row.get(key, "")).strip()
    if not re.fullmatch(r"[A-Za-z0-9._/-]{1,32}", code):
        raise BrokerError("선물 종목코드를 확인할 수 없어 기존 잔고를 유지합니다.")
    return code


def quantity(row: dict, key: str) -> float:
    value = number(row, key)
    if value < 0 or not value.is_integer():
        raise BrokerError("선물 계약 수량을 확인할 수 없어 기존 잔고를 유지합니다.")
    return value


def domestic_quantity(row: dict, *, night: bool) -> float:
    if night:
        return quantity(row, "tdy_ny_stl_qty")
    # 주간 잔고에는 금일미결제수량이 없다. 전일 잔고에 당일 증감을 반영한다.
    # 청산가능수량은 미체결 청산 주문에 묶인 계약을 제외할 수 있어 사용하지 않는다.
    previous = quantity(row, "bf_dd_ny_stl_qty")
    change = number(row, "bnc_ind_qty")
    current = previous + change
    if not change.is_integer() or not current.is_integer() or current < 0:
        raise BrokerError("선물 계약 수량을 확인할 수 없어 기존 잔고를 유지합니다.")
    return current


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
        night = now.hour >= 18 or now.hour < 6
        path = "/krfuture/inquiry/v1/nightBalance" if night else "/krfuture/inquiry/v1/balance"
        body = {"act_no": account}
        if night:
            body.update({"ost_dit_cd": "9", "ost_dit_cd1": "1"})
        pages = await namuh.pages(user, cid, path, body, env)
        total = object_block(next((page for page in reversed(pages) if page.get("Output_1")), pages[-1]), "Output_1")
        equity, pnl = number(total, "nas_tal"), number(total, "tot_eal_pls")
        positions = []
        for page in pages:
            for row in record_block(page, "Output_0"):
                qty = domestic_quantity(row, night=night)
                if not qty:
                    continue
                side = str(row.get("sby_dit_nm", "")).strip()
                if side not in {"매수", "매도"}:
                    raise BrokerError("선물의 매수·매도 구분을 확인할 수 없습니다.")
                code = contract_code(row, "fno_iem_cd" if night else "iem_cd")
                name = str(row.get("iem_nm") or code).strip()
                terms = await contract_terms(user, link, code, name)
                positions.append({**terms, "code": code, "name": name, "stock_code": "KRFUT_" + code,
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
    if product == "krfuture":
        rows, cash = domestic_holdings(positions, equity)
        snapshot.update({"display": "holdings", "cash": cash, "quantity_unit": "underlying"})
        return rows, {"_snapshot": snapshot}
    return valuation_rows(equity, pnl), {"_snapshot": snapshot}



def domestic_holdings(positions: list[dict], equity: float) -> tuple[list[dict], float]:
    # 주식과 같은 가격 단위를 쓰고 계약수를 거래승수만큼 환산한다.
    # 동일 월물의 양방향 잔고는 부호 있는 수량·원가를 합산한다.
    grouped = {}
    notional = Decimal(0)
    for position in positions:
        qty = Decimal(str(position["quantity"])) * Decimal(str(position["multiplier"]))
        if position["side"] == "매도":
            qty = -qty
        current, average = (Decimal(str(position[key])) for key in ("current_price", "average_price"))
        if current <= 0 or average < 0:
            raise BrokerError("선물 가격을 확인할 수 없어 기존 잔고를 유지합니다.")
        notional += qty * current
        code = position["stock_code"]
        if code not in grouped:
            grouped[code] = {"quantity": Decimal(0), "cost": Decimal(0), "position": position}
        grouped[code]["quantity"] += qty
        grouped[code]["cost"] += qty * average
    rows = []
    for code, value in grouped.items():
        qty, position = value["quantity"], value["position"]
        if not qty:
            continue
        rows.append({"stock_code": code, "stock_name": position["name"], "quantity": float(qty),
                     "avg_price": float(value["cost"] / qty), "avg_price_currency": "KRW", "currency": "KRW",
                     "memo": "만기일 " + position["expiry_date"]})
    # 선물매도 평가액 - 선물매수 평가액 + 평가기준액 + 평가손익.
    # 현재 평가액을 써야 음수 종목 평가액과 더했을 때 NH 순자산총액과 일치한다.
    cash = float(Decimal(str(equity)) - notional)
    rows.append({"stock_code": "CASH_KRW", "stock_name": "예수금(KRW)", "quantity": cash,
                 "avg_price": 1, "avg_price_currency": "KRW", "currency": "KRW"})
    return rows, cash
