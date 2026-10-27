"""NH 당일 매매의 결제 예정 현금. 잔고·수익 원장에 다시 가산하지 않는다."""

from datetime import datetime

from domain.market_calendar import is_trading_day
from domain.timeutil import KST
from repositories.broker_secrets import BrokerError
from services.brokers import namuh
from services.brokers.activity import _check_message, numeric
from services.brokers.symbols import summary

PATH = "/krstock/inquiry/v1/realizedPnl"


async def fetch(user: str, link: dict, balances: dict) -> list[dict]:
    day = datetime.now(KST).date()
    trading = is_trading_day(day)
    if trading is False:
        return []  # 휴장일에는 NH가 전 거래일 합계를 그대로 반환할 수 있다.
    if trading is None:
        raise BrokerError("거래일 달력을 확인할 수 없어 당일 매매 자동 대조를 보류했습니다.")
    pages = await namuh.pages(user, link["credential_id"], PATH, {
        "act_no": link["account_no"], "iqr_dit_cd1": "2", "fee_dit_cd": "1",
        "qut_dit_cd": "UNT", "aly_qut_cd": "2",
    }, "live")
    for page in pages:
        _check_message(page)
    if not pages or datetime.now(KST).date() != day:
        raise BrokerError("NH 당일 매매의 조회일자를 확인할 수 없습니다.")
    # 연속조회 마지막 합계만 사용한다. 페이지마다 반복되는 계좌 합계를 더하지 않는다.
    total = summary(next((page for page in reversed(pages) if page.get("Output_0")), pages[-1]))
    buy, sell, charges, tomorrow, settlement = (numeric(total, key) for key in
        ("tdt_byn_amt", "tdt_sll_amt", "sdr_xps", "nxt_dd_dca", "nxt2_dd_dca"))
    if (any(value is None for value in (buy, sell, charges, tomorrow, settlement))
            or min(buy, sell, charges) < 0 or settlement - tomorrow != sell - buy - charges
            or tomorrow != numeric(balances["KRW"], "nxt_dd_dca")
            or settlement != numeric(balances["KRW"], "nxt2_dd_dca")):
        raise BrokerError("NH 당일 매매금액·비용과 결제 예정 예수금이 맞지 않아 자동 대조를 보류했습니다.")
    return [{"booked_date": day.isoformat(), "date": day.isoformat(), "currency": "KRW", "kind": kind, "amount": float(amount)}
            for kind, amount in (("trade_buy", -buy), ("trade_sell", sell), ("trade_charges", -charges))]
