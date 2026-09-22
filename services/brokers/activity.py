"""NH 종합거래내역을 현금 변경 없이 정규화한다. 개인 식별 원문은 저장하지 않는다."""

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from domain.broker_activity import INCOME_KINDS, KST
from domain.broker_activity import stamp as stamp
from repositories.broker_secrets import BrokerError
from services.brokers import namuh


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def numeric(row: dict, key: str) -> Decimal | None:
    value = row.get(key)
    if value is None or str(value).strip() == "":
        return None
    try:
        if isinstance(value, bool):
            raise ValueError
        number = Decimal(str(value).replace(",", "").strip())
        if not number.is_finite() or abs(number) > Decimal("1e15"):
            raise ValueError
        return number
    except (InvalidOperation, ValueError):
        raise BrokerError("NH 거래내역에 올바르지 않은 금액이 있어 이전 내역을 유지했습니다.") from None


def classify(label: str, net: Decimal | None, interest: Decimal | None) -> str:
    label = re.sub(r"\s+", "", label)
    # 정정·취소는 원거래 연결이 명세에 없어 자동으로 수입/입출금에 더하지 않는다.
    if any(word in label for word in ("취소", "정정", "반환", "환급", "대출", "융자", "상환")):
        return "review"
    if net is not None and net > 0:
        if any(word in label for word in ("배당", "분배금")) and "주식배당" not in label:
            return "dividend"
        if "이자" in label or "예탁금이용료" in label or (interest is not None and interest > 0):
            return "interest"
        if any(word in label for word in ("대여수수료", "대여료수입", "대여료입금")):
            return "other_income"
    if any(word in label for word in ("환전", "외화매입", "외화매도", "RP매", "CMA매", "발행어음매")):
        return "internal"
    if any(word in label for word in ("매수", "매도", "매매", "입고", "출고", "결제")):
        return "trade"
    if net is not None and net < 0 and any(word in label for word in ("수수료", "세금", "소득세", "주민세")):
        return "fee"
    if net and any(word in label for word in ("입금", "출금", "입출금", "이체")):
        return "transfer"
    return "review"


def normalize(row: dict, link: dict) -> dict:
    if not isinstance(row, dict) or row.get("act_no") not in (None, "", link["account_no"]):
        raise BrokerError("NH 거래내역의 계좌를 확인할 수 없습니다.")
    try:
        day = datetime.strptime(str(row["trd_dt"]).strip(), "%Y%m%d").date().isoformat()
    except (KeyError, ValueError, TypeError):
        raise BrokerError("NH 응답의 거래일자가 없거나 올바르지 않아 수입·입출금 내역 가져오기를 보류했습니다.") from None
    try:
        raw_serial = row["trd_sno"]
        if not isinstance(raw_serial, (str, int)) or isinstance(raw_serial, bool):
            raise ValueError
        serial = str(raw_serial).strip()
        if not re.fullmatch(r"[0-9A-Za-z-]{1,40}", serial):
            raise ValueError
    except (KeyError, ValueError, TypeError):
        raise BrokerError("NH 응답의 거래 일련번호가 없거나 올바르지 않아 수입·입출금 내역 가져오기를 보류했습니다.") from None
    currency = str(row.get("cur_cd") or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", currency):
        raise BrokerError("NH 거래내역의 통화를 확인할 수 없습니다.")
    suffix = "dca" if currency == "KRW" else "fc_dca"
    before, after = (numeric(row, "trd_" + part + "_" + suffix) for part in ("bf", "af"))
    net = after - before if before is not None and after is not None else None
    label = " · ".join(dict.fromkeys(str(row.get(key) or "").strip() for key in ("sps_cd_krl_anm", "act_trd_tp_nm"))).strip(" ·")[:200]
    tax, fee, gross, interest = (numeric(row, key) for key in ("tax_sum", "trd_orn_fee", "trd_amt", "int_amt"))
    kind = classify(label, net, interest)
    income = net if kind in INCOME_KINDS else None
    # RP 등 원금과 이자가 함께 입금되면 원금은 수입이 아니다. 명시적인 이자/세금만 사용한다.
    if kind == "interest" and interest and interest > 0:
        income = interest - tax if currency == "KRW" and tax is not None else None
        if income is not None and not 0 <= income <= net:
            income = None
    if kind in INCOME_KINDS and (income is None or income <= 0):
        kind = "review"
        income = None
    # 총액·공제액의 통화/의미를 추정하지 않고 실제 예수금 증감과 맞을 때만 세전 표시.
    verified_gross = gross is not None and tax is not None and fee is not None and net is not None and gross == net + tax + fee
    fx = Decimal(1) if currency == "KRW" else numeric(row, "aly_xcg_rt")
    # NH 환율의 단위가 통화마다 다를 수 있으므로 원화환산은 KRW만 자동 확정한다.
    # 외화 금액은 원통화로 보존하고, 사용자가 거래내역에서 1단위당 환율을 확인한다.
    fx = fx if currency == "KRW" else None
    raw_code = str(row.get("iem_cd") or "").strip()
    code = raw_code[3:9] if re.fullmatch(r"KR7[0-9A-Z]{6}[0-9]{3}", raw_code) else raw_code
    code = code[1:] if re.fullmatch(r"A[0-9A-Z]{6}", code) else code
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{0,23}", code):
        code = ""
    identity = [link["account_fingerprint"], link["environment"], day, serial.lstrip("0") or "0", currency]
    data = {"date": day, "serial": serial, "currency": currency, "stock_code": code,
            "stock_name": str(row.get("iem_nm") or "")[:80], "description": label,
            "net_amount": float(net) if net is not None else None,
            "income_amount": float(income) if income is not None else None,
            "gross_amount": float(gross) if verified_gross else None,
            "tax_amount": float(tax) if verified_gross else None,
            "fee_amount": float(fee) if verified_gross else None,
            "fx_rate": float(fx) if fx else None, "auto_kind": kind}
    return {**data, "source_key": digest(identity), "source_revision": digest(data)}


async def fetch(user: str, link: dict, start: date, end: date) -> list[dict]:
    if link["environment"] != "live":
        raise BrokerError("NH 종합거래내역은 실계좌에서만 제공됩니다.")
    if start > end or end > datetime.now(KST).date() or (end - start).days > 366:
        raise BrokerError("거래내역 조회 기간은 오늘까지 최대 1년으로 지정해 주세요.")
    collected = {}
    while start <= end:
        until = min(start + timedelta(days=30), end)
        pages = await namuh.pages(user, link["credential_id"], "/common/inquiry/v1/totalTransaction", {
            "act_no": link["account_no"], "iqr_tp_cd": "1", "iqr_rge_cd": "2",
            "iqr_sta_dt": start.strftime("%Y%m%d"), "iqr_end_dt": until.strftime("%Y%m%d"),
            "iem_llf_cd": "00", "act_trd_dtl_cd": "00",
        }, "live")
        for page in pages:
            message = page.get("message")
            detail = str(message.get("usr_msg") or "") if isinstance(message, dict) else ""
            if any(word in detail for word in ("오류", "실패", "불가", "권한", "초과", "잘못")):
                raise BrokerError("NH 거래내역 조회를 처리하지 못했습니다. 기존 내역을 유지합니다.")
            rows = page.get("Output_0")
            if not isinstance(rows, list):
                # 누락 블록을 0건 성공으로 처리하면 누락된 내역이 이후에도 숨겨진다.
                raise BrokerError("NH 거래내역 응답을 확인할 수 없어 이전 내역을 유지했습니다.")
            for row in rows:
                data = normalize(row, link)
                if not start.isoformat() <= data["date"] <= until.isoformat():
                    raise BrokerError("NH 조회 기간 밖의 거래가 반환되었습니다.")
                old = collected.get(data["source_key"])
                if old and old != data:
                    raise BrokerError("동일 거래의 내용이 달라 내역 갱신을 보류했습니다.")
                collected[data["source_key"]] = data
        start = until + timedelta(days=1)
    return list(collected.values())
