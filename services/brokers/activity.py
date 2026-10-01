"""NH 종합거래내역을 현금 변경 없이 정규화한다. 개인 식별 원문은 저장하지 않는다."""

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from domain.broker_activity import INCOME_KINDS
from domain.broker_activity import stamp as stamp
from domain.timeutil import KST
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


# ---------------------------------------------------------------------------
# 배당 전용 가져오기 (2026-10 운영 응답 검증)
#
# 종합거래내역에는 거래 일련번호·거래 전 예수금이 없어 일반 수입·입출금 자동 반영은 계속
# 보류한다. 배당만 아래 두 경로로 가져온다. 현금·NAV 입출금은 변경하지 않는다.
# - 국내: 종합거래내역의 원화 '배당금'/'분배금' 행. 키 = 계좌 지문·처리일·적요·종목·동일 키 순번.
#   금액은 키가 아닌 내용 버전에 두어, 증권사 정정은 중복 대신 '확인 필요'로 전환된다.
# - 해외: 해외주식 일별거래내역(입금, 외화주식). 키 = 계좌 지문·처리일·거래일련번호.

GB_DAILY = "/gbstock/inquiry/v1/dailyTransaction"
# 조회할 내역이 없으면 Output 블록 없이 이 응답코드만 온다(운영 확인).
EMPTY_RESPONSE_CODES = frozenset({"13578"})
# 내용 버전에서 제외하는 파생 값. 조회 범위·종목 마스터에 따라 달라질 수 있다.
VOLATILE_FIELDS = frozenset({"balance_check", "stock_code"})


def _compact(value) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def _ymd(row: dict, key: str) -> str | None:
    try:
        return datetime.strptime(str(row.get(key) or "").strip(), "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def _finish(data: dict, identity: list) -> dict:
    stable = {key: value for key, value in data.items() if key not in VOLATILE_FIELDS}
    return {**data, "source_key": digest(identity), "source_revision": digest(stable)}


def _lenient(row: dict, key: str) -> Decimal | None:
    try:
        return numeric(row, key)
    except BrokerError:
        return None


def is_domestic_dividend_label(label: str) -> bool:
    text = _compact(label)
    if not text or any(word in text for word in ("주식배당", "외화", "취소", "정정", "환급", "반환", "출금")):
        return False
    return "배당" in text or "분배금" in text


def domestic_dividend(row: dict, link: dict, ordinal: int, previous: dict | None) -> dict | None:
    """종합거래내역 원화 배당 행. 배당이 아니거나 국내 종목이 아니면 None(보류 유지)."""
    if not isinstance(row, dict) or not is_domestic_dividend_label(row.get("sps_cd_krl_anm")):
        return None
    currency = str(row.get("cur_cd") or "").strip().upper() or "KRW"
    raw_code = str(row.get("iem_cd") or "").strip().upper()
    code = raw_code[3:9] if re.fullmatch(r"KR7[0-9A-Z]{6}[0-9]{3}", raw_code) else raw_code
    code = code[1:] if re.fullmatch(r"A[0-9A-Z]{6}", code) else code
    if currency != "KRW" or not re.fullmatch(r"[0-9][0-9A-Z]{5}", code):
        return None
    if row.get("act_no") not in (None, "", link["account_no"]):
        raise BrokerError("NH 거래내역의 계좌를 확인할 수 없습니다.")
    booked = _ymd(row, "trd_dt")
    if not booked:
        raise BrokerError("NH 응답의 거래일자가 없거나 올바르지 않아 배당 가져오기를 보류했습니다.")
    actual = _ymd(row, "ral_trd_dt")
    label = str(row.get("sps_cd_krl_anm") or "").strip()[:100]
    gross, tax, fee = numeric(row, "trd_amt"), numeric(row, "tax_sum"), numeric(row, "trd_orn_fee")
    valid = gross is not None and tax is not None and gross > 0 and 0 <= tax < gross and not fee
    net = gross - tax if valid else None
    # 종합거래내역은 거래 전 예수금이 없다. 바로 앞 행의 거래 후 예수금과의 차이로 검산한다.
    after, before = _lenient(row, "trd_af_dca"), _lenient(previous, "trd_af_dca") if previous else None
    check = "no_previous" if before is None or after is None else "matched" if net is not None and after - before == net else "mismatch"
    data = {"date": actual if actual and actual <= booked else booked, "booked_date": booked, "serial": "",
            "currency": "KRW", "stock_code": code, "symbol": "", "stock_name": str(row.get("iem_nm") or "")[:80],
            "description": label, "net_amount": float(net) if net is not None else None,
            "income_amount": float(net) if net is not None else None,
            "gross_amount": float(gross) if valid else None, "tax_amount": float(tax) if valid else None,
            "fee_amount": 0.0 if valid else None, "fx_rate": 1.0, "domestic_tax_krw": 0,
            "auto_kind": "dividend" if valid else "review", "nh_dividend": True, "source": "nh_total",
            "verification": "nh_confirmed" if valid else "needs_review", "balance_check": check}
    identity = [link["account_fingerprint"], link["environment"], "nh-krdiv", booked, _compact(label), code, ordinal]
    return _finish(data, identity)


def overseas_symbol(symbol: str) -> tuple[str, str]:
    """일별거래내역 종목코드 '티커 시장'(예: 'XYZ US')을 분리한다."""
    match = re.fullmatch(r"([A-Z0-9./-]{1,16})\s+([A-Z]{2})", str(symbol or "").strip().upper())
    return (match[1], match[2]) if match else ("", "")


async def resolve_overseas_code(symbol: str, currency: str) -> str:
    """잔고 동기화와 같은 종목코드로 해석한다. 실패해도 배당은 원래 티커로 기록한다."""
    from services.brokers import overseas_realtime
    from services.brokers.symbols import foreign_code
    ticker, market = overseas_symbol(symbol)
    if not ticker:
        return ""
    try:
        if market in {"US", "HK", "JP"}:
            return foreign_code(ticker, {"US": "200", "HK": "120", "JP": "070"}[market])
        nation, listing = {"AU": ("AUS", "AUD"), "DE": ("DEU", "EUR"), "VN": ("VNM", "VND")}.get(market, (None, None))
        if nation and currency == listing:
            await overseas_realtime.ensure_master()
            return overseas_realtime.code_for_balance(nation + ticker, listing) or ""
    except BrokerError:
        return ""
    return ""


def verified_rate(base: Decimal | None, rate: Decimal | None, taxable_krw: Decimal | None) -> Decimal | None:
    """NH 적용환율을 원화 과세표준으로 자가검증한다. 1단위당 원이 아니면 100단위 표기를 한 번만 시도한다."""
    if base is None or rate is None or taxable_krw is None or base <= 0 or rate <= 0 or taxable_krw <= 0:
        return None
    for candidate in (rate, rate / 100):
        if abs((base * candidate).quantize(Decimal(1), rounding=ROUND_HALF_UP) - taxable_krw) <= 1:
            return candidate
    return None


def overseas_dividend(row: dict, link: dict, stock_code: str = "") -> dict | None:
    """해외주식 일별거래내역 입금 행. 배당·세금 정산 외의 행은 None."""
    if not isinstance(row, dict):
        raise BrokerError("NH 해외 거래내역 형식이 올바르지 않습니다.")
    label = _compact(row.get("sps_cd_nm"))
    if "제세금환급" in label:
        role = "tax_refund"
    elif "배당" in label or "분배" in label:
        role = "dividend" if not any(word in label for word in ("취소", "정정", "주식배당")) else "review"
    else:
        return None
    booked = _ymd(row, "trd_dt")
    raw_serial = row.get("trd_sno")
    serial = str(raw_serial).strip() if isinstance(raw_serial, (int, str)) and not isinstance(raw_serial, bool) else ""
    if not booked or not re.fullmatch(r"[0-9]{1,10}", serial):
        raise BrokerError("NH 해외 거래내역의 거래일자·일련번호를 확인할 수 없어 배당 가져오기를 보류했습니다.")
    currency = str(row.get("cur_cd_nm") or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", currency) or currency == "KRW":
        raise BrokerError("NH 해외 배당의 통화를 확인할 수 없어 배당 가져오기를 보류했습니다.")
    actual = _ymd(row, "ral_trd_dt")
    net, local, domestic = numeric(row, "fc_trd_amt"), numeric(row, "fc_tax_sum"), numeric(row, "tax_sum")
    rate, taxable = numeric(row, "aly_xcg_rt"), numeric(row, "krw_sas_amt")
    checks = net is not None and net > 0 and local is not None and local >= 0 and domestic is not None and domestic >= 0
    # 예수금 전후 값이 오면 외화 +순입금, 원화 −국내세 관계를 검산한다(운영 69/69 성립).
    fc_before, fc_after = _lenient(row, "trd_bf_fc_dca"), _lenient(row, "trd_af_fc_dca")
    krw_before, krw_after = _lenient(row, "trd_bf_dca"), _lenient(row, "trd_af_dca")
    if checks and fc_before is not None and fc_after is not None and abs(fc_after - fc_before - net) > Decimal("0.005"):
        checks = False
    if checks and krw_before is not None and krw_after is not None and abs(krw_after - krw_before + domestic) > Decimal("0.5"):
        checks = False
    # 배당: 세전 = 순입금 + 현지세. 세금 정산: fc_tax_sum 이 원배당 세전(과세표준 기준)이다.
    gross = net + local if checks and role == "dividend" else None
    base = gross if role == "dividend" else local
    fx = verified_rate(base, rate, taxable) if checks and role != "review" else None
    verified = fx is not None
    net_krw = (net * fx).quantize(Decimal(1), rounding=ROUND_HALF_UP) - domestic if verified else None
    ticker, market = overseas_symbol(row.get("iem_cd"))
    name = str(row.get("iem_krl_nm") or row.get("oss_iem_nm") or "")[:80]
    description = "외화제세금환급 · 배당 세금 정산" if role == "tax_refund" else str(row.get("sps_cd_nm") or "").strip()[:100]
    data = {"date": actual if actual and actual <= booked else booked, "booked_date": booked, "serial": serial,
            "currency": currency, "stock_code": stock_code, "symbol": f"{ticker} {market}".strip(), "stock_name": name,
            "description": description, "net_amount": float(net) if net is not None else None,
            "income_amount": float(net) if net is not None and net > 0 else None,
            "gross_amount": float(gross) if gross is not None else None,
            "tax_amount": float(local) if role == "dividend" and local is not None else None,
            "fee_amount": None, "fx_rate": float(fx) if verified else None,
            "domestic_tax_krw": float(domestic) if domestic is not None else None,
            "gross_krw": float(taxable) if verified and role == "dividend" else None,
            "net_krw": float(net_krw) if net_krw is not None else None,
            "auto_kind": "dividend" if verified else "review", "nh_dividend": role == "dividend",
            "adjustment": "tax_refund" if role == "tax_refund" else None, "source": "nh_gbdaily",
            "verification": "nh_confirmed" if verified else "needs_review"}
    if role == "tax_refund":
        data["refund_base_gross"] = float(local) if local is not None else None
    identity = [link["account_fingerprint"], link["environment"], "nh-gbdaily", booked, serial.lstrip("0") or "0"]
    return _finish(data, identity)


def _check_message(page: dict):
    message = page.get("message")
    detail = str(message.get("usr_msg") or "") if isinstance(message, dict) else ""
    if any(word in detail for word in ("오류", "실패", "불가", "권한", "초과", "잘못")):
        raise BrokerError("NH 거래내역 조회를 처리하지 못했습니다. 기존 내역을 유지합니다.")


def _put(collected: dict, data: dict):
    old = collected.get(data["source_key"])
    if old and old != data:
        raise BrokerError("동일 거래의 내용이 달라 내역 갱신을 보류했습니다.")
    collected[data["source_key"]] = data


async def _domestic_dividends(user: str, link: dict, start: date, end: date, collected: dict):
    ordinals: dict[tuple, int] = {}
    while start <= end:
        until = min(start + timedelta(days=30), end)
        pages = await namuh.pages(user, link["credential_id"], "/common/inquiry/v1/totalTransaction", {
            "act_no": link["account_no"], "iqr_tp_cd": "1", "iqr_rge_cd": "2",
            "iqr_sta_dt": start.strftime("%Y%m%d"), "iqr_end_dt": until.strftime("%Y%m%d"),
            "iem_llf_cd": "00", "act_trd_dtl_cd": "00",
        }, "live")
        previous = None
        for page in pages:
            _check_message(page)
            rows = page.get("Output_0")
            if not isinstance(rows, list):
                # 누락 블록을 0건 성공으로 처리하면 누락된 내역이 이후에도 숨겨진다.
                raise BrokerError("NH 거래내역 응답을 확인할 수 없어 이전 내역을 유지했습니다.")
            for row in rows:
                if not isinstance(row, dict):
                    raise BrokerError("NH 거래내역 형식이 올바르지 않습니다.")
                if is_domestic_dividend_label(row.get("sps_cd_krl_anm")):
                    group = (str(row.get("trd_dt")), _compact(row.get("sps_cd_krl_anm")), str(row.get("iem_cd") or "").strip())
                    ordinals[group] = ordinals.get(group, 0) + 1
                    data = domestic_dividend(row, link, ordinals[group], previous)
                    if data:
                        if not start.isoformat() <= data["booked_date"] <= until.isoformat():
                            raise BrokerError("NH 조회 기간 밖의 거래가 반환되었습니다.")
                        _put(collected, data)
                previous = row
        start = until + timedelta(days=1)


async def _overseas_dividends(user: str, link: dict, start: date, end: date, collected: dict):
    pages = await namuh.pages(user, link["credential_id"], GB_DAILY, {
        "act_no": link["account_no"], "iqr_sta_dt": start.strftime("%Y%m%d"), "iqr_end_dt": end.strftime("%Y%m%d"),
        # 01=입금, 00001=외화주식. 해외 ETF 분배금도 외화주식 입금으로 온다(운영 확인).
        "act_trd_cfc_cd": "01", "iem_mlf_cd": "00001", "iem_cd": "",
    }, "live")
    codes: dict[tuple, str] = {}
    for page in pages:
        _check_message(page)
        rows = page.get("Output_0")
        if rows is None and str(page.get("rsp_cd") or "").strip() in EMPTY_RESPONSE_CODES:
            continue
        if not isinstance(rows, list):
            raise BrokerError("NH 해외 거래내역 응답을 확인할 수 없어 이전 내역을 유지했습니다.")
        for row in rows:
            if not isinstance(row, dict):
                raise BrokerError("NH 해외 거래내역 형식이 올바르지 않습니다.")
            if not overseas_dividend(row, link):
                continue
            currency = str(row.get("cur_cd_nm") or "").strip().upper()
            key = (str(row.get("iem_cd") or ""), currency)
            if key not in codes:
                codes[key] = await resolve_overseas_code(*key)
            data = overseas_dividend(row, link, codes[key])
            if not start.isoformat() <= data["booked_date"] <= end.isoformat():
                raise BrokerError("NH 조회 기간 밖의 거래가 반환되었습니다.")
            _put(collected, data)


async def fetch(user: str, link: dict, start: date, end: date) -> list[dict]:
    """배당 전용 가져오기. 일반 수입·입출금은 거래 식별자 검증 전까지 보류한다."""
    if link["environment"] != "live":
        raise BrokerError("NH 종합거래내역은 실계좌에서만 제공됩니다.")
    if start > end or end > datetime.now(KST).date() or (end - start).days > 366:
        raise BrokerError("거래내역 조회 기간은 오늘까지 최대 1년으로 지정해 주세요.")
    collected: dict[str, dict] = {}
    await _domestic_dividends(user, link, start, end, collected)
    if link.get("product", "stocks") == "stocks":
        await _overseas_dividends(user, link, start, end, collected)
    return list(collected.values())
