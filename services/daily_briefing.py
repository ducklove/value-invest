"""AI 브리핑 — 예약 배치로 미리 생성해 메신저로 푸시한다.

철학: '기다리지 않는 화면'. 사용자가 접속해 생성을 기다리는 대신, 정해진 시각의
systemd timer 배치가 결산 데이터로 브리핑을 만들어
이미 연결된 알림 채널(텔레그램/카카오)로 보낸다.

구성:
* ``build_briefing_context``  — 순수 데이터 조립(테스트 대상). 어제 NAV 변화,
  기여 상위/하위 종목, 신규 공시·리포트, 오늘 경제 일정, 시장 지표. 모두
  기존 저장 데이터/캐시에서 읽는다 — 새 스크래핑 없음.
* ``generate_briefing``       — 'daily_briefing' 모델 프로필로 LLM 호출
  (services.ai_client 경유, usage ledger 자동 기록). LLM 실패 시 컨텍스트를
  템플릿으로 렌더한 비-AI 요약으로 폴백 — 발송이 OpenRouter 가용성에 묶이지
  않는다.
* ``send_briefings``          — 옵트인 사용자 순회·생성·발송. 사용자별로
  격리(한 명의 실패가 다른 사용자를 막지 않음)하고 system_events 에 결과를
  남긴다.

옵트인은 user_settings K/V(``daily_briefing_enabled`` = "true")로, 기본 OFF.
의존 방향: ai_config / repositories / 형제 서비스만. routes 는 import 금지.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import ai_config
import cache
import close_price_client
import market_indicators
import market_movers
from repositories import dart_review as dart_review_repo
from repositories import notifications as notifications_repo
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from repositories import user_settings as user_settings_repo
from repositories import wiki as wiki_repo
from services import ai_client
from services.notifications import channels
from services.portfolio import ai_analysis
from services.portfolio import time_windows

logger = logging.getLogger(__name__)

FEATURE = "daily_briefing"
OPT_IN_KEY = "daily_briefing_enabled"
CUSTOM_INSTRUCTIONS_KEY = "daily_briefing_custom_instructions"
DEFAULT_BRIEFING_TYPE = "morning"

MAX_TOKENS = 1200  # 10~15줄 요약이면 충분 — 폭주 방지 상한.
MOVER_COUNT = 3
CALENDAR_EVENT_LIMIT = 6
FEED_ITEM_LIMIT = 5
MIN_USABLE_AI_LINES = 3
MAX_CUSTOM_INSTRUCTIONS_CHARS = 1200
OVERSEAS_GROUP_LIMIT = 3
DOMESTIC_INDEX_LABELS = {"KOSPI": "코스피", "KOSDAQ": "코스닥"}
FLOW_MARKET_LABELS = {"kospi": "코스피", "kosdaq": "코스닥"}
FLOW_INVESTOR_LABELS = {"individual": "개인", "foreign": "외국인", "institution": "기관"}
TODAY_PORTFOLIO_BRIEFING_TYPES = {"market_close", "night"}

BRIEFING_PROFILES: dict[str, dict[str, str]] = {
    "morning": {
        "name": "모닝 브리핑",
        "title": "🌅 모닝 브리핑",
        "schedule_label": "평일 07:30",
        "description": "개장 전, 전일 결산과 오늘 확인할 이벤트를 정리합니다.",
        "focus": "개장 전 의사결정에 필요한 전일 포트폴리오 변화, 해외/야간 변수, 오늘 일정을 우선합니다.",
        "outline": (
            "전일 포트폴리오 변동 요약(금액·%), 기여 상위/하위 종목, 해외 그룹 성과, "
            "야간선물 일간 변동, 오늘의 일정, 새 공시·리포트, 오늘 주요 경제 일정, "
            "오늘 확인할 포인트 1~2개."
        ),
        "enabled_key": OPT_IN_KEY,
        "instructions_key": CUSTOM_INSTRUCTIONS_KEY,
    },
    "market_close": {
        "name": "클로징 브리핑",
        "title": "🔔 클로징 브리핑",
        "schedule_label": "평일 15:35",
        "description": "정규장 마감 직후, 당일 국내장 흐름과 보유 종목 변동을 정리합니다.",
        "focus": "정규장 마감 직후 코스피·코스닥 지수, 투자자 수급, 오늘 포트폴리오 성과를 우선합니다.",
        "outline": (
            "코스피·코스닥 지수와 수급 동향을 먼저 요약하고, 이어서 오늘 포트폴리오 성과, "
            "새 공시·리포트, 마감 후 확인할 포인트 1~2개."
        ),
        "enabled_key": "daily_briefing_market_close_enabled",
        "instructions_key": "daily_briefing_market_close_custom_instructions",
    },
    "night": {
        "name": "나이트 브리핑",
        "title": "🌙 나이트 브리핑",
        "schedule_label": "평일 20:40",
        "description": "정산 직후, 장후 변화·하루 결산·내일 포인트를 정리합니다.",
        "focus": "장 마감 이후 변경 내용, 오늘 포트폴리오 성과, 내일 시장 전망 재료를 우선합니다.",
        "outline": (
            "장 마감 이후 새 공시·리포트와 해외/야간 변수 변화를 먼저 정리하고, "
            "오늘 포트폴리오 성과, 야간선물·환율·미국장 등 내일 시장 전망 재료, "
            "내일 확인할 포인트 1~2개."
        ),
        "enabled_key": "daily_briefing_night_enabled",
        "instructions_key": "daily_briefing_night_custom_instructions",
    },
}
BRIEFING_ORDER = tuple(BRIEFING_PROFILES.keys())

SYSTEM_PROMPT = """당신은 개인 투자자를 위한 포트폴리오 브리핑 작성자입니다.
규칙:
- 제공된 데이터에만 근거하고, 없는 수치를 꾸며내지 마세요.
- 메신저(텔레그램/카카오) plain text 로 전송되므로 마크다운·HTML 없이
  이모지와 줄바꿈만으로 구성하세요.
- 전체 10~15줄, 한 줄은 짧게. 한국어로 작성하세요.
- 단정적 매수/매도 권유 대신 확인할 포인트를 제시하세요."""


# ---------------------------------------------------------------------------
# Opt-in (user_settings K/V — 기본 OFF)
# ---------------------------------------------------------------------------


def normalize_briefing_type(value: object = None) -> str:
    kind = str(value or DEFAULT_BRIEFING_TYPE).strip().lower().replace("-", "_")
    if kind in {"daily", "daily_briefing", "am"}:
        kind = DEFAULT_BRIEFING_TYPE
    if kind in {"close", "closing", "marketclose"}:
        kind = "market_close"
    if kind not in BRIEFING_PROFILES:
        raise ValueError(f"지원하지 않는 브리핑 유형입니다: {value}")
    return kind


def briefing_profile(briefing_type: object = None) -> dict[str, str]:
    kind = normalize_briefing_type(briefing_type)
    return {"kind": kind, **BRIEFING_PROFILES[kind]}


def briefing_profiles() -> list[dict[str, str]]:
    return [briefing_profile(kind) for kind in BRIEFING_ORDER]


def _enabled_key(briefing_type: object = None) -> str:
    return BRIEFING_PROFILES[normalize_briefing_type(briefing_type)]["enabled_key"]


def _instructions_key(briefing_type: object = None) -> str:
    return BRIEFING_PROFILES[normalize_briefing_type(briefing_type)]["instructions_key"]


async def is_enabled(google_sub: str, briefing_type: object = None) -> bool:
    value = await user_settings_repo.get_user_setting(google_sub, _enabled_key(briefing_type))
    return (value or "").strip().lower() == "true"


async def set_enabled(google_sub: str, enabled: bool, briefing_type: object = None) -> None:
    await user_settings_repo.set_user_setting(
        google_sub, _enabled_key(briefing_type), "true" if enabled else "false"
    )


async def opted_in_users(briefing_type: object = None) -> list[str]:
    return await user_settings_repo.get_users_with_setting(_enabled_key(briefing_type), "true")


def normalize_custom_instructions(value: object) -> str:
    """User-supplied briefing guidance, bounded before it enters the LLM prompt."""
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    return text[:MAX_CUSTOM_INSTRUCTIONS_CHARS]


async def get_custom_instructions(google_sub: str, briefing_type: object = None) -> str:
    value = await user_settings_repo.get_user_setting(google_sub, _instructions_key(briefing_type))
    return normalize_custom_instructions(value)


async def set_custom_instructions(google_sub: str, instructions: object, briefing_type: object = None) -> str:
    text = normalize_custom_instructions(instructions)
    await user_settings_repo.set_user_setting(google_sub, _instructions_key(briefing_type), text)
    return text


async def briefing_setting(google_sub: str, briefing_type: object = None) -> dict:
    profile = briefing_profile(briefing_type)
    return {
        "kind": profile["kind"],
        "name": profile["name"],
        "title": profile["title"],
        "schedule_label": profile["schedule_label"],
        "description": profile["description"],
        "enabled": await is_enabled(google_sub, profile["kind"]),
        "custom_instructions": await get_custom_instructions(google_sub, profile["kind"]),
        "max_custom_instructions_chars": MAX_CUSTOM_INSTRUCTIONS_CHARS,
    }


async def briefing_settings(google_sub: str) -> list[dict]:
    return [await briefing_setting(google_sub, kind) for kind in BRIEFING_ORDER]


def _instruction_preferences(custom_instructions: str | None) -> dict[str, bool]:
    """Detect template sections explicitly requested by the user."""
    text = normalize_custom_instructions(custom_instructions)
    lower = text.lower()
    return {
        "has_custom": bool(text),
        "overseas_groups": (
            any(token in text for token in ("해외", "해외 그룹", "해외주식", "미국", "글로벌"))
            or any(token in lower for token in ("foreign", "overseas", "global"))
        ),
        "night_futures": (
            any(token in text for token in ("야간선물", "야간 선물", "나이트선물"))
            or any(token in lower for token in ("night futures", "overnight futures"))
        ),
        "calendar_alerts": (
            "캘린더" in text and any(token in text for token in ("알림", "알람", "켜"))
        )
        or ("calendar" in lower and any(token in lower for token in ("alert", "alarm", "enabled"))),
    }


# ---------------------------------------------------------------------------
# Context assembly (pure data — no LLM)
# ---------------------------------------------------------------------------


def _nav_block(latest: dict | None, prev: dict | None) -> dict | None:
    if not latest or not latest.get("total_value"):
        return None
    block: dict = {
        "date": latest.get("date"),
        "total_value": float(latest["total_value"]),
        "prev_date": None,
        "prev_value": None,
        "change_krw": None,
        "change_pct": None,
    }
    prev_value = float(prev["total_value"]) if prev and prev.get("total_value") else None
    if prev_value and prev_value > 0:
        change = block["total_value"] - prev_value
        block.update(
            prev_date=prev.get("date"),
            prev_value=prev_value,
            change_krw=change,
            change_pct=change / prev_value * 100.0,
        )
    return block


def _safe_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _price_row_date(row: dict) -> str | None:
    text = str(
        row.get("date")
        or row.get("trade_date")
        or row.get("business_date")
        or row.get("stck_bsop_date")
        or ""
    ).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10] or None


def _price_change_from_rows(rows: list[dict], target_date: str) -> dict | None:
    points: list[tuple[str, float]] = []
    for row in rows:
        row_date = _price_row_date(row)
        close = _safe_float(row.get("close") or row.get("close_price") or row.get("stck_clpr"))
        if row_date and row_date <= target_date and close is not None and close > 0:
            points.append((row_date, close))
    points.sort(key=lambda item: item[0])
    if len(points) < 2:
        return None

    current_date, current_close = points[-1]
    prev_date, prev_close = points[-2]
    if current_date != target_date or prev_close <= 0:
        return None

    return {
        "date": current_date,
        "price": current_close,
        "prev_date": prev_date,
        "prev_price": prev_close,
        "change_pct": (current_close / prev_close - 1.0) * 100.0,
    }


async def _daily_price_changes_by_code(codes: list[str], since: str | None, until: str | None) -> dict[str, dict]:
    if not codes or not since or not until:
        return {}
    try:
        rows_by_code = await close_price_client.get_daily_prices_batch(
            codes,
            since=since,
            until=until,
            fields=("close",),
        )
    except Exception as exc:
        logger.warning("briefing daily price change block failed: %s", exc)
        return {}

    changes: dict[str, dict] = {}
    for code in codes:
        rows = rows_by_code.get(code) or rows_by_code.get(code.upper()) or []
        change = _price_change_from_rows(rows, until)
        if change is not None:
            changes[code] = change
    return changes


def _movers(
    curr_rows: list[dict],
    prev_rows: list[dict],
    names: dict[str, str],
    price_changes: dict[str, dict] | None = None,
    *,
    allow_value_fallback: bool = False,
) -> dict:
    """종목별 가격 변동률로 가격 기여 상위/하위를 계산한다.

    평가액 증감률은 수량 변경의 영향을 받으므로 퍼센트와 기여액 모두 종가의
    일간 변동률을 기준으로 잡는다. 두 날짜 모두 존재하고 가격을 확인할 수
    있는 종목만 우선 비교하며, 현금성 코드는 제외한다. 일별 종가 API가 비거나
    특정 코드만 빠진 날에는 브리핑이 NAV 한 줄로 축소되지 않도록 평가액 변화
    기준 폴백을 쓴다. 이 폴백은 종목별 설명에 "평가액"으로 표시한다.
    """
    prev_by_code = {r["stock_code"]: float(r.get("market_value") or 0) for r in prev_rows}
    price_changes = price_changes or {}
    deltas: list[dict] = []
    for row in curr_rows:
        code = row["stock_code"]
        if code.startswith("CASH_"):
            continue
        prev_mv = prev_by_code.get(code)
        if prev_mv is None or prev_mv <= 0:
            continue
        price_change = price_changes.get(code)
        if price_change:
            change_pct = price_change["change_pct"]
            change = prev_mv * change_pct / 100.0
            basis = "price"
            extra = {
                "price": price_change["price"],
                "prev_price": price_change["prev_price"],
            }
        elif allow_value_fallback:
            mv = float(row.get("market_value") or 0)
            change = mv - prev_mv
            if change == 0:
                continue
            change_pct = change / prev_mv * 100.0
            basis = "market_value"
            extra = {}
        else:
            continue
        deltas.append(
            {
                "stock_code": code,
                "stock_name": names.get(code, code),
                "change_krw": change,
                "change_pct": change_pct,
                "basis": basis,
                **extra,
            }
        )
    deltas.sort(key=lambda d: d["change_krw"], reverse=True)
    top = [d for d in deltas[:MOVER_COUNT] if d["change_krw"] > 0]
    bottom = [d for d in reversed(deltas[-MOVER_COUNT:]) if d["change_krw"] < 0]
    return {"top": top, "bottom": bottom}


def _is_overseas_group_name(group_name: str | None) -> bool:
    text = str(group_name or "").strip()
    lower = text.lower()
    return (
        any(token in text for token in ("해외", "미국", "글로벌"))
        or any(token in lower for token in ("foreign", "overseas", "global"))
        or lower in {"us", "usa", "international", "intl"}
    )


def _overseas_group_performance(rows: list[dict]) -> list[dict]:
    candidates = [r for r in rows if _is_overseas_group_name(r.get("group_name")) and r.get("date")]
    if not candidates:
        return []
    latest_date = max(str(r["date"]) for r in candidates)
    latest_rows = [r for r in candidates if str(r.get("date")) == latest_date]
    by_group: dict[str, list[dict]] = {}
    for row in candidates:
        by_group.setdefault(str(row.get("group_name") or "해외"), []).append(row)

    out: list[dict] = []
    for row in latest_rows:
        name = str(row.get("group_name") or "해외")
        value = _safe_float(row.get("market_value"))
        history = sorted(by_group.get(name) or [], key=lambda r: str(r.get("date") or ""))
        prev = None
        for hist in reversed(history):
            if str(hist.get("date") or "") < latest_date:
                prev = hist
                break
        prev_value = _safe_float((prev or {}).get("market_value"))
        change_krw = None
        change_pct = None
        if value is not None and prev_value is not None and prev_value > 0:
            change_krw = value - prev_value
            change_pct = change_krw / prev_value * 100.0
        out.append(
            {
                "group_name": name,
                "date": latest_date,
                "market_value": value,
                "prev_date": (prev or {}).get("date"),
                "prev_value": prev_value,
                "change_krw": change_krw,
                "change_pct": change_pct,
                "weight_pct": _safe_float(row.get("weight_pct")),
                "stock_count": int(row.get("stock_count") or 0),
            }
        )
    out.sort(key=lambda item: abs(item.get("change_krw") or 0), reverse=True)
    return out[:OVERSEAS_GROUP_LIMIT]


async def _fetch_overseas_groups(google_sub: str) -> list[dict]:
    rows = await snapshots_repo.get_group_weight_history(google_sub)
    return _overseas_group_performance(rows)


def _indicator_change_text(item: dict) -> str:
    raw = str(item.get("change_pct") or item.get("change") or "").strip()
    if not raw:
        return ""
    raw = raw.removeprefix("up").removeprefix("down").strip()
    direction = str(item.get("direction") or "").strip().lower()
    if direction == "up":
        return f"▲{raw.lstrip('+-')}"
    if direction == "down":
        return f"▼{raw.lstrip('+-')}"
    return raw


def _format_domestic_index(code: str, item: dict | None) -> str | None:
    if not item or not item.get("value"):
        return None
    change = _indicator_change_text(item)
    suffix = f" ({change})" if change else ""
    return f"{DOMESTIC_INDEX_LABELS.get(code, code)} {item.get('value')}{suffix}"


async def _fetch_domestic_market_block() -> list[str]:
    data = await market_indicators.fetch_indicators(["KOSPI", "KOSDAQ"])
    rows: list[str] = []
    for code in ("KOSPI", "KOSDAQ"):
        line = _format_domestic_index(code, data.get(code) or {})
        if line:
            rows.append(line)
    return rows


def _format_flow_value(value: str | None, direction: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    if direction == "up" and not text.startswith(("+", "-")):
        text = f"+{text}"
    return f"{text}억"


def _format_market_flow(market: str, row: dict | None) -> str | None:
    if not row:
        return None
    parts: list[str] = []
    for key, label in FLOW_INVESTOR_LABELS.items():
        item = row.get(key) or {}
        parts.append(f"{label} {_format_flow_value(item.get('value'), item.get('direction'))}")
    date_text = f"({row.get('date')})" if row.get("date") else ""
    return f"{FLOW_MARKET_LABELS.get(market, market)}{date_text} " + " / ".join(parts)


async def _fetch_market_flow_block() -> list[str]:
    flows = await market_movers.fetch_investor_flows()
    rows: list[str] = []
    for market in ("kospi", "kosdaq"):
        line = _format_market_flow(market, flows.get(market))
        if line:
            rows.append(line)
    return rows


async def _net_cashflow_since_settlement(google_sub: str, snap_date: str | None) -> float:
    if not snap_date:
        return 0.0
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT type, amount FROM portfolio_cashflows"
        " WHERE google_sub = ? AND created_at > ?",
        (google_sub, time_windows.settlement_marker_seconds(snap_date)),
    )
    net = 0.0
    for row in await cursor.fetchall():
        amount = _safe_float(row["amount"]) or 0.0
        if row["type"] == "deposit":
            net += amount
        elif row["type"] == "withdrawal":
            net -= amount
    return net


async def _fetch_today_portfolio_block(google_sub: str, today_iso: str) -> dict | None:
    """Today-card style portfolio performance for closing/night briefings.

    Before the 20:00 settlement exists, value the current portfolio with live
    quotes. After the settlement exists, use that snapshot. In both cases,
    compare against the previous settlement and remove same-day cashflows from
    the performance delta.
    """
    prev = await snapshots_repo.get_latest_snapshot_before_date(google_sub, today_iso)
    if not prev or not prev.get("total_value"):
        return None

    today_snapshot = await snapshots_repo.get_snapshot_by_date(google_sub, today_iso)
    source = "settlement" if today_snapshot and today_snapshot.get("total_value") else "live"
    if source == "settlement":
        total_value = _safe_float(today_snapshot.get("total_value"))
    else:
        import snapshot_intraday

        total_value = await snapshot_intraday._fetch_total_value(google_sub, today_iso)

    prev_value = _safe_float(prev.get("total_value"))
    if total_value is None or total_value <= 0 or prev_value is None or prev_value <= 0:
        return None

    net_cashflow = await _net_cashflow_since_settlement(google_sub, prev.get("date"))
    investment_change = total_value - net_cashflow - prev_value
    return {
        "date": today_iso,
        "prev_date": prev.get("date"),
        "total_value": total_value,
        "prev_value": prev_value,
        "change_krw": investment_change,
        "change_pct": investment_change / prev_value * 100.0,
        "net_cashflow": net_cashflow,
        "source": source,
    }


def _signed_indicator_value(value: str | None, direction: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith(("+", "-")):
        return text
    if direction == "up":
        return f"+{text}"
    if direction == "down":
        return f"-{text}"
    return text


async def _fetch_night_futures_block() -> dict | None:
    data = (await market_indicators.fetch_indicators(["NIGHT_FUTURES"])).get("NIGHT_FUTURES") or {}
    if not data.get("value"):
        return None
    direction = data.get("direction") or ""
    return {
        "value": data.get("value") or "",
        "change": _signed_indicator_value(data.get("change"), direction),
        "change_pct": _signed_indicator_value(data.get("change_pct"), direction),
        "direction": direction,
        "_stale": bool(data.get("_stale")),
    }


async def _today_calendar_alerts(google_sub: str, today_iso: str) -> list[dict]:
    subs = await notifications_repo.list_calendar_subscriptions(google_sub, pending_only=True)
    return [
        {
            "time": s.get("event_datetime") or "",
            "country_name": s.get("country_name") or s.get("country") or "",
            "event": s.get("event") or "",
            "importance": s.get("importance") or "",
            "forecast": s.get("forecast") or "",
            "previous": s.get("previous") or "",
        }
        for s in subs
        if str(s.get("event_date") or "") == today_iso
    ][:CALENDAR_EVENT_LIMIT]


async def build_briefing_context(google_sub: str, briefing_type: object = None) -> dict:
    """브리핑 입력 데이터를 모아 dict 로 돌려준다 — LLM 없이 테스트 가능.

    각 소스는 독립적으로 best-effort: 하나가 비거나 실패해도 나머지로 브리핑을
    만든다 (모두 기존 저장 데이터 — 새 외부 수집 없음).
    """
    profile = briefing_profile(briefing_type)
    today = date.today()
    context: dict = {
        "google_sub": google_sub,
        "date": today.isoformat(),
        "briefing_type": profile["kind"],
        "briefing_name": profile["name"],
        "briefing_title": profile["title"],
        "briefing_schedule": profile["schedule_label"],
        "briefing_focus": profile["focus"],
        "briefing_outline": profile["outline"],
        "nav": None,
        "portfolio_today": None,
        "movers": {"top": [], "bottom": []},
        "overseas_groups": [],
        "filings": [],
        "reports": [],
        "calendar": [],
        "tomorrow_calendar": [],
        "calendar_alerts": [],
        "night_futures": None,
        "domestic_market": [],
        "market_flows": [],
        "market": [],
    }

    # --- 어제 NAV 변화 + 기여 종목 (결산 스냅샷 기반) ---
    # 클로징 브리핑은 당일 장 마감 직후 메시지라 전일/최근 결산 스냅샷 기반
    # 데이터가 섞이지 않도록 이 블록을 수집하지 않는다.
    if profile["kind"] != "market_close":
        try:
            latest = await snapshots_repo.get_latest_snapshot(google_sub)
            if latest:
                prev = await snapshots_repo.get_latest_snapshot_before_date(google_sub, latest["date"])
                context["nav"] = _nav_block(latest, prev)
                curr_rows = await snapshots_repo.get_stock_snapshots_by_date(google_sub, latest["date"])
                prev_rows = await snapshots_repo.get_stock_snapshots_before_date(google_sub, latest["date"])
                names = {
                    r["stock_code"]: r.get("stock_name") or r["stock_code"]
                    for r in await snapshots_repo.get_latest_stock_snapshot_rows(google_sub)
                }
                prev_codes = {r["stock_code"] for r in prev_rows}
                mover_codes = sorted(
                    row["stock_code"]
                    for row in curr_rows
                    if row["stock_code"] in prev_codes and not row["stock_code"].startswith("CASH_")
                )
                price_changes = await _daily_price_changes_by_code(
                    mover_codes,
                    prev.get("date") if prev else None,
                    latest["date"],
                )
                context["movers"] = _movers(
                    curr_rows,
                    prev_rows,
                    names,
                    price_changes,
                    allow_value_fallback=True,
                )
                movers = (context["movers"].get("top") or []) + (context["movers"].get("bottom") or [])
                context["diagnostics"] = {
                    "mover_candidates": len(mover_codes),
                    "price_change_codes": len(price_changes),
                    "mover_value_fallbacks": sum(1 for m in movers if m.get("basis") == "market_value"),
                }
        except Exception as exc:
            logger.warning("briefing NAV block failed user=%s: %s", google_sub[:8], exc)

    # --- 오늘 포트폴리오 성과 (클로징/나이트: 현재 평가액 vs 전일 결산, 입출금 제외) ---
    if profile["kind"] in TODAY_PORTFOLIO_BRIEFING_TYPES:
        try:
            context["portfolio_today"] = await _fetch_today_portfolio_block(google_sub, today.isoformat())
        except Exception as exc:
            logger.warning("briefing today portfolio block failed user=%s: %s", google_sub[:8], exc)

    # --- 해외 그룹 성과 (그룹 스냅샷 기반) ---
    if profile["kind"] != "market_close":
        try:
            context["overseas_groups"] = await _fetch_overseas_groups(google_sub)
        except Exception as exc:
            logger.warning("briefing overseas group block failed user=%s: %s", google_sub[:8], exc)

    # --- 신규 공시 리뷰 / 증권사 리포트 (보유 종목만) ---
    try:
        holdings = await portfolio_repo.get_portfolio(google_sub=google_sub)
        codes = [it["stock_code"] for it in holdings if not it["stock_code"].startswith("CASH_")]
        holding_names = {
            it["stock_code"]: it.get("stock_name") or it["stock_code"]
            for it in holdings
        }
        since = (
            f"{today.isoformat()}T15:30:00"
            if profile["kind"] == "night"
            else (today - timedelta(days=1)).isoformat()
        )
        context["filings"] = await dart_review_repo.list_recent_reviews(
            codes, since, limit=FEED_ITEM_LIMIT
        )
        reports = await wiki_repo.list_recent_entries(
            codes, since, limit=FEED_ITEM_LIMIT
        )
        context["reports"] = [
            {**report, "stock_name": holding_names.get(report.get("stock_code"), report.get("stock_code"))}
            for report in reports
        ]
    except Exception as exc:
        logger.warning("briefing feed block failed user=%s: %s", google_sub[:8], exc)

    # --- 오늘 주요 경제 일정 (캐시되는 기존 캘린더 fetch — 중요도 '상'만) ---
    try:
        import economic_calendar

        data = await economic_calendar.fetch_economic_calendar(
            start_date=today.isoformat(),
            end_date=today.isoformat(),
            importance=["high"],
        )
        context["calendar"] = [
            {
                "time": e.get("time") or "",
                "country_name": e.get("country_name") or "",
                "flag": e.get("flag") or "",
                "event": e.get("event") or "",
            }
            for e in (data.get("events") or [])[:CALENDAR_EVENT_LIMIT]
        ]
    except Exception as exc:
        logger.warning("briefing calendar block failed: %s", exc)

    # --- 내일 주요 일정 (나이트 브리핑 전망 재료) ---
    if profile["kind"] == "night":
        try:
            import economic_calendar

            tomorrow = today + timedelta(days=1)
            data = await economic_calendar.fetch_economic_calendar(
                start_date=tomorrow.isoformat(),
                end_date=tomorrow.isoformat(),
                importance=["high"],
            )
            context["tomorrow_calendar"] = [
                {
                    "time": e.get("time") or "",
                    "country_name": e.get("country_name") or "",
                    "flag": e.get("flag") or "",
                    "event": e.get("event") or "",
                }
                for e in (data.get("events") or [])[:CALENDAR_EVENT_LIMIT]
            ]
        except Exception as exc:
            logger.warning("briefing tomorrow calendar block failed: %s", exc)

    # --- 오늘 알림이 켜져 있는 캘린더 이벤트 ---
    try:
        context["calendar_alerts"] = await _today_calendar_alerts(google_sub, today.isoformat())
    except Exception as exc:
        logger.warning("briefing calendar alert block failed user=%s: %s", google_sub[:8], exc)

    # --- 야간선물 일간 상승률/하락률 ---
    if profile["kind"] != "market_close":
        try:
            context["night_futures"] = await _fetch_night_futures_block()
        except Exception as exc:
            logger.warning("briefing night futures block failed: %s", exc)

    # --- 국내 지수 + 투자자 수급 (클로징/나이트 브리핑 핵심 입력) ---
    if profile["kind"] in {"market_close", "night"}:
        try:
            context["domestic_market"] = await _fetch_domestic_market_block()
        except Exception as exc:
            logger.warning("briefing domestic market block failed: %s", exc)
        try:
            context["market_flows"] = await _fetch_market_flow_block()
        except Exception as exc:
            logger.warning("briefing market flow block failed: %s", exc)

    # --- 시장 지표 (ai_analysis 의 헬퍼 재사용 — 실패 시 안내 한 줄) ---
    context["market"] = await ai_analysis.market_summary_lines()
    return context


# ---------------------------------------------------------------------------
# Rendering (template fallback + LLM prompt share the same line builders)
# ---------------------------------------------------------------------------


def _fmt_money(value: float | None) -> str:
    if value is None:
        return "-"
    text = ai_analysis.fmt_krw(value)
    return text[:-1] if text.endswith("원") else text


def _fmt_signed_krw(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}{_fmt_money(abs(value))}"


def _fmt_mover(mover: dict) -> str:
    pct = mover.get("change_pct")
    label = "가격" if mover.get("basis") == "price" else "평가액"
    pct_text = f"({label} {pct:+.1f}%)" if pct is not None else ""
    return f"{mover['stock_name']} {_fmt_signed_krw(mover['change_krw'])}{pct_text}"


def _fmt_overseas_group(group: dict) -> str:
    value = group.get("market_value")
    value_text = _fmt_money(value) if value is not None else "-"
    change = group.get("change_krw")
    pct = group.get("change_pct")
    weight = group.get("weight_pct")
    parts = [f"{group.get('group_name') or '해외'} {value_text}"]
    if change is not None:
        pct_text = f", {pct:+.2f}%" if pct is not None else ""
        parts.append(f"{_fmt_signed_krw(change)}{pct_text}")
    if weight is not None:
        parts.append(f"비중 {weight:.1f}%")
    return " (".join([parts[0], " · ".join(parts[1:]) + ")"]) if len(parts) > 1 else parts[0]


def _fmt_calendar_alert(item: dict) -> str:
    event = item.get("event") or "일정"
    head = " ".join(p for p in (item.get("time"), item.get("country_name")) if p)
    details = " / ".join(
        f"{label} {value}"
        for label, value in (("예상", item.get("forecast")), ("이전", item.get("previous")))
        if value
    )
    text = f"{head} {event}".strip()
    return f"{text} ({details})" if details else text


def _fmt_nav_line(nav: dict, label: str) -> str:
    prefix = f"📊 {label}({nav['date']}) 총평가 {_fmt_money(nav['total_value'])}"
    if nav.get("change_krw") is None:
        return prefix
    return f"{prefix} ({_fmt_signed_krw(nav['change_krw'])}, {nav['change_pct']:+.2f}%)"


def _fmt_today_portfolio_line(today: dict) -> str:
    line = _fmt_nav_line(today, "오늘")
    if today.get("net_cashflow"):
        line += " · 입출금 제외"
    if today.get("source") == "live":
        line += " · 실시간"
    return line


def _feed_lines(context: dict, *, night_prefix: bool = False) -> list[str]:
    lines: list[str] = []
    for f in context.get("filings") or []:
        name = f.get("corp_name") or f.get("stock_code")
        prefix = "🕘 장 마감 이후 공시 리뷰" if night_prefix else "📑 새 공시 리뷰"
        lines.append(f"{prefix}: [{name}] {f.get('report_name') or ''}".rstrip())
    for r in context.get("reports") or []:
        name = r.get("stock_name") or r.get("stock_code")
        head = " · ".join(p for p in (r.get("firm"), r.get("title")) if p)
        prefix = "🕘 장 마감 이후 리포트" if night_prefix else "📈 새 리포트"
        lines.append(f"{prefix}: [{name}] {head}".rstrip())
    return lines


def _calendar_line(label: str, events: list[dict]) -> str | None:
    if not events:
        return None
    parts = ", ".join(
        " ".join(p for p in (e.get("time"), e.get("flag"), e.get("event")) if p)
        for e in events
    )
    return f"📅 {label}: {parts}"


def _night_futures_line(night: dict | None) -> str | None:
    if not night:
        return None
    move = " ".join(p for p in (night.get("change"), night.get("change_pct")) if p)
    stale = " · 이전값" if night.get("_stale") else ""
    if move:
        return f"🌙 야간선물: {night.get('value')} ({move}{stale})"
    return f"🌙 야간선물: {night.get('value')}{stale}"


def _market_context_lines(context: dict) -> list[str]:
    market = context.get("market") or []
    if not market:
        return []
    return ["🌐 시장 지표:", *market]


def _requested_missing_context_lines(context: dict, custom_instructions: str | None) -> list[str]:
    requested = _instruction_preferences(custom_instructions)
    kind = context.get("briefing_type") or DEFAULT_BRIEFING_TYPE
    lines: list[str] = []
    if kind != "market_close" and requested["overseas_groups"] and not context.get("overseas_groups"):
        lines.append("🌏 해외 그룹 성과: 최근 그룹 스냅샷에서 해외 그룹 데이터를 찾지 못했습니다.")
    if requested["calendar_alerts"] and not context.get("calendar_alerts"):
        lines.append("📅 오늘의 일정: 오늘 예정된 알림 설정 이벤트가 없습니다.")
    if kind != "market_close" and requested["night_futures"] and not context.get("night_futures"):
        lines.append("🌙 야간선물: 현재 조회 가능한 값이 없습니다.")
    return lines


def _context_lines(context: dict, custom_instructions: str | None = None) -> list[str]:
    """컨텍스트를 사람이 읽을 수 있는 줄들로 변환 — 템플릿 폴백 본문이자
    LLM 프롬프트의 데이터 섹션."""
    lines: list[str] = []
    kind = context.get("briefing_type") or DEFAULT_BRIEFING_TYPE

    domestic_market = context.get("domestic_market") or []
    if kind == "market_close" and domestic_market:
        lines.append(f"🇰🇷 국내 지수: {', '.join(domestic_market)}")

    market_flows = context.get("market_flows") or []
    if kind == "market_close" and market_flows:
        lines.append(f"💰 수급 동향: {', '.join(market_flows)}")

    if kind == "night":
        feed = _feed_lines(context, night_prefix=True)
        lines.extend(feed if feed else ["🕘 장 마감 이후 변경: 새 공시·리포트 없음"])

    portfolio_today = context.get("portfolio_today")
    if kind in TODAY_PORTFOLIO_BRIEFING_TYPES and portfolio_today:
        lines.append(_fmt_today_portfolio_line(portfolio_today))

    nav = context.get("nav")
    if nav and kind not in TODAY_PORTFOLIO_BRIEFING_TYPES:
        lines.append(_fmt_nav_line(nav, "어제"))
    elif nav and kind == "night" and not portfolio_today:
        lines.append(_fmt_nav_line(nav, "최근 결산"))

    movers = context.get("movers") or {}
    if kind != "market_close" and movers.get("top"):
        parts = ", ".join(_fmt_mover(m) for m in movers["top"])
        lines.append(f"📈 상승 기여: {parts}")
    if kind != "market_close" and movers.get("bottom"):
        parts = ", ".join(_fmt_mover(m) for m in movers["bottom"])
        lines.append(f"📉 하락 기여: {parts}")

    overseas_groups = context.get("overseas_groups") or []
    if kind != "market_close" and overseas_groups:
        parts = ", ".join(_fmt_overseas_group(g) for g in overseas_groups)
        lines.append(f"🌏 해외 그룹 성과: {parts}")

    if kind != "night":
        lines.extend(_feed_lines(context))

    cal_line = _calendar_line("오늘 주요 일정", context.get("calendar") or [])
    if cal_line:
        lines.append(cal_line)

    alerts = context.get("calendar_alerts") or []
    if alerts:
        parts = ", ".join(_fmt_calendar_alert(item) for item in alerts)
        lines.append(f"📅 오늘의 일정: {parts}")

    night_line = _night_futures_line(context.get("night_futures")) if kind != "market_close" else None
    if night_line:
        lines.append(night_line)

    tomorrow_line = _calendar_line("내일 주요 일정", context.get("tomorrow_calendar") or [])
    if tomorrow_line:
        lines.append(tomorrow_line)

    if kind == "night":
        lines.append("🔎 내일 시장 전망: 야간선물·환율·미국장 흐름과 내일 주요 일정을 함께 점검하세요.")
        if domestic_market:
            lines.append(f"🇰🇷 국내 지수: {', '.join(domestic_market)}")
        if market_flows:
            lines.append(f"💰 수급 동향: {', '.join(market_flows)}")

    lines.extend(_market_context_lines(context))
    return lines + _requested_missing_context_lines(context, custom_instructions)


def _nonempty_lines(text: str) -> list[str]:
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def _template_body_lines(context: dict, custom_instructions: str | None = None) -> list[str]:
    body = _context_lines(context, custom_instructions)
    if not body:
        body = ["오늘은 요약할 새 데이터가 없습니다."]
    movers = context.get("movers") or {}
    kind = context.get("briefing_type") or DEFAULT_BRIEFING_TYPE
    if kind != "market_close" and context.get("nav") and not movers.get("top") and not movers.get("bottom"):
        body.append("기여 종목: 종목별 종가 데이터가 비어 있어 세부 기여는 생략했습니다.")
    if len(body) < MIN_USABLE_AI_LINES:
        body.append("오늘 확인: 큰 변동의 원인을 종목별 가격·환율·현금흐름으로 나눠 점검하세요.")
    return body


_INCOMPLETE_LINE_SUFFIXES = ("(", "[", "{", "+", "-", "/", "·", ",", ":", "(+", "(-")


def _ai_text_rejection_reason(text: str, finish_reason: str | None = None) -> str | None:
    if finish_reason == "length":
        return "finish_reason_length"
    lines = _nonempty_lines(text)
    if len(lines) < MIN_USABLE_AI_LINES:
        return "too_short"
    if not any("브리핑" in line for line in lines[:2]):
        return "missing_title"
    last = lines[-1].strip()
    if last.endswith(_INCOMPLETE_LINE_SUFFIXES):
        return "incomplete_tail"
    if text.count("(") > text.count(")") or text.count("[") > text.count("]"):
        return "unbalanced_delimiter"
    return None


def _ai_text_is_usable(text: str, finish_reason: str | None = None) -> bool:
    return _ai_text_rejection_reason(text, finish_reason) is None


def _briefing_stats(context: dict, text: str) -> dict:
    movers = context.get("movers") or {}
    mover_rows = (movers.get("top") or []) + (movers.get("bottom") or [])
    return {
        "briefing_type": context.get("briefing_type") or DEFAULT_BRIEFING_TYPE,
        "text_lines": len(_nonempty_lines(text)),
        "context_lines": len(_context_lines(context)),
        "has_nav": bool(context.get("nav")),
        "mover_top": len(movers.get("top") or []),
        "mover_bottom": len(movers.get("bottom") or []),
        "mover_value_fallbacks": sum(1 for m in mover_rows if m.get("basis") == "market_value"),
        "overseas_groups": len(context.get("overseas_groups") or []),
        "has_night_futures": bool(context.get("night_futures")),
        "filings": len(context.get("filings") or []),
        "reports": len(context.get("reports") or []),
        "calendar": len(context.get("calendar") or []),
        "calendar_alerts": len(context.get("calendar_alerts") or []),
        "market": len(context.get("market") or []),
        **(context.get("diagnostics") or {}),
    }


def render_template_briefing(context: dict, custom_instructions: str | None = None) -> str:
    """LLM 없이 컨텍스트만으로 만든 브리핑 — OpenRouter 장애 시에도 발송된다."""
    body = _template_body_lines(context, custom_instructions)
    title = context.get("briefing_title") or BRIEFING_PROFILES[DEFAULT_BRIEFING_TYPE]["title"]
    return "\n".join([f"{title} ({context.get('date')})", *body])


def build_prompt(context: dict, custom_instructions: str | None = None) -> str:
    data = "\n".join(_context_lines(context, custom_instructions)) or "(데이터 없음)"
    custom = normalize_custom_instructions(custom_instructions)
    title = context.get("briefing_title") or BRIEFING_PROFILES[DEFAULT_BRIEFING_TYPE]["title"]
    name = context.get("briefing_name") or BRIEFING_PROFILES[DEFAULT_BRIEFING_TYPE]["name"]
    focus = context.get("briefing_focus") or BRIEFING_PROFILES[DEFAULT_BRIEFING_TYPE]["focus"]
    outline = context.get("briefing_outline") or BRIEFING_PROFILES[DEFAULT_BRIEFING_TYPE]["outline"]
    custom_block = ""
    if custom:
        custom_block = f"""
사용자 추가 지시:
{custom}

위 추가 지시는 제공 데이터와 시스템 규칙을 벗어나지 않는 범위에서 반영하세요.
"""
    return f"""오늘 날짜: {context.get('date')}

아래는 한 투자자의 포트폴리오 데이터입니다.

{data}
{custom_block}

위 데이터로 {name}을 작성하세요.
- 첫 줄: "{title} ({context.get('date')})"
- 브리핑 성격: {focus}
- 구성 순서: {outline}
- 금액은 숫자만 표시하고 통화 단위 '원'은 붙이지 마세요.
- 사용자 추가 지시가 요구한 섹션은 데이터가 없더라도 '대상 없음' 또는 '현재 조회 불가'로 짧게 표시하세요.
- 그 외 데이터가 없는 섹션은 건너뛰세요. 전체 10~15줄."""


# ---------------------------------------------------------------------------
# Generation (LLM → template fallback)
# ---------------------------------------------------------------------------


async def generate_briefing(google_sub: str, briefing_type: object = None) -> dict:
    """한 사용자의 브리핑 텍스트 생성.

    LLM 경로는 ai_client.post_chat_completion 을 쓰므로 usage ledger
    (feature='daily_briefing') 기록은 성공/실패 모두 자동이다. 어떤 이유로든
    LLM 텍스트를 못 얻으면 템플릿 렌더로 폴백 — 발송 자체는 항상 가능하다.
    """
    profile = briefing_profile(briefing_type)
    context = await build_briefing_context(google_sub, profile["kind"])
    custom_instructions = await get_custom_instructions(google_sub, profile["kind"])
    fallback_text = render_template_briefing(context, custom_instructions)
    try:
        model = await ai_config.get_model_for_feature(FEATURE)
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_prompt(context, custom_instructions)},
            ],
            "max_tokens": MAX_TOKENS,
            **ai_config.openrouter_reasoning_controls(model, effort="low"),
        }
        result = await ai_client.post_chat_completion(
            feature=FEATURE,
            payload=payload,
            google_sub=google_sub,
            model=model,
            model_profile=FEATURE,
            ok_if_content=True,
        )
        text = (result.get("content") or "").strip()
        if text:
            rejection_reason = _ai_text_rejection_reason(text, result.get("finish_reason"))
            if rejection_reason is None:
                return {
                    "text": text,
                    "source": "ai",
                    "model": result.get("model") or model,
                    "context": context,
                    "stats": _briefing_stats(context, text),
                    "briefing_type": profile["kind"],
                    "briefing_name": profile["name"],
                    "custom_instructions": bool(custom_instructions),
                }
            logger.warning(
                "daily briefing LLM unusable user=%s reason=%s lines=%s finish_reason=%s",
                google_sub[:8],
                rejection_reason,
                len(_nonempty_lines(text)),
                result.get("finish_reason"),
            )
            return {
                "text": fallback_text,
                "source": "template",
                "model": None,
                "fallback_reason": f"ai_{rejection_reason}",
                "context": context,
                "stats": _briefing_stats(context, fallback_text),
                "briefing_type": profile["kind"],
                "briefing_name": profile["name"],
                "custom_instructions": bool(custom_instructions),
            }
        logger.warning("daily briefing LLM returned empty content user=%s", google_sub[:8])
    except Exception as exc:
        logger.warning("daily briefing LLM failed user=%s: %s", google_sub[:8], exc)
    return {
        "text": fallback_text,
        "source": "template",
        "model": None,
        "fallback_reason": "ai_failed_or_empty",
        "context": context,
        "stats": _briefing_stats(context, fallback_text),
        "briefing_type": profile["kind"],
        "briefing_name": profile["name"],
        "custom_instructions": bool(custom_instructions),
    }


async def generate_test_message(google_sub: str, briefing_type: object = None) -> dict:
    """Build the exact one-off message used by the UI's test-send button."""
    profile = briefing_profile(briefing_type)
    briefing = await generate_briefing(google_sub, profile["kind"])
    text = f"🧪 {profile['name']} 테스트 발송\n" + briefing["text"]
    return {**briefing, "text": text}


# ---------------------------------------------------------------------------
# Batch send
# ---------------------------------------------------------------------------


async def send_briefings(briefing_type: object = None) -> dict:
    """옵트인 사용자 전체에 브리핑 생성·발송. 사용자별 결과는 system_events
    (source='daily_briefing')에 남고, 한 사용자의 실패가 다음 사용자를 막지
    않는다."""
    import observability

    profile = briefing_profile(briefing_type)
    users = await opted_in_users(profile["kind"])
    sent = failed = skipped = 0
    for google_sub in users:
        try:
            if not await channels.has_active_channel(google_sub):
                skipped += 1
                await observability.record_event(
                    "daily_briefing",
                    "send_skipped",
                    details={
                        "user": google_sub[:8],
                        "briefing_type": profile["kind"],
                        "briefing_name": profile["name"],
                        "reason": "no_active_channel",
                    },
                    wait=True,
                )
                continue
            briefing = await generate_briefing(google_sub, profile["kind"])
            delivered = await channels.dispatch(google_sub, briefing["text"])
            if delivered > 0:
                sent += 1
                stats = briefing.get("stats") or _briefing_stats(briefing.get("context") or {}, briefing["text"])
                await observability.record_event(
                    "daily_briefing",
                    "send_ok",
                    details={
                        "user": google_sub[:8],
                        "briefing_type": profile["kind"],
                        "briefing_name": profile["name"],
                        "source": briefing["source"],
                        "model": briefing.get("model"),
                        "fallback_reason": briefing.get("fallback_reason"),
                        "stats": stats,
                        "channels": delivered,
                    },
                    wait=True,
                )
            else:
                failed += 1
                await observability.record_event(
                    "daily_briefing",
                    "send_fail",
                    level="warning",
                    details={
                        "user": google_sub[:8],
                        "briefing_type": profile["kind"],
                        "briefing_name": profile["name"],
                        "reason": "no_channel_delivered",
                    },
                    wait=True,
                )
        except Exception as exc:
            failed += 1
            logger.warning("daily briefing failed user=%s: %s", google_sub[:8], exc)
            try:
                await observability.record_event(
                    "daily_briefing",
                    "send_fail",
                    level="error",
                    details={
                        "user": google_sub[:8],
                        "briefing_type": profile["kind"],
                        "briefing_name": profile["name"],
                        "error": str(exc)[:300],
                    },
                    wait=True,
                )
            except Exception:
                pass
    return {
        "briefing_type": profile["kind"],
        "briefing_name": profile["name"],
        "users": len(users),
        "sent": sent,
        "failed": failed,
        "skipped": skipped,
    }
