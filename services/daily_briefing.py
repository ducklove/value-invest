"""AI 데일리 브리핑 — 아침 배치로 미리 생성해 메신저로 푸시한다.

철학: '기다리지 않는 화면'. 사용자가 접속해 생성을 기다리는 대신, 평일 아침
(daily-briefing.timer, 08:20 KST) 배치가 어제 결산 데이터로 브리핑을 만들어
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
from repositories import dart_review as dart_review_repo
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from repositories import user_settings as user_settings_repo
from repositories import wiki as wiki_repo
from services import ai_client
from services.notifications import channels
from services.portfolio import ai_analysis

logger = logging.getLogger(__name__)

FEATURE = "daily_briefing"
OPT_IN_KEY = "daily_briefing_enabled"

MAX_TOKENS = 1200  # 10~15줄 요약이면 충분 — 폭주 방지 상한.
MOVER_COUNT = 3
CALENDAR_EVENT_LIMIT = 6
FEED_ITEM_LIMIT = 5

SYSTEM_PROMPT = """당신은 개인 투자자를 위한 아침 브리핑 작성자입니다.
규칙:
- 제공된 데이터에만 근거하고, 없는 수치를 꾸며내지 마세요.
- 메신저(텔레그램/카카오) plain text 로 전송되므로 마크다운·HTML 없이
  이모지와 줄바꿈만으로 구성하세요.
- 전체 10~15줄, 한 줄은 짧게. 한국어로 작성하세요.
- 단정적 매수/매도 권유 대신 확인할 포인트를 제시하세요."""


# ---------------------------------------------------------------------------
# Opt-in (user_settings K/V — 기본 OFF)
# ---------------------------------------------------------------------------


async def is_enabled(google_sub: str) -> bool:
    value = await user_settings_repo.get_user_setting(google_sub, OPT_IN_KEY)
    return (value or "").strip().lower() == "true"


async def set_enabled(google_sub: str, enabled: bool) -> None:
    await user_settings_repo.set_user_setting(
        google_sub, OPT_IN_KEY, "true" if enabled else "false"
    )


async def opted_in_users() -> list[str]:
    return await user_settings_repo.get_users_with_setting(OPT_IN_KEY, "true")


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


def _movers(curr_rows: list[dict], prev_rows: list[dict], names: dict[str, str]) -> dict:
    """종목별 평가액 변화(원) 기준 기여 상위/하위. 두 날짜 모두 존재하는 종목만
    비교한다(신규 매수/전량 매도를 등락으로 오인하지 않도록). 현금성 코드는 제외."""
    prev_by_code = {r["stock_code"]: float(r.get("market_value") or 0) for r in prev_rows}
    deltas: list[dict] = []
    for row in curr_rows:
        code = row["stock_code"]
        if code.startswith("CASH_"):
            continue
        prev_mv = prev_by_code.get(code)
        if prev_mv is None or prev_mv <= 0:
            continue
        mv = float(row.get("market_value") or 0)
        change = mv - prev_mv
        deltas.append(
            {
                "stock_code": code,
                "stock_name": names.get(code, code),
                "change_krw": change,
                "change_pct": change / prev_mv * 100.0,
            }
        )
    deltas.sort(key=lambda d: d["change_krw"], reverse=True)
    top = [d for d in deltas[:MOVER_COUNT] if d["change_krw"] > 0]
    bottom = [d for d in reversed(deltas[-MOVER_COUNT:]) if d["change_krw"] < 0]
    return {"top": top, "bottom": bottom}


async def build_briefing_context(google_sub: str) -> dict:
    """브리핑 입력 데이터를 모아 dict 로 돌려준다 — LLM 없이 테스트 가능.

    각 소스는 독립적으로 best-effort: 하나가 비거나 실패해도 나머지로 브리핑을
    만든다 (모두 기존 저장 데이터 — 새 외부 수집 없음).
    """
    today = date.today()
    context: dict = {
        "google_sub": google_sub,
        "date": today.isoformat(),
        "nav": None,
        "movers": {"top": [], "bottom": []},
        "filings": [],
        "reports": [],
        "calendar": [],
        "market": [],
    }

    # --- 어제 NAV 변화 + 기여 종목 (결산 스냅샷 기반) ---
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
            context["movers"] = _movers(curr_rows, prev_rows, names)
    except Exception as exc:
        logger.warning("briefing NAV block failed user=%s: %s", google_sub[:8], exc)

    # --- 신규 공시 리뷰 / 증권사 리포트 (어제 이후 수집분, 보유 종목만) ---
    try:
        holdings = await portfolio_repo.get_portfolio(google_sub=google_sub)
        codes = [it["stock_code"] for it in holdings if not it["stock_code"].startswith("CASH_")]
        since = (today - timedelta(days=1)).isoformat()
        context["filings"] = await dart_review_repo.list_recent_reviews(
            codes, since, limit=FEED_ITEM_LIMIT
        )
        context["reports"] = await wiki_repo.list_recent_entries(
            codes, since, limit=FEED_ITEM_LIMIT
        )
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

    # --- 시장 지표 (ai_analysis 의 헬퍼 재사용 — 실패 시 안내 한 줄) ---
    context["market"] = await ai_analysis.market_summary_lines()
    return context


# ---------------------------------------------------------------------------
# Rendering (template fallback + LLM prompt share the same line builders)
# ---------------------------------------------------------------------------


def _fmt_signed_krw(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}{ai_analysis.fmt_krw(abs(value))}"


def _context_lines(context: dict) -> list[str]:
    """컨텍스트를 사람이 읽을 수 있는 줄들로 변환 — 템플릿 폴백 본문이자
    LLM 프롬프트의 데이터 섹션."""
    lines: list[str] = []
    nav = context.get("nav")
    if nav:
        if nav.get("change_krw") is not None:
            lines.append(
                f"📊 어제({nav['date']}) 총평가 {ai_analysis.fmt_krw(nav['total_value'])} "
                f"({_fmt_signed_krw(nav['change_krw'])}, {nav['change_pct']:+.2f}%)"
            )
        else:
            lines.append(f"📊 어제({nav['date']}) 총평가 {ai_analysis.fmt_krw(nav['total_value'])}")

    movers = context.get("movers") or {}
    if movers.get("top"):
        parts = ", ".join(
            f"{m['stock_name']} {_fmt_signed_krw(m['change_krw'])}({m['change_pct']:+.1f}%)"
            for m in movers["top"]
        )
        lines.append(f"📈 상승 기여: {parts}")
    if movers.get("bottom"):
        parts = ", ".join(
            f"{m['stock_name']} {_fmt_signed_krw(m['change_krw'])}({m['change_pct']:+.1f}%)"
            for m in movers["bottom"]
        )
        lines.append(f"📉 하락 기여: {parts}")

    for f in context.get("filings") or []:
        name = f.get("corp_name") or f.get("stock_code")
        lines.append(f"📑 새 공시 리뷰: [{name}] {f.get('report_name') or ''}".rstrip())
    for r in context.get("reports") or []:
        head = " · ".join(p for p in (r.get("firm"), r.get("title")) if p)
        lines.append(f"📈 새 리포트: [{r.get('stock_code')}] {head}".rstrip())

    cal = context.get("calendar") or []
    if cal:
        parts = ", ".join(
            " ".join(p for p in (e.get("time"), e.get("flag"), e.get("event")) if p)
            for e in cal
        )
        lines.append(f"📅 오늘 주요 일정: {parts}")

    market = context.get("market") or []
    if market:
        lines.append("🌐 시장 지표:")
        lines.extend(market)
    return lines


def render_template_briefing(context: dict) -> str:
    """LLM 없이 컨텍스트만으로 만든 브리핑 — OpenRouter 장애 시에도 발송된다."""
    body = _context_lines(context)
    if not body:
        body = ["오늘은 요약할 새 데이터가 없습니다."]
    return "\n".join([f"🌅 데일리 브리핑 ({context.get('date')})", *body])


def build_prompt(context: dict) -> str:
    data = "\n".join(_context_lines(context)) or "(데이터 없음)"
    return f"""오늘 날짜: {context.get('date')}

아래는 한 투자자의 포트폴리오 데이터입니다.

{data}

위 데이터로 아침 데일리 브리핑을 작성하세요.
- 첫 줄: "🌅 데일리 브리핑 ({context.get('date')})"
- 이어서: 어제 포트폴리오 변동 요약(원·%), 기여 상위/하위 종목, 새 공시·리포트가
  있으면 한 줄씩, 오늘 주요 경제 일정, 마지막으로 오늘 확인할 포인트 1~2개.
- 데이터가 없는 섹션은 건너뛰세요. 전체 10~15줄."""


# ---------------------------------------------------------------------------
# Generation (LLM → template fallback)
# ---------------------------------------------------------------------------


async def generate_briefing(google_sub: str) -> dict:
    """한 사용자의 브리핑 텍스트 생성.

    LLM 경로는 ai_client.post_chat_completion 을 쓰므로 usage ledger
    (feature='daily_briefing') 기록은 성공/실패 모두 자동이다. 어떤 이유로든
    LLM 텍스트를 못 얻으면 템플릿 렌더로 폴백 — 발송 자체는 항상 가능하다.
    """
    context = await build_briefing_context(google_sub)
    try:
        model = await ai_config.get_model_for_feature(FEATURE)
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_prompt(context)},
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
            return {"text": text, "source": "ai", "model": result.get("model") or model, "context": context}
        logger.warning("daily briefing LLM returned empty content user=%s", google_sub[:8])
    except Exception as exc:
        logger.warning("daily briefing LLM failed user=%s: %s", google_sub[:8], exc)
    return {"text": render_template_briefing(context), "source": "template", "model": None, "context": context}


# ---------------------------------------------------------------------------
# Batch send
# ---------------------------------------------------------------------------


async def send_briefings() -> dict:
    """옵트인 사용자 전체에 브리핑 생성·발송. 사용자별 결과는 system_events
    (source='daily_briefing')에 남고, 한 사용자의 실패가 다음 사용자를 막지
    않는다."""
    import observability

    users = await opted_in_users()
    sent = failed = skipped = 0
    for google_sub in users:
        try:
            if not await channels.has_active_channel(google_sub):
                skipped += 1
                await observability.record_event(
                    "daily_briefing",
                    "send_skipped",
                    details={"user": google_sub[:8], "reason": "no_active_channel"},
                    wait=True,
                )
                continue
            briefing = await generate_briefing(google_sub)
            delivered = await channels.dispatch(google_sub, briefing["text"])
            if delivered > 0:
                sent += 1
                await observability.record_event(
                    "daily_briefing",
                    "send_ok",
                    details={
                        "user": google_sub[:8],
                        "source": briefing["source"],
                        "model": briefing.get("model"),
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
                    details={"user": google_sub[:8], "reason": "no_channel_delivered"},
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
                    details={"user": google_sub[:8], "error": str(exc)[:300]},
                    wait=True,
                )
            except Exception:
                pass
    return {"users": len(users), "sent": sent, "failed": failed, "skipped": skipped}
