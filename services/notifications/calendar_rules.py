"""Validate recurring calendar subscriptions and discover matching releases."""

from datetime import date, datetime

import economic_calendar
from repositories import calendar_rules as rules_repo
from services.notifications.alert_delivery import KST, now_kst


def validate_rules(payload: dict) -> list[dict]:
    items = payload.get("rules")
    if not isinstance(items, list) or len(items) > len(economic_calendar.COUNTRY_META):
        raise ValueError("국가별 알림 조건을 목록으로 입력하세요.")
    rules = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("알림 조건이 올바르지 않습니다.")
        country = item.get("country")
        minimum = item.get("min_importance")
        if not isinstance(country, str) or country not in economic_calendar.COUNTRY_META:
            raise ValueError("지원하는 국가를 선택하세요.")
        if country in seen:
            raise ValueError("한 국가에는 하나의 알림 조건만 설정할 수 있습니다.")
        if minimum not in ("all", "mid", "high"):
            raise ValueError("중요도는 전체, 중 이상, 상 중에서 선택하세요.")
        seen.add(country)
        rules.append({"country": country, "min_importance": minimum})
    return rules


async def settings(google_sub: str) -> dict:
    rules = {row["country"]: row for row in await rules_repo.list_rules(google_sub)}
    return {
        "rules": [{key: rules[country][key] for key in ("country", "min_importance", "starts_at")}
                  for country in economic_calendar.COUNTRY_META if country in rules],
        "countries": [{"code": code, "name": meta["name"], "flag": meta["flag"]}
                      for code, meta in economic_calendar.COUNTRY_META.items()],
    }


def matches(rule: dict, event: dict) -> bool:
    if rule["country"] != event.get("country"):
        return False
    return (rule["min_importance"] == "all" or event.get("importance") == "high"
            or (rule["min_importance"] == "mid" and event.get("importance") == "mid"))


def starts_after_rule(rule: dict, event: dict) -> bool:
    """Do not flood newly subscribed users with previously released results."""
    try:
        since = datetime.fromisoformat(rule["starts_at"])
        if since.tzinfo is None:
            since = since.replace(tzinfo=KST)
        raw = event.get("datetime") or ""
        if raw:
            scheduled = datetime.fromisoformat(raw)
            if scheduled.tzinfo is None:
                scheduled = scheduled.replace(tzinfo=KST)
            return scheduled >= since
        day = date.fromisoformat(event.get("date") or "")
        # For date-only releases on the subscription day, enroll only while
        # unreleased; subsequent results are handled by the persisted row.
        return day > since.date() or (day == since.date() and not str(event.get("actual") or "").strip())
    except (TypeError, ValueError):
        return False


async def discover(rules: list[dict], events: list[dict]) -> None:
    matches_to_save = [
        (rule, event) for rule in rules for event in events
        if event.get("index_id") and event.get("date")
        and matches(rule, event) and starts_after_rule(rule, event)
    ]
    if matches_to_save:
        await rules_repo.enroll_events(matches_to_save, now_kst().isoformat())
