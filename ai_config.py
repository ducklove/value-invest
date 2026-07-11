"""Runtime AI configuration and usage accounting."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from core.errors import RateLimitError
from repositories import ai_usage as ai_usage_repo
from repositories import app_settings as app_settings_repo

OPENROUTER_KEY_SETTING = "OPENROUTER_API_KEY"
# OpenRouter 별칭 대신 명시적 모델 ID를 등급 기본값으로 둔다. 실제 운영값은
# 관리자 DB 설정 또는 등급별 환경변수로 재정의할 수 있다.
LIGHT_MODEL = "qwen/qwen3.6-flash"
BALANCED_MODEL = "google/gemini-3.5-flash"
PREMIUM_MODEL = "openai/gpt-5.6-terra"
GEMINI_35_FLASH = BALANCED_MODEL
DEFAULT_PORTFOLIO_AI_MODEL = BALANCED_MODEL
_TIER_MODEL_SETTING_PREFIX = "AI_TIER_MODEL::"

# 모델 ID는 기능별로 흩어 두지 않고 등급 설정 한 곳에서 관리한다. DB에 저장된
# 관리자 설정이 최우선이고, 그 다음 새 등급 env, 기존 호환 env, 코드 기본값 순이다.
MODEL_TIERS: dict[str, dict[str, Any]] = {
    "LOCAL": {
        "label": "LOCAL",
        "description": "외부 LLM을 호출하지 않는 규칙/템플릿 모드",
        "external": False,
    },
    "LIGHT": {
        "label": "LIGHT",
        "description": "대량·저비용 요약과 Q&A",
        "env": "AI_LIGHT_MODEL",
        "legacy_envs": ("AI_FAST_MODEL",),
        "default": LIGHT_MODEL,
        "external": True,
    },
    "BALANCED": {
        "label": "BALANCED",
        "description": "일반 금융 분석과 기본 포트폴리오 분석",
        "env": "AI_BALANCED_MODEL",
        "legacy_envs": ("AI_DEFAULT_MODEL",),
        "default": BALANCED_MODEL,
        "external": True,
    },
    "PREMIUM": {
        "label": "PREMIUM",
        "description": "사용자가 명시적으로 실행하는 고급 분석",
        "env": "AI_PREMIUM_MODEL",
        "legacy_envs": (),
        "default": PREMIUM_MODEL,
        "external": True,
    },
}

MODEL_FEATURES: dict[str, dict[str, str]] = {
    "portfolio_fast": {
        "label": "포트폴리오 인사이트 빠름",
        "tier": "LIGHT",
    },
    "portfolio_balanced": {
        "label": "포트폴리오 인사이트 균형",
        "tier": "BALANCED",
    },
    "portfolio_premium": {
        "label": "포트폴리오 인사이트 고급",
        "tier": "PREMIUM",
    },
    "wiki_qa": {
        "label": "종목 위키 Q&A",
        "tier": "LIGHT",
    },
    "wiki_ingestion": {
        "label": "리포트 위키 요약",
        "tier": "LIGHT",
    },
    "dart_report_review": {
        "label": "DART 정기보고서 리뷰",
        "tier": "BALANCED",
    },
    "market_daily": {
        "label": "금일 시황",
        "tier": "BALANCED",
    },
    "daily_briefing": {
        "label": "AI 데일리 브리핑",
        "tier": "LIGHT",
    },
    "masters_review": {
        "label": "투자 대가 포트폴리오 진단",
        "tier": "BALANCED",
    },
}


def _tier_model_setting_key(tier: str) -> str:
    return f"{_TIER_MODEL_SETTING_PREFIX}{tier}"


def _configured_tier_model(spec: dict[str, Any]) -> str:
    value = os.getenv(str(spec["env"]))
    if value:
        return value
    for legacy_env in spec.get("legacy_envs", ()):
        legacy_value = os.getenv(str(legacy_env))
        if legacy_value:
            return legacy_value
    return str(spec["default"])


def _load_key_from_file(name: str) -> str:
    keys_path = Path(__file__).parent / "keys.txt"
    if not keys_path.exists():
        return ""
    for raw_line in keys_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == name:
            return value.strip()
    return ""


def _mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 10:
        return value[:2] + "..." + value[-2:]
    return value[:6] + "..." + value[-4:]


async def get_openrouter_key() -> str:
    stored = await app_settings_repo.get_app_setting(OPENROUTER_KEY_SETTING)
    if stored and stored.get("value"):
        return str(stored["value"])
    env_or_file = os.getenv(OPENROUTER_KEY_SETTING, "") or _load_key_from_file(OPENROUTER_KEY_SETTING)
    if env_or_file:
        return env_or_file
    # Compatibility for tests/legacy modules that patched the previous
    # routes.portfolio module global. Runtime config still uses DB/env first.
    pf_mod = sys.modules.get("routes.portfolio")
    return str(getattr(pf_mod, "_OPENROUTER_KEY", "") or "")


async def openrouter_key_status() -> dict[str, Any]:
    stored = await app_settings_repo.get_app_setting(OPENROUTER_KEY_SETTING)
    if stored and stored.get("value"):
        value = str(stored["value"])
        return {
            "configured": True,
            "source": "admin-db",
            "masked": _mask_secret(value),
            "updated_at": stored.get("updated_at"),
            "updated_by": stored.get("updated_by"),
        }
    env_value = os.getenv(OPENROUTER_KEY_SETTING, "")
    if env_value:
        return {"configured": True, "source": "env", "masked": _mask_secret(env_value)}
    file_value = _load_key_from_file(OPENROUTER_KEY_SETTING)
    if file_value:
        return {"configured": True, "source": "keys.txt", "masked": _mask_secret(file_value)}
    return {"configured": False, "source": "missing", "masked": ""}


async def set_openrouter_key(value: str, actor: str | None):
    value = value.strip()
    if not value:
        raise ValueError("OpenRouter API key is required.")
    await app_settings_repo.set_app_setting(OPENROUTER_KEY_SETTING, value, is_secret=True, updated_by=actor)


async def delete_openrouter_key():
    await app_settings_repo.delete_app_setting(OPENROUTER_KEY_SETTING)


async def get_model_for_tier(tier: str) -> str:
    normalized = str(tier or "").strip().upper()
    spec = MODEL_TIERS.get(normalized)
    if not spec:
        raise ValueError(f"Unknown AI tier: {tier}")
    if not spec.get("external"):
        return ""
    stored = await app_settings_repo.get_app_setting(_tier_model_setting_key(normalized))
    if stored and stored.get("value"):
        return str(stored["value"])
    return _configured_tier_model(spec)


async def get_model_for_feature(feature: str) -> str:
    spec = MODEL_FEATURES.get(feature)
    if not spec:
        raise ValueError(f"Unknown AI feature: {feature}")
    return await get_model_for_tier(spec["tier"])


async def migrate_legacy_model_defaults() -> dict[str, Any]:
    """Compatibility hook retained for startup after switching to tier routing.

    Legacy ``AI_MODEL::<feature>`` rows intentionally remain untouched for audit
    history, but no longer participate in runtime resolution. Tier settings are
    the single source of truth from this release onward.
    """
    return {"migrated": False, "reason": "tier_routing_active"}


async def model_profiles() -> dict[str, str]:
    return {
        "fast": await get_model_for_feature("portfolio_fast"),
        "balanced": await get_model_for_feature("portfolio_balanced"),
        "premium": await get_model_for_feature("portfolio_premium"),
    }


async def ai_admin_config(days: int = 30) -> dict[str, Any]:
    tiers = []
    for key, spec in MODEL_TIERS.items():
        stored = await app_settings_repo.get_app_setting(_tier_model_setting_key(key))
        model = await get_model_for_tier(key)
        tiers.append(
            {
                "key": key,
                "label": spec["label"],
                "description": spec["description"],
                "model": model,
                "external": bool(spec.get("external")),
                "source": "admin-db" if stored and stored.get("value") else "env/default",
                "updated_at": stored.get("updated_at") if stored else None,
                "updated_by": stored.get("updated_by") if stored else None,
            }
        )
    features = []
    for key, spec in MODEL_FEATURES.items():
        model = await get_model_for_feature(key)
        features.append(
            {
                "key": key,
                "label": spec["label"],
                "tier": spec["tier"],
                "model": model,
                "source": f"tier:{spec['tier']}",
            }
        )
    return {
        "openrouter": await openrouter_key_status(),
        "tiers": tiers,
        "features": features,
        "usage": await ai_usage_repo.summarize_ai_usage(days=days),
    }


async def save_tier_models(models: dict[str, Any], actor: str | None):
    for raw_tier, value in models.items():
        tier = str(raw_tier or "").strip().upper()
        spec = MODEL_TIERS.get(tier)
        if not spec:
            raise ValueError(f"Unknown AI tier: {raw_tier}")
        if not spec.get("external"):
            raise ValueError(f"{tier} does not use an external model.")
        model = str(value or "").strip()
        if not model:
            raise ValueError(f"{tier} model is required.")
        await app_settings_repo.set_app_setting(_tier_model_setting_key(tier), model, updated_by=actor)


def openrouter_reasoning_controls(model: str, *, effort: str | None = None) -> dict[str, Any]:
    """Return conservative OpenRouter reasoning controls for user-facing text.

    Some reasoning-capable models count hidden reasoning against the same
    completion budget used for the visible answer. For short UI analyses we
    prefer a small or disabled reasoning budget and hide reasoning content so
    the browser reliably receives final text.
    """
    allowed_efforts = {"none", "minimal", "low", "medium", "high", "xhigh"}
    requested_effort = str(effort or "").strip().lower()
    if requested_effort:
        if requested_effort not in allowed_efforts:
            requested_effort = "minimal"
        return {
            "reasoning": {"effort": requested_effort, "exclude": True},
            "include_reasoning": False,
        }

    normalized = (model or "").lower()
    if "gemini-3" in normalized:
        return {
            "reasoning": {"effort": "minimal", "exclude": True},
            "include_reasoning": False,
        }
    return {
        "reasoning": {"effort": "none", "exclude": True},
        "include_reasoning": False,
    }


async def record_usage(
    *,
    google_sub: str | None,
    feature: str,
    model: str,
    model_profile: str | None,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cost_usd: float = 0.0,
    latency_ms: int | None = None,
    ok: bool = True,
    error: str | None = None,
):
    await ai_usage_repo.insert_ai_usage_event(
        google_sub=google_sub,
        feature=feature,
        model=model,
        model_profile=model_profile,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=cost_usd,
        latency_ms=latency_ms,
        ok=ok,
        error=error,
    )


# --- Daily budget caps (ST-11) ---------------------------------------------
# Prevents runaway OpenRouter spend: a per-user AND a site-wide daily USD cap,
# enforced BEFORE a completion call. Both default to 0 (disabled) so existing
# behavior is unchanged unless an operator sets the env var. This is a soft
# guard — it only stops *new* calls; already-running calls finish.

def _budget_env_float(name: str, default: float = 0.0) -> float:
    """Read a non-negative USD budget from env. 0 = cap disabled."""
    try:
        value = float(os.getenv(name, "") or default)
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


class BudgetExceededError(RateLimitError):
    """Raised when a daily AI cost cap would be exceeded (ST-11).

    Subclassing core.errors.RateLimitError makes AppError's exception handler
    map it to HTTP 429 automatically — callers see a clean "rate limited"
    response instead of a bare 500. ``__cause__`` is preserved so logs keep the
    original traceback.
    """

    def __init__(self, scope: str, spent: float, cap: float):
        self.scope = scope
        self.spent = spent
        self.cap = cap
        super().__init__(
            f"daily AI cost cap reached for {scope}: "
            f"${spent:.4f} >= ${cap:.4f} (try again tomorrow KST)"
        )


async def enforce_budget_caps(google_sub: str | None) -> dict | None:
    """Raise BudgetExceededError if the user or site-wide daily cap is hit.

    Returns a small summary dict when within budget (useful for logging/admin),
    or None when caps are disabled. Called by the AI client before posting a
    completion. Best-effort: a DB read failure is swallowed to never block a
    legitimate call (the cost is tracked regardless after the call).
    """
    site_cap = _budget_env_float("AI_DAILY_BUDGET_USD")
    user_cap = _budget_env_float("AI_USER_DAILY_BUDGET_USD")
    if site_cap <= 0 and user_cap <= 0:
        return None
    try:
        if user_cap > 0 and google_sub:
            spent = await ai_usage_repo.get_daily_cost_usd(google_sub)
            if spent >= user_cap:
                raise BudgetExceededError(f"user:{google_sub}", spent, user_cap)
        if site_cap > 0:
            spent = await ai_usage_repo.get_daily_cost_usd(None)
            if spent >= site_cap:
                raise BudgetExceededError("site", spent, site_cap)
    except BudgetExceededError:
        raise
    except Exception:  # noqa: BLE001 — never block on a guard read failure
        return None
    return {
        "site_cap": site_cap or None,
        "user_cap": user_cap or None,
        "google_sub": google_sub,
    }
