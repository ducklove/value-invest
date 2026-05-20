"""Runtime AI configuration and usage accounting."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import cache


OPENROUTER_KEY_SETTING = "OPENROUTER_API_KEY"
DEFAULT_PORTFOLIO_AI_MODEL = "~google/gemini-flash-latest"
DEFAULT_WIKI_QA_MODEL = "moonshotai/kimi-k2.6"
_MODEL_SETTING_PREFIX = "AI_MODEL::"
_WIKI_QA_KIMI_MIGRATION_KEY = "AI_MIGRATION::wiki_qa_kimi_k2_6"
_WIKI_QA_LEGACY_DEFAULT_MODELS = {"google/gemma-4-31b-it"}
_PORTFOLIO_FLASH_MIGRATION_KEY = "AI_MIGRATION::portfolio_gemini_flash_latest"
_PORTFOLIO_QWEN_FLASH_MIGRATION_KEY = "AI_MIGRATION::portfolio_qwen_flash_to_gemini_flash_latest"
_PORTFOLIO_LEGACY_DEFAULT_MODELS: dict[str, set[str]] = {
    "portfolio_fast": {"google/gemma-4-31b-it"},
    "portfolio_balanced": {"qwen/qwen3.6-plus"},
    "portfolio_premium": {"qwen/qwen3.6-plus"},
}
_PORTFOLIO_QWEN_FLASH_LEGACY_MODELS = {
    "qwen/qwen3.6-flash",
    "qwen/qwen3.6-plus",
}

MODEL_FEATURES: dict[str, dict[str, str]] = {
    "portfolio_fast": {
        "label": "포트폴리오 인사이트 빠름",
        "env": "AI_FAST_MODEL",
        "default": DEFAULT_PORTFOLIO_AI_MODEL,
    },
    "portfolio_balanced": {
        "label": "포트폴리오 인사이트 균형",
        "env": "AI_DEFAULT_MODEL",
        "default": DEFAULT_PORTFOLIO_AI_MODEL,
    },
    "portfolio_premium": {
        "label": "포트폴리오 인사이트 고급",
        "env": "AI_PREMIUM_MODEL",
        "default": os.getenv("AI_DEFAULT_MODEL", DEFAULT_PORTFOLIO_AI_MODEL),
    },
    "wiki_qa": {
        "label": "종목 위키 Q&A",
        "env": "WIKI_QA_MODEL",
        "default": DEFAULT_WIKI_QA_MODEL,
    },
    "wiki_ingestion": {
        "label": "리포트 위키 요약",
        "env": "WIKI_MODEL",
        "default": os.getenv("AI_DEFAULT_MODEL", "qwen/qwen3.6-plus"),
    },
    "dart_report_review": {
        "label": "DART 정기보고서 리뷰",
        "env": "AI_DART_REVIEW_MODEL",
        "default": "deepseek/deepseek-v4-flash",
    },
    "market_daily": {
        "label": "금일 시황",
        "env": "AI_MARKET_DAILY_MODEL",
        "default": "google/gemini-3.5-flash",
    },
}


def _model_setting_key(feature: str) -> str:
    return f"{_MODEL_SETTING_PREFIX}{feature}"


def _configured_default_model(spec: dict[str, str]) -> str:
    return os.getenv(spec["env"], spec["default"])


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
    stored = await cache.get_app_setting(OPENROUTER_KEY_SETTING)
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
    stored = await cache.get_app_setting(OPENROUTER_KEY_SETTING)
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
    await cache.set_app_setting(OPENROUTER_KEY_SETTING, value, is_secret=True, updated_by=actor)


async def delete_openrouter_key():
    await cache.delete_app_setting(OPENROUTER_KEY_SETTING)


async def get_model_for_feature(feature: str) -> str:
    spec = MODEL_FEATURES.get(feature)
    if not spec:
        raise ValueError(f"Unknown AI feature: {feature}")
    stored = await cache.get_app_setting(_model_setting_key(feature))
    if stored and stored.get("value"):
        return str(stored["value"])
    return _configured_default_model(spec)


async def _migrate_legacy_wiki_qa_default() -> dict[str, Any]:
    feature = "wiki_qa"
    setting_key = _model_setting_key(feature)
    marker = await cache.get_app_setting(_WIKI_QA_KIMI_MIGRATION_KEY)
    if marker:
        return {"migrated": False, "reason": "already_marked"}

    stored = await cache.get_app_setting(setting_key)
    stored_value = str((stored or {}).get("value") or "").strip()
    target = _configured_default_model(MODEL_FEATURES[feature])
    if stored_value in _WIKI_QA_LEGACY_DEFAULT_MODELS and stored_value != target:
        await cache.set_app_setting(setting_key, target, updated_by="system:migration")
        await cache.set_app_setting(_WIKI_QA_KIMI_MIGRATION_KEY, target, updated_by="system:migration")
        return {"migrated": True, "feature": feature, "from": stored_value, "to": target}

    await cache.set_app_setting(
        _WIKI_QA_KIMI_MIGRATION_KEY,
        "skipped",
        updated_by="system:migration",
    )
    return {"migrated": False, "reason": "no_legacy_value"}


async def _migrate_legacy_portfolio_model_defaults() -> dict[str, Any]:
    marker = await cache.get_app_setting(_PORTFOLIO_FLASH_MIGRATION_KEY)
    if marker:
        return {"migrated": False, "reason": "already_marked"}

    migrated: list[str] = []
    for feature, legacy_values in _PORTFOLIO_LEGACY_DEFAULT_MODELS.items():
        stored = await cache.get_app_setting(_model_setting_key(feature))
        stored_value = str((stored or {}).get("value") or "").strip()
        target = _configured_default_model(MODEL_FEATURES[feature])
        if stored_value in legacy_values and stored_value != target:
            await cache.set_app_setting(
                _model_setting_key(feature),
                target,
                updated_by="system:migration",
            )
            migrated.append(feature)

    await cache.set_app_setting(
        _PORTFOLIO_FLASH_MIGRATION_KEY,
        ",".join(migrated) if migrated else "skipped",
        updated_by="system:migration",
    )
    if migrated:
        return {"migrated": True, "features": migrated, "to": DEFAULT_PORTFOLIO_AI_MODEL}
    return {"migrated": False, "reason": "no_legacy_value"}


async def _migrate_qwen_flash_portfolio_overrides() -> dict[str, Any]:
    marker = await cache.get_app_setting(_PORTFOLIO_QWEN_FLASH_MIGRATION_KEY)
    if marker:
        return {"migrated": False, "reason": "already_marked"}

    migrated: list[str] = []
    for feature in ("portfolio_fast", "portfolio_balanced", "portfolio_premium"):
        stored = await cache.get_app_setting(_model_setting_key(feature))
        stored_value = str((stored or {}).get("value") or "").strip().lower()
        target = _configured_default_model(MODEL_FEATURES[feature])
        if stored_value in _PORTFOLIO_QWEN_FLASH_LEGACY_MODELS and stored_value != target.lower():
            await cache.set_app_setting(
                _model_setting_key(feature),
                target,
                updated_by="system:migration",
            )
            migrated.append(feature)

    await cache.set_app_setting(
        _PORTFOLIO_QWEN_FLASH_MIGRATION_KEY,
        ",".join(migrated) if migrated else "skipped",
        updated_by="system:migration",
    )
    if migrated:
        return {"migrated": True, "features": migrated, "to": DEFAULT_PORTFOLIO_AI_MODEL}
    return {"migrated": False, "reason": "no_legacy_value"}


async def migrate_legacy_model_defaults() -> dict[str, Any]:
    """One-shot migrations for model defaults that were persisted in admin DB."""
    wiki = await _migrate_legacy_wiki_qa_default()
    portfolio = await _migrate_legacy_portfolio_model_defaults()
    portfolio_qwen_flash = await _migrate_qwen_flash_portfolio_overrides()
    return {
        "migrated": bool(
            wiki.get("migrated")
            or portfolio.get("migrated")
            or portfolio_qwen_flash.get("migrated")
        ),
        "wiki_qa": wiki,
        "portfolio": portfolio,
        "portfolio_qwen_flash": portfolio_qwen_flash,
    }


async def model_profiles() -> dict[str, str]:
    return {
        "fast": await get_model_for_feature("portfolio_fast"),
        "balanced": await get_model_for_feature("portfolio_balanced"),
        "premium": await get_model_for_feature("portfolio_premium"),
    }


async def ai_admin_config(days: int = 30) -> dict[str, Any]:
    features = []
    for key, spec in MODEL_FEATURES.items():
        stored = await cache.get_app_setting(_model_setting_key(key))
        model = await get_model_for_feature(key)
        features.append(
            {
                "key": key,
                "label": spec["label"],
                "model": model,
                "source": "admin-db" if stored and stored.get("value") else "env/default",
                "updated_at": stored.get("updated_at") if stored else None,
                "updated_by": stored.get("updated_by") if stored else None,
            }
        )
    return {
        "openrouter": await openrouter_key_status(),
        "features": features,
        "usage": await cache.summarize_ai_usage(days=days),
    }


async def save_feature_models(models: dict[str, Any], actor: str | None):
    for feature, value in models.items():
        if feature not in MODEL_FEATURES:
            raise ValueError(f"Unknown AI feature: {feature}")
        model = str(value or "").strip()
        if not model:
            raise ValueError(f"{feature} model is required.")
        await cache.set_app_setting(_model_setting_key(feature), model, updated_by=actor)


def openrouter_reasoning_controls(model: str) -> dict[str, Any]:
    """Return conservative OpenRouter reasoning controls for user-facing text.

    Some reasoning-capable models count hidden reasoning against the same
    completion budget used for the visible answer. For short UI analyses we
    prefer a small or disabled reasoning budget and hide reasoning content so
    the browser reliably receives final text.
    """
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
    await cache.insert_ai_usage_event(
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
