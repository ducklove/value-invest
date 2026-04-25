"""Runtime AI configuration and usage accounting."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import cache


OPENROUTER_KEY_SETTING = "OPENROUTER_API_KEY"

MODEL_FEATURES: dict[str, dict[str, str]] = {
    "portfolio_fast": {
        "label": "포트폴리오 인사이트 빠름",
        "env": "AI_FAST_MODEL",
        "default": os.getenv("WIKI_QA_MODEL", "google/gemma-4-31b-it"),
    },
    "portfolio_balanced": {
        "label": "포트폴리오 인사이트 균형",
        "env": "AI_DEFAULT_MODEL",
        "default": "qwen/qwen3.6-plus",
    },
    "portfolio_premium": {
        "label": "포트폴리오 인사이트 고급",
        "env": "AI_PREMIUM_MODEL",
        "default": os.getenv("AI_DEFAULT_MODEL", "qwen/qwen3.6-plus"),
    },
    "wiki_qa": {
        "label": "종목 위키 Q&A",
        "env": "WIKI_QA_MODEL",
        "default": os.getenv("AI_FAST_MODEL", "google/gemma-4-31b-it"),
    },
    "wiki_ingestion": {
        "label": "리포트 위키 요약",
        "env": "WIKI_MODEL",
        "default": os.getenv("AI_DEFAULT_MODEL", "qwen/qwen3.6-plus"),
    },
}


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
    stored = await cache.get_app_setting(f"AI_MODEL::{feature}")
    if stored and stored.get("value"):
        return str(stored["value"])
    return os.getenv(spec["env"], spec["default"])


async def model_profiles() -> dict[str, str]:
    return {
        "fast": await get_model_for_feature("portfolio_fast"),
        "balanced": await get_model_for_feature("portfolio_balanced"),
        "premium": await get_model_for_feature("portfolio_premium"),
    }


async def ai_admin_config(days: int = 30) -> dict[str, Any]:
    features = []
    for key, spec in MODEL_FEATURES.items():
        stored = await cache.get_app_setting(f"AI_MODEL::{key}")
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
        await cache.set_app_setting(f"AI_MODEL::{feature}", model, updated_by=actor)


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
