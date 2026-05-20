from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any, Callable, AsyncIterator

import httpx

import ai_config


OPENROUTER_CHAT_COMPLETIONS_URL = "https://openrouter.ai/api/v1/chat/completions"


class OpenRouterError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class MissingOpenRouterKeyError(OpenRouterError):
    pass


def openrouter_headers(openrouter_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {openrouter_key}",
        "Content-Type": "application/json",
    }


async def require_openrouter_key() -> str:
    openrouter_key = await ai_config.get_openrouter_key()
    if not openrouter_key:
        raise MissingOpenRouterKeyError("OpenRouter API key is not configured.")
    return openrouter_key


def usage_from_response(
    data: dict[str, Any] | None,
    *,
    cost_estimator: Callable[[int, int], float] | None = None,
) -> dict[str, Any]:
    usage = (data or {}).get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
    cost_usd = float(usage.get("cost") or usage.get("total_cost") or 0.0)
    if not cost_usd and cost_estimator and (input_tokens or output_tokens):
        cost_usd = float(cost_estimator(input_tokens, output_tokens) or 0.0)
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
    }


def message_content_from_response(data: dict[str, Any] | None) -> str:
    try:
        message = ((data or {}).get("choices") or [{}])[0].get("message") or {}
    except Exception:
        return ""
    content = message.get("content") or ""
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or ""))
            else:
                parts.append(str(item))
        content = "".join(parts)
    return str(content or "").strip()


async def post_chat_completion(
    *,
    feature: str,
    payload: dict[str, Any],
    google_sub: str | None,
    model: str | None = None,
    model_profile: str | None = None,
    timeout: httpx.Timeout | float = 90.0,
    retry_without_reasoning: bool = True,
    cost_estimator: Callable[[int, int], float] | None = None,
    record_usage: bool = True,
    ok_if_content: bool = False,
    empty_content_error: str = "empty_content",
) -> dict[str, Any]:
    openrouter_key = await require_openrouter_key()
    request_payload = dict(payload)
    resolved_model = model or str(request_payload.get("model") or await ai_config.get_model_for_feature(feature))
    request_payload["model"] = resolved_model
    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                OPENROUTER_CHAT_COMPLETIONS_URL,
                headers=openrouter_headers(openrouter_key),
                json=request_payload,
            )
            if (
                retry_without_reasoning
                and resp.status_code in {400, 422}
                and ("reasoning" in request_payload or "include_reasoning" in request_payload)
            ):
                retry_payload = dict(request_payload)
                retry_payload.pop("reasoning", None)
                retry_payload.pop("include_reasoning", None)
                resp = await client.post(
                    OPENROUTER_CHAT_COMPLETIONS_URL,
                    headers=openrouter_headers(openrouter_key),
                    json=retry_payload,
                )
        if resp.status_code != 200:
            raise OpenRouterError(
                f"OpenRouter HTTP {resp.status_code}: {resp.text[:300]}",
                status_code=resp.status_code,
            )
        data = resp.json()
        content = message_content_from_response(data)
        usage = usage_from_response(data, cost_estimator=cost_estimator)
        used_model = data.get("model") or resolved_model
        latency_ms = int((time.monotonic() - started) * 1000)
        ok = bool(content) if ok_if_content else True
        if record_usage:
            await ai_config.record_usage(
                google_sub=google_sub,
                feature=feature,
                model=used_model,
                model_profile=model_profile or feature,
                input_tokens=usage["input_tokens"],
                output_tokens=usage["output_tokens"],
                cost_usd=usage["cost_usd"],
                latency_ms=latency_ms,
                ok=ok,
                error=None if ok else empty_content_error,
            )
        return {
            "data": data,
            "content": content,
            "model": used_model,
            "latency_ms": latency_ms,
            **usage,
        }
    except Exception as exc:
        if record_usage:
            await ai_config.record_usage(
                google_sub=google_sub,
                feature=feature,
                model=resolved_model,
                model_profile=model_profile or feature,
                ok=False,
                error=str(exc),
                latency_ms=int((time.monotonic() - started) * 1000),
            )
        raise


@asynccontextmanager
async def stream_chat_completion(
    client: httpx.AsyncClient,
    payload: dict[str, Any],
    *,
    openrouter_key: str | None = None,
) -> AsyncIterator[httpx.Response]:
    key = openrouter_key or await require_openrouter_key()
    async with client.stream(
        "POST",
        OPENROUTER_CHAT_COMPLETIONS_URL,
        headers=openrouter_headers(key),
        json=payload,
    ) as resp:
        yield resp
