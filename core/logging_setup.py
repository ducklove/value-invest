"""Process logging setup: secret redaction + quiet HTTP client loggers.

httpx logs every request at INFO as ``HTTP Request: GET <full url> "HTTP/1.1
200 OK"``. Several upstreams carry credentials in the URL — OpenDART
``crtfc_key``, FRED ``api_key``, data.go.kr ``serviceKey``, the ECOS key as a
path segment and the Telegram bot token as ``/bot<token>/`` — so those lines
leaked secrets into journald. ``httpx.HTTPStatusError`` messages (and their
tracebacks) embed the same URL.

``install_logging()`` (called from ``main.py``):

* lowers the ``httpx``/``httpcore`` loggers to WARNING, and
* installs :class:`SecretRedactingFilter` on the root handlers, uvicorn's
  handlers and the ``httpx``/``httpcore`` loggers, so any record that reaches
  a handler has secret values masked in its message, args and formatted
  exception text.

The helpers are idempotent — calling them twice does not stack filters.
"""
from __future__ import annotations

import logging
import re
from typing import Any

REDACTED = "***"

# Query/form parameter names whose values are secrets (case-insensitive).
SECRET_PARAM_NAMES: tuple[str, ...] = (
    "crtfc_key",
    "apikey",
    "api_key",
    "key",
    "serviceKey",
    "token",
    "access_token",
    "appkey",
    "appsecret",
    "secret",
    "password",
)

_PARAM_RE = re.compile(
    # Only URL query context ([?&;] before the name) — a bare "key=..." in a
    # free-text log line (e.g. a cache key) is left alone.
    r"(?P<prefix>[?&;](?:"
    + "|".join(re.escape(name) for name in SECRET_PARAM_NAMES)
    + r")=)(?P<value>[^&;#\s\"'<>)]+)",
    re.IGNORECASE,
)
# Telegram Bot API: /bot<id>:<secret>/method and /file/bot<id>:<secret>/path.
_TELEGRAM_BOT_RE = re.compile(r"(?P<prefix>/bot)(?P<value>\d+:[A-Za-z0-9_-]+)")
# 한국은행 ECOS: https://ecos.bok.or.kr/api/<Service>/<KEY>/json/...
_ECOS_PATH_RE = re.compile(
    r"(?P<prefix>ecos\.bok\.or\.kr/api/[A-Za-z]+/)(?P<value>[^/\s\"'?#]+)",
    re.IGNORECASE,
)

_NOISY_HTTP_LOGGERS = ("httpx", "httpcore")
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def redact_secrets(text: str) -> str:
    """Mask secret query params, Telegram bot tokens and ECOS path keys."""
    if not text:
        return text
    text = _PARAM_RE.sub(lambda m: m.group("prefix") + REDACTED, text)
    text = _TELEGRAM_BOT_RE.sub(lambda m: m.group("prefix") + REDACTED, text)
    text = _ECOS_PATH_RE.sub(lambda m: m.group("prefix") + REDACTED, text)
    return text


def _may_contain_secret(text: str) -> bool:
    return "=" in text or "/bot" in text or "ecos" in text.lower()


def _redact_arg(value: Any) -> Any:
    # Only str-like args are rewritten; numbers keep their type so %d/%f
    # format specifiers in the message still work.
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    text = str(value)
    if _may_contain_secret(text):
        redacted = redact_secrets(text)
        if redacted != text:
            return redacted
    return value


class SecretRedactingFilter(logging.Filter):
    """Mask secrets in a record's msg, args and exception text. Never drops."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, str):
                record.msg = redact_secrets(record.msg)
            elif record.msg is not None and not isinstance(record.msg, (int, float)):
                text = str(record.msg)
                if _may_contain_secret(text):
                    record.msg = redact_secrets(text)
            args = record.args
            if isinstance(args, tuple):
                record.args = tuple(_redact_arg(arg) for arg in args)
            elif isinstance(args, dict):
                record.args = {key: _redact_arg(val) for key, val in args.items()}
            if record.exc_info and not record.exc_text:
                # Formatter.format() reuses a pre-set exc_text, so formatting
                # it here lets us redact URLs embedded in exception messages
                # (e.g. httpx.HTTPStatusError "... for url '...crtfc_key=...'").
                record.exc_text = logging.Formatter().formatException(record.exc_info)
            if record.exc_text:
                record.exc_text = redact_secrets(record.exc_text)
            if record.stack_info:
                record.stack_info = redact_secrets(record.stack_info)
        except (TypeError, ValueError, AttributeError):  # pragma: no cover - defensive
            # Redaction must never break logging itself.
            return True
        return True


def _has_redacting_filter(filterer: logging.Filterer) -> bool:
    return any(isinstance(f, SecretRedactingFilter) for f in filterer.filters)


def _add_filter_once(filterer: logging.Filterer, flt: SecretRedactingFilter) -> None:
    if not _has_redacting_filter(filterer):
        filterer.addFilter(flt)


def install_secret_redaction() -> SecretRedactingFilter:
    """Attach the redacting filter to every handler/logger that matters.

    Handler-level filters see records propagated from any child logger;
    logger-level filters on httpx/httpcore also cover handlers attached later
    (e.g. a test's caplog) for those loggers' own records.
    """
    flt = SecretRedactingFilter()
    root = logging.getLogger()
    for handler in root.handlers:
        _add_filter_once(handler, flt)
    for name in _UVICORN_LOGGERS:
        for handler in logging.getLogger(name).handlers:
            _add_filter_once(handler, flt)
    for name in _NOISY_HTTP_LOGGERS:
        _add_filter_once(logging.getLogger(name), flt)
    return flt


def quiet_http_client_loggers(level: int = logging.WARNING) -> None:
    """Stop httpx/httpcore from logging every request URL at INFO/DEBUG."""
    for name in _NOISY_HTTP_LOGGERS:
        logging.getLogger(name).setLevel(level)


def install_logging(level: int = logging.INFO) -> None:
    """main.py's logging setup: basicConfig + quiet HTTP loggers + redaction."""
    logging.basicConfig(level=level)
    quiet_http_client_loggers()
    install_secret_redaction()
