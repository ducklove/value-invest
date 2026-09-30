# vendored from value-invest ecosystem/python/vc_publish.py — do not edit in sibling repos; run node scripts/sync-ecosystem.mjs --write
"""Value Compass published-data envelope v1 (stdlib only, Python >= 3.9).

Sibling dashboards publish a small ``summary.json`` (+ ``version.json``) at
their GitHub Pages root so the hub never has to download multi-MB data files.
Contract: value-invest ``docs/ecosystem/data-contract.md``.

Canonical JSON (the input of ``contentHash``) is byte-identical to the JS twin
``ecosystem/js/vc-publish.mjs``:

* object keys sorted by Unicode code point, separators ``,`` and ``:``;
* strings as ``json.dumps(ensure_ascii=False)`` (== ``JSON.stringify``);
* numbers formatted like ECMAScript ``Number#toString`` (``1.0`` -> ``1``,
  ``1e-07`` -> ``1e-7``), NaN/Infinity rejected, ``|n| <= 2**53 - 1``;
* UTF-8 bytes, no BOM, no trailing newline.

Published files are the canonical form of the whole envelope plus ``\\n``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Union

SCHEMA_VERSION = 1
KST = timezone(timedelta(hours=9))
KINDS = ("summary",)
# Registry ids (value-invest config/ecosystem.json). New tools may be added;
# validate_envelope only checks the id *shape*, not membership.
KNOWN_TOOLS = (
    "holding_value",
    "common_preferred_spread",
    "spac-hunter",
    "buybacks",
    "eiayn",
    "gold_gap",
    "all-about-gold",
    "nps-tracker",
    "bond-mate",
)
MAX_SAFE_INTEGER = 2**53 - 1
ENVELOPE_KEYS = ("schemaVersion", "tool", "kind", "generatedAt", "asOf", "sources", "contentHash", "data")

_TOOL_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_SOURCE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_GENERATED_AT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?\+09:00$")
_AS_OF_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?\+09:00)?$")
_URL_RE = re.compile(r"^https?://\S+$")
_SURROGATE_RE = re.compile("[\ud800-\udfff]")


class EnvelopeError(ValueError):
    """Raised when data or an envelope violates the v1 contract."""


# --------------------------------------------------------------------------
# canonical JSON
# --------------------------------------------------------------------------

def _format_number(value: Union[int, float], path: str) -> str:
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise EnvelopeError(f"{path}: NaN/Infinity is not allowed (use null for unknown)")
    if abs(value) > MAX_SAFE_INTEGER:
        raise EnvelopeError(f"{path}: |number| exceeds 2**53-1")
    if isinstance(value, int):
        return str(value)
    if value == 0:
        return "0"  # also -0.0, like JS
    # repr() is the shortest round-trip form — the same digits JS picks.
    text = repr(value)
    sign = ""
    if text[0] == "-":
        sign, text = "-", text[1:]
    mantissa, _, exp_part = text.partition("e")
    exponent = int(exp_part) if exp_part else 0
    int_part, _, frac_part = mantissa.partition(".")
    raw = int_part + frac_part
    leading_zeros = len(raw) - len(raw.lstrip("0"))
    digits = raw.strip("0")
    # value = 0.<digits> * 10**n  (the k/n of ECMA-262 Number::toString)
    k, n = len(digits), len(int_part) + exponent - leading_zeros
    if k <= n <= 21:
        out = digits + "0" * (n - k)
    elif 0 < n <= 21:
        out = digits[:n] + "." + digits[n:]
    elif -6 < n <= 0:
        out = "0." + "0" * (-n) + digits
    else:
        e = n - 1
        e_text = ("+" if e >= 0 else "-") + str(abs(e))
        out = digits[0] + ("." + digits[1:] if k > 1 else "") + "e" + e_text
    return sign + out


def _encode(value: Any, path: str, out: list) -> None:
    if value is None:
        out.append("null")
    elif value is True:
        out.append("true")
    elif value is False:
        out.append("false")
    elif isinstance(value, (int, float)):
        out.append(_format_number(value, path))
    elif isinstance(value, str):
        if _SURROGATE_RE.search(value):
            raise EnvelopeError(f"{path}: lone surrogate in string")
        out.append(json.dumps(value, ensure_ascii=False))
    elif isinstance(value, Mapping):
        keys = list(value.keys())
        for key in keys:
            if not isinstance(key, str):
                raise EnvelopeError(f"{path}: object keys must be strings, got {type(key).__name__}")
        out.append("{")
        for i, key in enumerate(sorted(keys)):
            if i:
                out.append(",")
            _encode(key, path, out)
            out.append(":")
            _encode(value[key], f"{path}.{key}", out)
        out.append("}")
    elif isinstance(value, (list, tuple)):
        out.append("[")
        for i, item in enumerate(value):
            if i:
                out.append(",")
            _encode(item, f"{path}[{i}]", out)
        out.append("]")
    else:
        raise EnvelopeError(f"{path}: unsupported type {type(value).__name__}")


def canonical_json(obj: Any) -> str:
    """Canonical compact JSON text (see module docstring)."""
    out: list = []
    _encode(obj, "$", out)
    return "".join(out)


def dumps_compact(obj: Any) -> str:
    """Compact JSON for published files (== canonical form, no newline)."""
    return canonical_json(obj)


def content_hash(data: Any) -> str:
    """``sha256:<hex>`` of the canonical UTF-8 JSON of ``data``."""
    return "sha256:" + hashlib.sha256(canonical_json(data).encode("utf-8")).hexdigest()


def file_hash(path: Union[str, os.PathLike]) -> str:
    """``sha256:<hex>`` of a file's raw bytes (for legacy files in version.json)."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


# --------------------------------------------------------------------------
# envelope
# --------------------------------------------------------------------------

def now_kst_iso() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def _kst_text(value: Union[str, date, datetime], *, allow_date: bool, field: str) -> str:
    if isinstance(value, datetime):
        dt = value.replace(tzinfo=KST) if value.tzinfo is None else value.astimezone(KST)
        return dt.isoformat(timespec="seconds")
    if isinstance(value, date):
        if not allow_date:
            raise EnvelopeError(f"{field}: a datetime is required")
        return value.isoformat()
    if isinstance(value, str):
        return value
    raise EnvelopeError(f"{field}: expected str/date/datetime, got {type(value).__name__}")


def _normalize_sources(sources: Iterable[Mapping[str, Any]]) -> list:
    result = []
    for src in sources or []:
        if not isinstance(src, Mapping):
            raise EnvelopeError("sources: each item must be an object")
        item = {"id": src.get("id"), "name": src.get("name")}
        if src.get("url") is not None:
            item["url"] = src.get("url")
        result.append(item)
    return result


def build_envelope(
    tool: str,
    data: Mapping[str, Any],
    *,
    as_of: Union[str, date, datetime],
    sources: Iterable[Mapping[str, Any]],
    generated_at: Optional[Union[str, datetime]] = None,
    kind: str = "summary",
) -> Dict[str, Any]:
    """Build and validate a v1 envelope around ``data``."""
    envelope = {
        "schemaVersion": SCHEMA_VERSION,
        "tool": tool,
        "kind": kind,
        "generatedAt": _kst_text(generated_at, allow_date=False, field="generatedAt") if generated_at else now_kst_iso(),
        "asOf": _kst_text(as_of, allow_date=True, field="asOf"),
        "sources": _normalize_sources(sources),
        "contentHash": content_hash(data),
        "data": data,
    }
    validate_envelope(envelope)
    return envelope


def validate_envelope(obj: Any) -> Dict[str, Any]:
    """Structural v1 checks (no jsonschema dependency). Returns ``obj``."""
    if not isinstance(obj, Mapping):
        raise EnvelopeError("envelope must be an object")
    missing = [k for k in ENVELOPE_KEYS if k not in obj]
    if missing:
        raise EnvelopeError(f"envelope missing keys: {', '.join(missing)}")
    extra = [k for k in obj if k not in ENVELOPE_KEYS]
    if extra:
        raise EnvelopeError(f"envelope has unknown keys: {', '.join(sorted(extra))}")
    sv = obj["schemaVersion"]
    if isinstance(sv, bool) or sv != SCHEMA_VERSION:
        raise EnvelopeError(f"schemaVersion must be {SCHEMA_VERSION}")
    if not isinstance(obj["tool"], str) or not _TOOL_RE.match(obj["tool"]):
        raise EnvelopeError("tool must be a registry id ([a-z0-9][a-z0-9_-]*)")
    if obj["kind"] not in KINDS:
        raise EnvelopeError(f"kind must be one of {KINDS}")
    if not isinstance(obj["generatedAt"], str) or not _GENERATED_AT_RE.match(obj["generatedAt"]):
        raise EnvelopeError("generatedAt must be ISO8601 with +09:00 (YYYY-MM-DDTHH:MM:SS+09:00)")
    if not isinstance(obj["asOf"], str) or not _AS_OF_RE.match(obj["asOf"]):
        raise EnvelopeError("asOf must be a KST date (YYYY-MM-DD) or datetime with +09:00")
    sources = obj["sources"]
    if not isinstance(sources, list) or not sources:
        raise EnvelopeError("sources must be a non-empty list")
    seen = set()
    for i, src in enumerate(sources):
        if not isinstance(src, Mapping):
            raise EnvelopeError(f"sources[{i}] must be an object")
        extra_src = [k for k in src if k not in ("id", "name", "url")]
        if extra_src:
            raise EnvelopeError(f"sources[{i}] has unknown keys: {', '.join(sorted(extra_src))}")
        sid = src.get("id")
        if not isinstance(sid, str) or not _SOURCE_ID_RE.match(sid):
            raise EnvelopeError(f"sources[{i}].id must match [a-z0-9][a-z0-9_.-]*")
        if sid in seen:
            raise EnvelopeError(f"sources[{i}].id duplicated: {sid}")
        seen.add(sid)
        if not isinstance(src.get("name"), str) or not src["name"].strip():
            raise EnvelopeError(f"sources[{i}].name must be a non-empty string")
        if "url" in src and (not isinstance(src["url"], str) or not _URL_RE.match(src["url"])):
            raise EnvelopeError(f"sources[{i}].url must be an http(s) URL")
    if not isinstance(obj["data"], Mapping):
        raise EnvelopeError("data must be an object")
    if not isinstance(obj["contentHash"], str) or not _HASH_RE.match(obj["contentHash"]):
        raise EnvelopeError("contentHash must be sha256:<64 hex>")
    actual = content_hash(obj["data"])
    if actual != obj["contentHash"]:
        raise EnvelopeError(f"contentHash mismatch: declared {obj['contentHash']}, actual {actual}")
    return obj  # type: ignore[return-value]


# --------------------------------------------------------------------------
# writing (atomic + no-op rule)
# --------------------------------------------------------------------------

def _read_json(path: Union[str, os.PathLike]) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _atomic_write(path: Union[str, os.PathLike], text: str) -> None:
    target = os.fspath(path)
    directory = os.path.dirname(os.path.abspath(target))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".vc-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, target)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _same_content(existing: Any, envelope: Mapping[str, Any]) -> bool:
    if not isinstance(existing, Mapping):
        return False
    return all(existing.get(k) == envelope.get(k) for k in ("schemaVersion", "tool", "kind", "asOf", "contentHash"))


def write_if_changed(path: Union[str, os.PathLike], envelope: Mapping[str, Any]) -> bool:
    """Write ``envelope`` unless the file already holds the same content.

    Same = equal schemaVersion, tool, kind, asOf and contentHash. generatedAt
    and sources are ignored, so a re-run with identical data never produces a
    git diff. Returns True when the file was (re)written.
    """
    validate_envelope(envelope)
    if _same_content(_read_json(path), envelope):
        return False
    _atomic_write(path, dumps_compact(envelope) + "\n")
    return True


def write_version(
    path: Union[str, os.PathLike],
    files: Mapping[str, Union[str, Mapping[str, Any]]],
    *,
    tool: Optional[str] = None,
    generated_at: Optional[Union[str, datetime]] = None,
) -> bool:
    """Write ``version.json`` = {schemaVersion, tool, generatedAt, files}.

    ``files`` maps a published file name to ``sha256:<hex>`` or to an envelope
    (its contentHash is used; ``tool`` is taken from it when omitted). Skips the
    write (returns False) when tool and the files map are unchanged.
    """
    normalized: Dict[str, str] = {}
    for name, value in files.items():
        if isinstance(value, Mapping):
            tool = tool or value.get("tool")
            value = value.get("contentHash")
        if not isinstance(name, str) or not name or not isinstance(value, str) or not _HASH_RE.match(value):
            raise EnvelopeError(f"version files[{name!r}] must be sha256:<64 hex>")
        normalized[name] = value
    if not normalized:
        raise EnvelopeError("version files must not be empty")
    if not isinstance(tool, str) or not _TOOL_RE.match(tool):
        raise EnvelopeError("version tool must be a registry id")
    existing = _read_json(path)
    if (
        isinstance(existing, Mapping)
        and existing.get("schemaVersion") == SCHEMA_VERSION
        and existing.get("tool") == tool
        and existing.get("files") == normalized
    ):
        return False
    doc = {
        "schemaVersion": SCHEMA_VERSION,
        "tool": tool,
        "generatedAt": _kst_text(generated_at, allow_date=False, field="generatedAt") if generated_at else now_kst_iso(),
        "files": normalized,
    }
    if not _GENERATED_AT_RE.match(doc["generatedAt"]):
        raise EnvelopeError("generatedAt must be ISO8601 with +09:00")
    _atomic_write(path, dumps_compact(doc) + "\n")
    return True


# --------------------------------------------------------------------------
# CLI:  python vc_publish.py validate summary.json [...]  |  hash data.json
# --------------------------------------------------------------------------

def main(argv: Optional[list] = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) < 2 or args[0] not in ("validate", "hash"):
        print("usage: vc_publish.py validate <envelope.json>... | hash <data.json>", file=sys.stderr)
        return 2
    status = 0
    for name in args[1:]:
        with open(name, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
        if args[0] == "hash":
            print(f"{content_hash(obj)}  {name}")
            continue
        try:
            validate_envelope(obj)
            print(f"ok  {name}  {obj['tool']}  {obj['contentHash']}")
        except EnvelopeError as exc:
            print(f"FAIL  {name}: {exc}", file=sys.stderr)
            status = 1
    return status


if __name__ == "__main__":
    raise SystemExit(main())
