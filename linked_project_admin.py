"""Admin helpers for editing linked-project configuration files.

The linked projects remain separate deployables, but their source-of-truth
lists are small `config.json` files. This module gives the value-invest admin
surface a safe, validated way to read/write those configs without importing
the sibling apps themselves.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import integrations


PROJECT_SPECS: dict[str, dict[str, Any]] = {
    "holdingValue": {
        "label": "지주사 목록",
        "repo": "ducklove/holding_value",
        "env_dir": "HOLDING_VALUE_DIR",
        "candidates": ["hodling-value", "holding_value"],
        "config_kind": "holding",
        "base_url_key": "holdingValue",
        "base_url_env": "HOLDING_VALUE_BASE_URL",
    },
    "preferredSpread": {
        "label": "우선주 pair 목록",
        "repo": "ducklove/common_preferred_spread",
        "env_dir": "PREFERRED_SPREAD_DIR",
        "candidates": ["common_preferred_spread"],
        "config_kind": "preferred",
        "base_url_key": "preferredSpread",
        "base_url_env": "PREFERRED_SPREAD_BASE_URL",
    },
    "goldGap": {
        "label": "금/비트코인 gap 설정",
        "repo": "ducklove/gold_gap",
        "env_dir": "GOLD_GAP_DIR",
        "candidates": ["gold_gap"],
        "config_kind": "gold",
        "base_url_key": "goldGap",
        "base_url_env": "GOLD_GAP_BASE_URL",
    },
}


class LinkedProjectConfigError(ValueError):
    """Raised when a linked-project config payload is invalid."""


def project_keys() -> list[str]:
    return list(PROJECT_SPECS)


def list_project_configs(workspace_root: Path | None = None, include_remote: bool = True) -> list[dict[str, Any]]:
    keys = project_keys()
    if not include_remote:
        return [
            get_project_config(key, workspace_root=workspace_root, include_remote=False)
            for key in keys
        ]
    with ThreadPoolExecutor(max_workers=len(keys)) as executor:
        return list(executor.map(
            lambda key: get_project_config(key, workspace_root=workspace_root, include_remote=True),
            keys,
        ))


def get_project_config(
    project_key: str,
    workspace_root: Path | None = None,
    include_remote: bool = True,
) -> dict[str, Any]:
    spec = _spec(project_key)
    project_dir = _project_dir(spec, workspace_root)
    config_path = project_dir / "config.json" if project_dir else None
    local_config = _read_json(config_path)
    remote_url = _remote_config_url(spec)
    remote_config = None
    remote_error = None
    if include_remote and remote_url:
        remote_config, remote_error = _read_remote_json(remote_url)
    sync_result = _empty_sync_result()
    if include_remote and remote_config is not None:
        local_config, sync_result = _sync_local_config_from_public(
            spec["config_kind"],
            config_path,
            local_config,
            remote_config,
        )
    config, source, diagnostics = _resolve_effective_config(
        spec["config_kind"],
        local_config,
        remote_config,
    )
    config_loaded = config is not None
    summary = _summarize_config(spec["config_kind"], config)
    local_loaded = local_config is not None
    remote_loaded = remote_config is not None
    return {
        "key": project_key,
        "label": spec["label"],
        "repo": spec["repo"],
        "configKind": spec["config_kind"],
        "localAvailable": bool(project_dir),
        "localConfigLoaded": local_loaded,
        "publicConfigLoaded": remote_loaded,
        "configLoaded": config_loaded,
        "writable": bool(config_path and (config_path.exists() or config_path.parent.exists())),
        "source": source,
        "configPath": str(config_path) if config_path else "",
        "publicConfigUrl": remote_url,
        "summary": summary,
        "diagnostics": {
            **diagnostics,
            "localCount": _config_count(spec["config_kind"], local_config),
            "publicCount": _config_count(spec["config_kind"], remote_config),
            "effectiveCount": summary.get("count", 0),
            "remoteError": remote_error,
            "sync": sync_result,
        },
        "config": config,
    }


def save_project_config(project_key: str, config: Any, workspace_root: Path | None = None) -> dict[str, Any]:
    spec = _spec(project_key)
    project_dir = _project_dir(spec, workspace_root)
    if not project_dir:
        raise LinkedProjectConfigError(f"{project_key} local project directory is not available.")
    config_path = project_dir / "config.json"
    normalized = validate_config(spec["config_kind"], config)
    _atomic_write_json(config_path, normalized)
    result = get_project_config(project_key, workspace_root=workspace_root, include_remote=False)
    result["saved"] = True
    return result


def validate_config(config_kind: str, config: Any) -> Any:
    if config_kind == "preferred":
        return _validate_preferred_config(config)
    if config_kind == "holding":
        return _validate_holding_config(config)
    if config_kind == "gold":
        return _validate_gold_config(config)
    raise LinkedProjectConfigError(f"Unsupported config kind: {config_kind}")


def _spec(project_key: str) -> dict[str, Any]:
    try:
        return PROJECT_SPECS[project_key]
    except KeyError as exc:
        raise LinkedProjectConfigError(f"Unknown linked project: {project_key}") from exc


def _workspace_root(workspace_root: Path | None) -> Path:
    if workspace_root is not None:
        return Path(workspace_root)
    return Path(os.getenv("LINKED_PROJECTS_ROOT", str(integrations.DEFAULT_WORKSPACE_ROOT)))


def _project_dir(spec: dict[str, Any], workspace_root: Path | None) -> Path | None:
    override = os.getenv(spec["env_dir"])
    if override:
        path = Path(override)
        return path if path.exists() else None
    root = _workspace_root(workspace_root)
    for name in spec["candidates"]:
        path = root / name
        if path.exists():
            return path
    return None


def _read_json(path: Path | None) -> Any:
    if not path or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _remote_config_url(spec: dict[str, Any]) -> str:
    base_key = spec.get("base_url_key")
    if not base_key:
        return ""
    base_url = os.getenv(spec.get("base_url_env", ""), integrations.DEFAULT_BASE_URLS.get(base_key, ""))
    base_url = str(base_url or "").rstrip("/")
    return f"{base_url}/config.json" if base_url else ""


def _read_remote_json(url: str, timeout: float = 5.0) -> tuple[Any, str | None]:
    try:
        req = Request(url, headers={"User-Agent": "value-invest-admin/1.0"})
        with urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
        return json.loads(raw), None
    except HTTPError as exc:
        return None, f"HTTP {exc.code}"
    except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        return None, str(exc)[:240]


def _resolve_effective_config(config_kind: str, local_config: Any, public_config: Any) -> tuple[Any, str, dict[str, Any]]:
    if config_kind in {"preferred", "holding"}:
        local_rows = local_config if isinstance(local_config, list) else None
        public_rows = public_config if isinstance(public_config, list) else None
        if local_rows is not None and public_rows is not None:
            merged, diagnostics = _merge_list_configs(config_kind, local_rows, public_rows)
            source = "merged" if diagnostics["missingLocally"] else "local"
            return merged, source, diagnostics
        if local_rows is not None:
            return _copy_rows(local_rows), "local", _empty_list_diagnostics()
        if public_rows is not None:
            return _copy_rows(public_rows), "public", _empty_list_diagnostics()
        return None, "missing", _empty_list_diagnostics()

    diagnostics = {
        "missingLocally": [],
        "missingPublicly": [],
        "missingLocallyCount": 0,
        "missingPubliclyCount": 0,
    }
    if local_config is not None:
        return local_config, "local", diagnostics
    if public_config is not None:
        return public_config, "public", diagnostics
    return None, "missing", diagnostics


def _merge_list_configs(config_kind: str, local_rows: list[Any], public_rows: list[Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    public_by_key = {
        key: row
        for row in public_rows
        if isinstance(row, dict) and (key := _row_key(config_kind, row))
    }
    local_by_key = {
        key: row
        for row in local_rows
        if isinstance(row, dict) and (key := _row_key(config_kind, row))
    }
    merged: list[dict[str, Any]] = []
    for raw in local_rows:
        if not isinstance(raw, dict):
            continue
        merged.append(dict(raw))
    for key, raw in public_by_key.items():
        if key in local_by_key:
            continue
        merged.append(dict(raw))

    missing_locally = [
        _diff_row(config_kind, row)
        for key, row in public_by_key.items()
        if key not in local_by_key
    ]
    missing_publicly = [
        _diff_row(config_kind, row)
        for key, row in local_by_key.items()
        if key not in public_by_key
    ]
    return merged, {
        "missingLocally": missing_locally[:50],
        "missingPublicly": missing_publicly[:50],
        "missingLocallyCount": len(missing_locally),
        "missingPubliclyCount": len(missing_publicly),
    }


def _copy_rows(rows: list[Any]) -> list[dict[str, Any]]:
    copied = []
    for raw in rows:
        if isinstance(raw, dict):
            copied.append(dict(raw))
    return copied


def _sync_local_config_from_public(
    config_kind: str,
    config_path: Path | None,
    local_config: Any,
    public_config: Any,
) -> tuple[Any, dict[str, Any]]:
    result = _empty_sync_result()
    if config_kind not in {"preferred", "holding"}:
        return local_config, result
    if not config_path or not (config_path.exists() or config_path.parent.exists()):
        return local_config, result
    public_rows = public_config if isinstance(public_config, list) else None
    if public_rows is None:
        return local_config, result
    if local_config is not None and not isinstance(local_config, list):
        result["error"] = "local config is not a list"
        return local_config, result

    local_rows = local_config if isinstance(local_config, list) else []
    local_keys = {
        key
        for row in local_rows
        if isinstance(row, dict) and (key := _row_key(config_kind, row))
    }
    additions = [
        dict(row)
        for row in public_rows
        if isinstance(row, dict)
        and (key := _row_key(config_kind, row))
        and key not in local_keys
    ]
    if not additions:
        return local_config, result

    merged = [dict(row) for row in local_rows if isinstance(row, dict)] + additions
    try:
        normalized = validate_config(config_kind, merged)
        _atomic_write_json(config_path, normalized)
    except (OSError, LinkedProjectConfigError) as exc:
        result["error"] = str(exc)[:240]
        return local_config, result

    result["updated"] = True
    result["addedFromPublicCount"] = len(additions)
    return normalized, result


def _empty_sync_result() -> dict[str, Any]:
    return {"updated": False, "addedFromPublicCount": 0, "error": None}


def _empty_list_diagnostics() -> dict[str, Any]:
    return {
        "missingLocally": [],
        "missingPublicly": [],
        "missingLocallyCount": 0,
        "missingPubliclyCount": 0,
    }


def _row_key(config_kind: str, row: dict[str, Any]) -> str:
    if config_kind == "preferred":
        return _ticker_code(row.get("preferredTicker") or row.get("preferredCode"))
    if config_kind == "holding":
        return _ticker_code(row.get("holdingTicker") or row.get("holdingCode"))
    return str(row.get("id") or "").strip().upper()


def _ticker_code(value: Any) -> str:
    return str(value or "").split(".", 1)[0].strip().upper()


def _diff_row(config_kind: str, row: dict[str, Any]) -> dict[str, Any]:
    if config_kind == "preferred":
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "commonTicker": row.get("commonTicker"),
            "preferredTicker": row.get("preferredTicker"),
            "commonName": row.get("commonName"),
            "preferredName": row.get("preferredName"),
        }
    if config_kind == "holding":
        return {
            "id": row.get("id"),
            "name": row.get("name"),
            "holdingTicker": row.get("holdingTicker"),
            "holdingName": row.get("holdingName"),
        }
    return dict(row)


def _config_count(config_kind: str, config: Any) -> int:
    return int(_summarize_config(config_kind, config).get("count", 0))


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _summarize_config(config_kind: str, config: Any) -> dict[str, Any]:
    if config_kind in {"preferred", "holding"} and isinstance(config, list):
        return {"count": len(config)}
    if config_kind == "gold" and isinstance(config, dict):
        assets = config.get("assets") if isinstance(config.get("assets"), dict) else {}
        return {"count": len(assets), "assets": sorted(assets)}
    return {"count": 0}


def _validate_preferred_config(config: Any) -> list[dict[str, Any]]:
    if not isinstance(config, list):
        raise LinkedProjectConfigError("preferred config must be a list.")
    seen_ids: set[str] = set()
    seen_pref: set[str] = set()
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(config, start=1):
        if not isinstance(raw, dict):
            raise LinkedProjectConfigError(f"preferred row #{idx} must be an object.")
        row = {
            "id": _required_text(raw, "id", idx),
            "name": _required_text(raw, "name", idx),
            "commonTicker": _required_text(raw, "commonTicker", idx),
            "preferredTicker": _required_text(raw, "preferredTicker", idx),
            "commonName": _required_text(raw, "commonName", idx),
            "preferredName": _required_text(raw, "preferredName", idx),
        }
        _ensure_unique(seen_ids, row["id"], f"duplicate preferred id: {row['id']}")
        _ensure_unique(
            seen_pref,
            row["preferredTicker"].upper(),
            f"duplicate preferredTicker: {row['preferredTicker']}",
        )
        rows.append(row)
    return rows


def _validate_holding_config(config: Any) -> list[dict[str, Any]]:
    if not isinstance(config, list):
        raise LinkedProjectConfigError("holding config must be a list.")
    seen_ids: set[str] = set()
    seen_holding: set[str] = set()
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(config, start=1):
        if not isinstance(raw, dict):
            raise LinkedProjectConfigError(f"holding row #{idx} must be an object.")
        subsidiaries_raw = raw.get("subsidiaries")
        if not isinstance(subsidiaries_raw, list) or not subsidiaries_raw:
            raise LinkedProjectConfigError(f"holding row #{idx} needs at least one subsidiary.")
        subsidiaries = []
        for sub_idx, sub in enumerate(subsidiaries_raw, start=1):
            if not isinstance(sub, dict):
                raise LinkedProjectConfigError(f"holding row #{idx} subsidiary #{sub_idx} must be an object.")
            subsidiaries.append(
                {
                    "name": _required_text(sub, "name", sub_idx),
                    "ticker": _required_text(sub, "ticker", sub_idx),
                    "sharesHeld": _non_negative_number(sub.get("sharesHeld"), f"subsidiary #{sub_idx} sharesHeld"),
                }
            )
        row = {
            "id": _required_text(raw, "id", idx),
            "name": _required_text(raw, "name", idx),
            "holdingName": _required_text(raw, "holdingName", idx),
            "holdingTicker": _required_text(raw, "holdingTicker", idx),
            "holdingTotalShares": _non_negative_number(raw.get("holdingTotalShares"), "holdingTotalShares"),
            "holdingTreasuryShares": _non_negative_number(raw.get("holdingTreasuryShares"), "holdingTreasuryShares"),
            "subsidiaries": subsidiaries,
        }
        _ensure_unique(seen_ids, row["id"], f"duplicate holding id: {row['id']}")
        _ensure_unique(
            seen_holding,
            row["holdingTicker"].upper(),
            f"duplicate holdingTicker: {row['holdingTicker']}",
        )
        rows.append(row)
    return rows


def _validate_gold_config(config: Any) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise LinkedProjectConfigError("gold config must be an object.")
    assets = config.get("assets")
    if not isinstance(assets, dict) or not assets:
        raise LinkedProjectConfigError("gold config needs assets object.")
    normalized = dict(config)
    normalized_assets = {}
    for key, raw in assets.items():
        if not isinstance(raw, dict):
            raise LinkedProjectConfigError(f"gold asset {key} must be an object.")
        portfolio_codes = raw.get("portfolioCodes") or []
        if not isinstance(portfolio_codes, list):
            raise LinkedProjectConfigError(f"gold asset {key} portfolioCodes must be a list.")
        asset = dict(raw)
        asset["label"] = _required_text(asset, "label", key)
        asset["portfolioCodes"] = [str(code).strip() for code in portfolio_codes if str(code).strip()]
        asset["thresholdPct"] = float(asset.get("thresholdPct", 0) or 0)
        normalized_assets[str(key)] = asset
    normalized["assets"] = normalized_assets
    return normalized


def _required_text(raw: dict[str, Any], field: str, row_label: Any) -> str:
    value = str(raw.get(field) or "").strip()
    if not value:
        raise LinkedProjectConfigError(f"row {row_label}: {field} is required.")
    return value


def _non_negative_number(value: Any, label: str) -> int | float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise LinkedProjectConfigError(f"{label} must be a number.") from exc
    if number < 0:
        raise LinkedProjectConfigError(f"{label} must be >= 0.")
    return int(number) if number.is_integer() else number


def _ensure_unique(seen: set[str], value: str, message: str) -> None:
    key = value.upper()
    if key in seen:
        raise LinkedProjectConfigError(message)
    seen.add(key)
